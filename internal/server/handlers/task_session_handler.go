// Copyright Contributors to the KubeOpenCode project

package handlers

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httputil"
	"net/url"
	"time"

	"github.com/go-chi/chi/v5"
	corev1 "k8s.io/api/core/v1"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"

	kubeopenv1alpha1 "github.com/kubeopencode/kubeopencode/api/v1alpha1"
	"github.com/kubeopencode/kubeopencode/internal/controller"
)

var sessionLog = ctrl.Log.WithName("task-session")

// sessionProxyTimeout is the timeout for proxying session requests to the Agent's OpenCode server.
const sessionProxyTimeout = 30 * time.Second

// TaskSessionHandler handles proxying session-related requests from a Task
// to the corresponding Agent's kubeadp-agent worker (the Task session ID is the job ID).
// This enables the KubeOpenCode UI to display conversation history without
// requiring direct access to the Agent's worker API.
type TaskSessionHandler struct {
	defaultClient client.Client
	clusterDomain string
}

// NewTaskSessionHandler creates a new TaskSessionHandler.
func NewTaskSessionHandler(c client.Client, clusterDomain string) *TaskSessionHandler {
	return &TaskSessionHandler{defaultClient: c, clusterDomain: clusterDomain}
}

// resolveTaskSessionURL looks up a Task, finds its session info and Agent server URL,
// and returns the base session URL on the Agent's OpenCode server.
// This skips the suspended check because session data is read-only and should be
// accessible even when the Agent is in standby.
func (h *TaskSessionHandler) resolveTaskSessionURL(ctx context.Context, namespace, taskName string) (string, string, string, error) {
	k8sClient := clientFromContext(ctx, h.defaultClient)

	// Get the Task
	var task kubeopenv1alpha1.Task
	if err := k8sClient.Get(ctx, client.ObjectKey{Namespace: namespace, Name: taskName}, &task); err != nil {
		return "", "", "", fmt.Errorf("task not found: %w", err)
	}

	// Check that task has session info
	if task.Status.Session == nil || task.Status.Session.ID == "" {
		return "", "", "", fmt.Errorf("task %q has no session information", taskName)
	}

	// Get the Agent's server URL directly from Agent status.
	// We intentionally skip the suspended check because session data is
	// immutable/read-only — users should be able to view conversation history
	// even when the Agent is suspended or in standby.
	if task.Status.AgentRef == nil {
		return "", "", "", fmt.Errorf("task %q has no agent reference", taskName)
	}

	agentName := task.Status.AgentRef.Name
	var agent kubeopenv1alpha1.Agent
	if err := k8sClient.Get(ctx, client.ObjectKey{Namespace: namespace, Name: agentName}, &agent); err != nil {
		return "", "", "", fmt.Errorf("agent not found: %w", err)
	}

	if agent.Status.URL == "" {
		return "", "", "", fmt.Errorf("agent %q has no server URL in status", agentName)
	}

	if err := validateServerURL(agent.Status.URL, h.clusterDomain); err != nil {
		return "", "", "", fmt.Errorf("agent %q has invalid server URL: %w", agentName, err)
	}

	var token string
	var secret corev1.Secret
	if err := k8sClient.Get(ctx, client.ObjectKey{Namespace: namespace, Name: controller.KubeadpSecretName(agentName)}, &secret); err == nil {
		if tok, ok := secret.Data[controller.KubeadpAPITokenKey]; ok {
			token = string(tok)
		}
	}

	return agent.Status.URL, task.Status.Session.ID, token, nil
}

// GetSession proxies GET /api/v1/jobs/:id (the job record: status, timing) of the
// Agent's kubeadp-agent worker. The Task session ID is the job ID.
// Route: GET /api/v1/namespaces/{namespace}/tasks/{name}/session
func (h *TaskSessionHandler) GetSession(w http.ResponseWriter, r *http.Request) {
	namespace := chi.URLParam(r, "namespace")
	taskName := chi.URLParam(r, "name")

	ctx := r.Context()
	serverURL, sessionID, token, err := h.resolveTaskSessionURL(ctx, namespace, taskName)
	if err != nil {
		sessionLog.Error(err, "Failed to resolve session", "namespace", namespace, "task", taskName)
		writeError(w, http.StatusBadGateway, "Cannot resolve task session", err.Error())
		return
	}

	h.proxyToSession(w, r, serverURL, fmt.Sprintf("/api/v1/jobs/%s", sessionID), "", token)
}

// GetSessionMessages proxies GET /api/v1/jobs/:id/log (the job log as text) of the
// Agent's kubeadp-agent worker. The Task session ID is the job ID.
// Route: GET /api/v1/namespaces/{namespace}/tasks/{name}/session/messages
func (h *TaskSessionHandler) GetSessionMessages(w http.ResponseWriter, r *http.Request) {
	namespace := chi.URLParam(r, "namespace")
	taskName := chi.URLParam(r, "name")

	ctx := r.Context()
	serverURL, sessionID, token, err := h.resolveTaskSessionURL(ctx, namespace, taskName)
	if err != nil {
		sessionLog.Error(err, "Failed to resolve session", "namespace", namespace, "task", taskName)
		writeError(w, http.StatusBadGateway, "Cannot resolve task session", err.Error())
		return
	}

	// Forward query parameters (e.g., before, limit for pagination) separately
	// to avoid embedding them in the path string.
	h.proxyToSession(w, r, serverURL, fmt.Sprintf("/api/v1/jobs/%s/log", sessionID), r.URL.RawQuery, token)
}

// proxyToSession creates a reverse proxy to the Agent's OpenCode server
// targeting the given path and query string.
func (h *TaskSessionHandler) proxyToSession(w http.ResponseWriter, r *http.Request, serverURL, targetPath, queryString, token string) {
	target, err := url.Parse(serverURL)
	if err != nil {
		writeError(w, http.StatusInternalServerError, "Invalid server URL", err.Error())
		return
	}

	// Use a bounded timeout instead of detaching completely from chi's context.
	ctx, cancel := context.WithTimeout(context.WithoutCancel(r.Context()), sessionProxyTimeout)
	defer cancel()

	proxy := &httputil.ReverseProxy{
		Director: func(req *http.Request) {
			req.URL.Scheme = target.Scheme
			req.URL.Host = target.Host
			req.URL.Path = targetPath
			req.URL.RawQuery = queryString
			req.Host = target.Host
			// Never forward the caller's credentials; authenticate to the worker instead.
			setUpstreamAuth(req.Header, token)
		},
		FlushInterval: -1,
		ErrorHandler: func(w http.ResponseWriter, r *http.Request, err error) {
			sessionLog.Error(err, "Session proxy error", "path", targetPath)
			writeError(w, http.StatusBadGateway, "Session proxy error", err.Error())
		},
	}

	sessionLog.V(1).Info("Proxying session request", "serverURL", serverURL, "path", targetPath)
	proxy.ServeHTTP(w, r.WithContext(ctx))
}
