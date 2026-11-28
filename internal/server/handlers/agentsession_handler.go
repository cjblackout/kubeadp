// Copyright Contributors to the KubeOpenCode project

package handlers

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/go-chi/chi/v5"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/yaml"

	kubeopenv1alpha1 "github.com/kubeopencode/kubeopencode/api/v1alpha1"
	"github.com/kubeopencode/kubeopencode/internal/controller"
	"github.com/kubeopencode/kubeopencode/internal/kubeadp"
	"github.com/kubeopencode/kubeopencode/internal/server/types"
)

// AgentSessionHandler handles AgentSession-related HTTP requests
type AgentSessionHandler struct {
	defaultClient client.Client
	clusterDomain string
}

// NewAgentSessionHandler creates a new AgentSessionHandler
func NewAgentSessionHandler(c client.Client, clusterDomain string) *AgentSessionHandler {
	return &AgentSessionHandler{
		defaultClient: c,
		clusterDomain: clusterDomain,
	}
}

func (h *AgentSessionHandler) getClient(r *http.Request) client.Client {
	return clientFromContext(r.Context(), h.defaultClient)
}

// ListAll returns all AgentSessions across all namespaces
func (h *AgentSessionHandler) ListAll(w http.ResponseWriter, r *http.Request) {
	h.listSessions(w, r, "")
}

// List returns AgentSessions in a namespace
func (h *AgentSessionHandler) List(w http.ResponseWriter, r *http.Request) {
	namespace := chi.URLParam(r, "namespace")
	h.listSessions(w, r, namespace)
}

func (h *AgentSessionHandler) listSessions(w http.ResponseWriter, r *http.Request, namespace string) {
	ctx := r.Context()
	k8sClient := h.getClient(r)

	filterOpts, err := ParseFilterOptions(r)
	if err != nil {
		writeError(w, http.StatusBadRequest, "Invalid filter parameters", err.Error())
		return
	}

	var sessionList kubeopenv1alpha1.AgentSessionList
	listOpts := BuildListOptions(namespace, filterOpts)
	if err := k8sClient.List(ctx, &sessionList, listOpts...); err != nil {
		writeError(w, http.StatusInternalServerError, "Failed to list sessions", err.Error())
		return
	}

	var filteredItems []kubeopenv1alpha1.AgentSession
	for _, s := range sessionList.Items {
		if !MatchesNameFilter(s.Name, filterOpts.Name) {
			continue
		}
		filteredItems = append(filteredItems, s)
	}

	sort.Slice(filteredItems, func(i, j int) bool {
		if filterOpts.SortOrder == "asc" {
			return filteredItems[i].CreationTimestamp.Before(&filteredItems[j].CreationTimestamp)
		}
		return filteredItems[j].CreationTimestamp.Before(&filteredItems[i].CreationTimestamp)
	})

	totalCount := len(filteredItems)
	start := min(filterOpts.Offset, totalCount)
	end := min(start+filterOpts.Limit, totalCount)
	paginatedItems := filteredItems[start:end]

	responses := make([]types.AgentSessionResponse, len(paginatedItems))
	for i, s := range paginatedItems {
		responses[i] = h.toResponse(&s)
	}

	writeJSON(w, http.StatusOK, types.AgentSessionListResponse{
		Sessions: responses,
		Total:    totalCount,
		Pagination: &types.Pagination{
			Limit:      filterOpts.Limit,
			Offset:     filterOpts.Offset,
			TotalCount: totalCount,
			HasMore:    end < totalCount,
		},
	})
}

// Get returns an AgentSession by name
func (h *AgentSessionHandler) Get(w http.ResponseWriter, r *http.Request) {
	namespace := chi.URLParam(r, "namespace")
	name := chi.URLParam(r, "name")
	ctx := r.Context()
	k8sClient := h.getClient(r)

	var session kubeopenv1alpha1.AgentSession
	if err := k8sClient.Get(ctx, client.ObjectKey{Namespace: namespace, Name: name}, &session); err != nil {
		if apierrors.IsNotFound(err) {
			writeError(w, http.StatusNotFound, "AgentSession not found", "")
			return
		}
		writeError(w, http.StatusInternalServerError, "Failed to get session", err.Error())
		return
	}

	if r.URL.Query().Get("output") == "yaml" {
		yamlBytes, err := yaml.Marshal(session)
		if err != nil {
			writeError(w, http.StatusInternalServerError, "Failed to marshal YAML", err.Error())
			return
		}
		w.Header().Set("Content-Type", "application/x-yaml")
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write(yamlBytes)
		return
	}

	writeJSON(w, http.StatusOK, h.toResponse(&session))
}

// Create creates a new AgentSession
func (h *AgentSessionHandler) Create(w http.ResponseWriter, r *http.Request) {
	namespace := chi.URLParam(r, "namespace")
	ctx := r.Context()
	k8sClient := h.getClient(r)

	var req types.CreateAgentSessionRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeError(w, http.StatusBadRequest, "Invalid request body", err.Error())
		return
	}

	if req.Name == "" || req.AgentName == "" {
		writeError(w, http.StatusBadRequest, "Name and agentName are required", "")
		return
	}

	var engine *kubeopenv1alpha1.AgentEngine
	if req.Engine != "" {
		eng := kubeopenv1alpha1.AgentEngine(req.Engine)
		engine = &eng
	}

	session := &kubeopenv1alpha1.AgentSession{
		ObjectMeta: metav1.ObjectMeta{
			Name:      req.Name,
			Namespace: namespace,
		},
		Spec: kubeopenv1alpha1.AgentSessionSpec{
			AgentRef: kubeopenv1alpha1.LocalAgentReference{
				Name: req.AgentName,
			},
			Engine:           engine,
			InstructionsFile: req.InstructionsFile,
			Sandbox:          req.Sandbox,
		},
	}

	if err := k8sClient.Create(ctx, session); err != nil {
		if apierrors.IsAlreadyExists(err) {
			writeError(w, http.StatusConflict, "Session already exists", err.Error())
			return
		}
		writeError(w, http.StatusInternalServerError, "Failed to create session", err.Error())
		return
	}

	writeJSON(w, http.StatusCreated, h.toResponse(session))
}

// Delete deletes an AgentSession
func (h *AgentSessionHandler) Delete(w http.ResponseWriter, r *http.Request) {
	namespace := chi.URLParam(r, "namespace")
	name := chi.URLParam(r, "name")
	ctx := r.Context()
	k8sClient := h.getClient(r)

	var session kubeopenv1alpha1.AgentSession
	session.Namespace = namespace
	session.Name = name

	if err := k8sClient.Delete(ctx, &session); err != nil {
		if apierrors.IsNotFound(err) {
			writeError(w, http.StatusNotFound, "Session not found", "")
			return
		}
		writeError(w, http.StatusInternalServerError, "Failed to delete session", err.Error())
		return
	}

	w.WriteHeader(http.StatusNoContent)
}

// Start resumes or restarts a session via worker client
func (h *AgentSessionHandler) Start(w http.ResponseWriter, r *http.Request) {
	h.forwardSessionAction(w, r, "start")
}

// Stop pauses or stops a session via worker client
func (h *AgentSessionHandler) Stop(w http.ResponseWriter, r *http.Request) {
	h.forwardSessionAction(w, r, "stop")
}

// Restart restarts a session process via worker client
func (h *AgentSessionHandler) Restart(w http.ResponseWriter, r *http.Request) {
	h.forwardSessionAction(w, r, "restart")
}

// Log streams or reads session logs
func (h *AgentSessionHandler) Log(w http.ResponseWriter, r *http.Request) {
	namespace := chi.URLParam(r, "namespace")
	name := chi.URLParam(r, "name")
	ctx := r.Context()
	k8sClient := h.getClient(r)

	client, err := h.getWorkerClientForSession(ctx, k8sClient, namespace, name)
	if err != nil {
		writeError(w, http.StatusBadGateway, "Failed to connect to worker", err.Error())
		return
	}

	var tail *uint64
	if tStr := r.URL.Query().Get("tail"); tStr != "" {
		if val, err := strconv.ParseUint(tStr, 10, 64); err == nil {
			tail = &val
		}
	}

	logData, err := client.SessionLog(ctx, name, tail)
	if err != nil {
		writeError(w, http.StatusInternalServerError, "Failed to read session log", err.Error())
		return
	}

	w.Header().Set("Content-Type", "text/plain; charset=utf-8")
	w.WriteHeader(http.StatusOK)
	_, _ = io.WriteString(w, logData)
}

// Message injects a prompt or text message into a running session
func (h *AgentSessionHandler) Message(w http.ResponseWriter, r *http.Request) {
	namespace := chi.URLParam(r, "namespace")
	name := chi.URLParam(r, "name")
	ctx := r.Context()
	k8sClient := h.getClient(r)

	var body struct {
		Text string `json:"text"`
	}
	if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
		writeError(w, http.StatusBadRequest, "Invalid request body", err.Error())
		return
	}

	client, err := h.getWorkerClientForSession(ctx, k8sClient, namespace, name)
	if err != nil {
		writeError(w, http.StatusBadGateway, "Failed to connect to worker", err.Error())
		return
	}

	resp, err := client.SessionMessage(ctx, name, body.Text)
	if err != nil {
		writeError(w, http.StatusInternalServerError, "Failed to send message to session", err.Error())
		return
	}

	writeJSON(w, http.StatusAccepted, resp)
}

func (h *AgentSessionHandler) forwardSessionAction(w http.ResponseWriter, r *http.Request, action string) {
	namespace := chi.URLParam(r, "namespace")
	name := chi.URLParam(r, "name")
	ctx := r.Context()
	k8sClient := h.getClient(r)

	client, err := h.getWorkerClientForSession(ctx, k8sClient, namespace, name)
	if err != nil {
		writeError(w, http.StatusBadGateway, "Failed to connect to worker", err.Error())
		return
	}

	var s *kubeopenv1alpha1.AgentSession
	var actErr error
	switch action {
	case "start":
		_, actErr = client.SessionStart(ctx, name)
	case "stop":
		_, actErr = client.SessionStop(ctx, name)
	case "restart":
		_, actErr = client.SessionRestart(ctx, name)
	}

	if actErr != nil {
		writeError(w, http.StatusInternalServerError, fmt.Sprintf("Failed to %s session", action), actErr.Error())
		return
	}

	// Re-fetch CRD and return
	var session kubeopenv1alpha1.AgentSession
	_ = k8sClient.Get(ctx, clientKey(namespace, name), &session)
	writeJSON(w, http.StatusOK, h.toResponse(&session))
	_ = s
}

func (h *AgentSessionHandler) getWorkerClientForSession(ctx context.Context, k8sClient client.Client, namespace, name string) (*kubeadp.Client, error) {
	var session kubeopenv1alpha1.AgentSession
	if err := k8sClient.Get(ctx, clientKey(namespace, name), &session); err != nil {
		return nil, err
	}
	var agent kubeopenv1alpha1.Agent
	if err := k8sClient.Get(ctx, clientKey(namespace, session.Spec.AgentRef.Name), &agent); err != nil {
		return nil, err
	}
	if agent.Status.URL == "" {
		return nil, fmt.Errorf("agent %q has no server URL in status", agent.Name)
	}
	if err := validateServerURL(agent.Status.URL, h.clusterDomain); err != nil {
		return nil, fmt.Errorf("agent %q has invalid server URL: %w", agent.Name, err)
	}
	var token string
	var secret corev1.Secret
	if err := k8sClient.Get(ctx, clientKey(namespace, controller.KubeadpSecretName(agent.Name)), &secret); err == nil {
		if tok, ok := secret.Data[controller.KubeadpAPITokenKey]; ok {
			token = strings.TrimSpace(string(tok))
		}
	}
	return kubeadp.New(agent.Status.URL, token)
}

func clientKey(namespace, name string) client.ObjectKey {
	return client.ObjectKey{Namespace: namespace, Name: name}
}

func (h *AgentSessionHandler) toResponse(s *kubeopenv1alpha1.AgentSession) types.AgentSessionResponse {
	var st *time.Time
	if s.Status.StartTime != nil {
		st = &s.Status.StartTime.Time
	}
	return types.AgentSessionResponse{
		Name:             s.Name,
		Namespace:        s.Namespace,
		AgentName:        s.Spec.AgentRef.Name,
		Engine:           s.Status.Engine,
		Phase:            string(s.Status.Phase),
		PID:              s.Status.PID,
		StartTime:        st,
		UptimeSecs:       s.Status.UptimeSecs,
		RestartCount:     s.Status.RestartCount,
		LastExitReason:   s.Status.LastExitReason,
		LastHealthResult: s.Status.LastHealthResult,
		Paused:           s.Status.Paused,
		Controlled:       s.Status.Controlled,
		EngineReady:      s.Status.EngineReady,
		CreatedAt:        s.CreationTimestamp.Time,
	}
}
