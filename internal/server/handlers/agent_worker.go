// Copyright Contributors to the KubeOpenCode project

package handlers

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"strings"

	"github.com/go-chi/chi/v5"
	corev1 "k8s.io/api/core/v1"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"

	kubeopenv1alpha1 "github.com/kubeopencode/kubeopencode/api/v1alpha1"
	"github.com/kubeopencode/kubeopencode/internal/controller"
	"github.com/kubeopencode/kubeopencode/internal/kubeadp"
)

var agentWorkerLog = ctrl.Log.WithName("agent-worker")

// AgentWorkerHandler handles proxying agent worker queries and maintenance operations.
type AgentWorkerHandler struct {
	defaultClient client.Client
	clusterDomain string
}

// NewAgentWorkerHandler creates a new AgentWorkerHandler.
func NewAgentWorkerHandler(c client.Client, clusterDomain string) *AgentWorkerHandler {
	return &AgentWorkerHandler{
		defaultClient: c,
		clusterDomain: clusterDomain,
	}
}

func (h *AgentWorkerHandler) resolveAgentClient(ctx context.Context, namespace, agentName string) (*kubeadp.Client, *kubeopenv1alpha1.Agent, error) {
	k8sClient := clientFromContext(ctx, h.defaultClient)

	var agent kubeopenv1alpha1.Agent
	if err := k8sClient.Get(ctx, client.ObjectKey{Namespace: namespace, Name: agentName}, &agent); err != nil {
		return nil, nil, fmt.Errorf("agent %q not found: %w", agentName, err)
	}

	if agent.Status.URL == "" {
		return nil, nil, fmt.Errorf("agent %q has no server URL in status", agentName)
	}

	if err := validateServerURL(agent.Status.URL, h.clusterDomain); err != nil {
		return nil, nil, fmt.Errorf("agent %q has invalid server URL: %w", agentName, err)
	}

	var token string
	var secret corev1.Secret
	if err := k8sClient.Get(ctx, client.ObjectKey{Namespace: namespace, Name: controller.KubeadpSecretName(agentName)}, &secret); err == nil {
		if tok, ok := secret.Data[controller.KubeadpAPITokenKey]; ok {
			token = strings.TrimSpace(string(tok))
		}
	}

	adpClient, err := kubeadp.New(agent.Status.URL, token)
	if err != nil {
		return nil, nil, fmt.Errorf("failed to create worker client: %w", err)
	}

	return adpClient, &agent, nil
}

// Info handles GET .../agents/{name}/worker/info
func (h *AgentWorkerHandler) Info(w http.ResponseWriter, r *http.Request) {
	ns := chi.URLParam(r, "namespace")
	name := chi.URLParam(r, "name")

	client, _, err := h.resolveAgentClient(r.Context(), ns, name)
	if err != nil {
		writeError(w, http.StatusBadGateway, "ResolveError", err.Error())
		return
	}

	info, err := client.Info(r.Context())
	if err != nil {
		writeError(w, http.StatusBadGateway, "WorkerError", err.Error())
		return
	}

	writeJSON(w, http.StatusOK, info)
}

// Capabilities handles GET .../agents/{name}/worker/capabilities
func (h *AgentWorkerHandler) Capabilities(w http.ResponseWriter, r *http.Request) {
	ns := chi.URLParam(r, "namespace")
	name := chi.URLParam(r, "name")

	client, _, err := h.resolveAgentClient(r.Context(), ns, name)
	if err != nil {
		writeError(w, http.StatusBadGateway, "ResolveError", err.Error())
		return
	}

	caps, err := client.Capabilities(r.Context())
	if err != nil {
		writeError(w, http.StatusBadGateway, "WorkerError", err.Error())
		return
	}

	writeJSON(w, http.StatusOK, caps)
}

// Models handles GET .../agents/{name}/worker/models
func (h *AgentWorkerHandler) Models(w http.ResponseWriter, r *http.Request) {
	ns := chi.URLParam(r, "namespace")
	name := chi.URLParam(r, "name")

	client, _, err := h.resolveAgentClient(r.Context(), ns, name)
	if err != nil {
		writeError(w, http.StatusBadGateway, "ResolveError", err.Error())
		return
	}

	models, err := client.Models(r.Context())
	if err != nil {
		writeError(w, http.StatusBadGateway, "WorkerError", err.Error())
		return
	}

	writeJSON(w, http.StatusOK, models)
}

// Quota handles GET .../agents/{name}/worker/quota
func (h *AgentWorkerHandler) Quota(w http.ResponseWriter, r *http.Request) {
	ns := chi.URLParam(r, "namespace")
	name := chi.URLParam(r, "name")

	fresh := r.URL.Query().Get("fresh") == "true"

	client, _, err := h.resolveAgentClient(r.Context(), ns, name)
	if err != nil {
		writeError(w, http.StatusBadGateway, "ResolveError", err.Error())
		return
	}

	quota, err := client.Quota(r.Context(), fresh)
	if err != nil {
		writeError(w, http.StatusBadGateway, "WorkerError", err.Error())
		return
	}

	writeJSON(w, http.StatusOK, quota)
}

// Projects handles GET .../agents/{name}/worker/projects
func (h *AgentWorkerHandler) Projects(w http.ResponseWriter, r *http.Request) {
	ns := chi.URLParam(r, "namespace")
	name := chi.URLParam(r, "name")

	client, _, err := h.resolveAgentClient(r.Context(), ns, name)
	if err != nil {
		writeError(w, http.StatusBadGateway, "ResolveError", err.Error())
		return
	}

	projects, err := client.Projects(r.Context())
	if err != nil {
		writeError(w, http.StatusBadGateway, "WorkerError", err.Error())
		return
	}

	writeJSON(w, http.StatusOK, projects)
}

// Jobs handles GET .../agents/{name}/worker/jobs
func (h *AgentWorkerHandler) Jobs(w http.ResponseWriter, r *http.Request) {
	ns := chi.URLParam(r, "namespace")
	name := chi.URLParam(r, "name")

	filter := kubeadp.ListFilter{
		Project: r.URL.Query().Get("project"),
		Engine:  r.URL.Query().Get("engine"),
	}
	if st := r.URL.Query().Get("status"); st != "" {
		filter.Status = strings.Split(st, ",")
	}

	client, _, err := h.resolveAgentClient(r.Context(), ns, name)
	if err != nil {
		writeError(w, http.StatusBadGateway, "ResolveError", err.Error())
		return
	}

	jobs, err := client.List(r.Context(), filter)
	if err != nil {
		writeError(w, http.StatusBadGateway, "WorkerError", err.Error())
		return
	}

	writeJSON(w, http.StatusOK, jobs)
}

// AuthStatus handles GET .../agents/{name}/worker/auth/status
func (h *AgentWorkerHandler) AuthStatus(w http.ResponseWriter, r *http.Request) {
	ns := chi.URLParam(r, "namespace")
	name := chi.URLParam(r, "name")

	client, _, err := h.resolveAgentClient(r.Context(), ns, name)
	if err != nil {
		writeError(w, http.StatusBadGateway, "ResolveError", err.Error())
		return
	}

	st, err := client.AuthStatus(r.Context())
	if err != nil {
		writeError(w, http.StatusBadGateway, "WorkerError", err.Error())
		return
	}

	writeJSON(w, http.StatusOK, st)
}

// AuthStart handles POST .../agents/{name}/worker/auth/start
func (h *AgentWorkerHandler) AuthStart(w http.ResponseWriter, r *http.Request) {
	ns := chi.URLParam(r, "namespace")
	name := chi.URLParam(r, "name")

	client, _, err := h.resolveAgentClient(r.Context(), ns, name)
	if err != nil {
		writeError(w, http.StatusBadGateway, "ResolveError", err.Error())
		return
	}

	startRes, err := client.AuthStart(r.Context())
	if err != nil {
		writeError(w, http.StatusBadGateway, "WorkerError", err.Error())
		return
	}

	writeJSON(w, http.StatusOK, startRes)
}

// AuthCode handles POST .../agents/{name}/worker/auth/code
func (h *AgentWorkerHandler) AuthCode(w http.ResponseWriter, r *http.Request) {
	ns := chi.URLParam(r, "namespace")
	name := chi.URLParam(r, "name")

	var req struct {
		Code string `json:"code"`
	}
	_ = json.NewDecoder(r.Body).Decode(&req)

	client, _, err := h.resolveAgentClient(r.Context(), ns, name)
	if err != nil {
		writeError(w, http.StatusBadGateway, "ResolveError", err.Error())
		return
	}

	if err := client.AuthCode(r.Context(), req.Code); err != nil {
		writeError(w, http.StatusBadGateway, "WorkerError", err.Error())
		return
	}

	writeJSON(w, http.StatusOK, map[string]string{"status": "ok"})
}

// AuthCancel handles POST .../agents/{name}/worker/auth/cancel
func (h *AgentWorkerHandler) AuthCancel(w http.ResponseWriter, r *http.Request) {
	ns := chi.URLParam(r, "namespace")
	name := chi.URLParam(r, "name")

	client, _, err := h.resolveAgentClient(r.Context(), ns, name)
	if err != nil {
		writeError(w, http.StatusBadGateway, "ResolveError", err.Error())
		return
	}

	if err := client.AuthCancel(r.Context()); err != nil {
		writeError(w, http.StatusBadGateway, "WorkerError", err.Error())
		return
	}

	writeJSON(w, http.StatusOK, map[string]string{"status": "cancelled"})
}

// RemoteControlStatus handles GET .../agents/{name}/worker/remote-control/status
func (h *AgentWorkerHandler) RemoteControlStatus(w http.ResponseWriter, r *http.Request) {
	ns := chi.URLParam(r, "namespace")
	name := chi.URLParam(r, "name")

	client, _, err := h.resolveAgentClient(r.Context(), ns, name)
	if err != nil {
		writeError(w, http.StatusBadGateway, "ResolveError", err.Error())
		return
	}

	st, err := client.RemoteControlStatus(r.Context())
	if err != nil {
		writeError(w, http.StatusBadGateway, "WorkerError", err.Error())
		return
	}

	writeJSON(w, http.StatusOK, st)
}

// RemoteControlEnable handles POST .../agents/{name}/worker/remote-control/enable
func (h *AgentWorkerHandler) RemoteControlEnable(w http.ResponseWriter, r *http.Request) {
	ns := chi.URLParam(r, "namespace")
	name := chi.URLParam(r, "name")

	client, _, err := h.resolveAgentClient(r.Context(), ns, name)
	if err != nil {
		writeError(w, http.StatusBadGateway, "ResolveError", err.Error())
		return
	}

	if err := client.RemoteControlEnable(r.Context()); err != nil {
		writeError(w, http.StatusBadGateway, "WorkerError", err.Error())
		return
	}

	writeJSON(w, http.StatusOK, map[string]string{"status": "enabled"})
}

// RemoteControlDisable handles POST .../agents/{name}/worker/remote-control/disable
func (h *AgentWorkerHandler) RemoteControlDisable(w http.ResponseWriter, r *http.Request) {
	ns := chi.URLParam(r, "namespace")
	name := chi.URLParam(r, "name")

	client, _, err := h.resolveAgentClient(r.Context(), ns, name)
	if err != nil {
		writeError(w, http.StatusBadGateway, "ResolveError", err.Error())
		return
	}

	if err := client.RemoteControlDisable(r.Context()); err != nil {
		writeError(w, http.StatusBadGateway, "WorkerError", err.Error())
		return
	}

	writeJSON(w, http.StatusOK, map[string]string{"status": "disabled"})
}

// GC handles POST .../agents/{name}/worker/gc
func (h *AgentWorkerHandler) GC(w http.ResponseWriter, r *http.Request) {
	ns := chi.URLParam(r, "namespace")
	name := chi.URLParam(r, "name")

	var req kubeadp.GCRequest
	_ = json.NewDecoder(r.Body).Decode(&req)

	client, _, err := h.resolveAgentClient(r.Context(), ns, name)
	if err != nil {
		writeError(w, http.StatusBadGateway, "ResolveError", err.Error())
		return
	}

	res, err := client.GC(r.Context(), req)
	if err != nil {
		writeError(w, http.StatusBadGateway, "WorkerError", err.Error())
		return
	}

	writeJSON(w, http.StatusOK, res)
}

// Init handles POST .../agents/{name}/worker/init
func (h *AgentWorkerHandler) Init(w http.ResponseWriter, r *http.Request) {
	ns := chi.URLParam(r, "namespace")
	name := chi.URLParam(r, "name")

	var req kubeadp.InitRequest
	_ = json.NewDecoder(r.Body).Decode(&req)

	client, _, err := h.resolveAgentClient(r.Context(), ns, name)
	if err != nil {
		writeError(w, http.StatusBadGateway, "ResolveError", err.Error())
		return
	}

	if err := client.Init(r.Context(), req); err != nil {
		writeError(w, http.StatusBadGateway, "WorkerError", err.Error())
		return
	}

	writeJSON(w, http.StatusOK, map[string]string{"status": "initialized"})
}

// RepairJobs handles POST .../agents/{name}/worker/jobs/repair
func (h *AgentWorkerHandler) RepairJobs(w http.ResponseWriter, r *http.Request) {
	ns := chi.URLParam(r, "namespace")
	name := chi.URLParam(r, "name")

	client, _, err := h.resolveAgentClient(r.Context(), ns, name)
	if err != nil {
		writeError(w, http.StatusBadGateway, "ResolveError", err.Error())
		return
	}

	if err := client.RepairAll(r.Context()); err != nil {
		writeError(w, http.StatusBadGateway, "WorkerError", err.Error())
		return
	}

	writeJSON(w, http.StatusOK, map[string]string{"status": "repaired"})
}

// JobsetEnd handles POST .../agents/{name}/worker/jobsets/{id}/end
func (h *AgentWorkerHandler) JobsetEnd(w http.ResponseWriter, r *http.Request) {
	ns := chi.URLParam(r, "namespace")
	name := chi.URLParam(r, "name")
	jobsetID := chi.URLParam(r, "id")

	client, _, err := h.resolveAgentClient(r.Context(), ns, name)
	if err != nil {
		writeError(w, http.StatusBadGateway, "ResolveError", err.Error())
		return
	}

	res, err := client.JobsetEnd(r.Context(), jobsetID, nil)
	if err != nil {
		writeError(w, http.StatusBadGateway, "WorkerError", err.Error())
		return
	}

	writeJSON(w, http.StatusOK, res)
}
