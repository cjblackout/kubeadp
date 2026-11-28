// Copyright Contributors to the KubeOpenCode project

package handlers

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strconv"
	"strings"

	"github.com/go-chi/chi/v5"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/tools/record"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"

	kubeopenv1alpha1 "github.com/kubeopencode/kubeopencode/api/v1alpha1"
	"github.com/kubeopencode/kubeopencode/internal/controller"
	"github.com/kubeopencode/kubeopencode/internal/kubeadp"
)

var actionLog = ctrl.Log.WithName("task-actions")

// TaskActionHandler handles proxying actions from Tasks to the corresponding Agent's worker.
type TaskActionHandler struct {
	defaultClient    client.Client
	defaultClientset kubernetes.Interface
	clusterDomain    string
	recorder         record.EventRecorder
}

// NewTaskActionHandler creates a new TaskActionHandler.
func NewTaskActionHandler(c client.Client, cs kubernetes.Interface, clusterDomain string) *TaskActionHandler {
	return &TaskActionHandler{
		defaultClient:    c,
		defaultClientset: cs,
		clusterDomain:    clusterDomain,
	}
}

// resolveTaskWorker looks up a Task, resolves its Agent, creates an internal/kubeadp client,
// and returns the client along with the task's job ID.
func (h *TaskActionHandler) resolveTaskWorker(ctx context.Context, namespace, taskName string) (*kubeadp.Client, *kubeopenv1alpha1.Task, string, error) {
	k8sClient := clientFromContext(ctx, h.defaultClient)

	var task kubeopenv1alpha1.Task
	if err := k8sClient.Get(ctx, client.ObjectKey{Namespace: namespace, Name: taskName}, &task); err != nil {
		return nil, nil, "", fmt.Errorf("task not found: %w", err)
	}

	jobID := ""
	if task.Status.Job != nil && task.Status.Job.ID != "" {
		jobID = task.Status.Job.ID
	} else if task.Status.Session != nil && task.Status.Session.ID != "" {
		jobID = task.Status.Session.ID
	}
	if jobID == "" {
		return nil, nil, "", fmt.Errorf("task %q has no running or completed worker job", taskName)
	}

	agentName := ""
	if task.Spec.AgentRef != nil {
		agentName = task.Spec.AgentRef.Name
	} else if task.Status.AgentRef != nil {
		agentName = task.Status.AgentRef.Name
	}
	if agentName == "" {
		return nil, nil, "", fmt.Errorf("task %q has no agent reference", taskName)
	}

	var agent kubeopenv1alpha1.Agent
	if err := k8sClient.Get(ctx, client.ObjectKey{Namespace: namespace, Name: agentName}, &agent); err != nil {
		return nil, nil, "", fmt.Errorf("agent %q not found: %w", agentName, err)
	}

	if agent.Status.URL == "" {
		return nil, nil, "", fmt.Errorf("agent %q has no server URL in status", agentName)
	}

	if err := validateServerURL(agent.Status.URL, h.clusterDomain); err != nil {
		return nil, nil, "", fmt.Errorf("agent %q has invalid server URL: %w", agentName, err)
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
		return nil, nil, "", fmt.Errorf("failed to create worker client: %w", err)
	}

	return adpClient, &task, jobID, nil
}

func (h *TaskActionHandler) recordAction(ctx context.Context, task *kubeopenv1alpha1.Task, action string, message string) {
	k8sClient := clientFromContext(ctx, h.defaultClient)
	clientset := clientsetFromContext(ctx, h.defaultClientset)

	now := metav1.Now()
	// Update lastAction in task status if possible
	if task.Status.Job == nil {
		task.Status.Job = &kubeopenv1alpha1.JobStatus{}
	}
	task.Status.Job.LastAction = &kubeopenv1alpha1.TaskAction{
		Type:   action,
		Time:   now,
		Result: message,
	}
	_ = k8sClient.Status().Update(ctx, task)

	// Also record Kubernetes Event
	if clientset != nil {
		event := &corev1.Event{
			ObjectMeta: metav1.ObjectMeta{
				GenerateName: task.Name + "-",
				Namespace:    task.Namespace,
			},
			InvolvedObject: corev1.ObjectReference{
				Kind:       "Task",
				Namespace:  task.Namespace,
				Name:       task.Name,
				UID:        task.UID,
				APIVersion: "kubeopencode.io/v1alpha1",
			},
			Reason:         "TaskAction" + strings.Title(action),
			Message:        message,
			Type:           corev1.EventTypeNormal,
			FirstTimestamp: now,
			LastTimestamp:  now,
			Count:          1,
		}
		_, _ = clientset.CoreV1().Events(task.Namespace).Create(ctx, event, metav1.CreateOptions{})
	}
}

// Retry handles POST .../tasks/{name}/retry
func (h *TaskActionHandler) Retry(w http.ResponseWriter, r *http.Request) {
	namespace := chi.URLParam(r, "namespace")
	taskName := chi.URLParam(r, "name")

	var req struct {
		Prompt string `json:"prompt"`
	}
	_ = json.NewDecoder(r.Body).Decode(&req)

	adpClient, task, jobID, err := h.resolveTaskWorker(r.Context(), namespace, taskName)
	if err != nil {
		writeError(w, http.StatusBadGateway, "ResolveError", err.Error())
		return
	}

	if err := adpClient.Retry(r.Context(), jobID, req.Prompt); err != nil {
		writeError(w, http.StatusBadGateway, "WorkerError", err.Error())
		return
	}

	h.recordAction(r.Context(), task, "retry", "Task retried with prompt: "+req.Prompt)
	writeJSON(w, http.StatusOK, map[string]string{"status": "retried"})
}

// Kill handles POST .../tasks/{name}/kill
func (h *TaskActionHandler) Kill(w http.ResponseWriter, r *http.Request) {
	namespace := chi.URLParam(r, "namespace")
	taskName := chi.URLParam(r, "name")

	adpClient, task, jobID, err := h.resolveTaskWorker(r.Context(), namespace, taskName)
	if err != nil {
		writeError(w, http.StatusBadGateway, "ResolveError", err.Error())
		return
	}

	if err := adpClient.Kill(r.Context(), jobID); err != nil {
		writeError(w, http.StatusBadGateway, "WorkerError", err.Error())
		return
	}

	h.recordAction(r.Context(), task, "kill", "Task job killed")
	writeJSON(w, http.StatusOK, map[string]string{"status": "killed"})
}

// Commit handles POST .../tasks/{name}/commit
func (h *TaskActionHandler) Commit(w http.ResponseWriter, r *http.Request) {
	namespace := chi.URLParam(r, "namespace")
	taskName := chi.URLParam(r, "name")

	var req struct {
		Message string   `json:"message"`
		Only    []string `json:"only"`
		Exclude []string `json:"exclude"`
	}
	_ = json.NewDecoder(r.Body).Decode(&req)

	adpClient, task, jobID, err := h.resolveTaskWorker(r.Context(), namespace, taskName)
	if err != nil {
		writeError(w, http.StatusBadGateway, "ResolveError", err.Error())
		return
	}

	if err := adpClient.Commit(r.Context(), jobID, req.Message, req.Only, req.Exclude); err != nil {
		writeError(w, http.StatusBadGateway, "WorkerError", err.Error())
		return
	}

	h.recordAction(r.Context(), task, "commit", "Changes committed: "+req.Message)
	writeJSON(w, http.StatusOK, map[string]string{"status": "committed"})
}

// Discard handles POST .../tasks/{name}/discard
func (h *TaskActionHandler) Discard(w http.ResponseWriter, r *http.Request) {
	namespace := chi.URLParam(r, "namespace")
	taskName := chi.URLParam(r, "name")

	var req struct {
		Files []string `json:"files"`
	}
	_ = json.NewDecoder(r.Body).Decode(&req)

	adpClient, task, jobID, err := h.resolveTaskWorker(r.Context(), namespace, taskName)
	if err != nil {
		writeError(w, http.StatusBadGateway, "ResolveError", err.Error())
		return
	}

	if err := adpClient.Discard(r.Context(), jobID, req.Files); err != nil {
		writeError(w, http.StatusBadGateway, "WorkerError", err.Error())
		return
	}

	h.recordAction(r.Context(), task, "discard", fmt.Sprintf("Discarded %d files", len(req.Files)))
	writeJSON(w, http.StatusOK, map[string]string{"status": "discarded"})
}

// Clean handles POST .../tasks/{name}/clean
func (h *TaskActionHandler) Clean(w http.ResponseWriter, r *http.Request) {
	namespace := chi.URLParam(r, "namespace")
	taskName := chi.URLParam(r, "name")

	var req struct {
		Force    bool `json:"force"`
		PurgeLog bool `json:"purge_log"`
	}
	_ = json.NewDecoder(r.Body).Decode(&req)

	adpClient, task, jobID, err := h.resolveTaskWorker(r.Context(), namespace, taskName)
	if err != nil {
		writeError(w, http.StatusBadGateway, "ResolveError", err.Error())
		return
	}

	if err := adpClient.Clean(r.Context(), jobID, req.Force, req.PurgeLog); err != nil {
		writeError(w, http.StatusBadGateway, "WorkerError", err.Error())
		return
	}

	h.recordAction(r.Context(), task, "clean", "Job worktree cleaned")
	writeJSON(w, http.StatusOK, map[string]string{"status": "cleaned"})
}

// Review handles POST .../tasks/{name}/review
func (h *TaskActionHandler) Review(w http.ResponseWriter, r *http.Request) {
	namespace := chi.URLParam(r, "namespace")
	taskName := chi.URLParam(r, "name")

	var req struct {
		ReviewerModel string `json:"reviewer_model"`
	}
	_ = json.NewDecoder(r.Body).Decode(&req)

	adpClient, task, jobID, err := h.resolveTaskWorker(r.Context(), namespace, taskName)
	if err != nil {
		writeError(w, http.StatusBadGateway, "ResolveError", err.Error())
		return
	}

	if err := adpClient.Review(r.Context(), jobID, req.ReviewerModel); err != nil {
		writeError(w, http.StatusBadGateway, "WorkerError", err.Error())
		return
	}

	h.recordAction(r.Context(), task, "review", "Review triggered")
	writeJSON(w, http.StatusOK, map[string]string{"status": "reviewed"})
}

// Message handles POST .../tasks/{name}/message
func (h *TaskActionHandler) Message(w http.ResponseWriter, r *http.Request) {
	namespace := chi.URLParam(r, "namespace")
	taskName := chi.URLParam(r, "name")

	var req struct {
		Text string `json:"text"`
	}
	_ = json.NewDecoder(r.Body).Decode(&req)

	adpClient, task, jobID, err := h.resolveTaskWorker(r.Context(), namespace, taskName)
	if err != nil {
		writeError(w, http.StatusBadGateway, "ResolveError", err.Error())
		return
	}

	if err := adpClient.Message(r.Context(), jobID, req.Text); err != nil {
		writeError(w, http.StatusBadGateway, "WorkerError", err.Error())
		return
	}

	h.recordAction(r.Context(), task, "message", "Message sent to job")
	writeJSON(w, http.StatusOK, map[string]string{"status": "sent"})
}

// Interrupt handles POST .../tasks/{name}/interrupt
func (h *TaskActionHandler) Interrupt(w http.ResponseWriter, r *http.Request) {
	namespace := chi.URLParam(r, "namespace")
	taskName := chi.URLParam(r, "name")

	adpClient, task, jobID, err := h.resolveTaskWorker(r.Context(), namespace, taskName)
	if err != nil {
		writeError(w, http.StatusBadGateway, "ResolveError", err.Error())
		return
	}

	if err := adpClient.Interrupt(r.Context(), jobID); err != nil {
		writeError(w, http.StatusBadGateway, "WorkerError", err.Error())
		return
	}

	h.recordAction(r.Context(), task, "interrupt", "Job interrupted")
	writeJSON(w, http.StatusOK, map[string]string{"status": "interrupted"})
}

// Pause handles POST .../tasks/{name}/pause
func (h *TaskActionHandler) Pause(w http.ResponseWriter, r *http.Request) {
	namespace := chi.URLParam(r, "namespace")
	taskName := chi.URLParam(r, "name")

	var req struct {
		Paused bool `json:"paused"`
	}
	_ = json.NewDecoder(r.Body).Decode(&req)

	adpClient, task, jobID, err := h.resolveTaskWorker(r.Context(), namespace, taskName)
	if err != nil {
		writeError(w, http.StatusBadGateway, "ResolveError", err.Error())
		return
	}

	if err := adpClient.Pause(r.Context(), jobID, req.Paused); err != nil {
		writeError(w, http.StatusBadGateway, "WorkerError", err.Error())
		return
	}

	actionStr := "resumed"
	if req.Paused {
		actionStr = "paused"
	}
	h.recordAction(r.Context(), task, actionStr, "Job "+actionStr)
	writeJSON(w, http.StatusOK, map[string]string{"status": actionStr})
}

// Compact handles POST .../tasks/{name}/compact
func (h *TaskActionHandler) Compact(w http.ResponseWriter, r *http.Request) {
	namespace := chi.URLParam(r, "namespace")
	taskName := chi.URLParam(r, "name")

	adpClient, task, jobID, err := h.resolveTaskWorker(r.Context(), namespace, taskName)
	if err != nil {
		writeError(w, http.StatusBadGateway, "ResolveError", err.Error())
		return
	}

	if err := adpClient.Compact(r.Context(), jobID); err != nil {
		writeError(w, http.StatusBadGateway, "WorkerError", err.Error())
		return
	}

	h.recordAction(r.Context(), task, "compact", "Job context compacted")
	writeJSON(w, http.StatusOK, map[string]string{"status": "compacted"})
}

// Fork handles POST .../tasks/{name}/fork
func (h *TaskActionHandler) Fork(w http.ResponseWriter, r *http.Request) {
	namespace := chi.URLParam(r, "namespace")
	taskName := chi.URLParam(r, "name")

	adpClient, task, jobID, err := h.resolveTaskWorker(r.Context(), namespace, taskName)
	if err != nil {
		writeError(w, http.StatusBadGateway, "ResolveError", err.Error())
		return
	}

	newID, err := adpClient.Fork(r.Context(), jobID)
	if err != nil {
		writeError(w, http.StatusBadGateway, "WorkerError", err.Error())
		return
	}

	h.recordAction(r.Context(), task, "fork", "Forked to "+newID)
	writeJSON(w, http.StatusOK, map[string]string{"status": "forked", "job_id": newID})
}

// Revert handles POST .../tasks/{name}/revert
func (h *TaskActionHandler) Revert(w http.ResponseWriter, r *http.Request) {
	namespace := chi.URLParam(r, "namespace")
	taskName := chi.URLParam(r, "name")

	var req struct {
		Commit string `json:"commit"`
	}
	_ = json.NewDecoder(r.Body).Decode(&req)

	adpClient, task, jobID, err := h.resolveTaskWorker(r.Context(), namespace, taskName)
	if err != nil {
		writeError(w, http.StatusBadGateway, "ResolveError", err.Error())
		return
	}

	if err := adpClient.Revert(r.Context(), jobID, req.Commit); err != nil {
		writeError(w, http.StatusBadGateway, "WorkerError", err.Error())
		return
	}

	h.recordAction(r.Context(), task, "revert", "Reverted commit "+req.Commit)
	writeJSON(w, http.StatusOK, map[string]string{"status": "reverted"})
}

// Export handles GET .../tasks/{name}/export
func (h *TaskActionHandler) Export(w http.ResponseWriter, r *http.Request) {
	namespace := chi.URLParam(r, "namespace")
	taskName := chi.URLParam(r, "name")

	adpClient, _, jobID, err := h.resolveTaskWorker(r.Context(), namespace, taskName)
	if err != nil {
		writeError(w, http.StatusBadGateway, "ResolveError", err.Error())
		return
	}

	reader, err := adpClient.Export(r.Context(), jobID)
	if err != nil {
		writeError(w, http.StatusBadGateway, "WorkerError", err.Error())
		return
	}
	defer reader.Close()

	w.Header().Set("Content-Type", "application/gzip")
	w.Header().Set("Content-Disposition", fmt.Sprintf("attachment; filename=\"%s-%s.tar.gz\"", taskName, jobID))
	_, _ = io.Copy(w, reader)
}

// Diff handles GET .../tasks/{name}/diff
func (h *TaskActionHandler) Diff(w http.ResponseWriter, r *http.Request) {
	namespace := chi.URLParam(r, "namespace")
	taskName := chi.URLParam(r, "name")

	stat := r.URL.Query().Get("stat") == "true"

	adpClient, _, jobID, err := h.resolveTaskWorker(r.Context(), namespace, taskName)
	if err != nil {
		writeError(w, http.StatusBadGateway, "ResolveError", err.Error())
		return
	}

	diffStr, err := adpClient.Diff(r.Context(), jobID, stat)
	if err != nil {
		writeError(w, http.StatusBadGateway, "WorkerError", err.Error())
		return
	}

	w.Header().Set("Content-Type", "text/plain; charset=utf-8")
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write([]byte(diffStr))
}

// Files handles GET .../tasks/{name}/files
func (h *TaskActionHandler) Files(w http.ResponseWriter, r *http.Request) {
	namespace := chi.URLParam(r, "namespace")
	taskName := chi.URLParam(r, "name")

	pathFilter := r.URL.Query().Get("path")
	var depth *uint32
	if dStr := r.URL.Query().Get("depth"); dStr != "" {
		if d, err := strconv.ParseUint(dStr, 10, 32); err == nil {
			d32 := uint32(d)
			depth = &d32
		}
	}

	adpClient, _, jobID, err := h.resolveTaskWorker(r.Context(), namespace, taskName)
	if err != nil {
		writeError(w, http.StatusBadGateway, "ResolveError", err.Error())
		return
	}

	fileList, err := adpClient.Files(r.Context(), jobID, pathFilter, depth)
	if err != nil {
		writeError(w, http.StatusBadGateway, "WorkerError", err.Error())
		return
	}

	writeJSON(w, http.StatusOK, fileList)
}

// Cat handles GET .../tasks/{name}/cat
func (h *TaskActionHandler) Cat(w http.ResponseWriter, r *http.Request) {
	namespace := chi.URLParam(r, "namespace")
	taskName := chi.URLParam(r, "name")

	filePath := r.URL.Query().Get("path")
	lines := r.URL.Query().Get("lines")
	var maxBytes *uint64
	if mbStr := r.URL.Query().Get("max_bytes"); mbStr != "" {
		if mb, err := strconv.ParseUint(mbStr, 10, 64); err == nil {
			maxBytes = &mb
		}
	}

	adpClient, _, jobID, err := h.resolveTaskWorker(r.Context(), namespace, taskName)
	if err != nil {
		writeError(w, http.StatusBadGateway, "ResolveError", err.Error())
		return
	}

	content, err := adpClient.Cat(r.Context(), jobID, filePath, lines, maxBytes)
	if err != nil {
		writeError(w, http.StatusBadGateway, "WorkerError", err.Error())
		return
	}

	w.Header().Set("Content-Type", "text/plain; charset=utf-8")
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write([]byte(content))
}

// Log handles GET .../tasks/{name}/job-log
func (h *TaskActionHandler) Log(w http.ResponseWriter, r *http.Request) {
	namespace := chi.URLParam(r, "namespace")
	taskName := chi.URLParam(r, "name")

	offset := uint64(0)
	if offStr := r.URL.Query().Get("offset"); offStr != "" {
		if off, err := strconv.ParseUint(offStr, 10, 64); err == nil {
			offset = off
		}
	}

	adpClient, _, jobID, err := h.resolveTaskWorker(r.Context(), namespace, taskName)
	if err != nil {
		writeError(w, http.StatusBadGateway, "ResolveError", err.Error())
		return
	}

	content, nextOffset, err := adpClient.Log(r.Context(), jobID, offset)
	if err != nil {
		writeError(w, http.StatusBadGateway, "WorkerError", err.Error())
		return
	}

	writeJSON(w, http.StatusOK, map[string]any{
		"log":         content,
		"next_offset": nextOffset,
	})
}

// Events handles GET .../tasks/{name}/events (SSE)
func (h *TaskActionHandler) Events(w http.ResponseWriter, r *http.Request) {
	namespace := chi.URLParam(r, "namespace")
	taskName := chi.URLParam(r, "name")

	adpClient, _, jobID, err := h.resolveTaskWorker(r.Context(), namespace, taskName)
	if err != nil {
		writeError(w, http.StatusBadGateway, "ResolveError", err.Error())
		return
	}

	eventChan, err := adpClient.Events(r.Context(), jobID)
	if err != nil {
		writeError(w, http.StatusBadGateway, "WorkerError", err.Error())
		return
	}

	flusher, ok := w.(http.Flusher)
	if !ok {
		writeError(w, http.StatusInternalServerError, "StreamingUnsupported", "Streaming unsupported")
		return
	}

	w.Header().Set("Content-Type", "text/event-stream")
	w.Header().Set("Cache-Control", "no-cache")
	w.Header().Set("Connection", "keep-alive")

	for {
		select {
		case <-r.Context().Done():
			return
		case ev, open := <-eventChan:
			if !open {
				return
			}
			data, _ := json.Marshal(ev)
			fmt.Fprintf(w, "event: %s\ndata: %s\n\n", ev.Kind, string(data))
			flusher.Flush()
		}
	}
}

// Todo handles GET .../tasks/{name}/todo
func (h *TaskActionHandler) Todo(w http.ResponseWriter, r *http.Request) {
	namespace := chi.URLParam(r, "namespace")
	taskName := chi.URLParam(r, "name")

	adpClient, _, jobID, err := h.resolveTaskWorker(r.Context(), namespace, taskName)
	if err != nil {
		writeError(w, http.StatusBadGateway, "ResolveError", err.Error())
		return
	}

	items, err := adpClient.Todo(r.Context(), jobID)
	if err != nil {
		writeError(w, http.StatusBadGateway, "WorkerError", err.Error())
		return
	}

	writeJSON(w, http.StatusOK, items)
}

// Permissions handles GET .../tasks/{name}/permissions
func (h *TaskActionHandler) Permissions(w http.ResponseWriter, r *http.Request) {
	namespace := chi.URLParam(r, "namespace")
	taskName := chi.URLParam(r, "name")

	adpClient, _, jobID, err := h.resolveTaskWorker(r.Context(), namespace, taskName)
	if err != nil {
		writeError(w, http.StatusBadGateway, "ResolveError", err.Error())
		return
	}

	perms, err := adpClient.Permissions(r.Context(), jobID)
	if err != nil {
		writeError(w, http.StatusBadGateway, "WorkerError", err.Error())
		return
	}

	writeJSON(w, http.StatusOK, perms)
}
