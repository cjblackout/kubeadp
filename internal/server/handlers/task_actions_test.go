// Copyright Contributors to the KubeOpenCode project

package handlers

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/go-chi/chi/v5"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	kubeopenv1alpha1 "github.com/kubeopencode/kubeopencode/api/v1alpha1"
	"github.com/kubeopencode/kubeopencode/internal/controller"
	"github.com/kubeopencode/kubeopencode/internal/kubeadp"
	fakekubeadp "github.com/kubeopencode/kubeopencode/internal/kubeadp/fake"
)

func TestTaskActionHandler(t *testing.T) {
	fw := fakekubeadp.New("token-123")
	defer fw.Close()

	fw.AddJob(&kubeadp.Job{
		JobID:  "job-1",
		Status: "running",
		Engine: "opencode",
	})

	scheme := runtime.NewScheme()
	_ = corev1.AddToScheme(scheme)
	_ = kubeopenv1alpha1.AddToScheme(scheme)

	agent := &kubeopenv1alpha1.Agent{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "test-agent",
			Namespace: "default",
		},
		Status: kubeopenv1alpha1.AgentStatus{
			URL:   fw.URL(),
			Ready: true,
		},
	}

	secret := &corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{
			Name:      controller.KubeadpSecretName("test-agent"),
			Namespace: "default",
		},
		Data: map[string][]byte{
			controller.KubeadpAPITokenKey: []byte("token-123"),
		},
	}

	task := &kubeopenv1alpha1.Task{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "test-task",
			Namespace: "default",
		},
		Spec: kubeopenv1alpha1.TaskSpec{
			AgentRef: &kubeopenv1alpha1.AgentReference{
				Name: "test-agent",
			},
		},
		Status: kubeopenv1alpha1.TaskExecutionStatus{
			Job: &kubeopenv1alpha1.JobStatus{
				ID: "job-1",
			},
		},
	}

	cl := fake.NewClientBuilder().WithScheme(scheme).WithStatusSubresource(&kubeopenv1alpha1.Task{}).WithObjects(agent, secret, task).Build()
	handler := NewTaskActionHandler(cl, nil, "cluster.local")

	// 1. Retry
	r := chi.NewRouter()
	r.Post("/namespaces/{namespace}/tasks/{name}/retry", handler.Retry)
	r.Post("/namespaces/{namespace}/tasks/{name}/kill", handler.Kill)
	r.Get("/namespaces/{namespace}/tasks/{name}/todo", handler.Todo)
	r.Get("/namespaces/{namespace}/tasks/{name}/diff", handler.Diff)

	req := httptest.NewRequest(http.MethodPost, "/namespaces/default/tasks/test-task/retry", stringsReader(`{"prompt":"retry please"}`))
	req.Header.Set("Content-Type", "application/json")
	w := httptest.NewRecorder()
	r.ServeHTTP(w, req)

	if w.Code != http.StatusOK {
		t.Fatalf("expected 200 OK from retry, got %d: %s", w.Code, w.Body.String())
	}

	// Verify task lastAction recorded
	var updatedTask kubeopenv1alpha1.Task
	if err := cl.Get(context.Background(), client.ObjectKey{Namespace: "default", Name: "test-task"}, &updatedTask); err != nil {
		t.Fatalf("failed to get updated task: %v", err)
	}
	if updatedTask.Status.Job == nil || updatedTask.Status.Job.LastAction == nil {
		t.Fatalf("expected lastAction to be set on task")
	}
	if updatedTask.Status.Job.LastAction.Type != "retry" {
		t.Errorf("expected lastAction.type retry, got %s", updatedTask.Status.Job.LastAction.Type)
	}

	// 2. Kill
	reqKill := httptest.NewRequest(http.MethodPost, "/namespaces/default/tasks/test-task/kill", nil)
	wKill := httptest.NewRecorder()
	r.ServeHTTP(wKill, reqKill)
	if wKill.Code != http.StatusOK {
		t.Fatalf("expected 200 OK from kill, got %d: %s", wKill.Code, wKill.Body.String())
	}

	// 3. Todo
	reqTodo := httptest.NewRequest(http.MethodGet, "/namespaces/default/tasks/test-task/todo", nil)
	wTodo := httptest.NewRecorder()
	r.ServeHTTP(wTodo, reqTodo)
	if wTodo.Code != http.StatusOK {
		t.Fatalf("expected 200 OK from todo, got %d: %s", wTodo.Code, wTodo.Body.String())
	}

	// 4. Diff
	reqDiff := httptest.NewRequest(http.MethodGet, "/namespaces/default/tasks/test-task/diff?stat=true", nil)
	wDiff := httptest.NewRecorder()
	r.ServeHTTP(wDiff, reqDiff)
	if wDiff.Code != http.StatusOK {
		t.Fatalf("expected 200 OK from diff, got %d: %s", wDiff.Code, wDiff.Body.String())
	}
}

func stringsReader(s string) *strings.Reader {
	return strings.NewReader(s)
}
