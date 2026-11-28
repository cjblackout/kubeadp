// Copyright Contributors to the KubeOpenCode project

package controller

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	kubeopenv1alpha1 "github.com/kubeopencode/kubeopencode/api/v1alpha1"
	fakekubeadp "github.com/kubeopencode/kubeopencode/internal/kubeadp/fake"
)

func TestSyncAgentWorker_Success(t *testing.T) {
	fw := fakekubeadp.New("test-token")
	defer fw.Close()

	scheme := runtime.NewScheme()
	_ = corev1.AddToScheme(scheme)
	_ = kubeopenv1alpha1.AddToScheme(scheme)

	agent := &kubeopenv1alpha1.Agent{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "test-agent",
			Namespace: "default",
		},
		Status: kubeopenv1alpha1.AgentStatus{
			URL: fw.URL(),
		},
	}

	secret := &corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{
			Name:      KubeadpSecretName("test-agent"),
			Namespace: "default",
		},
		Data: map[string][]byte{
			KubeadpAPITokenKey: []byte("test-token"),
		},
	}

	cl := fake.NewClientBuilder().WithScheme(scheme).WithObjects(agent, secret).Build()
	r := &AgentReconciler{
		Client: cl,
		Scheme: scheme,
	}

	interval, err := r.syncAgentWorker(context.Background(), agent)
	if err != nil {
		t.Fatalf("syncAgentWorker failed: %v", err)
	}

	if interval != 30*time.Second {
		t.Errorf("expected 30s sync interval, got %v", interval)
	}

	if agent.Status.Worker == nil {
		t.Fatalf("expected status.worker to be populated")
	}

	if !agent.Status.Worker.Ready {
		t.Errorf("expected agent.status.worker.ready to be true")
	}

	if agent.Status.Worker.LastSyncTime == nil {
		t.Errorf("expected lastSyncTime to be set")
	}

	cond := agent.Status.Conditions
	foundReachable := false
	for _, c := range cond {
		if c.Type == kubeopenv1alpha1.ConditionTypeWorkerReachable && c.Status == metav1.ConditionTrue {
			foundReachable = true
			break
		}
	}
	if !foundReachable {
		t.Errorf("expected WorkerReachable=True condition")
	}
}

func TestSyncAgentWorker_Unreachable(t *testing.T) {
	scheme := runtime.NewScheme()
	_ = corev1.AddToScheme(scheme)
	_ = kubeopenv1alpha1.AddToScheme(scheme)

	// Create server and close it immediately
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Error(w, "down", http.StatusInternalServerError)
	}))
	ts.Close()

	agent := &kubeopenv1alpha1.Agent{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "unreachable-agent",
			Namespace: "default",
		},
		Status: kubeopenv1alpha1.AgentStatus{
			URL: ts.URL,
		},
	}

	cl := fake.NewClientBuilder().WithScheme(scheme).WithObjects(agent).Build()
	r := &AgentReconciler{
		Client: cl,
		Scheme: scheme,
	}

	interval, err := r.syncAgentWorker(context.Background(), agent)
	if err != nil {
		t.Fatalf("expected nil error on unreachable worker, got %v", err)
	}

	if interval != 5*time.Second {
		t.Errorf("expected 5s backoff interval for unreachable worker, got %v", interval)
	}

	if agent.Status.Worker == nil || agent.Status.Worker.Ready {
		t.Errorf("expected agent.status.worker.ready to be false")
	}

	foundReachableFalse := false
	for _, c := range agent.Status.Conditions {
		if c.Type == kubeopenv1alpha1.ConditionTypeWorkerReachable && c.Status == metav1.ConditionFalse {
			foundReachableFalse = true
			break
		}
	}
	if !foundReachableFalse {
		t.Errorf("expected WorkerReachable=False condition")
	}
}
