// Copyright Contributors to the KubeOpenCode project

package handlers

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/go-chi/chi/v5"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	kubeopenv1alpha1 "github.com/kubeopencode/kubeopencode/api/v1alpha1"
	"github.com/kubeopencode/kubeopencode/internal/controller"
	fakekubeadp "github.com/kubeopencode/kubeopencode/internal/kubeadp/fake"
)

func TestAgentWorkerHandler(t *testing.T) {
	fw := fakekubeadp.New("token-xyz")
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
			controller.KubeadpAPITokenKey: []byte("token-xyz"),
		},
	}

	cl := fake.NewClientBuilder().WithScheme(scheme).WithObjects(agent, secret).Build()
	h := NewAgentWorkerHandler(cl, "cluster.local")

	r := chi.NewRouter()
	r.Get("/namespaces/{namespace}/agents/{name}/worker/info", h.Info)
	r.Get("/namespaces/{namespace}/agents/{name}/worker/capabilities", h.Capabilities)
	r.Get("/namespaces/{namespace}/agents/{name}/worker/models", h.Models)
	r.Get("/namespaces/{namespace}/agents/{name}/worker/quota", h.Quota)
	r.Get("/namespaces/{namespace}/agents/{name}/worker/projects", h.Projects)
	r.Get("/namespaces/{namespace}/agents/{name}/worker/auth/status", h.AuthStatus)
	r.Get("/namespaces/{namespace}/agents/{name}/worker/remote-control/status", h.RemoteControlStatus)
	r.Post("/namespaces/{namespace}/agents/{name}/worker/remote-control/enable", h.RemoteControlEnable)
	r.Post("/namespaces/{namespace}/agents/{name}/worker/remote-control/disable", h.RemoteControlDisable)
	r.Post("/namespaces/{namespace}/agents/{name}/worker/gc", h.GC)
	r.Post("/namespaces/{namespace}/agents/{name}/worker/init", h.Init)
	r.Post("/namespaces/{namespace}/agents/{name}/worker/jobs/repair", h.RepairJobs)

	// 1. Info
	req := httptest.NewRequest(http.MethodGet, "/namespaces/default/agents/test-agent/worker/info", nil)
	w := httptest.NewRecorder()
	r.ServeHTTP(w, req)
	if w.Code != http.StatusOK {
		t.Fatalf("expected 200 from worker info, got %d: %s", w.Code, w.Body.String())
	}

	// 2. Capabilities
	reqCaps := httptest.NewRequest(http.MethodGet, "/namespaces/default/agents/test-agent/worker/capabilities", nil)
	wCaps := httptest.NewRecorder()
	r.ServeHTTP(wCaps, reqCaps)
	if wCaps.Code != http.StatusOK {
		t.Fatalf("expected 200 from worker capabilities, got %d: %s", wCaps.Code, wCaps.Body.String())
	}

	// 3. Models
	reqModels := httptest.NewRequest(http.MethodGet, "/namespaces/default/agents/test-agent/worker/models", nil)
	wModels := httptest.NewRecorder()
	r.ServeHTTP(wModels, reqModels)
	if wModels.Code != http.StatusOK {
		t.Fatalf("expected 200 from worker models, got %d: %s", wModels.Code, wModels.Body.String())
	}

	// 4. Remote Control Enable
	reqRC := httptest.NewRequest(http.MethodPost, "/namespaces/default/agents/test-agent/worker/remote-control/enable", nil)
	wRC := httptest.NewRecorder()
	r.ServeHTTP(wRC, reqRC)
	if wRC.Code != http.StatusOK {
		t.Fatalf("expected 200 from worker remote-control enable, got %d: %s", wRC.Code, wRC.Body.String())
	}

	// 5. GC
	reqGC := httptest.NewRequest(http.MethodPost, "/namespaces/default/agents/test-agent/worker/gc", strings.NewReader(`{}`))
	wGC := httptest.NewRecorder()
	r.ServeHTTP(wGC, reqGC)
	if wGC.Code != http.StatusOK {
		t.Fatalf("expected 200 from worker gc, got %d: %s", wGC.Code, wGC.Body.String())
	}
}
