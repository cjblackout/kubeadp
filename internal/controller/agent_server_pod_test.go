// Copyright Contributors to the KubeOpenCode project

package controller

import (
	"context"
	"strings"
	"testing"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	kubeopenv1alpha1 "github.com/kubeopencode/kubeopencode/api/v1alpha1"
)

func newResolveTestScheme(t *testing.T) *runtime.Scheme {
	t.Helper()
	s := runtime.NewScheme()
	if err := corev1.AddToScheme(s); err != nil {
		t.Fatalf("failed to add corev1 to scheme: %v", err)
	}
	if err := kubeopenv1alpha1.AddToScheme(s); err != nil {
		t.Fatalf("failed to add kubeopencode types to scheme: %v", err)
	}
	return s
}

func serverPodObject(namespace, name string, phase corev1.PodPhase, ready bool) *corev1.Pod {
	pod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:      name,
			Namespace: namespace,
			Labels:    getServerLabels("my-agent"),
		},
		Status: corev1.PodStatus{Phase: phase},
	}
	if ready {
		pod.Status.Conditions = []corev1.PodCondition{
			{Type: corev1.PodReady, Status: corev1.ConditionTrue},
		}
	}
	return pod
}

func TestResolveAgentServerPod(t *testing.T) {
	tests := []struct {
		name    string
		noAgent bool
		agent   *kubeopenv1alpha1.Agent
		pods    []runtime.Object
		wantPod string
		wantErr string
	}{
		{
			name:    "agent not found",
			noAgent: true,
			wantErr: `agent "my-agent" not found`,
		},
		{
			name: "agent is suspended",
			agent: &kubeopenv1alpha1.Agent{
				ObjectMeta: metav1.ObjectMeta{Name: "my-agent", Namespace: "default"},
				Spec:       kubeopenv1alpha1.AgentSpec{WorkspaceDir: "/workspace"},
				Status:     kubeopenv1alpha1.AgentStatus{Suspended: true},
			},
			wantErr: `agent "my-agent" is suspended`,
		},
		{
			name: "agent not ready",
			agent: &kubeopenv1alpha1.Agent{
				ObjectMeta: metav1.ObjectMeta{Name: "my-agent", Namespace: "default"},
				Spec:       kubeopenv1alpha1.AgentSpec{WorkspaceDir: "/workspace"},
			},
			wantErr: `agent "my-agent" is not ready`,
		},
		{
			name: "no pods at all",
			agent: &kubeopenv1alpha1.Agent{
				ObjectMeta: metav1.ObjectMeta{Name: "my-agent", Namespace: "default"},
				Spec:       kubeopenv1alpha1.AgentSpec{WorkspaceDir: "/workspace"},
				Status:     kubeopenv1alpha1.AgentStatus{Ready: true},
			},
			wantErr: `no ready server pod found`,
		},
		{
			name: "skips pods of other agents (label match)",
			agent: &kubeopenv1alpha1.Agent{
				ObjectMeta: metav1.ObjectMeta{Name: "my-agent", Namespace: "default"},
				Spec:       kubeopenv1alpha1.AgentSpec{WorkspaceDir: "/workspace"},
				Status:     kubeopenv1alpha1.AgentStatus{Ready: true},
			},
			pods: []runtime.Object{
				func() *corev1.Pod {
					p := serverPodObject("default", "other-agent-server-abc", corev1.PodRunning, true)
					p.Labels = getServerLabels("other-agent")
					return p
				}(),
			},
			wantErr: `no ready server pod found`,
		},
		{
			name: "skips pending pods",
			agent: &kubeopenv1alpha1.Agent{
				ObjectMeta: metav1.ObjectMeta{Name: "my-agent", Namespace: "default"},
				Spec:       kubeopenv1alpha1.AgentSpec{WorkspaceDir: "/workspace"},
				Status:     kubeopenv1alpha1.AgentStatus{Ready: true},
			},
			pods: []runtime.Object{
				serverPodObject("default", "my-agent-server-pending", corev1.PodPending, false),
			},
			wantErr: `no ready server pod found`,
		},
		{
			name: "skips running but not ready pods",
			agent: &kubeopenv1alpha1.Agent{
				ObjectMeta: metav1.ObjectMeta{Name: "my-agent", Namespace: "default"},
				Spec:       kubeopenv1alpha1.AgentSpec{WorkspaceDir: "/workspace"},
				Status:     kubeopenv1alpha1.AgentStatus{Ready: true},
			},
			pods: []runtime.Object{
				serverPodObject("default", "my-agent-server-running", corev1.PodRunning, false),
			},
			wantErr: `no ready server pod found`,
		},
		{
			name: "finds ready pod and returns the main container",
			agent: &kubeopenv1alpha1.Agent{
				ObjectMeta: metav1.ObjectMeta{Name: "my-agent", Namespace: "default"},
				Spec:       kubeopenv1alpha1.AgentSpec{WorkspaceDir: "/workspace"},
				Status:     kubeopenv1alpha1.AgentStatus{Ready: true},
			},
			pods: []runtime.Object{
				serverPodObject("default", "my-agent-server-abc123", corev1.PodRunning, true),
			},
			wantPod: "my-agent-server-abc123",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var objects []runtime.Object
			if !tt.noAgent {
				objects = append(objects, tt.agent)
			}
			objects = append(objects, tt.pods...)

			c := fake.NewClientBuilder().
				WithScheme(newResolveTestScheme(t)).
				WithRuntimeObjects(objects...).
				WithStatusSubresource(&kubeopenv1alpha1.Agent{}).
				Build()

			ref, err := ResolveAgentServerPod(context.Background(), c, "default", "my-agent")
			if tt.wantErr != "" {
				if err == nil {
					t.Fatalf("expected error containing %q, got nil", tt.wantErr)
				}
				if !strings.Contains(err.Error(), tt.wantErr) {
					t.Errorf("expected error containing %q, got %q", tt.wantErr, err.Error())
				}
				return
			}
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if ref.Pod != tt.wantPod {
				t.Errorf("pod = %q, want %q", ref.Pod, tt.wantPod)
			}
			if ref.Container != ServerContainerName {
				t.Errorf("container = %q, want %q", ref.Container, ServerContainerName)
			}
		})
	}
}
