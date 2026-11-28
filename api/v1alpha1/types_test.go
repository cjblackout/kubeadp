// Copyright Contributors to the KubeOpenCode project

package v1alpha1

import (
	"encoding/json"
	"os"
	"path/filepath"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/yaml"
)

func TestTaskJSONRoundTrip(t *testing.T) {
	ptrBool := func(b bool) *bool { return &b }
	dur := metav1.Duration{Duration: 10 * time.Minute}
	task := Task{
		TypeMeta: metav1.TypeMeta{
			APIVersion: "kubeopencode.io/v1alpha1",
			Kind:       "Task",
		},
		ObjectMeta: metav1.ObjectMeta{
			Name:      "test-task",
			Namespace: "default",
		},
		Spec: TaskSpec{
			AgentRef: &AgentReference{Name: "dev-agent"},
			Project:  "my-repo",
			GitRef:   "feat/xyz",
			Model:    "claude-3-7-sonnet",
			Effort:   "high",
			Sandbox:  ptrBool(true),
			AddDirs:  []string{"/workspace/extra"},
			Plan:     ptrBool(false),
			Timeout:  &dur,
			Hindsight: &TaskHindsight{
				Enabled: ptrBool(true),
				Bank:    "dev-bank",
				Query:   "context priming",
			},
			Proxy: "http://proxy.local:8080",
			FromPatch: &FromPatchSource{
				Text: "diff --git a/file b/file...",
			},
			Jobset: "jobset-123",
		},
		Status: TaskExecutionStatus{
			Phase: TaskPhaseRunning,
			Job: &JobStatus{
				ID:             "job-456",
				Status:         "running",
				Engine:         "agy",
				Model:          "claude-3-7-sonnet",
				ElapsedSeconds: 42,
				ConversationID: "conv-789",
				Branch:         "agent/worktree-1",
				Worktree:       "/workspace/.agent-jobs/job-456",
				Commit:         "abcdef1",
				Diff: &DiffStat{
					Files:     2,
					Additions: 20,
					Deletions: 5,
				},
				Todo: []TodoItemStatus{
					{Content: "Refactor core", Status: "done"},
					{Content: "Run tests", Status: "in_progress"},
				},
				PendingPermissions: 1,
				LastAction: &TaskAction{
					Type:   "interrupt",
					By:     "user-admin",
					Time:   metav1.Now(),
					Result: "ok",
				},
			},
		},
	}

	bytes, err := json.Marshal(task)
	if err != nil {
		t.Fatalf("failed to marshal task: %v", err)
	}

	var decoded Task
	if err := json.Unmarshal(bytes, &decoded); err != nil {
		t.Fatalf("failed to unmarshal task: %v", err)
	}

	if decoded.Spec.Project != "my-repo" {
		t.Errorf("expected project 'my-repo', got %q", decoded.Spec.Project)
	}
	if decoded.Status.Job == nil || decoded.Status.Job.ID != "job-456" {
		t.Errorf("expected job id 'job-456', got %+v", decoded.Status.Job)
	}
}

func TestAgentWorkerConfigJSONRoundTrip(t *testing.T) {
	ptrBool := func(b bool) *bool { return &b }
	ptrInt32 := func(i int32) *int32 { return &i }
	eng := AgentEngineAgy

	agent := Agent{
		TypeMeta: metav1.TypeMeta{
			APIVersion: "kubeopencode.io/v1alpha1",
			Kind:       "Agent",
		},
		ObjectMeta: metav1.ObjectMeta{
			Name:      "test-agent",
			Namespace: "default",
		},
		Spec: AgentSpec{
			Engine: &eng,
			Worker: &WorkerConfig{
				MaxParallel:       ptrInt32(8),
				GCMaxAgeDays:      ptrInt32(7),
				StallMinutes:      ptrInt32(15),
				LaunchTimeoutSecs: ptrInt32(30),
				ShutdownGraceSecs: ptrInt32(25),
				DefaultModel:      "claude-3-7-sonnet",
				DefaultEffort:     "medium",
				DefaultBaseBranch: "main",
				DefaultEngine:     &eng,
				MaxRuntime:        "2h",
				JobsDirName:       ".agent-jobs",
				ReposRoot:         "/workspace/.agent-repos",
				Proxy: &WorkerProxyConfig{
					HTTP: &ValueOrSecretRef{
						SecretKeyRef: &corev1.SecretKeySelector{
							LocalObjectReference: corev1.LocalObjectReference{Name: "proxy-secret"},
							Key:                  "http_proxy",
						},
					},
				},
				Hindsight: &WorkerHindsightConfig{
					Enabled:    ptrBool(true),
					Bank:       "bank-1",
					AutoRecall: ptrBool(true),
					AutoRetain: ptrBool(false),
					APIKeySecretRef: &corev1.SecretKeySelector{
						LocalObjectReference: corev1.LocalObjectReference{Name: "hindsight-secret"},
						Key:                  "api-key",
					},
				},
				RemoteControl: &WorkerRemoteControlConfig{
					Enabled:   ptrBool(true),
					Autostart: ptrBool(true),
					CheckSecs: ptrInt32(30),
				},
				A2A: &WorkerA2AConfig{
					Enabled:          ptrBool(true),
					InterruptMinSecs: ptrInt32(15),
					LiveStdin:        ptrBool(true),
					Echo:             ptrBool(true),
				},
				Sandbox: &WorkerSandboxConfig{
					ROPaths:  []string{"/workspace/read-only", "/etc/instructions"},
					EnvAllow: []string{"CUSTOM_FLAG"},
				},
			},
		},
		Status: AgentStatus{
			Worker: &WorkerStatus{
				Version: "2026.11.64",
				Ready:   true,
				Capacity: &WorkerCapacityStatus{
					MaxParallel: 8,
					Running:     2,
				},
				Disk: &WorkerDiskStatus{
					TotalBytes: 100000000000,
					FreeBytes:  50000000000,
				},
				Auth: &WorkerAuthStatus{
					State: "ok",
					Engines: []WorkerEngineAuthState{
						{Engine: "opencode", State: "authenticated"},
						{Engine: "agy", State: "authenticated"},
					},
				},
				Capabilities: []WorkerCapability{
					{
						Engine:    "agy",
						Transport: "local",
						Endpoints: map[string]string{
							"job_start": "native",
							"pause":     "native",
						},
					},
				},
				Models: []string{"claude-3-7-sonnet", "gemini-2.5-pro"},
				Quota: []WorkerQuotaStatus{
					{
						Group:            "claude",
						RemainingPercent: 85,
					},
				},
				Projects: []WorkerProjectStatus{
					{
						Name:   "kubeadp",
						Branch: "main",
						Jobs:   12,
					},
				},
			},
		},
	}

	bytes, err := json.Marshal(agent)
	if err != nil {
		t.Fatalf("failed to marshal agent: %v", err)
	}

	var decoded Agent
	if err := json.Unmarshal(bytes, &decoded); err != nil {
		t.Fatalf("failed to unmarshal agent: %v", err)
	}

	if decoded.Spec.Worker == nil || *decoded.Spec.Worker.MaxParallel != 8 {
		t.Errorf("expected MaxParallel 8, got %+v", decoded.Spec.Worker)
	}
	if decoded.Status.Worker == nil || decoded.Status.Worker.Capacity.Running != 2 {
		t.Errorf("expected worker status running 2, got %+v", decoded.Status.Worker)
	}
}

func TestCRDYAMLsContainCELRulesAndEnums(t *testing.T) {
	crdFiles := []string{
		"deploy/crds/kubeopencode.io_tasks.yaml",
		"deploy/crds/kubeopencode.io_agents.yaml",
		"deploy/crds/kubeopencode.io_agenttemplates.yaml",
		"deploy/crds/kubeopencode.io_kubeopencodeconfigs.yaml",
		"charts/kubeopencode/crds/kubeopencode.io_tasks.yaml",
		"charts/kubeopencode/crds/kubeopencode.io_agents.yaml",
		"charts/kubeopencode/crds/kubeopencode.io_agenttemplates.yaml",
		"charts/kubeopencode/crds/kubeopencode.io_kubeopencodeconfigs.yaml",
	}

	for _, rel := range crdFiles {
		path := filepath.Join("../..", rel)
		data, err := os.ReadFile(path)
		if err != nil {
			t.Fatalf("failed to read CRD file %s: %v", path, err)
		}

		var raw map[string]any
		if err := yaml.Unmarshal(data, &raw); err != nil {
			t.Fatalf("failed to parse YAML in %s: %v", path, err)
		}

		content := string(data)
		switch {
		case filepath.Base(rel) == "kubeopencode.io_tasks.yaml":
			// Assert CEL rules and enum values exist
			for _, exp := range []string{
				"either agentRef or templateRef must be specified",
				"quota-exhausted",
				"self-terminated",
			} {
				if !contains(content, exp) {
					t.Errorf("%s missing expected token %q", rel, exp)
				}
			}
		case filepath.Base(rel) == "kubeopencode.io_agents.yaml":
			for _, exp := range []string{
				"spec.engine must match spec.worker.defaultEngine when both are",
				"opencode",
				"agy",
				"claude",
				"roPaths entries must be absolute paths without",
			} {
				if !contains(content, exp) {
					t.Errorf("%s missing expected token %q", rel, exp)
				}
			}
		}
	}
}

func contains(s, substr string) bool {
	return len(s) >= len(substr) && (s == substr || len(substr) == 0 || searchString(s, substr))
}

func searchString(s, substr string) bool {
	for i := 0; i+len(substr) <= len(s); i++ {
		if s[i:i+len(substr)] == substr {
			return true
		}
	}
	return false
}
