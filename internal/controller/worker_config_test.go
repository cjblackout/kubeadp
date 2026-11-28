// Copyright Contributors to the KubeOpenCode project

package controller

import (
	"strings"
	"testing"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	kubeopenv1alpha1 "github.com/kubeopencode/kubeopencode/api/v1alpha1"
)

func TestRenderWorkerConfigGolden(t *testing.T) {
	ptrBool := func(b bool) *bool { return &b }
	ptrInt32 := func(i int32) *int32 { return &i }
	engAgy := kubeopenv1alpha1.AgentEngineAgy
	engOpenCode := kubeopenv1alpha1.AgentEngineOpenCode
	proxyVal := "http://proxy.corp.internal:8080"

	clusterCfg := &kubeopenv1alpha1.KubeOpenCodeConfig{
		Spec: kubeopenv1alpha1.KubeOpenCodeConfigSpec{
			WorkerDefaults: &kubeopenv1alpha1.WorkerConfig{
				MaxParallel:       ptrInt32(4),
				GCMaxAgeDays:      ptrInt32(14),
				DefaultModel:      "default-cluster-model",
				DefaultEngine:     &engOpenCode,
				ShutdownGraceSecs: ptrInt32(30),
			},
		},
	}

	tmpl := &kubeopenv1alpha1.AgentTemplate{
		Spec: kubeopenv1alpha1.AgentTemplateSpec{
			Engine: &engAgy,
			Worker: &kubeopenv1alpha1.WorkerConfig{
				MaxParallel:  ptrInt32(8),
				DefaultModel: "template-model",
				StallMinutes: ptrInt32(20),
				A2A: &kubeopenv1alpha1.WorkerA2AConfig{
					InterruptMinSecs: ptrInt32(15),
					LiveStdin:        ptrBool(true),
					Echo:             ptrBool(true),
				},
				Sandbox: &kubeopenv1alpha1.WorkerSandboxConfig{
					ROPaths: []string{"/etc/global-instructions", "/workspace/shared"},
				},
			},
		},
	}

	agent := &kubeopenv1alpha1.Agent{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "test-agent",
			Namespace: "default",
		},
		Spec: kubeopenv1alpha1.AgentSpec{
			Engine: &engAgy,
			Worker: &kubeopenv1alpha1.WorkerConfig{
				MaxParallel:       ptrInt32(16), // Overrides tmpl (8) and cluster (4)
				LaunchTimeoutSecs: ptrInt32(25),
				MaxRuntime:        "1h30m",
				Proxy: &kubeopenv1alpha1.WorkerProxyConfig{
					HTTP: &kubeopenv1alpha1.ValueOrSecretRef{
						Value: &proxyVal,
					},
					HTTPS: &kubeopenv1alpha1.ValueOrSecretRef{
						SecretKeyRef: &corev1.SecretKeySelector{
							LocalObjectReference: corev1.LocalObjectReference{Name: "proxy-secret"},
							Key:                  "https_proxy",
						},
					},
				},
				Hindsight: &kubeopenv1alpha1.WorkerHindsightConfig{
					Enabled:    ptrBool(true),
					Bank:       "bank-42",
					AutoRecall: ptrBool(true),
					APIKeySecretRef: &corev1.SecretKeySelector{
						LocalObjectReference: corev1.LocalObjectReference{Name: "hs-secret"},
						Key:                  "key",
					},
				},
				RemoteControl: &kubeopenv1alpha1.WorkerRemoteControlConfig{
					Enabled:   ptrBool(true),
					Autostart: ptrBool(true),
					CheckSecs: ptrInt32(20),
				},
			},
		},
	}

	rendered := RenderWorkerConfig(agent, tmpl, clusterCfg)
	if len(rendered.Errors) > 0 {
		t.Fatalf("unexpected validation errors: %+v", rendered.Errors)
	}

	tomlStr := string(rendered.ConfigTOML)

	// Verify sections and key values in rendered TOML
	expectedLines := []string{
		"[common]",
		`http_proxy = "http://proxy.corp.internal:8080"`,
		`sandbox_ro_paths = ["/etc/global-instructions", "/workspace/shared"]`,
		"[worker]",
		`a2a_echo = true`,
		`a2a_interrupt_min_secs = 15`,
		`a2a_live_stdin = true`,
		`default_engine = "agy"`,
		`default_model = "template-model"`,
		`gc_max_age_days = 14`,
		`hindsight_auto_recall = true`,
		`hindsight_bank = "bank-42"`,
		`hindsight_enabled = true`,
		`launch_timeout_secs = 25`,
		`max_parallel = 16`,
		`max_runtime = "1h30m"`,
		`remote_control = true`,
		`remote_control_autostart = true`,
		`remote_control_check_secs = 20`,
		`shutdown_grace_secs = 30`,
		`stall_minutes = 20`,
	}

	for _, line := range expectedLines {
		if !strings.Contains(tomlStr, line) {
			t.Errorf("rendered TOML missing line %q\nFull output:\n%s", line, tomlStr)
		}
	}

	// Verify environment variables (Secret refs)
	if len(rendered.EnvVars) != 2 {
		t.Fatalf("expected 2 secret env vars, got %d: %+v", len(rendered.EnvVars), rendered.EnvVars)
	}

	foundHTTPSProxy := false
	foundHindsightKey := false
	for _, env := range rendered.EnvVars {
		if env.Name == "AGENT_RUN_HTTPS_PROXY" && env.ValueFrom.SecretKeyRef.Name == "proxy-secret" {
			foundHTTPSProxy = true
		}
		if env.Name == "AGENT_RUN_HINDSIGHT_API_KEY" && env.ValueFrom.SecretKeyRef.Name == "hs-secret" {
			foundHindsightKey = true
		}
	}
	if !foundHTTPSProxy || !foundHindsightKey {
		t.Errorf("expected secret env vars AGENT_RUN_HTTPS_PROXY and AGENT_RUN_HINDSIGHT_API_KEY, got %+v", rendered.EnvVars)
	}
}

func TestRenderWorkerConfigValidationErrors(t *testing.T) {
	ptrInt32 := func(i int32) *int32 { return &i }

	agent := &kubeopenv1alpha1.Agent{
		Spec: kubeopenv1alpha1.AgentSpec{
			Worker: &kubeopenv1alpha1.WorkerConfig{
				MaxParallel:       ptrInt32(100), // Out of bounds (1-64)
				GCMaxAgeDays:      ptrInt32(0),   // Must be >= 1
				StallMinutes:      ptrInt32(-1),  // Must be >= 1
				LaunchTimeoutSecs: ptrInt32(0),   // Must be >= 1
				ShutdownGraceSecs: ptrInt32(0),   // Must be >= 1
				RemoteControl: &kubeopenv1alpha1.WorkerRemoteControlConfig{
					CheckSecs: ptrInt32(5), // Minimum 10
				},
				A2A: &kubeopenv1alpha1.WorkerA2AConfig{
					InboxTTLDurationSecs: ptrInt32(3), // Minimum 10
				},
				Sandbox: &kubeopenv1alpha1.WorkerSandboxConfig{
					ROPaths: []string{"relative/path", "/var/../etc"},
				},
			},
		},
	}

	rendered := RenderWorkerConfig(agent, nil, nil)
	if len(rendered.Errors) != 9 {
		t.Fatalf("expected 9 validation errors, got %d: %+v", len(rendered.Errors), rendered.Errors)
	}

	errorKeys := make(map[string]bool)
	for _, err := range rendered.Errors {
		errorKeys[err.Key] = true
	}

	for _, expectedKey := range []string{
		"worker.max_parallel",
		"worker.gc_max_age_days",
		"worker.stall_minutes",
		"worker.launch_timeout_secs",
		"worker.shutdown_grace_secs",
		"worker.remote_control_check_secs",
		"worker.a2a_inbox_ttl_secs",
		"common.sandbox_ro_paths",
	} {
		if !errorKeys[expectedKey] {
			t.Errorf("expected validation error for key %q, got errors: %+v", expectedKey, rendered.Errors)
		}
	}
}
