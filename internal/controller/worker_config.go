// Copyright Contributors to the KubeOpenCode project

package controller

import (
	"bytes"
	"fmt"
	"sort"
	"strconv"
	"strings"

	corev1 "k8s.io/api/core/v1"

	kubeopenv1alpha1 "github.com/kubeopencode/kubeopencode/api/v1alpha1"
)

// WorkerConfigError represents a typed validation error for a specific worker configuration key.
type WorkerConfigError struct {
	Key     string
	Message string
}

func (e WorkerConfigError) Error() string {
	return fmt.Sprintf("invalid worker config for %s: %s", e.Key, e.Message)
}

// RenderedWorkerConfig contains the generated TOML bytes and environment variables.
type RenderedWorkerConfig struct {
	ConfigTOML []byte
	EnvVars    []corev1.EnvVar
	Errors     []WorkerConfigError
}

// RenderWorkerConfig takes an Agent, optional AgentTemplate, and optional KubeOpenCodeConfig,
// merges them (Agent > Template > Config defaults), validates the combined settings,
// and produces the config.toml file content and Secret-derived environment variables.
func RenderWorkerConfig(
	agent *kubeopenv1alpha1.Agent,
	tmpl *kubeopenv1alpha1.AgentTemplate,
	clusterCfg *kubeopenv1alpha1.KubeOpenCodeConfig,
) RenderedWorkerConfig {
	var errors []WorkerConfigError

	var agentWorker *kubeopenv1alpha1.WorkerConfig
	var tmplWorker *kubeopenv1alpha1.WorkerConfig
	var clusterWorker *kubeopenv1alpha1.WorkerConfig

	if agent != nil {
		agentWorker = agent.Spec.Worker
	}
	if tmpl != nil {
		tmplWorker = tmpl.Spec.Worker
	}
	if clusterCfg != nil {
		clusterWorker = clusterCfg.Spec.WorkerDefaults
	}

	merged := MergeWorkerConfigs(agentWorker, tmplWorker, clusterWorker)

	// Ensure engine setting aligns
	var engine *kubeopenv1alpha1.AgentEngine
	if agent != nil && agent.Spec.Engine != nil {
		engine = agent.Spec.Engine
	} else if tmpl != nil && tmpl.Spec.Engine != nil {
		engine = tmpl.Spec.Engine
	} else if merged.DefaultEngine != nil {
		engine = merged.DefaultEngine
	}
	if engine != nil {
		merged.DefaultEngine = engine
	}

	// Validate bounds and constraints
	if merged.MaxParallel != nil {
		if *merged.MaxParallel < 1 || *merged.MaxParallel > 64 {
			errors = append(errors, WorkerConfigError{
				Key:     "worker.max_parallel",
				Message: fmt.Sprintf("value %d out of bounds (1-64)", *merged.MaxParallel),
			})
		}
	}
	if merged.GCMaxAgeDays != nil && *merged.GCMaxAgeDays < 1 {
		errors = append(errors, WorkerConfigError{
			Key:     "worker.gc_max_age_days",
			Message: "must be at least 1",
		})
	}
	if merged.StallMinutes != nil && *merged.StallMinutes < 1 {
		errors = append(errors, WorkerConfigError{
			Key:     "worker.stall_minutes",
			Message: "must be at least 1",
		})
	}
	if merged.LaunchTimeoutSecs != nil && *merged.LaunchTimeoutSecs < 1 {
		errors = append(errors, WorkerConfigError{
			Key:     "worker.launch_timeout_secs",
			Message: "must be at least 1",
		})
	}
	if merged.ShutdownGraceSecs != nil && *merged.ShutdownGraceSecs < 1 {
		errors = append(errors, WorkerConfigError{
			Key:     "worker.shutdown_grace_secs",
			Message: "must be at least 1",
		})
	}
	if merged.RemoteControl != nil && merged.RemoteControl.CheckSecs != nil {
		if *merged.RemoteControl.CheckSecs < 10 {
			errors = append(errors, WorkerConfigError{
				Key:     "worker.remote_control_check_secs",
				Message: "must be at least 10",
			})
		}
	}
	if merged.A2A != nil {
		if merged.A2A.InboxTTLDurationSecs != nil && *merged.A2A.InboxTTLDurationSecs < 10 {
			errors = append(errors, WorkerConfigError{
				Key:     "worker.a2a_inbox_ttl_secs",
				Message: "must be at least 10",
			})
		}
	}
	if merged.Sandbox != nil {
		for _, p := range merged.Sandbox.ROPaths {
			if !strings.HasPrefix(p, "/") || strings.Contains(p, "..") {
				errors = append(errors, WorkerConfigError{
					Key:     "common.sandbox_ro_paths",
					Message: fmt.Sprintf("path %q must be absolute and contain no '..'", p),
				})
			}
		}
	}

	var envVars []corev1.EnvVar
	commonKeys := make(map[string]string)
	workerKeys := make(map[string]string)

	// Proxy
	if merged.Proxy != nil {
		handleValueOrSecret := func(vos *kubeopenv1alpha1.ValueOrSecretRef, tomlKey, envName string) {
			if vos == nil {
				return
			}
			if vos.Value != nil && *vos.Value != "" {
				commonKeys[tomlKey] = quoteTOML(*vos.Value)
			} else if vos.SecretKeyRef != nil {
				envVars = append(envVars, corev1.EnvVar{
					Name: envName,
					ValueFrom: &corev1.EnvVarSource{
						SecretKeyRef: vos.SecretKeyRef,
					},
				})
			}
		}
		handleValueOrSecret(merged.Proxy.HTTP, "http_proxy", "AGENT_RUN_HTTP_PROXY")
		handleValueOrSecret(merged.Proxy.HTTPS, "https_proxy", "AGENT_RUN_HTTPS_PROXY")
		handleValueOrSecret(merged.Proxy.NoProxy, "no_proxy", "AGENT_RUN_NO_PROXY")
	}

	// Sandbox
	if merged.Sandbox != nil {
		if len(merged.Sandbox.ROPaths) > 0 {
			var quoted []string
			for _, p := range merged.Sandbox.ROPaths {
				quoted = append(quoted, quoteTOML(p))
			}
			commonKeys["sandbox_ro_paths"] = "[" + strings.Join(quoted, ", ") + "]"
		}
		if len(merged.Sandbox.EnvAllow) > 0 {
			var quoted []string
			for _, ea := range merged.Sandbox.EnvAllow {
				quoted = append(quoted, quoteTOML(ea))
			}
			commonKeys["sandbox_env_allow"] = "[" + strings.Join(quoted, ", ") + "]"
		}
	}

	// Worker fields
	if merged.MaxParallel != nil {
		workerKeys["max_parallel"] = strconv.FormatInt(int64(*merged.MaxParallel), 10)
	}
	if merged.GCMaxAgeDays != nil {
		workerKeys["gc_max_age_days"] = strconv.FormatInt(int64(*merged.GCMaxAgeDays), 10)
	}
	if merged.StallMinutes != nil {
		workerKeys["stall_minutes"] = strconv.FormatInt(int64(*merged.StallMinutes), 10)
	}
	if merged.LaunchTimeoutSecs != nil {
		workerKeys["launch_timeout_secs"] = strconv.FormatInt(int64(*merged.LaunchTimeoutSecs), 10)
	}
	if merged.ShutdownGraceSecs != nil {
		workerKeys["shutdown_grace_secs"] = strconv.FormatInt(int64(*merged.ShutdownGraceSecs), 10)
	}
	if merged.DefaultModel != "" {
		workerKeys["default_model"] = quoteTOML(merged.DefaultModel)
	}
	if merged.DefaultEffort != "" {
		workerKeys["default_effort"] = quoteTOML(merged.DefaultEffort)
	}
	if merged.DefaultBaseBranch != "" {
		workerKeys["default_base_branch"] = quoteTOML(merged.DefaultBaseBranch)
	}
	if merged.DefaultEngine != nil && *merged.DefaultEngine != "" {
		workerKeys["default_engine"] = quoteTOML(string(*merged.DefaultEngine))
	}
	if merged.MaxRuntime != "" {
		workerKeys["max_runtime"] = quoteTOML(merged.MaxRuntime)
	}
	if merged.JobsDirName != "" {
		workerKeys["jobs_dir_name"] = quoteTOML(merged.JobsDirName)
	}
	if merged.ReposRoot != "" {
		workerKeys["repos_root"] = quoteTOML(merged.ReposRoot)
	}

	// Hindsight
	if merged.Hindsight != nil {
		if merged.Hindsight.Enabled != nil {
			workerKeys["hindsight_enabled"] = strconv.FormatBool(*merged.Hindsight.Enabled)
		}
		if merged.Hindsight.Bank != "" {
			workerKeys["hindsight_bank"] = quoteTOML(merged.Hindsight.Bank)
		}
		if merged.Hindsight.Tag != "" {
			workerKeys["hindsight_tag"] = quoteTOML(merged.Hindsight.Tag)
		}
		if merged.Hindsight.URL != "" {
			workerKeys["hindsight_url"] = quoteTOML(merged.Hindsight.URL)
		}
		if merged.Hindsight.Profile != "" {
			workerKeys["hindsight_profile"] = quoteTOML(merged.Hindsight.Profile)
		}
		if merged.Hindsight.AutoRecall != nil {
			workerKeys["hindsight_auto_recall"] = strconv.FormatBool(*merged.Hindsight.AutoRecall)
		}
		if merged.Hindsight.AutoRetain != nil {
			workerKeys["hindsight_auto_retain"] = strconv.FormatBool(*merged.Hindsight.AutoRetain)
		}
		if merged.Hindsight.APIKeySecretRef != nil {
			envVars = append(envVars, corev1.EnvVar{
				Name: "AGENT_RUN_HINDSIGHT_API_KEY",
				ValueFrom: &corev1.EnvVarSource{
					SecretKeyRef: merged.Hindsight.APIKeySecretRef,
				},
			})
		}
	}

	// Remote Control
	if merged.RemoteControl != nil {
		if merged.RemoteControl.Enabled != nil {
			workerKeys["remote_control"] = strconv.FormatBool(*merged.RemoteControl.Enabled)
		}
		if merged.RemoteControl.Autostart != nil {
			workerKeys["remote_control_autostart"] = strconv.FormatBool(*merged.RemoteControl.Autostart)
		}
		if merged.RemoteControl.CheckSecs != nil {
			workerKeys["remote_control_check_secs"] = strconv.FormatInt(int64(*merged.RemoteControl.CheckSecs), 10)
		}
	}

	// A2A
	if merged.A2A != nil {
		if merged.A2A.InterruptMinSecs != nil {
			workerKeys["a2a_interrupt_min_secs"] = strconv.FormatInt(int64(*merged.A2A.InterruptMinSecs), 10)
		}
		if merged.A2A.LiveStdin != nil {
			workerKeys["a2a_live_stdin"] = strconv.FormatBool(*merged.A2A.LiveStdin)
		}
		if merged.A2A.LiveIdleSecs != nil {
			workerKeys["a2a_live_idle_secs"] = strconv.FormatInt(int64(*merged.A2A.LiveIdleSecs), 10)
		}
		if merged.A2A.MCPEnabled != nil {
			workerKeys["a2a_mcp_enabled"] = strconv.FormatBool(*merged.A2A.MCPEnabled)
		}
		if merged.A2A.AckSecs != nil {
			workerKeys["a2a_ack_secs"] = strconv.FormatInt(int64(*merged.A2A.AckSecs), 10)
		}
		if merged.A2A.ForceSecs != nil {
			workerKeys["a2a_force_secs"] = strconv.FormatInt(int64(*merged.A2A.ForceSecs), 10)
		}
		if merged.A2A.Echo != nil {
			workerKeys["a2a_echo"] = strconv.FormatBool(*merged.A2A.Echo)
		}
		if merged.A2A.InboxTTLDurationSecs != nil {
			workerKeys["a2a_inbox_ttl_secs"] = strconv.FormatInt(int64(*merged.A2A.InboxTTLDurationSecs), 10)
		}
	}

	// Render TOML with deterministic key order
	var buf bytes.Buffer
	if len(commonKeys) > 0 {
		buf.WriteString("[common]\n")
		var sortedKeys []string
		for k := range commonKeys {
			sortedKeys = append(sortedKeys, k)
		}
		sort.Strings(sortedKeys)
		for _, k := range sortedKeys {
			buf.WriteString(fmt.Sprintf("%s = %s\n", k, commonKeys[k]))
		}
		buf.WriteString("\n")
	}

	if len(workerKeys) > 0 {
		buf.WriteString("[worker]\n")
		var sortedKeys []string
		for k := range workerKeys {
			sortedKeys = append(sortedKeys, k)
		}
		sort.Strings(sortedKeys)
		for _, k := range sortedKeys {
			buf.WriteString(fmt.Sprintf("%s = %s\n", k, workerKeys[k]))
		}
	}

	return RenderedWorkerConfig{
		ConfigTOML: buf.Bytes(),
		EnvVars:    envVars,
		Errors:     errors,
	}
}

func quoteTOML(s string) string {
	return strconv.Quote(s)
}

// MergeWorkerConfigs merges Agent, Template, and Cluster WorkerConfig objects.
// Agent takes precedence over Template, which takes precedence over Cluster.
// Scalar fields: first non-nil / non-zero wins.
// List fields: replace (Agent replaces Template; Template replaces Cluster).
func MergeWorkerConfigs(
	agent *kubeopenv1alpha1.WorkerConfig,
	tmpl *kubeopenv1alpha1.WorkerConfig,
	cluster *kubeopenv1alpha1.WorkerConfig,
) kubeopenv1alpha1.WorkerConfig {
	var merged kubeopenv1alpha1.WorkerConfig

	all := []*kubeopenv1alpha1.WorkerConfig{cluster, tmpl, agent}
	for _, c := range all {
		if c == nil {
			continue
		}
		if c.MaxParallel != nil {
			merged.MaxParallel = c.MaxParallel
		}
		if c.GCMaxAgeDays != nil {
			merged.GCMaxAgeDays = c.GCMaxAgeDays
		}
		if c.StallMinutes != nil {
			merged.StallMinutes = c.StallMinutes
		}
		if c.LaunchTimeoutSecs != nil {
			merged.LaunchTimeoutSecs = c.LaunchTimeoutSecs
		}
		if c.ShutdownGraceSecs != nil {
			merged.ShutdownGraceSecs = c.ShutdownGraceSecs
		}
		if c.DefaultModel != "" {
			merged.DefaultModel = c.DefaultModel
		}
		if c.DefaultEffort != "" {
			merged.DefaultEffort = c.DefaultEffort
		}
		if c.DefaultBaseBranch != "" {
			merged.DefaultBaseBranch = c.DefaultBaseBranch
		}
		if c.DefaultEngine != nil {
			merged.DefaultEngine = c.DefaultEngine
		}
		if c.MaxRuntime != "" {
			merged.MaxRuntime = c.MaxRuntime
		}
		if c.JobsDirName != "" {
			merged.JobsDirName = c.JobsDirName
		}
		if c.ReposRoot != "" {
			merged.ReposRoot = c.ReposRoot
		}

		if c.Proxy != nil {
			if merged.Proxy == nil {
				merged.Proxy = &kubeopenv1alpha1.WorkerProxyConfig{}
			}
			if c.Proxy.HTTP != nil {
				merged.Proxy.HTTP = c.Proxy.HTTP
			}
			if c.Proxy.HTTPS != nil {
				merged.Proxy.HTTPS = c.Proxy.HTTPS
			}
			if c.Proxy.NoProxy != nil {
				merged.Proxy.NoProxy = c.Proxy.NoProxy
			}
		}

		if c.Hindsight != nil {
			if merged.Hindsight == nil {
				merged.Hindsight = &kubeopenv1alpha1.WorkerHindsightConfig{}
			}
			if c.Hindsight.Enabled != nil {
				merged.Hindsight.Enabled = c.Hindsight.Enabled
			}
			if c.Hindsight.Bank != "" {
				merged.Hindsight.Bank = c.Hindsight.Bank
			}
			if c.Hindsight.Tag != "" {
				merged.Hindsight.Tag = c.Hindsight.Tag
			}
			if c.Hindsight.URL != "" {
				merged.Hindsight.URL = c.Hindsight.URL
			}
			if c.Hindsight.Profile != "" {
				merged.Hindsight.Profile = c.Hindsight.Profile
			}
			if c.Hindsight.AutoRecall != nil {
				merged.Hindsight.AutoRecall = c.Hindsight.AutoRecall
			}
			if c.Hindsight.AutoRetain != nil {
				merged.Hindsight.AutoRetain = c.Hindsight.AutoRetain
			}
			if c.Hindsight.APIKeySecretRef != nil {
				merged.Hindsight.APIKeySecretRef = c.Hindsight.APIKeySecretRef
			}
		}

		if c.RemoteControl != nil {
			if merged.RemoteControl == nil {
				merged.RemoteControl = &kubeopenv1alpha1.WorkerRemoteControlConfig{}
			}
			if c.RemoteControl.Enabled != nil {
				merged.RemoteControl.Enabled = c.RemoteControl.Enabled
			}
			if c.RemoteControl.Autostart != nil {
				merged.RemoteControl.Autostart = c.RemoteControl.Autostart
			}
			if c.RemoteControl.CheckSecs != nil {
				merged.RemoteControl.CheckSecs = c.RemoteControl.CheckSecs
			}
		}

		if c.A2A != nil {
			if merged.A2A == nil {
				merged.A2A = &kubeopenv1alpha1.WorkerA2AConfig{}
			}
			if c.A2A.InterruptMinSecs != nil {
				merged.A2A.InterruptMinSecs = c.A2A.InterruptMinSecs
			}
			if c.A2A.LiveStdin != nil {
				merged.A2A.LiveStdin = c.A2A.LiveStdin
			}
			if c.A2A.LiveIdleSecs != nil {
				merged.A2A.LiveIdleSecs = c.A2A.LiveIdleSecs
			}
			if c.A2A.MCPEnabled != nil {
				merged.A2A.MCPEnabled = c.A2A.MCPEnabled
			}
			if c.A2A.AckSecs != nil {
				merged.A2A.AckSecs = c.A2A.AckSecs
			}
			if c.A2A.ForceSecs != nil {
				merged.A2A.ForceSecs = c.A2A.ForceSecs
			}
			if c.A2A.Echo != nil {
				merged.A2A.Echo = c.A2A.Echo
			}
			if c.A2A.InboxTTLDurationSecs != nil {
				merged.A2A.InboxTTLDurationSecs = c.A2A.InboxTTLDurationSecs
			}
		}

		if c.Sandbox != nil {
			if merged.Sandbox == nil {
				merged.Sandbox = &kubeopenv1alpha1.WorkerSandboxConfig{}
			}
			if c.Sandbox.ROPaths != nil {
				merged.Sandbox.ROPaths = c.Sandbox.ROPaths
			}
			if c.Sandbox.EnvAllow != nil {
				merged.Sandbox.EnvAllow = c.Sandbox.EnvAllow
			}
		}
	}

	return merged
}
