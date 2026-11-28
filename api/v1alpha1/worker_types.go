// Copyright Contributors to the KubeOpenCode project

package v1alpha1

import (
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
)

// AgentEngine represents the execution engine type.
// +kubebuilder:validation:Enum=opencode;agy;claude
type AgentEngine string

const (
	// AgentEngineOpenCode represents the OpenCode engine.
	AgentEngineOpenCode AgentEngine = "opencode"
	// AgentEngineAgy represents the agy engine.
	AgentEngineAgy AgentEngine = "agy"
	// AgentEngineClaude represents the Claude Code engine.
	AgentEngineClaude AgentEngine = "claude"
)

// ValueOrSecretRef represents a string configuration value that can either be
// specified directly inline or retrieved from a Secret.
// Exactly one of value or secretKeyRef must be specified.
//
// +kubebuilder:validation:XValidation:rule="has(self.value) != has(self.secretKeyRef)",message="exactly one of value or secretKeyRef must be specified"
type ValueOrSecretRef struct {
	// Value specified directly.
	// +optional
	Value *string `json:"value,omitempty"`

	// SecretKeyRef selects a key of a Secret in the resource's namespace.
	// +optional
	SecretKeyRef *corev1.SecretKeySelector `json:"secretKeyRef,omitempty"`
}

// WorkerProxyConfig defines HTTP/HTTPS/NO_PROXY settings for the worker.
// Secrets must be provided by reference only.
type WorkerProxyConfig struct {
	// HTTP proxy URL.
	// Maps to [common] http_proxy / AGENT_RUN_HTTP_PROXY.
	// +optional
	HTTP *ValueOrSecretRef `json:"http,omitempty"`

	// HTTPS proxy URL.
	// Maps to [common] https_proxy / AGENT_RUN_HTTPS_PROXY.
	// +optional
	HTTPS *ValueOrSecretRef `json:"https,omitempty"`

	// NoProxy specifies comma-separated hosts to bypass proxy.
	// Maps to [common] no_proxy / AGENT_RUN_NO_PROXY.
	// +optional
	NoProxy *ValueOrSecretRef `json:"noProxy,omitempty"`
}

// WorkerHindsightConfig defines Hindsight memory settings for the worker.
// Secrets (like API key) must only be referenced via SecretKeyRef.
type WorkerHindsightConfig struct {
	// Enabled enables or disables Hindsight memory.
	// Maps to [worker] hindsight_enabled / AGENT_RUN_HINDSIGHT_ENABLED.
	// +optional
	Enabled *bool `json:"enabled,omitempty"`

	// Bank specifies the Hindsight memory bank id.
	// Maps to [worker] hindsight_bank / AGENT_RUN_HINDSIGHT_BANK.
	// +optional
	Bank string `json:"bank,omitempty"`

	// Tag specifies the project tag.
	// Maps to [worker] hindsight_tag / AGENT_RUN_HINDSIGHT_TAG.
	// +optional
	Tag string `json:"tag,omitempty"`

	// URL specifies the remote Hindsight API base URL.
	// Maps to [worker] hindsight_url / AGENT_RUN_HINDSIGHT_URL.
	// +optional
	URL string `json:"url,omitempty"`

	// Profile specifies the Hindsight profile name.
	// Maps to [worker] hindsight_profile / AGENT_RUN_HINDSIGHT_PROFILE.
	// +optional
	Profile string `json:"profile,omitempty"`

	// AutoRecall specifies whether Hindsight context should automatically be recalled.
	// Maps to [worker] hindsight_auto_recall / AGENT_RUN_HINDSIGHT_AUTO_RECALL.
	// +optional
	AutoRecall *bool `json:"autoRecall,omitempty"`

	// AutoRetain specifies whether Hindsight memory should automatically retain learnings.
	// Maps to [worker] hindsight_auto_retain / AGENT_RUN_HINDSIGHT_AUTO_RETAIN.
	// +optional
	AutoRetain *bool `json:"autoRetain,omitempty"`

	// APIKeySecretRef references a Secret containing the Hindsight API key.
	// Injected as AGENT_RUN_HINDSIGHT_API_KEY environment variable.
	// Inline secrets are rejected.
	// +optional
	APIKeySecretRef *corev1.SecretKeySelector `json:"apiKeySecretRef,omitempty"`
}

// WorkerRemoteControlConfig defines remote control daemon settings for the worker.
type WorkerRemoteControlConfig struct {
	// Enabled enables or disables remote control.
	// Maps to [worker] remote_control / AGENT_RUN_REMOTE_CONTROL.
	// +optional
	Enabled *bool `json:"enabled,omitempty"`

	// Autostart automatically starts the remote control daemon when the worker starts.
	// Maps to [worker] remote_control_autostart / AGENT_RUN_REMOTE_CONTROL_AUTOSTART.
	// +optional
	Autostart *bool `json:"autostart,omitempty"`

	// CheckSecs specifies check interval in seconds (minimum 10).
	// Maps to [worker] remote_control_check_secs / AGENT_RUN_REMOTE_CONTROL_CHECK_SECS.
	// +optional
	// +kubebuilder:validation:Minimum=10
	CheckSecs *int32 `json:"checkSecs,omitempty"`
}

// WorkerA2AConfig defines Agent-to-Agent protocol and messaging configuration for the worker.
type WorkerA2AConfig struct {
	// Enabled enables or disables A2A messaging.
	// Maps to [worker] a2a_mcp_enabled / AGENT_RUN_A2A_MCP_ENABLED (when false, disables tools).
	// +optional
	Enabled *bool `json:"enabled,omitempty"`

	// InterruptMinSecs specifies minimum seconds between interrupt steps on one job.
	// Maps to [worker] a2a_interrupt_min_secs / AGENT_RUN_A2A_INTERRUPT_MIN_SECS.
	// +optional
	// +kubebuilder:validation:Minimum=0
	InterruptMinSecs *int32 `json:"interruptMinSecs,omitempty"`

	// LiveStdin specifies whether live stdin transport is enabled for idle turn channel.
	// Maps to [worker] a2a_live_stdin / AGENT_RUN_A2A_LIVE_STDIN.
	// +optional
	LiveStdin *bool `json:"liveStdin,omitempty"`

	// LiveIdleSecs specifies idle seconds before live channel close (0 or >= 10).
	// Maps to [worker] a2a_live_idle_secs / AGENT_RUN_A2A_LIVE_IDLE_SECS.
	// +optional
	// +kubebuilder:validation:Minimum=0
	LiveIdleSecs *int32 `json:"liveIdleSecs,omitempty"`

	// MCPEnabled specifies whether agent A2A MCP tools are enabled.
	// Maps to [worker] a2a_mcp_enabled / AGENT_RUN_A2A_MCP_ENABLED.
	// +optional
	MCPEnabled *bool `json:"mcpEnabled,omitempty"`

	// AckSecs specifies escalation ladder ack window in seconds.
	// Maps to [worker] a2a_ack_secs / AGENT_RUN_A2A_ACK_SECS.
	// +optional
	// +kubebuilder:validation:Minimum=0
	AckSecs *int32 `json:"ackSecs,omitempty"`

	// ForceSecs specifies escalation ladder force timeout (0 = never kill).
	// Maps to [worker] a2a_force_secs / AGENT_RUN_A2A_FORCE_SECS.
	// +optional
	// +kubebuilder:validation:Minimum=0
	ForceSecs *int32 `json:"forceSecs,omitempty"`

	// Echo specifies user-visible A2A echo in transcripts.
	// Maps to [worker] a2a_echo / AGENT_RUN_A2A_ECHO.
	// +optional
	Echo *bool `json:"echo,omitempty"`

	// InboxTTLDurationSecs specifies inbox message TTL in seconds (min 10).
	// Maps to [worker] a2a_inbox_ttl_secs / AGENT_RUN_A2A_INBOX_TTL_SECS.
	// +optional
	// +kubebuilder:validation:Minimum=10
	InboxTTLDurationSecs *int32 `json:"inboxTtlSecs,omitempty"`
}

// WorkerSandboxConfig defines sandbox read-only paths and environment allowlist.
type WorkerSandboxConfig struct {
	// ROPaths specifies absolute paths appended to the read-only list of every sandbox policy.
	// Must be absolute paths without '..' directory traversal.
	// Maps to [common] sandbox_ro_paths / KUBEADP_SANDBOX_RO_PATHS.
	//
	// +optional
	// +listType=atomic
	// +kubebuilder:validation:MaxItems=100
	// +kubebuilder:validation:items:MaxLength=4096
	// +kubebuilder:validation:XValidation:rule="self.all(p, p.startsWith('/') && !p.contains('..'))",message="roPaths entries must be absolute paths without .."
	ROPaths []string `json:"roPaths,omitempty"`

	// EnvAllow specifies environment variable names allowed into the sandbox.
	// +optional
	// +listType=atomic
	EnvAllow []string `json:"envAllow,omitempty"`
}

// WorkerConfig defines typed configuration for the kubeadp-agent worker daemon,
// mirroring SPEC 9.2 [common] and [worker] configuration keys.
// All fields are optional. Secrets are never stored inline.
type WorkerConfig struct {
	// MaxParallel limits concurrent jobs on this worker.
	// Maps to [worker] max_parallel / AGENT_RUN_MAX_PARALLEL.
	// +optional
	// +kubebuilder:validation:Minimum=1
	// +kubebuilder:validation:Maximum=64
	MaxParallel *int32 `json:"maxParallel,omitempty"`

	// GCMaxAgeDays specifies maximum age in days before completed jobs are garbage collected.
	// Maps to [worker] gc_max_age_days / AGENT_RUN_GC_MAX_AGE_DAYS.
	// +optional
	// +kubebuilder:validation:Minimum=1
	GCMaxAgeDays *int32 `json:"gcMaxAgeDays,omitempty"`

	// StallMinutes specifies inactivity timeout in minutes before a job is flagged stalled.
	// Maps to [worker] stall_minutes / AGENT_RUN_STALL_MINUTES.
	// +optional
	// +kubebuilder:validation:Minimum=1
	StallMinutes *int32 `json:"stallMinutes,omitempty"`

	// LaunchTimeoutSecs specifies timeout in seconds for agent process launch.
	// Maps to [worker] launch_timeout_secs / AGENT_RUN_LAUNCH_TIMEOUT_SECS.
	// +optional
	// +kubebuilder:validation:Minimum=1
	LaunchTimeoutSecs *int32 `json:"launchTimeoutSecs,omitempty"`

	// ShutdownGraceSecs specifies graceful shutdown period in seconds for job processes.
	// Maps to [worker] shutdown_grace_secs / AGENT_RUN_SHUTDOWN_GRACE.
	// +optional
	// +kubebuilder:validation:Minimum=1
	ShutdownGraceSecs *int32 `json:"shutdownGraceSecs,omitempty"`

	// DefaultModel specifies default model name passed to jobs.
	// Maps to [worker] default_model / AGENT_RUN_DEFAULT_MODEL.
	// +optional
	DefaultModel string `json:"defaultModel,omitempty"`

	// DefaultEffort specifies default reasoning effort.
	// Maps to [worker] default_effort / AGENT_RUN_DEFAULT_EFFORT.
	// +optional
	DefaultEffort string `json:"defaultEffort,omitempty"`

	// DefaultBaseBranch specifies default base branch for git worktrees.
	// Maps to [worker] default_base_branch / AGENT_RUN_DEFAULT_BASE_BRANCH.
	// +optional
	DefaultBaseBranch string `json:"defaultBaseBranch,omitempty"`

	// DefaultEngine specifies default engine.
	// Maps to [worker] default_engine / AGENT_RUN_DEFAULT_ENGINE.
	// +optional
	// +kubebuilder:validation:Enum=opencode;agy;claude
	DefaultEngine *AgentEngine `json:"defaultEngine,omitempty"`

	// MaxRuntime specifies maximum execution duration string for jobs (e.g. "2h", "30m").
	// Maps to [worker] max_runtime / AGENT_RUN_MAX_RUNTIME.
	// +optional
	MaxRuntime string `json:"maxRuntime,omitempty"`

	// JobsDirName specifies directory name for job worktrees under workspace root.
	// Maps to [worker] jobs_dir_name / AGENT_RUN_JOBS_DIR_NAME.
	// +optional
	JobsDirName string `json:"jobsDirName,omitempty"`

	// ReposRoot specifies path to bare repository cache.
	// Maps to [worker] repos_root / AGENT_RUN_REPOS_ROOT.
	// +optional
	ReposRoot string `json:"reposRoot,omitempty"`

	// Proxy defines HTTP/HTTPS/NO_PROXY settings.
	// +optional
	Proxy *WorkerProxyConfig `json:"proxy,omitempty"`

	// Hindsight defines Hindsight memory configuration.
	// +optional
	Hindsight *WorkerHindsightConfig `json:"hindsight,omitempty"`

	// RemoteControl defines remote control daemon settings.
	// +optional
	RemoteControl *WorkerRemoteControlConfig `json:"remoteControl,omitempty"`

	// A2A defines Agent-to-Agent protocol configuration.
	// +optional
	A2A *WorkerA2AConfig `json:"a2a,omitempty"`

	// Sandbox defines sandbox read-only paths and allowed environment variables.
	// +optional
	Sandbox *WorkerSandboxConfig `json:"sandbox,omitempty"`
}

// WorkerCapacityStatus reflects worker job capacity.
type WorkerCapacityStatus struct {
	// MaxParallel is maximum allowed parallel jobs.
	// +optional
	MaxParallel int32 `json:"maxParallel,omitempty"`

	// Running is current count of running jobs.
	// +optional
	Running int32 `json:"running,omitempty"`
}

// WorkerDiskStatus reflects disk usage on worker storage.
type WorkerDiskStatus struct {
	// TotalBytes of storage.
	// +optional
	TotalBytes int64 `json:"totalBytes,omitempty"`

	// FreeBytes of available storage.
	// +optional
	FreeBytes int64 `json:"freeBytes,omitempty"`
}

// WorkerEngineAuthState describes authentication state for a specific engine.
type WorkerEngineAuthState struct {
	// Engine name (e.g., opencode, agy, claude).
	Engine string `json:"engine"`

	// State of engine authentication.
	State string `json:"state"`
}

// WorkerAuthStatus reflects overall worker authentication state.
type WorkerAuthStatus struct {
	// State represents overall auth status summary.
	// +optional
	State string `json:"state,omitempty"`

	// Engines reports auth status per engine.
	// +optional
	Engines []WorkerEngineAuthState `json:"engines,omitempty"`
}

// WorkerCapability describes engine capability matrix.
type WorkerCapability struct {
	// Engine name.
	Engine string `json:"engine"`

	// Transport mechanism.
	// +optional
	Transport string `json:"transport,omitempty"`

	// Endpoints maps endpoint keys to support level: native, emulated, or unsupported.
	// +optional
	Endpoints map[string]string `json:"endpoints,omitempty"`
}

// WorkerQuotaStatus represents quota tracking per group/window.
type WorkerQuotaStatus struct {
	// Group name for quota bucket.
	Group string `json:"group"`

	// Window duration label.
	// +optional
	Window string `json:"window,omitempty"`

	// RemainingPercent of quota remaining (0-100).
	// +optional
	RemainingPercent int32 `json:"remainingPercent,omitempty"`

	// ResetsAt indicates when the quota window resets.
	// +optional
	ResetsAt *metav1.Time `json:"resetsAt,omitempty"`
}

// WorkerProjectStatus represents a project tracked in the worker.
type WorkerProjectStatus struct {
	// Name of the project.
	Name string `json:"name"`

	// Branch currently checked out.
	// +optional
	Branch string `json:"branch,omitempty"`

	// Head commit hash.
	// +optional
	Head string `json:"head,omitempty"`

	// LastSync time.
	// +optional
	LastSync *metav1.Time `json:"lastSync,omitempty"`

	// Jobs count for this project.
	// +optional
	Jobs int32 `json:"jobs,omitempty"`
}

// WorkerRemoteControlStatus represents the status of remote control on the worker.
type WorkerRemoteControlStatus struct {
	// Enabled indicates whether remote control is enabled.
	// +optional
	Enabled bool `json:"enabled,omitempty"`

	// State indicates current remote control daemon state.
	// +optional
	State string `json:"state,omitempty"`
}

// WorkerStatus reflects the live state mirrored from the kubeadp-agent worker daemon.
type WorkerStatus struct {
	// Version of kubeadp-agent worker.
	// +optional
	Version string `json:"version,omitempty"`

	// Ready indicates worker is ready to accept jobs (passing /readyz).
	// +optional
	Ready bool `json:"ready,omitempty"`

	// LastSyncTime is when status was last refreshed from the worker.
	// +optional
	LastSyncTime *metav1.Time `json:"lastSyncTime,omitempty"`

	// Capacity holds running and maximum parallel job counts.
	// +optional
	Capacity *WorkerCapacityStatus `json:"capacity,omitempty"`

	// Disk reports total and free disk bytes on worker storage.
	// +optional
	Disk *WorkerDiskStatus `json:"disk,omitempty"`

	// Toolchains reports installed toolchain versions/paths.
	// +optional
	// +kubebuilder:pruning:PreserveUnknownFields
	Toolchains *runtime.RawExtension `json:"toolchains,omitempty"`

	// Auth reports auth state overall and per engine.
	// +optional
	Auth *WorkerAuthStatus `json:"auth,omitempty"`

	// Capabilities matrix per engine.
	// +optional
	Capabilities []WorkerCapability `json:"capabilities,omitempty"`

	// Models available on the worker, capped at 200 entries.
	// +optional
	// +kubebuilder:validation:MaxItems=200
	Models []string `json:"models,omitempty"`

	// Quota usage across engines and groups.
	// +optional
	Quota []WorkerQuotaStatus `json:"quota,omitempty"`

	// Projects tracked in the worker workspace.
	// +optional
	Projects []WorkerProjectStatus `json:"projects,omitempty"`

	// RemoteControl reports status of remote control daemon.
	// +optional
	RemoteControl *WorkerRemoteControlStatus `json:"remoteControl,omitempty"`
}
