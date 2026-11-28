// Copyright Contributors to the KubeOpenCode project

package v1alpha1

import (
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// AgentSessionPhase defines the lifecycle phase of an AgentSession.
// +kubebuilder:validation:Enum=Pending;Running;Stopped;Failed
type AgentSessionPhase string

const (
	// AgentSessionPhasePending indicates the session is scheduled or preparing to launch.
	AgentSessionPhasePending AgentSessionPhase = "Pending"
	// AgentSessionPhaseRunning indicates the session supervisor process is currently active.
	AgentSessionPhaseRunning AgentSessionPhase = "Running"
	// AgentSessionPhaseStopped indicates the session was manually stopped or paused.
	AgentSessionPhaseStopped AgentSessionPhase = "Stopped"
	// AgentSessionPhaseFailed indicates the session crashed repeatedly or failed health checks.
	AgentSessionPhaseFailed AgentSessionPhase = "Failed"
)

// AgentSessionSpec defines the desired state of an AgentSession.
type AgentSessionSpec struct {
	// AgentRef specifies the target Agent running the session supervisor.
	// Required.
	AgentRef LocalAgentReference `json:"agentRef"`

	// Engine specifies the execution engine (claude, agy, opencode).
	// If omitted, defaults to the Agent's default engine.
	// +optional
	// +kubebuilder:validation:Enum=opencode;agy;claude
	Engine *AgentEngine `json:"engine,omitempty"`

	// InstructionsFile specifies the absolute path of this session's instruction file.
	// +optional
	InstructionsFile string `json:"instructionsFile,omitempty"`

	// Sandbox indicates whether the session runs under sandbox containment. Default true.
	// +optional
	Sandbox *bool `json:"sandbox,omitempty"`

	// ReadinessPollSecs specifies auth-readiness check interval in seconds.
	// +optional
	ReadinessPollSecs *int32 `json:"readinessPollSecs,omitempty"`

	// HangTimeoutSecs specifies silent-PTY seconds before kill+restart (0 = disabled).
	// +optional
	HangTimeoutSecs *int32 `json:"hangTimeoutSecs,omitempty"`

	// KillGraceSecs specifies SIGTERM to SIGKILL grace period in seconds.
	// +optional
	KillGraceSecs *int32 `json:"killGraceSecs,omitempty"`

	// MaxCrashes specifies crashes within crash_window_secs before marking session failed.
	// +optional
	MaxCrashes *int32 `json:"maxCrashes,omitempty"`

	// CrashWindowSecs specifies the crash-threshold window in seconds.
	// +optional
	CrashWindowSecs *int32 `json:"crashWindowSecs,omitempty"`

	// FailedCooldownSecs specifies how long a failed session stays failed before retrying.
	// +optional
	FailedCooldownSecs *int32 `json:"failedCooldownSecs,omitempty"`

	// Suspended pauses or stops the session.
	// +optional
	Suspended bool `json:"suspended,omitempty"`
}

// LocalAgentReference contains enough information to let you locate the referenced Agent in the same namespace.
type LocalAgentReference struct {
	// Name of the referent.
	Name string `json:"name"`
}

// AgentSessionStatus defines the observed state of an AgentSession.
type AgentSessionStatus struct {
	// Phase is the high-level lifecycle status of the session.
	// +optional
	Phase AgentSessionPhase `json:"phase,omitempty"`

	// PID is the operating system process ID on the agent container.
	// +optional
	PID *int `json:"pid,omitempty"`

	// StartTime is RFC3339 timestamp when the session started.
	// +optional
	StartTime *metav1.Time `json:"startTime,omitempty"`

	// UptimeSecs is the elapsed seconds the session has been continuously running.
	// +optional
	UptimeSecs int64 `json:"uptimeSecs,omitempty"`

	// RestartCount is how many times the process has been restarted by the supervisor.
	// +optional
	RestartCount int `json:"restartCount,omitempty"`

	// LastExitReason describes the exit code or signal from the last termination.
	// +optional
	LastExitReason string `json:"lastExitReason,omitempty"`

	// LastHealthResult describes the outcome of the latest readiness/health check.
	// +optional
	LastHealthResult string `json:"lastHealthResult,omitempty"`

	// Paused indicates if the session is currently paused.
	// +optional
	Paused bool `json:"paused,omitempty"`

	// Controlled indicates whether remote control daemon is actively attached.
	// +optional
	Controlled bool `json:"controlled,omitempty"`

	// Engine reported by the worker daemon.
	// +optional
	Engine string `json:"engine,omitempty"`

	// EngineReady is reported for server-backed engines requiring warm-up.
	// +optional
	EngineReady *bool `json:"engineReady,omitempty"`

	// Conditions represent the latest available observations of the session's state.
	// +optional
	Conditions []metav1.Condition `json:"conditions,omitempty"`
}

// +genclient
// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object
// +kubebuilder:object:root=true
// +kubebuilder:subresource:status
// +kubebuilder:resource:scope="Namespaced",shortName=ags
// +kubebuilder:printcolumn:JSONPath=`.spec.agentRef.name`,name="Agent",type=string
// +kubebuilder:printcolumn:JSONPath=`.status.phase`,name="Status",type=string
// +kubebuilder:printcolumn:JSONPath=`.status.engine`,name="Engine",type=string
// +kubebuilder:printcolumn:JSONPath=`.status.pid`,name="PID",type=integer,priority=1
// +kubebuilder:printcolumn:JSONPath=`.status.restartCount`,name="Restarts",type=integer,priority=1
// +kubebuilder:printcolumn:JSONPath=`.metadata.creationTimestamp`,name="Age",type=date

// AgentSession manages a long-running, supervised interactive agent session on an Agent pod.
type AgentSession struct {
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata,omitempty"`

	Spec   AgentSessionSpec   `json:"spec,omitempty"`
	Status AgentSessionStatus `json:"status,omitempty"`
}

// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object
// +kubebuilder:object:root=true

// AgentSessionList contains a list of AgentSessions.
type AgentSessionList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitempty"`
	Items           []AgentSession `json:"items"`
}
