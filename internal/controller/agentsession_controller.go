// Copyright Contributors to the KubeOpenCode project

package controller

import (
	"context"
	"fmt"
	"strings"
	"time"

	corev1 "k8s.io/api/core/v1"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/client-go/tools/record"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/log"

	kubeopenv1alpha1 "github.com/kubeopencode/kubeopencode/api/v1alpha1"
	"github.com/kubeopencode/kubeopencode/internal/kubeadp"
)

// AgentSessionReconciler reconciles AgentSession resources.
type AgentSessionReconciler struct {
	client.Client
	Scheme   *runtime.Scheme
	Recorder record.EventRecorder
}

// +kubebuilder:rbac:groups=kubeopencode.io,resources=agentsessions,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=kubeopencode.io,resources=agentsessions/status,verbs=get;update;patch
// +kubebuilder:rbac:groups=kubeopencode.io,resources=agentsessions/finalizers,verbs=update

// Reconcile handles AgentSession reconciliation.
func (r *AgentSessionReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	logger := log.FromContext(ctx)

	var session kubeopenv1alpha1.AgentSession
	if err := r.Get(ctx, req.NamespacedName, &session); err != nil {
		if apierrors.IsNotFound(err) {
			return ctrl.Result{}, nil
		}
		logger.Error(err, "Failed to get AgentSession")
		return ctrl.Result{}, err
	}

	// Fetch referenced Agent
	var agent kubeopenv1alpha1.Agent
	agentKey := client.ObjectKey{
		Namespace: session.Namespace,
		Name:      session.Spec.AgentRef.Name,
	}
	if err := r.Get(ctx, agentKey, &agent); err != nil {
		if apierrors.IsNotFound(err) {
			meta.SetStatusCondition(&session.Status.Conditions, metav1.Condition{
				Type:    "AgentReady",
				Status:  metav1.ConditionFalse,
				Reason:  "AgentNotFound",
				Message: fmt.Sprintf("Referenced Agent %s not found in namespace %s", session.Spec.AgentRef.Name, session.Namespace),
			})
			session.Status.Phase = kubeopenv1alpha1.AgentSessionPhasePending
			_ = r.Status().Update(ctx, &session)
			return ctrl.Result{RequeueAfter: 10 * time.Second}, nil
		}
		return ctrl.Result{}, err
	}

	// Build worker client
	workerClient, err := buildWorkerClientForAgent(ctx, r.Client, &agent)
	if err != nil {
		logger.V(1).Info("Worker client not ready for AgentSession", "error", err)
		meta.SetStatusCondition(&session.Status.Conditions, metav1.Condition{
			Type:    "WorkerReachable",
			Status:  metav1.ConditionFalse,
			Reason:  "ClientBuildFailed",
			Message: err.Error(),
		})
		session.Status.Phase = kubeopenv1alpha1.AgentSessionPhasePending
		_ = r.Status().Update(ctx, &session)
		return ctrl.Result{RequeueAfter: 10 * time.Second}, nil
	}

	// Query worker for session state
	wSession, err := workerClient.SessionGet(ctx, session.Name)
	if err != nil {
		// If session is suspended, we don't start it
		if session.Spec.Suspended {
			session.Status.Phase = kubeopenv1alpha1.AgentSessionPhaseStopped
			session.Status.Paused = true
			_ = r.Status().Update(ctx, &session)
			return ctrl.Result{RequeueAfter: 30 * time.Second}, nil
		}

		// Try starting the session
		startResp, startErr := workerClient.SessionStart(ctx, session.Name)
		if startErr != nil {
			logger.V(1).Info("Could not start session on worker", "session", session.Name, "error", startErr)
			session.Status.Phase = kubeopenv1alpha1.AgentSessionPhasePending
			meta.SetStatusCondition(&session.Status.Conditions, metav1.Condition{
				Type:    "SessionStarted",
				Status:  metav1.ConditionFalse,
				Reason:  "StartFailed",
				Message: startErr.Error(),
			})
			_ = r.Status().Update(ctx, &session)
			return ctrl.Result{RequeueAfter: 5 * time.Second}, nil
		}
		wSession = startResp
	}

	// Handle desired suspension
	if session.Spec.Suspended && !wSession.Paused && wSession.Status != "stopped" {
		stopResp, stopErr := workerClient.SessionStop(ctx, session.Name)
		if stopErr == nil && stopResp != nil {
			wSession = stopResp
		}
	} else if !session.Spec.Suspended && (wSession.Paused || wSession.Status == "stopped") {
		startResp, startErr := workerClient.SessionStart(ctx, session.Name)
		if startErr == nil && startResp != nil {
			wSession = startResp
		}
	}

	// Mirror status
	r.mirrorSessionStatus(&session, wSession)
	if err := r.Status().Update(ctx, &session); err != nil {
		logger.Error(err, "Failed to update AgentSession status")
		return ctrl.Result{}, err
	}

	pollInterval := 10 * time.Second
	if session.Status.Phase == kubeopenv1alpha1.AgentSessionPhaseRunning {
		pollInterval = 5 * time.Second
	}
	return ctrl.Result{RequeueAfter: pollInterval}, nil
}

func (r *AgentSessionReconciler) mirrorSessionStatus(session *kubeopenv1alpha1.AgentSession, wSession *kubeadp.Session) {
	if wSession == nil {
		return
	}
	session.Status.PID = wSession.PID
	session.Status.UptimeSecs = wSession.UptimeSecs
	session.Status.RestartCount = wSession.RestartCount
	session.Status.LastExitReason = wSession.LastExitReason
	session.Status.LastHealthResult = wSession.LastHealthResult
	session.Status.Paused = wSession.Paused
	session.Status.Controlled = wSession.Controlled
	if wSession.Engine != nil {
		session.Status.Engine = *wSession.Engine
	}
	session.Status.EngineReady = wSession.EngineReady

	if wSession.StartTime != "" {
		if t, err := time.Parse(time.RFC3339, wSession.StartTime); err == nil {
			mTime := metav1.NewTime(t)
			session.Status.StartTime = &mTime
		}
	}

	switch wSession.Status {
	case "running":
		session.Status.Phase = kubeopenv1alpha1.AgentSessionPhaseRunning
	case "stopped":
		session.Status.Phase = kubeopenv1alpha1.AgentSessionPhaseStopped
	case "failed":
		session.Status.Phase = kubeopenv1alpha1.AgentSessionPhaseFailed
	default:
		session.Status.Phase = kubeopenv1alpha1.AgentSessionPhasePending
	}

	meta.SetStatusCondition(&session.Status.Conditions, metav1.Condition{
		Type:               ConditionReady,
		Status:             metav1.ConditionTrue,
		Reason:             "Synchronized",
		Message:            fmt.Sprintf("Session status mirrored: %s", wSession.Status),
		ObservedGeneration: session.Generation,
	})
}

func buildWorkerClientForAgent(ctx context.Context, c client.Client, agent *kubeopenv1alpha1.Agent) (*kubeadp.Client, error) {
	if agent.Status.URL == "" {
		return nil, fmt.Errorf("agent %q has no URL in status", agent.Name)
	}
	secret := &corev1.Secret{}
	secretKey := client.ObjectKey{Name: KubeadpSecretName(agent.Name), Namespace: agent.Namespace}
	token := ""
	if err := c.Get(ctx, secretKey, secret); err == nil {
		token = strings.TrimSpace(string(secret.Data[KubeadpAPITokenKey]))
	}
	return kubeadp.New(agent.Status.URL, token)
}
func (r *AgentSessionReconciler) SetupWithManager(mgr ctrl.Manager) error {
	return ctrl.NewControllerManagedBy(mgr).
		For(&kubeopenv1alpha1.AgentSession{}).
		Complete(r)
}
