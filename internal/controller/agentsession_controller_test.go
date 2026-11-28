// Copyright Contributors to the KubeOpenCode project

package controller

import (
	"testing"
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	kubeopenv1alpha1 "github.com/kubeopencode/kubeopencode/api/v1alpha1"
	"github.com/kubeopencode/kubeopencode/internal/kubeadp"
)

func TestMirrorSessionStatus(t *testing.T) {
	r := &AgentSessionReconciler{}
	session := &kubeopenv1alpha1.AgentSession{
		ObjectMeta: metav1.ObjectMeta{
			Name:       "test-session",
			Namespace:  "default",
			Generation: 1,
		},
	}

	pid := 1234
	engine := "claude"
	engineReady := true
	wSession := &kubeadp.Session{
		Name:             "test-session",
		Status:           "running",
		PID:              &pid,
		StartTime:        "2026-11-28T12:00:00Z",
		UptimeSecs:       42,
		RestartCount:     2,
		LastExitReason:   "SIGTERM",
		LastHealthResult: "ok",
		Paused:           false,
		Controlled:       true,
		Engine:           &engine,
		EngineReady:      &engineReady,
	}

	r.mirrorSessionStatus(session, wSession)

	if session.Status.Phase != kubeopenv1alpha1.AgentSessionPhaseRunning {
		t.Errorf("expected phase Running, got %s", session.Status.Phase)
	}
	if session.Status.PID == nil || *session.Status.PID != 1234 {
		t.Errorf("expected PID 1234, got %v", session.Status.PID)
	}
	if session.Status.UptimeSecs != 42 {
		t.Errorf("expected uptime 42, got %d", session.Status.UptimeSecs)
	}
	if session.Status.RestartCount != 2 {
		t.Errorf("expected restarts 2, got %d", session.Status.RestartCount)
	}
	if session.Status.Engine != "claude" {
		t.Errorf("expected engine claude, got %s", session.Status.Engine)
	}
	if session.Status.EngineReady == nil || !*session.Status.EngineReady {
		t.Errorf("expected engineReady true")
	}
	if session.Status.StartTime == nil {
		t.Errorf("expected non-nil startTime")
	} else if session.Status.StartTime.Time.Format(time.RFC3339) != "2026-11-28T12:00:00Z" {
		t.Errorf("unexpected startTime: %v", session.Status.StartTime.Time)
	}
}
