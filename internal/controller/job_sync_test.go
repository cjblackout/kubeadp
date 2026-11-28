// Copyright Contributors to the KubeOpenCode project

package controller

import (
	"context"
	"strings"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	kubeopenv1alpha1 "github.com/kubeopencode/kubeopencode/api/v1alpha1"
	"github.com/kubeopencode/kubeopencode/internal/kubeadp"
	fakeWorker "github.com/kubeopencode/kubeopencode/internal/kubeadp/fake"
)

func TestJobSyncTransitions(t *testing.T) {
	scheme := runtime.NewScheme()
	_ = corev1.AddToScheme(scheme)
	_ = kubeopenv1alpha1.AddToScheme(scheme)

	fw := fakeWorker.New("token-xyz")
	defer fw.Close()

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
			Name:      KubeadpSecretName("test-agent"),
			Namespace: "default",
		},
		Data: map[string][]byte{
			KubeadpAPITokenKey: []byte("token-xyz"),
		},
	}

	// Test 1: Running state syncing todo, permissions and worker reachable
	jobID := "job-running-1"
	fw.AddJob(&kubeadp.Job{
		JobID:  jobID,
		Status: "running",
		Engine: "agy",
	})
	fw.SetTodos(jobID, []kubeadp.TodoItem{
		{Task: "Implement sync", Status: "in_progress"},
	})

	task := &kubeopenv1alpha1.Task{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "task-1",
			Namespace: "default",
		},
		Spec: kubeopenv1alpha1.TaskSpec{
			AgentRef: &kubeopenv1alpha1.AgentReference{Name: "test-agent"},
		},
		Status: kubeopenv1alpha1.TaskExecutionStatus{
			Phase: kubeopenv1alpha1.TaskPhaseRunning,
			Job: &kubeopenv1alpha1.JobStatus{
				ID:     jobID,
				Status: "running",
			},
		},
	}

	client := fake.NewClientBuilder().WithScheme(scheme).WithStatusSubresource(&kubeopenv1alpha1.Task{}).WithObjects(agent, secret, task).Build()
	reconciler := &TaskReconciler{
		Client: client,
		Scheme: scheme,
	}

	res, err := reconciler.syncTaskJob(context.Background(), task)
	if err != nil {
		t.Fatalf("syncTaskJob returned error: %v", err)
	}
	if res.RequeueAfter != 5*time.Second {
		t.Errorf("expected 5s requeue for running job, got %v", res.RequeueAfter)
	}

	if len(task.Status.Job.Todo) != 1 || task.Status.Job.Todo[0].Content != "Implement sync" {
		t.Errorf("unexpected todos: %+v", task.Status.Job.Todo)
	}

	// Test 2: Terminal done with result and diff stat
	jobIDDone := "job-done-1"
	fw.AddJob(&kubeadp.Job{
		JobID:  jobIDDone,
		Status: "done",
		Engine: "agy",
	})
	fw.SetResult(jobIDDone, &kubeadp.JobResult{
		JobID:    jobIDDone,
		Status:   "done",
		Response: "all good",
		Usage: &kubeadp.JobUsage{
			InputTokens:  100,
			OutputTokens: 200,
			Cost:         0.015,
		},
	})
	fw.SetDiff(jobIDDone, "2 files changed, 10 insertions(+), 2 deletions(-)")

	taskDone := &kubeopenv1alpha1.Task{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "task-done",
			Namespace: "default",
		},
		Spec: kubeopenv1alpha1.TaskSpec{
			AgentRef: &kubeopenv1alpha1.AgentReference{Name: "test-agent"},
		},
		Status: kubeopenv1alpha1.TaskExecutionStatus{
			Phase: kubeopenv1alpha1.TaskPhaseRunning,
			Job: &kubeopenv1alpha1.JobStatus{
				ID:     jobIDDone,
				Status: "running",
			},
		},
	}
	_ = client.Create(context.Background(), taskDone)

	res, err = reconciler.syncTaskJob(context.Background(), taskDone)
	if err != nil {
		t.Fatalf("syncTaskJob done returned error: %v", err)
	}
	if res.RequeueAfter != 0 {
		t.Errorf("expected no requeue for terminal job, got %v", res.RequeueAfter)
	}
	if taskDone.Status.Phase != kubeopenv1alpha1.TaskPhaseCompleted {
		t.Errorf("expected phase Completed, got %s", taskDone.Status.Phase)
	}
	if taskDone.Status.Job.Diff == nil || taskDone.Status.Job.Diff.Files != 2 || taskDone.Status.Job.Diff.Additions != 10 {
		t.Errorf("expected parsed diff stat files=2 adds=10, got %+v", taskDone.Status.Job.Diff)
	}
	if taskDone.Status.Session == nil || taskDone.Status.Session.Summary == nil || taskDone.Status.Session.Summary.TokenUsage.Input != 100 {
		t.Errorf("expected session token usage summary, got %+v", taskDone.Status.Session)
	}

	// Test 3: Lost job (404 from worker)
	taskLost := &kubeopenv1alpha1.Task{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "task-lost",
			Namespace: "default",
		},
		Spec: kubeopenv1alpha1.TaskSpec{
			AgentRef: &kubeopenv1alpha1.AgentReference{Name: "test-agent"},
		},
		Status: kubeopenv1alpha1.TaskExecutionStatus{
			Phase: kubeopenv1alpha1.TaskPhaseRunning,
			Job: &kubeopenv1alpha1.JobStatus{
				ID:     "nonexistent-job-404",
				Status: "running",
			},
		},
	}
	_ = client.Create(context.Background(), taskLost)

	_, err = reconciler.syncTaskJob(context.Background(), taskLost)
	if err != nil {
		t.Fatalf("syncTaskJob lost returned error: %v", err)
	}
	if taskLost.Status.Job.Status != "lost" {
		t.Errorf("expected job status 'lost', got %s", taskLost.Status.Job.Status)
	}

	// Test 4: Unreachable worker sets WorkerReachable=False and does not fail task
	agentBroken := &kubeopenv1alpha1.Agent{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "broken-agent",
			Namespace: "default",
		},
		Status: kubeopenv1alpha1.AgentStatus{
			URL:   "http://127.0.0.1:59999", // closed port
			Ready: true,
		},
	}
	_ = client.Create(context.Background(), agentBroken)

	taskUnreach := &kubeopenv1alpha1.Task{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "task-unreach",
			Namespace: "default",
		},
		Spec: kubeopenv1alpha1.TaskSpec{
			AgentRef: &kubeopenv1alpha1.AgentReference{Name: "broken-agent"},
		},
		Status: kubeopenv1alpha1.TaskExecutionStatus{
			Phase: kubeopenv1alpha1.TaskPhaseRunning,
			Job: &kubeopenv1alpha1.JobStatus{
				ID:     "some-job",
				Status: "running",
			},
		},
	}
	_ = client.Create(context.Background(), taskUnreach)

	res, err = reconciler.syncTaskJob(context.Background(), taskUnreach)
	if err != nil {
		t.Fatalf("syncTaskJob unreachable should not return error, got: %v", err)
	}
	if res.RequeueAfter != 10*time.Second {
		t.Errorf("expected 10s backoff for unreachable worker, got %v", res.RequeueAfter)
	}
	foundWorkerReachableFalse := false
	for _, c := range taskUnreach.Status.Conditions {
		if c.Type == kubeopenv1alpha1.ConditionTypeWorkerReachable && c.Status == metav1.ConditionFalse {
			foundWorkerReachableFalse = true
		}
	}
	if !foundWorkerReachableFalse {
		t.Errorf("expected WorkerReachable=False condition on unreachable worker, got: %+v", taskUnreach.Status.Conditions)
	}
	if taskUnreach.Status.Phase != kubeopenv1alpha1.TaskPhaseRunning {
		t.Errorf("task phase must not change on unreachable worker, got: %s", taskUnreach.Status.Phase)
	}
}

func TestBuildTaskSubmitCommandPerSpecField(t *testing.T) {
	ptrBool := func(b bool) *bool { return &b }
	dur := metav1.Duration{Duration: 45 * time.Minute}
	printDur := metav1.Duration{Duration: 15 * time.Second}
	eng := kubeopenv1alpha1.AgentEngineAgy

	task := &kubeopenv1alpha1.Task{
		Spec: kubeopenv1alpha1.TaskSpec{
			Project:      "backend-service",
			GitRef:       "feature/v2",
			Model:        "claude-3-7-sonnet",
			Effort:       "high",
			Sandbox:      ptrBool(true),
			Plan:         ptrBool(false),
			AddDirs:      []string{"/shared/schemas", "/shared/docs"},
			PrintTimeout: &printDur,
			Mode:         "architect",
			Proxy:        "http://proxy.internal:3128",
			Jobset:       "jobset-team-a",
			Hindsight: &kubeopenv1alpha1.TaskHindsight{
				Enabled: ptrBool(true),
				Bank:    "memory-bank-1",
				Query:   "api conventions",
			},
			FromPatch: &kubeopenv1alpha1.FromPatchSource{
				Text: "patch-content",
			},
			Timeout: &dur,
		},
	}

	cfg := agentConfig{
		workspaceDir: "/workspace",
		engine:       &eng,
	}

	cmd := buildTaskSubmitCommand(task, cfg, "http://server:4096", "backend-service")
	joined := strings.Join(cmd, " ")

	expectedTokens := []string{
		"/kubeopencode task-submit",
		"--url http://server:4096",
		"--task-file /workspace/task.md",
		"--project backend-service",
		"--engine agy",
		"--model claude-3-7-sonnet",
		"--effort high",
		"--git-ref feature/v2",
		"--sandbox=true",
		"--plan=false",
		"--mode architect",
		"--proxy http://proxy.internal:3128",
		"--jobset-id jobset-team-a",
		"--add-dir /shared/schemas",
		"--add-dir /shared/docs",
		"--print-timeout 15s",
		"--hindsight=true",
		"--hindsight-bank memory-bank-1",
		"--hindsight-query api conventions",
		"--from-patch-content patch-content",
		"--timeout 2700s",
	}

	for _, token := range expectedTokens {
		if !strings.Contains(joined, token) {
			t.Errorf("command missing token %q\nFull command:\n%s", token, joined)
		}
	}
}
