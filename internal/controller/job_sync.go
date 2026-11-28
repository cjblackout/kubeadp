// Copyright Contributors to the KubeOpenCode project

package controller

import (
	"context"
	"fmt"
	"strconv"
	"strings"
	"time"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/log"

	kubeopenv1alpha1 "github.com/kubeopencode/kubeopencode/api/v1alpha1"
	"github.com/kubeopencode/kubeopencode/internal/kubeadp"
)

// syncTaskJob polls the worker for job status while status.job.id is known,
// and updates task.Status.Job accordingly.
func (r *TaskReconciler) syncTaskJob(ctx context.Context, task *kubeopenv1alpha1.Task) (ctrl.Result, error) {
	logger := log.FromContext(ctx)

	if task.Status.Job == nil || task.Status.Job.ID == "" {
		return ctrl.Result{}, nil
	}

	jobID := task.Status.Job.ID
	agentName := taskAgentName(task)
	if agentName == "" {
		return ctrl.Result{}, nil
	}

	agent := &kubeopenv1alpha1.Agent{}
	if err := r.Get(ctx, types.NamespacedName{Name: agentName, Namespace: task.Namespace}, agent); err != nil {
		logger.V(1).Info("cannot sync job: agent not found", "agent", agentName, "error", err)
		return ctrl.Result{}, nil
	}
	if agent.Status.URL == "" {
		logger.V(1).Info("cannot sync job: agent has no URL", "agent", agentName)
		return ctrl.Result{}, nil
	}

	// Fetch token
	secret := &corev1.Secret{}
	secretKey := types.NamespacedName{Name: KubeadpSecretName(agentName), Namespace: task.Namespace}
	token := ""
	if err := r.Get(ctx, secretKey, secret); err == nil {
		token = strings.TrimSpace(string(secret.Data[KubeadpAPITokenKey]))
	}

	client, err := kubeadp.New(agent.Status.URL, token)
	if err != nil {
		logger.Error(err, "invalid worker client URL", "url", agent.Status.URL)
		return ctrl.Result{}, nil
	}

	// Fetch job
	job, err := client.Get(ctx, jobID)
	if err != nil {
		// Check if unreachable
		if apiErr, ok := err.(*kubeadp.APIError); ok && apiErr.Status == 404 {
			// Job not found on worker -> lost
			task.Status.Job.Status = "lost"
			meta.SetStatusCondition(&task.Status.Conditions, metav1.Condition{
				Type:    kubeopenv1alpha1.ReasonJobLost,
				Status:  metav1.ConditionTrue,
				Reason:  kubeopenv1alpha1.ReasonJobLost,
				Message: fmt.Sprintf("job %s was lost on worker", jobID),
			})
			_ = r.Status().Update(ctx, task)
			return ctrl.Result{}, nil
		}

		// Failure to reach worker sets WorkerReachable=False, never fails the Task
		logger.V(1).Info("worker unreachable during job sync", "error", err)
		meta.SetStatusCondition(&task.Status.Conditions, metav1.Condition{
			Type:    kubeopenv1alpha1.ConditionTypeWorkerReachable,
			Status:  metav1.ConditionFalse,
			Reason:  "WorkerUnreachable",
			Message: err.Error(),
		})
		_ = r.Status().Update(ctx, task)
		return ctrl.Result{RequeueAfter: 10 * time.Second}, nil
	}

	// Worker is reachable
	meta.SetStatusCondition(&task.Status.Conditions, metav1.Condition{
		Type:    kubeopenv1alpha1.ConditionTypeWorkerReachable,
		Status:  metav1.ConditionTrue,
		Reason:  "WorkerReachable",
		Message: "Worker API responded successfully",
	})

	// Update job status fields
	task.Status.Job.Status = job.Status
	if job.Engine != "" {
		task.Status.Job.Engine = job.Engine
	}

	// Query todo and permissions while running if supported
	if job.Status == "running" {
		if todoList, err := client.Todo(ctx, jobID); err == nil {
			var items []kubeopenv1alpha1.TodoItemStatus
			for _, item := range todoList {
				items = append(items, kubeopenv1alpha1.TodoItemStatus{
					Content: item.Task,
					Status:  item.Status,
				})
			}
			task.Status.Job.Todo = items
		}
		if perms, err := client.Permissions(ctx, jobID); err == nil {
			task.Status.Job.PendingPermissions = int32(len(perms))
		}
	}

	// Terminal states handling
	if isTerminalJobStatus(job.Status) {
		// Read result
		if res, err := client.Result(ctx, jobID); err == nil {
			if res.Usage != nil {
				if task.Status.Session == nil {
					task.Status.Session = &kubeopenv1alpha1.SessionInfo{}
				}
				task.Status.Session.Summary = &kubeopenv1alpha1.SessionSummary{
					Cost: fmt.Sprintf("%.6f", res.Usage.Cost),
					TokenUsage: &kubeopenv1alpha1.TokenUsage{
						Input:     res.Usage.InputTokens,
						Output:    res.Usage.OutputTokens,
						Reasoning: res.Usage.ReasoningTokens,
						Cache:     res.Usage.CacheTokens,
					},
				}
			}
		}

		// Read diff stat
		if diffStr, err := client.Diff(ctx, jobID, true); err == nil && diffStr != "" {
			task.Status.Job.Diff = parseDiffStat(diffStr)
		}

		// Map to Phase and Conditions per M1 specification
		mapJobStatusToTaskOutcome(task, job.Status)

		if err := r.Status().Update(ctx, task); err != nil {
			// If not found in cluster, don't fail reconcile
			if errors.IsNotFound(err) {
				return ctrl.Result{}, nil
			}
			return ctrl.Result{}, err
		}
		return ctrl.Result{}, nil
	}

	if err := r.Status().Update(ctx, task); err != nil {
		return ctrl.Result{}, err
	}

	// Poll every 5s while running
	return ctrl.Result{RequeueAfter: 5 * time.Second}, nil
}

// mapJobStatusToTaskOutcome maps terminal worker job status to Task Phase and Conditions:
// done -> Completed
// failed|incomplete|quota-exhausted -> Completed with condition Succeeded=False and reason = status
// self-terminated|killed -> Completed with condition Stopped
func mapJobStatusToTaskOutcome(task *kubeopenv1alpha1.Task, status string) {
	task.Status.ObservedGeneration = task.Generation
	task.Status.Phase = kubeopenv1alpha1.TaskPhaseCompleted
	if task.Status.CompletionTime == nil {
		now := metav1.Now()
		task.Status.CompletionTime = &now
	}

	switch status {
	case "done":
		meta.SetStatusCondition(&task.Status.Conditions, metav1.Condition{
			Type:    kubeopenv1alpha1.ConditionTypeSucceeded,
			Status:  metav1.ConditionTrue,
			Reason:  "Completed",
			Message: "Job completed successfully",
		})
	case "failed", "incomplete", "quota-exhausted":
		meta.SetStatusCondition(&task.Status.Conditions, metav1.Condition{
			Type:    kubeopenv1alpha1.ConditionTypeSucceeded,
			Status:  metav1.ConditionFalse,
			Reason:  status,
			Message: fmt.Sprintf("Job finished with status %s", status),
		})
	case "self-terminated", "killed":
		meta.SetStatusCondition(&task.Status.Conditions, metav1.Condition{
			Type:    kubeopenv1alpha1.ConditionTypeStopped,
			Status:  metav1.ConditionTrue,
			Reason:  status,
			Message: fmt.Sprintf("Job stopped with status %s", status),
		})
	}
}

// isTerminalJobStatus reports whether the job status is terminal.
func isTerminalJobStatus(s string) bool {
	switch s {
	case "done", "failed", "self-terminated", "incomplete", "quota-exhausted", "killed", "lost":
		return true
	}
	return false
}

// parseDiffStat parses summary line of git diff --stat (e.g., "3 files changed, 25 insertions(+), 5 deletions(-)")
func parseDiffStat(diff string) *kubeopenv1alpha1.DiffStat {
	lines := strings.Split(strings.TrimSpace(diff), "\n")
	if len(lines) == 0 {
		return nil
	}
	lastLine := strings.TrimSpace(lines[len(lines)-1])
	parts := strings.Split(lastLine, ",")

	var files, adds, dels int32
	for _, p := range parts {
		p = strings.TrimSpace(p)
		fields := strings.Fields(p)
		if len(fields) > 0 {
			if v, err := strconv.ParseInt(fields[0], 10, 32); err == nil {
				if strings.Contains(p, "file") {
					files = int32(v)
				} else if strings.Contains(p, "insertion") {
					adds = int32(v)
				} else if strings.Contains(p, "deletion") {
					dels = int32(v)
				}
			}
		}
	}

	return &kubeopenv1alpha1.DiffStat{
		Files:     files,
		Additions: adds,
		Deletions: dels,
	}
}
