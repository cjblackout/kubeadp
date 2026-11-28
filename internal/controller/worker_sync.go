// Copyright Contributors to the KubeOpenCode project

package controller

import (
	"context"
	"strings"
	"time"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/log"

	kubeopenv1alpha1 "github.com/kubeopencode/kubeopencode/api/v1alpha1"
	"github.com/kubeopencode/kubeopencode/internal/kubeadp"
)

// syncAgentWorker reads info, capabilities, models, quota, projects, auth status,
// and remote-control status from the worker via the internal/kubeadp client and mirrors
// them into agent.Status.Worker.
// Sync interval is 30s (5s until first success).
func (r *AgentReconciler) syncAgentWorker(ctx context.Context, agent *kubeopenv1alpha1.Agent) (time.Duration, error) {
	logger := log.FromContext(ctx)

	if agent.Status.URL == "" {
		return 5 * time.Second, nil
	}

	// Read Secret token
	secret := &corev1.Secret{}
	secretKey := types.NamespacedName{Name: KubeadpSecretName(agent.Name), Namespace: agent.Namespace}
	token := ""
	if err := r.Get(ctx, secretKey, secret); err == nil {
		token = strings.TrimSpace(string(secret.Data[KubeadpAPITokenKey]))
	}

	client, err := kubeadp.New(agent.Status.URL, token)
	if err != nil {
		logger.Error(err, "invalid worker client URL", "url", agent.Status.URL)
		return 30 * time.Second, nil
	}

	// 1. Info
	info, err := client.Info(ctx)
	if err != nil {
		// Worker is unreachable
		logger.V(1).Info("worker unreachable during sync", "agent", agent.Name, "error", err)
		meta.SetStatusCondition(&agent.Status.Conditions, metav1.Condition{
			Type:    kubeopenv1alpha1.ConditionTypeWorkerReachable,
			Status:  metav1.ConditionFalse,
			Reason:  "WorkerUnreachable",
			Message: err.Error(),
		})
		if agent.Status.Worker == nil {
			agent.Status.Worker = &kubeopenv1alpha1.WorkerStatus{}
		}
		agent.Status.Worker.Ready = false
		return 5 * time.Second, nil
	}

	meta.SetStatusCondition(&agent.Status.Conditions, metav1.Condition{
		Type:    kubeopenv1alpha1.ConditionTypeWorkerReachable,
		Status:  metav1.ConditionTrue,
		Reason:  "WorkerReachable",
		Message: "Worker responded to /api/v1/info",
	})

	if agent.Status.Worker == nil {
		agent.Status.Worker = &kubeopenv1alpha1.WorkerStatus{}
	}

	now := metav1.Now()
	agent.Status.Worker.LastSyncTime = &now
	agent.Status.Worker.Ready = true

	// Parse server info snapshot
	if info != nil {
		if srv, ok := info.Server["version"].(string); ok {
			agent.Status.Worker.Version = srv
		}
		if capMap, ok := info.Capacity["max_parallel"].(float64); ok {
			if agent.Status.Worker.Capacity == nil {
				agent.Status.Worker.Capacity = &kubeopenv1alpha1.WorkerCapacityStatus{}
			}
			agent.Status.Worker.Capacity.MaxParallel = int32(capMap)
		}
		if runMap, ok := info.Capacity["running"].(float64); ok {
			if agent.Status.Worker.Capacity == nil {
				agent.Status.Worker.Capacity = &kubeopenv1alpha1.WorkerCapacityStatus{}
			}
			agent.Status.Worker.Capacity.Running = int32(runMap)
		}
		if diskMap, ok := info.Disk["total_bytes"].(float64); ok {
			if agent.Status.Worker.Disk == nil {
				agent.Status.Worker.Disk = &kubeopenv1alpha1.WorkerDiskStatus{}
			}
			agent.Status.Worker.Disk.TotalBytes = int64(diskMap)
		}
		if diskFree, ok := info.Disk["free_bytes"].(float64); ok {
			if agent.Status.Worker.Disk == nil {
				agent.Status.Worker.Disk = &kubeopenv1alpha1.WorkerDiskStatus{}
			}
			agent.Status.Worker.Disk.FreeBytes = int64(diskFree)
		}
	}

	// 2. Capabilities
	if caps, err := client.Capabilities(ctx); err == nil && caps != nil {
		var list []kubeopenv1alpha1.WorkerCapability
		for _, c := range caps.Capabilities {
			list = append(list, kubeopenv1alpha1.WorkerCapability{
				Engine:    c.Engine,
				Transport: c.Transport,
				Endpoints: c.Endpoints,
			})
		}
		agent.Status.Worker.Capabilities = list
	}

	// 3. Models (capped at 200)
	if mods, err := client.Models(ctx); err == nil && mods != nil {
		var modelNames []string
		for i, m := range mods {
			if i >= 200 {
				break
			}
			modelNames = append(modelNames, m.ID)
		}
		agent.Status.Worker.Models = modelNames
	}

	// 4. Quota
	if q, err := client.Quota(ctx, false); err == nil && q != nil {
		var buckets []kubeopenv1alpha1.WorkerQuotaStatus
		for _, b := range q.Buckets {
			buckets = append(buckets, kubeopenv1alpha1.WorkerQuotaStatus{
				Group:            b.Name,
				RemainingPercent: int32(b.Percent),
			})
		}
		agent.Status.Worker.Quota = buckets
	}

	// 5. Projects
	if projs, err := client.Projects(ctx); err == nil && projs != nil {
		var list []kubeopenv1alpha1.WorkerProjectStatus
		for _, p := range projs {
			list = append(list, kubeopenv1alpha1.WorkerProjectStatus{
				Name:   p.Name,
				Branch: p.Branch,
				Head:   p.Head,
				Jobs:   int32(p.JobsCount),
			})
		}
		agent.Status.Worker.Projects = list
	}

	// 6. Auth status
	if auth, err := client.AuthStatus(ctx); err == nil && auth != nil {
		if agent.Status.Worker.Auth == nil {
			agent.Status.Worker.Auth = &kubeopenv1alpha1.WorkerAuthStatus{}
		}
		if auth.Authenticated {
			agent.Status.Worker.Auth.State = "authenticated"
		} else {
			agent.Status.Worker.Auth.State = "unauthenticated"
		}
	}

	// 7. Remote control status
	if rc, err := client.RemoteControlStatus(ctx); err == nil && rc != nil {
		agent.Status.Worker.RemoteControl = &kubeopenv1alpha1.WorkerRemoteControlStatus{
			Enabled: rc.Enabled,
			State:   rc.Status,
		}
	}

	return 30 * time.Second, nil
}
