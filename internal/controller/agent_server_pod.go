// Copyright Contributors to the KubeOpenCode project

package controller

import (
	"context"
	"fmt"

	corev1 "k8s.io/api/core/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"

	kubeopenv1alpha1 "github.com/kubeopencode/kubeopencode/api/v1alpha1"
)

// ServerPodRef identifies the running server pod of an Agent and the container
// inside it that hosts the agent runtime.
type ServerPodRef struct {
	Pod       string
	Container string
}

// ResolveAgentServerPod finds the running, ready server pod of an Agent and the
// name of its main container (the container that runs the agent worker).
//
// It is shared by the web terminal handler and the `kubeoc agent attach` CLI so
// both resolve the exec target the same way. The Agent must exist, not be
// suspended, and be ready; otherwise a descriptive error is returned.
func ResolveAgentServerPod(ctx context.Context, c client.Client, namespace, agentName string) (*ServerPodRef, error) {
	var agent kubeopenv1alpha1.Agent
	if err := c.Get(ctx, client.ObjectKey{Namespace: namespace, Name: agentName}, &agent); err != nil {
		return nil, fmt.Errorf("agent %q not found in namespace %q: %w", agentName, namespace, err)
	}

	if agent.Status.Suspended {
		return nil, fmt.Errorf("agent %q is suspended; resume it first (kubeoc agent resume %s -n %s)", agentName, agentName, namespace)
	}

	if !agent.Status.Ready {
		return nil, fmt.Errorf("agent %q is not ready; check: kubectl get deployment %s -n %s", agentName, ServerDeploymentName(agentName), namespace)
	}

	labels := getServerLabels(agentName)

	podList := &corev1.PodList{}
	if err := c.List(ctx, podList,
		client.InNamespace(namespace),
		client.MatchingLabels(labels),
	); err != nil {
		return nil, fmt.Errorf("failed to list server pods: %w", err)
	}

	for i := range podList.Items {
		pod := &podList.Items[i]
		if pod.Status.Phase != corev1.PodRunning {
			continue
		}
		ready := false
		for _, cond := range pod.Status.Conditions {
			if cond.Type == corev1.PodReady && cond.Status == corev1.ConditionTrue {
				ready = true
				break
			}
		}
		if !ready {
			continue
		}
		return &ServerPodRef{Pod: pod.Name, Container: ServerContainerName}, nil
	}

	return nil, fmt.Errorf("no ready server pod found for agent %q; check: kubectl get pods -n %s", agentName, namespace)
}
