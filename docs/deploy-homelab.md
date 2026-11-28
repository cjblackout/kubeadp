# Homelab Deployment Guide for KubeADP

This guide details how to deploy and operate KubeADP in a homelab Kubernetes cluster (k3s, Kind, or bare-metal), covering all components: the operator, admission controllers, Web UI, and `kubeadp-agent` worker runtime with multi-engine support (`opencode`, `agy`, `claude`).

---

## 1. Prerequisites

- **Kubernetes Cluster**: v1.28+ (e.g. k3s, k0s, or Kind)
- **Container Runtime**: `containerd` with standard cgroups v2
- **Helm**: v3.12+
- **Kubectl**: Configured to access the target cluster
- **Storage**: Default `StorageClass` supporting dynamic PVC provisioning (for workspace & session persistence)

---

## 2. Pinned Container Images

Ensure the following image versions (or your custom homelab registry mirror) are accessible by cluster nodes:

| Component | Default Image Pin | Notes |
|---|---|---|
| **Operator & Server** | `ghcr.io/kubeopencode/kubeopencode:v0.2.0` | Controller and management API server |
| **Worker Daemon** | `ghcr.io/cjblackout/kubeopencode/kubeadp-agent:2026.11.64` | `kubeadp-agent worker` daemon + tools |
| **Default Executor** | `ghcr.io/kubeopencode/kubeopencode-agent-devbox:latest` | Devbox environment with toolchains |

---

## 3. Secrets and Authentication

Before running tasks, create necessary provider secrets in the namespaces where your agents run (e.g., `default` or `kubeadp-system`):

```bash
# Example AI Provider Secrets
kubectl create secret generic ai-credentials \
  --from-literal=ANTHROPIC_API_KEY="sk-ant-..." \
  --from-literal=OPENAI_API_KEY="sk-..." \
  -n default
```

> **Security Note:** Secrets must **never** be passed inline in CRDs. Always reference keys via Kubernetes Secret selectors (`secretKeyRef`).

---

## 4. Helm Deployment

Deploy KubeADP using the official chart in `charts/kubeopencode`:

```bash
helm upgrade --install kubeadp ./charts/kubeopencode \
  --namespace kubeopencode-system \
  --create-namespace \
  --set server.enabled=true \
  --set server.port=2746
```

Verify controller and server pods are running:

```bash
kubectl get pods -n kubeopencode-system
```

---

## 5. Minimal Agent & Task Manifests

### 5.1 Agent with Multi-Engine Support

```yaml
apiVersion: kubeopencode.io/v1alpha1
kind: Agent
metadata:
  name: homelab-coder
  namespace: default
spec:
  engine: claude # Supported: opencode, agy, claude
  workspaceDir: /workspace
  serviceAccountName: default
  worker:
    maxParallel: 4
    defaultModel: claude-3-5-sonnet-20241022
    defaultEffort: medium
    sandbox:
      roPaths:
        - /opt/tools
        - /etc/ssl
  persistence:
    workspace:
      size: 10Gi
    sessions:
      size: 2Gi
```

Apply the agent:

```bash
kubectl apply -f agent.yaml
```

Wait until the agent is ready:

```bash
kubectl get agent homelab-coder -n default -w
```

### 5.2 Executing a Task

```yaml
apiVersion: kubeopencode.io/v1alpha1
kind: Task
metadata:
  name: fix-bug-1
  namespace: default
spec:
  agentRef:
    name: homelab-coder
  project: workspace
  description: "Investigate and fix the failing unit test in pkg/utils"
  model: claude-3-5-sonnet-20241022
  effort: high
  plan: false
```

```bash
kubectl apply -f task.yaml
kubectl get task fix-bug-1 -n default -w
```

### 5.3 Supervised Interactive Session

To run an interactive session:

```yaml
apiVersion: kubeopencode.io/v1alpha1
kind: AgentSession
metadata:
  name: interactive-dev
  namespace: default
spec:
  agentRef:
    name: homelab-coder
  engine: claude
  sandbox: true
```

---

## 6. Accessing the Web UI

To access the UI dashboard:

```bash
kubectl port-forward -n kubeopencode-system svc/kubeopencode-server 2746:2746
```

Open `http://localhost:2746` in your browser. You can inspect Tasks, diff outputs, file trees, Agent worker status, models, quota, and interactive Sessions directly from the UI.

---

## 7. Verification and Smoke Checklist

- [ ] Operator controller and server pods are in `Running` state.
- [ ] Agent deployment becomes `Ready` (`GET /readyz` health check succeeds).
- [ ] Submitting a test Task initiates a job worktree and logs progress in `status.job`.
- [ ] Task diff and file inspection endpoints respond with patch statistics.
- [ ] Stopping a task (`kubectl annotate task <name> kubeopencode.io/stop=true`) gracefully aborts the worker job.
- [ ] Session supervisor allows sending interactive messages and displays ring logs.
