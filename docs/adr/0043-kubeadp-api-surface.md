# ADR 0043: Full KubeADP Agent Worker API Surface Integration

## Status
Accepted

## Context
KubeOpenCode has migrated its worker container model to `kubeadp-agent` (a native, secure multi-engine worker daemon). The worker daemon exposes a comprehensive REST/SSE API (`/api/v1`) for job execution, lifecycle management, live status mirroring, capabilities, models, authentication, and remote control across three engines: OpenCode, Antigravity (`agy`), and Claude Code.

Previously, many operator features relied on ad-hoc HTTP proxies, direct OpenCode-specific protocols, or lacked representation in Kubernetes Custom Resource Definitions (CRDs) and user interfaces.

## Decision
1. **Unified Typed Go Client**:
   Consolidate all communication between the operator controllers, API server, task-submit pods, and the `kubeadp-agent` daemon into a single typed Go package (`internal/kubeadp`). Prevent route drift via test assertions against `src/kubeadp-agent/docs/routes.json`.

2. **Declarative State & Imperative Actions**:
   - Declarative fields for jobs (project, gitRef, model, effort, plan, hindsight, sandbox) reside in `Task.spec`.
   - Worker daemon configuration (`[common]` and `[worker]` TOML keys) resides in `Agent.spec.worker` and `AgentTemplate.spec.worker`.
   - Execution engine (`opencode`, `agy`, `claude`) is configurable per Agent / AgentTemplate.
   - Imperative actions (retry, kill, commit, discard, clean, review, message, interrupt, pause, compact, fork, revert) are exposed via operator REST endpoints authorized with Kubernetes `SubjectAccessReview` (SAR) RBAC and audited as Kubernetes Events on the Task resource.

3. **Status Mirroring**:
   - Task controller polls `/api/v1/jobs/{id}` and mirrors execution status, branch, worktree, diff statistics, and todo checklists into `Task.status.job`.
   - Agent controller reconciles `/readyz` readiness, config rollout (via Secret/ConfigMap hash), and periodically mirrors version, capacity, quota, models, and capabilities into `Agent.status.worker`.

4. **Engine Capability Awareness**:
   The operator REST API and UI check the worker's reported capability matrix before rendering or dispatching actions, ensuring only supported endpoints are active for each engine.

## Consequences
- Single, strictly typed client layer simplifies testing and maintenance.
- Operators and developers can manage multi-engine agent fleets natively in Kubernetes using standard CRD declarations, GitOps, and the unified UI.
- No secrets are passed inline in CRDs or config files; secrets use standard Kubernetes Secret references.
