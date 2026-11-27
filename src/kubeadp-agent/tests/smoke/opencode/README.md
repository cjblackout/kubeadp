# opencode Engine Smoke Test (`tests/smoke/opencode`)

A live Kubernetes smoke test verifying the `opencode` engine running under the `kubeadp-agent worker` daemon against a dependency-free OpenAI-compatible mock LLM in the same Pod.

## What It Tests

1. **Direct `opencode` execution against mock LLM**:
   - `opencode run --format json -m mock/mock-1 "say hi"` executes directly and receives canned response (`MOCK-OK from the mock llm`) without crashing.
   - Verifies the JSON event stream produces `step_finish`.
2. **`kubeadp-agent worker` daemon lifecycle**:
   - Worker starts, binds port 8002, and passes `/healthz`.
   - `/api/v1/capabilities` reports the `opencode` engine with expected capability channels (`hook`, `tool_result`, `idle_turn`).
3. **`opencode` job execution through REST API**:
   - Submits `POST /api/v1/jobs` with engine `opencode` and model `mock/mock-1`.
   - Polls job status until terminal state (`done`).
   - Retrieves `/api/v1/jobs/{id}/result` and verifies canned response text.
   - Inspects `/api/v1/jobs/{id}/log` and SSE event stream (`/api/v1/jobs/{id}/events`).
4. **Conversation retry and resumption**:
   - Submits `POST /api/v1/jobs/{id}/retry` and verifies the conversation resumes to completion (`done`).
5. **OpenCode harness configuration materialization**:
   - Verifies `~/.config/opencode/opencode.json` contains generated MCP configurations (`mcp.a2a`) while preserving custom provider definitions.
   - Executes `kubeadp-agent a2a-hook transform` on empty input.

## What It Does NOT Test

- **A2A message delivery through the plugin hook**: The live npm-installed `@opencode/plugin` message transform hook is not exercised end-to-end with real inter-agent messaging in this test.
- **`idle_turn` execution**: The live binary test runs batch prompt turns (`run`), not daemon idle turn wakeups.
- **Session mode PTY**: The supervisor session lifecycle (interactive PTY, bracketed paste) is tested in CLI integration tests and `agent-orchestrate`, not in this worker smoke test.
- **`mcp-a2a` tool results in a live run**: The mock LLM produces canned replies directly without calling MCP tools.

## Why a Mock Server (and not llama.cpp)

1. **Egress policies**: The `agents` Kubernetes namespace egress network policy blocks external model hubs (such as `huggingface.co`).
2. **Zero external dependencies**: Downloading multi-gigabyte GGUF models or running heavyweight local inference is unnecessary for validating harness wiring, CLI arguments, process supervision, event streaming, and config synthesis.
3. **Determinism**: `mock-llm.js` is a single-file, zero-dependency Node.js HTTP server implementing `/v1/models` and streaming/non-streaming `/v1/chat/completions` with instant canned replies.

## Prerequisites

- `kubectl` configured with cluster access and permissions to manage Pods and ConfigMaps in the target namespace (default: `agents`).
- Network egress to `registry.npmjs.org` if `opencode` plugins are installed via npm in the pod.
- Prebuilt container image for `kubeadp-agent` (see below).

## Environment Gotchas Learned

1. **Minimal image runtime**: The `kubeadp-agent` container image contains only `node`, `curl`, and `bash`. It does **not** include `jq` or `python`. JSON processing in scripts uses Node.js one-liners.
2. **Root key requirements**: `kubeadp-agent worker` requires `KUBEADP_JOBSET_ROOT_KEY_FILE` pointing to a file containing at least 32 bytes of key material.
3. **Workspace and sandbox layout**: The worker sandbox requires `/opt/tools` (or symlink to `/tools`) and the workspace root (`AGENT_RUN_WORKSPACE_ROOT`, e.g. `/workspace`).
4. **Sandbox home override**: The worker environment sets `KUBEADP_SANDBOX_HOME=/work/home` and mounts emptyDir volumes to avoid contaminating image layers.

## Running the Smoke Test

Run `run.sh` with the target container image specified in `SMOKE_IMAGE`:

```bash
SMOKE_IMAGE=ghcr.io/cjblackout/kubeopencode/kubeadp-agent:2026.11.61 \
  src/kubeadp-agent/tests/smoke/opencode/run.sh
```

### Environment Variables

| Variable | Default | Description |
|---|---|---|
| `SMOKE_IMAGE` | *(required or latest git/gh tag)* | Container image for mock and smoke containers (`:latest` is not accepted) |
| `SMOKE_NAMESPACE` | `agents` | Kubernetes namespace for the test Pod and ConfigMap |
| `SMOKE_TIMEOUT` | `900` | Maximum wait time in seconds (default 15 minutes) |
| `SMOKE_RUNTIME_CLASS` | `gvisor` | Pod `runtimeClassName` (`none` to omit) |
| `SMOKE_NODE_SELECTOR` | `kubernetes.io/arch: amd64, topology.kubernetes.io/region: bwcloud` | Pod `nodeSelector` (`none` to omit) |
| `SMOKE_KEEP` | `0` | If `1`, retains Pod and ConfigMap after execution for debugging |

## Reading the Output

- Each step prints progressive assertions starting with `SMOKE PASS <check>` or `SMOKE FAIL <check>`.
- The final line printed by the test container is always:
  - `SMOKE RESULT: PASS` (all checks succeeded)
  - `SMOKE RESULT: FAIL` (one or more checks failed)
- `run.sh` exits with code `0` if and only if `SMOKE RESULT: PASS` is present and no `SMOKE FAIL` was encountered.
