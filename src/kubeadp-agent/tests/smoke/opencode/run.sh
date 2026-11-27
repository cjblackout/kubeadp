#!/usr/bin/env bash
# run.sh - Runner for opencode engine smoke test on Kubernetes.
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
SMOKE_NAMESPACE="${SMOKE_NAMESPACE:-agents}"
POD_NAME="oc-smoke"
CONFIGMAP_NAME="oc-smoke-scripts"
TIMEOUT_SECS="${SMOKE_TIMEOUT:-900}" # 15 minutes default
SMOKE_KEEP="${SMOKE_KEEP:-0}"

# Resolve SMOKE_IMAGE: must be provided, or resolved from newest release tag via git describe/gh.
# latest is NOT accepted as a default because image tags are release versions
# (vX.Y.Z-fork.N). An operator-only build has no agent image: set SMOKE_IMAGE explicitly then.
SMOKE_IMAGE="${SMOKE_IMAGE:-}"
if [ -z "$SMOKE_IMAGE" ]; then
  # Try git describe --tags
  RESOLVED_TAG="$(git describe --tags --abbrev=0 2>/dev/null || true)"
  if [ -z "$RESOLVED_TAG" ] && command -v gh >/dev/null 2>&1; then
    RESOLVED_TAG="$(gh release view --json tagName -q .tagName 2>/dev/null || true)"
  fi

  if [ -n "$RESOLVED_TAG" ]; then
    SMOKE_IMAGE="ghcr.io/cjblackout/kubeopencode/kubeadp-agent:${RESOLVED_TAG}"
    echo "Resolved SMOKE_IMAGE from release tag: ${SMOKE_IMAGE}"
  else
    echo "ERROR: SMOKE_IMAGE is not set and no release tag could be discovered via git or gh." >&2
    echo "       Please provide SMOKE_IMAGE explicitly, e.g.:" >&2
    echo "       SMOKE_IMAGE=ghcr.io/cjblackout/kubeopencode/kubeadp-agent:2026.11.61 $0" >&2
    exit 1
  fi
fi

# Determine optional runtimeClassName and nodeSelector
# Cluster defaults: runtimeClassName: gvisor, nodeSelector: {kubernetes.io/arch: amd64, topology.kubernetes.io/region: bwcloud}
# Can be overridden or set to "none" to omit.
SMOKE_RUNTIME_CLASS="${SMOKE_RUNTIME_CLASS:-gvisor}"
SMOKE_NODE_SELECTOR="${SMOKE_NODE_SELECTOR:-kubernetes.io/arch: amd64, topology.kubernetes.io/region: bwcloud}"

if [ "$SMOKE_RUNTIME_CLASS" = "none" ] || [ -z "$SMOKE_RUNTIME_CLASS" ]; then
  RENDERED_RUNTIME_CLASS=""
else
  RENDERED_RUNTIME_CLASS="  runtimeClassName: ${SMOKE_RUNTIME_CLASS}"
fi

if [ "$SMOKE_NODE_SELECTOR" = "none" ] || [ -z "$SMOKE_NODE_SELECTOR" ]; then
  RENDERED_NODE_SELECTOR=""
else
  # Support inline key: val, key2: val2 syntax
  RENDERED_NODE_SELECTOR="  nodeSelector: {${SMOKE_NODE_SELECTOR}}"
fi

cleanup() {
  if [ "$SMOKE_KEEP" = "1" ]; then
    echo "SMOKE_KEEP=1: Leaving pod/${POD_NAME} and configmap/${CONFIGMAP_NAME} in namespace ${SMOKE_NAMESPACE}."
  else
    echo "Cleaning up test pod and configmap..."
    kubectl delete pod "$POD_NAME" -n "$SMOKE_NAMESPACE" --ignore-not-found=true --wait=false >/dev/null 2>&1 || true
    kubectl delete configmap "$CONFIGMAP_NAME" -n "$SMOKE_NAMESPACE" --ignore-not-found=true >/dev/null 2>&1 || true
  fi
}
trap cleanup EXIT

echo "=== opencode Engine Smoke Test ==="
echo "Namespace:      ${SMOKE_NAMESPACE}"
echo "Image:          ${SMOKE_IMAGE}"
echo "RuntimeClass:   ${SMOKE_RUNTIME_CLASS}"
echo "NodeSelector:   ${SMOKE_NODE_SELECTOR}"
echo "Timeout:        ${TIMEOUT_SECS}s"
echo "==================================="

# Step 1: Create or update ConfigMap oc-smoke-scripts
echo "1. Applying ConfigMap ${CONFIGMAP_NAME}..."
kubectl create configmap "$CONFIGMAP_NAME" \
  --namespace="$SMOKE_NAMESPACE" \
  --from-file=mock-llm.js="${SCRIPT_DIR}/mock-llm.js" \
  --from-file=smoke.sh="${SCRIPT_DIR}/smoke.sh" \
  --dry-run=client -o yaml | kubectl apply -f -

# Step 2: Delete previous pod if present
echo "2. Ensuring no previous pod/${POD_NAME} exists..."
kubectl delete pod "$POD_NAME" -n "$SMOKE_NAMESPACE" --ignore-not-found=true --wait=true

# Step 3: Render pod.yaml and apply
echo "3. Rendering and applying pod.yaml..."
RENDERED_POD=$(sed \
  -e "s|__SMOKE_NAMESPACE__|${SMOKE_NAMESPACE}|g" \
  -e "s|__SMOKE_IMAGE__|${SMOKE_IMAGE}|g" \
  -e "s|^__SMOKE_RUNTIME_CLASS__.*|${RENDERED_RUNTIME_CLASS}|g" \
  -e "s|^__SMOKE_NODE_SELECTOR__.*|${RENDERED_NODE_SELECTOR}|g" \
  "${SCRIPT_DIR}/pod.yaml")

# Strip empty lines resulting from blank replacements
printf "%s\n" "$RENDERED_POD" | grep -v '^[[:space:]]*$' | kubectl apply -f -

# Step 4: Wait for smoke container to terminate
echo "4. Waiting for container 'smoke' in pod/${POD_NAME} to finish (timeout ${TIMEOUT_SECS}s)..."
START_TIME=$(date +%s)
TERMINATED=0

while true; do
  NOW=$(date +%s)
  ELAPSED=$(( NOW - START_TIME ))
  if [ "$ELAPSED" -ge "$TIMEOUT_SECS" ]; then
    echo "ERROR: Timed out after ${TIMEOUT_SECS}s waiting for smoke test completion." >&2
    break
  fi

  # Check pod phase and container states via jsonpath
  POD_STATUS=$(kubectl get pod "$POD_NAME" -n "$SMOKE_NAMESPACE" -o json 2>/dev/null || true)
  if [ -z "$POD_STATUS" ]; then
    sleep 3
    continue
  fi

  # Check for common failure states across init and regular containers
  FAIL_REASON=$(echo "$POD_STATUS" | grep -oE '"reason":"(ImagePullBackOff|ErrImagePull|CrashLoopBackOff)"' | head -1 | cut -d'"' -f4 || true)
  if [ -n "$FAIL_REASON" ]; then
    echo "ERROR: Pod entered failure state: ${FAIL_REASON}" >&2
    kubectl describe pod "$POD_NAME" -n "$SMOKE_NAMESPACE" | tail -30 >&2
    exit 1
  fi

  # Check if smoke container has terminated
  SMOKE_TERMINATED=$(echo "$POD_STATUS" | node -e '
    let s = "";
    process.stdin.on("data", d => s += d).on("end", () => {
      try {
        const pod = JSON.parse(s);
        const cStatuses = pod.status && pod.status.containerStatuses ? pod.status.containerStatuses : [];
        const smoke = cStatuses.find(c => c.name === "smoke");
        if (smoke && smoke.state && smoke.state.terminated) {
          console.log("yes");
          return;
        }
      } catch (e) {}
      console.log("no");
    });
  ')

  if [ "$SMOKE_TERMINATED" = "yes" ]; then
    TERMINATED=1
    echo "Container 'smoke' has terminated."
    break
  fi

  sleep 5
done

if [ "$TERMINATED" -ne 1 ]; then
  echo "ERROR: Smoke container did not terminate within timeout." >&2
  kubectl describe pod "$POD_NAME" -n "$SMOKE_NAMESPACE" | tail -30 >&2
  exit 1
fi

# Step 5: Print logs and evaluate exit code
echo "5. Outputting logs from 'smoke' container:"
echo "----------------------------------------"
SMOKE_LOGS=$(kubectl logs "$POD_NAME" -c smoke -n "$SMOKE_NAMESPACE" 2>&1 || true)
echo "$SMOKE_LOGS"
echo "----------------------------------------"

if echo "$SMOKE_LOGS" | grep -q 'SMOKE RESULT: PASS' && ! echo "$SMOKE_LOGS" | grep -q 'SMOKE FAIL'; then
  echo "Smoke test SUCCEEDED."
  exit 0
else
  echo "Smoke test FAILED." >&2
  exit 1
fi
