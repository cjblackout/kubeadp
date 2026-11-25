#!/usr/bin/env bash
# R07_descendants.sh - test R7: when the agent root exits, every descendant
# (setsid, double fork, nohup, python daemonize) is gone within 5 s.
#
# This case runs MOSTLY host-side in the harness: it is R07 that verifies the
# teardown. This in-sandbox portion spawns the descendants and signals
# "SPAWNED-READY" so the harness can proceed to exit the agent root and time
# the disappearance. If the case cannot run (no python3), it reports SKIP and
# the harness skips R07's host-side wait.

# shellcheck disable=SC1091
. "$KUBEADP_TEST_B_TOOLS/lib_case.sh"

C="R07"
DESCLOG="${KUBEADP_TEST_DESCLOG:-}"

if [ -z "$DESCLOG" ]; then
  emit_verdict "$C.spawn" "SKIP" "no desc logdir supplied by harness"
  exit 0
fi

mkdir -p "$DESCLOG"
if command -v python3 >/dev/null 2>&1; then
  # spawn the tree, then sleep forever so the harness can TERM this root
  python3 "$KUBEADP_TEST_B_TOOLS/spawn_descendants.py" "$DESCLOG" "${KUBEADP_TEST_TAG:-d}" &
  # write our info for the harness
  if [ -n "$DESCLOG" ]; then
    printf 'rootpid=%s\n' "$$" > "$DESCLOG/rootpid"
  fi
  emit_verdict "$C.spawn" "PASS" "descendants spawned, waiting for sandbox teardown"
  # wait indefinitely; harness TERMs us
  while :; do sleep 300 || break; done
else
  emit_verdict "$C.spawn" "SKIP" "python3 tool missing"
fi

exit 0
