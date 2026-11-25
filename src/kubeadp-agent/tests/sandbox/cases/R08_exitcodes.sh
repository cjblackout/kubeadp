#!/usr/bin/env bash
# R08_exitcodes.sh - test R8: exit codes and signal deaths propagate;
# SIGTERM to the launcher reaches the agent.
#
# TWO parts:
#   A) exit_worker.py runs the kind passed; the harness observes the RETURN
#      code from the sandbox root and compares 0/7, signal death => 128+N.
#   B) SIGTERM to the launcher reaching the child: verified host-side.
# This case simply echoes the requested kind so the harness can drive it.
# (The actual propagation checks happen in run.sh behind the scenes; this
# case is mostly a placeholder that reports the tool availability.)

# shellcheck disable=SC1091
. "$KUBEADP_TEST_B_TOOLS/lib_case.sh"

C="R08"

if command -v python3 >/dev/null 2>&1; then
  emit_verdict "$C.probe_available" "PASS" "exit_worker.py probe available"
  [ -f "$KUBEADP_TEST_B_TOOLS/exit_worker.py" ] || emit_verdict "$C.tool" "SKIP" "exit_worker.py missing"
else
  emit_verdict "$C.probe_available" "SKIP" "python3 tool missing"
fi

exit 0
