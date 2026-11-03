#!/usr/bin/env bash
# R03_ptrace.sh - R3: cannot ptrace / process_vm_readv / pidfd_getfd another
# agent's process.
#
# Runs INSIDE agent B's sandbox. Discovers the victim pid and runs ctypes
# probes: PTRACE_ATTACH, PTRACE_SEIZE, process_vm_readv at the fixed secret
# address, pidfd_getfd. A successful attack => FAIL (leak).

# shellcheck disable=SC1091
. "$KUBEADP_TEST_B_TOOLS/lib_case.sh"

C="R03"
TPID="$(pick_target)"

if [ -z "$TPID" ] || [ "$TPID" = "0" ]; then
  emit_verdict "$C.discover" "SKIP" "no target pid visible"
  exit 0
fi

if command -v python3 >/dev/null 2>&1; then
  python3 "$KUBEADP_TEST_B_TOOLS/probe_ptrace.py" "$TPID" || true
else
  emit_verdict "$C.probe" "SKIP" "python3 tool missing"
fi

exit 0
