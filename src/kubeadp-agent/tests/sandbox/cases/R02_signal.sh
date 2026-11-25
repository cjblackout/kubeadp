#!/usr/bin/env bash
# R02_signal.sh - R2: cannot signal another agent / the parent daemon.
#
# Runs INSIDE agent B's sandbox. Discovers the victim pid (same-namespace
# scan for the run tag, else the env-supplied host pid) and attempts
# kill/tgkill/pidfd_send_signal against it and the daemon sleeper. A delivered
# signal => FAIL (leak).

# shellcheck disable=SC1091
. "$KUBEADP_TEST_B_TOOLS/lib_case.sh"

C="R02"
TPID="$(pick_target)"
DAEMONS="${KUBEADP_TEST_DAEMON_PIDS:-}"

if [ -z "$TPID" ] || [ "$TPID" = "0" ]; then
  emit_verdict "$C.discover" "SKIP" "no target pid visible"
  exit 0
fi

if command -v python3 >/dev/null 2>&1; then
  python3 "$KUBEADP_TEST_B_TOOLS/probe_signal.py" "$TPID" $DAEMONS || true
else
  # shell fallback
  for sig in TERM KILL; do
    if kill "-$sig" "$TPID" 2>/dev/null; then
      emit_verdict "$C.kill_$sig" "FAIL" "kill -$sig delivered"
    else
      emit_verdict "$C.kill_$sig" "PASS" "kill -$sig blocked"
    fi
  done
  for d in $DAEMONS; do
    if kill -TERM "$d" 2>/dev/null; then
      emit_verdict "$C.daemon_SIGTERM" "FAIL" "signalled parent daemon $d"
    else
      emit_verdict "$C.daemon_SIGTERM" "PASS" "parent daemon $d not signalable"
    fi
  done
fi

exit 0
