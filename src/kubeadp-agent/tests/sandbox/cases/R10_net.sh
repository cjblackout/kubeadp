#!/usr/bin/env bash
# R10_net.sh - test R10: network stays shared; B can reach a TCP port A
# listens on (127.0.0.1).
#
# Runs INSIDE agent B's sandbox. Connects to the victim's TCP listener using
# the host port the harness discovered (KUBEADP_TEST_PORT). This is a POSITIVE
# check: a successful connect => PASS (shared net required).

# shellcheck disable=SC1091
. "$KUBEADP_TEST_B_TOOLS/lib_case.sh"

C="R10"
PORT="${KUBEADP_TEST_PORT:-0}"

if [ "$PORT" = "0" ] || [ -z "$PORT" ]; then
  emit_verdict "$C.tcp_connect" "SKIP" "no victim port supplied"
  exit 0
fi

if command -v python3 >/dev/null 2>&1; then
  python3 "$KUBEADP_TEST_B_TOOLS/net_probe.py" "$PORT" "hl86-listener-OK" || true
else
  # fallback: bash /dev/tcp
  if timeout 5 bash -c "echo > /dev/tcp/127.0.0.1/$PORT" 2>/dev/null; then
    emit_verdict "$C.tcp_connect" "PASS" "connected to 127.0.0.1:$PORT via bash /dev/tcp"
  else
    emit_verdict "$C.tcp_connect" "FAIL" "cannot connect to 127.0.0.1:$PORT"
  fi
fi

exit 0
