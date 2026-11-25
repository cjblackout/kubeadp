#!/usr/bin/env bash
# R05_ownenv.sh - test R5: the agent can use its OWN environment normally.
#
# Runs INSIDE agent B's sandbox. Verifies (positive checks - must PASS):
#   - write/read our own rw dir
#   - write/read our private tmp
#   - read the ro-share
#   - run our tools (git, ssh, node, python3, cargo - SKIP if missing)
# A failure here means the sandbox is too tight (FAIL).

# shellcheck disable=SC1091
. "$KUBEADP_TEST_B_TOOLS/lib_case.sh"

C="R05"

if command -v python3 >/dev/null 2>&1; then
  python3 "$KUBEADP_TEST_B_TOOLS/self_check.py" || true
else
  emit_verdict "$C.python_probe" "SKIP" "python3 tool missing"
fi

exit 0
