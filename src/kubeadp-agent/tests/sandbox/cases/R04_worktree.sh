#!/usr/bin/env bash
# R04_worktree.sh - R4: cannot read/write/list another agent's worktree or
# private tmp; /tmp of others unreadable.
#
# Runs INSIDE agent B's sandbox. Attempts several vectors against agent A's
# worktree and private tmp:
#   - direct host path (env), which on a real sandbox is hidden (ENOENT)
#   - /proc/<pid>/root traversal inside B to reach A's private tmp
#   - shared /tmp + /dev/shm enumeration (INFO on kata where they are masked)
# A successful read/write/list => FAIL (leak).

# shellcheck disable=SC1091
. "$KUBEADP_TEST_B_TOOLS/lib_case.sh"

C="R04"
TPID="$(pick_target)"
VICTIM_RW="${KUBEADP_TEST_TARGET_RW:-}"

if [ -z "$TPID" ] || [ "$TPID" = "0" ]; then
  emit_verdict "$C.discover" "SKIP" "no target pid visible"
fi

if command -v python3 >/dev/null 2>&1; then
  python3 "$KUBEADP_TEST_B_TOOLS/probe_worktree.py" "$TPID" "$VICTIM_RW" || true
else
  emit_verdict "$C.probe" "SKIP" "python3 tool missing"
fi

exit 0
