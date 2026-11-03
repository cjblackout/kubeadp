#!/usr/bin/env bash
# R01_read_proc.sh - test isolation R1.
#
# Runs INSIDE agent B's sandbox. Tries to read the OTHER agent's (victim A)
# /proc entries: environ, maps, mem, fd/*, root, cwd, task/*/environ, cmdline
# (cmdline is only a documented/INFO check). Also uses os.open() and cat(1).
#
# Reads the target pid and worktree paths from the TEST_* env the harness
# passes. Every successful read is a leak => FAIL (unless the harness runs in
# fake-stub mode, in which case FAIL is the expected negative control).

# shellcheck disable=SC1091
. "$KUBEADP_TEST_B_TOOLS/lib_case.sh"

C="R01"
TPID="$(pick_target)"
B_TOOLS="${KUBEADP_TEST_B_TOOLS:-.}"

if [ -z "$TPID" ] || [ "$TPID" = "0" ]; then
  no_target_verdict "$C.discover" "no target pid (fresh pid namespace = isolation, shared = scan failed)"
  # still exercise /proc scan to be thorough (with no target, the probes
  # exercise try-fail paths on pid 0 / unresolvable pids)
fi

# python probe covers environ/readlink/mem/task/fd via multiple access paths
if command -v python3 >/dev/null 2>&1; then
  python3 "$B_TOOLS/probe_mem.py" "$TPID" || true
  python3 "$B_TOOLS/probe_procfs.py" "$TPID" || true
else
  emit_verdict "$C.python_probe" "SKIP" "python3 tool missing"
fi

# cat(1) direct read of victim environ/maps (the simplest leak vector)
if ! command -v cat >/dev/null 2>&1; then
  emit_verdict "$C.cat" "SKIP" "cat missing"
else
  # victim environ via /proc
  out="$(cat "/proc/$TPID/environ" 2>&1)"
  rc=$?
  if [ -n "$out" ] && [ "$rc" = "0" ]; then
    emit_verdict "$C.cat_environ" "FAIL" "cat read victim /proc/environ ($(echo -n "$out" | wc -c) bytes)"
  elif [ "$rc" = "0" ]; then
    emit_verdict "$C.cat_environ" "PASS" "cat read empty environ (no leak data)"
  else
    emit_verdict "$C.cat_environ" "PASS" "cat blocked ($([ -n "$out" ] && echo "$out" | head -c 40 || echo EACCES))"
  fi

  # victim maps via /proc
  out="$(cat "/proc/$TPID/maps" 2>&1)"
  rc=$?
  if [ -n "$out" ] && [ "$rc" = "0" ]; then
    emit_verdict "$C.cat_maps" "FAIL" "cat read victim maps ($(echo -n "$out" | wc -l) lines)"
  elif [ "$rc" = "0" ]; then
    emit_verdict "$C.cat_maps" "PASS" "cat read empty maps"
  else
    emit_verdict "$C.cat_maps" "PASS" "cat blocked"
  fi
fi

# cmdline is a DOCUMENTED accepted difference (visible proc lists on
# kata/runc-Landlock); never FAILs, it is a read-only observation.
if [ -r "/proc/$TPID/cmdline" ]; then
  emit_verdict "$C.cmdline" "INFO" "victim cmdline visible (documented accepted difference)"
else
  emit_verdict "$C.cmdline" "PASS" "victim cmdline not visible"
fi

exit 0
