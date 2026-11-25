# lib_case.sh - helpers for the in-sandbox attack case scripts (agent B).
#
# Sourced by every cases/R0X_*.sh inside B's sandbox. Emits VERDICT lines.

# emit_verdict <id> <PASS|FAIL|SKIP|INFO> <message>
emit_verdict() {
  printf 'VERDICT %s %s %s\n' "$1" "$2" "$3"
}

# python3, resolved once (mirrors lib.sh's PY3).
PY3="$(command -v python3 || echo /usr/bin/python3)"

# Runtime facts derived by the harness from `sandbox-probe --json`:
#   KUBEADP_TEST_PIDNS=1  -> the sandbox gives the agent a FRESH pid namespace
#                            (gvisor, runc-namespaces). The host pid the harness
#                            placed in KUBEADP_TEST_TARGET_PID is unresolvable
#                            inside B by definition; only a same-namespace scan
#                            can find a target, and finding none is the EXPECTED
#                            isolation outcome.
#   KUBEADP_TEST_PIDNS=0  -> shared /proc (kata, runc-Landlock, fake). The host
#                            pid IS the pid B can address; metadata of other
#                            processes is visible by design (Landlock only
#                            blocks data-bearing files: environ/maps/mem).
PIDNS_FRESH="${KUBEADP_TEST_PIDNS:-0}"

# tp - target pid from env (host pid passed by the harness). With a fresh pid
# namespace the host pid is meaningless inside B, so `tp` yields nothing and
# cases must rely on the same-namespace scan (or report no_target).
tp() {
  if [ "$PIDNS_FRESH" = "1" ]; then
    printf ''
  else
    printf '%s' "${KUBEADP_TEST_TARGET_PID:-0}"
  fi
}

# no_target_verdict <id> <what> - emit a verdict for "could not find a target
# pid". Under a fresh pid namespace that is the expected isolation outcome
# (B cannot see A's pids), so it is PASS; in a shared namespace (or unknown)
# it means the scan failed, so it is SKIP.
no_target_verdict() {
  if [ "$PIDNS_FRESH" = "1" ]; then
    emit_verdict "$1" "PASS" "no target pid in own (fresh) pid namespace: $2"
  else
    emit_verdict "$1" "SKIP" "no target pid found: $2"
  fi
}

# discover_target - same-namespace scan for the victim's pid using the run tag
# (non-secret) in cmdline. Prints the pid or nothing.
discover_target() {
  local tag="${KUBEADP_TEST_TAG:-}"
  [ -n "$tag" ] || return 0
  local p dir pid
  for p in /proc/[0-9]*/cmdline; do
    [ -r "$p" ] || continue
    if tr '\0' ' ' < "$p" 2>/dev/null | grep -q "$tag"; then
      dir="$(dirname "$p")"
      printf '%s' "${dir#/proc/}"
      return 0
    fi
  done
  return 1
}

# pick_target - prefer the in-namespace scan; fall back to the env pid only
# when the namespace is shared (with a fresh pid namespace the env pid cannot
# exist inside B).
pick_target() {
  local s
  s="$(discover_target)"
  if [ -n "$s" ]; then
    printf '%s' "$s"
  else
    tp
  fi
}

# metadata_visible - true when this runtime shares /proc with the pod's other
# processes (kata or runc-Landlock): the agent can list/stat/readlink other
# processes' entry dirs and read their cmdline, but the data-bearing files
# (environ, maps, mem) stay blocked by the Landlock domain. Used to downgrade
# purely-metadata R01 observations from FAIL to the documented-INFO level
# rather than failing a real, correctly-isolated kata pod.
metadata_visible() {
  [ "${KUBEADP_TEST_PIDNS:-0}" = "0" ] \
    && [ "${KUBEADP_TEST_RUNTIME:-}" != "fake" ]
}
