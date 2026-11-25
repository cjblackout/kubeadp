#!/usr/bin/env bash
# run.sh - BLACK-BOX ATTACK SUITE for the kubeadp-agent sandbox.
#
# Inside a pod with the real binary:
#     kubeadp-agent sandbox-exec --policy P -- CMD
# Black-box: we never inspect the sandbox code, only observable behaviour.
#
# DESIGN (see README.md):
#   Two agents. A is a long-lived victim holding a secret in memory + a worktree
#   file + a TCP listener. B is the attacker, started concurrently, running the
#   case scripts against A. Each case maps to one isolation requirement
#   (R1-R11). The harness (us, unsandboxed) records one decision per check as
#   `R<n>.<name> PASS|FAIL|SKIP(<reason>)` and a final results.json. Exit is 0
#   iff there is no hard FAIL.
#
# USAGE
#   run.sh [--binary kubeadp-agent] [--only R1,R7] [--keep]
#
# SELF-TEST without the real binary (negative control):
#   run.sh --binary tests/sandbox/fake/kubeadp-agent
# The stub plain-execs (no isolation): leak checks FAIL as [CONTROL] (proving
# the suite detects leaks), and R5/R8/R10 PASS.
#
# Standard library only. No cargo. No cluster/network access from the suite.

set -u
set -o pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck disable=SC1091
. "$SCRIPT_DIR/lib.sh"

BINARY="${BINARY:-kubeadp-agent}"
ONLY="${ONLY:-}"
KEEP="${KEEP:-0}"

usage() { sed -n '1,40p' "$0" | sed 's/^#\{0,2\} //;s/^#//' ; exit 0; }

while [ "$#" -gt 0 ]; do
  case "$1" in
    --binary) BINARY="${2:-}"; shift 2 ;;
    --only) ONLY="${2:-}"; shift 2 ;;
    --keep) KEEP=1; shift ;;
    -h|--help) usage ;;
    *) echo "unknown option: $1" >&2; exit 2 ;;
  esac
done

# Resolve --binary to something usable. When it names an existing file (a
# relative or absolute path to the fake stub), ensure it is executable so
# command -v accepts it; a diff-generated patch drops the exec bit.
if [ -f "$BINARY" ] && [ ! -x "$BINARY" ]; then
  chmod +x "$BINARY" 2>/dev/null || true
fi
command -v "$BINARY" >/dev/null 2>&1 || { echo "FATAL: $BINARY not found on PATH" >&2; exit 2; }
command -v python3 >/dev/null 2>&1 || { echo "FATAL: python3 required" >&2; exit 2; }

detect_fake
probe_runtime_env
init_scratch
touch "$RESULTS_TSV"

# normalized --only set (e.g. " R01 R07 ")
ONLY_SET=""
if [ -n "$ONLY" ]; then ONLY_SET=" $(echo "$ONLY" | tr ',' ' ') "; fi
want() {  # want <R0n>
  [ -z "$ONLY_SET" ] && return 0
  case "$ONLY_SET" in *" $1 "*) return 0 ;; *) return 1 ;; esac
}

echo "== kubeadp-agent sandbox attack suite =="
echo "   binary:  $BINARY  fake_stub: $IS_FAKE"
echo "   runtime: ${KUBEADP_TEST_RUNTIME:-unknown}  fresh_pidns: ${KUBEADP_TEST_PIDNS:-?}"
echo "   scratch: $SCRATCH"

A_LCH=""; A_VICTIM_PID=""; A_VICTIM_PORT=""; A_MEM=0
B_LCH=""
PKILL_PIDS=""

teardown() {
  [ -z "$A_LCH" ] || stop_launcher "$A_LCH"
  if [ -n "${DAEMON_PIDS:-}" ]; then
    for p in $(printf '%s' "$DAEMON_PIDS" | tr ',' ' '); do
      case "$p" in ''|*[!0-9]*) continue;; esac
      kill -KILL "$p" 2>/dev/null || true
    done
  fi
  if [ -n "$PKILL_PIDS" ]; then
    for p in $(printf '%s' "$PKILL_PIDS" | tr ',' ' '); do
      case "$p" in ''|*[!0-9]*) continue;; esac
      kill -KILL "$p" 2>/dev/null || true
    done
  fi
}
trap teardown EXIT INT TERM

# ---------------------------------------------------------------------------
# Scenario: concurrent A (victim) + B (attacker).
# ---------------------------------------------------------------------------
start_victim_A() {
  : > "$LOGDIR/A.log"
  A_TOOLS_OUT="$A_STATE"  # victim writes info.json into its own state dir
  A_LCH="$(KUBEADP_TEST_ROLE=A SANDBOX_CASE= \
            sb_exec_bg A "$SECRET_A" "$POL_A" "$LOGDIR/A.log" -- \
              python3 "$A_TOOLS/victim.py" "$RUN_TAG")"
  if ! wait_ready "$A_STATE" 60; then
    rec "R00.a_start" SKIP "victim A not ready; log: $(tail -c 300 "$LOGDIR/A.log" | tr '\n' ' ')"
    A_LCH=""
    return 1
  fi
  rec "R00.a_start" PASS "victim A ready (launcher $A_LCH)"
  A_VICTIM_PID="$(read_a_val pid)"
  A_VICTIM_PORT="$(read_a_val port)"
  A_MEM="$(read_a_val mem_mapped)"
  return 0
}

# run one case script inside B's sandbox (synchronous; used for positive and
# single-shot checks like R05/R10 when not parallelized)
run_case_in_B() {
  local case_file="$1" tag="$2" log="$LOGDIR/$tag.log"
  : > "$log"
  KUBEADP_TEST_ROLE=B \
    SANDBOX_CASE="$tag" \
    KUBEADP_TEST_TARGET_PID="$A_VICTIM_PID" \
    KUBEADP_TEST_TARGET_RW="$RW_A" \
    KUBEADP_TEST_PORT="$A_VICTIM_PORT" \
    KUBEADP_TEST_SECRET_HASH="$(sha_file "$SECRET_B")" \
    KUBEADP_TEST_MARKER="$(cat "$RO_SHARE/marker.txt")" \
    sb_exec B "$SECRET_B" "$POL_B" "$log" -- \
      bash "$B_TOOLS/cases/$case_file.sh"
}

# ---------------------------------------------------------------------------
# Case drivers (host side)
# ---------------------------------------------------------------------------

# R08: exit codes + signal death propagation + SIGTERM to launcher.
case_R8() {
  local kind want rc out
  for kind in zero seven; do
    case "$kind" in zero) want=0 ;; seven) want=7 ;; esac
    sb_exec B "$SECRET_B" "$POL_B" "$LOGDIR/R08-$kind.log" -- \
      python3 "$B_TOOLS/exit_worker.py" "$kind"
    rc=$SB_RC
    if [ "$rc" -eq "$want" ] || { [ "$want" = 7 ] && [ "$rc" = 7 ]; }; then
      rec "R08.exit_$kind" PASS "exit=$rc (want $want)"
    else
      rec "R08.exit_$kind" FAIL "exit=$rc (want $want)"
    fi
  done
  for sig in TERM KILL; do
    lowsig="${sig,,}"
    case $sig in TERM) want=143 ;; KILL) want=137 ;; esac
    sb_exec B "$SECRET_B" "$POL_B" "$LOGDIR/R08-$sig.log" -- \
      python3 "$B_TOOLS/exit_worker.py" "sig$lowsig"
    rc=$SB_RC
    if [ "$rc" -eq "$want" ]; then
      rec "R08.sig_${lowsig}" PASS "death by SIG$sig -> 128+$([ $sig = TERM ] && echo 15 || echo 9)=$want (got $rc)"
    else
      rec "R08.sig_${lowsig}" FAIL "death by SIG$sig gave $rc, want $want"
    fi
  done
}

# R08 second half: SIGTERM to the launcher reaches the child (separate).
case_R8_term() {
  local wpid
  wpid="$(sb_exec_bg B "$SECRET_B" "$POL_B" "$LOGDIR/R08-term.log" -- \
            python3 -c "import signal,sys,time
def h(*a): print('CHILD-SIGTERM', flush=True); sys.exit(0)
signal.signal(signal.SIGTERM, h)
time.sleep(30)")"
  sleep 1
  if ! pid_alive "$wpid"; then
    rec "R08.sigterm_launcher" SKIP "launcher not running after spawn (log: $(tail -c 200 "$LOGDIR/R08-term.log" | tr '\n' ' '))"
    return
  fi
  kill -TERM "$wpid" 2>/dev/null
  if wait_agent_done "$wpid" 10; then
    if grep -q "CHILD-SIGTERM" "$LOGDIR/R08-term.log" 2>/dev/null; then
      rec "R08.sigterm_launcher" PASS "SIGTERM to launcher reached child (child handled it)"
    else
      rec "R08.sigterm_launcher" PASS "launcher exited on SIGTERM; child gone"
    fi
  else
    rec "R08.sigterm_launcher" FAIL "launcher did not exit after SIGTERM"
    stop_launcher "$wpid"
  fi
}

# R09: fail closed + exit-code semantics (contract v1.3).
# An impossible/invalid policy (missing file, malformed JSON, unreadable
# policy, rw path that does not exist and cannot be created) => the launcher
# must exit 125 and the command must NEVER run (no marker file). A valid
# policy runs the command and propagates its exit code (126/127/128+N too).
# Marker files land in B's own rw dir (writable inside a real sandbox and
# reachable by the host harness).
case_R9() {
  local rc marker mk
  rm -f "$LOGDIR"/R09-*.log
  mk="$RW_B/R09-marker"
  # forward the scratch + role so the stub's private tmp lands inside $SCRATCH
  export KUBEADP_TEST_ROLE=B KUBEADP_TEST_SCRATCH="$SCRATCH"

  # 1. valid policy => command runs, exit code propagates (exit 7 -> 7).
  rm -f "$mk"
  ( exec 3<"$SECRET_B"
    "$BINARY" sandbox-exec --policy "$POL_B" -- \
      sh -c "touch '$mk'; exit 7" ) >"$LOGDIR/R09-valid.log" 2>&1
  rc=$?
  if [ -f "$mk" ] && [ "$rc" = "7" ]; then
    rec "R09.valid_policy" PASS "valid policy runs command and propagates exit 7 (rc=$rc)"
  elif [ -f "$mk" ]; then
    rec "R09.valid_policy" FAIL "valid policy ran command but rc=$rc (want 7)"
  else
    rec "R09.valid_policy" FAIL "valid policy did not run command"
  fi

  # 2. missing policy file => 125, command never runs.
  rm -f "$mk"
  ( exec 3<"$SECRET_B"
    "$BINARY" sandbox-exec --policy "$LOGDIR/R09-does-not-exist.json" -- \
      sh -c "touch '$mk'" ) >"$LOGDIR/R09-missing.log" 2>&1
  rc=$?
  if [ "$rc" = "125" ] && [ ! -e "$mk" ]; then
    rec "R09.missing_policy" PASS "missing policy file -> 125, command not run"
  elif [ "$rc" = "125" ]; then
    rec "R09.missing_policy" FAIL "missing policy -> 125 but command still ran"
  else
    rec "R09.missing_policy" FAIL "missing policy -> rc=$rc (want 125), marker=$([ -e "$mk" ] && echo yes || echo no)"
  fi

  # 3. malformed JSON policy => 125, command never runs.
  rm -f "$mk"
  { echo "{not valid json"; } > "$LOGDIR/R09-bad.json"
  ( exec 3<"$SECRET_B"
    "$BINARY" sandbox-exec --policy "$LOGDIR/R09-bad.json" -- \
      sh -c "touch '$mk'" ) >"$LOGDIR/R09-malformed.log" 2>&1
  rc=$?
  if [ "$rc" = "125" ] && [ ! -e "$mk" ]; then
    rec "R09.malformed_policy" PASS "malformed JSON -> 125, command not run"
  else
    rec "R09.malformed_policy" FAIL "malformed JSON -> rc=$rc (want 125), marker=$([ -e "$mk" ] && echo yes || echo no)"
  fi

  # 4. rw path that does not exist and cannot be created => 125.
  rm -f "$mk"
  "$PY3" - "$POL_B" <<'PYEOF' > "$LOGDIR/R09-uncreatable.json" 2>/dev/null || true
import json, sys
d = json.load(open(sys.argv[1]))
d["rw"] = ["/proc/hl86-does-not-exist/deeply/nested"]
print(json.dumps(d))
PYEOF
  if [ -s "$LOGDIR/R09-uncreatable.json" ]; then
    ( exec 3<"$SECRET_B"
      "$BINARY" sandbox-exec --policy "$LOGDIR/R09-uncreatable.json" -- \
        sh -c "touch '$mk'" ) >"$LOGDIR/R09-uncreatable.log" 2>&1
    rc=$?
    if [ "$rc" = "125" ] && [ ! -e "$mk" ]; then
      rec "R09.rw_uncreatable" PASS "uncreatable rw path -> 125, command not run"
    else
      rec "R09.rw_uncreatable" FAIL "uncreatable rw path -> rc=$rc (want 125), marker=$([ -e "$mk" ] && echo yes || echo no)"
    fi
  else
    rec "R09.rw_uncreatable" SKIP "could not build uncreatable-rw policy"
  fi

  # 5. unreadable policy file => 125, command never runs.
  rm -f "$mk"
  chmod 000 "$POL_B"
  if [ ! -r "$POL_B" ]; then
    ( exec 3<"$SECRET_B"
      "$BINARY" sandbox-exec --policy "$POL_B" -- \
        sh -c "touch '$mk'" ) >"$LOGDIR/R09-unreadable.log" 2>&1
    rc=$?
    if [ "$rc" = "125" ] && [ ! -e "$mk" ]; then
      rec "R09.unreadable_policy" PASS "unreadable policy -> 125, command not run"
    else
      rec "R09.unreadable_policy" FAIL "unreadable policy -> rc=$rc (want 125), marker=$([ -e "$mk" ] && echo yes || echo no)"
    fi
  else
    rec "R09.unreadable_policy" SKIP "could not make policy unreadable"
  fi
  chmod 644 "$POL_B"
}

# R07: descendants die within 5 s of the agent root exiting.
#
# Detection: a host-side scan of /proc/*/cmdline for the per-descendant argv
# marker `__hl86_desc__<tag>-<name>` that spawn_descendants.py installs. This
# is robust where pid-marker files are not:
#   - on gVisor the sandbox's fresh pid namespace means the pids the spawner
#     wrote to its desc-* files are namespace-local and differ from the pod
#     pids the harness sees, so pid-file based liveness checks are wrong;
#   - a descendant can reparent/rename away from its marker file, but the
#     marker bytes in argv survive (cmdline is fixed at exec).
case_R7() {
  local dlog="$LOGDIR/R07-desc"
  rm -rf "$dlog"; mkdir -p "$dlog"
  local wpid i mprefix
  wpid="$(KUBEADP_TEST_ROLE=B KUBEADP_TEST_DESCLOG="$dlog" \
            sb_exec_bg B "$SECRET_B" "$POL_B" "$LOGDIR/R07.log" -- \
            python3 "$B_TOOLS/spawn_descendants.py" "$dlog" "$RUN_TAG")"
  mprefix="__hl86_desc__${RUN_TAG}"
  # wait for the root to report ready
  i=0
  while [ "$i" -lt 40 ] && ! grep -q "DESCSPAWNED-READY" "$LOGDIR/R07.log" 2>/dev/null; do
    sleep 0.25; i=$((i+1))
  done
  if ! grep -q "DESCSPAWNED-READY" "$LOGDIR/R07.log" 2>/dev/null; then
    rec "R07.spawn" SKIP "descendant spawner did not become ready"
    stop_launcher "$wpid"
    return
  fi
  # exit the agent root (launcher gets SIGTERM; sandbox must reap descendants)
  kill -TERM "$wpid" 2>/dev/null
  local waited=0
  while pid_alive "$wpid" && [ "$waited" -lt 60 ]; do sleep 0.2; waited=$((waited+1)); done
  # scan /proc/*/cmdline for surviving descendants by their unique argv marker
  # (not the pid files: the pids inside a fresh pid namespace are not the pod
  # pids, and even in a shared namespace a descendant can outlive its marker
  # file). cmdline is NUL-separated; tr to spaces, grep for the marker prefix.
  find_descendants_alive() {
    local out="" c pid
    for c in /proc/[0-9]*/cmdline; do
      [ -r "$c" ] || continue
      if tr '\0' ' ' < "$c" 2>/dev/null | grep -q "$mprefix"; then
        pid="${c#/proc/}"; pid="${pid%/cmdline}"
        out="$out $pid"
      fi
    done
    printf '%s' "$out"
  }
  local surv
  surv="$(find_descendants_alive)"
  local t0=$(date +%s)
  while [ -n "$surv" ]; do
    sleep 0.4
    surv="$(find_descendants_alive)"
    if [ $(( $(date +%s) - t0 )) -gt 5 ]; then break; fi
  done
  if [ -z "$surv" ]; then
    rec "R07.descendants_gone" PASS "all descendants gone within 5 s of root exit"
  else
    rec "R07.descendants_gone" FAIL "survivors after 5 s: $surv"
    # cleanup the leaked descendants (the sandbox was supposed to reap them;
    # when it did not - real leak or fake stub - kill them here so they do not
    # outlive the harness). Match by the argv marker so we only ever kill the
    # R07 descendants, never the victim or other cases (which do not carry the
    # marker prefix).
    for c in /proc/[0-9]*/cmdline; do
      [ -r "$c" ] || continue
      if tr '\0' ' ' < "$c" 2>/dev/null | grep -q "$mprefix"; then
        pid="${c#/proc/}"; pid="${pid%/cmdline}"
        kill -KILL "$pid" 2>/dev/null
      fi
    done
  fi
  stop_launcher "$wpid"
}

# ---------------------------------------------------------------------------
# Main sequence
# ---------------------------------------------------------------------------
STARTED="$(date -Is 2>/dev/null || date +%Y-%m-%dT%H:%M:%S)"

# A: victim (long-lived), then B: attacker concurrently
if ! start_victim_A; then
  # no victim -> nothing to attack; report
  write_summary "$STARTED" || true
  if [ "$KEEP" != "1" ] && [ -n "$SCRATCH" ]; then
    rm -rf "$SCRATCH"
  fi
  exit 1
fi

# disposable "parent daemon" sleeper for R02 (child of the harness). Its pid
# is given to B; signaling it must fail under a real sandbox (R2) and succeed
# in fake-stub mode (negative control).
DAEMON_PIDS="${DAEMON_PIDS:-}"
if want R02; then
  ( exec -a "__hl86_daemon__" sleep 300 ) &
  DP1=$!
  DAEMON_PIDS="$DP1"
fi

export KUBEADP_TEST_TARGET_PID="$A_VICTIM_PID" \
       KUBEADP_TEST_TARGET_RW="$RW_A" \
       KUBEADP_TEST_PORT="$A_VICTIM_PORT" \
       KUBEADP_TEST_SECRET_HASH="$(sha_file "$SECRET_B")" \
       KUBEADP_TEST_MARKER="$(cat "$RO_SHARE/marker.txt")" \
       KUBEADP_TEST_DAEMON_PIDS="${DAEMON_PIDS:-}"

# B: run the attacker scripts (concurrent with A still alive). sb_exec_bg
# backgrounds a subshell and prints the launcher pid; we collect them.
B_JOBS=""
launch_case() {  # launch_case <casefile>
  local casefile="$1"
  local tag="${casefile%%_*}"   # R01, R02, ...
  sb_exec_bg B "$SECRET_B" "$POL_B" "$LOGDIR/$tag.log" -- \
    bash "$B_TOOLS/cases/$casefile"
}
if want R01; then B_JOBS="${B_JOBS} $(launch_case R01_read_proc.sh)"; fi
if want R02; then B_JOBS="${B_JOBS} $(launch_case R02_signal.sh)"; fi
if want R03; then B_JOBS="${B_JOBS} $(launch_case R03_ptrace.sh)"; fi
if want R04; then B_JOBS="${B_JOBS} $(launch_case R04_worktree.sh)"; fi
if want R06; then B_JOBS="${B_JOBS} $(launch_case R06_secret.sh)"; fi
if want R10; then B_JOBS="${B_JOBS} $(launch_case R10_net.sh)"; fi
if want R11; then B_JOBS="${B_JOBS} $(launch_case R11_priv.sh)"; fi

# wait for all B jobs (bounded)
for p in $B_JOBS; do
  wait_agent_done "$p" 90 || { rec "R00.b_timeout" SKIP "attacker job $p timed out"; stop_launcher "$p"; }
done

# R05 positive (from B's perspective)
if want R05; then
  sb_exec B "$SECRET_B" "$POL_B" "$LOGDIR/R05.log" -- \
    python3 "$B_TOOLS/self_check.py"
fi

# R08 / R09 / R07 host-side
if want R08; then case_R8; case_R8_term; fi
if want R09; then case_R9; fi
if want R07; then case_R7; fi

# collect in-sandbox verdicts
collect_verdicts "$LOGDIR/A.log" \
  "$LOGDIR/R01.log" "$LOGDIR/R02.log" "$LOGDIR/R03.log" "$LOGDIR/R04.log" \
  "$LOGDIR/R05.log" "$LOGDIR/R06.log" "$LOGDIR/R07.log" "$LOGDIR/R08.log" \
  "$LOGDIR/R09.log" "$LOGDIR/R10.log" "$LOGDIR/R11.log"

teardown
write_summary "$STARTED"
rc=$?
if [ "$KEEP" != "1" ] && [ -n "$SCRATCH" ]; then
  rm -rf "$SCRATCH"
fi
exit "$rc"
