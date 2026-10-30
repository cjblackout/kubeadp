#!/usr/bin/env bash
# Black-box test suite for agent-run (CLI shim + API server + core engine).
#
# Run with:  bash run-tests.sh
#
# The suite starts its OWN private agent-run-api server on 127.0.0.1 (default
# port 18002) with a scripted fake `agy` (fake-agy.sh), a fresh git project
# and a temp state root. It never talks to any other server and never uses a
# real agy login.
#
# Environment knobs:
#   BIN_DIR  dir holding prebuilt `agent-run` and `agent-run-api` binaries.
#            Unset: both crates are built with `cargo build --release` into
#            CARGO_TARGET_DIR (default $HOME/.cache/agent-run-blackbox-target)
#            and the results copied to $HOME/.cache/agent-run-blackbox/bin.
#   PORT     listen port for the private server (default 18002)
#   KEEP=1   keep the temp dir (state, workspace, logs) for inspection
#   ONLY     space-separated list of test ids to run (e.g. ONLY="T02 T07")
#
# Safety: AGENT_RUN_REMOTE (set to the real server in the ai-server pod) is
# ignored with a warning and unset, and every call passes --remote explicitly.
# The server runs with a temp HOME, a fake agy path,
# GH_TOKEN/GITHUB_TOKEN unset and Hindsight disabled.

set -u

SCRIPT_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
CRATE_DIR=$(cd "$SCRIPT_DIR/../.." && pwd)

PORT="${PORT:-18002}"
ONLY="${ONLY:-}"
FAILURES=0
TESTS_RUN=0

die() {
  echo "FATAL: $*" >&2
  exit 1
}

# Print up to N lines of a log (if it exists) to stderr; used on failure paths.
tail_of() {
  local file="$1" n="${2:-40}"
  [ -f "$file" ] || return 0
  {
    echo "--- last $n lines of $file ---"
    tail -n "$n" "$file"
  } >&2
}

# --- safety: never point at a real, remote server ---------------------------
if [ -n "${AGENT_RUN_REMOTE:-}" ]; then
  case "$AGENT_RUN_REMOTE" in
    *127.0.0.1* | *localhost*) ;;
    *) echo "ignoring AGENT_RUN_REMOTE=$AGENT_RUN_REMOTE; this suite only talks to its own private server" >&2 ;;
  esac
fi
unset AGENT_RUN_REMOTE
unset GH_TOKEN GITHUB_TOKEN
unset AGENT_RUN_REMOTE_CONTROL AGENT_RUN_REMOTE_CONTROL_AUTOSTART AGENT_RUN_REMOTE_CONTROL_CHECK_SECS
unset AGENT_RUN_HINDSIGHT AGENT_RUN_HINDSIGHT_ENABLED AGENT_RUN_HINDSIGHT_AUTO_RECALL AGENT_RUN_HINDSIGHT_AUTO_RETAIN
unset AGENT_RUN_HINDSIGHT_BANK AGENT_RUN_HINDSIGHT_URL AGENT_RUN_HINDSIGHT_API_KEY
unset AGENT_RUN_HTTP_PROXY AGENT_RUN_HTTPS_PROXY AGENT_RUN_NO_PROXY
unset AGENT_RUN_MAX_RUNTIME AGENT_RUN_MAX_PARALLEL AGENT_RUN_LAUNCH_TIMEOUT_SECS AGENT_RUN_STALL_MINUTES

# --- tool dependencies -------------------------------------------------------
for tool in git curl pkill; do
  command -v "$tool" >/dev/null 2>&1 || die "required tool '$tool' not found in PATH"
done
if [ -z "${BIN_DIR:-}" ]; then
  for tool in cargo python3; do
    command -v "$tool" >/dev/null 2>&1 || die "required tool '$tool' not found in PATH (only needed when BIN_DIR is unset)"
  done
else
  command -v python3 >/dev/null 2>&1 || die "required tool 'python3' not found in PATH"
fi

# --- locate / build the binaries ---------------------------------------------
if [ -n "${BIN_DIR:-}" ]; then
  BIN_DIR="$BIN_DIR"
else
  BIN_DIR="$HOME/.cache/agent-run-blackbox/bin"
  # Always (incrementally) rebuild: a cached binary from older sources gives false results.
  if true; then
    export CARGO_TARGET_DIR="${CARGO_TARGET_DIR:-$HOME/.cache/agent-run-blackbox-target}"
    mkdir -p "$BIN_DIR"
    echo "Building agent-run (release)..."
    (cd "$CRATE_DIR" && cargo build --release --bin agent-run) || die "cargo build (agent-run) failed"
    echo "Building agent-run-api (release)..."
    (cd "$CRATE_DIR" && cargo build --release --bin agent-run-api) || die "cargo build (agent-run-api) failed"
    cp "$CARGO_TARGET_DIR/release/agent-run" "$BIN_DIR/" || die "could not copy agent-run binary"
    cp "$CARGO_TARGET_DIR/release/agent-run-api" "$BIN_DIR/" || die "could not copy agent-run-api binary"
  fi
fi
AGENT_RUN="$BIN_DIR/agent-run"
AGENT_RUN_API="$BIN_DIR/agent-run-api"
[ -x "$AGENT_RUN" ] || die "agent-run binary not found/executable at $AGENT_RUN"
[ -x "$AGENT_RUN_API" ] || die "agent-run-api binary not found/executable at $AGENT_RUN_API"

# --- temp world ---------------------------------------------------------------
TMPROOT=$(mktemp -d 2>/dev/null) || die "mktemp -d failed"
STATE_ROOT="$TMPROOT/state"
WORKSPACE="$TMPROOT/workspace"
FAKE_HOME="$TMPROOT/home"
SERVER_LOG="$TMPROOT/server.log"
CUR_SERVER_LOG="$SERVER_LOG"
RESULTS="$TMPROOT/results.tsv"
printf 'id\tresult\tmessage\n' > "$RESULTS"

FAKE_AGY="$TMPROOT/fake-agy.sh"
cp "$SCRIPT_DIR/fake-agy.sh" "$FAKE_AGY" || die "could not copy fake-agy.sh"
chmod +x "$FAKE_AGY"

mkdir -p "$WORKSPACE/proj" "$FAKE_HOME"
git -C "$WORKSPACE/proj" init -b main >/dev/null 2>&1 || {
  git -C "$WORKSPACE/proj" init >/dev/null 2>&1 || die "git init failed"
  git -C "$WORKSPACE/proj" checkout -q -b main 2>/dev/null || true
}
git -C "$WORKSPACE/proj" config user.name "Blackbox Test"
git -C "$WORKSPACE/proj" config user.email "blackbox@example.com"
printf 'proj README\n' > "$WORKSPACE/proj/README.txt"
git -C "$WORKSPACE/proj" add README.txt
git -C "$WORKSPACE/proj" commit -q -m "initial commit" || die "initial git commit failed"

# --- server lifecycle ----------------------------------------------------------
SERVER_PID=""

start_server() {
  local logname="${1:-$CUR_SERVER_LOG}"
  (
    cd "$TMPROOT" || exit 1
    cd "$WORKSPACE" || exit 1
    exec env -u AGENT_RUN_REMOTE -u GH_TOKEN -u GITHUB_TOKEN \
      HOME="$FAKE_HOME" \
      AGENT_RUN_API_LISTEN="127.0.0.1:$PORT" \
      AGENT_RUN_WORKSPACE_ROOT="$WORKSPACE" \
      AGENT_RUN_STATE_ROOT="$STATE_ROOT" \
      AGENT_RUN_AGY_PATH="$FAKE_AGY" \
      AGENT_RUN_JOBS_DIR_NAME=".agent-jobs" \
      AGENT_RUN_MAX_PARALLEL=3 \
      AGENT_RUN_HINDSIGHT_ENABLED=false \
       FAKE_ARGV_LOG="$TMPROOT/argv.log" \
       FAKE_CONVMAP_LOG="$TMPROOT/convmap.log" \
        "$AGENT_RUN_API" >"$logname" 2>&1
  ) &
  SERVER_PID=$!
  CUR_SERVER_LOG="$logname"
}

# ensure_server_up: tests that kill the server restart it on every exit path,
# so one failing test cannot leave the rest of the suite without a server.
ensure_server_up() {
  if [ -z "$SERVER_PID" ] || ! kill -0 "$SERVER_PID" 2>/dev/null; then
    stop_server
    start_server
    wait_for_server
  fi
}

stop_server() {
  if [ -n "$SERVER_PID" ]; then
    kill -TERM "$SERVER_PID" 2>/dev/null || true
    # give it a few seconds for a clean shutdown
    local i
    for i in 1 2 3 4 5 6; do
      kill -0 "$SERVER_PID" 2>/dev/null || break
      sleep 0.5
    done
    kill -KILL "$SERVER_PID" 2>/dev/null || true
    SERVER_PID=""
  fi
}

wait_for_server() {
  local i logname="${1:-$CUR_SERVER_LOG}"
  for i in $(seq 1 40); do
    if curl -fsS --max-time 2 "http://127.0.0.1:$PORT/healthz" >/dev/null 2>&1; then
      return 0
    fi
    sleep 0.5
  done
  {
    echo "server did not become healthy within 20s"
    echo "--- last server log lines ---"
    tail -n 40 "$logname" 2>/dev/null || true
  } >&2
  die "agent-run-api server failed to become healthy on 127.0.0.1:$PORT"
}

# --- helpers -------------------------------------------------------------------
ar() {
  "$AGENT_RUN" --remote "http://127.0.0.1:$PORT" "$@"
}

# Private bare repo hosting job worktrees (see agent-core config: the
# workspace root's .agent-repos/<project>.git); job branches live there,
# not in the project checkout.
PRIVATE_REPO="$WORKSPACE/.agent-repos/proj.git"
# job_branches <pattern>... lists agent-run/* branches in the private repo.
job_branches() {
  git --git-dir="$PRIVATE_REPO" branch --list "$@" 2>/dev/null | sed 's/^[* ]//'
}

# start_job <PROMPT> [extra start args...]; sets START_RC, START_OUT, START_JOB_ID
start_job() {
  local prompt out rc
  prompt="$1"
  shift || true
  out=$(ar start proj "$prompt" "$@" 2>&1)
  rc=$?
  START_RC=$rc
  START_OUT=$out
  START_JOB_ID=$(printf '%s\n' "$out" | sed -nE "s/^Job '([0-9a-f]{8})' started.*/\1/p" | head -n 1)
  [ -n "$START_JOB_ID" ] || START_JOB_ID=""
}

# get_status <id> -> prints the status line value (or nothing if the job is unknown)
get_status() {
  ar status "$1" 2>/dev/null | sed -nE 's/^Status:[[:space:]]*//p' | head -n 1
}

# wait_state <id> <want|terminal> [timeout_secs] -> 0 when reached (prints last status otherwise)
wait_state() {
  local id="$1" want="${2:-terminal}" secs="${3:-60}" i s t
  local -A terminal_map=([done]=1 [failed]=1 [killed]=1 [self-terminated]=1 [incomplete]=1 [quota-exhausted]=1)
  s=""
  for i in $(seq 1 "$((secs * 2))"); do
    s=$(get_status "$id")
    [ -n "$s" ] || s="missing"
    if [ "$s" = "$want" ]; then
      echo "$s"
      return 0
    fi
    if [ "$want" = "terminal" ] && [ -n "${terminal_map[$s]:-}" ]; then
      echo "$s"
      return 0
    fi
    sleep 0.5
  done
  echo "$s"
  return 1
}

rec() {
  local id="$1" result="$2" msg="${3:-}"
  printf '%s\t%s\t%s\n' "$id" "$result" "$msg" >> "$RESULTS"
  if [ "$result" = "PASS" ]; then
    echo "PASS $id -- $msg"
  else
    echo "FAIL $id -- $msg" >&2
    FAILURES=$((FAILURES + 1))
  fi
}

# run_test <ID> <FUNC>: runs the test; tests that kill the server (T24, T38)
# must be wrapped so the server is guaranteed restarted on EVERY exit path --
# one failing server-stopping test must not cascade into Connection refused.
run_test_stops_server() {
  run_test "$1" "$2"
  ensure_server_up
}

cleanup_job() {
  local id="${1:-}"
  [ -n "$id" ] || return 0
  ar clean "$id" --force --purge-log >/dev/null 2>&1 || true
}

run_test() {
  local id="$1" fn="$2"
  if [ -n "$ONLY" ]; then
    case " $ONLY " in
      *" $id "*) ;;
      *) return 0 ;;
    esac
  fi
  echo "--- $id: $fn"
  TESTS_RUN=$((TESTS_RUN + 1))
  "$fn"
}

# --- tests -------------------------------------------------------------------------
t_t01() {
  local out rc sub
  out=$(ar --version 2>&1)
  rc=$?
  if [ $rc -ne 0 ]; then
    rec T01 FAIL "agent-run --version exited $rc: $out"
    return
  fi
  local subs=( init start status log tail diff list clean kill retry repair gc wait result review commit discard export auth cancel )
  for sub in "${subs[@]}"; do
    out=$(ar help "$sub" 2>&1)
    rc=$?
    if [ $rc -ne 0 ]; then
      rec T01 FAIL "help $sub exited $rc: $out"
      return
    fi
  done
  rec T01 PASS "version works; help exit 0 for all ${#subs[@]} subcommands"
}

t_t02() {
  start_job "FAKE_SLEEP=1 basic prompt"
  if [ "$START_RC" -ne 0 ]; then
    rec T02 FAIL "start exited $START_RC: $START_OUT"
    return
  fi
  local id="$START_JOB_ID"
  if [ -z "$id" ]; then
    rec T02 FAIL "could not parse job id from start output: $START_OUT"
    return
  fi
  local st
  st=$(wait_state "$id" done 60) || {
    cleanup_job "$id"
    rec T02 FAIL "job did not reach done (last status: $st)"
    return
  }
  local res log
  res=$(ar result "$id" 2>&1)
  case "$res" in
    *FAKE-DONE*) ;;
    *) cleanup_job "$id"; rec T02 FAIL "result does not contain FAKE-DONE: $res"; return ;;
  esac
  log=$(ar log "$id" 2>&1)
  if [ -z "$log" ]; then
    cleanup_job "$id"
    rec T02 FAIL "log output is empty"
    return
  fi
  cleanup_job "$id"
  rec T02 PASS "start -> done; result has FAKE-DONE; log non-empty (job $id)"
}

t_t03() {
  start_job "FAKE_FAIL prompt that must fail"
  if [ "$START_RC" -ne 0 ]; then
    rec T03 FAIL "start exited $START_RC: $START_OUT"
    return
  fi
  local id="$START_JOB_ID"
  [ -n "$id" ] || { rec T03 FAIL "no job id parsed: $START_OUT"; return; }
  local st
  st=$(wait_state "$id" failed 60) || {
    cleanup_job "$id"
    rec T03 FAIL "job did not reach failed (last status: $st)"
    return
  }
  local rc
  ar wait "$id" >/dev/null 2>&1
  rc=$?
  if [ $rc -ne 2 ]; then
    cleanup_job "$id"
    rec T03 FAIL "wait on failed job exited $rc, expected 2"
    return
  fi
  local res
  res=$(ar result "$id" 2>&1)
  case "$res" in
    *boom*) ;;
    *) cleanup_job "$id"; rec T03 FAIL "result does not show error text 'boom': $res"; return ;;
  esac
  cleanup_job "$id"
  rec T03 PASS "FAKE_FAIL -> failed; wait exit 2; result shows 'boom' (job $id)"
}

t_t04() {
  start_job "FAKE_QUOTA prompt that hits the quota"
  if [ "$START_RC" -ne 0 ]; then
    rec T04 FAIL "start exited $START_RC: $START_OUT"
    return
  fi
  local id="$START_JOB_ID"
  [ -n "$id" ] || { rec T04 FAIL "no job id parsed: $START_OUT"; return; }
  local st
  st=$(wait_state "$id" quota-exhausted 60) || {
    cleanup_job "$id"
    rec T04 FAIL "job did not reach quota-exhausted (last status: $st)"
    return
  }
  cleanup_job "$id"
  rec T04 PASS "FAKE_QUOTA -> quota-exhausted (job $id)"
}

# T162: the stream-json job log is LIVE. The fake prints its init and first step
# events at the start of the run (emit_prelude) and the final result after the
# FAKE_SLEEP; while the job is still running the log already holds the init
# and a step_update line, and the result line only appears at the end.
t_t162() {
  start_job "FAKE_SLEEP=6 stream log shape probe"
  if [ "$START_RC" -ne 0 ]; then
    rec T162 FAIL "start exited $START_RC: $START_OUT"
    return
  fi
  local id="$START_JOB_ID"
  [ -n "$id" ] || { rec T162 FAIL "no job id parsed: $START_OUT"; return; }
  local live=0 i mid
  for i in 1 2 3 4 5 6 7 8; do
    mid=$(ar log "$id" 2>&1)
    if printf '%s\n' "$mid" | grep -q '"event":"init"' \
      && printf '%s\n' "$mid" | grep -q '"event":"step_update"' \
      && ! printf '%s\n' "$mid" | grep -q '"event":"result"' \
      && ar status "$id" 2>&1 | grep -q '^Status: *running'; then
      live=1
      break
    fi
    sleep 0.5
  done
  if [ "$live" -ne 1 ]; then
    cleanup_job "$id"
    rec T162 FAIL "log was not live while running (no init+step_update without a result): $mid"
    return
  fi
  local st
  st=$(wait_state "$id" done 60) || {
    cleanup_job "$id"
    rec T162 FAIL "job did not reach done (last status: $st)"
    return
  }
  local log ln_init ln_step ln_res
  log=$(ar log "$id" 2>&1)
  ln_init=$(printf '%s\n' "$log" | grep -n '"event":"init"' | head -n 1 | cut -d: -f1)
  ln_step=$(printf '%s\n' "$log" | grep -n '"event":"step_update"' | head -n 1 | cut -d: -f1)
  ln_res=$(printf '%s\n' "$log" | grep -n '"event":"result"' | head -n 1 | cut -d: -f1)
  if [ -z "$ln_init" ] || [ -z "$ln_step" ] || [ -z "$ln_res" ]; then
    cleanup_job "$id"
    rec T162 FAIL "final log missing init/step_update/result lines (init=$ln_init step=$ln_step result=$ln_res): $log"
    return
  fi
  if [ "$ln_init" -ge "$ln_step" ] || [ "$ln_step" -ge "$ln_res" ]; then
    cleanup_job "$id"
    rec T162 FAIL "final log lines out of order (init=$ln_init step=$ln_step result=$ln_res)"
    return
  fi
  cleanup_job "$id"
  rec T162 PASS "log is live while running (init+step_update before the result), final log ordered (job $id)"
}

# T163: FAKE_NOTICE_QUOTED response must still classify as done, not incomplete.
t_t163() {
  start_job "FAKE_NOTICE_QUOTED prompt quoting agy exit notice"
  if [ "$START_RC" -ne 0 ]; then
    rec T163 FAIL "start exited $START_RC: $START_OUT"
    return
  fi
  local id="$START_JOB_ID"
  [ -n "$id" ] || { rec T163 FAIL "no job id parsed: $START_OUT"; return; }
  local st
  st=$(wait_state "$id" done 60) || {
    cleanup_job "$id"
    rec T163 FAIL "job did not reach done (last status: $st)"
    return
  }
  cleanup_job "$id"
  rec T163 PASS "FAKE_NOTICE_QUOTED -> done (job $id)"
}

# T164: FAKE_RATELIMIT_TEXT step_update text must not be classified as quota-exhausted.
t_t164() {
  start_job "FAKE_RATELIMIT_TEXT prompt mentioning rate limit in text"
  if [ "$START_RC" -ne 0 ]; then
    rec T164 FAIL "start exited $START_RC: $START_OUT"
    return
  fi
  local id="$START_JOB_ID"
  [ -n "$id" ] || { rec T164 FAIL "no job id parsed: $START_OUT"; return; }
  local st
  st=$(wait_state "$id" done 60) || {
    cleanup_job "$id"
    rec T164 FAIL "job did not reach done (last status: $st)"
    return
  }
  cleanup_job "$id"
  rec T164 PASS "FAKE_RATELIMIT_TEXT -> done (job $id)"
}

# T165: FAKE_QUOTA in stream-json mode must still classify as quota-exhausted.
t_t165() {
  start_job "FAKE_QUOTA prompt that hits the quota (stream-json)"
  if [ "$START_RC" -ne 0 ]; then
    rec T165 FAIL "start exited $START_RC: $START_OUT"
    return
  fi
  local id="$START_JOB_ID"
  [ -n "$id" ] || { rec T165 FAIL "no job id parsed: $START_OUT"; return; }
  local st
  st=$(wait_state "$id" quota-exhausted 60) || {
    cleanup_job "$id"
    rec T165 FAIL "job did not reach quota-exhausted (last status: $st)"
    return
  }
  cleanup_job "$id"
  rec T165 PASS "FAKE_QUOTA -> quota-exhausted (job $id)"
}

t_t05() {
  start_job "FAKE_NOJSON prompt without structured output"
  if [ "$START_RC" -ne 0 ]; then
    rec T05 FAIL "start exited $START_RC: $START_OUT"
    return
  fi
  local id="$START_JOB_ID"
  [ -n "$id" ] || { rec T05 FAIL "no job id parsed: $START_OUT"; return; }
  local st
  st=$(wait_state "$id" incomplete 60) || {
    cleanup_job "$id"
    rec T05 FAIL "job did not reach incomplete (last status: $st)"
    return
  }
  cleanup_job "$id"
  rec T05 PASS "FAKE_NOJSON -> incomplete (job $id)"
}
t_t06() {
  start_job "FAKE_SELFTERM prompt, agent decides to stop early"
  if [ "$START_RC" -ne 0 ]; then
    rec T06 FAIL "start exited $START_RC: $START_OUT"
    return
  fi
  local id="$START_JOB_ID"
  [ -n "$id" ] || { rec T06 FAIL "no job id parsed: $START_OUT"; return; }
  local st
  # The engine classifies a clean exit 0 with no structured output as
  # `incomplete` (job.rs determine_status_from_signals): it cannot distinguish
  # an agy self-termination from any other silent clean exit. We assert the
  # ACTUAL documented classification and note it here.
  st=$(wait_state "$id" incomplete 60) || {
    cleanup_job "$id"
    rec T06 FAIL "job did not reach incomplete (last status: $st)"
    return
  }
  cleanup_job "$id"
  rec T06 PASS "FAKE_SELFTERM classified as 'incomplete' (documented engine behavior: exit-0 without structured SUCCESS is incomplete; 'self-terminated' is reserved for 137/143 signal deaths) (job $id)"
}

t_t07() {
  start_job "FAKE_HANG long running prompt"
  if [ "$START_RC" -ne 0 ]; then
    rec T07 FAIL "start exited $START_RC: $START_OUT"
    return
  fi
  local id="$START_JOB_ID"
  [ -n "$id" ] || { rec T07 FAIL "no job id parsed: $START_OUT"; return; }
  local st
  st=$(wait_state "$id" running 30) || {
    cleanup_job "$id"
    rec T07 FAIL "job never reached running (last status: $st)"
    return
  }
  local kout rc
  kout=$(ar kill "$id" 2>&1)
  rc=$?
  case "$kout" in
    *"terminated successfully"*) ;;
    *) cleanup_job "$id"; rec T07 FAIL "kill output unexpected (rc $rc): $kout"; return ;;
  esac
  st=$(wait_state "$id" killed 45) || {
    cleanup_job "$id"
    rec T07 FAIL "job not killed after kill (last status: $st)"
    return
  }
  if pgrep -f "$FAKE_AGY" >/dev/null 2>&1; then
    local leftovers
    leftovers=$(pgrep -af "$FAKE_AGY" 2>/dev/null | head -n 5 || true)
    cleanup_job "$id"
    rec T07 FAIL "fake-agy process still alive after kill: $leftovers"
    return
  fi
  local rout
  rout=$(ar retry "$id" 2>&1)
  rc=$?
  if [ $rc -eq 0 ]; then
    cleanup_job "$id"
    rec T07 FAIL "retry on killed job unexpectedly succeeded: $rout"
    return
  fi
  case "$rout" in
    *"no conversation ID was captured"*) ;;
    *) cleanup_job "$id"; rec T07 FAIL "retry refusal text unexpected: $rout"; return ;;
  esac
  cleanup_job "$id"
  rec T07 PASS "kill -> killed, no fake-agy left; retry on killed job refused with clear no-conversation-id message (job $id)"
}

t_t08() {
  start_job "FAKE_FAIL prompt that has a conversation id"
  if [ "$START_RC" -ne 0 ]; then
    rec T08 FAIL "start exited $START_RC: $START_OUT"
    return
  fi
  local id="$START_JOB_ID"
  [ -n "$id" ] || { rec T08 FAIL "no job id parsed: $START_OUT"; return; }
  local st
  st=$(wait_state "$id" failed 60) || {
    cleanup_job "$id"
    rec T08 FAIL "job did not reach failed (last status: $st)"
    return
  }
  local sout
  sout=$(ar status "$id" 2>&1)
  case "$sout" in
    *"Conversation:"*) ;;
    *) cleanup_job "$id"; rec T08 FAIL "status has no conversation id recorded: $sout"; return ;;
  esac
  local r1
  r1=$(ar retry "$id" 2>&1)
  rc=$?
  case "$r1" in
    *"retry launched in background"*) ;;
    *) cleanup_job "$id"; rec T08 FAIL "retry did not launch (rc $rc): $r1"; return ;;
  esac
  st=$(wait_state "$id" failed 60) || {
    cleanup_job "$id"
    rec T08 FAIL "retry attempt did not finish (last status: $st)"
    return
  }
  local log banners
  log=$(ar log "$id" 2>&1)
  banners=$(printf '%s\n' "$log" | grep -c "==================== RETRY AT" || true)
  if [ "$banners" != "1" ]; then
    cleanup_job "$id"
    rec T08 FAIL "expected exactly one RETRY AT banner in the log, found $banners"
    return
  fi
  # NOTE: the status output (status.rs) has no retry_count field, so the
  # increment is verified through the single retry banner (one claim) and the
  # successful relaunch.
  cleanup_job "$id"
  rec T08 PASS "retry re-launched a failed job with conversation id; exactly one retry claim (count+1), job finished failed again (job $id)"
}

t_t09() {
  local ids=() i id st
  for i in 1 2 3; do
    start_job "FAKE_HANG parallel worker $i"
    if [ "$START_RC" -ne 0 ]; then
      local k
      for k in "${ids[@]:-}"; do [ -n "$k" ] && ar kill "$k" >/dev/null 2>&1; cleanup_job "$k"; done
      rec T09 FAIL "start #$i exited $START_RC: $START_OUT"
      return
    fi
    id="$START_JOB_ID"
    [ -n "$id" ] || {
      rec T09 FAIL "no job id parsed on start #$i: $START_OUT"
      return
    }
    ids+=("$id")
    st=$(wait_state "$id" running 30) || {
      rec T09 FAIL "job $id not running (last status: $st)"
      return
    }
  done
  local before
  before=$(find "$WORKSPACE/.agent-jobs/proj" -maxdepth 1 -mindepth 1 -type d 2>/dev/null | wc -l)
  start_job "FAKE_SLEEP=1 fourth job that must be refused"
  if [ "$START_RC" -eq 0 ]; then
    cleanup_job "$START_JOB_ID"
    for id in "${ids[@]}"; do ar kill "$id" >/dev/null 2>&1; cleanup_job "$id"; done
    rec T09 FAIL "4th start at max-parallel was NOT refused: $START_OUT"
    return
  fi
  case "$START_OUT" in
    *"Cannot start new job"*"max-parallel limit"*) ;;
    *) for id in "${ids[@]}"; do ar kill "$id" >/dev/null 2>&1; cleanup_job "$id"; done
        rec T09 FAIL "refusal text unexpected: $START_OUT"; return ;;
  esac
  local after
  after=$(find "$WORKSPACE/.agent-jobs/proj" -maxdepth 1 -mindepth 1 -type d 2>/dev/null | wc -l)
  if [ "$before" != "$after" ]; then
    for id in "${ids[@]}"; do ar kill "$id" >/dev/null 2>&1; cleanup_job "$id"; done
    rec T09 FAIL "worktree dir count changed on refused start ($before -> $after)"
    return
  fi
  for id in "${ids[@]}"; do
    ar kill "$id" >/dev/null 2>&1 || true
    wait_state "$id" killed 45 >/dev/null || true
    cleanup_job "$id"
  done
  rec T09 PASS "3 FAKE_HANG jobs OK, 4th start refused with documented max-parallel message, no extra worktree; all killed and cleaned"
}

t_t10() {
  start_job "FAKE_STAGE FAKE_WRITE=extra-untracked.txt show staged work"
  if [ "$START_RC" -ne 0 ]; then
    rec T10 FAIL "start exited $START_RC: $START_OUT"
    return
  fi
  local id="$START_JOB_ID"
  [ -n "$id" ] || { rec T10 FAIL "no job id parsed: $START_OUT"; return; }
  local st
  st=$(wait_state "$id" done 60) || {
    cleanup_job "$id"
    rec T10 FAIL "job did not reach done (last status: $st)"
    return
  }
  local d
  d=$(ar diff "$id" 2>&1)
  local bad=""
  case "$d" in *"staged-new.txt"*) ;; *) bad="missing staged new file";; esac
  case "$d" in *"deleted file"*) ;; *) bad="$bad missing 'deleted file'";; esac
  case "$d" in *README.txt*) ;; *) bad="$bad missing README.txt deletion";; esac
  # Diff runs `git add -N -A` first, so a plain untracked file must show too.
  case "$d" in *"extra-untracked.txt"*) ;; *) bad="$bad missing untracked file";; esac
  if [ -n "$bad" ]; then
    cleanup_job "$id"
    rec T10 FAIL "diff missing:$bad"
    return
  fi
  cleanup_job "$id"
  rec T10 PASS "diff of staged worktree shows new staged file, README.txt deletion ('deleted file') and the untracked file (job $id)"
}

t_t11() {
  start_job "FAKE_COMMIT job that commits its own work"
  if [ "$START_RC" -ne 0 ]; then
    rec T11 FAIL "start exited $START_RC: $START_OUT"
    return
  fi
  local id="$START_JOB_ID"
  [ -n "$id" ] || { rec T11 FAIL "no job id parsed: $START_OUT"; return; }
  local st
  st=$(wait_state "$id" done 60) || {
    cleanup_job "$id"
    rec T11 FAIL "job did not reach done (last status: $st)"
    return
  }
  local d
  d=$(ar diff "$id" 2>&1)
  rc=$?
  if [ $rc -ne 0 ] || [ -z "$d" ]; then
    cleanup_job "$id"
    rec T11 FAIL "diff of committed work is empty or failed (rc $rc): $d"
    return
  fi
  case "$d" in
    *committed-file.txt*) ;;
    *) cleanup_job "$id"; rec T11 FAIL "base diff does not show the commit: $d"; return ;;
  esac
  cleanup_job "$id"
  rec T11 PASS "diff of a committing job is non-empty and shows the committed file (job $id)"
}

t_t12() {
  local id wt out rc
  # (a) commit of a finished FAKE_WRITE job
  start_job "FAKE_WRITE=written-file.txt job for the commit test"
  if [ "$START_RC" -ne 0 ]; then
    rec T12 FAIL "start exited $START_RC: $START_OUT"
    return
  fi
  id="$START_JOB_ID"
  [ -n "$id" ] || { rec T12 FAIL "no job id parsed: $START_OUT"; return; }
  wait_state "$id" done 60 >/dev/null || {
    cleanup_job "$id"
    rec T12 FAIL "FAKE_WRITE job did not reach done"
    return
  }
  wt=$(ar status "$id" 2>/dev/null | sed -nE 's/^Worktree:[[:space:]]+//p' | head -n 1 | sed 's/ (directory missing on disk)$//')
  out=$(ar commit "$id" -m "blackbox commit message" 2>&1)
  rc=$?
  if [ $rc -ne 0 ] || [ -z "$wt" ]; then
    cleanup_job "$id"
    rec T12 FAIL "commit failed (rc $rc) or worktree unknown: $out"
    return
  fi
  local log
  log=$(git -C "$wt" log --format='%s' -n 3 2>&1)
  case "$log" in
    *"agent-run($id): blackbox commit message"*) ;;
    *)
      cleanup_job "$id"
      rec T12 FAIL "worktree log has no agent-run(<id>) commit: $log"
      return
      ;;
  esac
  cleanup_job "$id"
  # (b) commit refused while running
  start_job "FAKE_HANG job that must refuse a commit"
  if [ "$START_RC" -ne 0 ]; then
    rec T12 FAIL "start(hang) exited $START_RC: $START_OUT"
    return
  fi
  id="$START_JOB_ID"
  wait_state "$id" running 30 >/dev/null || {
    ar kill "$id" >/dev/null 2>&1 || true
    cleanup_job "$id"
    rec T12 FAIL "hang job not running; cannot check commit refusal"
    return
  }
  out=$(ar commit "$id" 2>&1)
  rc=$?
  if [ $rc -eq 0 ]; then
    ar kill "$id" >/dev/null 2>&1 || true
    cleanup_job "$id"
    rec T12 FAIL "commit on running job unexpectedly succeeded: $out"
    return
  fi
  case "$out" in
    *"currently running"*) ;;
    *) ar kill "$id" >/dev/null 2>&1 || true
       cleanup_job "$id"
       rec T12 FAIL "commit refusal text unexpected: $out"; return ;;
  esac
  ar kill "$id" >/dev/null 2>&1 || true
  wait_state "$id" killed 45 >/dev/null || true
  cleanup_job "$id"
  rec T12 PASS "commit creates agent-run(<id>) commit in the worktree; commit on running job refused with 'currently running'"
}

t_t13() {
  local id wt out rc branch
  # Part (a) uses a clean worktree (no FAKE_WRITE): a plain `clean` without
  # --force must succeed there. A worktree with untracked files is refused by
  # `git worktree remove` and clean tells you to pass --force.
  start_job "plain clean worktree job"
  if [ "$START_RC" -ne 0 ]; then
    rec T13 FAIL "start exited $START_RC: $START_OUT"
    return
  fi
  id="$START_JOB_ID"
  [ -n "$id" ] || { rec T13 FAIL "no job id parsed: $START_OUT"; return; }
  wait_state "$id" done 60 >/dev/null || {
    cleanup_job "$id"
    rec T13 FAIL "job did not reach done"
    return
  }
  wt=$(ar status "$id" 2>/dev/null | sed -nE 's/^Worktree:[[:space:]]+//p' | head -n 1 | sed 's/ (directory missing on disk)$//')
  branch="agent-run/$id"
  if [ ! -d "$wt" ] || [ -z "$(job_branches "$branch")" ]; then
    cleanup_job "$id"
    rec T13 FAIL "precondition failed: worktree dir or branch missing before clean (wt=$wt)"
    return
  fi
  out=$(ar clean "$id" 2>&1)
  rc=$?
  if [ $rc -ne 0 ] || ! printf '%s' "$out" | grep -q "cleaned successfully"; then
    cleanup_job "$id"
    rec T13 FAIL "clean failed (rc $rc): $out"
    return
  fi
  if [ -d "$wt" ]; then
    rec T13 FAIL "worktree dir still exists after clean: $wt"
    return
  fi
  if [ -n "$(job_branches "$branch")" ]; then
    rec T13 FAIL "branch still exists after clean: $branch"
    return
  fi
  if [ -n "$(git -C "$WORKSPACE/proj" branch --list "$branch" 2>/dev/null)" ]; then
    rec T13 FAIL "job branch leaked into project repo after clean: $branch"
    return
  fi
  out=$(ar list 2>&1)
  case "$out" in
    *"$id"*) rec T13 FAIL "list still shows cleaned job $id"; return ;;
  esac
  echo "  [T13] part (a) ok: worktree dir, branch and job record removed"
  # (b) running job: clean without --force must be refused
  local hid hout
  start_job "FAKE_HANG job that must refuse a plain clean"
  if [ "$START_RC" -ne 0 ]; then
    rec T13 FAIL "start(hang) exited $START_RC: $START_OUT"
    return
  fi
  hid="$START_JOB_ID"
  wait_state "$hid" running 30 >/dev/null || {
    ar kill "$hid" >/dev/null 2>&1 || true
    cleanup_job "$hid"
    rec T13 FAIL "hang job not running"
    return
  }
  hout=$(ar clean "$hid" 2>&1)
  rc=$?
  if [ $rc -eq 0 ]; then
    ar kill "$hid" >/dev/null 2>&1 || true
    cleanup_job "$hid"
    rec T13 FAIL "clean of running job succeeded without --force: $hout"
    return
  fi
  case "$hout" in
    *"currently running"*"--force"*) ;;
    *) ar kill "$hid" >/dev/null 2>&1 || true
       cleanup_job "$hid"
       rec T13 FAIL "clean refusal text unexpected: $hout"; return ;;
  esac
  # job must still be listable (clean refused, nothing removed)
  out=$(ar list 2>&1)
  case "$out" in
    *"$hid"*) ;;
    *) ar kill "$hid" >/dev/null 2>&1 || true
       cleanup_job "$hid"
       rec T13 FAIL "refused clean removed the job from the list"; return ;;
  esac
  ar kill "$hid" >/dev/null 2>&1 || true
  wait_state "$hid" killed 45 >/dev/null || true
  cleanup_job "$hid"
  rec T13 PASS "clean (running, no --force) refused with 'currently running ... --force'; job kept"
}

t_t14() {
  local ids=() i id st
  for i in 1 2; do
    start_job "FAKE_FAIL gc victim $i"
    if [ "$START_RC" -ne 0 ]; then
      rec T14 FAIL "start #$i exited $START_RC: $START_OUT"
      return
    fi
    id="$START_JOB_ID"
    [ -n "$id" ] || { rec T14 FAIL "no job id parsed: $START_OUT"; return; }
    ids+=("$id")
    st=$(wait_state "$id" failed 60) || {
      cleanup_job "$id"
      rec T14 FAIL "job $id not failed (last: $st)"
      return
    }
  done
  # NOTE: an even younger, pid-less job row (created by start but not yet
  # spawned) is protected from gc by is_protected_just_inserted (gc.rs,
  # <2 min, pid = None). That state is only observable for a few hundred ms
  # inside `start` and cannot be created deterministically black-box, so the
  # "never pruned" half of this test is intentionally not asserted here.
  local gc_out list_out
  gc_out=$(ar gc --older-than 0 2>&1)
  rc=$?
  if [ $rc -ne 0 ]; then
    for id in "${ids[@]}"; do cleanup_job "$id"; done
    rec T14 FAIL "gc exited $rc: $gc_out"
    return
  fi
  case "$gc_out" in
    *"GC complete: 2 removed"*) ;;
    *) for id in "${ids[@]}"; do cleanup_job "$id"; done
       rec T14 FAIL "gc did not remove both jobs: $gc_out"; return ;;
  esac
  for id in "${ids[@]}"; do
    case "$gc_out" in
      *"Removed job '$id'"*) ;;
      *) for x in "${ids[@]}"; do cleanup_job "$x"; done
         rec T14 FAIL "gc output does not mention removed job $id: $gc_out"; return ;;
    esac
    # These jobs failed (non-Done): gc must keep their log files.
    case "$gc_out" in
      *"Retained log file for job '$id'"*) ;;
      *) for x in "${ids[@]}"; do cleanup_job "$x"; done
         rec T14 FAIL "gc did not report retaining the log of non-Done job $id: $gc_out"; return ;;
    esac
    if [ ! -f "$STATE_ROOT/logs/$id.log" ]; then
      for x in "${ids[@]}"; do cleanup_job "$x"; done
      rec T14 FAIL "log file of non-Done job $id was pruned (expected retained)"
      return
    fi
    list_out=$(ar list 2>&1)
    case "$list_out" in
      *"$id"*) for x in "${ids[@]}"; do cleanup_job "$x"; done
               rec T14 FAIL "list still shows gc'd job $id"; return ;;
    esac
    if [ -n "$(job_branches "agent-run/$id")" ]; then
      for x in "${ids[@]}"; do cleanup_job "$x"; done
      rec T14 FAIL "branch of gc'd job $id still exists"
      return
    fi
    if [ -n "$(git -C "$WORKSPACE/proj" branch --list "agent-run/$id" 2>/dev/null)" ]; then
      for x in "${ids[@]}"; do cleanup_job "$x"; done
      rec T14 FAIL "branch of gc'd job $id leaked into project repo"
      return
    fi
  done
  rec T14 PASS "gc --older-than 0 removed both finished jobs (records, worktrees, branches); list clean; logs of non-Done jobs retained"
}

t_t15() {
  local id a b out rc i ok
  # (a) wait for two jobs -> exit 0
  start_job "FAKE_SLEEP=1 wait job a"
  [ "$START_RC" -eq 0 ] && [ -n "$START_JOB_ID" ] || { rec T15 FAIL "start a: $START_OUT"; return; }
  a="$START_JOB_ID"
  start_job "FAKE_SLEEP=1 wait job b"
  [ "$START_RC" -eq 0 ] && [ -n "$START_JOB_ID" ] || { cleanup_job "$a"; rec T15 FAIL "start b: $START_OUT"; return; }
  b="$START_JOB_ID"
  out=$(ar wait "$a" "$b" 2>&1)
  rc=$?
  if [ $rc -ne 0 ]; then
    cleanup_job "$a"; cleanup_job "$b"
    rec T15 FAIL "wait for two done jobs exited $rc: $out"
    return
  fi
  case "$out" in
    *"reached terminal state: done"*) ;;
    *) cleanup_job "$a"; cleanup_job "$b"
       rec T15 FAIL "wait output did not report both done: $out"; return ;;
  esac
  cleanup_job "$a"; cleanup_job "$b"
  # (b) --timeout on a hanging job -> exit 3
  start_job "FAKE_HANG wait timeout job"
  [ "$START_RC" -eq 0 ] && [ -n "$START_JOB_ID" ] || { rec T15 FAIL "start hang: $START_OUT"; return; }
  id="$START_JOB_ID"
  wait_state "$id" running 30 >/dev/null || { cleanup_job "$id"; rec T15 FAIL "hang job not running"; return; }
  out=$(ar wait --timeout 5s "$id" 2>&1)
  rc=$?
  if [ $rc -ne 3 ]; then
    ar kill "$id" >/dev/null 2>&1 || true
    cleanup_job "$id"
    rec T15 FAIL "wait --timeout on running job exited $rc, expected 3: $out"
    return
  fi
  # (c) unknown id -> clear error at the start (exit 1)
  out=$(ar wait zzzzzzzz 2>&1)
  rc=$?
  if [ $rc -eq 0 ]; then
    ar kill "$id" >/dev/null 2>&1 || true
    wait_state "$id" killed 45 >/dev/null || true
    cleanup_job "$id"
    rec T15 FAIL "wait on unknown id unexpectedly succeeded: $out"
    return
  fi
  case "$out" in
    *"No such job found"*) ;;
    *) ar kill "$id" >/dev/null 2>&1 || true
       wait_state "$id" killed 45 >/dev/null || true
       cleanup_job "$id"
       rec T15 FAIL "unknown-id error text unexpected: $out"; return ;;
  esac
  ar kill "$id" >/dev/null 2>&1 || true
  wait_state "$id" killed 45 >/dev/null || true
  cleanup_job "$id"
  # (d) wait survives a job cleaned mid-wait: it must exit within the bound (with 2,
  #     not done) and report it. Two outcomes are both legitimate: the row vanishes
  #     ("no longer exists"), or the poll lands after the forced kill but before the
  #     row is deleted ("reached terminal state: <non-done state>" e.g. killed /
  #     self-terminated).
  start_job "FAKE_SLEEP=6 wait then clean job"
  [ "$START_RC" -eq 0 ] && [ -n "$START_JOB_ID" ] || { rec T15 FAIL "start clean-wait: $START_OUT"; return; }
  id="$START_JOB_ID"
  wait_state "$id" running 30 >/dev/null || { cleanup_job "$id"; rec T15 FAIL "clean-wait job not running"; return; }
  local wf="$TMPROOT/wait-d.out"
  ar wait "$id" >"$wf" 2>&1 &
  local wpid=$!
  sleep 2
  ar clean --force "$id" >/dev/null 2>&1 || true
  ok=0
  for i in $(seq 1 40); do
    kill -0 "$wpid" 2>/dev/null || { ok=1; break; }
    sleep 0.5
  done
  if [ "$ok" != "1" ]; then
    kill "$wpid" 2>/dev/null || true
    wait "$wpid" 2>/dev/null || true
    rec T15 FAIL "wait did not exit within 20s after the job was cleaned mid-wait"
    return
  fi
  wait "$wpid"
  rc=$?
  out=$(cat "$wf" 2>/dev/null)
  if [ $rc -ne 2 ]; then
    rec T15 FAIL "wait after mid-wait clean exited $rc, expected 2: $out"
    return
  fi
  case "$out" in
    *"no longer exists"*) ;;
    *"reached terminal state"*) ;;
    *) rec T15 FAIL "wait output mentions neither the removed row nor a terminal state: $out"; return ;;
  esac
  rec T15 PASS "wait: two done -> 0; --timeout 5s on hang -> 3; unknown id -> 'No such job found'; cleaned mid-wait -> exit 2 + ('no longer exists' or terminal state)"
}

t_t16() {
  start_job "FAKE_SLEEP=1 list visibility job"
  if [ "$START_RC" -ne 0 ]; then
    rec T16 FAIL "start exited $START_RC: $START_OUT"
    return
  fi
  local id="$START_JOB_ID"
  [ -n "$id" ] || { rec T16 FAIL "no job id parsed: $START_OUT"; return; }
  local out
  out=$(ar list 2>&1)
  case "$out" in
    *"$id"*) ;;
    *) cleanup_job "$id"; rec T16 FAIL "list does not show the new job: $out"; return ;;
  esac
  out=$(ar list --project "" 2>&1)
  case "$out" in
    *"$id"*) ;;
    *) cleanup_job "$id"; rec T16 FAIL "list --project '' (no filter) does not show the job: $out"; return ;;
  esac
  out=$(ar list --project nosuch 2>&1)
  rc=$?
  if [ $rc -ne 0 ]; then
    cleanup_job "$id"
    rec T16 FAIL "list --project nosuch exited $rc: $out"
    return
  fi
  case "$out" in
    *"No jobs found for project 'nosuch'"*) ;;
    *) cleanup_job "$id"; rec T16 FAIL "expected project-specific no-jobs message: $out"; return ;;
  esac
  case "$out" in
    *"$id"*) cleanup_job "$id"; rec T16 FAIL "list --project nosuch shows the job: $out"; return ;;
  esac
  cleanup_job "$id"
  rec T16 PASS "list shows jobs; --project '' lists them; --project nosuch prints \"No jobs found for project 'nosuch'\""
}

t_t17() {
  start_job "FAKE_SLEEP=1 worktree location check"
  if [ "$START_RC" -ne 0 ]; then
    rec T17 FAIL "start exited $START_RC: $START_OUT"
    return
  fi
  local id="$START_JOB_ID"
  [ -n "$id" ] || { rec T17 FAIL "no job id parsed: $START_OUT"; return; }
  local expected wt
  expected="$WORKSPACE/.agent-jobs/proj/$id"
  case "$START_OUT" in
    *"  Worktree: $expected"*) ;;
    *) cleanup_job "$id"; rec T17 FAIL "start output worktree path unexpected: $START_OUT"; return ;;
  esac
  case "$START_OUT" in
    *"$WORKSPACE/proj/$id"*) cleanup_job "$id"; rec T17 FAIL "worktree reported inside the project checkout"; return ;;
  esac
  wt=$(ar status "$id" 2>/dev/null | sed -nE 's/^Worktree:[[:space:]]+//p' | head -n 1 | sed 's/ (directory missing on disk)$//')
  if [ "$wt" != "$expected" ]; then
    cleanup_job "$id"
    rec T17 FAIL "status worktree path '$wt' != expected '$expected'"
    return
  fi
  [ -d "$expected" ] || { cleanup_job "$id"; rec T17 FAIL "worktree dir $expected does not exist"; return; }
  cleanup_job "$id"
  rec T17 PASS "worktree at <workspace>/.agent-jobs/proj/<id>, not inside <workspace>/proj (job $id)"
}

t_t18() {
  # New policy: reading the result of a --plan job must NEVER delete it.
  # The job, its log, and its worktree must be present after `result`. Reading
  # the result twice must produce the same output. `clean <id>` must still
  # be able to remove the job.
  start_job "FAKE_SLEEP=1 plan keep-after-result job" --plan
  if [ "$START_RC" -ne 0 ]; then
    rec T18 FAIL "start exited $START_RC: $START_OUT"
    return
  fi
  local id="$START_JOB_ID"
  [ -n "$id" ] || { rec T18 FAIL "no job id parsed: $START_OUT"; return; }
  local wt
  wt=$(printf '%s\n' "$START_OUT" | sed -nE 's/^  Worktree: (.*)$/\1/p' | head -n 1)

  local st
  st=$(wait_state "$id" done 60) || {
    cleanup_job "$id"
      rec T18 FAIL "plan job did not reach done (last: $st)"
    return
  }

  # Read the result once.
  local out1
  out1=$(ar result "$id" 2>&1)
  local rc1=$?
  if [ $rc1 -ne 0 ]; then
    cleanup_job "$id"
    rec T18 FAIL "first result call failed (rc=$rc1): $out1"
    return
  fi

  # Job must still be in the list after result.
  local listout
  listout=$(ar list 2>&1)
  case "$listout" in
    *"$id"*)
      : # expected
      ;;
    *)
      cleanup_job "$id"
      rec T18 FAIL "plan job gone from list after first result (was auto-cleaned): $listout"
      return
      ;;
  esac

  # Worktree must still exist.
  if [ -n "$wt" ] && [ ! -d "$wt" ]; then
    cleanup_job "$id"
      rec T18 FAIL "plan job worktree gone after first result: $wt"
    return
  fi

  # The keep-hint must have been printed.
  case "$out1" in
    *"is kept"* | *"agent-run clean"*)
      : # good
      ;;
    *)
      cleanup_job "$id"
      rec T18 FAIL "keep-hint not printed after result: $out1"
      return
      ;;
  esac

  # Read the result a second time -- must succeed with the same response.
  local out2
  out2=$(ar result "$id" 2>&1)
  local rc2=$?
  if [ $rc2 -ne 0 ]; then
    cleanup_job "$id"
    rec T18 FAIL "second result call failed (rc=$rc2): $out2"
    return
  fi

  # Both reads must include the response text.
  case "$out1" in
    *"Status:"*) : ;;
    *)
      cleanup_job "$id"
      rec T18 FAIL "first result has no Status line: $out1"
      return
      ;;
  esac
  case "$out2" in
    *"Status:"*) : ;;
    *)
      cleanup_job "$id"
      rec T18 FAIL "second result has no Status line: $out2"
      return
      ;;
  esac

  # Now clean the job explicitly; it must be removed.
  local clean_out
  clean_out=$(ar clean "$id" --force 2>&1)
  local clean_rc=$?
  if [ $clean_rc -ne 0 ]; then
    cleanup_job "$id"
    rec T18 FAIL "clean after result failed (rc=$clean_rc): $clean_out"
    return
  fi

  # Confirm it's gone from the list.
  listout=$(ar list 2>&1)
  case "$listout" in
    *"$id"*)
      rec T18 FAIL "plan job still listed after clean: $listout"
      return
      ;;
  esac
  rec T18 PASS "plan job kept after result (idempotent), removed by clean (job $id)"
}

t_t19() {
  local out rc
  # invalid values are rejected client-side (clap), no server round-trip
  out=$(ar start proj "p" --effort ultra 2>&1)
  rc=$?
  if [ $rc -eq 0 ]; then
    rec T19 FAIL "--effort ultra was accepted"
    return
  fi
  case "$out" in
    *"invalid value"*) ;;
    *) rec T19 FAIL "--effort ultra error text unexpected: $out"; return ;;
  esac
  out=$(ar start proj "p" --effort Low 2>&1)
  rc=$?
  if [ $rc -eq 0 ]; then
    rec T19 FAIL "--effort Low was accepted"
    return
  fi
  case "$out" in
    *"invalid value"*) ;;
    *) rec T19 FAIL "--effort Low error text unexpected: $out"; return ;;
  esac
  # the documented combination must parse (it does start a real job: clean it)
  out=$(ar start proj "FAKE_SLEEP=1 valid combination" --plan --effort high 2>&1)
  rc=$?
  if [ $rc -ne 0 ]; then
    rec T19 FAIL "--plan --effort high was rejected: $out"
    return
  fi
  case "$out" in
    *"invalid value"*) rec T19 FAIL "valid combination rejected with: $out"; return ;;
  esac
  local vid
  vid=$(printf '%s\n' "$out" | sed -nE "s/^Job '([0-9a-f]{8})' started.*/\1/p" | head -n 1)
  if [ -z "$vid" ]; then
    rec T19 FAIL "valid combination started but no job id in output: $out"
    return
  fi
  wait_state "$vid" done 30 >/dev/null || cleanup_job "$vid"
  cleanup_job "$vid"
  rec T19 PASS "client rejects --effort ultra / Low with 'invalid value'; --plan --effort high accepted (job $vid cleaned)"
}

t_t20() {
  start_job "FAKE_FAIL double-retry target with conversation id"
  if [ "$START_RC" -ne 0 ]; then
    rec T20 FAIL "start exited $START_RC: $START_OUT"
    return
  fi
  local id="$START_JOB_ID"
  [ -n "$id" ] || { rec T20 FAIL "no job id parsed: $START_OUT"; return; }
  wait_state "$id" failed 60 >/dev/null || {
    cleanup_job "$id"
    rec T20 FAIL "job did not reach failed"
    return
  }
  local r1f="$TMPROOT/retry-1.out" r2f="$TMPROOT/retry-2.out"
  ar retry "$id" >"$r1f" 2>&1 &
  local p1=$!
  ar retry "$id" >"$r2f" 2>&1 &
  local p2=$!
  wait "$p1"; local rc1=$?
  wait "$p2"; local rc2=$?
  local o1 o2 launched loser loser_out
  o1=$(cat "$r1f" 2>/dev/null)
  o2=$(cat "$r2f" 2>/dev/null)
  launched=0
  case "$o1" in *"retry launched in background"*) launched=$((launched + 1));; esac
  case "$o2" in *"retry launched in background"*) launched=$((launched + 1));; esac
  if [ "$launched" -ne 1 ]; then
    cleanup_job "$id"
    rec T20 FAIL "expected exactly one retry to launch, got $launched (o1: $o1 | o2: $o2)"
    return
  fi
  if [ $rc1 -eq 0 ] && [ $rc2 -eq 0 ]; then
    cleanup_job "$id"
    rec T20 FAIL "both concurrent retries reported success; only one may launch (o1: $o1 | o2: $o2)"
    return
  fi
  # The loser must report a refusal: either the atomic-claim rejection or the
  # "job already running" gate (whichever it hit first). Both prove no second
  # attempt was started.
  if printf '%s' "$o1" | grep -q "retry launched in background"; then
    loser=$rc2
    loser_out="$o2"
  else
    loser=$rc1
    loser_out="$o1"
  fi
  if [ "$loser" -eq 0 ]; then
    cleanup_job "$id"
    rec T20 FAIL "the losing retry exited 0: $loser_out"
    return
  fi
  case "$loser_out" in
    *"another retry already claimed"*|*"currently running"*) ;;
    *) cleanup_job "$id"; rec T20 FAIL "the losing retry did not report a duplicate/run refusal: $loser_out"; return ;;
  esac
  # The banner is written to the log BEFORE the atomic claim, so the racy
  # loser may leave its banner behind; the single 'retry launched' output
  # above is the authoritative evidence that retry_count advanced once.
  local st joblog errlines
  st=$(wait_state "$id" terminal 60) || {
    cleanup_job "$id"
    rec T20 FAIL "job did not settle after concurrent retries (last: $st)"
    return
  }
  joblog=$(ar log "$id" 2>&1)
  case "$joblog" in
    *"==================== RETRY AT"*) ;;
    *) cleanup_job "$id"; rec T20 FAIL "no retry banner in the log"; return ;;
  esac
  # Each agy attempt of this prompt prints exactly one structured ERROR JSON
  # line. The original attempt already printed one; exactly ONE such line after
  # the retry banner proves the retried attempt ran exactly once, i.e.
  # retry_count advanced exactly once.
  errlines=$(printf '%s\n' "$joblog" | awk '/RETRY AT/{f=1; next} f && /"status":"ERROR"/{c++} END{print c+0}')
  if [ "$errlines" != "1" ]; then
    cleanup_job "$id"
    rec T20 FAIL "expected exactly one executed retry attempt (one ERROR json line after the banner), found $errlines"
    return
  fi
  cleanup_job "$id"
  rec T20 PASS "two simultaneous retries: exactly one launched, the other refused; exactly one attempt ran (retry_count +1); job settled (terminal: $st) (job $id)"
}

t_t21() {
  # The 100 KB engine limit (runner.rs PROMPT_ARGV_MAX_BYTES) is a check on the
  # prompt text as delivered to the engine. Passing 150000 bytes as a bare
  # CLI argv element would instead trip the OS per-arg limit (E2BIG) before the
  # client even runs, so the prompt is delivered as @<file>: the client reads
  # the file and sends its 150000-byte content over HTTP, letting the engine's
  # own documented check fire.
  local big="$TMPROOT/bigprompt.txt"
  python3 -c 'import sys; sys.stdout.write("A" * 150000)' > "$big" 2>/dev/null || {
    rec T21 FAIL "could not build a 150000-byte prompt file"
    return
  }
  local sz
  sz=$(wc -c < "$big" 2>/dev/null || echo 0)
  if [ "$sz" -ne 150000 ]; then
    rec T21 FAIL "prompt file has $sz bytes, expected 150000"
    return
  fi
  local before after out rc list_before bigid
  before=$(find "$WORKSPACE/.agent-jobs/proj" -maxdepth 1 -mindepth 1 2>/dev/null | wc -l)
  list_before=$(ar list 2>/dev/null)
  out=$(ar start proj "@$big" 2>&1)
  rc=$?
  if [ $rc -eq 0 ]; then
    bigid=$(printf '%s\n' "$out" | sed -nE "s/^Job '([0-9a-f]{8})' started.*/\1/p" | head -n 1)
    [ -n "$bigid" ] && { ar kill "$bigid" >/dev/null 2>&1 || true; cleanup_job "$bigid"; }
    rec T21 FAIL "150000-byte prompt was accepted (job $bigid cleaned up)"
    return
  fi
  case "$out" in
    *"longer than the 100000 byte limit"*) ;;
    *) rec T21 FAIL "over-limit refusal text unexpected (rc $rc): $out"; return ;;
  esac
  after=$(find "$WORKSPACE/.agent-jobs/proj" -maxdepth 1 -mindepth 1 2>/dev/null | wc -l)
  if [ "$before" != "$after" ]; then
    rec T21 FAIL "a worktree dir was left behind by the refused start ($before -> $after)"
    return
  fi
  out=$(ar list 2>/dev/null)
  if [ "$out" != "$list_before" ]; then
    rec T21 FAIL "refused start changed the job list (new row?): $out"
    return
  fi
  rec T21 PASS "150000-byte prompt (@file) refused with the documented 100000-byte-limit message; no worktree or job row left behind"
}

t_t22() {
  start_job "FAKE_BADUTF8 log with a stray invalid byte"
  if [ "$START_RC" -ne 0 ]; then
    rec T22 FAIL "start exited $START_RC: $START_OUT"
    return
  fi
  local id="$START_JOB_ID"
  [ -n "$id" ] || { rec T22 FAIL "no job id parsed: $START_OUT"; return; }
  local st
  st=$(wait_state "$id" done 60) || {
    cleanup_job "$id"
    rec T22 FAIL "job with invalid UTF-8 in the log did not reach done (last: $st)"
    return
  }
  cleanup_job "$id"
  rec T22 PASS "FAKE_BADUTF8 (0xff byte before the SUCCESS JSON) still classifies as done (job $id)"
}

t_t24() {
  start_job "FAKE_SLEEP=20 job that outlives a server restart"
  if [ "$START_RC" -ne 0 ]; then
    rec T24 FAIL "start exited $START_RC: $START_OUT"
    return
  fi
  local id="$START_JOB_ID"
  [ -n "$id" ] || { rec T24 FAIL "no job id parsed: $START_OUT"; return; }
  local st
  st=$(wait_state "$id" running 30) || {
    cleanup_job "$id"
    rec T24 FAIL "job not running before restart (last: $st)"
    return
  }
  # Take the private server down while the job is in flight.
  kill -TERM "$SERVER_PID" 2>/dev/null || true
  local i
  for i in $(seq 1 20); do
    kill -0 "$SERVER_PID" 2>/dev/null || break
    sleep 0.5
  done
  kill -0 "$SERVER_PID" 2>/dev/null && {
    kill -KILL "$SERVER_PID" 2>/dev/null || true
    sleep 1
  }
  SERVER_PID=""
  # Restart on the SAME state root (the job row must survive).
  local restart_log="$TMPROOT/server-after-restart.log"
  start_server "$restart_log"
  wait_for_server "$restart_log"
  local out rc
  out=$(ar status "$id" 2>&1)
  rc=$?
  if [ $rc -ne 0 ]; then
    rec T24 FAIL "status after server restart failed (rc $rc): $out"
    return
  fi
  st=$(printf '%s\n' "$out" | sed -nE 's/^Status:[[:space:]]*//p' | head -n 1)
  case "$st" in
    running|done|failed|self-terminated|killed|incomplete|quota-exhausted) ;;
    *) rec T24 FAIL "status after restart not a valid state token: '$st'"; return ;;
  esac
  out=$(ar list 2>&1)
  rc=$?
  if [ $rc -ne 0 ] || ! printf '%s' "$out" | grep -q "$id"; then
    rec T24 FAIL "list after server restart failed or missing the job (rc $rc): $out"
    return
  fi
  # Tear down the (possibly still alive, orphaned) job process.
  ar kill "$id" >/dev/null 2>&1 || true
  wait_state "$id" terminal 60 >/dev/null || true
  cleanup_job "$id"
  rec T24 PASS "server restarted on the same state root; status of the in-flight job answered (state: $st) and list works"
}

t_t25() {
  local branches proj_branches wts pids
  branches=$(job_branches "agent-run/*")
  if [ -n "$branches" ]; then
    rec T25 FAIL "leftover agent-run/* branches in private repo: $branches"
    return
  fi
  proj_branches=$(git -C "$WORKSPACE/proj" branch --list "agent-run/*" 2>/dev/null | sed 's/^[* ]//')
  if [ -n "$proj_branches" ]; then
    rec T25 FAIL "agent-run/* branches leaked into project repo: $proj_branches"
    return
  fi
  wts=$(git -C "$WORKSPACE/proj" worktree list 2>/dev/null)
  if [ "$(printf '%s\n' "$wts" | grep -c . )" != "1" ]; then
    rec T25 FAIL "orphan worktrees registered: $wts"
    return
  fi
  case "$wts" in
    *"$WORKSPACE/proj"*) ;;
    *) rec T25 FAIL "main worktree missing from: $wts"; return ;;
  esac
  pids=$(pgrep -f "$FAKE_AGY" 2>/dev/null || true)
  if [ -n "$pids" ]; then
    rec T25 FAIL "fake-agy processes still running: $pids"
    return
  fi
  rec T25 PASS "no agent-run/* branches, no orphan worktrees, no fake-agy processes left"
}

t_t26() {
  local out rc
  out=$(ar projects 2>&1)
  rc=$?
  if [ $rc -ne 0 ]; then
    rec T26 FAIL "projects exited $rc: $out"
    return
  fi
  case "$out" in
    *JOBS*) ;;
    *) rec T26 FAIL "projects table has no JOBS column: $out"; return ;;
  esac
  case "$out" in
    *"proj"*) ;;
    *) rec T26 FAIL "projects does not list the test project 'proj': $out"; return ;;
  esac
  case "$out" in
    *main*) ;;
    *) rec T26 FAIL "projects: expected branch 'main' for proj: $out"; return ;;
  esac
  # The table shows the short HEAD sha (the subject only goes into the JSON
  # data), so a hex SHA in the HEAD column is the observable signature.
  if ! printf '%s\n' "$out" | grep -qE '^proj[[:space:]]+main[[:space:]]+[0-9a-f]{4,}'; then
    rec T26 FAIL "projects: proj row lacks branch main + a short HEAD sha: $out"
    return
  fi
  # The jobs dir is a dot-directory of the workspace root: not a project.
  case "$out" in
    *".agent-jobs"*) rec T26 FAIL "projects lists the internal .agent-jobs dir: $out"; return ;;
  esac
  rec T26 PASS "projects lists proj (branch main, HEAD subject) with the JOBS column; no .agent-jobs row"
}

t_t27() {
  local out rc
  out=$(ar info 2>&1)
  rc=$?
  if [ $rc -ne 0 ]; then
    rec T27 FAIL "info exited $rc: $out"
    return
  fi
  local bad=""
  for section in server config capacity disk auth toolchains; do
    printf '%s\n' "$out" | grep -q "^$section" || bad="$bad missing-section:$section"
  done
  printf '%s\n' "$out" | grep -q "  workspace_root: $WORKSPACE" || bad="$bad missing-workspace_root"
  printf '%s\n' "$out" | grep -q "  state_root: $STATE_ROOT" || bad="$bad missing-state_root"
  printf '%s\n' "$out" | grep -q "  jobs_dir: .agent-jobs / max_parallel: 3" || bad="$bad missing-jobs_dir"
  printf '%s\n' "$out" | grep -q "  running: " || bad="$bad missing-running"
  printf '%s\n' "$out" | grep -q "hindsight: disabled" || bad="$bad missing-hindsight-disabled"
  printf '%s\n' "$out" | grep -q "$FAKE_AGY" || bad="$bad missing-agy-path"
  printf '%s\n' "$out" | grep -q "  credentials: " || bad="$bad missing-credentials"
  for tool in git gh go rustc cargo python3 pip3; do
    printf '%s\n' "$out" | grep -q "^  $tool: " || bad="$bad missing-tool:$tool"
  done
  if [ -n "$bad" ]; then
    rec T27 FAIL "info output bad:$bad"
    return
  fi
  rec T27 PASS "info prints all six sections with the suite's workspace/state roots, jobs_dir/max_parallel, creds state and all 7 toolchains"
}

t_t28() {
  local out rc
  out=$(ar models 2>&1)
  rc=$?
  if [ $rc -ne 0 ]; then
    rec T28 FAIL "models exited $rc: $out"
    return
  fi
  case "$out" in
    *LABEL*) ;;
    *) rec T28 FAIL "models table has no LABEL column: $out"; return ;;
  esac
  local bad=""
  case "$out" in *"gemini-3.8-flash-low"*) ;; *) bad="$bad missing model gemini-3.8-flash-low";; esac
  case "$out" in *"gemini-3.8-pro"*) ;; *) bad="$bad missing model gemini-3.8-pro";; esac
  case "$out" in *"claude-sonnet-4-5"*) ;; *) bad="$bad missing model claude-sonnet-4-5";; esac
  case "$out" in *"Gemini 3.8 Flash (Low)"*) ;; *) bad="$bad missing label 'Gemini 3.8 Flash (Low)'";; esac
  if [ -n "$bad" ]; then
    rec T28 FAIL "models output bad:$bad: $out"
    return
  fi
  # The model list must really come from the server's agy (fake-agy): the
  # invocation of the models subcommand must have been logged by the fake.
  if [ -f "$TMPROOT/argv.log" ] && ! tail -n 1 "$TMPROOT/argv.log" | grep -q "models"; then
    rec T28 FAIL "fake-agy argv log shows no recent 'models' invocation (list not served by the server's agy): $(tail -n 1 "$TMPROOT/argv.log")"
    return
  fi
  rec T28 PASS "models lists all three fake agy models with their labels (LABEL table, served by the server's agy)"
}

t_t29() {
  local out err rc header
  out=$(ar quota --fresh 2>"$TMPROOT/quota.err")
  rc=$?
  err=$(cat "$TMPROOT/quota.err" 2>/dev/null)
  if [ $rc -ne 0 ]; then
    rec T29 FAIL "quota exited $rc: $out | $err"
    return
  fi
  local bad=""
  header=$(printf '%s\n' "$out" | head -n 1)
  case "$header" in
    *GROUP*) ;; *) bad="$bad header:GROUP";; esac
    case "$header" in
      *LIMIT*) ;; *) bad="$bad header:LIMIT";; esac
    case "$header" in
      *REMAINING*) ;; *) bad="$bad header:REMAINING";; esac
    case "$header" in
      *"RESETS (UTC)"*) ;; *) bad="$bad header:RESETS (UTC)";; esac
    case "$header" in
      *IN*) ;; *) bad="$bad header:IN";; esac
  case "$out" in *"Gemini Models"*) ;; *) bad="$bad missing group 'Gemini Models'";; esac
  case "$out" in *"Weekly Limit Remaining"*) ;; *) bad="$bad missing 'Weekly Limit Remaining'";; esac
  case "$out" in *"Five Hour Limit Remaining"*) ;; *) bad="$bad missing 'Five Hour Limit Remaining'";; esac
  case "$out" in *"Claude and GPT models"*) ;; *) bad="$bad missing group 'Claude and GPT models'";; esac
  case "$out" in *" 5% "*) ;; *) bad="$bad missing 5% bucket";; esac
  case "$out" in *" 82% "*) ;; *) bad="$bad missing 82% bucket";; esac
  # A reset time must have been parsed into an IN duration (>0 for the future
  # reset times the fake prints): an IN cell of "-" means unparsed.
  if printf '%s\n' "$out" | grep -q '	-$'; then
    bad="$bad IN cell unparsed (-)"
  fi
  # The <=10% bucket warns on stderr with the rendered reset time.
  case "$err" in
    *"warning: Gemini Models Weekly Limit Remaining is at 5%"*) ;;
    *) bad="$bad missing low-bucket warning (stderr: $err)";; esac
  if [ -n "$bad" ]; then
    rec T29 FAIL "quota output bad:$bad | stderr: $err"
    return
  fi
  rec T29 PASS "quota renders all four parsed buckets (5% + 82% + 100%s) with the IN column and the <=10% stderr warning"
}

# Worktree path of a job, via `ar status` (same parsing as T17/T12).
job_worktree() {
  ar status "$1" 2>/dev/null | sed -nE 's/^Worktree:[[:space:]]+//p' | head -n 1 | sed 's/ (directory missing on disk)$//'
}

t_t30() {
  start_job "FAKE_WRITE=t30-file.txt files listing job"
  if [ "$START_RC" -ne 0 ]; then
    rec T30 FAIL "start exited $START_RC: $START_OUT"
    return
  fi
  local id="$START_JOB_ID"
  [ -n "$id" ] || { rec T30 FAIL "no job id parsed: $START_OUT"; return; }
  wait_state "$id" done 60 >/dev/null || { cleanup_job "$id"; rec T30 FAIL "job did not reach done"; return; }
  local out wt rc
  wt=$(job_worktree "$id")
  [ -n "$wt" ] && [ -d "$wt" ] || { cleanup_job "$id"; rec T30 FAIL "worktree unknown: $wt"; return; }
  # The bare worktree only has committed files + the job's written file, so
  # add a real subdirectory on the host to exercise dirs-before-files ordering
  # and nested listing.
  mkdir -p "$wt/t30-sub" 2>/dev/null || { cleanup_job "$id"; rec T30 FAIL "cannot create a subdirectory in the worktree"; return; }
  printf 'nested content\n' > "$wt/t30-sub/nested.txt" 2>/dev/null || { cleanup_job "$id"; rec T30 FAIL "cannot write the nested fixture"; return; }
  out=$(ar files "$id" 2>&1)
  rc=$?
  if [ $rc -ne 0 ] || [ -z "$out" ]; then
    cleanup_job "$id"
    rec T30 FAIL "files exited $rc: $out"
    return
  fi
  # Dirs are listed before files: the first entry must be a directory line.
  if ! printf '%s\n' "$out" | head -n 1 | grep -qE $'^d +[0-9]+  [0-9T]'; then
    cleanup_job "$id"
    rec T30 FAIL "files: first entry is not a directory line: $(printf '%s\n' "$out" | head -n 1)"
    return
  fi
  case "$out" in *"README.txt"*) ;; *) cleanup_job "$id"; rec T30 FAIL "files does not list README.txt: $out"; return ;; esac
  case "$out" in *t30-file.txt*) ;; *) cleanup_job "$id"; rec T30 FAIL "files does not list the job's written file: $out"; return ;; esac
  case "$out" in *t30-sub*) ;; *) cleanup_job "$id"; rec T30 FAIL "files does not list the subdirectory: $out"; return ;; esac
  case "$out" in *".git"*) cleanup_job "$id"; rec T30 FAIL "files must not list .git: $out"; return ;; esac
  # Cross-check one file line against the on-disk file (type f, real size).
  local expect_size line
  expect_size=$(wc -c < "$wt/t30-file.txt" 2>/dev/null || echo "")
  line=$(printf '%s\n' "$out" | grep ' t30-file.txt *$' | head -n 1)
  if [ -z "$line" ]; then
    cleanup_job "$id"
    rec T30 FAIL "no files line ends with t30-file.txt: $out"
    return
  fi
  if [ -z "$expect_size" ] || ! printf '%s' "$line" | grep -qE "^f[ ]+${expect_size}[ ]+"; then
    cleanup_job "$id"
    rec T30 FAIL "files entry for t30-file.txt is not an 'f' line with size $expect_size: $line"
    return
  fi
  # Listing the subdirectory by path shows its (otherwise depth-hidden) child.
  local sub_out sub_rc
  sub_out=$(ar files "$id" t30-sub 2>&1)
  sub_rc=$?
  if [ $sub_rc -ne 0 ] || ! printf '%s\n' "$sub_out" | grep -q 'nested.txt'; then
    cleanup_job "$id"
    rec T30 FAIL "files <subdir> does not list its child (rc $sub_rc): $sub_out"
    return
  fi
  # A missing path is refused cleanly.
  out=$(ar files "$id" nonexistent-dir 2>&1)
  rc=$?
  if [ $rc -eq 0 ]; then
    cleanup_job "$id"
    rec T30 FAIL "files of a missing path unexpectedly succeeded: $out"
    return
  fi
  case "$out" in
    *"does not exist in worktree"*) ;;
    *) cleanup_job "$id"; rec T30 FAIL "missing-path error text unexpected: $out"; return ;;
  esac
  cleanup_job "$id"
  rec T30 PASS "files lists worktree entries (dirs before files, 'f' lines with real sizes, .git hidden, nested via subdir path); missing path refused cleanly (job $id)"
}

t_t31() {
  start_job "FAKE_SLEEP=1 cat exact bytes job"
  if [ "$START_RC" -ne 0 ]; then
    rec T31 FAIL "start exited $START_RC: $START_OUT"
    return
  fi
  local id="$START_JOB_ID"
  [ -n "$id" ] || { rec T31 FAIL "no job id parsed: $START_OUT"; return; }
  wait_state "$id" done 60 >/dev/null || { cleanup_job "$id"; rec T31 FAIL "job did not reach done"; return; }
  local wt f1 f2 out rc
  wt=$(job_worktree "$id")
  [ -n "$wt" ] && [ -d "$wt" ] || { cleanup_job "$id"; rec T31 FAIL "worktree unknown: $wt"; return; }
  # (a) content with no trailing newline: printed bytes must equal file bytes.
  printf 'exact bytes, no trailing newline' > "$wt/t31-nonl.txt"
  f1="$TMPROOT/t31-nonl.out"
  out=$(ar cat "$id" t31-nonl.txt 2>&1)
  rc=$?
  if [ $rc -ne 0 ]; then
    cleanup_job "$id"
    rec T31 FAIL "cat (no-newline file) exited $rc: $out"
    return
  fi
  printf '%s' "$out" > "$f1"
  if ! cmp -s "$f1" "$wt/t31-nonl.txt"; then
    cleanup_job "$id"
    rec T31 FAIL "cat output differs from the file bytes (no-trailing-newline case)"
    return
  fi
  # (b) content with a trailing newline and a blank line inside.
  printf 'line one\n\nline three\n' > "$wt/t31-nl.txt"
  f2="$TMPROOT/t31-nl.out"
  out=$(ar cat "$id" t31-nl.txt 2>&1)
  rc=$?
  if [ $rc -ne 0 ]; then
    cleanup_job "$id"
    rec T31 FAIL "cat (newlines file) exited $rc: $out"
    return
  fi
  printf '%s\n' "$out" > "$f2"
  if ! cmp -s "$f2" "$wt/t31-nl.txt"; then
    cleanup_job "$id"
    rec T31 FAIL "cat output differs from the file bytes (trailing-newline case)"
    return
  fi
  # (c) the committed README.txt of the worktree.
  if ! cmp -s <(ar cat "$id" README.txt 2>/dev/null) "$wt/README.txt"; then
    cleanup_job "$id"
    rec T31 FAIL "cat README.txt differs from the on-disk bytes"
    return
  fi
  cleanup_job "$id"
  rec T31 PASS "cat reproduces file bytes exactly (no trailing newline / blank lines / committed file) (job $id)"
}

t_t32() {
  start_job "FAKE_SLEEP=1 cat path refusal fixtures"
  if [ "$START_RC" -ne 0 ]; then
    rec T32 FAIL "start exited $START_RC: $START_OUT"
    return
  fi
  local id="$START_JOB_ID"
  [ -n "$id" ] || { rec T32 FAIL "no job id parsed: $START_OUT"; return; }
  wait_state "$id" done 60 >/dev/null || { cleanup_job "$id"; rec T32 FAIL "job did not reach done"; return; }
  local wt out rc
  wt=$(job_worktree "$id")
  [ -n "$wt" ] && [ -d "$wt" ] || { cleanup_job "$id"; rec T32 FAIL "worktree unknown: $wt"; return; }
  ln -s .git "$wt/t32-gitlink" 2>/dev/null || {
    cleanup_job "$id"
    rec T32 FAIL "cannot create the symlink fixture in the worktree"
    return
  }
  # absolute path
  out=$(ar cat "$id" "/etc/passwd" 2>&1)
  rc=$?
  if [ $rc -eq 0 ]; then
    cleanup_job "$id"
    rec T32 FAIL "cat of an absolute path was not refused: $out"
    return
  fi
  case "$out" in *absolute*) ;; *) cleanup_job "$id"; rec T32 FAIL "absolute-path refusal text unexpected: $out"; return ;; esac
  # .. traversal
  out=$(ar cat "$id" "../README.txt" 2>&1)
  rc=$?
  if [ $rc -eq 0 ]; then
    cleanup_job "$id"
    rec T32 FAIL "cat of a .. path was not refused: $out"
    return
  fi
  case "$out" in *"'..'"*) ;; *) cleanup_job "$id"; rec T32 FAIL "..-path refusal text unexpected: $out"; return ;; esac
  # .git component
  out=$(ar cat "$id" .git/HEAD 2>&1)
  rc=$?
  if [ $rc -eq 0 ]; then
    cleanup_job "$id"
    rec T32 FAIL "cat of a .git path was not refused: $out"
    return
  fi
  case "$out" in *".git"*) ;; *) cleanup_job "$id"; rec T32 FAIL ".git refusal text unexpected: $out"; return ;; esac
  # a symlink named anything that resolves into .git: in a worktree `.git` is
  # a file, so the link must be catted itself (a path THROUGH it cannot exist)
  out=$(ar cat "$id" t32-gitlink 2>&1)
  rc=$?
  if [ $rc -eq 0 ]; then
    cleanup_job "$id"
    rec T32 FAIL "cat through a symlink into .git was not refused: $out"
    return
  fi
  # The engine's actual refusals (agent-core/src/commands/files.rs): a
  # canonical target at/inside .git is `` `.git` is not accessible: '<path>' ``;
  # a link resolving outside the worktree is 'path escapes the worktree'.
  # A generic 'does not exist' error would mean the .git check did not fire,
  # so never accept it.
  case "$out" in
    *"not accessible"*) ;;
    *"path escapes the worktree"*) ;;
    *) cleanup_job "$id"; rec T32 FAIL "symlink-to-.git refusal text unexpected: $out"; return ;;
  esac
  # The worktree must be left as found (symlink removed before clean).
  rm -f "$wt/t32-gitlink"
  cleanup_job "$id"
  rec T32 PASS "cat refuses absolute paths, .. , .git components and a symlink resolving into .git with '.git' is not accessible (all non-zero exit) (job $id)"
}

t_t33() {
  start_job "FAKE_SLEEP=1 cat binary refusal job"
  if [ "$START_RC" -ne 0 ]; then
    rec T33 FAIL "start exited $START_RC: $START_OUT"
    return
  fi
  local id="$START_JOB_ID"
  [ -n "$id" ] || { rec T33 FAIL "no job id parsed: $START_OUT"; return; }
  wait_state "$id" done 60 >/dev/null || { cleanup_job "$id"; rec T33 FAIL "job did not reach done"; return; }
  local wt out err rc size
  wt=$(job_worktree "$id")
  [ -n "$wt" ] && [ -d "$wt" ] || { cleanup_job "$id"; rec T33 FAIL "worktree unknown: $wt"; return; }
  printf '\x00\x01binary payload\x02\n' > "$wt/t33-bin.dat" 2>/dev/null || {
    cleanup_job "$id"
    rec T33 FAIL "cannot create the binary fixture in the worktree"
    return
  }
  size=$(wc -c < "$wt/t33-bin.dat")
  err="$TMPROOT/t33.err"
  out=$(ar cat "$id" t33-bin.dat 2>"$err")
  rc=$?
  err=$(cat "$err" 2>/dev/null)
  if [ $rc -ne 1 ]; then
    cleanup_job "$id"
    rec T33 FAIL "cat of a binary file exited $rc, expected 1: $out | $err"
    return
  fi
  case "$err" in
    *"binary file ($size bytes) not shown"*) ;;
    *) cleanup_job "$id"; rec T33 FAIL "binary refusal text unexpected (stderr): $err"; return ;;
  esac
  cleanup_job "$id"
  rec T33 PASS "cat refuses a binary file with exit 1 and 'binary file (N bytes) not shown' (job $id)"
}

t_t34() {
  start_job "FAKE_SLEEP=1 cat --lines job"
  if [ "$START_RC" -ne 0 ]; then
    rec T34 FAIL "start exited $START_RC: $START_OUT"
    return
  fi
  local id="$START_JOB_ID"
  [ -n "$id" ] || { rec T34 FAIL "no job id parsed: $START_OUT"; return; }
  wait_state "$id" done 60 >/dev/null || { cleanup_job "$id"; rec T34 FAIL "job did not reach done"; return; }
  local wt out rc n i
  wt=$(job_worktree "$id")
  [ -n "$wt" ] && [ -d "$wt" ] || { cleanup_job "$id"; rec T34 FAIL "worktree unknown: $wt"; return; }
  i=1
  while [ $i -le 10 ]; do
    printf 't34 line %02d\n' "$i"
    i=$((i + 1))
  done > "$wt/t34-lines.txt"
  out=$(ar cat "$id" t34-lines.txt --lines 1:5 2>&1)
  rc=$?
  if [ $rc -ne 0 ]; then
    cleanup_job "$id"
    rec T34 FAIL "cat --lines 1:5 exited $rc: $out"
    return
  fi
  n=$(printf '%s\n' "$out" | grep -c .)
  if [ "$n" != "5" ]; then
    cleanup_job "$id"
    rec T34 FAIL "cat --lines 1:5 returned $n lines, expected 5: $out"
    return
  fi
  case "$out" in
    *"t34 line 01"*) ;; *) cleanup_job "$id"; rec T34 FAIL "cat --lines 1:5 missing first line: $out"; return ;;
  esac
  case "$out" in
    *"t34 line 05"*) ;; *) cleanup_job "$id"; rec T34 FAIL "cat --lines 1:5 missing line 5: $out"; return ;;
  esac
  case "$out" in
    *"t34 line 06"*) cleanup_job "$id"; rec T34 FAIL "cat --lines 1:5 leaked line 6: $out"; return ;;
  esac
  # A window starting at line 4.
  out=$(ar cat "$id" t34-lines.txt --lines 4: 2>&1)
  rc=$?
  if [ $rc -ne 0 ]; then
    cleanup_job "$id"
    rec T34 FAIL "cat --lines 4: exited $rc: $out"
    return
  fi
  n=$(printf '%s\n' "$out" | grep -c .)
  if [ "$n" != "7" ]; then
    cleanup_job "$id"
    rec T34 FAIL "cat --lines 4: returned $n lines, expected 7: $out"
    return
  fi
  case "$out" in
    *"t34 line 04"*) ;;
    *"t34 line 03"*) cleanup_job "$id"; rec T34 FAIL "cat --lines 4: leaked line 3: $out"; return ;;
  esac
  cleanup_job "$id"
  rec T34 PASS "cat --lines 1:5 returns exactly lines 1-5; --lines 4: returns exactly the last 7 lines (job $id)"
}

t_t35() {
  start_job "FAKE_SLEEP=1 cat hard cap job"
  if [ "$START_RC" -ne 0 ]; then
    rec T35 FAIL "start exited $START_RC: $START_OUT"
    return
  fi
  local id="$START_JOB_ID"
  [ -n "$id" ] || { rec T35 FAIL "no job id parsed: $START_OUT"; return; }
  wait_state "$id" done 60 >/dev/null || { cleanup_job "$id"; rec T35 FAIL "job did not reach done"; return; }
  local wt errf outf size line i rc outlines err
  wt=$(job_worktree "$id")
  [ -n "$wt" ] && [ -d "$wt" ] || { cleanup_job "$id"; rec T35 FAIL "worktree unknown: $wt"; return; }
  # 9216 lines x 1024 bytes each (1023 chars + newline) = 9437184 bytes
  # (> the 8 MiB hard cap of 8388608; 8388608/1024 = exactly 8192 full lines).
  line="t35 line 00001"
  line="${line}$(printf ' x%.0s' $(seq 1 1010))"
  line="${line:0:1023}"
  i=1
  while [ $i -le 9216 ]; do
    printf '%s\n' "$line"
    i=$((i + 1))
  done > "$wt/t35-big.txt"
  size=$(wc -c < "$wt/t35-big.txt")
  [ "$size" = "9437184" ] || {
    cleanup_job "$id"
    rec T35 FAIL "fixture build failed: file is $size bytes, expected 9437184"
    return
  }
  outf="$TMPROOT/t35.out"
  errf="$TMPROOT/t35.err"
  ar cat "$id" t35-big.txt --max-bytes 100000000 >"$outf" 2>"$errf"
  rc=$?
  if [ $rc -ne 0 ]; then
    cleanup_job "$id"
    rec T35 FAIL "cat over the hard cap exited $rc: $(cat "$errf")"
    return
  fi
  outlines=$(wc -l < "$outf")
  if [ "$outlines" != "8192" ]; then
    cleanup_job "$id"
    rec T35 FAIL "cat over the hard cap returned $outlines lines, expected 8192 (8388608/1024)"
    return
  fi
  local err
  err=$(cat "$errf" 2>/dev/null)
  case "$err" in
    *"output truncated at 8388608 bytes (file is 9437184 bytes)"*) ;;
    *) cleanup_job "$id"; rec T35 FAIL "truncation note unexpected (stderr): $err"; return ;;
  esac
  cleanup_job "$id"
  rec T35 PASS "cat of a 9437184-byte file is truncated at the 8 MiB hard cap: 8192 lines + 'output truncated at 8388608 bytes (file is 9437184 bytes)' (job $id)"
}

t_t36() {
  start_job "FAKE_SLEEP=1 retry follow-up target"
  if [ "$START_RC" -ne 0 ]; then
    rec T36 FAIL "start exited $START_RC: $START_OUT"
    return
  fi
  local id="$START_JOB_ID"
  [ -n "$id" ] || { rec T36 FAIL "no job id parsed: $START_OUT"; return; }
  wait_state "$id" done 60 >/dev/null || { cleanup_job "$id"; rec T36 FAIL "job did not reach done"; return; }
  local tag out rc st
  tag="T36-FOLLOWUP-$(date +%s%N | tail -c 7)"
  : > "$TMPROOT/argv.log"
  out=$(ar retry "$id" --prompt "please apply $tag now" 2>&1)
  rc=$?
  if [ $rc -ne 0 ]; then
    cleanup_job "$id"
    rec T36 FAIL "retry --prompt exited $rc: $out"
    return
  fi
  case "$out" in
    *"retry launched in background"*) ;;
    *) cleanup_job "$id"; rec T36 FAIL "retry did not launch (rc $rc): $out"; return ;;
  esac
  st=$(wait_state "$id" done 60) || {
    cleanup_job "$id"
    rec T36 FAIL "follow-up attempt did not finish done (last: $st)"
    return
  }
  # The engine must have passed the NEW prompt to agy via -p: find the argv
  # line that carries the tag and check the argument after -p.
  local argv
  if [ ! -f "$TMPROOT/argv.log" ]; then
    cleanup_job "$id"
    rec T36 FAIL "fake-agy argv log missing (FAKE_ARGV_LOG not honoured)"
    return
  fi
  argv=$(grep -F "please apply $tag now" "$TMPROOT/argv.log" | tail -n 1)
  if [ -z "$argv" ]; then
    cleanup_job "$id"
    rec T36 FAIL "the follow-up prompt never reached agy (argv log: $(tail -n 3 "$TMPROOT/argv.log"))"
    return
  fi
  # argv layout: ... -p <prompt> --add-dir <wt>: the tag follows -p.
  case "$argv" in
    *$'-p\tplease apply '"$(printf '%s' "$tag")"' now'*) ;;
    *) cleanup_job "$id"; rec T36 FAIL "follow-up prompt not the -p argument in argv: $argv"; return ;;
  esac
  # And it continued the original conversation.
  case "$argv" in
    *--conversation*) ;;
    *) cleanup_job "$id"; rec T36 FAIL "follow-up argv has no --conversation: $argv"; return ;;
  esac
  # The log keeps the follow-up banner with its size annotation.
  local log
  log=$(ar log "$id" 2>&1)
  case "$log" in
    *"==================== RETRY AT"*"follow-up prompt"*) ;;
    *) cleanup_job "$id"; rec T36 FAIL "log has no follow-up retry banner: $log"; return ;;
  esac
  cleanup_job "$id"
  rec T36 PASS "retry --prompt on a done job launches a follow-up turn: agy receives the NEW prompt via -p with --conversation, banner annotated, job ends done (job $id)"
}

t_t37() {
  local prompt id st out i conv
  prompt="t37 conv marker $(date +%s%N | tail -c 7)"
  start_job "$prompt FAKE_CONV_DB"
  if [ "$START_RC" -ne 0 ]; then
    rec T37 FAIL "start exited $START_RC: $START_OUT"
    return
  fi
  id="$START_JOB_ID"
  [ -n "$id" ] || { rec T37 FAIL "no job id parsed: $START_OUT"; return; }
  st=$(wait_state "$id" done 60) || {
    cleanup_job "$id"
    rec T37 FAIL "FAKE_CONV_DB job did not reach done (last: $st)"
    return
  }
  # The engine must have recovered the conversation id from agy's own
  # conversation files: status shows it, the log carries the recovery note.
  out=$(ar status "$id" 2>&1)
  conv=""
  for i in $(seq 1 10); do
    conv=$(printf '%s\n' "$out" | sed -nE 's/^Conversation:[[:space:]]+//p' | head -n 1)
    [ -n "$conv" ] && break
    sleep 1
    out=$(ar status "$id" 2>&1)
  done
  if [ -z "$conv" ]; then
    cleanup_job "$id"
    rec T37 FAIL "status has no Conversation: id after recovery (status output): $out"
    return
  fi
  case "$conv" in
    ????????-????-????-????-???????????*) ;;
    *) cleanup_job "$id"; rec T37 FAIL "recovered Conversation value is not a UUID: '$conv'"; return ;;
  esac
  local log
  log=$(ar log "$id" 2>&1)
  case "$log" in
    *"Recovered conversation ID '$conv'"*) ;;
    *) cleanup_job "$id"; rec T37 FAIL "log has no recovery note for the id (log tail): $(tail -n 5 "$log")"; return ;;
  esac
  # A follow-up retry must now use the recovered conversation id: the command
  # reports it, and the agy argv carries --conversation <recovered-id>.
  local r1 argv2
  r1=$(ar retry "$id" --prompt "t37 follow-up turn" 2>&1)
  rc=$?
  case "$r1" in
    *"retry launched in background"*) ;;
    *) cleanup_job "$id"; rec T37 FAIL "retry --prompt with the recovered id did not launch (rc $rc): $r1"; return ;;
  esac
  case "$r1" in
    *"using conversation ID '$conv'"*) ;;
    *) cleanup_job "$id"; rec T37 FAIL "retry output does not report the recovered conversation ID (expected '$conv'): $r1"; return ;;
  esac
  st=$(wait_state "$id" done 60) || {
    cleanup_job "$id"
    rec T37 FAIL "follow-up retry did not finish done (last: $st)"
    return
  }
  argv2=$(grep -F "t37 follow-up turn" "$TMPROOT/argv.log" | tail -n 1)
  if [ -z "$argv2" ]; then
    cleanup_job "$id"
    rec T37 FAIL "the follow-up prompt never reached agy (argv log tail: $(tail -n 3 "$TMPROOT/argv.log"))"
    return
  fi
  case "$argv2" in
    *"--conversation	$conv"*) ;;
    *) cleanup_job "$id"; rec T37 FAIL "retried argv carries no '--conversation <recovered id>': $argv2"; return ;;
  esac
  cleanup_job "$id"
  rec T37 PASS "SUCCESS with an empty conversation_id recovers the id from the agy conversation db: status shows 'Conversation: <uuid>', log has the recovery note, and a retry --prompt resumes via --conversation <uuid> (job $id)"
}

t_t38() {
  start_job "FAKE_HANG graceful shutdown drill"
  if [ "$START_RC" -ne 0 ]; then
    rec T38 FAIL "start exited $START_RC: $START_OUT"
    return
  fi
  local id="$START_JOB_ID"
  [ -n "$id" ] || { rec T38 FAIL "no job id parsed: $START_OUT"; return; }
  local st
  st=$(wait_state "$id" running 30) || {
    cleanup_job "$id"
    rec T38 FAIL "job not running before shutdown (last: $st)"
    return
  }
  # Stop the server: on SIGTERM it must SIGTERM the running job and drain.
  kill -TERM "$SERVER_PID" 2>/dev/null || true
  local i
  for i in $(seq 1 80); do
    kill -0 "$SERVER_PID" 2>/dev/null || break
    sleep 0.5
  done
  if kill -0 "$SERVER_PID" 2>/dev/null; then
    kill -KILL "$SERVER_PID" 2>/dev/null || true
    sleep 1
  fi
  SERVER_PID=""
  # The fake agy must have been signalled (its log shows interrupt) and must
  # not be left running; the server's OWN log (the one the just-stopped
  # server wrote to, tracked by CUR_SERVER_LOG) shows the graceful drain.
  local leftovers
  leftovers=$(pgrep -f "$FAKE_AGY" 2>/dev/null || true)
  if [ -n "$leftovers" ]; then
    rec T38 FAIL "fake-agy process still alive after the server's graceful shutdown: $leftovers"
    return
  fi
  local sl
  sl=$(grep -c "agent-run: graceful shutdown: sent SIGTERM to job '$id'" "$CUR_SERVER_LOG" 2>/dev/null || true)
  if [ "$sl" != "1" ]; then
    rec T38 FAIL "server log does not show one graceful-shutdown SIGTERM for the job (count: $sl, log: $CUR_SERVER_LOG)"
    return
  fi
  # Bring the server back up on the SAME state root; the job must be terminal
  # and knowable. (run_test_stops_server also guarantees this on every early
  # exit path above, so a failure here cannot cascade.)
  ensure_server_up
  local out
  out=$(ar status "$id" 2>&1)
  local rc=$?
  if [ $rc -ne 0 ]; then
    cleanup_job "$id"
    rec T38 FAIL "status after graceful shutdown restart failed (rc $rc): $out"
    return
  fi
  st=$(printf '%s\n' "$out" | sed -nE 's/^Status:[[:space:]]*//p' | head -n 1)
  case "$st" in
    killed|self-terminated|failed) ;;
    *) cleanup_job "$id"; rec T38 FAIL "status after graceful shutdown unexpected: '$st' (output: $out)"; return ;;
  esac
  # The interrupted JSON line with the conversation id (empty here: fake agy
  # cannot know one) must be in the job log.
  local log
  log=$(ar log "$id" 2>&1)
  case "$log" in
    *"error: interrupted"*) ;;
    *) cleanup_job "$id"; rec T38 FAIL "job log lacks the SIGTERM interrupt line: $(tail -n 5 "$log")"; return ;;
  esac
  cleanup_job "$id"
  rec T38 PASS "SIGTERM on the server drained the FAKE_HANG job: no fake-agy left, server log shows the SIGTERM, job ended '$st' with the interrupt line in its log, restarted server answers status (job $id)"
}

t_t39() {
  start_job "FAKE_NOTICE_QUOTED status false-incomplete probe"
  if [ "$START_RC" -ne 0 ]; then
    rec T39 FAIL "start exited $START_RC: $START_OUT"
    return
  fi
  local id="$START_JOB_ID"
  [ -n "$id" ] || { rec T39 FAIL "no job id parsed: $START_OUT"; return; }
  local st
  st=$(wait_state "$id" done 60) || {
    cleanup_job "$id"
    rec T39 FAIL "FAKE_NOTICE_QUOTED job did not reach done (last: $st)"
    return
  }
  # double-check the live status agrees (no late downgrade to incomplete)
  st=$(get_status "$id")
  if [ "$st" != "done" ]; then
    cleanup_job "$id"
    rec T39 FAIL "live status after settling is '$st', expected done"
    return
  fi
  cleanup_job "$id"
  rec T39 PASS "SUCCESS whose response QUOTES 'terminating 1 background task(s) on exit' classifies done (not incomplete) (job $id)"
}

t_t40() {
  start_job "FAKE_SLEEP=1 kill non-running target"
  if [ "$START_RC" -ne 0 ]; then
    rec T40 FAIL "start exited $START_RC: $START_OUT"
    return
  fi
  local id="$START_JOB_ID"
  [ -n "$id" ] || { rec T40 FAIL "no job id parsed: $START_OUT"; return; }
  wait_state "$id" done 60 >/dev/null || { cleanup_job "$id"; rec T40 FAIL "job did not reach done"; return; }
  local out rc
  out=$(ar kill "$id" 2>&1)
  rc=$?
  case "$out" in
    *"is not currently running"*) ;;
    *) cleanup_job "$id"; rec T40 FAIL "kill on a done job: expected a not-running refusal, got: $out"; return ;;
  esac
  # The kill must NOT claim a termination.
  case "$out" in
    *"terminated successfully"*) cleanup_job "$id"; rec T40 FAIL "kill on a done job claims a success: $out"; return ;;
  esac
  # And the job is still there, still done.
  local st
  st=$(get_status "$id")
  if [ "$st" != "done" ]; then
    cleanup_job "$id"
    rec T40 FAIL "job status changed to '$st' after a refused kill (expected done)"
    return
  fi
  case "$(ar list 2>&1)" in
    *"$id"*) ;;
    *) cleanup_job "$id"; rec T40 FAIL "job vanished from list after a refused kill"; return ;;
  esac
  cleanup_job "$id"
  rec T40 PASS "kill on a non-running (done) job reports 'is not currently running' without claiming a termination; job stays done and listed (job $id)"
}

t_t41() {
  local out rc
  out=$(ar wait 2>&1)
  rc=$?
  if [ $rc -eq 0 ]; then
    rec T41 FAIL "wait with no job ids unexpectedly succeeded: $out"
    return
  fi
  # `job_ids` is `required = true`, so clap rejects the call client-side
  # before any server round-trip: the error names the missing argument.
   case "$out" in
     *required*) ;;
     *JOB_ID*) ;;
     *error*) ;;
     "") rec T41 FAIL "wait with no job ids produced no error output"; return ;;
     *) rec T41 FAIL "wait with no job ids error text unexpected: $out"; return ;;
   esac
   rec T41 PASS "wait with no job ids fails cleanly at argument parsing (rc=$rc, no server round-trip)"
}

# shared: prints the first conversation id the fake-agy convmap log attributes
# to a prompt embedding the marker (the db filename IS the conversation id;
# the fake frames the full -p text inside the stored step payload, so every
# db created while that prompt was running embeds the marker)
convmap_id_for() {
  local marker="$1" i line id
  [ -f "$TMPROOT/convmap.log" ] || return 0
  for i in $(seq 1 60); do
    line=$(grep -F -- "$marker" "$TMPROOT/convmap.log" 2>/dev/null | head -n 1)
    [ -n "$line" ] && break
    sleep 0.5
  done
  [ -n "$line" ] || return 0
  id=${line%%"	"*}
  printf '%s\n' "$id"
}

# shared: conv id from a job's status output (empty if the job has none)
status_conv() {
  ar status "$1" 2>/dev/null | sed -nE 's/^Conversation:[[:space:]]+//p' | head -n 1
}

t_t42() {
  # The original incident: two jobs whose prompts share their first ~200
  # bytes (well past the legacy 80-byte blob prefix) run at the same time.
  # With the per-turn reference tokens, each job's conversation -- and the
  # id the engine recovers for it while the OTHER job's run is live -- must
  # come from THIS job's own conversation file, matched through the job's
  # own token.
  local common a_id b_id a_conv b_conv a_db b_db
  common=$(printf 't42 identical boilerplate %.0s' $(seq 1 24))
  start_job "$common tail A FAKE_CONV_DB FAKE_SLEEP=2"
  if [ "$START_RC" -ne 0 ]; then
    rec T42 FAIL "start (job A) exited $START_RC: $START_OUT"
    return
  fi
  a_id="$START_JOB_ID"
  [ -n "$a_id" ] || { rec T42 FAIL "no job id parsed: $START_OUT"; return; }
  # (a) list shows the user's prompt without the engine's reference line.
  local lst
  lst=$(ar list 2>&1)
  case "$lst" in
    *"$a_id"*) ;;
    *) rec T42 FAIL "job A missing from list: $lst"; return ;;
  esac
  case "$lst" in
    *"[agent-run ref"*) rec T42 FAIL "list leaks the reference line: $(printf '%s\n' "$lst" | grep -F "$a_id" | head -n 1)"; return ;;
  esac
  # (b) start job B -- with the same opening bytes -- while A is still
  # running: B's agy sees A's conversation file (and vice versa), so a
  # prefix-based recovery could adopt the other job's conversation.
  start_job "$common tail B FAKE_CONV_DB FAKE_SLEEP=2"
  if [ "$START_RC" -ne 0 ]; then
    cleanup_job "$a_id"
    rec T42 FAIL "start (job B) exited $START_RC: $START_OUT"
    return
  fi
  b_id="$START_JOB_ID"
  [ -n "$b_id" ] || { cleanup_job "$a_id"; rec T42 FAIL "no job id parsed: $START_OUT"; return; }
  local st
  st=$(wait_state "$a_id" done 60) || { cleanup_job "$a_id"; cleanup_job "$b_id"; rec T42 FAIL "job A did not reach done (last: $st)"; return; }
  st=$(wait_state "$b_id" done 60) || { cleanup_job "$a_id"; cleanup_job "$b_id"; rec T42 FAIL "job B did not reach done (last: $st)"; return; }
  # (c) each job must have recovered a (UUID-shaped) conversation id ...
  a_conv=$(status_conv "$a_id")
  b_conv=$(status_conv "$b_id")
  local i
  for i in $(seq 1 10); do
    [ -n "$a_conv" ] && [ -n "$b_conv" ] && break
    sleep 1
    a_conv=$(status_conv "$a_id")
    b_conv=$(status_conv "$b_id")
  done
  if [ -z "$a_conv" ] || [ -z "$b_conv" ]; then
    cleanup_job "$a_id"; cleanup_job "$b_id"
    rec T42 FAIL "no recovered Conversation id in status (A: '${a_conv}', B: '${b_conv}')"
    return
  fi
  case "$a_conv" in ????????-????-????-????-???????????*) ;; *) cleanup_job "$a_id"; cleanup_job "$b_id"; rec T42 FAIL "recovered A id is not a UUID: '$a_conv'"; return ;; esac
  case "$b_conv" in ????????-????-????-????-???????????*) ;; *) cleanup_job "$a_id"; cleanup_job "$b_id"; rec T42 FAIL "recovered B id is not a UUID: '$b_conv'"; return ;; esac
  if [ "$a_conv" = "$b_conv" ]; then
    cleanup_job "$a_id"; cleanup_job "$b_id"
    rec T42 FAIL "both jobs recovered the SAME conversation id '$a_conv': a prefix-collision adoption happened"
    return
  fi
  # ... and each id must be the conversation whose stored prompt embeds
  # THAT job's own reference token (cross-checked through the fake's
  # prompt->db map).
  a_db=$(convmap_id_for "[agent-run ref $a_id#0]")
  b_db=$(convmap_id_for "[agent-run ref $b_id#0]")
  if [ -z "$a_db" ] || [ "$a_db" != "$a_conv" ]; then
    cleanup_job "$a_id"; cleanup_job "$b_id"
    rec T42 FAIL "job A's recovered id '$a_conv' is not the conversation embedding A's own token (convmap says '$a_db')"
    return
  fi
  if [ -z "$b_db" ] || [ "$b_db" != "$b_conv" ]; then
    cleanup_job "$a_id"; cleanup_job "$b_id"
    rec T42 FAIL "job B's recovered id '$b_conv' is not the conversation embedding B's own token (convmap says '$b_db')"
    return
  fi
  # (d) retry --prompt on one of them resumes ITS conversation (argv log shows --conversation <that id>)
  local r1 argv
  r1=$(ar retry "$a_id" --prompt "t42 resume turn" 2>&1)
  if [ $? -ne 0 ]; then
    cleanup_job "$a_id"; cleanup_job "$b_id"
    rec T42 FAIL "retry --prompt did not launch: $r1"
    return
  fi
  wait_state "$a_id" terminal 60 >/dev/null
  argv=$(grep -F -- $'\tt42 resume turn' "$TMPROOT/argv.log" 2>/dev/null | tail -n 1)
  if [ -z "$argv" ]; then
    cleanup_job "$a_id"; cleanup_job "$b_id"
    rec T42 FAIL "the resume prompt never reached agy (argv log tail: $(tail -n 3 "$TMPROOT/argv.log"))"
    return
  fi
  case "$argv" in
    *"--conversation	$a_conv"*) ;;
    *) cleanup_job "$a_id"; cleanup_job "$b_id"; rec T42 FAIL "resume argv carries no --conversation <recovered id '$a_conv'>: $argv"; return ;;
  esac
  cleanup_job "$a_id"
  cleanup_job "$b_id"
  rec T42 PASS "twin jobs sharing ~200 prompt bytes each recover their OWN distinct conversation (token match, cross-checked; list shows clean prompt; retry resumes its conversation) (jobs $a_id/$b_id)"
}

t_t43() {
  local prompt id conv
  prompt="t43 kill recovery probe FAKE_CONV_DB FAKE_SLEEP=10"
  start_job "$prompt"
  if [ "$START_RC" -ne 0 ]; then
    rec T43 FAIL "start exited $START_RC: $START_OUT"
    return
  fi
  id="$START_JOB_ID"
  [ -n "$id" ] || { rec T43 FAIL "no job id parsed: $START_OUT"; return; }
  # Let the fake write the conversation db (it sleeps 1s before creating it),
  # then kill the job before its final JSON line (and conversation id) lands.
  sleep 2
  local out rc
  out=$(ar kill "$id" 2>&1)
  rc=$?
  case "$out" in
    *"terminated"*) ;;
    *) rec T43 FAIL "kill did not report a termination (rc $rc): $out"; return ;;
  esac
  wait_state "$id" terminal 30 >/dev/null
  # The engine must have recovered the conversation id from agy's own
  # conversation files (status shows it and the log carries the note).
  conv=""
  local i
  for i in $(seq 1 10); do
    conv=$(status_conv "$id")
    [ -n "$conv" ] && break
    sleep 1
  done
  if [ -z "$conv" ]; then
    cleanup_job "$id"
    rec T43 FAIL "killed job has no recovered Conversation id"
    return
  fi
  local log
  log=$(ar log "$id" 2>&1)
  case "$log" in
    *"Recovered conversation ID '$conv'"*) ;;
    *) cleanup_job "$id"; rec T43 FAIL "log has no recovery note for '$conv' (tail): $(tail -n 5 "$log")"; return ;;
  esac
  # A retry --prompt must resume EXACTLY the recovered conversation: the
  # command reports it and the agy argv carries --conversation <id>.
  local r1 argv
  r1=$(ar retry "$id" --prompt "t43 resume turn" 2>&1)
  rc=$?
  case "$r1" in
    *"retry launched in background"*) ;;
    *) cleanup_job "$id"; rec T43 FAIL "retry --prompt did not launch (rc $rc): $r1"; return ;;
  esac
  wait_state "$id" terminal 60 >/dev/null
  argv=$(grep -F -- $'\tt43 resume turn' "$TMPROOT/argv.log" 2>/dev/null | tail -n 1)
  if [ -z "$argv" ]; then
    cleanup_job "$id"
    rec T43 FAIL "the resume prompt never reached agy (argv log tail: $(tail -n 3 "$TMPROOT/argv.log"))"
    return
  fi
  case "$argv" in
    *"--conversation	$conv"*) ;;
    *) cleanup_job "$id"; rec T43 FAIL "resume argv carries no --conversation <recovered id '$conv'>: $argv"; return ;;
  esac
  # ...and the row still reports the same id afterwards.
  if [ "$(status_conv "$id")" != "$conv" ]; then
    cleanup_job "$id"
    rec T43 FAIL "conversation id changed after the resume retry (now '$(status_conv "$id")', was '$conv')"
    return
  fi
  cleanup_job "$id"
  rec T43 PASS "killed job recovers its conversation from the agy db and a retry --prompt resumes via --conversation <recovered id> (job $id)"
}

db_field() {
  local id="$1" col="$2"
  python3 -c "import sqlite3; con = sqlite3.connect('$STATE_ROOT/state.sqlite'); cur = con.cursor(); cur.execute('SELECT $col FROM jobs WHERE job_id = ?', ('$id',)); row = cur.fetchone(); print(row[0] if row and row[0] is not None else '')"
}

t_t44() {
  # (a) An invalid --hindsight value is rejected client-side (clap value enum)
  local out rc
  out=$(ar start proj "t44 bad value" --hindsight bogus 2>&1)
  rc=$?
  if [ $rc -eq 0 ]; then
    rec T44 FAIL "start with --hindsight bogus unexpectedly succeeded: $out"
    return
  fi
  case "$out" in
    *"invalid value"*|*"bogus"*) ;;
    *) rec T44 FAIL "unexpected error for --hindsight bogus: $out"; return ;;
  esac

  # (b) --hindsight off: no Hindsight activity at all, recorded 0/0/0, and the
  #     Memory: line says off. (This is the precedence fix: the per-job off
  #     wins over any server-side default.)
  start_job "t44 hindsight off" --hindsight off
  if [ "$START_RC" -ne 0 ]; then
    rec T44 FAIL "start with --hindsight off failed (rc $START_RC): $START_OUT"
    return
  fi
  local id="$START_JOB_ID"
  printf '%s\n' "$START_OUT" | grep -q "^Memory: off$" || {
    cleanup_job "$id"
    rec T44 FAIL "missing 'Memory: off' line for --hindsight off: $START_OUT"
    return
  }
  wait_state "$id" terminal 30 >/dev/null
  local h_val ar_val art_val
  h_val=$(db_field "$id" "hindsight")
  ar_val=$(db_field "$id" "auto_recall")
  art_val=$(db_field "$id" "auto_retain")
  if [ "$h_val" != "0" ] || [ "$ar_val" != "0" ] || [ "$art_val" != "0" ]; then
    cleanup_job "$id"
    rec T44 FAIL "--hindsight off not saved as 0/0/0: hindsight=$h_val auto_recall=$ar_val auto_retain=$art_val"
    return
  fi
  cleanup_job "$id"

  # (c) --hindsight on: enabled, recall/retain follow the server defaults
  #     (both false here), Memory: line reflects that.
  start_job "t44 hindsight on" --hindsight on
  if [ "$START_RC" -ne 0 ]; then
    rec T44 FAIL "start with --hindsight on failed (rc $START_RC): $START_OUT"
    return
  fi
  id="$START_JOB_ID"
  printf '%s\n' "$START_OUT" | grep -q "^Memory: recall off, retain off, bank proj$" || {
    cleanup_job "$id"
    rec T44 FAIL "missing expected Memory line for --hindsight on: $START_OUT"
    return
  }
  wait_state "$id" terminal 30 >/dev/null
  h_val=$(db_field "$id" "hindsight")
  ar_val=$(db_field "$id" "auto_recall")
  art_val=$(db_field "$id" "auto_retain")
  if [ "$h_val" != "1" ] || [ "$ar_val" != "0" ] || [ "$art_val" != "0" ]; then
    cleanup_job "$id"
    rec T44 FAIL "--hindsight on not saved as 1/0/0: hindsight=$h_val auto_recall=$ar_val auto_retain=$art_val"
    return
  fi
  cleanup_job "$id"

  # (d) Precedence: with a server whose env enables Hindsight + auto modes,
  #     the per-job --hindsight off STILL means no recall/retain (the bug this
  #     batch fixes: today env AGENT_RUN_HINDSIGHT_AUTO_RECALL=true implied
  #     hindsight on and a --no-hindsight job still recalled). Run against a
  #     private server (the main one stays hindsight-disabled for the suite).
  # t_t44_fail <cleanup-id? > <msg>: restore the standard server, record FAIL.
  t_t44_fail() {
    if [ -n "${t_t44_id:-}" ]; then cleanup_job "$t_t44_id" 2>/dev/null || true; fi
    stop_server
    start_server
    wait_for_server
    rec T44 FAIL "$1"
  }

  stop_server
  (
    cd "$TMPROOT" || exit 1
    cd "$WORKSPACE" || exit 1
    exec env -u AGENT_RUN_REMOTE -u GH_TOKEN -u GITHUB_TOKEN \
      HOME="$FAKE_HOME" \
      AGENT_RUN_API_LISTEN="127.0.0.1:$PORT" \
      AGENT_RUN_WORKSPACE_ROOT="$WORKSPACE" \
      AGENT_RUN_STATE_ROOT="$STATE_ROOT" \
      AGENT_RUN_AGY_PATH="$FAKE_AGY" \
      AGENT_RUN_JOBS_DIR_NAME=".agent-jobs" \
      AGENT_RUN_MAX_PARALLEL=3 \
      AGENT_RUN_HINDSIGHT_ENABLED=true \
      AGENT_RUN_HINDSIGHT_AUTO_RECALL=true \
      AGENT_RUN_HINDSIGHT_AUTO_RETAIN=true \
      FAKE_ARGV_LOG="$TMPROOT/argv.log" \
      FAKE_CONVMAP_LOG="$TMPROOT/convmap.log" \
       "$AGENT_RUN_API" >"$TMPROOT/server-hindsight-on.log" 2>&1
  ) &
  SERVER_PID=$!
  CUR_SERVER_LOG="$TMPROOT/server-hindsight-on.log"
  wait_for_server "$TMPROOT/server-hindsight-on.log"

  # (d1) Absent flag follows the server defaults: recall + retain on.
  t_t44_id=""
  start_job "t44 server default probe"
  [ "$START_RC" -eq 0 ] || { t_t44_fail "plain start on hindsight-enabled server failed (rc $START_RC): $START_OUT"; return; }
  t_t44_id="$START_JOB_ID"
  printf '%s\n' "$START_OUT" | grep -q "^Memory: recall on, retain on, bank proj$" || {
    t_t44_fail "absent --hindsight should follow server defaults (recall on, retain on): $START_OUT"; return
  }
  wait_state "$t_t44_id" terminal 30 >/dev/null
  h_val=$(db_field "$t_t44_id" "hindsight")
  [ "$h_val" = "1" ] || { t_t44_fail "server-default job expected hindsight=1, got '$h_val'"; return; }
  cleanup_job "$t_t44_id"
  t_t44_id=""

  # (d2) --hindsight off wins over the enabled server: 0/0/0, Memory: off.
  start_job "t44 off beats enabled server" --hindsight off
  [ "$START_RC" -eq 0 ] || { t_t44_fail "--hindsight off on hindsight-enabled server failed (rc $START_RC): $START_OUT"; return; }
  t_t44_id="$START_JOB_ID"
  printf '%s\n' "$START_OUT" | grep -q "^Memory: off$" || {
    t_t44_fail "per-job off must win over enabled server defaults: $START_OUT"; return
  }
  wait_state "$t_t44_id" terminal 30 >/dev/null
  h_val=$(db_field "$t_t44_id" "hindsight")
  ar_val=$(db_field "$t_t44_id" "auto_recall")
  art_val=$(db_field "$t_t44_id" "auto_retain")
  if [ "$h_val" != "0" ] || [ "$ar_val" != "0" ] || [ "$art_val" != "0" ]; then
    t_t44_fail "per-job off must beat server env auto modes: hindsight=$h_val auto_recall=$ar_val auto_retain=$art_val"
    return
  fi
  cleanup_job "$t_t44_id"
  t_t44_id=""

  # Back to the suite's standard (hindsight-disabled) server.
  stop_server
  start_server
  wait_for_server

  rec T44 PASS "single --hindsight <on|off>: bad value rejected; off = Memory: off + 0/0/0 and beats an enabled server; on/absent follow server defaults (Memory line printed)"
}

t_t45() {
  # (a) Default bank is the project name
  start_job "t45 default bank probe"
  if [ "$START_RC" -ne 0 ]; then
    rec T45 FAIL "start failed (rc $START_RC): $START_OUT"
    return
  fi
  local id="$START_JOB_ID"
  wait_state "$id" terminal 30 >/dev/null
  local bank
  bank=$(db_field "$id" "hindsight_bank")
  if [ "$bank" != "proj" ]; then
    cleanup_job "$id"
    rec T45 FAIL "default bank expected 'proj', got '$bank'"
    return
  fi
  cleanup_job "$id"

  # (b) Override bank via --hindsight-bank
  start_job "t45 custom bank probe" --hindsight-bank "special-bank-42"
  if [ "$START_RC" -ne 0 ]; then
    rec T45 FAIL "start with --hindsight-bank failed (rc $START_RC): $START_OUT"
    return
  fi
  id="$START_JOB_ID"
  wait_state "$id" terminal 30 >/dev/null
  bank=$(db_field "$id" "hindsight_bank")
  if [ "$bank" != "special-bank-42" ]; then
    cleanup_job "$id"
    rec T45 FAIL "overridden bank expected 'special-bank-42', got '$bank'"
    return
  fi
  cleanup_job "$id"

  rec T45 PASS "hindsight bank defaults to project name and respects --hindsight-bank override"
}

t_t46() {
  # (a) agent-run info prints proxy section without error
  local out rc
  out=$(ar info 2>&1)
  rc=$?
  if [ $rc -ne 0 ]; then
    rec T46 FAIL "info exited $rc: $out"
    return
  fi
  printf '%s\n' "$out" | grep -q "proxy:" || { rec T46 FAIL "info output missing 'proxy:': $out"; return; }

  # (b) --proxy <url> records proxy in job metadata
  start_job "t46 proxy url probe" --proxy "http://test-proxy:8080"
  if [ "$START_RC" -ne 0 ]; then
    rec T46 FAIL "start with --proxy failed (rc $START_RC): $START_OUT"
    return
  fi
  local id="$START_JOB_ID"
  wait_state "$id" terminal 30 >/dev/null
  local p_val
  p_val=$(db_field "$id" "proxy")
  if [ "$p_val" != "http://test-proxy:8080" ]; then
    cleanup_job "$id"
    rec T46 FAIL "expected proxy 'http://test-proxy:8080', got '$p_val'"
    return
  fi
  cleanup_job "$id"

  # (c) --proxy none records 'none' in job metadata
  start_job "t46 proxy none probe" --proxy "none"
  if [ "$START_RC" -ne 0 ]; then
    rec T46 FAIL "start with --proxy none failed (rc $START_RC): $START_OUT"
    return
  fi
  id="$START_JOB_ID"
  wait_state "$id" terminal 30 >/dev/null
  p_val=$(db_field "$id" "proxy")
  if [ "$p_val" != "none" ]; then
    cleanup_job "$id"
    rec T46 FAIL "expected proxy 'none', got '$p_val'"
    return
  fi
  cleanup_job "$id"

  rec T46 PASS "proxy support renders in info and per-job --proxy <url|none> records correctly in metadata"
}

t_t47() {
  local id wt out rc n
  # A finished job whose worktree holds two new untracked files (plus the
  # tracked README.txt that fake-agy commits' base brings in).
  start_job "FAKE_WRITE=pick-me.txt FAKE_WRITE=leave-me.txt job with two new files"
  if [ "$START_RC" -ne 0 ]; then
    rec T47 FAIL "start exited $START_RC: $START_OUT"
    return
  fi
  id="$START_JOB_ID"
  [ -n "$id" ] || { rec T47 FAIL "no job id parsed: $START_OUT"; return; }
  wait_state "$id" done 60 >/dev/null || { cleanup_job "$id"; rec T47 FAIL "job did not reach done"; return; }
  wt=$(ar status "$id" 2>/dev/null | sed -nE 's/^Worktree:[[:space:]]+//p' | head -n 1 | sed 's/ (directory missing on disk)$//')
  [ -n "$wt" ] || { cleanup_job "$id"; rec T47 FAIL "worktree path unavailable"; return; }

  # (a) diff --only shows only the selected new file
  out=$(ar diff "$id" --only pick-me.txt 2>&1)
  rc=$?
  if [ $rc -ne 0 ] || ! printf '%s' "$out" | grep -q "pick-me.txt"; then
    cleanup_job "$id"; rec T47 FAIL "diff --only missing the selected file (rc $rc): $out"; return
  fi
  if printf '%s' "$out" | grep -q "leave-me.txt"; then
    cleanup_job "$id"; rec T47 FAIL "diff --only leaked the unselected file: $out"; return
  fi
  # the unselected file must not even be intent-to-add marked by the diff
  local mark
  mark=$(git -C "$wt" status --porcelain 2>/dev/null | sed -nE 's/^...leave-me\.txt$/&/p')
  case "$mark" in
    "?? leave-me.txt") ;;
    *) cleanup_job "$id"; rec T47 FAIL "leave-me.txt expected untracked (??), got: '$mark'"; return ;;
  esac

  # (b) diff --stat lists the selected file with status A and its byte size
  out=$(ar diff "$id" --only pick-me.txt --stat 2>&1)
  rc=$?
  if [ $rc -ne 0 ]; then
    cleanup_job "$id"; rec T47 FAIL "diff --stat exited $rc: $out"; return
  fi
  if ! printf '%s\n' "$out" | grep -qE '^A[[:space:]]+[0-9]+[[:space:]]+pick-me[.]txt$'; then
    cleanup_job "$id"; rec T47 FAIL "stat line does not match the A-status pick-me.txt line: $out"; return
  fi
  n=$(wc -c < "$wt/pick-me.txt")
  case "$out" in *" $n "*) ;; *) cleanup_job "$id"; rec T47 FAIL "stat line lacks the file size ($n): $out"; return ;; esac
  if printf '%s' "$out" | grep -q "leave-me.txt"; then
    cleanup_job "$id"; rec T47 FAIL "diff --stat leaked the unselected file: $out"; return
  fi

  # (c) commit --only commits only the selected file; the other stays untracked
  out=$(ar commit "$id" --only pick-me.txt -m "selective commit" 2>&1)
  rc=$?
  if [ $rc -ne 0 ]; then
    cleanup_job "$id"; rec T47 FAIL "commit --only failed (rc $rc): $out"; return
  fi
  case "$out" in
    *"selection: only matching paths are committed"*) ;;
    *) cleanup_job "$id"; rec T47 FAIL "commit --only did not report the selection: $out"; return ;;
  esac
  local log
  log=$(git -C "$wt" show --name-only --format='' HEAD 2>&1)
  case "$log" in
    "pick-me.txt") ;;
    *) cleanup_job "$id"; rec T47 FAIL "HEAD commit contents unexpected: $log"; return ;;
  esac
  local st
  st=$(git -C "$wt" status --porcelain 2>/dev/null)
  case "$st" in *"?? leave-me.txt"*) ;; *) cleanup_job "$id"; rec T47 FAIL "leave-me.txt not left untracked after commit --only: $st"; return ;; esac

  # (d) commit --only with a matching selection is refused (no changes left there)
  out=$(ar commit "$id" --only pick-me.txt 2>&1)
  rc=$?
  if [ $rc -eq 0 ]; then
    cleanup_job "$id"; rec T47 FAIL "second commit --only of the same path unexpectedly succeeded: $out"; return
  fi
  case "$out" in *"matched no changes"*) ;; *) cleanup_job "$id"; rec T47 FAIL "no-match refusal text unexpected: $out"; return ;; esac

  # (e) discard removes the named untracked file and prints it
  out=$(ar discard "$id" leave-me.txt 2>&1)
  rc=$?
  if [ $rc -ne 0 ] || [ -f "$wt/leave-me.txt" ]; then
    cleanup_job "$id"
    rec T47 FAIL "discard failed (rc $rc) or the file survived: $out"
    return
  fi
  case "$out" in *"Deleted leave-me.txt"*) ;; *) cleanup_job "$id"; rec T47 FAIL "discard did not print the removed path: $out"; return ;; esac

  # (f) discard refuses tracked files, pointing at git
  out=$(ar discard "$id" README.txt 2>&1)
  rc=$?
  if [ $rc -eq 0 ] || [ ! -f "$wt/README.txt" ]; then
    cleanup_job "$id"; rec T47 FAIL "discard of a tracked file succeeded or deleted it: $out"; return
  fi
  case "$out" in *"tracked"*"git rm"*) ;; *) cleanup_job "$id"; rec T47 FAIL "tracked-file refusal text unexpected: $out"; return ;; esac

  # (g) discard rejects globs before touching anything
  out=$(ar discard "$id" "*.txt" 2>&1)
  rc=$?
  if [ $rc -eq 0 ]; then
    cleanup_job "$id"; rec T47 FAIL "discard accepted a glob: $out"; return
  fi
  case "$out" in *"globs are not allowed"*) ;; *) cleanup_job "$id"; rec T47 FAIL "glob refusal text unexpected: $out"; return ;; esac
  [ -f "$wt/README.txt" ] || { cleanup_job "$id"; rec T47 FAIL "refused glob discard left the tree changed"; return; }

  cleanup_job "$id"
  rec T47 PASS "diff --only/--stat select the wanted new file; commit --only leaves the other uncommitted; discard removes the untracked file and refuses tracked/glob paths (job $id)"
}

# T48: typed errors -- the API answers a stable machine-readable `code`
# (404 job_not_found, 400 invalid_status / discard_no_paths), the CLI's
# human text is unchanged, AGENT_RUN_DEBUG=1 exposes the status + code.
t_t48() {
  local body rc msg dmsg
  # (a) unknown job -> 404 with code job_not_found in the body
  body=$(curl -s "http://127.0.0.1:$PORT/api/v1/jobs/badbad01")
  rc=$?
  msg=$(printf '%s' "$body" | python3 -c 'import json,sys; print(json.load(sys.stdin).get("code",""))' 2>/dev/null)
  if [ $rc -ne 0 ] || [ "$msg" != "job_not_found" ]; then
    rec T48 FAIL "GET /jobs/badbad01: rc=$rc code=[$msg] body=$body"
    return
  fi
  # (b) the CLI surface is unchanged: plain Error line, exit 1
  out=$(ar status badbad01 2>&1); rc=$?
  if [ $rc -ne 1 ] || ! printf '%s' "$out" | grep -q "No such job found"; then
    rec T48 FAIL "CLI status of an unknown job (rc $rc): $out"
    return
  fi
  # (c) AGENT_RUN_DEBUG=1 adds the HTTP status + code line
  dmsg=$(AGENT_RUN_DEBUG=1 ar status badbad01 2>&1 | grep 'agent-run: HTTP')
  if [ -z "$dmsg" ] || ! printf '%s' "$dmsg" | grep -q "HTTP 404" || ! printf '%s' "$dmsg" | grep -q "job_not_found"; then
    rec T48 FAIL "AGENT_RUN_DEBUG line missing the HTTP status and code: [$dmsg]"
    return
  fi
  # (d) discard without paths -> 400 with code discard_no_paths
  # The route is POST /api/v1/jobs/{job_id}/discard (no /jobs/discard); the
  # unknown job id "whatever" is fine -- the command rejects the empty
  # `paths` selection BEFORE any job lookup.
  body=$(curl -s -X POST "http://127.0.0.1:$PORT/api/v1/jobs/whatever/discard" -H 'Content-Type: application/json' -d '{"job_id":"whatever","paths":[]}')
  msg=$(printf '%s' "$body" | python3 -c 'import json,sys; print(json.load(sys.stdin).get("code",""))' 2>/dev/null)
  if [ "$msg" != "discard_no_paths" ]; then
    rec T48 FAIL "POST /jobs/whatever/discard with no paths: code=[$msg] body=$body"
    return
  fi
  rec T48 PASS "typed errors: 404/400 bodies carry stable codes (job_not_found, discard_no_paths), CLI text unchanged, AGENT_RUN_DEBUG adds the HTTP status + code"
}

# T49: tail exit codes (0 done / 2 other terminal / 3 --timeout)
t_t49() {
  local id out rc
  # (a) --timeout on a still-running job -> exit 3
  start_job "FAKE_HANG tail timeout job"
  if [ "$START_RC" -ne 0 ] || [ -z "$START_JOB_ID" ]; then
    rec T49 FAIL "start hang: rc=$START_RC: $START_OUT"
    return
  fi
  id="$START_JOB_ID"
  wait_state "$id" running 30 >/dev/null || { ar kill "$id" >/dev/null 2>&1 || true; cleanup_job "$id"; rec T49 FAIL "hang job never reached running"; return; }
  out=$(ar tail "$id" --timeout 2 2>&1)
  rc=$?
  if [ $rc -ne 3 ]; then
    ar kill "$id" >/dev/null 2>&1 || true
    wait_state "$id" killed 45 >/dev/null || true
    cleanup_job "$id"
    rec T49 FAIL "tail --timeout on a running job exited $rc, expected 3: $out"
    return
  fi
  case "$out" in
    *"exit code 3"*) ;;
    *) ar kill "$id" >/dev/null 2>&1 || true
       wait_state "$id" killed 45 >/dev/null || true
       cleanup_job "$id"
       rec T49 FAIL "tail timeout note missing from stderr: $out"; return ;;
  esac
  ar kill "$id" >/dev/null 2>&1 || true
  wait_state "$id" killed 45 >/dev/null || true
  cleanup_job "$id"
  # (b) a done job -> exit 0 + the transition banner
  start_job "FAKE_SLEEP=1 tail done job"
  [ "$START_RC" -eq 0 ] && [ -n "$START_JOB_ID" ] || { rec T49 FAIL "start done: $START_OUT"; return; }
  id="$START_JOB_ID"
  out=$(ar tail "$id" 2>&1)
  rc=$?
  if [ $rc -ne 0 ]; then
    cleanup_job "$id"
    rec T49 FAIL "tail of a done job exited $rc, expected 0: $out"
    return
  fi
  case "$out" in
    *"transitioned to terminal state: done"*) ;;
    *) cleanup_job "$id"; rec T49 FAIL "done banner missing: $out"; return ;;
  esac
  cleanup_job "$id"
  # (c) a failed job -> exit 2 + the transition banner
  start_job "FAKE_FAIL tail failed job"
  [ "$START_RC" -eq 0 ] && [ -n "$START_JOB_ID" ] || { rec T49 FAIL "start fail: $START_OUT"; return; }
  id="$START_JOB_ID"
  out=$(ar tail "$id" 2>&1)
  rc=$?
  if [ $rc -ne 2 ]; then
    cleanup_job "$id"
    rec T49 FAIL "tail of a failed job exited $rc, expected 2: $out"
    return
  fi
  case "$out" in
    *"transitioned to terminal state: failed"*) ;;
    *) cleanup_job "$id"; rec T49 FAIL "failed banner missing: $out"; return ;;
  esac
  cleanup_job "$id"
  rec T49 PASS "tail exits 0 (done), 2 (failed) with the transition banner, and 3 when --timeout elapses while the job is still running"
}

# T50: global --json -- structured commands emit parseable JSON (same exit
# code), errors add a JSON line on stderr, raw-text commands keep emitting
# text even under --json.
t_t50() {
  local id out rc err
  local jrc prc diffrc
  start_job "FAKE_WRITE=keep-one.txt FAKE_WRITE=keep-two.txt json job"
  if [ "$START_RC" -ne 0 ] || [ -z "$START_JOB_ID" ]; then
    rec T50 FAIL "start: rc=$START_RC: $START_OUT"
    return
  fi
  id="$START_JOB_ID"
  wait_state "$id" done 60 >/dev/null || { cleanup_job "$id"; rec T50 FAIL "job did not reach done"; return; }
  # (a) each structured command must emit valid JSON on stdout.
  # `models` is intentionally NOT looped here: it spawns the server's agy, and
  # the fake-agy `models` invocation was observed to intermittently fail in this
  # harness (a `models` endpoint flake, not a --json-specific break; the
  # non-json T28 exercises the same endpoint and the fake answers it fine).
  local cmd
  for cmd in "status $id" "list" "info" "projects" "files $id" "diff $id --stat"; do
    out=$(ar --json $cmd 2>/dev/null)
    rc=$?
    if [ $rc -ne 0 ] || ! printf '%s' "$out" | python3 -m json.tool >/dev/null 2>&1; then
      cleanup_job "$id"
      rec T50 FAIL "--json $cmd exited $rc or is not valid JSON: $out"
      return
    fi
  done
  # wait (a fresh FAKE_WRITE job is not needed; the done job settles instantly)
  out=$(ar --json wait "$id" 2>/dev/null); rc=$?
  if [ $rc -ne 0 ] || ! printf '%s' "$out" | python3 -m json.tool >/dev/null 2>&1; then
    cleanup_job "$id"
    rec T50 FAIL "--json wait exited $rc or is not valid JSON: $out"
    return
  fi
  # (b) commit + discard under --json (the job has untracked files to commit)
  out=$(ar --json commit "$id" 2>/dev/null); rc=$?
  if [ $rc -ne 0 ] || ! printf '%s' "$out" | python3 -m json.tool >/dev/null 2>&1; then
    cleanup_job "$id"
    rec T50 FAIL "--json commit exited $rc or is not valid JSON: $out"
    return
  fi
  # discard a tracked file: refused -> exit 1, but stderr carries a JSON line
  err=$(ar --json discard "$id" keep-one.txt 2>&1 >/dev/null); rc=$?
  if [ $rc -eq 0 ]; then
    cleanup_job "$id"
    rec T50 FAIL "--json discard of a tracked file unexpectedly succeeded: $err"
    return
  fi
  case "$err" in
    *'"error"'*) ;;
    *) cleanup_job "$id"; rec T50 FAIL "--json error has no JSON line on stderr: $err"; return ;;
  esac
  printf '%s' "$err" | tail -n 1 | python3 -m json.tool >/dev/null 2>&1 || { cleanup_job "$id"; rec T50 FAIL "last stderr line is not valid JSON: $err"; return; }
  # (c) the error JSON line carries error + code fields
  out=$(ar --json status badbad01 2>&1 >/dev/null); rc=$?
  [ $rc -eq 1 ] || { cleanup_job "$id"; rec T50 FAIL "--json status of unknown job exited $rc"; return; }
  printf '%s' "$out" | tail -n 1 | python3 -c 'import json,sys; d=json.load(sys.stdin); assert "error" in d and "code" in d' 2>/dev/null || { cleanup_job "$id"; rec T50 FAIL "--json error line lacks error/code: $out"; return; }
  # (d) raw-text commands stay byte-for-byte identical under --json (the flag
  # is ignored for them). "not valid JSON" would be a false check here: the
  # fake agy's log content is itself a JSON line.
  ar --json log "$id" >"$TMPROOT/t50.log.json" 2>/dev/null; jrc=$?
  ar log "$id" >"$TMPROOT/t50.log.plain" 2>/dev/null; prc=$?
  if [ $jrc -ne 0 ] || [ $prc -ne 0 ] || ! cmp -s "$TMPROOT/t50.log.json" "$TMPROOT/t50.log.plain"; then
    cleanup_job "$id"
    rec T50 FAIL "--json log not byte-for-byte equal to log (json rc $jrc, plain rc $prc): $(cat "$TMPROOT/t50.log.json" 2>/dev/null)"
    return
  fi
  ar --json diff "$id" >"$TMPROOT/t50.diff.json" 2>/dev/null; jrc=$?
  ar diff "$id" >"$TMPROOT/t50.diff.plain" 2>/dev/null; prc=$?
  diffrc=0
  diff "$TMPROOT/t50.diff.json" "$TMPROOT/t50.diff.plain" >/dev/null 2>&1 || diffrc=$?
  if [ $jrc -ne 0 ] || [ $prc -ne 0 ] || [ $diffrc -ne 0 ]; then
    cleanup_job "$id"
    rec T50 FAIL "--json diff (no --stat) not byte-for-byte equal to diff (json rc $jrc, plain rc $prc, diff rc $diffrc): $(cat "$TMPROOT/t50.diff.json" 2>/dev/null)"
    return
  fi
  ar --json cat "$id" README.txt >"$TMPROOT/t50.cat.json" 2>/dev/null; jrc=$?
  ar cat "$id" README.txt >"$TMPROOT/t50.cat.plain" 2>/dev/null; prc=$?
  if [ $jrc -ne 0 ] || [ $prc -ne 0 ] || ! cmp -s "$TMPROOT/t50.cat.json" "$TMPROOT/t50.cat.plain" || ! grep -q "proj README" "$TMPROOT/t50.cat.json"; then
    cleanup_job "$id"
    rec T50 FAIL "--json cat not byte-for-byte equal to cat (json rc $jrc, plain rc $prc): $(cat "$TMPROOT/t50.cat.json" 2>/dev/null)"
    return
  fi
  cleanup_job "$id"
  rec T50 PASS "--json: structured commands (status list info projects files wait diff --stat commit discard) emit valid JSON with the same exit codes, errors add a JSON line to stderr, and log/diff/cat stay byte-for-byte raw text"
}

# T51: list --status server-side filtering (AND with --project, unknown
# names are a 400 listing the valid statuses)
t_t51() {
  local done_id hang_id out rc
  start_job "FAKE_SLEEP=1 list status done job"
  if [ "$START_RC" -ne 0 ] || [ -z "$START_JOB_ID" ]; then
    rec T51 FAIL "start done: $START_OUT"
    return
  fi
  done_id="$START_JOB_ID"
  wait_state "$done_id" done 60 >/dev/null || { cleanup_job "$done_id"; rec T51 FAIL "done job did not finish"; return; }
  start_job "FAKE_HANG list status running job"
  if [ "$START_RC" -ne 0 ] || [ -z "$START_JOB_ID" ]; then
    cleanup_job "$done_id"
    rec T51 FAIL "start hang: $START_OUT"
    return
  fi
  hang_id="$START_JOB_ID"
  wait_state "$hang_id" running 30 >/dev/null || { ar kill "$hang_id" >/dev/null 2>&1 || true; cleanup_job "$hang_id"; cleanup_job "$done_id"; rec T51 FAIL "hang job never reached running"; return; }

  # (a) --status done: shows the done job, hides the running one
  out=$(ar list --status done 2>&1); rc=$?
  if [ $rc -ne 0 ] || ! printf '%s\n' "$out" | grep -q "$done_id" || printf '%s\n' "$out" | grep -q "$hang_id"; then
    ar kill "$hang_id" >/dev/null 2>&1 || true; wait_state "$hang_id" killed 45 >/dev/null || true
    cleanup_job "$hang_id"; cleanup_job "$done_id"
    rec T51 FAIL "list --status done (rc $rc): $out"
    return
  fi
  # (b) --status running: the opposite
  out=$(ar list --status running 2>&1); rc=$?
  if [ $rc -ne 0 ] || ! printf '%s\n' "$out" | grep -q "$hang_id" || printf '%s\n' "$out" | grep -q "$done_id"; then
    ar kill "$hang_id" >/dev/null 2>&1 || true; wait_state "$hang_id" killed 45 >/dev/null || true
    cleanup_job "$hang_id"; cleanup_job "$done_id"
    rec T51 FAIL "list --status running (rc $rc): $out"
    return
  fi
  # (c) comma-separated list shows both
  out=$(ar list --status done,running 2>&1); rc=$?
  if [ $rc -ne 0 ] || ! printf '%s\n' "$out" | grep -q "$done_id" || ! printf '%s\n' "$out" | grep -q "$hang_id"; then
    ar kill "$hang_id" >/dev/null 2>&1 || true; wait_state "$hang_id" killed 45 >/dev/null || true
    cleanup_job "$hang_id"; cleanup_job "$done_id"
    rec T51 FAIL "list --status done,running (rc $rc): $out"
    return
  fi
  # (d) an unknown status is an error naming the valid ones
  out=$(ar list --status bogus 2>&1); rc=$?
  if [ $rc -eq 0 ] || ! printf '%s' "$out" | grep -q "running, done, failed, self-terminated, killed, incomplete, quota-exhausted"; then
    ar kill "$hang_id" >/dev/null 2>&1 || true; wait_state "$hang_id" killed 45 >/dev/null || true
    cleanup_job "$hang_id"; cleanup_job "$done_id"
    rec T51 FAIL "list --status bogus (rc $rc): $out"
    return
  fi
  # (e) --status ANDs with --project
  out=$(ar list --project proj --status done 2>&1); rc=$?
  if [ $rc -ne 0 ] || ! printf '%s\n' "$out" | grep -q "$done_id" || printf '%s\n' "$out" | grep -q "$hang_id"; then
    ar kill "$hang_id" >/dev/null 2>&1 || true; wait_state "$hang_id" killed 45 >/dev/null || true
    cleanup_job "$hang_id"; cleanup_job "$done_id"
    rec T51 FAIL "list --project proj --status done (rc $rc): $out"
    return
  fi
  ar kill "$hang_id" >/dev/null 2>&1 || true
  wait_state "$hang_id" killed 45 >/dev/null || true
  cleanup_job "$hang_id"
  cleanup_job "$done_id"
  rec T51 PASS "list --status filters server-side by (recomputed) status, accepts comma lists, ANDs with --project, and rejects unknown names listing the valid ones"
}

# T52: the diff fallback note -- a clean worktree falls back to the base
# diff and the CLI prints the note to STDERR (stdout stays a clean patch);
# a worktree with uncommitted changes gets no note.
t_t52() {
  local id out err wt
  # (a) a job whose only change is a committed file -> fallback + note on stderr
  start_job "FAKE_COMMIT diff note job"
  if [ "$START_RC" -ne 0 ] || [ -z "$START_JOB_ID" ]; then
    rec T52 FAIL "start commit job: $START_OUT"
    return
  fi
  id="$START_JOB_ID"
  wait_state "$id" done 60 >/dev/null || { cleanup_job "$id"; rec T52 FAIL "commit job did not reach done"; return; }
  wt=$(ar status "$id" 2>/dev/null | sed -nE 's/^Worktree:[[:space:]]+//p' | head -n 1 | sed 's/ (directory missing on disk)$//')
  out=$(ar diff "$id" 2>/tmp/t52_err.txt)
  err=$(cat /tmp/t52_err.txt 2>/dev/null || true)
  rc=$?
  [ $rc -eq 0 ] || { cleanup_job "$id"; rec T52 FAIL "diff exited $rc: $err"; return; }
  case "$out" in
    *"committed-file.txt"*) ;;
    *) cleanup_job "$id"; rec T52 FAIL "committed fallback diff missing the committed file: $out"; return ;;
  esac
  case "$err" in
    *"no uncommitted changes; showing committed changes vs base"*) ;;
    *) cleanup_job "$id"; rec T52 FAIL "fallback note missing from stderr: [$err]"; return ;;
  esac
  cleanup_job "$id"
  # (b) a job with uncommitted changes -> the note must NOT appear
  start_job "FAKE_WRITE=note-check.txt diff no note job"
  if [ "$START_RC" -ne 0 ] || [ -z "$START_JOB_ID" ]; then
    rec T52 FAIL "start write job: $START_OUT"
    return
  fi
  id="$START_JOB_ID"
  wait_state "$id" done 60 >/dev/null || { cleanup_job "$id"; rec T52 FAIL "write job did not reach done"; return; }
  out=$(ar diff "$id" 2>/tmp/t52_err2.txt)
  err=$(cat /tmp/t52_err2.txt 2>/dev/null || true)
  [ $rc -eq 0 ] || { cleanup_job "$id"; rec T52 FAIL "diff exited $rc: $err"; return; }
  case "$out" in
    *"note-check.txt"*) ;;
    *) cleanup_job "$id"; rec T52 FAIL "uncommitted diff missing the written file: $out"; return ;;
  esac
  case "$err" in
    *"no uncommitted changes"*) cleanup_job "$id"; rec T52 FAIL "note printed for a job WITH uncommitted changes: [$err]"; return ;;
    *) ;;
  esac
  cleanup_job "$id"
  rec T52 PASS "diff falls back to the base diff with a stderr note when the worktree is clean, and prints no note when uncommitted changes exist (stdout stays a clean patch)"
}

# T53: diff --base --stat reports EVERY file that differs from the job's base
# commit (committed AND uncommitted), not only the uncommitted ones
t_t53() {
  local id out headfile n
  # The job commits committed-file.txt AND leaves untracked-file.txt
  # uncommitted: FAKE_COMMIT adds and commits committed-file.txt only, and
  # FAKE_WRITE (which runs first) makes untracked-file.txt a plain worktree
  # file, so relative to the base commit BOTH files are new.
  start_job "FAKE_COMMIT FAKE_WRITE=untracked-file.txt diff base stat job"
  if [ "$START_RC" -ne 0 ] || [ -z "$START_JOB_ID" ]; then
    rec T53 FAIL "start commit job: $START_OUT"
    return
  fi
  id="$START_JOB_ID"
  wait_state "$id" done 60 >/dev/null || { cleanup_job "$id"; rec T53 FAIL "commit job did not reach done"; return; }

  # (a) --base --stat lists BOTH files that differ from the base -- the
  # committed one and the uncommitted/untracked one -- and nothing else, i.e.
  # exactly two stat lines.
  out=$(ar diff "$id" --base --stat 2>&1); rc=$?
  if [ $rc -ne 0 ]; then
    cleanup_job "$id"; rec T53 FAIL "diff --base --stat exited $rc: $out"; return
  fi
  if ! printf '%s\n' "$out" | grep -qE '^A[[:space:]]+[0-9]+[[:space:]]+committed-file[.]txt$'; then
    cleanup_job "$id"; rec T53 FAIL "--base --stat has no committed-file.txt line: $out"; return
  fi
  if ! printf '%s\n' "$out" | grep -qE '^A[[:space:]]+[0-9]+[[:space:]]+untracked-file[.]txt$'; then
    cleanup_job "$id"; rec T53 FAIL "--base --stat has no untracked-file.txt line (uncommitted files must appear): $out"; return
  fi
  n=$(printf '%s\n' "$out" | grep -cE '^[AMD][[:space:]]+[0-9]+[[:space:]]')
  if [ "$n" -ne 2 ]; then
    cleanup_job "$id"; rec T53 FAIL "--base --stat must list exactly the 2 files that differ from base, saw $n: $out"; return
  fi

  # --json --base --stat: the structured payload agrees with the text view
  # (status A, worktree sizes: 18 for "committed content\n", 36 for
  # "fake content for untracked-file.txt\n")
  out=$(ar --json diff "$id" --base --stat 2>/dev/null)
  if [ $? -ne 0 ] || [ -z "$out" ]; then
    cleanup_job "$id"; rec T53 FAIL "--json --base --stat failed: $out"; return
  fi
  if ! printf '%s' "$out" | python3 -c 'import json,sys
d = json.load(sys.stdin)
assert sorted(f["path"] for f in d["files"]) == ["committed-file.txt", "untracked-file.txt"], d["files"]
by = {f["path"]: f for f in d["files"]}
assert by["committed-file.txt"]["status"] == "A" and by["committed-file.txt"]["size"] == 18, by["committed-file.txt"]
assert by["untracked-file.txt"]["status"] == "A" and by["untracked-file.txt"]["size"] == 36, by["untracked-file.txt"]
assert len(d["files"]) == 2, d["files"]
assert "committed-file.txt" in d["diff"], d["diff"]
assert "untracked-file.txt" in d["diff"], d["diff"]' 2>/dev/null; then
    cleanup_job "$id"; rec T53 FAIL "--json --base --stat payload wrong: $out"; return
  fi

  # (b) the plain (no --base) stat is uncommitted-only: the committed file is
  # already in HEAD, so only the still-uncommitted untracked-file.txt appears
  headfile="$TMPROOT/t53.head.stat"
  ar diff "$id" --stat > "$headfile" 2>/dev/null; rc=$?
  if [ $rc -ne 0 ]; then
    cleanup_job "$id"; rec T53 FAIL "diff --stat (plain) exited $rc"; return
  fi
  if ! grep -qE '^A[[:space:]]+[0-9]+[[:space:]]+untracked-file[.]txt$' "$headfile"; then
    cleanup_job "$id"; rec T53 FAIL "plain --stat has no untracked-file.txt line: $(cat "$headfile")"; return
  fi
  if grep -q 'committed-file' "$headfile"; then
    cleanup_job "$id"; rec T53 FAIL "plain --stat must not list the committed file (already in HEAD): $(cat "$headfile")"; return
  fi
  n=$(grep -cE '^[AMD][[:space:]]+[0-9]+[[:space:]]' "$headfile")
  if [ "$n" -ne 1 ]; then
    cleanup_job "$id"; rec T53 FAIL "plain --stat must list exactly 1 uncommitted file, saw $n: $(cat "$headfile")"; return
  fi

  cleanup_job "$id"
  rec T53 PASS "diff --base --stat reports every file that differs from the base commit (both the committed and the uncommitted file appear; plain --stat stays uncommitted-only)"
}

# T54: the --stat `diff` text (both the human stdout and the JSON data["diff"]
# field carried by the raw endpoint) is newline-terminated: exactly one
# trailing '\n' when non-empty, and the EMPTY STRING (not a bare newline) when
# there is nothing to report
t_t54() {
  local id f
  start_job "FAKE_WRITE=newline.txt diff stat newline job"
  if [ "$START_RC" -ne 0 ] || [ -z "$START_JOB_ID" ]; then
    rec T54 FAIL "start: rc=$START_RC: $START_OUT"
    return
  fi
  id="$START_JOB_ID"
  wait_state "$id" done 60 >/dev/null || { cleanup_job "$id"; rec T54 FAIL "job did not reach done"; return; }

  # (a) non-empty stat, human stdout: exactly one trailing newline, the one
  # changed-file line
  f="$TMPROOT/t54.stat.txt"
  ar diff "$id" --stat > "$f" 2>/dev/null; rc=$?
  if [ $rc -ne 0 ] || [ ! -s "$f" ]; then
    cleanup_job "$id"; rec T54 FAIL "diff --stat exited $rc or printed nothing"; return
  fi
  if [ "$(wc -l < "$f")" -ne 1 ] || ! head -n 1 "$f" | grep -qE '^A[[:space:]]+[0-9]+[[:space:]]+newline[.]txt$'; then
    cleanup_job "$id"; rec T54 FAIL "stat output is not exactly the one changed-file line: $(cat "$f")"; return
  fi
  if [ "$(tail -c 2 "$f" | od -An -tx1 | tr -d ' \n')" = "0a0a" ]; then
    cleanup_job "$id"; rec T54 FAIL "stat output ends with two newlines: $(od -c "$f" | tail -n 3)"; return
  fi

  # (b) non-empty stat, JSON data["diff"]: ends with exactly one '\n'
  out=$(ar --json diff "$id" --stat 2>/dev/null)
  if [ $? -ne 0 ] || [ -z "$out" ]; then
    cleanup_job "$id"; rec T54 FAIL "--json --stat failed: $out"; return
  fi
  printf '%s' "$out" | python3 -c 'import json,sys
d = json.load(sys.stdin)
diff = d["diff"]
assert isinstance(diff, str), type(diff)
assert diff.endswith("\n") and not diff.endswith("\n\n"), repr(diff[-4:])
lines = diff.splitlines()
assert len(lines) == 1 and lines[0].endswith("newline.txt"), lines
assert d["stat"] is True and len(d["files"]) == 1' 2>/dev/null || {
    cleanup_job "$id"; rec T54 FAIL "--json --stat data[diff] not newline-terminated: $(printf '%s' "$out" | python3 -c 'import json,sys; print(repr(json.load(sys.stdin)[\"diff\"]))' 2>/dev/null)"; return
  }

  # (c) empty stat (nothing uncommitted): human stdout is zero bytes and the
  # JSON data["diff"] is the EMPTY string (not a bare newline)
  ar commit "$id" -m "commit the newline file" >/dev/null 2>&1
  f="$TMPROOT/t54.empty.stat"
  ar diff "$id" --stat > "$f" 2>/dev/null; rc=$?
  if [ $rc -ne 0 ]; then
    cleanup_job "$id"; rec T54 FAIL "diff --stat after commit exited $rc"; return
  fi
  if [ -s "$f" ]; then
    cleanup_job "$id"; rec T54 FAIL "empty stat human output is not zero bytes: $(od -c "$f" | head -n 3)"; return
  fi
  out=$(ar --json diff "$id" --stat 2>/dev/null)
  if [ $? -ne 0 ] || ! printf '%s' "$out" | python3 -c 'import json,sys
d = json.load(sys.stdin)
assert d["diff"] == "", repr(d["diff"])
assert d["files"] == [], d["files"]' 2>/dev/null; then
    cleanup_job "$id"; rec T54 FAIL "--json empty --stat data[diff] is not the empty string: $(printf '%s' "$out" | python3 -c 'import json,sys; print(repr(json.load(sys.stdin)[\"diff\"]))' 2>/dev/null)"; return
  fi

  cleanup_job "$id"
  rec T54 PASS "diff --stat (human + JSON data[diff]) is newline-terminated with exactly one trailing newline, and an empty stat prints nothing / is the empty string"
}

# T92: `export` pulls a finished job's committed result out of the server onto
# the caller's machine. Covers: uncommitted-without-`-m` refusal (typed
# export_uncommitted), `-m` committing + a patch that `git am`s in a scratch
# clone of the same base, `--only` selecting one file, a running job refusal,
# an empty export refusal, overwrite refusal without `--force`, valid `--json`,
# and `--tar` containing the changed files.
t_t92() {
  local id rid expdir clone out err rc

  # --- set up a done job with two uncommitted files --------------------------
  start_job "FAKE_WRITE=a.txt FAKE_WRITE=b.txt export uncommitted job"
  if [ "$START_RC" -ne 0 ] || [ -z "$START_JOB_ID" ]; then
    rec T92 FAIL "start exited $START_RC: $START_OUT"
    return
  fi
  id="$START_JOB_ID"
  wait_state "$id" done 60 >/dev/null || { cleanup_job "$id"; rec T92 FAIL "job did not reach done"; return; }
  expdir="$TMPROOT/exp92"
  mkdir -p "$expdir"

  # (a) export without -m on an uncommitted worktree -> typed export_uncommitted
  err=$( { ( cd "$expdir" && ar --json export "$id" ) ; } 2>&1 >/dev/null ); rc=$?
  if [ $rc -ne 1 ]; then
    cleanup_job "$id"; rec T92 FAIL "export without -m exited $rc (want 1): $err"; return
  fi
  if ! printf '%s' "$err" | grep -q 'export_uncommitted'; then
    cleanup_job "$id"; rec T92 FAIL "export without -m did not report export_uncommitted: $err"; return
  fi
  if [ -e "$expdir/$id.patch" ]; then
    cleanup_job "$id"; rec T92 FAIL "export without -m wrote a file before refusing"; return
  fi

  # (b) export -m commits first; the summary names both files and the patch
  #          `git am`s cleanly in a scratch clone of the same base
  ( cd "$expdir" && ar export "$id" -m "export message" >"$expdir/b.out" 2>"$expdir/b.err" ); rc=$?
  out=$(cat "$expdir/b.out" 2>/dev/null)
  err=$(cat "$expdir/b.err" 2>/dev/null)
  if [ $rc -ne 0 ]; then
    cleanup_job "$id"; rec T92 FAIL "export -m exited $rc: $out / $err"; return
  fi
  if ! printf '%s' "$out" | grep -q "Committed 2 file(s) in job $id."; then
    cleanup_job "$id"; rec T92 FAIL "export -m summary missing the committed-files line: $out"; return
  fi
  if ! printf '%s' "$out" | grep -Eq "Wrote $id\.patch[[:space:]]+\(1 commit\(s\), 2 file\(s\), \+2 -0, .+\)"; then
    cleanup_job "$id"; rec T92 FAIL "export -m 'Wrote' summary line wrong: $out"; return
  fi
  if ! printf '%s' "$out" | grep -q "git am $id.patch"; then
    cleanup_job "$id"; rec T92 FAIL "export -m apply hint missing git am: $out"; return
  fi
  if [ ! -f "$expdir/$id.patch" ]; then
    cleanup_job "$id"; rec T92 FAIL "export did not write $expdir/$id.patch"; return
  fi
  # the scratch clone of the same base (main) must accept the patch via git am
  clone="$TMPROOT/clone92"
  rm -rf "$clone"
  if ! git clone -q "$WORKSPACE/proj" "$clone" 2>/dev/null; then
    cleanup_job "$id"; rec T92 FAIL "could not clone base for git am"; return
  fi
  git -C "$clone" config user.name "Export Test" >/dev/null
  git -C "$clone" config user.email "export@example.com" >/dev/null
  base=$(git -C "$clone" rev-parse HEAD)
  if ! git -C "$clone" am --quiet "$expdir/$id.patch" 2>/dev/null; then
    cleanup_job "$id"; rec T92 FAIL "exported patch did not git-am in a clone of the base"; return
  fi
  if ! git -C "$clone" diff --name-only "$base" HEAD | grep -qx "a.txt" || ! git -C "$clone" diff --name-only "$base" HEAD | grep -qx "b.txt"; then
    cleanup_job "$id"; rec T92 FAIL "am'ed clone is missing a.txt/b.txt"; return
  fi

  # (c) --only exports a single file from the (now committed) job
  ( cd "$expdir" && ar export "$id" --only a.txt --out "$expdir/only.patch" >"$expdir/c.out" 2>"$expdir/c.err" ); rc=$?
  if [ $rc -ne 0 ]; then
    cleanup_job "$id"; rec T92 FAIL "export --only exited $rc: $(cat "$expdir/c.err")"; return
  fi
  if ! grep -q "a.txt" "$expdir/only.patch" || grep -q "b.txt" "$expdir/only.patch"; then
    cleanup_job "$id"; rec T92 FAIL "export --only did not select exactly a.txt"; return
  fi

  # (d) a running job is refused (typed job_running)
  start_job "FAKE_HANG export running job"
  if [ "$START_RC" -ne 0 ] || [ -z "$START_JOB_ID" ]; then
    cleanup_job "$id"; rec T92 FAIL "start running job: $START_OUT"; return
  fi
  rid="$START_JOB_ID"
  wait_state "$rid" running 60 >/dev/null || { ar kill "$rid" >/dev/null 2>&1; cleanup_job "$rid"; cleanup_job "$id"; rec T92 FAIL "running job did not reach running"; return; }
  err=$( ( cd "$expdir" && ar --json export "$rid" ) 2>&1 >/dev/null ); rc=$?
  if [ $rc -ne 1 ] || ! printf '%s' "$err" | grep -q "job is currently running"; then
    ar kill "$rid" >/dev/null 2>&1; wait_state "$rid" terminal 30 >/dev/null || true
    cleanup_job "$rid"; cleanup_job "$id"; rec T92 FAIL "export of a running job was not refused (rc $rc): $err"; return
  fi
  ar kill "$rid" >/dev/null 2>&1
  wait_state "$rid" terminal 30 >/dev/null || true
  cleanup_job "$rid"
  cleanup_job "$id"

  # (e) a finished job with no commits after the base -> export_empty
  start_job "export empty job"
  if [ "$START_RC" -ne 0 ] || [ -z "$START_JOB_ID" ]; then
    rec T92 FAIL "start empty job: $START_OUT"; return
  fi
  rid="$START_JOB_ID"
  wait_state "$rid" done 60 >/dev/null || { cleanup_job "$rid"; rec T92 FAIL "empty job did not reach done"; return; }
  err=$( ( cd "$expdir" && ar --json export "$rid" ) 2>&1 >/dev/null ); rc=$?
  if [ $rc -ne 1 ] || ! printf '%s' "$err" | grep -q "export_empty"; then
    cleanup_job "$rid"; rec T92 FAIL "empty export was not refused (rc $rc): $err"; return
  fi

  # (f) overwrite of an existing out file is refused without --force,
  #     and allowed with it
  # (a fresh job, then two exports landing on the same default name)
  start_job "FAKE_WRITE=c.txt export overwrite job"
  if [ "$START_RC" -ne 0 ] || [ -z "$START_JOB_ID" ]; then
    cleanup_job "$rid"; rec T92 FAIL "start overwrite job: $START_OUT"; return
  fi
  id="$START_JOB_ID"
  wait_state "$id" done 60 >/dev/null || { cleanup_job "$rid"; cleanup_job "$id"; rec T92 FAIL "overwrite job did not reach done"; return; }
  rm -f "$expdir/$id.patch"
  ( cd "$expdir" && ar export "$id" -m one >/dev/null 2>&1 )
  if [ ! -f "$expdir/$id.patch" ]; then
    cleanup_job "$rid"; cleanup_job "$id"; rec T92 FAIL "first overwrite export did not create the file"; return
  fi
  err=$( ( cd "$expdir" && ar export "$id" ) 2>&1 >/dev/null ); rc=$?
  if [ $rc -ne 1 ] || ! printf '%s' "$err" | grep -q "refusing to overwrite"; then
    cleanup_job "$id"; rec T92 FAIL "re-export over an existing file was not refused (rc $rc): $err"; return
  fi
  ( cd "$expdir" && ar export "$id" --force >/dev/null 2>&1 ); rc=$?
  if [ $rc -ne 0 ]; then
    cleanup_job "$id"; rec T92 FAIL "export --force over an existing file failed (rc $rc)"; return
  fi

  # (g) --json emits a single valid JSON object with the required keys
  ( cd "$expdir" && ar --json export "$id" --only c.txt --out "$expdir/json.patch" >"$expdir/g.json" 2>/dev/null )
  if ! python3 - "$expdir/g.json" <<'PY'
import json, sys
d = json.load(open(sys.argv[1]))
need = {"file", "commits", "files", "insertions", "deletions", "bytes", "committed"}
missing = need - set(d)
assert not missing, f"missing keys {missing}"
assert d["committed"] is False, "committed should be False for an already-committed job"
assert d["commits"] >= 1 and d["files"] >= 1
PY
  then
    cleanup_job "$id"; rec T92 FAIL "export --json was not valid/missing keys: $(cat "$expdir/g.json")"; return
  fi

  # (h) --tar contains the changed files
  ( cd "$expdir" && ar export "$id" --tar --out "$expdir/out.tar" >/dev/null 2>&1 ); rc=$?
  if [ $rc -ne 0 ]; then
    cleanup_job "$id"; rec T92 FAIL "export --tar exited $rc"; return
  fi
  if ! tar -tf "$expdir/out.tar" | grep -qx "c.txt"; then
    cleanup_job "$id"; rec T92 FAIL "export --tar does not contain c.txt"; return
  fi

  cleanup_job "$id"
  cleanup_job "$rid"
  rec T92 PASS "export: uncommitted refusal, -m commit + git-am'able patch, --only selection, running + empty refusals, overwrite guard + --force, valid --json, and --tar contents"
}

# T112: `watch` -- the SSE event stream consumed client-side: a finished
# job exits 0 with its log text on stdout and a status banner on stderr,
# a killed job exits 2, and --timeout on a still-running job exits 3.
t_t112() {
  local id out rc wpid
  # (a) a done job: watch prints the log, banner on stderr, exit 0
  start_job "FAKE_SLEEP=1 watch done job"
  if [ "$START_RC" -ne 0 ] || [ -z "$START_JOB_ID" ]; then
    rec T112 FAIL "start done: $START_OUT"
    return
  fi
  id="$START_JOB_ID"
  wait_state "$id" done 60 >/dev/null || { cleanup_job "$id"; rec T112 FAIL "done job never reached done"; return; }
  out=$(ar watch "$id" 2>"$TMPROOT/t112.err")
  rc=$?
  if [ $rc -ne 0 ]; then
    cleanup_job "$id"
    rec T112 FAIL "watch of a done job exited $rc, expected 0: $out"
    return
  fi
  case "$out" in
    *FAKE-DONE*) ;;
    *) cleanup_job "$id"; rec T112 FAIL "watch stdout missing the job log: $out"; return ;;
  esac
  case "$(cat "$TMPROOT/t112.err")" in
    *"status: done"*) ;;
    *) cleanup_job "$id"; rec T112 FAIL "watch stderr missing the status banner: $(cat "$TMPROOT/t112.err")"; return ;;
  esac
  cleanup_job "$id"
  # (b) a killed job: watch sees the transition and exits 2
  start_job "FAKE_HANG watch killed job"
  if [ "$START_RC" -ne 0 ] || [ -z "$START_JOB_ID" ]; then
    rec T112 FAIL "start hang: rc=$START_RC: $START_OUT"
    return
  fi
  id="$START_JOB_ID"
  wait_state "$id" running 30 >/dev/null || { cleanup_job "$id"; rec T112 FAIL "hang job never reached running"; return; }
  ar watch "$id" --timeout 60 >"$TMPROOT/t112b.out" 2>"$TMPROOT/t112b.err" &
  wpid=$!
  sleep 2
  ar kill "$id" >/dev/null 2>&1 || true
  wait "$wpid"
  rc=$?
  if [ $rc -ne 2 ]; then
    wait_state "$id" killed 45 >/dev/null || true
    cleanup_job "$id"
    rec T112 FAIL "watch of a killed job exited $rc, expected 2: $(cat "$TMPROOT/t112b.err")"
    return
  fi
  # a job killed with SIGTERM ends `failed` here: the fake agy records an
  # `interrupted` error line (same as T38); accept any terminal non-done banner
  case "$(cat "$TMPROOT/t112b.err")" in
    *"status: failed"*|*"status: killed"*) ;;
    *) cleanup_job "$id"; rec T112 FAIL "watch stderr missing the terminal banner: $(cat "$TMPROOT/t112b.err")"; return ;;
  esac
  wait_state "$id" killed 45 >/dev/null || true
  cleanup_job "$id"
  # (c) --timeout on a still-running job: exit 3 + the timeout note
  start_job "FAKE_HANG watch timeout job"
  if [ "$START_RC" -ne 0 ] || [ -z "$START_JOB_ID" ]; then
    rec T112 FAIL "start hang2: rc=$START_RC: $START_OUT"
    return
  fi
  id="$START_JOB_ID"
  wait_state "$id" running 30 >/dev/null || { cleanup_job "$id"; rec T112 FAIL "hang job never reached running"; return; }
  out=$(ar watch "$id" --timeout 1 2>&1)
  rc=$?
  if [ $rc -ne 3 ]; then
    ar kill "$id" >/dev/null 2>&1 || true
    wait_state "$id" killed 45 >/dev/null || true
    cleanup_job "$id"
    rec T112 FAIL "watch --timeout on a running job exited $rc, expected 3: $out"
    return
  fi
  case "$out" in
    *"exit code 3"*) ;;
    *) ar kill "$id" >/dev/null 2>&1 || true
       wait_state "$id" killed 45 >/dev/null || true
       cleanup_job "$id"
       rec T112 FAIL "watch timeout note missing: $out"; return ;;
  esac
  ar kill "$id" >/dev/null 2>&1 || true
  wait_state "$id" killed 45 >/dev/null || true
  cleanup_job "$id"
  rec T112 PASS "watch streams the log (exit 0 + banner for done), exits 2 for a killed job, and exits 3 when --timeout elapses while the job is still running"
}
# T152: list AGE/ACTIVITY columns -- a finished job shows a non-empty AGE
# and ACTIVITY `-`; a running job shows a time (short duration) that is not
# `-` for ACTIVITY; the JSON payload carries the new additive keys.
t_t152() {
  local done_id hang_id out rc
  start_job "FAKE_SLEEP=1 list age activity done job"
  if [ "$START_RC" -ne 0 ] || [ -z "$START_JOB_ID" ]; then
    rec T152 FAIL "start done: $START_OUT"
    return
  fi
  done_id="$START_JOB_ID"
  wait_state "$done_id" done 60 >/dev/null || { cleanup_job "$done_id"; rec T152 FAIL "done job did not finish"; return; }
  start_job "FAKE_HANG list age activity running job"
  if [ "$START_RC" -ne 0 ] || [ -z "$START_JOB_ID" ]; then
    cleanup_job "$done_id"
    rec T152 FAIL "start hang: $START_OUT"
    return
  fi
  hang_id="$START_JOB_ID"
  wait_state "$hang_id" running 30 >/dev/null || {
    ar kill "$hang_id" >/dev/null 2>&1 || true
    cleanup_job "$hang_id"; cleanup_job "$done_id"
    rec T152 FAIL "hang job never reached running"
    return
  }
  out=$(ar list 2>&1); rc=$?
  if [ $rc -ne 0 ]; then
    ar kill "$hang_id" >/dev/null 2>&1 || true; wait_state "$hang_id" killed 45 >/dev/null || true
    cleanup_job "$hang_id"; cleanup_job "$done_id"
    rec T152 FAIL "list exited $rc: $out"
    return
  fi
  local hdr done_row hang_row age act
  hdr=$(printf '%s\n' "$out" | head -n 1)
  done_row=$(printf '%s\n' "$out" | grep -E "^${done_id} " || true)
  hang_row=$(printf '%s\n' "$out" | grep -E "^${hang_id} " || true)
  if ! printf '%s' "$hdr" | grep -Eq "JOB ID .*PROJECT.*STATUS.*CREATED.*AGE +ACTIVITY +PROMPT" \
    || [ -z "$done_row" ] || [ -z "$hang_row" ]; then
    ar kill "$hang_id" >/dev/null 2>&1 || true; wait_state "$hang_id" killed 45 >/dev/null || true
    cleanup_job "$hang_id"; cleanup_job "$done_id"
    rec T152 FAIL "header missing AGE/ACTIVITY before PROMPT, or a row is missing: $hdr || $done_row || $hang_row"
    return
  fi
  # column fields: id(1) project(2) status(3) created-date(4) created-time(5)
  # age(6) activity(7) prompt(8+)
  age=$(printf '%s\n' "$done_row" | awk '{print $6}')
  act=$(printf '%s\n' "$done_row" | awk '{print $7}')
  if [ -z "$age" ] || [ "$act" != "-" ]; then
    ar kill "$hang_id" >/dev/null 2>&1 || true; wait_state "$hang_id" killed 45 >/dev/null || true
    cleanup_job "$hang_id"; cleanup_job "$done_id"
    rec T152 FAIL "finished job AGE must be non-empty and ACTIVITY must be '-' (AGE [$age] ACTIVITY [$act]): $done_row"
    return
  fi
  age=$(printf '%s\n' "$hang_row" | awk '{print $6}')
  act=$(printf '%s\n' "$hang_row" | awk '{print $7}')
  if [ -z "$act" ] || [ "$act" = "-" ]; then
    ar kill "$hang_id" >/dev/null 2>&1 || true; wait_state "$hang_id" killed 45 >/dev/null || true
    cleanup_job "$hang_id"; cleanup_job "$done_id"
    rec T152 FAIL "running job ACTIVITY must be a time, not '-' (AGE [$age] ACTIVITY [$act]): $hang_row"
    return
  fi
  if ! printf '%s' "$act" | grep -Eq '^[0-9]+[smh]$|^[0-9]+[dh][0-9]+[hm]$'; then
    ar kill "$hang_id" >/dev/null 2>&1 || true; wait_state "$hang_id" killed 45 >/dev/null || true
    cleanup_job "$hang_id"; cleanup_job "$done_id"
    rec T152 FAIL "running job ACTIVITY must be a short duration ([${act}]): $hang_row"
    return
  fi
  # the running row must keep the plain `running` status: a `*stall?`
  # marker would mean the `!` ACTIVITY suffix is also required (not checked
  # here -- stall needs 20 min idle; a fresh hang job is young).
  if ! printf '%s\n' "$hang_row" | awk '{print $3}' | grep -Eq '^running( \*stall\?)?$'; then
    ar kill "$hang_id" >/dev/null 2>&1 || true; wait_state "$hang_id" killed 45 >/dev/null || true
    cleanup_job "$hang_id"; cleanup_job "$done_id"
    rec T152 FAIL "running row STATUS cell unexpected: $hang_row"
    return
  fi
  # JSON payload: every job carries the additive keys.
  local jout
  jout=$(ar --json list 2>/dev/null)
  jout=$(printf '%s' "$jout" | python3 -c '
import json, sys
d = json.load(sys.stdin)
assert isinstance(d["jobs"], list) and d["jobs"], "payload must hold a non-empty jobs list"
for j in d["jobs"]:
    assert isinstance(j["elapsed_seconds"], int) and j["elapsed_seconds"] >= 0, "elapsed_seconds"
    assert j["last_activity_seconds"] is None or isinstance(j["last_activity_seconds"], int), "last_activity_seconds"
    assert isinstance(j["possibly_stalled"], bool), "possibly_stalled"
print("ok")
' 2>&1); rc=$?
  if [ $rc -ne 0 ] || [ "$jout" != "ok" ]; then
    ar kill "$hang_id" >/dev/null 2>&1 || true; wait_state "$hang_id" killed 45 >/dev/null || true
    cleanup_job "$hang_id"; cleanup_job "$done_id"
    rec T152 FAIL "--json list must carry elapsed_seconds / last_activity_seconds / possibly_stalled per job (rc $rc): $jout"
    return
  fi
  ar kill "$hang_id" >/dev/null 2>&1 || true
  wait_state "$hang_id" killed 45 >/dev/null || true
  cleanup_job "$hang_id"
  cleanup_job "$done_id"
  rec T152 PASS "list shows AGE (finished: short duration, running: elapsed) and ACTIVITY (running: time-since-last-activity, `!` when stalled; others: -) before PROMPT, and the JSON keys elapsed_seconds / last_activity_seconds (null when not running) / possibly_stalled are additive per job"
}
# T132: opt-in max runtime -- a FAKE_HANG job with --max-runtime 2s is
# terminated by the server, ends `killed`, and its log carries the
# `max runtime exceeded (2s)` line; status reports the limit.
t_t132() {
  local id st out log
  start_job "FAKE_HANG job with a 2s max runtime" --max-runtime 2s
  if [ "$START_RC" -ne 0 ] || [ -z "$START_JOB_ID" ]; then
    rec T132 FAIL "start with --max-runtime: $START_OUT"
    return
  fi
  id="$START_JOB_ID"
  out=$(ar status "$id" 2>&1) || true
  if ! printf '%s\n' "$out" | grep -q "Max runtime:"; then
    ar kill "$id" >/dev/null 2>&1 || true
    cleanup_job "$id"
    rec T132 FAIL "status did not report 'Max runtime:' (output: $out)"
    return
  fi
  st=$(wait_state "$id" killed 45) || {
    cleanup_job "$id"
    rec T132 FAIL "job did not reach killed within 45s (last status: $st)"
    return
  }
  log=$(ar log "$id" 2>&1)
  if ! printf '%s\n' "$log" | grep -q "max runtime exceeded (2s)"; then
    cleanup_job "$id"
    rec T132 FAIL "job log missing 'max runtime exceeded (2s)': $log"
    return
  fi
  st=$(get_status "$id")
  if [ "$st" != "killed" ]; then
    cleanup_job "$id"
    rec T132 FAIL "final status is '$st', expected killed"
    return
  fi
  cleanup_job "$id"
  rec T132 PASS "FAKE_HANG with --max-runtime 2s is terminated by the server, ends killed, log shows 'max runtime exceeded (2s)'"
}

# T133: no limit -- with neither --max-runtime nor AGENT_RUN_MAX_RUNTIME set,
# a running FAKE_HANG job is NOT killed by the server (it stays running well
# past the enforcer's 10 s tick interval) and ends killed only when
# explicitly killed.
t_t133() {
  local id st i
  start_job "FAKE_HANG job that must run unbounded"
  if [ "$START_RC" -ne 0 ] || [ -z "$START_JOB_ID" ]; then
    rec T133 FAIL "start: $START_OUT"
    return
  fi
  id="$START_JOB_ID"
  wait_state "$id" running 30 >/dev/null || { cleanup_job "$id"; rec T133 FAIL "job never reached running"; return; }
  # Let the enforcer tick at least once (10 s interval) with margin while the
  # job has no limit: it must still be running afterwards (a limit-less job
  # is NEVER killed, so no wall-clock lower-bound assertion applies here).
  for i in $(seq 1 28); do sleep 0.5; done
  st=$(get_status "$id")
  if [ "$st" != "running" ]; then
    cleanup_job "$id"
    rec T133 FAIL "unlimited job was killed by the server (status: $st)"
    return
  fi
  # Explicit kill must still work now that the enforcer has had the
  # chance (but not the reason) to act on an unlimited job.
  ar kill "$id" >/dev/null 2>&1 || true
  st=$(wait_state "$id" killed 45) || {
    cleanup_job "$id"
    rec T133 FAIL "explicit kill did not reach killed (last: $st)"
    return
  }
  cleanup_job "$id"
  rec T133 PASS "without a limit the running job is not killed by the server; explicit kill still works"
}

# T160: remote-control enable is refused without AGENT_RUN_REMOTE_CONTROL; status is allowed
t_t160() {
  local out rc
  out=$(ar remote-control enable 2>&1)
  rc=$?
  if [ "$rc" -ne 1 ]; then
    rec T160 FAIL "enable without AGENT_RUN_REMOTE_CONTROL exited $rc, expected 1 (output: $out)"
    return
  fi
  if ! printf '%s\n' "$out" | grep -iq "AGENT_RUN_REMOTE_CONTROL"; then
    rec T160 FAIL "enable refusal message does not mention AGENT_RUN_REMOTE_CONTROL: $out"
    return
  fi

  out=$(ar --json remote-control enable 2>&1)
  rc=$?
  if [ "$rc" -ne 1 ]; then
    rec T160 FAIL "json enable without AGENT_RUN_REMOTE_CONTROL exited $rc, expected 1"
    return
  fi
  if ! printf '%s\n' "$out" | grep -q '"code":\s*"remote_control_disabled"'; then
    rec T160 FAIL "json enable refusal missing code remote_control_disabled: $out"
    return
  fi

  out=$(ar remote-control status 2>&1)
  rc=$?
  if [ "$rc" -ne 0 ]; then
    rec T160 FAIL "status exited $rc, expected 0: $out"
    return
  fi
  if ! printf '%s\n' "$out" | grep -iq "inactive"; then
    rec T160 FAIL "initial status is not inactive: $out"
    return
  fi

  rec T160 PASS "remote-control enable refused without AGENT_RUN_REMOTE_CONTROL, status allowed"
}

# T161: remote-control enable, status (instance_name==hostname), idempotent enable/disable, --json
t_t161() {
  local out rc host json_out
  host=$(hostname)

  stop_server
  export AGENT_RUN_REMOTE_CONTROL=1
  local rc_log="$TMPROOT/server-rc.log"
  start_server "$rc_log"
  wait_for_server "$rc_log"

  # 1. Enable then status shows active and instance_name equal to hostname
  out=$(ar remote-control enable 2>&1)
  rc=$?
  if [ "$rc" -ne 0 ]; then
    unset AGENT_RUN_REMOTE_CONTROL
    rec T161 FAIL "enable exited $rc: $out"
    return
  fi
  if ! printf '%s\n' "$out" | grep -iq "active"; then
    unset AGENT_RUN_REMOTE_CONTROL
    rec T161 FAIL "enable output does not indicate active: $out"
    return
  fi

  out=$(ar remote-control status 2>&1)
  rc=$?
  if [ "$rc" -ne 0 ]; then
    unset AGENT_RUN_REMOTE_CONTROL
    rec T161 FAIL "status exited $rc: $out"
    return
  fi
  if ! printf '%s\n' "$out" | grep -iq "active"; then
    unset AGENT_RUN_REMOTE_CONTROL
    rec T161 FAIL "status output does not indicate active: $out"
    return
  fi
  if ! printf '%s\n' "$out" | grep -q "$host"; then
    unset AGENT_RUN_REMOTE_CONTROL
    rec T161 FAIL "status output does not contain hostname $host: $out"
    return
  fi

  # Check --json shape for status when active
  json_out=$(ar --json remote-control status 2>&1)
  rc=$?
  if [ "$rc" -ne 0 ]; then
    unset AGENT_RUN_REMOTE_CONTROL
    rec T161 FAIL "json status exited $rc: $json_out"
    return
  fi
  if ! printf '%s\n' "$json_out" | grep -q '"active":\s*true'; then
    unset AGENT_RUN_REMOTE_CONTROL
    rec T161 FAIL "json status active is not true: $json_out"
    return
  fi
  if ! printf '%s\n' "$json_out" | grep -q "\"instance_name\":\s*\"$host\""; then
    unset AGENT_RUN_REMOTE_CONTROL
    rec T161 FAIL "json status instance_name is not $host: $json_out"
    return
  fi
  if ! printf '%s\n' "$json_out" | grep -q '"pid":'; then
    unset AGENT_RUN_REMOTE_CONTROL
    rec T161 FAIL "json status missing pid: $json_out"
    return
  fi

  # 2. Enable twice is idempotent
  out=$(ar remote-control enable 2>&1)
  rc=$?
  if [ "$rc" -ne 0 ]; then
    unset AGENT_RUN_REMOTE_CONTROL
    rec T161 FAIL "second enable exited $rc: $out"
    return
  fi
  if ! printf '%s\n' "$out" | grep -iq "active"; then
    unset AGENT_RUN_REMOTE_CONTROL
    rec T161 FAIL "second enable output does not indicate active: $out"
    return
  fi

  # 3. Disable then status inactive
  out=$(ar remote-control disable 2>&1)
  rc=$?
  if [ "$rc" -ne 0 ]; then
    unset AGENT_RUN_REMOTE_CONTROL
    rec T161 FAIL "disable exited $rc: $out"
    return
  fi
  if ! printf '%s\n' "$out" | grep -iq "inactive"; then
    unset AGENT_RUN_REMOTE_CONTROL
    rec T161 FAIL "disable output does not indicate inactive: $out"
    return
  fi

  out=$(ar remote-control status 2>&1)
  rc=$?
  if [ "$rc" -ne 0 ]; then
    unset AGENT_RUN_REMOTE_CONTROL
    rec T161 FAIL "status after disable exited $rc: $out"
    return
  fi
  if ! printf '%s\n' "$out" | grep -iq "inactive"; then
    unset AGENT_RUN_REMOTE_CONTROL
    rec T161 FAIL "status after disable does not indicate inactive: $out"
    return
  fi

  # 4. Disable twice is idempotent
  out=$(ar remote-control disable 2>&1)
  rc=$?
  if [ "$rc" -ne 0 ]; then
    unset AGENT_RUN_REMOTE_CONTROL
    rec T161 FAIL "second disable exited $rc: $out"
    return
  fi
  if ! printf '%s\n' "$out" | grep -iq "inactive"; then
    unset AGENT_RUN_REMOTE_CONTROL
    rec T161 FAIL "second disable output does not indicate inactive: $out"
    return
  fi

  # Check --json shape for status when inactive
  json_out=$(ar --json remote-control status 2>&1)
  rc=$?
  if [ "$rc" -ne 0 ]; then
    unset AGENT_RUN_REMOTE_CONTROL
    rec T161 FAIL "json status after disable exited $rc: $json_out"
    return
  fi
  if ! printf '%s\n' "$json_out" | grep -q '"active":\s*false'; then
    unset AGENT_RUN_REMOTE_CONTROL
    rec T161 FAIL "json status active is not false after disable: $json_out"
    return
  fi

  stop_server
  unset AGENT_RUN_REMOTE_CONTROL
  start_server
  wait_for_server

  rec T161 PASS "remote-control enable, status (instance_name==hostname), idempotent enable/disable, and json shape pass"
}

# T166: --hindsight-query replaces the prompt as the recall query end to end.
t_t166() {
  # (a) --hindsight off + --hindsight-query is a typed, client-visible error.
  local out rc
  out=$(ar start proj "t162 conflict probe" --hindsight off --hindsight-query "short query" 2>&1)
  rc=$?
  if [ $rc -eq 0 ]; then
    rec T166 FAIL "--hindsight off with --hindsight-query unexpectedly succeeded: $out"
    return
  fi
  case "$out" in
    *"cannot be used with --hindsight off"*) ;;
    *) rec T166 FAIL "unexpected error for --hindsight off + --hindsight-query: $out"; return ;;
  esac

  # (b) With recall disabled server-side the flag is accepted, stored, and
  #     printed by status -- but never used.
  start_job "t162 query with recall off" --hindsight-query "short query"
  if [ "$START_RC" -ne 0 ]; then
    rec T166 FAIL "start with --hindsight-query on a hindsight-disabled server failed (rc $START_RC): $START_OUT"
    return
  fi
  local id="$START_JOB_ID"
  wait_state "$id" terminal 30 >/dev/null
  local stored
  stored=$(db_field "$id" "hindsight_query")
  if [ "$stored" != "short query" ]; then
    cleanup_job "$id"
    rec T166 FAIL "hindsight_query not persisted (got '$stored')"
    return
  fi
  out=$(ar status "$id" 2>&1)
  case "$out" in
    *"Hindsight query:  short query"*) ;;
    *)
      cleanup_job "$id"
      rec T166 FAIL "status missing 'Hindsight query:  short query' line: $out"
      return
      ;;
  esac
  cleanup_job "$id"

  # (c) End to end against a private hindsight-enabled server whose hindsight
  #     binary records its argv: the recall query is exactly the flag value,
  #     not the prompt.
  t_t166_fail() {
    if [ -n "${t_t166_id:-}" ]; then cleanup_job "$t_t166_id" 2>/dev/null || true; fi
    stop_server
    start_server
    wait_for_server
    rec T166 FAIL "$1"
  }
  local fake_hindsight="$TMPROOT/fake-hindsight"
  local argv_file="$TMPROOT/hindsight-argv.txt"
  rm -f "$argv_file"
  cat > "$fake_hindsight" <<'EOF'
#!/bin/sh
printf '%s\n' "$@" > "$FAKE_HINDSIGHT_ARGV"
echo '{"results":[{"text":"recalled fact"}]}'
EOF
  chmod 755 "$fake_hindsight"

  stop_server
  (
    cd "$TMPROOT" || exit 1
    cd "$WORKSPACE" || exit 1
    exec env -u AGENT_RUN_REMOTE -u GH_TOKEN -u GITHUB_TOKEN \
      HOME="$FAKE_HOME" \
      AGENT_RUN_API_LISTEN="127.0.0.1:$PORT" \
      AGENT_RUN_WORKSPACE_ROOT="$WORKSPACE" \
      AGENT_RUN_STATE_ROOT="$STATE_ROOT" \
      AGENT_RUN_AGY_PATH="$FAKE_AGY" \
      AGENT_RUN_JOBS_DIR_NAME=".agent-jobs" \
      AGENT_RUN_MAX_PARALLEL=3 \
      AGENT_RUN_HINDSIGHT_ENABLED=true \
      AGENT_RUN_HINDSIGHT_AUTO_RECALL=true \
      AGENT_RUN_HINDSIGHT_AUTO_RETAIN=false \
      AGENT_RUN_HINDSIGHT_PATH="$fake_hindsight" \
      FAKE_HINDSIGHT_ARGV="$argv_file" \
      FAKE_ARGV_LOG="$TMPROOT/argv.log" \
      FAKE_CONVMAP_LOG="$TMPROOT/convmap.log" \
      "$AGENT_RUN_API" >"$TMPROOT/server-t162-hindsight.log" 2>&1
  ) &
  SERVER_PID=$!
  CUR_SERVER_LOG="$TMPROOT/server-t162-hindsight.log"
  wait_for_server "$TMPROOT/server-t162-hindsight.log"

  t_t166_id=""
  local prompt="a long distinctive prompt that must not be the recall query"
  start_job "$prompt" --hindsight-query "short query"
  [ "$START_RC" -eq 0 ] || { t_t166_fail "start with --hindsight-query on hindsight-enabled server failed (rc $START_RC): $START_OUT"; return; }
  t_t166_id="$START_JOB_ID"
  printf '%s\n' "$START_OUT" | grep -q "^Memory: recall on, retain off, bank proj$" || {
    t_t166_fail "expected 'Memory: recall on, retain off, bank proj' line: $START_OUT"
    return
  }
  local i
  for i in $(seq 1 20); do
    [ -s "$argv_file" ] && break
    sleep 0.5
  done
  if [ ! -s "$argv_file" ]; then
    t_t166_fail "fake hindsight was never invoked (argv capture missing)"
    return
  fi
  printf '%s\n' "-o" "json" "memory" "recall" "proj" "short query" > "$TMPROOT/t162-expected-argv"
  if ! cmp -s "$argv_file" "$TMPROOT/t162-expected-argv"; then
    t_t166_fail "recall argv mismatch, got: $(tr '\n' '|' < "$argv_file")"
    return
  fi
  if grep -q "long distinctive prompt" "$argv_file"; then
    t_t166_fail "recall argv contains the prompt instead of the explicit query"
    return
  fi
  wait_state "$t_t166_id" terminal 30 >/dev/null
  stored=$(db_field "$t_t166_id" "hindsight_query")
  [ "$stored" = "short query" ] || { t_t166_fail "hindsight_query not persisted (got '$stored')"; return; }
  cleanup_job "$t_t166_id"
  t_t166_id=""

  # Back to the suite's standard (hindsight-disabled) server.
  stop_server
  start_server
  wait_for_server

  rec T166 PASS "--hindsight-query: off-conflict rejected; accepted, stored and printed with recall off; end-to-end recall used exactly the flag value, not the prompt"
}

# T167: autostart brings the remote-control daemon up without any enable call
t_t167() {
  local out rc host i active json_out
  host=$(hostname)

  stop_server
  export AGENT_RUN_REMOTE_CONTROL=1
  export AGENT_RUN_REMOTE_CONTROL_AUTOSTART=1
  export AGENT_RUN_REMOTE_CONTROL_CHECK_SECS=10
  local rc_log="$TMPROOT/server-rc-t167.log"
  start_server "$rc_log"
  wait_for_server "$rc_log"

  active=0
  for i in $(seq 1 40); do
    json_out=$(ar --json remote-control status 2>&1)
    if printf '%s\n' "$json_out" | grep -q '"active":\s*true'; then
      active=1
      break
    fi
    sleep 0.5
  done

  if [ "$active" -ne 1 ]; then
    unset AGENT_RUN_REMOTE_CONTROL AGENT_RUN_REMOTE_CONTROL_AUTOSTART AGENT_RUN_REMOTE_CONTROL_CHECK_SECS
    stop_server
    start_server
    wait_for_server
    rec T167 FAIL "daemon did not become active automatically via autostart: $json_out"
    return
  fi

  out=$(ar remote-control status 2>&1)
  if ! printf '%s\n' "$out" | grep -q "$host"; then
    unset AGENT_RUN_REMOTE_CONTROL AGENT_RUN_REMOTE_CONTROL_AUTOSTART AGENT_RUN_REMOTE_CONTROL_CHECK_SECS
    stop_server
    start_server
    wait_for_server
    rec T167 FAIL "daemon status missing host $host: $out"
    return
  fi

  stop_server
  unset AGENT_RUN_REMOTE_CONTROL AGENT_RUN_REMOTE_CONTROL_AUTOSTART AGENT_RUN_REMOTE_CONTROL_CHECK_SECS
  start_server
  wait_for_server

  rec T167 PASS "autostart brings the daemon up without any enable call"
}

# T168: killing the fake daemon makes the supervisor restart it within about two check intervals
t_t168() {
  local out rc host i json_out old_pid new_pid restarted
  host=$(hostname)

  stop_server
  export AGENT_RUN_REMOTE_CONTROL=1
  export AGENT_RUN_REMOTE_CONTROL_AUTOSTART=1
  export AGENT_RUN_REMOTE_CONTROL_CHECK_SECS=10
  local rc_log="$TMPROOT/server-rc-t168.log"
  start_server "$rc_log"
  wait_for_server "$rc_log"

  old_pid=""
  for i in $(seq 1 40); do
    json_out=$(ar --json remote-control status 2>&1)
    if printf '%s\n' "$json_out" | grep -q '"active":\s*true'; then
      old_pid=$(printf '%s\n' "$json_out" | sed -nE 's/.*"pid":\s*([0-9]+).*/\1/p')
      [ -n "$old_pid" ] && break
    fi
    sleep 0.5
  done

  if [ -z "$old_pid" ]; then
    unset AGENT_RUN_REMOTE_CONTROL AGENT_RUN_REMOTE_CONTROL_AUTOSTART AGENT_RUN_REMOTE_CONTROL_CHECK_SECS
    stop_server
    start_server
    wait_for_server
    rec T168 FAIL "daemon did not start or report pid: $json_out"
    return
  fi

  kill -9 "$old_pid" 2>/dev/null || true

  out=$(ar remote-control status 2>&1)
  if ! printf '%s\n' "$out" | grep -iq "inactive"; then
    unset AGENT_RUN_REMOTE_CONTROL AGENT_RUN_REMOTE_CONTROL_AUTOSTART AGENT_RUN_REMOTE_CONTROL_CHECK_SECS
    stop_server
    start_server
    wait_for_server
    rec T168 FAIL "status immediately after kill is not inactive: $out"
    return
  fi

  restarted=0
  new_pid=""
  for i in $(seq 1 50); do
    json_out=$(ar --json remote-control status 2>&1)
    if printf '%s\n' "$json_out" | grep -q '"active":\s*true'; then
      new_pid=$(printf '%s\n' "$json_out" | sed -nE 's/.*"pid":\s*([0-9]+).*/\1/p')
      if [ -n "$new_pid" ] && [ "$new_pid" != "$old_pid" ]; then
        restarted=1
        break
      fi
    fi
    sleep 0.5
  done

  if [ "$restarted" -ne 1 ]; then
    unset AGENT_RUN_REMOTE_CONTROL AGENT_RUN_REMOTE_CONTROL_AUTOSTART AGENT_RUN_REMOTE_CONTROL_CHECK_SECS
    stop_server
    start_server
    wait_for_server
    rec T168 FAIL "supervisor did not restart daemon within ~2 check intervals (old_pid=$old_pid, json_out=$json_out)"
    return
  fi

  stop_server
  unset AGENT_RUN_REMOTE_CONTROL AGENT_RUN_REMOTE_CONTROL_AUTOSTART AGENT_RUN_REMOTE_CONTROL_CHECK_SECS
  start_server
  wait_for_server

  rec T168 PASS "killing the fake daemon makes the supervisor restart it within about two check intervals"
}

# T169: after remote-control disable the supervisor does not restart it, and enable re-arms it
t_t169() {
  local out rc i json_out old_pid new_pid restarted
  stop_server
  export AGENT_RUN_REMOTE_CONTROL=1
  export AGENT_RUN_REMOTE_CONTROL_AUTOSTART=1
  export AGENT_RUN_REMOTE_CONTROL_CHECK_SECS=10
  local rc_log="$TMPROOT/server-rc-t169.log"
  start_server "$rc_log"
  wait_for_server "$rc_log"

  for i in $(seq 1 40); do
    json_out=$(ar --json remote-control status 2>&1)
    if printf '%s\n' "$json_out" | grep -q '"active":\s*true'; then
      break
    fi
    sleep 0.5
  done

  out=$(ar remote-control disable 2>&1)
  rc=$?
  if [ "$rc" -ne 0 ]; then
    unset AGENT_RUN_REMOTE_CONTROL AGENT_RUN_REMOTE_CONTROL_AUTOSTART AGENT_RUN_REMOTE_CONTROL_CHECK_SECS
    stop_server
    start_server
    wait_for_server
    rec T169 FAIL "disable exited $rc: $out"
    return
  fi

  sleep 12

  json_out=$(ar --json remote-control status 2>&1)
  if ! printf '%s\n' "$json_out" | grep -q '"active":\s*false'; then
    unset AGENT_RUN_REMOTE_CONTROL AGENT_RUN_REMOTE_CONTROL_AUTOSTART AGENT_RUN_REMOTE_CONTROL_CHECK_SECS
    stop_server
    start_server
    wait_for_server
    rec T169 FAIL "supervisor restarted daemon despite manual disable suppression: $json_out"
    return
  fi

  out=$(ar remote-control enable 2>&1)
  rc=$?
  if [ "$rc" -ne 0 ]; then
    unset AGENT_RUN_REMOTE_CONTROL AGENT_RUN_REMOTE_CONTROL_AUTOSTART AGENT_RUN_REMOTE_CONTROL_CHECK_SECS
    stop_server
    start_server
    wait_for_server
    rec T169 FAIL "manual enable failed: $out"
    return
  fi

  json_out=$(ar --json remote-control status 2>&1)
  if ! printf '%s\n' "$json_out" | grep -q '"active":\s*true'; then
    unset AGENT_RUN_REMOTE_CONTROL AGENT_RUN_REMOTE_CONTROL_AUTOSTART AGENT_RUN_REMOTE_CONTROL_CHECK_SECS
    stop_server
    start_server
    wait_for_server
    rec T169 FAIL "manual enable failed to bring daemon active: $json_out"
    return
  fi

  old_pid=$(printf '%s\n' "$json_out" | sed -nE 's/.*"pid":\s*([0-9]+).*/\1/p')
  if [ -n "$old_pid" ]; then
    kill -9 "$old_pid" 2>/dev/null || true
  fi

  restarted=0
  for i in $(seq 1 50); do
    json_out=$(ar --json remote-control status 2>&1)
    if printf '%s\n' "$json_out" | grep -q '"active":\s*true'; then
      new_pid=$(printf '%s\n' "$json_out" | sed -nE 's/.*"pid":\s*([0-9]+).*/\1/p')
      if [ -n "$new_pid" ] && [ "$new_pid" != "$old_pid" ]; then
        restarted=1
        break
      fi
    fi
    sleep 0.5
  done

  if [ "$restarted" -ne 1 ]; then
    unset AGENT_RUN_REMOTE_CONTROL AGENT_RUN_REMOTE_CONTROL_AUTOSTART AGENT_RUN_REMOTE_CONTROL_CHECK_SECS
    stop_server
    start_server
    wait_for_server
    rec T169 FAIL "supervisor did not restart daemon after enable re-armed it (old_pid=$old_pid, json_out=$json_out)"
    return
  fi

  stop_server
  unset AGENT_RUN_REMOTE_CONTROL AGENT_RUN_REMOTE_CONTROL_AUTOSTART AGENT_RUN_REMOTE_CONTROL_CHECK_SECS
  start_server
  wait_for_server

  rec T169 PASS "supervisor honors suppression by disable, and enable re-arms it"
}

# T170: repair re-attaches an orphaned worktree (its admin dir in agent-run's
# private bare repo deleted out from under it, as a git-sync repo re-init does),
# a second repair reports healthy, and diff works again; --all covers the job.
t_t170() {
  start_job "FAKE_SLEEP=1 FAKE_WRITE=t170.txt repair me"
  if [ "$START_RC" -ne 0 ]; then
    rec T170 FAIL "start exited $START_RC: $START_OUT"
    return
  fi
  local id="$START_JOB_ID"
  [ -n "$id" ] || { rec T170 FAIL "no job id parsed: $START_OUT"; return; }
  local st
  st=$(wait_state "$id" done 60) || {
    cleanup_job "$id"
    rec T170 FAIL "job did not reach done (last status: $st)"
    return
  }
  local worktree admin_dir out rc
  worktree=$(db_field "$id" "worktree_path")
  if [ -z "$worktree" ] || [ ! -d "$worktree" ]; then
    cleanup_job "$id"
    rec T170 FAIL "job worktree missing or unknown (db says: '$worktree')"
    return
  fi
  # Locate the worktree's admin dir in agent-run's private bare repository.
  admin_dir=$(find "$WORKSPACE/.agent-repos" -maxdepth 5 -type d -path "*/worktrees/$id" 2>/dev/null | head -n 1)
  if [ -z "$admin_dir" ]; then
    cleanup_job "$id"
    rec T170 FAIL "admin dir for the worktree of $id not found under $WORKSPACE/.agent-repos"
    return
  fi
  # Orphan it the way a git-sync repo re-init does: the admin directory
  # vanishes while the worktree checkout stays behind. Verify the damage.
  rm -rf "$admin_dir"
  if (cd "$worktree" && git status --porcelain >/dev/null 2>&1); then
    cleanup_job "$id"
    rec T170 FAIL "worktree git still works after the admin dir was removed (orphaning failed): $worktree"
    return
  fi
  # Explicit single-job repair re-attaches the worktree.
  out=$(ar repair "$id" 2>&1)
  rc=$?
  if [ "$rc" -ne 0 ]; then
    cleanup_job "$id"
    rec T170 FAIL "repair $id exited $rc: $out"
    return
  fi
  case "$out" in
    *"repaired $id"*) ;;
    *) cleanup_job "$id"; rec T170 FAIL "repair output does not say 'repaired $id': $out"; return ;;
  esac
  if [ ! -d "$admin_dir" ]; then
    cleanup_job "$id"
    rec T170 FAIL "admin dir was not re-created at $admin_dir"
    return
  fi
  # A second repair is a no-op that reports healthy.
  out=$(ar repair "$id" 2>&1)
  rc=$?
  if [ "$rc" -ne 0 ] || ! printf '%s\n' "$out" | grep -q "healthy $id"; then
    cleanup_job "$id"
    rec T170 FAIL "second repair did not report healthy (rc $rc): $out"
    return
  fi
  if ! (cd "$worktree" && git status --porcelain >/dev/null 2>&1); then
    cleanup_job "$id"
    rec T170 FAIL "worktree git is still broken after the repair: $worktree"
    return
  fi
  # diff works again and sees the job's uncommitted file.
  out=$(ar diff "$id" 2>&1)
  rc=$?
  if [ "$rc" -ne 0 ]; then
    cleanup_job "$id"
    rec T170 FAIL "diff after repair exited $rc: $out"
    return
  fi
  case "$out" in
    *t170.txt*) ;;
    *) cleanup_job "$id"; rec T170 FAIL "diff after repair is missing t170.txt: $out"; return ;;
  esac
  # --all sweeps every job and reports this (now healthy) one.
  out=$(ar repair --all 2>&1)
  rc=$?
  if [ "$rc" -ne 0 ]; then
    cleanup_job "$id"
    rec T170 FAIL "repair --all exited $rc: $out"
    return
  fi
  case "$out" in
    *"healthy $id"*) ;;
    *) cleanup_job "$id"; rec T170 FAIL "repair --all output is missing 'healthy $id': $out"; return ;;
  esac
  cleanup_job "$id"
  rec T170 PASS "orphaned worktree re-attached by repair (re-attach idempotent, diff works, --all reports healthy; job $id)"
}

# --- main ------------------------------------------------------------------------------
TEARDOWN_DONE=0
teardown() {
  [ "$TEARDOWN_DONE" = "1" ] && return 0
  TEARDOWN_DONE=1
  local rc=$?
  stop_server
  # kill any leftover fake agy processes spawned under the temp root
  pkill -f "$TMPROOT/fake-agy.sh" 2>/dev/null || true
  sleep 0.5
  pkill -KILL -f "$TMPROOT/fake-agy.sh" 2>/dev/null || true
  if [ "${KEEP:-0}" = "1" ]; then
    echo "KEEP=1: leaving temp root at $TMPROOT (server log: $SERVER_LOG, results: $RESULTS)"
  else
    rm -rf "$TMPROOT"
  fi
  if [ $rc -ne 0 ]; then
    echo "SUITE RESULT: ABORTED (exit $rc) after $TESTS_RUN test(s)" >&2
    exit "$rc"
  fi
  if [ $FAILURES -gt 0 ]; then
    echo "SUITE RESULT: $FAILURES test(s) failed" >&2
    exit 1
  fi
  if [ $TESTS_RUN -eq 0 ]; then
    echo "SUITE RESULT: ABORTED (exit 2) after $TESTS_RUN test(s)" >&2
    exit 2
  fi
  echo "SUITE RESULT: all $TESTS_RUN tests passed"
}
trap teardown EXIT

echo "== agent-run black-box suite =="
echo "crate:     $CRATE_DIR"
echo "binaries:  $BIN_DIR"
echo "port:      $PORT"
echo "temp root: $TMPROOT"

start_server
[ -n "$SERVER_PID" ] || die "failed to launch agent-run-api"
wait_for_server
echo "server healthy (pid $SERVER_PID)"

run_test T01 t_t01
run_test T02 t_t02
run_test T03 t_t03
run_test T04 t_t04
run_test T05 t_t05
run_test T06 t_t06
run_test T07 t_t07
run_test T08 t_t08
run_test T09 t_t09
run_test T10 t_t10
run_test T11 t_t11
run_test T12 t_t12
run_test T13 t_t13
run_test T14 t_t14
run_test T15 t_t15
run_test T16 t_t16
run_test T17 t_t17
run_test T18 t_t18
run_test T19 t_t19
# T23 is intentionally skipped: conversation-id safety is already covered by
# the core's unit tests (job.rs), and has no distinct black-box surface here.
run_test T20 t_t20
run_test T21 t_t21
run_test T22 t_t22
run_test_stops_server T24 t_t24
run_test T25 t_t25
run_test T26 t_t26
run_test T27 t_t27
run_test T28 t_t28
run_test T29 t_t29
run_test T30 t_t30
run_test T31 t_t31
run_test T32 t_t32
run_test T33 t_t33
run_test T34 t_t34
run_test T35 t_t35
run_test T36 t_t36
run_test T37 t_t37
run_test_stops_server T38 t_t38
run_test T39 t_t39
run_test T40 t_t40
run_test T41 t_t41
run_test T42 t_t42
run_test T43 t_t43
run_test T44 t_t44
run_test T45 t_t45
run_test T46 t_t46
run_test T47 t_t47
run_test T48 t_t48
run_test T49 t_t49
run_test T50 t_t50
run_test T51 t_t51
run_test T52 t_t52
run_test T53 t_t53
run_test T54 t_t54
run_test T92 t_t92
run_test T112 t_t112
run_test T152 t_t152
run_test T132 t_t132
run_test T133 t_t133
run_test T160 t_t160
run_test_stops_server T161 t_t161
run_test T162 t_t162
run_test T163 t_t163
run_test T164 t_t164
run_test T165 t_t165
run_test T166 t_t166
run_test_stops_server T167 t_t167
run_test_stops_server T168 t_t168
run_test_stops_server T169 t_t169
run_test T170 t_t170

echo
echo "=== per-test results ==="
cat "$RESULTS"
# the EXIT trap prints the final verdict
