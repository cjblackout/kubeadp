#!/usr/bin/env bash
# real-agy-smoke: a few live checks against the REAL agy, over a PRIVATE local
# agent-run-api on 127.0.0.1:$PORT (default 18003). It never talks to the
# production agent-run-api. Real agy usage is rate-limited and the account can
# get blocked, so the run is opt-in, sequential, cheap and capped.
#
# Usage:
#   CONFIRM_REAL_AGY=yes bash real-agy-smoke.sh login    # one-time interactive
#                                                        #   OAuth (or AGY_AUTH_CODE=<code>)
#   CONFIRM_REAL_AGY=yes bash real-agy-smoke.sh          # the checks (R1..R8)
#
# Environment knobs:
#   CONFIRM_REAL_AGY=yes   required; refuses (exit 2) without it
#   PORT                   private server port (default 18003)
#   MODEL                  model for the jobs (default gemini-3.8-flash-low, always --effort low)
#   MAX_JOBS               hard cap on model jobs (default 6)
#   PAUSE                  seconds between model jobs (default 20)
#   BIN_DIR                dir holding prebuilt agent-run/agent-run-api; unset: both crates are
#                          cargo build --release'd (CARGO_TARGET_DIR default
#                          ~/.cache/agent-run-blackbox-target, as in run-tests.sh) and the
#                          results copied to ~/.cache/agent-run-blackbox/bin
#   AGY_PATH               the real agy binary (default /usr/local/libexec/agy)
#   AGY_AUTH_CODE          for `login`: passed as --code instead of reading stdin
#   WORK                   workspace/state/results dir (default $HOME/.agent-run-real-test)
#   AGY_TEST_HOME          token dir (default $WORK/home, mode 700)
#
# Safety: AGENT_RUN_REMOTE is unset and every call passes --remote
# http://127.0.0.1:$PORT. The server runs with HOME=$AGY_TEST_HOME (holds the
# login token -- created 700, reusable between runs, never deleted, never
# inside the repo), a private workspace/state under $WORK/{ws,state},
# AGENT_RUN_MAX_PARALLEL=2, GH_TOKEN/GITHUB_TOKEN unset, Hindsight disabled.
# Every `agent-run start` counts against MAX_JOBS, every wait has --timeout 6m,
# and every job started is cleaned (clean --force) on exit.

set -u

MODE=run
case "${1:-}" in
  "") ;;
  login) MODE=login ;;
  *) echo "usage: bash real-agy-smoke.sh [login]" >&2; exit 2 ;;
esac

die() {
  echo "FATAL: $*" >&2
  exit 1
}

SCRIPT_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
CRATE_DIR=$(cd "$SCRIPT_DIR/../.." && pwd)

# --- configuration -----------------------------------------------------------
PORT="${PORT:-18003}"
MODEL="${MODEL:-gemini-3.8-flash-low}"
MAX_JOBS="${MAX_JOBS:-6}"
PAUSE="${PAUSE:-20}"
BIN_DIR="${BIN_DIR:-}"
AGY="${AGY_PATH:-/usr/local/libexec/agy}"
WORK="${WORK:-$HOME/.agent-run-real-test}"
AGY_TEST_HOME="${AGY_TEST_HOME:-$WORK/home}"

WS="$WORK/ws"
STATE="$WORK/state"
RESULTS="$WORK/results.tsv"
SERVER_LOG="$WORK/server.log"
URL="http://127.0.0.1:$PORT"
PROJECT="scratch-proj"
PROJECT_DIR="$WS/$PROJECT"

PASS_COUNT=0
FAIL_COUNT=0
INFO_COUNT=0
MODEL_JOBS=0
SERVER_PID=""
CREATED_JOBS=()
START_ID=""
START_RC=0
HAS_AR_MODELS=0
HAS_AR_QUOTA=0
HAS_QUOTA_FRESH=0

# --- opt-in gate ---------------------------------------------------------------
if [ "${CONFIRM_REAL_AGY:-}" != "yes" ]; then
  if [ "$MODE" = "login" ]; then
    echo "Refusing to run: CONFIRM_REAL_AGY=yes is not set."
    echo "This login authenticates the operator's own agy account (no model jobs are"
    echo "started) through a private local agent-run-api on $URL. The token is stored"
    echo "in $AGY_TEST_HOME (mode 700) and reused by later runs."
    echo "Re-run with:"
    echo "  CONFIRM_REAL_AGY=yes bash real-agy-smoke.sh login"
  else
    echo "Refusing to run: CONFIRM_REAL_AGY=yes is not set."
    echo "Once confirmed, this run:"
    echo "  - starts at most $MAX_JOBS model jobs (this run plans 3: R1, R2, R4), model: $MODEL, effort: low"
    echo "  - uses the operator's own agy login stored in $AGY_TEST_HOME"
    echo "  - talks only to a private agent-run-api on $URL (never the production server),"
    echo "    jobs sequential with a ${PAUSE}s pause between them"
    echo "Re-run with:"
    echo "  CONFIRM_REAL_AGY=yes bash real-agy-smoke.sh"
  fi
  exit 2
fi

# --- safety: never point at a real, remote server ------------------------------
if [ -n "${AGENT_RUN_REMOTE:-}" ]; then
  case "$AGENT_RUN_REMOTE" in
    *127.0.0.1* | *localhost*) ;;
    *) echo "ignoring AGENT_RUN_REMOTE=$AGENT_RUN_REMOTE; this script only talks to its own private server" >&2 ;;
  esac
fi
unset AGENT_RUN_REMOTE
unset GH_TOKEN GITHUB_TOKEN

# --- tool dependencies ---------------------------------------------------------
for tool in git curl python3 timeout; do
  command -v "$tool" >/dev/null 2>&1 || die "required tool '$tool' not found in PATH"
done
if [ -z "$BIN_DIR" ]; then
  command -v cargo >/dev/null 2>&1 || die "required tool 'cargo' not found in PATH (only needed when BIN_DIR is unset)"
fi
[ -x "$AGY" ] || die "agy binary not found/executable at $AGY (set AGY_PATH)"
case "$MAX_JOBS" in '' | *[!0-9]*) die "MAX_JOBS must be a positive integer (got '$MAX_JOBS')" ;; esac
case "$PAUSE" in '' | *[!0-9]*) die "PAUSE must be a non-negative integer (got '$PAUSE')" ;; esac
[ "$MAX_JOBS" -ge 1 ] || die "MAX_JOBS must be >= 1"

# --- locate / build the binaries (same cache dirs as run-tests.sh) --------------
if [ -n "$BIN_DIR" ]; then
  BIN_DIR="$BIN_DIR"
else
  BIN_DIR="$HOME/.cache/agent-run-blackbox/bin"
  if [ ! -x "$BIN_DIR/agent-run" ] || [ ! -x "$BIN_DIR/kubeadp-agent" ]; then
    export CARGO_TARGET_DIR="${CARGO_TARGET_DIR:-$HOME/.cache/agent-run-blackbox-target}"
    mkdir -p "$BIN_DIR" || die "could not create $BIN_DIR"
    echo "Building agent-run (release)..."
    (cd "$CRATE_DIR" && cargo build --release --bin agent-run) || die "cargo build (agent-run) failed"
    echo "Building kubeadp-agent (release)..."
    (cd "$CRATE_DIR" && cargo build --release --bin kubeadp-agent) || die "cargo build (kubeadp-agent) failed"
    cp "$CARGO_TARGET_DIR/release/agent-run" "$BIN_DIR/" || die "could not copy agent-run binary"
    cp "$CARGO_TARGET_DIR/release/kubeadp-agent" "$BIN_DIR/" || die "could not copy kubeadp-agent binary"
  fi
fi
AGENT_RUN="$BIN_DIR/agent-run"
KUBEADP_AGENT="$BIN_DIR/kubeadp-agent"
[ -x "$AGENT_RUN" ] || die "agent-run binary not found/executable at $AGENT_RUN"
[ -x "$KUBEADP_AGENT" ] || die "kubeadp-agent binary not found/executable at $KUBEADP_AGENT"

# --- work dirs (token dir is mode 700 and survives across runs) ----------------
mkdir -p "$WS" "$STATE" "$AGY_TEST_HOME" || die "could not create $WS / $STATE / $AGY_TEST_HOME"
chmod 700 "$WORK" 2>/dev/null || true
chmod 700 "$AGY_TEST_HOME" 2>/dev/null || true
printf 'id\tresult\tmessage\n' > "$RESULTS" || die "could not write results to $RESULTS"

if [ ! -d "$PROJECT_DIR/.git" ]; then
  mkdir -p "$PROJECT_DIR" || die "could not create $PROJECT_DIR"
  git -C "$PROJECT_DIR" init -b main >/dev/null 2>&1 || {
    git -C "$PROJECT_DIR" init >/dev/null 2>&1 || die "git init failed in $PROJECT_DIR"
    git -C "$PROJECT_DIR" checkout -q -b main 2>/dev/null || true
  }
  git -C "$PROJECT_DIR" config user.name "Real Agy Smoke"
  git -C "$PROJECT_DIR" config user.email "real-agy-smoke@localhost"
  printf 'scratch project for real-agy-smoke\n' > "$PROJECT_DIR/README.txt"
  git -C "$PROJECT_DIR" add README.txt
  git -C "$PROJECT_DIR" commit -q -m "initial commit" || die "initial git commit failed in $PROJECT_DIR"
fi

# --- helpers -------------------------------------------------------------------
ar() {
  "$AGENT_RUN" --remote "$URL" "$@"
}

rec() {
  local id="$1" result="$2" msg="${3:-}"
  case "$result" in
    PASS) PASS_COUNT=$((PASS_COUNT + 1)) ;;
    FAIL) FAIL_COUNT=$((FAIL_COUNT + 1)) ;;
    INFO) INFO_COUNT=$((INFO_COUNT + 1)) ;;
  esac
  printf '%s\t%s\t%s\n' "$id" "$result" "$msg" | tee -a "$RESULTS"
}

get_status() {
  ar status "$1" 2>/dev/null | sed -nE 's/^Status:[[:space:]]*//p' | head -n 1
}

clean_job() {
  local id="$1" i
  local keep=()
  [ -n "$id" ] || return 0
  ar clean "$id" --force >/dev/null 2>&1 || true
  for i in ${CREATED_JOBS[@]+"${CREATED_JOBS[@]}"}; do
    [ "$i" = "$id" ] || keep+=("$i")
  done
  CREATED_JOBS=(${keep[@]+"${keep[@]}"})
}

# start_model_job <CHECK_ID> <PROMPT> [extra start args...]
# Counted against MAX_JOBS; sets START_ID ("" on failure), START_RC.
start_model_job() {
  local check_id="$1" prompt="$2"
  shift 2
  MODEL_JOBS=$((MODEL_JOBS + 1))
  if [ "$MODEL_JOBS" -gt "$MAX_JOBS" ]; then
    rec "JOB-CAP" FAIL "refusing model job #$MODEL_JOBS: cap MAX_JOBS=$MAX_JOBS reached (check $check_id)"
    exit 1
  fi
  local out
  out=$(ar start "$PROJECT" "$prompt" "$@" 2>&1)
  START_RC=$?
  START_ID=$(printf '%s\n' "$out" | sed -nE "s/^Job '([0-9a-f]{8})' started.*/\1/p" | head -n 1)
  if [ -z "$START_ID" ]; then
    rec "$check_id" FAIL "start failed (rc=$START_RC): $(printf '%s\n' "$out" | head -n 3 | tr '\n' ' ')"
    exit 1
  fi
  CREATED_JOBS+=("$START_ID")
}

# agy /usage probe (tiny model call) -- the base script's quota line
usage() {
  HOME="$AGY_TEST_HOME" timeout 60 "$AGY" -p "/usage" --output-format json 2>/dev/null </dev/null | python3 -c 'import sys, json
try:
    r = json.loads(sys.stdin.read())["response"]
    print(" | ".join(l.replace("\t", " ") for l in r.strip().split("\n")))
except Exception as e:
    print("usage-unavailable:", e)'
}

# One line: current quota state (agent-run quota --fresh if available, else the agy probe)
quota_now() {
  if [ "$HAS_AR_QUOTA" = "1" ]; then
    if [ "$HAS_QUOTA_FRESH" = "1" ]; then
      ar quota --fresh 2>&1 | tr '\n' ' ' | head -c 200
    else
      ar quota 2>&1 | tr '\n' ' ' | head -c 200
    fi
  else
    usage
  fi
}

# --- server lifecycle (same pattern as run-tests.sh) ----------------------------
start_server() {
  (
    cd "$WORK" || exit 1
    exec env -u AGENT_RUN_REMOTE -u GH_TOKEN -u GITHUB_TOKEN \
      HOME="$AGY_TEST_HOME" \
      AGENT_RUN_API_LISTEN="127.0.0.1:$PORT" \
      AGENT_RUN_WORKSPACE_ROOT="$WS" \
      AGENT_RUN_STATE_ROOT="$STATE" \
      AGENT_RUN_AGY_PATH="$AGY" \
      AGENT_RUN_JOBS_DIR_NAME=".agent-jobs" \
      AGENT_RUN_MAX_PARALLEL=2 \
      AGENT_RUN_HINDSIGHT_ENABLED=false \
      KUBEADP_ROLE=worker \
      "$KUBEADP_AGENT" >"$SERVER_LOG" 2>&1
  ) &
  SERVER_PID=$!
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
  local i
  for i in $(seq 1 40); do
    if curl -fsS --max-time 2 "$URL/healthz" >/dev/null 2>&1; then
      return 0
    fi
    sleep 0.5
  done
  {
    echo "server did not become healthy within 20s"
    echo "--- last server log lines ---"
    tail -n 40 "$SERVER_LOG" 2>/dev/null || true
  } >&2
  die "agent-run-api server failed to become healthy on $URL"
}

on_exit() {
  # clean up every job we started (needs the server), then stop the server.
  # Never touches $AGY_TEST_HOME: the login token is kept for the next run.
  local id
  if [ -n "$SERVER_PID" ] && kill -0 "$SERVER_PID" 2>/dev/null; then
    for id in ${CREATED_JOBS[@]+"${CREATED_JOBS[@]}"}; do
      ar clean "$id" --force >/dev/null 2>&1 || true
    done
  fi
  stop_server
}
trap 'on_exit' EXIT

# --- main flow -------------------------------------------------------------------
echo "real-agy-smoke: mode=$MODE url=$URL model=$MODEL max_jobs=$MAX_JOBS pause=${PAUSE}s"
echo "token dir: $AGY_TEST_HOME | workspace: $WS | state: $STATE"
start_server
wait_for_server

if [ "$MODE" = "login" ]; then
  echo
  echo "Interactive agy OAuth: open the printed link and paste the code when prompted."
  if [ -n "${AGY_AUTH_CODE:-}" ]; then
    ar auth --code "$AGY_AUTH_CODE" || die "auth failed (see above)"
  else
    ar auth || die "auth failed (see above)"
  fi
  echo
  echo "Login complete. The token is stored in $AGY_TEST_HOME and reused by later runs."
  exit 0
fi

if ! curl -s -m 5 "$URL/api/v1/auth/status" | grep -q '"authenticated":true'; then
  echo "not authenticated against the private server" >&2
  echo "run: CONFIRM_REAL_AGY=yes bash real-agy-smoke.sh login"
  exit 2
fi

# feature detection on this CLI build (R7/R8 are forward-looking subcommands)
if ar models --help >/dev/null 2>&1; then
  HAS_AR_MODELS=1
fi
if ar quota --help >/dev/null 2>&1; then
  HAS_AR_QUOTA=1
  if ar quota --help 2>&1 | grep -q -- '--fresh'; then
    HAS_QUOTA_FRESH=1
  fi
fi

rec QUOTA-BEFORE INFO "$(quota_now)"

# R1 read-only PONG
start_model_job R1 "Reply with exactly the single word PONG and do nothing else." --plan --model "$MODEL" --effort low
ar wait "$START_ID" --timeout 6m >/dev/null 2>&1
wait_rc=$?
res=$(ar result "$START_ID" 2>&1)
case "$res" in
  *PONG*) rec R1 PASS "read-only job done, PONG (wait rc=$wait_rc)" ;;
  *) rec R1 FAIL "no PONG: $(printf '%s\n' "$res" | head -n 3 | tr '\n' ' ')" ;;
esac
clean_job "$START_ID"
sleep "$PAUSE"

# R2 edit + diff + worktree location
start_model_job R2 "Create a file named HELLO.txt containing exactly one line: hello. Change nothing else." --model "$MODEL" --effort low
ar wait "$START_ID" --timeout 6m >/dev/null 2>&1
st=$(get_status "$START_ID")
d=$(ar diff "$START_ID" 2>&1)
wt=$(ar status "$START_ID" 2>&1 | sed -nE 's/^Worktree:[[:space:]]*//p' | head -n 1)
if [ "$st" = "done" ] && printf '%s\n' "$d" | grep -q '^+hello'; then
  rec R2 PASS "edit job done; diff has +hello; worktree $wt"
else
  rec R2 FAIL "status=$st diff=$(printf '%s\n' "$d" | head -n 3 | tr '\n' ' ')"
fi

# R3 retry --prompt follow-up
ar retry "$START_ID" --prompt "Append a second line to HELLO.txt: second. Keep the first line." >/dev/null 2>&1
ar wait "$START_ID" --timeout 6m >/dev/null 2>&1
d=$(ar diff "$START_ID" 2>&1)
lg=$(ar log "$START_ID" 2>&1)
if printf '%s\n' "$d" | grep -q '^+second' && printf '%s\n' "$lg" | grep -q 'follow-up prompt'; then
  rec R3 PASS "retry --prompt: +second in diff, banner in log"
else
  rec R3 FAIL "diff=$(printf '%s\n' "$d" | grep '^+' | head -n 3 | tr '\n' ' ')"
fi
ar commit "$START_ID" >/dev/null 2>&1
clean_job "$START_ID"
sleep "$PAUSE"

# R4 kill mid-turn, then retry-after-kill (R4b is INFO only -- never a failure)
start_model_job R4 "Write a very detailed 300-line tutorial about git worktrees into notes.md, section by section. Take your time." --model "$MODEL" --effort low
sleep 8
ar kill "$START_ID" >/dev/null 2>&1
st=$(get_status "$START_ID")
if [ "$st" = "killed" ]; then
  rec R4 PASS "kill mid-turn -> killed"
else
  rec R4 FAIL "status after kill: $st"
fi
rr=$(ar retry "$START_ID" 2>&1 | head -n 2 | tr '\n' ' ')
case "$rr" in
  *launched*) rec R4b INFO "retry-after-kill: ACCEPTED" ;;
  *) rec R4b INFO "retry-after-kill: refused: $rr" ;;
esac
ar kill "$START_ID" >/dev/null 2>&1
clean_job "$START_ID"
sleep "$PAUSE"

# R5 agy models lists >= 3 models
m=$(HOME="$AGY_TEST_HOME" timeout 40 "$AGY" models 2>&1 | grep -c "$(printf '\t')")
if [ "$m" -ge 3 ]; then
  rec R5 PASS "agy models lists $m models"
else
  rec R5 FAIL "models=$m (AGY=$AGY)"
fi

# R6 no leftover jobs
n=$(ar list 2>&1 | grep -c -E '^[0-9a-f]{8} ')
if [ "$n" -eq 0 ]; then
  rec R6 PASS "no leftover jobs"
else
  rec R6 FAIL "$n leftover jobs"
fi

# R7 agent-run models (only if the subcommand exists in this CLI build)
if [ "$HAS_AR_MODELS" = "1" ]; then
  mout=$(ar models 2>/dev/null)
  # data rows: non-blank, non-separator lines, minus the header line
  rows=$(printf '%s\n' "$mout" | grep -vE '^[[:space:]-]+$' | tail -n +2 | grep -c '[^[:space:]]')
  if [ "$rows" -ge 3 ]; then
    rec R7 PASS "agent-run models lists $rows model rows"
  else
    rec R7 FAIL "agent-run models: $rows rows (need >= 3)"
  fi
else
  rec R7 INFO "agent-run models subcommand not available in this CLI build"
fi

# R8 agent-run quota (only if the subcommand exists in this CLI build)
if [ "$HAS_AR_QUOTA" = "1" ]; then
  if [ "$HAS_QUOTA_FRESH" = "1" ]; then
    qout=$(ar quota --fresh 2>&1)
    qrc=$?
  else
    qout=$(ar quota 2>&1)
    qrc=$?
  fi
  if [ "$qrc" -eq 0 ] && printf '%s\n' "$qout" | grep -q '%'; then
    rec R8 PASS "agent-run quota: exit 0, table with %"
  else
    rec R8 FAIL "agent-run quota rc=$qrc: $(printf '%s\n' "$qout" | head -n 3 | tr '\n' ' ')"
  fi
else
  rec R8 INFO "agent-run quota subcommand not available in this CLI build"
fi

rec QUOTA-AFTER INFO "$(quota_now)"

# --- summary ---------------------------------------------------------------------
echo
echo "SUMMARY: PASS=$PASS_COUNT FAIL=$FAIL_COUNT INFO=$INFO_COUNT (model jobs used: $MODEL_JOBS/$MAX_JOBS, model: $MODEL)"
if [ "$FAIL_COUNT" -gt 0 ]; then
  echo "RESULT: FAIL (results: $RESULTS)"
  exit 1
fi
echo "RESULT: OK (results: $RESULTS)"
exit 0
