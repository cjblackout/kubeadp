#!/usr/bin/env bash
# Scripted fake `agy` for the agent-run black-box test suite.
#
# The engine invokes it (see agent-core/src/runner.rs build_command_and_args):
#   fake-agy.sh --dangerously-skip-permissions --mode <m> --output-format <fmt> \
#               -p <prompt> [--model M] [--effort E] [--add-dir <worktree> ...]
# cwd is the job worktree; stdout+stderr are captured into the job log.
#
# Output formats (--output-format):
#   json         one bare result object, single line (the job default today)
#   stream-json  real agy's NDJSON envelope: an init line
#                {"event":"init","conversation_id":"<id>","init":{"model":"fake","cwd":...}},
#                then DONE step_update lines (user_input, tool run_command,
#                agent_response with RESPONSE_DELTA as text_delta), then a final
#                {"event":"result","result":<object>} line. <object> is the SAME
#                text the json format prints; its "conversation_id" value (may be
#                empty) seeds the init line. emit_final <object> (helper below)
#                does the switching; the /usage quota path (json only) always
#                prints the bare object.
#
# Behaviour is chosen by markers inside the prompt text:
#   FAKE_SLEEP=<n>   sleep n seconds (default 1; combinable with any marker)
#   FAKE_FAIL        print a structured ERROR JSON (error "boom") WITH a real
#                    conversation_id, exit 1                     -> status failed
#   FAKE_QUOTA       print a quota/rate-limit banner (matches the
#                    QUOTA_MARKERS in agent-core/src/job.rs), exit 1
#                                                            -> status quota-exhausted
#   FAKE_HANG        sleep 600s; on SIGTERM/SIGINT print the interrupted JSON
#                    and exit 1                                  -> kill target
#   FAKE_WRITE=<f>   write file <f> into the current dir (the worktree),
#                    repeatable with different names
#   FAKE_STAGE       write staged-new.txt, `git add` it, and `git rm`
#                    README.txt (staged change + staged deletion)
#   FAKE_COMMIT      write committed-file.txt and `git commit` it in the worktree
#   FAKE_NOJSON      plain text only, exit 0, no structured JSON -> status incomplete
#   FAKE_SELFTERM    exit 0 early with no JSON at all           -> status incomplete
#                    (the engine cannot distinguish self-termination from a
#                    clean exit-0 without output; see job.rs determine_status_from_signals)
#   FAKE_BADUTF8     print a line containing a raw 0xff byte before the SUCCESS JSON
#   FAKE_CONV_DB     create a SQLite conversation db under
#                    $HOME/.gemini/antigravity-cli/conversations/ (agy's on-disk
#                    layout) whose steps table holds this prompt, then print the
#                    SUCCESS JSON with an EMPTY conversation_id -- the engine must
#                    recover the id from that file. When FAKE_CONVMAP_LOG=<file>
#                    is set, the created conversation is appended to it as a
#                    single line "<conversation-id>\t<prompt>" (the prompt is
#                    the full -p text, newlines flattened to spaces, including
#                    the engine's per-turn reference line), so a test can map
#                    a prompt to the db filename (the db filename IS the
#                    conversation id)
#   FAKE_NOTICE_QUOTED   SUCCESS JSON whose response QUOTES agy's background-task
#                    exit notice ("terminating 1 background task(s) on exit") --
#                    the job must still classify done, not incomplete. In
#                    stream-json mode the quoted notice additionally appears in
#                    the text_delta of the agent_response step_update line.
#   FAKE_RATELIMIT_TEXT  like the base SUCCESS, but in stream-json mode the
#                    text_delta of the agent_response step_update line contains
#                    the words "we discuss the rate limit policy"
#   FAKE_DELIVER_QUEUE  on a resumed invocation, read the run's per-run message
#                    queue file `.engine-message-queue` (E4 emulation; one
#                    escaped line per message, in the run worktree) and fold
#                    the delivered messages into the SUCCESS response
#                    ("DELIVERED:<text>") so a test can assert the queued text
#                    arrived in the resume turn's prompt
#
# Direct (non job-runner) invocations, matching the engine's own calls:
#   <fake> models                         model list for `agent-run models`
#   <fake> -p /usage --output-format json /usage quota JSON for `agent-run quota`
#
# If FAKE_ARGV_LOG=<file> or KUBEADP_ARGV_LOG=<file> is set, every invocation
# first appends its full argv (tab-separated, one line) to that file.
#
# No marker: base behaviour (sleep 1s, print a SUCCESS JSON line, exit 0).

set -u

ARGV_LOG_FILE="${KUBEADP_ARGV_LOG:-${FAKE_ARGV_LOG:-}}"
CONVMAP_LOG_FILE="${KUBEADP_CONVMAP_LOG:-${FAKE_CONVMAP_LOG:-}}"

# --- argv logging for the black-box suite -------------------------------------
{
  printf '%s' "$0"
  for a in "$@"; do printf '\t%s' "$a"; done
  printf '\n'
} >>.fake-argv 2>/dev/null || true

if [ -n "$ARGV_LOG_FILE" ]; then
  {
    printf '%s' "$0"
    for a in "$@"; do printf '\t%s' "$a"; done
    printf '\n'
  } >>"$ARGV_LOG_FILE" 2>/dev/null || true
fi

START_TIME=$(date +%s)
PROMPT=""
OUTPUT_FORMAT=""
RESPONSE_DELTA=""

# --- direct invocations (the engine calls these without -p) -------------------
if [ "${1:-}" = "models" ]; then
  # `agent-run models` runs `<agy> models`: progress line first, then one
  # `<id>\t<label>` line per model (label-less lines are dropped server-side).
  echo "Fetching available models..."
  printf 'gemini-3.8-flash-low\tGemini 3.8 Flash (Low)\n'
  printf 'gemini-3.8-pro\tGemini 3.8 Pro\n'
  printf 'claude-sonnet-4-5\tClaude Sonnet 4.5\n'
  exit 0
fi

if [ "${1:-}" = "remote-control" ]; then
  STATE_FILE="${FAKE_HOME:-$HOME}/remote-control.state"
  ACTION="${2:-}"
  case "$ACTION" in
    start)
      NAME=""
      shift 2
      while [ $# -gt 0 ]; do
        case "$1" in
          --name)
            NAME="${2:-}"
            shift 2
            ;;
          --name=*)
            NAME="${1#*=}"
            shift 1
            ;;
          *)
            shift 1
            ;;
        esac
      done
      sleep 3600 </dev/null >/dev/null 2>&1 &
      SERVE_PID=$!
      echo "active $SERVE_PID $NAME" > "$STATE_FILE"
      echo "Instance name set to \"$NAME\"."
      echo "Starting the daemon as a background process instead; it will not restart if it crashes or start at boot."
      echo "Daemon logs: /tmp/fake-agy-daemon.log"
      echo "Remote-control daemon started."
      echo "Open https://antigravity.google.com and look for \"$NAME\"."
      exit 0
      ;;
    stop)
      if [ -f "$STATE_FILE" ]; then
        read -r STATUS PID NAME < "$STATE_FILE" || true
        if [ -n "${PID:-}" ] && [ "$PID" != "0" ] && kill -0 "$PID" 2>/dev/null; then
          kill "$PID" 2>/dev/null || true
        fi
        echo "inactive 0 ${NAME:-}" > "$STATE_FILE"
      else
        echo "inactive 0" > "$STATE_FILE"
      fi
      echo "Remote-control daemon stopped."
      exit 0
      ;;
    status)
      if [ -f "$STATE_FILE" ]; then
        read -r STATUS PID NAME < "$STATE_FILE" || true
      else
        STATUS="inactive"
        PID="0"
        NAME=""
      fi
      if [ "$STATUS" = "active" ]; then
        IS_ALIVE=0
        if [ -n "${PID:-}" ] && [ "$PID" != "0" ] && [ -d "/proc/$PID" ]; then
          STATE=$(grep -m1 '^State:' "/proc/$PID/status" 2>/dev/null | awk '{print $2}')
          if [ "$STATE" != "Z" ] && [ -n "$STATE" ]; then
            IS_ALIVE=1
          fi
        fi
        if [ "$IS_ALIVE" -eq 0 ]; then
          STATUS="inactive"
        fi
      fi
      if [ "$STATUS" = "active" ]; then
        echo "Daemon status: active (background process, PID $PID)"
        if [ -n "$NAME" ]; then
          echo "Instance name: $NAME (find it at https://antigravity.google.com)"
        else
          echo "Instance name: not assigned yet; the daemon generates one on its first start."
        fi
        echo "Check logs in: /tmp/fake-agy-daemon.log"
      else
        echo "Daemon status: inactive"
        if [ -n "$NAME" ]; then
          echo "Instance name: $NAME (find it at https://antigravity.google.com)"
        else
          echo "Instance name: not assigned yet; the daemon generates one on its first start."
        fi
      fi
      exit 0
      ;;
    *)
      echo "unknown remote-control action: $ACTION" >&2
      exit 1
      ;;
  esac
fi

while [ $# -gt 0 ]; do
  case "$1" in
    -p|--print|--prompt)
      if [ $# -gt 1 ]; then
        PROMPT="$2"
        shift 2
      else
        shift 1
      fi
      ;;
    --print=*|--prompt=*)
      PROMPT="${1#*=}"
      shift 1
      ;;
    --output-format)
      if [ $# -gt 1 ]; then
        OUTPUT_FORMAT="$2"
        shift 2
      else
        shift 1
      fi
      ;;
    --output-format=*)
      OUTPUT_FORMAT="${1#*=}"
      shift 1
      ;;
    *)
      shift 1
      ;;
  esac
done

# --- `agent-run quota`: -p /usage --output-format json ------------------------
# Agy's /usage turn: tab-separated `<group>\t<limit>\t<remaining%>\t<reset>`
# lines in the response; one bucket is low (5%) so the <=10% warning path runs.
if [ "$PROMPT" = "/usage" ] && [ "$OUTPUT_FORMAT" = "json" ]; then
  r1=$(date -u -d '+6 hours'   +%Y-%m-%dT%H:%M:%SZ 2>/dev/null || date -u +%Y-%m-%dT%H:%M:%SZ)
  r2=$(date -u -d '+2 hours'   +%Y-%m-%dT%H:%M:%SZ 2>/dev/null || date -u +%Y-%m-%dT%H:%M:%SZ)
  r3=$(date -u -d '+8 days'    +%Y-%m-%dT%H:%M:%SZ 2>/dev/null || date -u +%Y-%m-%dT%H:%M:%SZ)
  r4=$(date -u -d '+5 hours'   +%Y-%m-%dT%H:%M:%SZ 2>/dev/null || date -u +%Y-%m-%dT%H:%M:%SZ)
  printf '{"conversation_id":"","status":"SUCCESS","response":"Gemini Models\\tWeekly Limit Remaining\\t5%%\\t%s\\nGemini Models\\tFive Hour Limit Remaining\\t100%%\\t%s\\nClaude and GPT models\\tWeekly Limit Remaining\\t82%%\\t%s\\nClaude and GPT models\\tFive Hour Limit Remaining\\t100%%\\t%s\\n","duration_seconds":0,"num_turns":0,"usage":{"input_tokens":0,"output_tokens":0,"thinking_tokens":0,"cache_read_tokens":0,"total_tokens":0}}\n' \
    "$r1" "$r2" "$r3" "$r4"
  exit 0
fi

uuid() {
  cat /proc/sys/kernel/random/uuid 2>/dev/null || echo "00000000-0000-0000-0000-000000000000"
}

json_usage() {
  printf '{"input_tokens":%d,"output_tokens":%d,"thinking_tokens":0,"cache_read_tokens":0,"total_tokens":%d}' "$1" "$2" $(( $1 + $2 ))
}

# emit_prelude: stream-json only. Real agy prints its init line and the first
# step events at the START of a run, long before the final result; do the same
# before the (possibly long) FAKE_SLEEP so the job log is live while it runs.
# Unless FAKE_CONV_DB is requested, emit a real conversation_id so interrupted
# or hanging jobs carry a valid session id for resume/retry.
PRELUDE_DONE=0
emit_prelude() {
  if [ "$OUTPUT_FORMAT" = "stream-json" ]; then
    local init_conv=""
    if [[ ! "$PROMPT" == *FAKE_CONV_DB* ]]; then
      [ -n "${CONV_ID:-}" ] || CONV_ID="$(uuid)"
      init_conv="$CONV_ID"
    fi
    printf '{"event":"init","conversation_id":"%s","init":{"model":"fake","cwd":"%s"}}\n' "$init_conv" "$(pwd)"
    printf '{"event":"step_update","step_update":{"conversation_id":"%s","step_index":0,"state":"DONE","step_type":"user_input"}}\n' "$init_conv"
    PRELUDE_DONE=1
  fi
}

# emit_final <object>: print the final result object for the parsed
# --output-format. json (and anything else): the bare object on one line,
# exactly as before. stream-json: agy's NDJSON envelope -- init line (the
# conversation_id is the object's own "conversation_id" value, may be empty),
# DONE user_input step, DONE tool step (run_command), DONE agent_response step
# whose text_delta is ${RESPONSE_DELTA:-working}, then the result line.
emit_final() {
  local obj="$1" conv_id
  if [ "$OUTPUT_FORMAT" = "stream-json" ]; then
    conv_id=$(printf '%s' "$obj" | python3 -c 'import json,sys
try:
    print(json.load(sys.stdin).get("conversation_id", ""))
except Exception:
    print("")' 2>/dev/null) || conv_id=""
    if [ "${PRELUDE_DONE:-0}" != 1 ]; then
      printf '{"event":"init","conversation_id":"%s","init":{"model":"fake","cwd":"%s"}}\n' \
        "$conv_id" "$(pwd)"
      printf '{"event":"step_update","step_update":{"conversation_id":"%s","step_index":0,"state":"DONE","step_type":"user_input"}}\n' \
        "$conv_id"
    fi
    printf '{"event":"step_update","step_update":{"conversation_id":"%s","step_index":1,"state":"DONE","step_type":"tool","tool_name":"run_command"}}\n' \
      "$conv_id"
    DELTA_VAL="${RESPONSE_DELTA:-working}" python3 -c '
import os, json, sys
delta = os.environ.get("DELTA_VAL", "")
conv = sys.argv[1]
print(json.dumps({"event":"step_update","step_update":{"conversation_id":conv,"step_index":2,"state":"DONE","step_type":"agent_response","text_delta":delta}}))
' "$conv_id"
    printf '%s\n' "$obj" | python3 -c '
import json, sys
data = json.load(sys.stdin)
print(json.dumps({"event":"result","result":data}))
'
  else
    printf '%s\n' "$obj"
  fi
}

emit_interrupted() {
  local end_time duration
  end_time=$(date +%s) || end_time="$START_TIME"
  duration=$(( end_time - START_TIME ))
  echo "error: interrupted" >&2
  emit_final "{\"conversation_id\":\"\",\"status\":\"ERROR\",\"response\":\"\",\"error\":\"interrupted\",\"duration_seconds\":$duration,\"num_turns\":1,\"usage\":$(json_usage 0 0)}"
}

on_sigterm() {
  trap - TERM INT
  emit_interrupted
  exit 1
}

# --- FAKE_SELFTERM: leave immediately, no structured output ----------------
if [[ "$PROMPT" == *FAKE_SELFTERM* ]]; then
  echo "agy finished early on its own"
  exit 0
fi

# --- normal sleep or hang (FAKE_SLEEP=<n>, default 1s) -----------------------
trap on_sigterm TERM INT
emit_prelude

# --- FAKE_HANG: a long-running job that honours SIGTERM --------------------
if [[ "$PROMPT" == *FAKE_HANG* ]]; then
  for ((i = 0; i < 600; i++)); do
    sleep 1 &
    wait $!
  done
  # Unreachable unless someone disarms the trap; behave like a finished job.
  :
fi

# --- FAKE_CONV_DB: create conversation db up front -------------------------
if [[ "$PROMPT" == *FAKE_CONV_DB* ]]; then
  conv_dir="${KUBEADP_AGY_HOME:-$HOME}/.gemini/antigravity-cli/conversations"
  if command -v python3 >/dev/null 2>&1; then
    mkdir -p "$conv_dir" 2>/dev/null || true
    CONV_ID="$(uuid)"
    FAKE_CONV_MARKER="$PROMPT" python3 -c '
import os, sqlite3, sys
db = sys.argv[1]
marker = os.environ.get("FAKE_CONV_MARKER", "")
if not marker:
    sys.exit(1)
conn = sqlite3.connect(db)
conn.execute("CREATE TABLE IF NOT EXISTS steps (idx INTEGER, step_payload BLOB)")
# Frame the prompt like agy protobuf step payloads: the prompt bytes sit
# inside a larger blob.
payload = ("proto-prefix-" + marker + "-proto-suffix").encode("utf-8")
conn.execute("INSERT INTO steps (idx, step_payload) VALUES (1, ?)",
             (payload,))
conn.commit()
conn.close()
' "$conv_dir/$CONV_ID.db" || echo "fake-agy: FAKE_CONV_DB: could not create the conversation db" >&2
    if [ -n "$CONVMAP_LOG_FILE" ]; then
      flat_prompt=$(printf '%s' "$PROMPT" | tr '\n' ' ')
      printf '%s\t%s\n' "$CONV_ID" "$flat_prompt" >>"$CONVMAP_LOG_FILE" 2>/dev/null || true
    fi
  else
    echo "fake-agy: FAKE_CONV_DB: python3 not available" >&2
  fi
fi

# --- normal sleep (FAKE_SLEEP=<n>, default 1s) -----------------------------
SLEEP_SECS=1
if [[ ! "$PROMPT" == *FAKE_HANG* && "$PROMPT" =~ FAKE_SLEEP=([0-9]+) ]]; then
  SLEEP_SECS="${BASH_REMATCH[1]}"
fi
for ((i = 0; i < SLEEP_SECS; i++)); do
  sleep 1 &
  wait $!
done

END_TIME=$(date +%s) || END_TIME="$START_TIME"
DURATION=$(( END_TIME - START_TIME ))

# --- FAKE_DELIVER_QUEUE: deliver the engine's per-run message queue ----------
# The E4 emulation layer keeps mid-run messages in a per-run queue file
# `.engine-message-queue` in the run worktree (one escaped line per message)
# and drains it into the resume prompt (retry.rs / EmulatingEngine::resume_job:
# the queued text is folded into the continuation prompt and the queue file is
# removed). On a resumed invocation the delivered messages therefore arrive as
# part of `$PROMPT`, after the marker line: feed them into the SUCCESS response
# so the black-box test can assert the queued text arrived.
FAKE_DELIVERED_QUEUE=""
if [[ "$PROMPT" == *FAKE_DELIVER_QUEUE* ]]; then
  tail="${PROMPT#*FAKE_DELIVER_QUEUE}"
  tail="${tail#*$'\n\n'}"
  tail="${tail%%$'\n\n[agent-run'*}"
  FAKE_DELIVERED_QUEUE=$(printf '%s\n' "$tail" | sed 's/[[:space:]]*$//')
fi

# --- worktree side effects (cwd is the job worktree) ------------------------
if [[ "$PROMPT" == *"FAKE_WRITE="* ]]; then
  rest="$PROMPT"
  while [[ "$rest" =~ FAKE_WRITE=([A-Za-z0-9._-]+) ]]; do
    f="${BASH_REMATCH[1]}"
    printf 'fake content for %s\n' "$f" > "$f"
    rest="${rest/"FAKE_WRITE=$f"/}"
  done
fi

if [[ "$PROMPT" == *FAKE_STAGE* ]]; then
  printf 'staged content\n' > staged-new.txt
fi

if [[ "$PROMPT" == *FAKE_COMMIT* ]]; then
  printf 'committed content\n' > committed-file.txt
fi

# --- final structured output -----------------------------------------------
if [[ "$PROMPT" == *FAKE_FAIL* ]]; then
  echo "error: the simulated failure happened" >&2
  emit_final "{\"conversation_id\":\"$(uuid)\",\"status\":\"ERROR\",\"response\":\"\",\"error\":\"boom\",\"duration_seconds\":$DURATION,\"num_turns\":1,\"usage\":$(json_usage 1 0)}"
  exit 1
fi

if [[ "$PROMPT" == *FAKE_QUOTA* ]]; then
  echo "API error 429: quota exceeded (resource_exhausted)" >&2
  echo "You have exceeded your quota. Please try again later." >&2
  exit 1
fi

if [[ "$PROMPT" == *FAKE_BADUTF8* ]]; then
  printf 'binary junk before the json \xff\xfe\n'
fi

if [[ "$PROMPT" == *FAKE_NOJSON* ]]; then
  echo "did some work but never emitted structured output"
  exit 0
fi

# --- FAKE_CONV_DB: leave a conversation db behind, emit no conversation id ----
# agy persists each conversation under
# $HOME/.gemini/antigravity-cli/conversations/<uuid>.db (SQLite, table `steps`);
# the runner recovers a missing conversation id by blob-scanning exactly that
# layout (agent-core/src/agy_conversations.rs). This branch creates a fresh
# db whose step payload embeds THIS prompt, then prints a SUCCESS line with an
# EMPTY conversation_id: the engine must find the id from the file.
if [[ "$PROMPT" == *FAKE_CONV_DB* ]]; then
  emit_final "{\"conversation_id\":\"\",\"status\":\"SUCCESS\",\"response\":\"FAKE-DONE-CONVDB\",\"duration_seconds\":$DURATION,\"num_turns\":1,\"usage\":$(json_usage 1 1)}"
  exit 0
fi

# --- FAKE_NOTICE_QUOTED: SUCCESS whose response QUOTES the exit notice --------
# agy's plain-text notice ("terminating 1 background task(s) on exit") would
# force `incomplete` if printed as its own log line; merely quoting it inside
# the structured SUCCESS response must not: the job classifies done.
if [[ "$PROMPT" == *FAKE_NOTICE_QUOTED* ]]; then
  RESPONSE_DELTA='All done. Note: agy printed \"terminating 1 background task(s) on exit\" in its log while I worked; every background task had finished by the time the turn ended.'
  emit_final "{\"conversation_id\":\"$(uuid)\",\"status\":\"SUCCESS\",\"response\":\"All done. Note: agy printed \\\"terminating 1 background task(s) on exit\\\" in its log while I worked; every background task had finished by the time the turn ended.\",\"duration_seconds\":$DURATION,\"num_turns\":1,\"usage\":$(json_usage 1 1)}"
  exit 0
fi

if [[ "$PROMPT" == *FAKE_RATELIMIT_TEXT* ]]; then
  RESPONSE_DELTA='we discuss the rate limit policy'
  emit_final "{\"conversation_id\":\"$(uuid)\",\"status\":\"SUCCESS\",\"response\":\"FAKE-DONE\",\"duration_seconds\":$DURATION,\"num_turns\":1,\"usage\":$(json_usage 1 1)}"
  exit 0
fi

# --- FAKE_DELIVER_QUEUE: the resume turn reports what the queue delivered -----
if [[ "$PROMPT" == *FAKE_DELIVER_QUEUE* ]]; then
  RESPONSE_DELTA="delivered queue: $FAKE_DELIVERED_QUEUE"
  OBJ=$(Q_VAL="$FAKE_DELIVERED_QUEUE" python3 -c '
import os, json, sys
q = os.environ.get("Q_VAL", "")
uuid_val = sys.argv[1]
dur = int(sys.argv[2])
res = {
    "conversation_id": uuid_val,
    "status": "SUCCESS",
    "response": "DELIVERED:" + q,
    "duration_seconds": dur,
    "num_turns": 1,
    "usage": {"input_tokens": 1, "output_tokens": 1, "thinking_tokens": 0, "cache_read_tokens": 0, "total_tokens": 2}
}
print(json.dumps(res))
' "$(uuid)" "$DURATION")
  emit_final "$OBJ"
  exit 0
fi

emit_final "{\"conversation_id\":\"$(uuid)\",\"status\":\"SUCCESS\",\"response\":\"FAKE-DONE\",\"duration_seconds\":$DURATION,\"num_turns\":1,\"usage\":$(json_usage 1 1)}"
exit 0
