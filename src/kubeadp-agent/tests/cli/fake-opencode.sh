#!/usr/bin/env bash
# Scripted fake `opencode` (`run --format json` harness) for the agent-run
# black-box test suite (KA-4 opencode job engine parity).
#
# The engine-aware runner invokes it for `engine:"opencode"` jobs (see
# src/engine/opencode.rs job_command):
#   fake-opencode.sh run --format json [--model provider/model] --auto \
#                       [--session <session-id> | --continue] \
#                       --dir <worktree> [message...]
# cwd is the job worktree; stdout+stderr are captured into the job log. The
# prompt is the trailing message positional (opencode's CLI contract: the
# message is NOT read from stdin).
#
# Output: opencode's `run --format json` NDJSON event stream, matching the
# event shapes captured live from opencode 1.19.4:
#   {"type":"step_start",...,"sessionID":"<sid>","part":{...}}
#   {"type":"text",...,"part":{"type":"text","text":"..."}}
#   {"type":"tool_use",...,"part":{"type":"tool","tool":"bash","state":{...}}}
#   {"type":"step_finish",...,"part":{"reason":"stop","tokens":{...},"cost":0}}
# The harness session id (`sessionID`) is the job's resume key (job.rs
# `extract_conversation_id` pattern 2c), so it is emitted on every event line.
#
# STRICT flag validation: this fake mirrors the real opencode `run` arg parser
# for the options the engine passes -- an option outside the allow-list makes
# it exit 1, so the runner cannot silently invent opencode argv.
#
# Behaviour is chosen by markers inside the prompt text (the message
# positional, joined):
#   FAKE_SLEEP=<n>   sleep n seconds (default 1; combinable with any marker)
#   FAKE_FAIL        emit a terminal error event, exit 1         -> status failed
#   FAKE_AUTHFAIL    emit the real opencode auth-failure shape: a `text` event
#                    carrying the reason then a terminal error event with the
#                    same text; exit 1                          -> status failed
#   FAKE_QUOTA       print a quota/rate-limit banner (matches the
#                    QUOTA_MARKERS in agent-core/src/job.rs) plus an error
#                    event, exit 1                              -> status quota-exhausted
#   FAKE_HANG        sleep forever (SIGTERM default kills)       -> kill target
#   FAKE_WRITE=<f>   write file <f> into the current dir (the worktree),
#                    repeatable with different names
#   FAKE_COMMIT      write committed-file.txt and `git commit` it in the worktree
#   FAKE_NOJSON      plain text only, exit 0, no event stream    -> status incomplete
#   FAKE_TOOLCALLS   emit a step_finish reason "tool-calls", sleep 1, then a
#                    final stop turn                             -> status done
#                    (proves a tool-calling turn does not complete the job)
#   FAKE_BADUTF8     print a line containing a raw 0xff byte before the events
#
# If FAKE_ARGV_LOG=<file> or KUBEADP_ARGV_LOG=<file> is set, every invocation
# first appends its full argv (tab-separated, one line) to that file, and
# FAKE_PROMPT_LOG=<file> appends the joined message positional so a test can
# assert the exact prompt the runner delivered.
#
# No marker: base behaviour (read the prompt, sleep 1s, emit step_start +
# text + step_finish stop, exit 0).

set -u

ARGV_LOG_FILE="${KUBEADP_ARGV_LOG:-${FAKE_ARGV_LOG:-}}"

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

RESUME_ID=""
MODEL=""
DIR=""
PROMPT=""

# --- parse the argv (the runner's headless `run --format json` shape) ---------
# Allow-list mirroring `opencode run --help` (v1.19.4): the job options the
# engine passes. Any other option is a hard error, exactly like the real CLI.
[ "${1:-}" = "run" ] || { echo "error: expected 'run', got '${1:-}'" >&2; exit 1; }
shift
while [ $# -gt 0 ]; do
  case "$1" in
    --session)
      if [ $# -gt 1 ]; then
        RESUME_ID="$2"
        shift 2
      else
        shift 1
      fi
      ;;
    --model|--dir|--agent|--title)
      if [ $# -gt 1 ]; then
        [ "$1" = "--dir" ] && DIR="$2"
        [ "$1" = "--model" ] && MODEL="$2"
        shift 2
      else
        shift 1
      fi
      ;;
    --format|--log-level)
      if [ $# -gt 1 ]; then
        shift 2
      else
        shift 1
      fi
      ;;
    -i|--interactive|--auto|-c|--continue|--fork|-m|--print-logs|--pure)
      shift 1
      ;;
    -*)
      echo "error: unknown option '$1'" >&2
      exit 1
      ;;
    *)
      # The message positional: everything else joins into the prompt.
      PROMPT="${PROMPT:+$PROMPT }$1"
      shift 1
      ;;
  esac
done

if [ -n "${FAKE_PROMPT_LOG:-}" ]; then
  printf '%s\n' "$PROMPT" >>"$FAKE_PROMPT_LOG" 2>/dev/null || true
fi
[ -n "$DIR" ] || DIR=`pwd`
cd "$DIR" 2>/dev/null || true

# --- harness session id -------------------------------------------------------
uuid() {
  cat /proc/sys/kernel/random/uuid 2>/dev/null || echo "00000000-0000-0000-0000-000000000000"
}
SESSION_ID=""
if [ -n "$RESUME_ID" ]; then
  SESSION_ID="$RESUME_ID"
else
  SESSION_ID="`uuid`"
fi
CHAT_MODEL="${MODEL:-provider/model}"

# --- emission helpers ---------------------------------------------------------
# emit <type> <json-part>: wraps a part payload in the opencode event envelope
# carrying the session id.
emit() {
  local ty="$1" part="$2"
  printf '{"type":"%s","timestamp":%s,"sessionID":"%s","part":%s}\n' \
    "$ty" "`date +%s%3N`" "$SESSION_ID" "$part"
}

# emit_text <text>: a visible assistant `text` event.
emit_text() {
  emit "text" "{\"type\":\"text\",\"text\":\"$1\",\"time\":{\"start\":`date +%s%3N`,\"end\":`date +%s%3N`}}"
}

# emit_tool_use: a tool invocation (bash).
emit_tool_use() {
  emit "tool_use" "{\"type\":\"tool\",\"tool\":\"bash\",\"callID\":\"chatcmpl-tool-oc\",\"state\":{\"status\":\"completed\",\"input\":{\"command\":\"echo fake\"}}}"
}

# emit_finish <reason>: a step_finish with token/cost accounting.
emit_finish() {
  local reason="$1"
  emit "step_finish" "{\"type\":\"step-finish\",\"reason\":\"${reason}\",\"tokens\":{\"total\":1000,\"input\":700,\"output\":300,\"reasoning\":0,\"cache\":{\"write\":0,\"read\":0}},\"cost\":0}"
}

# emit_error <message>: the terminal opencode error event.
emit_error() {
  printf '{"type":"error","timestamp":%s,"sessionID":"%s","error":{"name":"UnknownError","data":{"message":"%s"}}}\n' \
    "`date +%s%3N`" "$SESSION_ID" "$1"
}

RESPONSE="OPENCODE-DONE"
if [ -n "$RESUME_ID" ]; then
  RESPONSE="OPENCODE-RESUMED:$RESUME_ID"
fi

# The harness emits its first step at the START of the run, so the job log is
# live and the session id is on the wire early (job.rs extracts it from any
# line).
emit "step_start" "{\"type\":\"step-start\",\"sessionID\":\"$SESSION_ID\"}"

# --- FAKE_HANG: run forever (a plain SIGTERM terminates it) -------------------
if [ -n "$PROMPT" ] && [[ "$PROMPT" == *FAKE_HANG* ]]; then
  while true; do
    sleep 1
  done
fi

# --- normal sleep (FAKE_SLEEP=<n>, default 1s) ---------------------------------
SLEEP_SECS=1
if [ -n "$PROMPT" ] && [[ "$PROMPT" =~ FAKE_SLEEP=([0-9]+) ]]; then
  SLEEP_SECS="${BASH_REMATCH[1]}"
fi

for ((i = 0; i < SLEEP_SECS; i++)); do
  sleep 1 &
  wait "$!" 2>/dev/null || true
done

# --- worktree side effects (cwd is the job worktree) --------------------------
if [ -n "$PROMPT" ]; then
  if [[ "$PROMPT" == *FAKE_WRITE=* ]]; then
    rest="$PROMPT"
    while [[ "$rest" =~ FAKE_WRITE=([A-Za-z0-9._-]+) ]]; do
      f="${BASH_REMATCH[1]}"
      printf 'fake content for %s\n' "$f" > "$f"
      rest="${rest/"FAKE_WRITE=$f"/}"
    done
  fi

  if [[ "$PROMPT" == *FAKE_COMMIT* ]]; then
    printf 'committed content\n' > committed-file.txt
    git add committed-file.txt 2>/dev/null
    git -c user.name=fake -c user.email=fake@test commit -q -m "fake opencode commit" 2>/dev/null || true
  fi
fi

# --- final structured output ---------------------------------------------------
if [ -n "$PROMPT" ] && [[ "$PROMPT" == *FAKE_FAIL* ]]; then
  emit_error "the simulated failure happened"
  exit 1
fi

# FAKE_AUTHFAIL: the real opencode auth-failure shape. A `text` event carries
# the reason, then a terminal error event with the same text; exit 1.
if [ -n "$PROMPT" ] && [[ "$PROMPT" == *FAKE_AUTHFAIL* ]]; then
  emit_text "Failed to authenticate: OAuth session expired and could not be refreshed"
  emit_error "Failed to authenticate: OAuth session expired and could not be refreshed"
  exit 1
fi

if [ -n "$PROMPT" ] && [[ "$PROMPT" == *FAKE_QUOTA* ]]; then
  echo "API error 429: quota exceeded (resource_exhausted)" >&2
  echo "You have exceeded your quota. Please try again later." >&2
  emit_error "API error 429: quota exceeded (resource_exhausted)"
  exit 1
fi

if [ -n "$PROMPT" ] && [[ "$PROMPT" == *FAKE_NOJSON* ]]; then
  echo "did some work but never emitted an event stream"
  exit 0
fi

if [ -n "$PROMPT" ] && [[ "$PROMPT" == *FAKE_BADUTF8* ]]; then
  printf 'binary junk before the json \xff\xfe\n'
fi

# A tool-calling turn (FAKE_TOOLCALLS): step_finish reason "tool-calls" must
# NOT complete the job; the following stop turn does.
if [ -n "$PROMPT" ] && [[ "$PROMPT" == *FAKE_TOOLCALLS* ]]; then
  emit_tool_use
  emit_finish "tool-calls"
  sleep 1
  emit "step_start" "{\"type\":\"step-start\",\"sessionID\":\"$SESSION_ID\"}"
  emit_text "done after tool calls"
  emit_finish "stop"
  exit 0
fi

emit_text "$RESPONSE"
emit_finish "stop"
exit 0
