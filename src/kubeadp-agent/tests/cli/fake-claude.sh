#!/usr/bin/env bash
# Scripted fake `claude` (stream-json headless harness) for the agent-run
# black-box test suite (HL-71 claude job engine parity).
#
# The engine-aware runner invokes it for `engine:"claude"` jobs (see
# agent-core/src/runner.rs build_claude_command_and_args):
#   fake-claude.sh --dangerously-skip-permissions -p \
#                  --input-format stream-json --output-format stream-json \
#                  --verbose [--permission-mode plan] [--resume <session-id>]
#
# STRICT flag validation (HL-71 S1): this fake mirrors the real `claude`
# CLI's arg parser for the options it knows -- any option outside the
# allow-list makes it exit 1 with `error: unknown option '<flag>'` (the
# exact failure the real claude 2.1.322 gave for the agy-only `--mode`
# flag), and `--permission-mode` must be one of
# default|acceptEdits|plan|bypassPermissions (exit 2 with `invalid value`
# otherwise). Without this the fake swallowed every argv the runner
# invented and the `--mode` bug shipped to the live worker.
# cwd is the job worktree; stdout+stderr are captured into the job log. The
# prompt is NOT an argv element: the runner writes one NDJSON user line
# (the same envelope `engine/claude/stream_json.rs::render_text_request`
# renders) to the piped stdin, which this script reads as its first line.
#
# Output: claude's stream-json NDJSON protocol, matching
# `engine/claude/{stream_json.rs,job.rs}`:
#   {"type":"system","subtype":"init"}
#   {"type":"assistant","message":{"role":"assistant","content":[{"type":"text","text":"..."}]},"session_id":"<sid>"}
#   {"type":"result","subtype":"success","is_error":false,"result":"...","session_id":"<sid>"}
# (an error run ends with a `result`/`system` error line and exit 1).
# The harness session id (`session_id`) is the job's resume key (job.rs
# `extract_conversation_id` pattern 2b), so it is emitted on the assistant
# and result lines.
#
# Behaviour is chosen by markers inside the prompt text (read from stdin):
#   FAKE_SLEEP=<n>   sleep n seconds (default 1; combinable with any marker)
#   FAKE_FAIL        emit a stream-json error result, exit 1  -> status failed
#   FAKE_AUTHFAIL    emit the REAL claude auth-failure shape: a synthetic
#                    `assistant` message carrying the reason text model, then
#                    an error `result` whose `result` field holds the same
#                    text (NO `error` field), plus a `[claude-code:...]` log
#                    line; exit 1                                -> status failed
#   FAKE_QUOTA       print a quota/rate-limit banner (matches the
#                    QUOTA_MARKERS in agent-core/src/job.rs), exit 1
#                                                          -> status quota-exhausted
#   FAKE_HANG        sleep forever (SIGTERM default kills)     -> kill target
#   FAKE_WRITE=<f>   write file <f> into the current dir (the worktree),
#                    repeatable with different names
#   FAKE_STAGE       write staged-new.txt, `git add` it, and `git rm`
#                    README.txt (staged change + staged deletion)
#   FAKE_COMMIT      write committed-file.txt and `git commit` it in the worktree
#   FAKE_NOJSON      plain text only, exit 0, no structured result -> status incomplete
#   FAKE_BADUTF8     print a line containing a raw 0xff byte before the success result
#
# If FAKE_ARGV_LOG=<file> is set, every invocation first appends its full argv
# (tab-separated, one line) to that file, and FAKE_PROMPT_LOG=<file> appends the
# raw received stdin line (the stream-json user envelope, tab-prefixed) so a
# test can assert the exact prompt the runner delivered over stdin.
#
# No marker: base behaviour (read the prompt, sleep 1s, print an assistant +
# success result line, exit 0).

set -u

# --- argv logging for the black-box suite -------------------------------------
{
  printf '%s' "$0"
  for a in "$@"; do printf '\t%s' "$a"; done
  printf '\n'
} >>.fake-argv 2>/dev/null || true

if [ -n "${FAKE_ARGV_LOG:-}" ]; then
  {
    printf '%s' "$0"
    for a in "$@"; do printf '\t%s' "$a"; done
    printf '\n'
  } >>"$FAKE_ARGV_LOG" 2>/dev/null || true
fi

RESUME_ID=""
PROMPT=""
PROMPT_RAW=""

# --- parse the argv (the runner's stream-json headless shape) ------------------
# Allow-list mirroring `claude --help` (v2.1.322): the headless job options
# plus the session/model extras. Any other option is a hard error, exactly
# like the real CLI.
PERMISSION_MODE=""
while [ $# -gt 0 ]; do
  case "$1" in
    --resume)
      if [ $# -gt 1 ]; then
        RESUME_ID="$2"
        shift 2
      else
        shift 1
      fi
      ;;
    --input-format|--output-format|--model|--session-id|--append-system-prompt|--add-dir)
      if [ $# -gt 1 ]; then
        shift 2
      else
        shift 1
      fi
      ;;
    --permission-mode)
      if [ $# -gt 1 ]; then
        PERMISSION_MODE="$2"
        shift 2
      else
        shift 1
      fi
      ;;
    -p|--print|--verbose|--dangerously-skip-permissions|--include-partial-messages)
      shift 1
      ;;
    --)
      break
      ;;
    -*)
      echo "error: unknown option '$1'" >&2
      exit 1
      ;;
    *)
      shift 1
      ;;
  esac
done
case "$PERMISSION_MODE" in
  default|acceptEdits|plan|bypassPermissions) ;;
  "") ;;
  *)
    echo "error: invalid value '$PERMISSION_MODE' for '--permission-mode <PERMISSION_MODE>': possible values are default, acceptEdits, plan, bypassPermissions" >&2
    exit 2
    ;;
esac

# --- read the prompt: exactly one NDJSON user line from the piped stdin --------
if IFS= read -r line; then
  PROMPT_RAW="$line"
  if [ -n "${FAKE_PROMPT_LOG:-}" ]; then
    printf '\t%s\n' "$line" >>"$FAKE_PROMPT_LOG" 2>/dev/null || true
  fi
  PROMPT=$(printf '%s' "$line" | python3 -c '
import json, sys
try:
    v = json.load(sys.stdin)
    parts = (v.get("message") or {}).get("content") or []
    text = " ".join(p.get("text", "") for p in parts if isinstance(p, dict))
    print(text)
except Exception:
    print("")
' 2>/dev/null || true)
fi

# --- markers ---------------------------------------------------------------
SID_FILE=""

uuid() {
  cat /proc/sys/kernel/random/uuid 2>/dev/null || echo "00000000-0000-0000-0000-000000000000"
}

# The harness session id for this run: on a resume the harness keeps the same
# conversation, so reuse the resume target; on a fresh run generate one. To
# keep the id stable across the black-box retry assertions it is pinned to
# the resume id when present, otherwise a fresh uuid per run.
SESSION_ID=""
if [ -n "$RESUME_ID" ]; then
  SESSION_ID="$RESUME_ID"
else
  SESSION_ID="$(uuid)"
fi

# emit_assistant <text>: one stream-json assistant message with the session id.
emit_assistant() {
  printf '{"type":"assistant","message":{"role":"assistant","content":[{"type":"text","text":"%s"}]},"session_id":"%s"}\n' \
    "$1" "$SESSION_ID"
}

# emit_synthetic_assistant <text>: one stream-json assistant message carrying
# model "<synthetic>" -- the shape a FAILING real claude run uses to deliver
# its reason text without a real model attribution (Defect 2, HL-71 S1).
emit_synthetic_assistant() {
  printf '{"type":"assistant","message":{"role":"assistant","model":"<synthetic>","content":[{"type":"text","text":"%s"}]},"session_id":"%s"}\n' \
    "$1" "$SESSION_ID"
}

# emit_result <text>: the terminal success `result` line with the session id.
emit_result() {
  printf '{"type":"result","subtype":"success","is_error":false,"result":"%s","session_id":"%s"}\n' \
    "$1" "$SESSION_ID"
}

# emit_error_result <err>: the terminal error `result` line.
emit_error_result() {
  printf '{"type":"result","subtype":"error","is_error":true,"error":"%s","session_id":"%s"}\n' \
    "$1" "$SESSION_ID"
}

# emit_error_result_with_result <err>: the REAL claude error shape (Defect 2,
# HL-71 S1): the reason lives in the `result` field, there is NO `error`
# field.
emit_error_result_with_result() {
  printf '{"type":"result","subtype":"error","is_error":true,"result":"%s","session_id":"%s"}\n' \
    "$1" "$SESSION_ID"
}

RESPONSE="CLAUDE-DONE"
if [ -n "$RESUME_ID" ]; then
  RESPONSE="CLAUDE-RESUMED:$RESUME_ID"
fi

# The harness emits its init and the first assistant message at the START of
# the run, long before the terminal result, so the job log is live and the
# session id is on the wire early (job.rs extracts it from any line).
printf '{"type":"system","subtype":"init"}\n'
emit_assistant "working"

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

  if [[ "$PROMPT" == *FAKE_STAGE* ]]; then
    printf 'staged content\n' > staged-new.txt
    git add staged-new.txt
    git rm -q README.txt 2>&1 || git rm -qf README.txt
  fi

  if [[ "$PROMPT" == *FAKE_COMMIT* ]]; then
    printf 'committed content\n' > committed-file.txt
    git add committed-file.txt
    git commit -q -m "work done by fake claude"
  fi
fi

# --- final structured output ---------------------------------------------------
if [ -n "$PROMPT" ] && [[ "$PROMPT" == *FAKE_FAIL* ]]; then
  echo "error: the simulated failure happened" >&2
  emit_error_result "boom"
  exit 1
fi

# FAKE_AUTHFAIL (Defect 2): the real claude authentication-failure shape. The
# harness emits a synthetic `assistant` message (model "<synthetic>") whose
# text content is the message, then an error `result` carrying the SAME text
# in its `result` field (no `error` field), plus the `[claude-code:...]` log
# line -- then exits 1.
if [ -n "$PROMPT" ] && [[ "$PROMPT" == *FAKE_AUTHFAIL* ]]; then
  printf '[claude-code:unrecognized_model] Failed to authenticate: OAuth session expired and could not be refreshed\n' >&2
  emit_synthetic_assistant "Failed to authenticate: OAuth session expired and could not be refreshed"
  emit_error_result_with_result "Failed to authenticate: OAuth session expired and could not be refreshed"
  exit 1
fi

if [ -n "$PROMPT" ] && [[ "$PROMPT" == *FAKE_QUOTA* ]]; then
  echo "API error 429: quota exceeded (resource_exhausted)" >&2
  echo "You have exceeded your quota. Please try again later." >&2
  exit 1
fi

if [ -n "$PROMPT" ] && [[ "$PROMPT" == *FAKE_NOJSON* ]]; then
  echo "did some work but never emitted a result"
  exit 0
fi

if [ -n "$PROMPT" ] && [[ "$PROMPT" == *FAKE_BADUTF8* ]]; then
  printf 'binary junk before the json \xff\xfe\n'
fi

emit_result "$RESPONSE"
exit 0
