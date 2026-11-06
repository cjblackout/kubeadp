#!/usr/bin/env bash
# lib.sh - shared helpers for the kubeadp-agent sandbox attack suite.
#
# Sourced by run.sh (the unsandboxed harness). Provides:
#   - result recording (rec/rec_skip/rec_info), the TSV accumulator
#   - scratch tree + policy generation (both agents)
#   - sandbox-exec launching primitives (sb_exec / sb_exec_bg / vpy_exec)
#   - concurrency helpers (wait_ready / wait_agent_done / stop_launcher)
#   - verdict collection from captured agent logs
#
# Plain bash + python3 (stdlib only). No cargo, no network, no cluster writes.

set -u

# ---------------------------------------------------------------------------
# Globals owned by lib.sh (set by init_scratch).
# ---------------------------------------------------------------------------
PY3="$(command -v python3 || echo /usr/bin/python3)"
SANDBOX_PYDIR=""
RUN_RUNID=""
RUN_TAG=""
SCRATCH=""
WORK=""
RW_A="" RW_B=""
RO_SHARE=""
SHARED=""
LOGDIR=""
SECRET_A="" SECRET_B=""
POL_A="" POL_B=""
A_TOOLS="" B_TOOLS=""
A_STATE="" B_STATE=""
RESULTS_TSV=""
HARD_FAILS=0
CONTROL_FAILS=0
BINARY="" IS_FAKE=0

die() { echo "FATAL: $*" >&2; exit 1; }

# ---------------------------------------------------------------------------
# Result recording.
#   rec <id> <PASS|FAIL|SKIP|INFO> <message>
# In fake/stub runs every FAIL is an EXPECTED leak detection (negative
# control), flagged [CONTROL] and excluded from the exit code.
# ---------------------------------------------------------------------------
rec() {
  local id="$1" result="$2" message="${3:-}"
  local control=0
  if [ "$result" = "FAIL" ] && [ "$IS_FAKE" = "1" ]; then
    control=1
  fi
  case "$result" in
    PASS) printf '%s PASS %s\n' "$id" "$message" ;;
    INFO) printf '%s INFO %s\n' "$id" "$message" ;;
    FAIL)
      if [ "$control" = "1" ]; then
        CONTROL_FAILS=$((CONTROL_FAILS + 1))
        printf '%s FAIL [CONTROL] %s\n' "$id" "$message"
      else
        HARD_FAILS=$((HARD_FAILS + 1))
        printf '%s FAIL %s\n' "$id" "$message"
      fi
      ;;
    SKIP) printf '%s SKIP(%s)\n' "$id" "$message" ;;
    *) die "rec: unknown result '$result' for $id" ;;
  esac
  message=$(printf '%s' "$message" | tr '\t' ' ')
  printf '%s\t%s\t%s\t%s\n' "$id" "$result" "$message" "$control" >> "$RESULTS_TSV"
}

rec_skip() { rec "$1" SKIP "$2"; }
rec_info() { rec "$1" INFO "$2"; }

# ---------------------------------------------------------------------------
# Fake stub detection.
# ---------------------------------------------------------------------------
detect_fake() {
  case "$BINARY" in
    *fake*) IS_FAKE=1 ;;
    *)
      if "$BINARY" sandbox-probe --json 2>/dev/null | grep -q '"fake"'; then
        IS_FAKE=1
      fi
      ;;
  esac
}

# ---------------------------------------------------------------------------
# Runtime / PID-namespace derivation for the in-sandbox cases.
#
# The harness runs unsandboxed in the pod; the binary's own `sandbox-probe
# --json` reports the POD runtime. From it we derive what the sandbox itself
# does to the agent:
#   - gvisor / runc-namespaces -> the sandbox puts the agent in a FRESH PID
#     namespace (and mounts a fresh procfs), so B cannot see A's pids at all
#     and the host pid supplied in env is unresolvable inside B.
#   - kata / runc-Landlock     -> the sandbox shares the pod's /proc under a
#     Landlock domain: metadata-only entries of other processes (stat,
#     cmdline, fd/task lists, readlink targets) are visible by design, while
#     the data-bearing files (environ, maps, mem) stay blocked by Landlock.
# Sets KUBEADP_TEST_RUNTIME + KUBEADP_TEST_PIDNS (both empty when the probe
# cannot be read; the cases then behave as before / most conservative).
# ---------------------------------------------------------------------------
KUBEADP_TEST_RUNTIME=""
KUBEADP_TEST_PIDNS=""
probe_runtime_env() {
  KUBEADP_TEST_RUNTIME=""
  KUBEADP_TEST_PIDNS=""
  local j rt cb
  j="$("$BINARY" sandbox-probe --json 2>/dev/null)" || return 0
  [ -n "$j" ] || return 0
  rt="$(printf '%s' "$j" | "$PY3" -c 'import json,sys
try: print(json.load(sys.stdin)["runtime"])
except Exception: print("")' 2>/dev/null)"
  cb="$(printf '%s' "$j" | "$PY3" -c 'import json,sys
try: print(json.load(sys.stdin)["chosen_backend"])
except Exception: print("")' 2>/dev/null)"
  case "$rt" in
    gvisor) KUBEADP_TEST_RUNTIME="gvisor"; KUBEADP_TEST_PIDNS="1" ;;
    kata)   KUBEADP_TEST_RUNTIME="kata";   KUBEADP_TEST_PIDNS="0" ;;
    fake)   KUBEADP_TEST_RUNTIME="fake";   KUBEADP_TEST_PIDNS="0" ;;
    runc)
      case "$cb" in
        *"Landlock"*)   KUBEADP_TEST_RUNTIME="runc-landlock"; KUBEADP_TEST_PIDNS="0" ;;
        *"namespaces"*) KUBEADP_TEST_RUNTIME="runc-ns";       KUBEADP_TEST_PIDNS="1" ;;
        *)              KUBEADP_TEST_RUNTIME="runc";          KUBEADP_TEST_PIDNS="" ;;
      esac
      ;;
    *) ;;
  esac
}

# ---------------------------------------------------------------------------
# Scratch tree + policy generation (contract v1.3).
# ---------------------------------------------------------------------------
init_scratch() {
  SANDBOX_PYDIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
  RUN_RUNID="hl86-$$"
  RUN_TAG="tg-$(printf '%s' "${RANDOM}${RANDOM}$$$(date +%s)" | sha256sum | cut -c1-12)"
  SCRATCH="$(mktemp -d "${TMPDIR:-/tmp}/${RUN_RUNID}.XXXXXX")" || die "mktemp failed"
  WORK="$SCRATCH/work"
  RW_A="$WORK/A.rw"
  RW_B="$WORK/B.rw"
  RO_SHARE="$SCRATCH/ro-share"
  SHARED="$WORK/shared"
  LOGDIR="$SCRATCH/logs"
  A_TOOLS="$RW_A/tools"
  B_TOOLS="$RW_B/tools"
  A_STATE="$RW_A/state"
  B_STATE="$RW_B/state"
  RESULTS_TSV="$SCRATCH/results.tsv"
  mkdir -p "$WORK" "$A_TOOLS" "$B_TOOLS" "$RO_SHARE" "$SHARED" "$LOGDIR" \
           "$SCRATCH/secrets" "$SCRATCH/policies"

  # helpers: python tools into every agent's OWN rw dir; case scripts into B
  cp -r "$SANDBOX_PYDIR/py/." "$A_TOOLS/" 2>/dev/null || true
  cp -r "$SANDBOX_PYDIR/py/." "$B_TOOLS/" 2>/dev/null || true
  mkdir -p "$B_TOOLS/cases"
  cp -r "$SANDBOX_PYDIR/cases/." "$B_TOOLS/cases/" 2>/dev/null || true
  cp "$SANDBOX_PYDIR/lib_case.sh" "$B_TOOLS/lib_case.sh" 2>/dev/null || true
  cp "$SANDBOX_PYDIR/lib_case.sh" "$A_TOOLS/lib_case.sh" 2>/dev/null || true

  printf 'hl86-ro-marker-%s\n' "$RUN_TAG" > "$RO_SHARE/marker.txt"
  chmod 644 "$RO_SHARE/marker.txt"
  chmod -R a+rX "$RO_SHARE"

  _mksecret A
  _mksecret B
  chmod 400 "$SECRET_A" "$SECRET_B"

  POL_A="$SCRATCH/policies/policy.A.json"
  POL_B="$SCRATCH/policies/policy.B.json"
  mk_policy A > "$POL_A"
  mk_policy B > "$POL_B"
  chmod 644 "$POL_A" "$POL_B"
}

_mksecret() {
  local role="$1" v
  v="hl86-secret-${role}-$(head -c 9 /dev/urandom | base64 | tr -dc 'a-zA-Z0-9' | head -c 14)"
  if [ "$role" = "A" ]; then SECRET_A="$SCRATCH/secrets/secret.A"; printf '%s\n' "$v" > "$SECRET_A";
  else                        SECRET_B="$SCRATCH/secrets/secret.B"; printf '%s\n' "$v" > "$SECRET_B"; fi
}

mk_policy() {
  local agent="$1" rwdir
  if [ "$agent" = "A" ]; then rwdir="$RW_A"; else rwdir="$RW_B"; fi
  "$PY3" - "$agent" "$RUN_RUNID" "$rwdir" "$RO_SHARE" "$SHARED" "$WORK" <<'PYEOF'
import json, sys
agent, runid, rwd, ro, shared, work = sys.argv[1:7]
# The read-only base dirs the case tools live on: under a Landlock-default-deny
# domain (kata / runc-Landlock) every exec / read of the tools would otherwise
# be blocked. They mirror the launcher's own default ro list.
ro_base = ["/opt/tools", "/usr", "/bin", "/lib", "/lib64", "/etc"]
ro_list = list(dict.fromkeys([ro] + ro_base))
print(json.dumps({
  "agent_id": agent,
  "jobset_id": "hl86-%s" % runid,
  "rw": [rwd],
  "ro": ro_list,
  "shared_rw": [shared],
  "hide": [work],
  "tmp": "auto",
  "secret": {"fd": 3, "dest": "secret"},
  "env_allow": ["PATH","HOME","LANG","TERM","TMPDIR","SSL_CERT_FILE",
                "REQUESTS_CA_BUNDLE","NODE_USE_SYSTEM_CA","KUBEADP_*","A2A_*"],
  "net": "shared",
}, indent=2, sort_keys=True))
PYEOF
}

# ---------------------------------------------------------------------------
# sandbox-exec launchers.
# env passing: the real sandbox filters to env_allow (KUBEADP_* + basics), so
# all cross-boundary data travels in KUBEADP_TEST_* variables. SIMPLE values
# knowingly do not carry secrets.
# ---------------------------------------------------------------------------
_sd_rw() { if [ "$1" = "A" ]; then echo "$RW_A"; else echo "$RW_B"; fi }

# forward TEST_* env into the sandboxed process. Ends with an exec of the
# given command, so call WITHOUT an `exec` prefix.
# Args: full command line to run (the binary + its args).
_forward_env() {
  local role="$1"; shift
  local toolsdir
  if [ "$role" = "A" ]; then toolsdir="$A_TOOLS"; else toolsdir="$B_TOOLS"; fi
  # everything under KUBEADP_TEST_ plus SANDBOX_* travels as env; TMPDIR is
  # set by the sandbox itself (and by the fake stub). The stub's private tmp
  # and the tools' temp files stay under $SCRATCH so the harness can remove
  # them in teardown. KUBEADP_* is in env_allow.
  exec env \
    KUBEADP_TEST_ROLE="$role" \
    KUBEADP_TEST_TAG="$RUN_TAG" \
    KUBEADP_TEST_CASE="${SANDBOX_CASE:-}" \
    KUBEADP_TEST_RW="$(_sd_rw "$role")" \
    KUBEADP_TEST_RO_SHARE="$RO_SHARE" \
    KUBEADP_TEST_SHARED="$SHARED" \
    KUBEADP_TEST_B_TOOLS="$toolsdir" \
    KUBEADP_TEST_TARGET_PID="${KUBEADP_TEST_TARGET_PID:-}" \
    KUBEADP_TEST_TARGET_RW="${KUBEADP_TEST_TARGET_RW:-}" \
    KUBEADP_TEST_PORT="${KUBEADP_TEST_PORT:-}" \
    KUBEADP_TEST_SECRET_HASH="${KUBEADP_TEST_SECRET_HASH:-}" \
    KUBEADP_TEST_MARKER="${KUBEADP_TEST_MARKER:-}" \
    KUBEADP_TEST_DESCLOG="${KUBEADP_TEST_DESCLOG:-}" \
    KUBEADP_TEST_DAEMON_PIDS="${KUBEADP_TEST_DAEMON_PIDS:-}" \
    KUBEADP_TEST_SCRATCH="$SCRATCH" \
    KUBEADP_TEST_RUNTIME="${KUBEADP_TEST_RUNTIME:-}" \
    KUBEADP_TEST_PIDNS="${KUBEADP_TEST_PIDNS:-}" \
    SANDBOX_TEST_ROLE="$role" \
    SANDBOX_TEST_TAG="$RUN_TAG" \
    "${@}"
}

# sb_exec_bg <role> <sec> <pol> <log> -- <cmd...>   : echo launcher pid
sb_exec_bg() {
  local role="$1" sec="$2" pol="$3" out="$4"; shift 4
  [ "$1" = "--" ] && shift
  (
    exec 3<"$sec"
    _forward_env "$role" \
      "$BINARY" sandbox-exec --policy "$pol" -- "$@"
  ) >"$out" 2>&1 &
  echo $!
}

# sb_exec ... : synchronous; sets SB_RC, SB_OUT (SB_ERR empty), appends to log
sb_exec() {
  local role="$1" sec="$2" pol="$3" out="$4"; shift 4
  [ "$1" = "--" ] && shift
  local tmp
  tmp="$(mktemp "${TMPDIR:-/tmp}/hl86-sbexec.XXXXXX")"
  (
    exec 3<"$sec"
    _forward_env "$role" \
      "$BINARY" sandbox-exec --policy "$pol" -- "$@"
  ) >"$tmp" 2>&1
  SB_RC=$?
  SB_OUT="$(cat "$tmp")"
  cat "$tmp" >> "$out"
  rm -f "$tmp"
  return 0
}

# vpy_exec <role> <sec> <pol> <log> -- <tool-relpath> [args...]
vpy_exec() {
  local role="$1" sec="$2" pol="$3" out="$4"; shift 4
  [ "$1" = "--" ] && shift
  local tool="$1"; shift
  sb_exec "$role" "$sec" "$pol" "$out" -- "$PY3" "$(_sd_rw "$role")/tools/$tool" "$@"
  echo "$SB_OUT"
}

# --- concurrency ------------------------------------------------------------
wait_ready() {  # wait_ready <state-dir> <timeout>
  local d="$1" t="$2" i=0
  while [ "$i" -lt "$((t * 10 / 3))" ]; do
    [ -f "$d/ready" ] && return 0
    sleep 0.3; i=$((i + 1))
  done
  return 1
}

# A zombie (state Z) is not alive: when the harness runs under a launcher that adopts reparented orphans and does not
# reap them, `kill -0` keeps succeeding for a process that has long exited.
pid_alive() {
  kill -0 "$1" 2>/dev/null || return 1
  ! grep -qE '^State:[[:space:]]+Z' "/proc/$1/status" 2>/dev/null
}

wait_agent_done() {
  local pid="$1" t="$2" i=0
  while pid_alive "$pid"; do
    sleep 0.3; i=$((i + 1))
    [ "$i" -ge "$((t * 10 / 3))" ] && return 1
  done
  return 0
}

stop_launcher() {
  local pid="${1:-}" t="${2:-6}" i=0
  [ -n "$pid" ] || return 0
  kill -TERM "$pid" 2>/dev/null
  while pid_alive "$pid" && [ "$i" -lt "$t" ]; do sleep 0.3; i=$((i + 1)); done
  kill -KILL "$pid" 2>/dev/null
  wait "$pid" 2>/dev/null
}

# A state read (host side reads the file A's victim wrote in its rw dir)
read_a_val() {
  local key="$1"
  "$PY3" -c "
import json
try:
  d=json.load(open('$A_STATE/info.json'))
  print(d.get('$key',''))
except Exception:
  print('')"
}

# ---------------------------------------------------------------------------
# Verdict / info collection from agent logs.
#   VERDICT <Rxx.name> <PASS|FAIL|SKIP|INFO> <message>
#   INFOHASH <role> <hex>
#   INFOMODE <role> <octal>
# ---------------------------------------------------------------------------
collect_from_log() {
  local f="$1"
  [ -f "$f" ] || return 0
  local line id res msg
  while IFS= read -r line; do
    case "$line" in
      VERDICT\ *)
        set -- $line
        [ $# -ge 3 ] || continue
        id="$2"; res="$3"; msg="${line#* * * }"
        rec "$id" "$res" "$msg"
        ;;
      INFOHASH\ *)
        set -- $line
        [ $# -ge 3 ] || continue
        local role="$2" hash="$3" expected
        if [ "$role" = "A" ]; then expected="$SECRET_A"; else expected="$SECRET_B"; fi
        expected=$(sha_file "$expected")
        if [ "$hash" = "$expected" ]; then
          rec "R06.${role,,}_reads_own_secret" PASS "agent $role read own secret; hash matches"
        else
          rec "R06.${role,,}_reads_own_secret" FAIL "agent $role own-secret hash mismatch"
        fi
        ;;
      INFOMODE\ *)
        set -- $line
        [ $# -ge 3 ] || continue
        local role="$2" mode="$3"
        case "$mode" in
          400|0400) rec "R06.${role,,}_secret_mode" PASS "secret file mode is 0400" ;;
          *)        rec "R06.${role,,}_secret_mode" SKIP "secret file mode $mode (want 0400)" ;;
        esac
        ;;
    esac
  done < "$f"
}

collect_verdicts() {
  local f
  for f in "$@"; do
    collect_from_log "$f"
  done
}

sha_file() {
  "$PY3" -c "import hashlib,sys;print(hashlib.sha256(open(sys.argv[1],'rb').read()).hexdigest())" "$1"
}

# ---------------------------------------------------------------------------
# results.json writer + final summary. Requires: STARTED, BINARY, IS_FAKE,
# RESULTS_TSV, SCRATCH. Sets exit code.
# ---------------------------------------------------------------------------
write_summary() {
  local started="${1:-$(date -Is 2>/dev/null || date +%Y-%m-%dT%H:%M:%S)}"
  local ended
  ended="$(date -Is 2>/dev/null || date +%Y-%m-%dT%H:%M:%S)"
  "$PY3" - "$RESULTS_TSV" "$SCRATCH/results.json" \
         "$BINARY" "$IS_FAKE" "$started" "$ended" <<'PYEOF'
import json, sys
tsv, out, binary, fake, started, ended = sys.argv[1:]
rows = []
counts = {"PASS": 0, "FAIL": 0, "SKIP": 0, "INFO": 0, "CONTROL": 0}
for line in open(tsv):
    f = line.rstrip("\n").split("\t")
    if len(f) < 4:
        continue
    cid, res, msg, ctrl = f[0], f[1], f[2], f[3]
    if ctrl == "1":
        counts["CONTROL"] += 1
        res_out = "FAIL(CONTROL)"
    else:
        res_out = res
        counts[res] = counts.get(res, 0) + 1
    rows.append({"check": cid, "result": res_out, "message": msg})
json.dump({
    "suite": "kubeadp-agent sandbox attack suite",
    "version": "1.3",
    "binary": binary,
    "fake_stub": int(fake) == 1,
    "started": started,
    "ended": ended,
    "counts": counts,
    "results": rows,
}, open(out, "w"), indent=2)
PYEOF
  echo
  echo "=== summary ==="
  echo "hard FAILs (real leaks): $HARD_FAILS"
  echo "control FAILs (expected, fake stub): $CONTROL_FAILS"
  echo "results.json: $SCRATCH/results.json"
  if [ "$HARD_FAILS" -gt 0 ]; then
    echo "RESULT: FAIL"
    return 1
  fi
  echo "RESULT: PASS"
  return 0
}
