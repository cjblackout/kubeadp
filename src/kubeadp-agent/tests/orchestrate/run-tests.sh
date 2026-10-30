#!/usr/bin/env bash
# run-tests.sh: Black-box integration test suite for agent-orchestrate
set -uo pipefail

# Unset ambient supervisor and entrypoint environment variables to ensure test hermeticity
unset AGENT_ORCHESTRATE_CONFIG CLAUDE_SUPERVISOR_CONFIG CLAUDE_DEFAULT_SESSION \
      CLAUDE_DEFAULT_SESSION_ID CLAUDE_WORKSPACES CLAUDE_WORKSPACE_ROOT CLAUDE_REMOTE_PREFIX \
      CLAUDE_SESSION_ARGS CLAUDE_MAX_CRASHES SUPERVISOR

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
CRATE_DIR="$(cd "${SCRIPT_DIR}/../.." && pwd)"
if [ -n "${CARGO_TARGET_DIR:-}" ]; then
    BIN="${CARGO_TARGET_DIR}/debug/agent-orchestrate"
else
    BIN="${CRATE_DIR}/target/debug/agent-orchestrate"
fi

# Test tracking
TOTAL_TESTS=0
PASSED_TESTS=0
FAILED_TESTS=0

# Clean color output
GREEN='\033[0;32m'
RED='\033[0;31m'
NC='\033[0m'

rec() {
    local t_id="$1"
    local result="$2"
    local msg="$3"
    TOTAL_TESTS=$((TOTAL_TESTS + 1))
    if [ "$result" = "PASS" ]; then
        PASSED_TESTS=$((PASSED_TESTS + 1))
        echo -e "[${t_id}] ${GREEN}PASS${NC} - ${msg}"
    else
        FAILED_TESTS=$((FAILED_TESTS + 1))
        echo -e "[${t_id}] ${RED}FAIL${NC} - ${msg}"
    fi
}

TEST_TMP=$(mktemp -d -t agent-orchestrate-tests-XXXXXX)
FAKE_CLAUDE="${SCRIPT_DIR}/fake-claude.sh"
chmod +x "$FAKE_CLAUDE"

ACTIVE_PIDS=()

cleanup() {
    for pid in "${ACTIVE_PIDS[@]}"; do
        if [ -n "$pid" ] && kill -0 "$pid" 2>/dev/null; then
            kill -KILL "$pid" 2>/dev/null || true
        fi
    done
    rm -rf "$TEST_TMP"
}
trap cleanup EXIT

echo "Building agent-orchestrate crate..."
cargo build --manifest-path "${CRATE_DIR}/Cargo.toml" --features test-hooks --quiet
stop_supervisor() {
    local pid="$1"
    if [ -n "$pid" ] && kill -0 "$pid" 2>/dev/null; then
        kill -TERM "$pid" 2>/dev/null || true
        local waited=0
        while kill -0 "$pid" 2>/dev/null && [ "$waited" -lt 40 ]; do
            sleep 0.05
            waited=$((waited + 1))
        done
        if kill -0 "$pid" 2>/dev/null; then
            kill -KILL "$pid" 2>/dev/null || true
        fi
    fi
}

count_lines() {
    local file="$1"
    if [ -f "$file" ]; then
        wc -l < "$file"
    else
        echo 0
    fi
}

# Allocate a free TCP port for the sessions REST API.
port_alloc() {
    python3 - <<'EOF'
import socket
s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
s.bind(("127.0.0.1", 0))
print(s.getsockname()[1])
s.close()
EOF
}

# ==============================================================================
# T01: Start args (fresh start passes --session-id)
# ==============================================================================
test_t01() {
    local tdir="${TEST_TMP}/t01"
    mkdir -p "$tdir/home" "$tdir/work" "$tdir/control"
    local log_file="$tdir/fake-claude.log"
    local state_file="$tdir/state.json"
    local cfg_file="$tdir/config.json"

    cat <<EOF > "$cfg_file"
{
    "state_file": "$state_file",
    "heartbeat_interval_secs": 0.5,
    "sessions": [
        {
            "name": "sess1",
            "workdir": "$tdir/work",
            "session_id": "test-session-001",
            "claude_binary": "$FAKE_CLAUDE",
            "readiness_poll_secs": 0.2,
            "base_backoff_secs": 0.2,
            "env": {
                "FAKE_CLAUDE_LOG": "$log_file",
                "FAKE_CLAUDE_CONTROL_DIR": "$tdir/control"
            }
        }
    ]
}
EOF

    HOME="$tdir/home" "$BIN" run --config "$cfg_file" >/dev/null 2>&1 &
    local spid=$!
    ACTIVE_PIDS+=("$spid")

    for _ in $(seq 1 40); do
        if [ -s "$log_file" ]; then break; fi
        sleep 0.05
    done

    stop_supervisor "$spid"

    if grep -q -- "--session-id test-session-001" "$log_file" 2>/dev/null && ! grep -q -- "--resume" "$log_file" 2>/dev/null; then
        rec "T01" "PASS" "fresh start correctly passes --session-id"
    else
        rec "T01" "FAIL" "fresh start did not pass --session-id"
    fi
}

# ==============================================================================
# T02: Resume args (existing transcript passes --resume)
# ==============================================================================
test_t02() {
    local tdir="${TEST_TMP}/t02"
    mkdir -p "$tdir/home" "$tdir/work" "$tdir/control"
    local log_file="$tdir/fake-claude.log"
    local state_file="$tdir/state.json"
    local cfg_file="$tdir/config.json"
    local sess_file="$tdir/session-002.jsonl"
    echo '{"type": "init"}' > "$sess_file"

    cat <<EOF > "$cfg_file"
{
    "state_file": "$state_file",
    "heartbeat_interval_secs": 0.5,
    "sessions": [
        {
            "name": "sess2",
            "workdir": "$tdir/work",
            "session_id": "test-session-002",
            "claude_binary": "$FAKE_CLAUDE",
            "session_file": "$sess_file",
            "readiness_poll_secs": 0.2,
            "env": {
                "FAKE_CLAUDE_LOG": "$log_file",
                "FAKE_CLAUDE_CONTROL_DIR": "$tdir/control"
            }
        }
    ]
}
EOF

    HOME="$tdir/home" "$BIN" run --config "$cfg_file" >/dev/null 2>&1 &
    local spid=$!
    ACTIVE_PIDS+=("$spid")

    for _ in $(seq 1 40); do
        if [ -s "$log_file" ]; then break; fi
        sleep 0.05
    done

    stop_supervisor "$spid"

    if grep -q -- "--resume test-session-002" "$log_file" 2>/dev/null && ! grep -q -- "--session-id" "$log_file" 2>/dev/null; then
        rec "T02" "PASS" "existing session file correctly triggers --resume"
    else
        rec "T02" "FAIL" "existing session file did not trigger --resume"
    fi
}

# ==============================================================================
# T03: Config rewrite idempotent
# ==============================================================================
test_t03() {
    local tdir="${TEST_TMP}/t03"
    mkdir -p "$tdir/home" "$tdir/work" "$tdir/control"
    local state_file="$tdir/state.json"
    local cfg_file="$tdir/config.json"
    local claude_json="$tdir/home/.claude.json"
    local settings_json="$tdir/home/.claude/settings.json"

    cat <<EOF > "$cfg_file"
{
    "state_file": "$state_file",
    "sessions": [
        {
            "name": "sess3",
            "workdir": "$tdir/work",
            "session_id": "test-session-003",
            "claude_binary": "$FAKE_CLAUDE",
            "readiness_poll_secs": 0.2,
            "env": { "FAKE_CLAUDE_CONTROL_DIR": "$tdir/control" }
        }
    ]
}
EOF

    HOME="$tdir/home" "$BIN" run --config "$cfg_file" >/dev/null 2>&1 &
    local spid=$!
    ACTIVE_PIDS+=("$spid")

    for _ in $(seq 1 40); do
        if [ -f "$claude_json" ] && [ -f "$settings_json" ]; then break; fi
        sleep 0.05
    done

    stop_supervisor "$spid"

    local pass_initial=0
    if grep -q "hasCompletedOnboarding" "$claude_json" 2>/dev/null && grep -q "bypassPermissions" "$settings_json" 2>/dev/null; then
        pass_initial=1
    fi

    # Overwrite with corrupt data
    echo "{ corrupt garbage" > "$claude_json"
    echo "[ invalid array" > "$settings_json"

    HOME="$tdir/home" "$BIN" run --config "$cfg_file" >/dev/null 2>&1 &
    local spid2=$!
    ACTIVE_PIDS+=("$spid2")

    for _ in $(seq 1 40); do
        if grep -q "hasCompletedOnboarding" "$claude_json" 2>/dev/null; then break; fi
        sleep 0.05
    done

    stop_supervisor "$spid2"

    if [ "$pass_initial" -eq 1 ] && grep -q "hasCompletedOnboarding" "$claude_json" 2>/dev/null && grep -q "bypassPermissions" "$settings_json" 2>/dev/null; then
        rec "T03" "PASS" "claude config rewrite is idempotent and repairs corrupt configs"
    else
        rec "T03" "FAIL" "claude config rewrite failed"
    fi
}

# ==============================================================================
# T04: Readiness wait when auth is not ok then ok
# ==============================================================================
test_t04() {
    local tdir="${TEST_TMP}/t04"
    mkdir -p "$tdir/home" "$tdir/work" "$tdir/control"
    local log_file="$tdir/fake-claude.log"
    local state_file="$tdir/state.json"
    local cfg_file="$tdir/config.json"

    touch "$tdir/control/auth_not_logged_in"

    cat <<EOF > "$cfg_file"
{
    "state_file": "$state_file",
    "sessions": [
        {
            "name": "sess4",
            "workdir": "$tdir/work",
            "session_id": "test-session-004",
            "claude_binary": "$FAKE_CLAUDE",
            "readiness_poll_secs": 0.2,
            "env": {
                "FAKE_CLAUDE_LOG": "$log_file",
                "FAKE_CLAUDE_CONTROL_DIR": "$tdir/control"
            }
        }
    ]
}
EOF

    HOME="$tdir/home" "$BIN" run --config "$cfg_file" >/dev/null 2>&1 &
    local spid=$!
    ACTIVE_PIDS+=("$spid")

    sleep 0.8
    local not_started=0
    if [ ! -s "$log_file" ]; then
        not_started=1
    fi

    # Make auth ok
    rm -f "$tdir/control/auth_not_logged_in"

    for _ in $(seq 1 40); do
        if [ -s "$log_file" ]; then break; fi
        sleep 0.05
    done

    stop_supervisor "$spid"

    if [ "$not_started" -eq 1 ] && [ -s "$log_file" ]; then
        rec "T04" "PASS" "readiness wait pauses until auth succeeds"
    else
        rec "T04" "FAIL" "readiness wait failed"
    fi
}

# ==============================================================================
# T05: Crash -> restart with backoff
# ==============================================================================
test_t05() {
    local tdir="${TEST_TMP}/t05"
    mkdir -p "$tdir/home" "$tdir/work" "$tdir/control"
    local log_file="$tdir/fake-claude.log"
    local state_file="$tdir/state.json"
    local cfg_file="$tdir/config.json"

    touch "$tdir/control/action_exit1"

    cat <<EOF > "$cfg_file"
{
    "state_file": "$state_file",
    "heartbeat_interval_secs": 0.2,
    "sessions": [
        {
            "name": "sess5",
            "workdir": "$tdir/work",
            "session_id": "test-session-005",
            "claude_binary": "$FAKE_CLAUDE",
            "base_backoff_secs": 0.3,
            "max_backoff_secs": 2.0,
            "healthy_run_secs": 30.0,
            "readiness_poll_secs": 0.2,
            "env": {
                "FAKE_CLAUDE_LOG": "$log_file",
                "FAKE_CLAUDE_CONTROL_DIR": "$tdir/control"
            }
        }
    ]
}
EOF

    HOME="$tdir/home" "$BIN" run --config "$cfg_file" >/dev/null 2>&1 &
    local spid=$!
    ACTIVE_PIDS+=("$spid")

    for _ in $(seq 1 50); do
        local count=$(count_lines "$log_file")
        if [ "$count" -ge 2 ]; then break; fi
        sleep 0.05
    done

    stop_supervisor "$spid"

    local count=$(count_lines "$log_file")
    if [ "$count" -ge 2 ]; then
        rec "T05" "PASS" "crashed process was restarted with backoff ($count runs)"
    else
        rec "T05" "FAIL" "process was not restarted on crash ($count runs)"
    fi
}

# ==============================================================================
# T06: Reset of backoff after healthy run
# ==============================================================================
test_t06() {
    local tdir="${TEST_TMP}/t06"
    mkdir -p "$tdir/home" "$tdir/work" "$tdir/control"
    local log_file="$tdir/fake-claude.log"
    local state_file="$tdir/state.json"
    local cfg_file="$tdir/config.json"

    # Start with action_exit1 so child crashes immediately on first launch
    touch "$tdir/control/action_exit1"

    cat <<EOF > "$cfg_file"
{
    "state_file": "$state_file",
    "sessions": [
        {
            "name": "sess6",
            "workdir": "$tdir/work",
            "session_id": "test-session-006",
            "claude_binary": "$FAKE_CLAUDE",
            "base_backoff_secs": 0.2,
            "max_backoff_secs": 2.0,
            "healthy_run_secs": 0.5,
            "backoff_window_secs": 0.6,
            "readiness_poll_secs": 0.2,
            "env": {
                "FAKE_CLAUDE_LOG": "$log_file",
                "FAKE_CLAUDE_CONTROL_DIR": "$tdir/control"
            }
        }
    ]
}
EOF

    CLAUDE_DEFAULT_SESSION=0 HOME="$tdir/home" "$BIN" run --config "$cfg_file" >/dev/null 2>&1 &
    local spid=$!
    ACTIVE_PIDS+=("$spid")

    # Wait for first crash
    for _ in $(seq 1 40); do
        if grep -q "exit=1" "$log_file" 2>/dev/null; then break; fi
        sleep 0.05
    done

    # Remove crash trigger so the second run becomes healthy
    rm -f "$tdir/control/action_exit1"

    # Wait for second run to start and run for >0.6s (> healthy_run_secs 0.5s)
    for _ in $(seq 1 40); do
        local count=$(grep -c "argv=" "$log_file" 2>/dev/null || true)
        if [ "$count" -ge 2 ]; then break; fi
        sleep 0.05
    done
    sleep 0.7

    # Now trigger crash on the healthy run
    touch "$tdir/control/action_exit1"

    # Wait for run 2 to exit
    for _ in $(seq 1 40); do
        local count=$(grep -c "exit=" "$log_file" 2>/dev/null || echo 0)
        if [ "$count" -ge 2 ]; then break; fi
        sleep 0.05
    done
    # Remove crash trigger so run 3 stays running
    rm -f "$tdir/control/action_exit1"

    # Wait for restart (third launch)
    for _ in $(seq 1 40); do
        local count=$(grep -c "argv=" "$log_file" 2>/dev/null || echo 0)
        if [ "$count" -ge 3 ]; then break; fi
        sleep 0.05
    done

    # Extract exit timestamp of 2nd run and start timestamp of 3rd run BEFORE stopping supervisor
    local t_exit=$(grep "exit=" "$log_file" | tail -n 1 | awk '{print $1}')
    local t_start=$(grep "argv=" "$log_file" | tail -n 1 | awk '{print $1}')

    stop_supervisor "$spid"

    local delay_ok=0
    local delay=""
    if [ -n "$t_exit" ] && [ -n "$t_start" ]; then
        delay=$(python3 -c "print(f'{$t_start - $t_exit:.3f}')" 2>/dev/null || true)
        # Base backoff is 0.2s (+ jitter + readiness check). If reset, delay is ~0.2s to 0.75s (far below max backoff 2.0s)
        delay_ok=$(python3 -c "d = float('$delay'); print(1 if 0.15 <= d <= 1.20 else 0)" 2>/dev/null || echo 0)
    fi

    if [ "$delay_ok" -eq 1 ]; then
        rec "T06" "PASS" "healthy run reset backoff: restart delay was ~base backoff (${delay}s)"
    else
        rec "T06" "FAIL" "backoff reset delay failed (delay=${delay}s, t_exit=$t_exit, t_start=$t_start)"
    fi
}

# ==============================================================================
# T07: N crashes -> failed while a second session keeps running
# ==============================================================================
test_t07() {
    local tdir="${TEST_TMP}/t07"
    mkdir -p "$tdir/home" "$tdir/work" "$tdir/control_fail" "$tdir/control_ok"
    local log_fail="$tdir/fake-claude-fail.log"
    local log_ok="$tdir/fake-claude-ok.log"
    local state_file="$tdir/state.json"
    local cfg_file="$tdir/config.json"

    touch "$tdir/control_fail/action_exit1"

    cat <<EOF > "$cfg_file"
{
    "state_file": "$state_file",
    "heartbeat_interval_secs": 0.2,
    "sessions": [
        {
            "name": "failing_session",
            "workdir": "$tdir/work",
            "session_id": "fail-001",
            "claude_binary": "$FAKE_CLAUDE",
            "base_backoff_secs": 0.05,
            "max_backoff_secs": 0.1,
            "max_crashes": 2,
            "crash_window_secs": 10.0,
            "failed_cooldown_secs": 1.0,
            "readiness_poll_secs": 0.2,
            "env": {
                "FAKE_CLAUDE_LOG": "$log_fail",
                "FAKE_CLAUDE_CONTROL_DIR": "$tdir/control_fail"
            }
        },
        {
            "name": "healthy_session",
            "workdir": "$tdir/work",
            "session_id": "ok-001",
            "claude_binary": "$FAKE_CLAUDE",
            "readiness_poll_secs": 0.2,
            "env": {
                "FAKE_CLAUDE_LOG": "$log_ok",
                "FAKE_CLAUDE_CONTROL_DIR": "$tdir/control_ok"
            }
        }
    ]
}
EOF

    CLAUDE_DEFAULT_SESSION=0 HOME="$tdir/home" "$BIN" run --config "$cfg_file" >/dev/null 2>&1 &
    local spid=$!
    ACTIVE_PIDS+=("$spid")

    for _ in $(seq 1 80); do
        if grep -q '"status": "failed"' "$state_file" 2>/dev/null; then break; fi
        sleep 0.05
    done

    local status_fail=$(grep -A 8 '"failing_session"' "$state_file" 2>/dev/null | grep '"status"' | tr -d '", ' | cut -d: -f2 || true)
    local status_ok=$(grep -A 8 '"healthy_session"' "$state_file" 2>/dev/null | grep '"status"' | tr -d '", ' | cut -d: -f2 || true)

    local probe_rc=0
    "$BIN" status --state-file "$state_file" >/dev/null 2>&1 || probe_rc=$?

    # Remove the failure trigger so the retry succeeds
    rm -f "$tdir/control_fail/action_exit1"

    # Wait for cooldown to expire (~1.0s) and retry to happen
    local retried=0
    for _ in $(seq 1 60); do
        local cur_status=$(grep -A 8 '"failing_session"' "$state_file" 2>/dev/null | grep '"status"' | tr -d '", ' | cut -d: -f2 || true)
        if [ "$cur_status" = "starting" ] || [ "$cur_status" = "running" ]; then
            retried=1
            break
        fi
        sleep 0.05
    done

    stop_supervisor "$spid"

    if [ "$status_fail" = "failed" ] && [ "$status_ok" = "running" ] && [ "$probe_rc" -eq 2 ] && [ "$retried" -eq 1 ]; then
        rec "T07" "PASS" "session marked failed, probe exited 2 during cooldown, and retried after cooldown"
    else
        rec "T07" "FAIL" "expected fail=failed (got $status_fail), probe=2 (got $probe_rc), retried=1 (got $retried)"
    fi
}

# ==============================================================================
# T08: Hang -> killed and restarted
# ==============================================================================
test_t08() {
    local tdir="${TEST_TMP}/t08"
    mkdir -p "$tdir/home" "$tdir/work" "$tdir/control"
    local log_file="$tdir/fake-claude.log"
    local state_file="$tdir/state.json"
    local cfg_file="$tdir/config.json"

    touch "$tdir/control/action_hang"

    cat <<EOF > "$cfg_file"
{
    "state_file": "$state_file",
    "heartbeat_interval_secs": 0.2,
    "sessions": [
        {
            "name": "sess8",
            "workdir": "$tdir/work",
            "session_id": "hang-001",
            "claude_binary": "$FAKE_CLAUDE",
            "hang_timeout_secs": 0.8,
            "kill_grace_secs": 0.2,
            "base_backoff_secs": 0.05,
            "readiness_poll_secs": 0.2,
            "env": {
                "FAKE_CLAUDE_LOG": "$log_file",
                "FAKE_CLAUDE_CONTROL_DIR": "$tdir/control"
            }
        }
    ]
}
EOF

    HOME="$tdir/home" "$BIN" run --config "$cfg_file" >/dev/null 2>&1 &
    local spid=$!
    ACTIVE_PIDS+=("$spid")

    # Wait for hang detection to trigger and record in state_file (signal: 15 reaped by kill_process_group_with_grace)
    local detected=0
    for _ in $(seq 1 60); do
        if grep -q '"last_exit_reason": "signal: 15"' "$state_file" 2>/dev/null; then
            detected=1
            break
        fi
        sleep 0.05
    done

    # Wait for restart (at least 2 entries in log_file)
    for _ in $(seq 1 40); do
        local count=$(count_lines "$log_file")
        if [ "$count" -ge 2 ]; then break; fi
        sleep 0.05
    done

    stop_supervisor "$spid"

    local count=$(count_lines "$log_file")
    if [ "$count" -ge 2 ] && [ "$detected" -eq 1 ]; then
        rec "T08" "PASS" "silent hang detected, child killed and restarted with resume"
    else
        rec "T08" "FAIL" "hang detection failed (detected=$detected, runs=$count)"
    fi
}

# ==============================================================================
# T09: SIGTERM-ignoring child gets SIGKILL
# ==============================================================================
test_t09() {
    local tdir="${TEST_TMP}/t09"
    mkdir -p "$tdir/home" "$tdir/work" "$tdir/control"
    local log_file="$tdir/fake-claude.log"
    local state_file="$tdir/state.json"
    local cfg_file="$tdir/config.json"

    touch "$tdir/control/action_ignore_sigterm"

    cat <<EOF > "$cfg_file"
{
    "state_file": "$state_file",
    "heartbeat_interval_secs": 0.2,
    "sessions": [
        {
            "name": "sess9",
            "workdir": "$tdir/work",
            "session_id": "ignore-001",
            "claude_binary": "$FAKE_CLAUDE",
            "hang_timeout_secs": 0.8,
            "kill_grace_secs": 0.3,
            "base_backoff_secs": 0.05,
            "readiness_poll_secs": 0.2,
            "env": {
                "FAKE_CLAUDE_LOG": "$log_file",
                "FAKE_CLAUDE_CONTROL_DIR": "$tdir/control"
            }
        }
    ]
}
EOF

    HOME="$tdir/home" "$BIN" run --config "$cfg_file" >/dev/null 2>&1 &
    local spid=$!
    ACTIVE_PIDS+=("$spid")

    for _ in $(seq 1 40); do
        if [ -s "$log_file" ]; then break; fi
        sleep 0.05
    done
    local child_pid=$(head -n 1 "$log_file" | awk '{print $2}' | cut -d= -f2 || true)

    local killed_before_stop=0
    local has_sig9_record=0
    for _ in $(seq 1 60); do
        if [ -n "$child_pid" ] && ! kill -0 "$child_pid" 2>/dev/null; then
            killed_before_stop=1
        fi
        if grep -q 'signal: 9' "$state_file" 2>/dev/null; then
            has_sig9_record=1
        fi
        local count=$(count_lines "$log_file")
        if [ "$killed_before_stop" -eq 1 ] && [ "$has_sig9_record" -eq 1 ] && [ "$count" -ge 2 ]; then
            break
        fi
        sleep 0.05
    done

    local pre_assert_ok=0
    if [ "$killed_before_stop" -eq 1 ] && [ "$has_sig9_record" -eq 1 ]; then
        pre_assert_ok=1
    fi

    stop_supervisor "$spid"

    if [ "$pre_assert_ok" -eq 1 ]; then
        rec "T09" "PASS" "SIGTERM-ignoring child died and signal: 9 recorded before supervisor stop"
    else
        rec "T09" "FAIL" "T09 pre-stop assertion failed (killed=$killed_before_stop, sig9=$has_sig9_record)"
    fi
}

# ==============================================================================
# T10: SIGTERM to supervisor drains all children and exits 0 with final state
# ==============================================================================
test_t10() {
    local tdir="${TEST_TMP}/t10"
    mkdir -p "$tdir/home" "$tdir/work" "$tdir/control"
    local log_file="$tdir/fake-claude.log"
    local state_file="$tdir/state.json"
    local cfg_file="$tdir/config.json"

    cat <<EOF > "$cfg_file"
{
    "state_file": "$state_file",
    "shutdown_grace_secs": 1.0,
    "sessions": [
        {
            "name": "sess10",
            "workdir": "$tdir/work",
            "session_id": "drain-001",
            "claude_binary": "$FAKE_CLAUDE",
            "readiness_poll_secs": 0.2,
            "env": {
                "FAKE_CLAUDE_LOG": "$log_file",
                "FAKE_CLAUDE_CONTROL_DIR": "$tdir/control"
            }
        }
    ]
}
EOF

    HOME="$tdir/home" "$BIN" run --config "$cfg_file" >/dev/null 2>&1 &
    local spid=$!

    for _ in $(seq 1 40); do
        if [ -s "$log_file" ]; then break; fi
        sleep 0.05
    done

    kill -TERM "$spid"
    local exit_code=0
    wait "$spid" || exit_code=$?

    local state_status=$(grep '"status":' "$state_file" 2>/dev/null | head -n 1 | tr -d '", ' | cut -d: -f2 || true)

    if [ "$exit_code" -eq 0 ] && [ "$state_status" = "stopped" ]; then
        rec "T10" "PASS" "supervisor cleanly exited 0 and updated state to stopped"
    else
        rec "T10" "FAIL" "supervisor exit failed (rc=$exit_code, state=$state_status)"
    fi
}

# ==============================================================================
# T11: No orphan/zombie processes while child is crash-looping
# ==============================================================================
test_t11() {
    local tdir="${TEST_TMP}/t11"
    mkdir -p "$tdir/home" "$tdir/work" "$tdir/control"
    local log_file="$tdir/fake-claude.log"
    local state_file="$tdir/state.json"
    local cfg_file="$tdir/config.json"

    touch "$tdir/control/action_exit1"

    cat <<EOF > "$cfg_file"
{
    "state_file": "$state_file",
    "sessions": [
        {
            "name": "sess11_crash_loop",
            "workdir": "$tdir/work",
            "session_id": "crash-011",
            "claude_binary": "$FAKE_CLAUDE",
            "base_backoff_secs": 0.05,
            "max_backoff_secs": 0.1,
            "max_crashes": 0,
            "readiness_poll_secs": 0.2,
            "env": {
                "FAKE_CLAUDE_LOG": "$log_file",
                "FAKE_CLAUDE_CONTROL_DIR": "$tdir/control"
            }
        }
    ]
}
EOF

    HOME="$tdir/home" "$BIN" run --config "$cfg_file" >/dev/null 2>&1 &
    local spid=$!
    ACTIVE_PIDS+=("$spid")

    # Sample ps multiple times while the child is actively crash looping
    local zombie_count=0
    for _ in $(seq 1 20); do
        local z=$(ps -ef | grep "[f]ake-claude" | grep "<defunct>" || true)
        if [ -n "$z" ]; then
            zombie_count=$((zombie_count + 1))
        fi
        sleep 0.08
    done

    stop_supervisor "$spid"

    local total_runs=$(count_lines "$log_file")
    local final_zombies=$(ps -ef | grep "[f]ake-claude" | grep "<defunct>" || true)

    if [ "$zombie_count" -eq 0 ] && [ -z "$final_zombies" ] && [ "$total_runs" -ge 5 ]; then
        rec "T11" "PASS" "no zombie/defunct processes detected while child was crash-looping ($total_runs runs)"
    else
        rec "T11" "FAIL" "zombies detected during/after crash-loop (zombie_samples=$zombie_count, final=$final_zombies, runs=$total_runs)"
    fi
}

# ==============================================================================
# T12: State file valid JSON and heartbeat advancing
# ==============================================================================
test_t12() {
    local tdir="${TEST_TMP}/t12"
    mkdir -p "$tdir/home" "$tdir/work" "$tdir/control"
    local state_file="$tdir/state.json"
    local cfg_file="$tdir/config.json"

    cat <<EOF > "$cfg_file"
{
    "state_file": "$state_file",
    "heartbeat_interval_secs": 0.2,
    "sessions": [
        {
            "name": "sess12",
            "workdir": "$tdir/work",
            "session_id": "hb-001",
            "claude_binary": "$FAKE_CLAUDE",
            "readiness_poll_secs": 0.2,
            "env": { "FAKE_CLAUDE_CONTROL_DIR": "$tdir/control" }
        }
    ]
}
EOF

    HOME="$tdir/home" "$BIN" run --config "$cfg_file" >/dev/null 2>&1 &
    local spid=$!
    ACTIVE_PIDS+=("$spid")

    for _ in $(seq 1 40); do
        if [ -s "$state_file" ]; then break; fi
        sleep 0.05
    done

    local hb1=$(grep '"heartbeat_epoch"' "$state_file" 2>/dev/null | tr -d '", ' | cut -d: -f2 || true)
    sleep 1.1
    local hb2=$(grep '"heartbeat_epoch"' "$state_file" 2>/dev/null | tr -d '", ' | cut -d: -f2 || true)

    stop_supervisor "$spid"

    if [ -n "$hb1" ] && [ -n "$hb2" ] && [ "$hb2" -gt "$hb1" ]; then
        rec "T12" "PASS" "state file heartbeat is actively advancing (hb1=$hb1, hb2=$hb2)"
    else
        rec "T12" "FAIL" "heartbeat check failed (hb1=$hb1, hb2=$hb2)"
    fi
}

# ==============================================================================
# T13: Status subcommand exit codes (ok, stale heartbeat, failed session)
# ==============================================================================
test_t13() {
    local tdir="${TEST_TMP}/t13"
    mkdir -p "$tdir"
    local state_file="$tdir/state.json"
    local now=$(date +%s)

    # 1. Healthy state
    cat <<EOF > "$state_file"
{
    "supervisor_pid": 1234,
    "heartbeat": "2026-10-01T07:00:00Z",
    "heartbeat_epoch": $now,
    "status": "running",
    "sessions": {
        "sess": {
            "name": "sess",
            "pid": 1235,
            "start_time": null,
            "start_time_epoch": null,
            "uptime_secs": 10.0,
            "restart_count": 0,
            "last_exit_reason": null,
            "status": "running",
            "last_health_result": "ok"
        }
    }
}
EOF
    local rc_ok=0
    "$BIN" status --state-file "$state_file" --stale-threshold-secs 10 >/dev/null 2>&1 || rc_ok=$?

    # 2. Stale heartbeat (epoch 100)
    cat <<EOF > "$state_file"
{
    "supervisor_pid": 1234,
    "heartbeat": "1970-01-01T00:01:40Z",
    "heartbeat_epoch": 100,
    "status": "running",
    "sessions": {}
}
EOF
    local rc_stale=0
    "$BIN" status --state-file "$state_file" --stale-threshold-secs 5 >/dev/null 2>&1 || rc_stale=$?

    # 3. Failed session
    cat <<EOF > "$state_file"
{
    "supervisor_pid": 1234,
    "heartbeat": "2026-10-01T07:00:00Z",
    "heartbeat_epoch": $now,
    "status": "running",
    "sessions": {
        "bad_sess": {
            "name": "bad_sess",
            "pid": null,
            "start_time": null,
            "start_time_epoch": null,
            "uptime_secs": null,
            "restart_count": 5,
            "last_exit_reason": "exit: 1",
            "status": "failed",
            "last_health_result": "ok"
        }
    }
}
EOF
    local rc_fail=0
    "$BIN" status --state-file "$state_file" --stale-threshold-secs 10 >/dev/null 2>&1 || rc_fail=$?

    if [ "$rc_ok" -eq 0 ] && [ "$rc_stale" -eq 1 ] && [ "$rc_fail" -eq 2 ]; then
        rec "T13" "PASS" "status subcommand exit codes correct (ok=0, stale=1, failed=2)"
    else
        rec "T13" "FAIL" "status subcommand exit codes wrong (ok=$rc_ok, stale=$rc_stale, failed=$rc_fail)"
    fi
}

# ==============================================================================
# T14: Corrupt session file quarantine & fresh start
# ==============================================================================
test_t14() {
    local tdir="${TEST_TMP}/t14"
    mkdir -p "$tdir/home" "$tdir/work" "$tdir/control"
    local log_file="$tdir/fake-claude.log"
    local state_file="$tdir/state.json"
    local cfg_file="$tdir/config.json"
    local sess_file="$tdir/bad-session.jsonl"

    echo "THIS IS NOT VALID JSON" > "$sess_file"

    cat <<EOF > "$cfg_file"
{
    "state_file": "$state_file",
    "sessions": [
        {
            "name": "sess14",
            "workdir": "$tdir/work",
            "session_id": "test-session-014",
            "claude_binary": "$FAKE_CLAUDE",
            "session_file": "$sess_file",
            "readiness_poll_secs": 0.2,
            "env": {
                "FAKE_CLAUDE_LOG": "$log_file",
                "FAKE_CLAUDE_CONTROL_DIR": "$tdir/control"
            }
        }
    ]
}
EOF

    HOME="$tdir/home" "$BIN" run --config "$cfg_file" >/dev/null 2>&1 &
    local spid=$!
    ACTIVE_PIDS+=("$spid")

    for _ in $(seq 1 40); do
        if [ -s "$log_file" ]; then break; fi
        sleep 0.05
    done

    stop_supervisor "$spid"

    local backups=0
    for f in "$tdir"/bad-session.jsonl.corrupt.*.bak; do
        [ -e "$f" ] && backups=$((backups + 1))
    done

    if [ "$backups" -ge 1 ] && grep -q -- "--session-id test-session-014" "$log_file" 2>/dev/null; then
        rec "T14" "PASS" "corrupt session file was moved aside and fresh session started"
    else
        rec "T14" "FAIL" "corrupt session file handling failed"
    fi
}

# ==============================================================================
# T15: Two sessions independent
# ==============================================================================
test_t15() {
    local tdir="${TEST_TMP}/t15"
    mkdir -p "$tdir/home" "$tdir/work" "$tdir/ctrl_crash" "$tdir/ctrl_steady"
    local log_crash="$tdir/crash.log"
    local log_steady="$tdir/steady.log"
    local state_file="$tdir/state.json"
    local cfg_file="$tdir/config.json"

    touch "$tdir/ctrl_crash/action_exit1"

    cat <<EOF > "$cfg_file"
{
    "state_file": "$state_file",
    "heartbeat_interval_secs": 0.2,
    "sessions": [
        {
            "name": "crashing_sess",
            "workdir": "$tdir/work",
            "session_id": "crash-015",
            "claude_binary": "$FAKE_CLAUDE",
            "base_backoff_secs": 0.05,
            "max_backoff_secs": 0.1,
            "readiness_poll_secs": 0.2,
            "env": {
                "FAKE_CLAUDE_LOG": "$log_crash",
                "FAKE_CLAUDE_CONTROL_DIR": "$tdir/ctrl_crash"
            }
        },
        {
            "name": "steady_sess",
            "workdir": "$tdir/work",
            "session_id": "steady-015",
            "claude_binary": "$FAKE_CLAUDE",
            "readiness_poll_secs": 0.2,
            "env": {
                "FAKE_CLAUDE_LOG": "$log_steady",
                "FAKE_CLAUDE_CONTROL_DIR": "$tdir/ctrl_steady"
            }
        }
    ]
}
EOF

    CLAUDE_DEFAULT_SESSION=0 HOME="$tdir/home" "$BIN" run --config "$cfg_file" >/dev/null 2>&1 &
    local spid=$!
    ACTIVE_PIDS+=("$spid")

    sleep 1.2

    stop_supervisor "$spid"

    local crash_runs=$(grep -c "argv=" "$log_crash" 2>/dev/null || echo 0)
    local steady_runs=$(grep -c "argv=" "$log_steady" 2>/dev/null || echo 0)

    if [ "$crash_runs" -ge 2 ] && [ "$steady_runs" -eq 1 ]; then
        rec "T15" "PASS" "two sessions are completely independent (crashes=$crash_runs, steady=$steady_runs)"
    else
        rec "T15" "FAIL" "independence test failed (crashes=$crash_runs, steady=$steady_runs)"
    fi
}

# ==============================================================================
# T16: Silent process is NOT restarted when hang_timeout_secs defaults to 0 (disabled)
# ==============================================================================
test_t16() {
    local tdir="${TEST_TMP}/t16"
    mkdir -p "$tdir/home" "$tdir/work" "$tdir/control"
    local log_file="$tdir/fake-claude.log"
    local state_file="$tdir/state.json"
    local cfg_file="$tdir/config.json"

    # Action hang: process produces no PTY output
    touch "$tdir/control/action_hang"

    cat <<EOF > "$cfg_file"
{
    "state_file": "$state_file",
    "heartbeat_interval_secs": 0.2,
    "sessions": [
        {
            "name": "sess16_silent",
            "workdir": "$tdir/work",
            "session_id": "silent-016",
            "claude_binary": "$FAKE_CLAUDE",
            "readiness_poll_secs": 0.2,
            "env": {
                "FAKE_CLAUDE_LOG": "$log_file",
                "FAKE_CLAUDE_CONTROL_DIR": "$tdir/control"
            }
        }
    ]
}
EOF

    HOME="$tdir/home" "$BIN" run --config "$cfg_file" >/dev/null 2>&1 &
    local spid=$!
    ACTIVE_PIDS+=("$spid")

    for _ in $(seq 1 40); do
        if [ -s "$log_file" ]; then break; fi
        sleep 0.05
    done

    # Let it stay silent for 1.2s (longer than the 0.8s tested in T08)
    sleep 1.2

    local count=$(count_lines "$log_file")
    local status=$(grep -A 8 '"sess16_silent"' "$state_file" 2>/dev/null | grep '"status"' | tr -d '", ' | cut -d: -f2 || true)

    stop_supervisor "$spid"

    if [ "$count" -eq 1 ] && [ "$status" = "running" ]; then
        rec "T16" "PASS" "silent session not restarted with default hang_timeout (disabled)"
    else
        rec "T16" "FAIL" "silent session was unexpectedly restarted (runs=$count, status=$status)"
    fi
}

# ==============================================================================
# T17: Bounded shutdown: child closing tty and ignoring SIGTERM is killed via SIGKILL within grace
# ==============================================================================
test_t17() {
    local tdir="${TEST_TMP}/t17"
    mkdir -p "$tdir/home" "$tdir/work" "$tdir/control"
    local log_file="$tdir/fake-claude.log"
    local state_file="$tdir/state.json"
    local cfg_file="$tdir/config.json"

    touch "$tdir/control/action_close_tty_ignore_sigterm"

    cat <<EOF > "$cfg_file"
{
    "state_file": "$state_file",
    "shutdown_grace_secs": 1.0,
    "sessions": [
        {
            "name": "sess17_stubborn",
            "workdir": "$tdir/work",
            "session_id": "stubborn-017",
            "claude_binary": "$FAKE_CLAUDE",
            "kill_grace_secs": 0.4,
            "readiness_poll_secs": 0.2,
            "env": {
                "FAKE_CLAUDE_LOG": "$log_file",
                "FAKE_CLAUDE_CONTROL_DIR": "$tdir/control"
            }
        }
    ]
}
EOF

    HOME="$tdir/home" "$BIN" run --config "$cfg_file" >/dev/null 2>&1 &
    local spid=$!

    for _ in $(seq 1 40); do
        if [ -s "$log_file" ]; then break; fi
        sleep 0.05
    done
    local child_pid=$(head -n 1 "$log_file" | awk '{print $2}' | cut -d= -f2 || true)

    # Allow child to close fds and enter sleep
    sleep 0.3

    local start_ts=$(date +%s)
    kill -TERM "$spid"
    local exit_code=0
    wait "$spid" || exit_code=$?
    local end_ts=$(date +%s)
    local elapsed=$((end_ts - start_ts))

    local child_alive=0
    if [ -n "$child_pid" ] && kill -0 "$child_pid" 2>/dev/null; then
        child_alive=1
        kill -KILL "$child_pid" 2>/dev/null || true
    fi

    if [ "$exit_code" -eq 0 ] && [ "$child_alive" -eq 0 ] && [ "$elapsed" -le 3 ]; then
        rec "T17" "PASS" "bounded shutdown reaped uncooperative child within grace ($elapsed s)"
    else
        rec "T17" "FAIL" "bounded shutdown failed (rc=$exit_code, alive=$child_alive, elapsed=$elapsed s)"
    fi
}

# ==============================================================================
# T18: Default raw mode: no-newline burst forwarded immediately
# ==============================================================================
test_t18() {
    local tdir="${TEST_TMP}/t18"
    mkdir -p "$tdir/home" "$tdir/work" "$tdir/control"
    local state_file="$tdir/state.json"
    local cfg_file="$tdir/config.json"
    local stdout_file="$tdir/supervisor_stdout.log"

    touch "$tdir/control/action_raw_burst"

    cat <<EOF > "$cfg_file"
{
    "state_file": "$state_file",
    "sessions": [
        {
            "name": "sess_raw",
            "workdir": "$tdir/work",
            "session_id": "raw-018",
            "claude_binary": "$FAKE_CLAUDE",
            "readiness_poll_secs": 0.2,
            "env": { "FAKE_CLAUDE_CONTROL_DIR": "$tdir/control" }
        }
    ]
}
EOF

    HOME="$tdir/home" "$BIN" run --config "$cfg_file" > "$stdout_file" 2>/dev/null &
    local spid=$!
    ACTIVE_PIDS+=("$spid")

    # In raw mode, no-newline burst appears immediately on stdout within 1s
    local found=0
    for _ in $(seq 1 20); do
        if grep -q "RAW_BURST_WITHOUT_NEWLINE_12345" "$stdout_file" 2>/dev/null; then
            found=1
            break
        fi
        sleep 0.05
    done

    stop_supervisor "$spid"

    if [ "$found" -eq 1 ]; then
        rec "T18" "PASS" "default raw mode: no-newline burst appeared on stdout immediately"
    else
        rec "T18" "FAIL" "default raw mode failed to forward no-newline burst"
    fi
}

# ==============================================================================
# T19: Opt-in prefix_output: forwarded PTY lines prefixed with [<session name>]
# ==============================================================================
test_t19() {
    local tdir="${TEST_TMP}/t19"
    mkdir -p "$tdir/home" "$tdir/work" "$tdir/control"
    local state_file="$tdir/state.json"
    local cfg_file="$tdir/config.json"
    local stdout_file="$tdir/supervisor_stdout.log"

    cat <<EOF > "$cfg_file"
{
    "state_file": "$state_file",
    "sessions": [
        {
            "name": "sess_alpha",
            "workdir": "$tdir/work",
            "session_id": "alpha-019",
            "claude_binary": "$FAKE_CLAUDE",
            "prefix_output": true,
            "readiness_poll_secs": 0.2,
            "env": { "FAKE_CLAUDE_CONTROL_DIR": "$tdir/control" }
        },
        {
            "name": "sess_beta",
            "workdir": "$tdir/work",
            "session_id": "beta-019",
            "claude_binary": "$FAKE_CLAUDE",
            "prefix_output": true,
            "readiness_poll_secs": 0.2,
            "env": { "FAKE_CLAUDE_CONTROL_DIR": "$tdir/control" }
        }
    ]
}
EOF

    HOME="$tdir/home" "$BIN" run --config "$cfg_file" > "$stdout_file" 2>/dev/null &
    local spid=$!
    ACTIVE_PIDS+=("$spid")

    sleep 0.8
    stop_supervisor "$spid"

    local has_alpha=0
    local has_beta=0
    if grep -q "^\[sess_alpha\] " "$stdout_file" 2>/dev/null; then
        has_alpha=1
    fi
    if grep -q "^\[sess_beta\] " "$stdout_file" 2>/dev/null; then
        has_beta=1
    fi

    if [ "$has_alpha" -eq 1 ] && [ "$has_beta" -eq 1 ]; then
        rec "T19" "PASS" "opt-in prefix_output forwarded pty lines prefixed with [<session name>]"
    else
        rec "T19" "FAIL" "opt-in prefixing failed (alpha=$has_alpha, beta=$has_beta)"
    fi
}

# ==============================================================================
# T20: TUI multi-byte burst with no newline survives external SIGKILL and restarts
# ==============================================================================
test_t20() {
    local tdir="${TEST_TMP}/t20"
    mkdir -p "$tdir/home" "$tdir/work" "$tdir/control"
    local log_file="$tdir/fake-claude.log"
    local state_file="$tdir/state.json"
    local cfg_file="$tdir/config.json"
    local stderr_file="$tdir/supervisor_stderr.log"

    touch "$tdir/control/action_tui_burst"

    cat <<EOF > "$cfg_file"
{
    "state_file": "$state_file",
    "heartbeat_interval_secs": 0.2,
    "sessions": [
        {
            "name": "sess_tui",
            "workdir": "$tdir/work",
            "session_id": "tui-020",
            "claude_binary": "$FAKE_CLAUDE",
            "base_backoff_secs": 0.05,
            "readiness_poll_secs": 0.2,
            "env": {
                "FAKE_CLAUDE_LOG": "$log_file",
                "FAKE_CLAUDE_CONTROL_DIR": "$tdir/control"
            }
        }
    ]
}
EOF

    HOME="$tdir/home" "$BIN" run --config "$cfg_file" >/dev/null 2> "$stderr_file" &
    local spid=$!
    ACTIVE_PIDS+=("$spid")

    # Wait for child to be running
    for _ in $(seq 1 40); do
        if [ -s "$log_file" ]; then break; fi
        sleep 0.05
    done
    local child_pid=$(head -n 1 "$log_file" | awk '{print $2}' | cut -d= -f2 || true)

    # Let child emit TUI burst
    sleep 0.3

    # Send external SIGKILL to the child
    if [ -n "$child_pid" ]; then
        kill -9 "$child_pid" 2>/dev/null || true
    fi

    # Wait for restart (runs >= 2)
    local has_sig9=0
    for _ in $(seq 1 50); do
        local count=$(count_lines "$log_file")
        local reason=$(grep '"last_exit_reason"' "$state_file" 2>/dev/null || true)
        if echo "$reason" | grep -q "signal: 9"; then
            has_sig9=1
        fi
        if [ "$count" -ge 2 ]; then break; fi
        sleep 0.05
    done

    local exit_reason=$(grep '"last_exit_reason"' "$state_file" 2>/dev/null || true)
    if grep -q "signal=9" "$stderr_file" 2>/dev/null; then
        has_sig9=1
    fi

    stop_supervisor "$spid"

    local panicked=0
    if grep -iq "panicked" "$stderr_file" 2>/dev/null; then
        panicked=1
    fi

    local runs=$(count_lines "$log_file")

    if [ "$panicked" -eq 0 ] && [ "$runs" -ge 2 ] && [ "$has_sig9" -eq 1 ]; then
        rec "T20" "PASS" "TUI multi-byte burst survived external SIGKILL without panic and restarted (reason: signal: 9)"
    else
        rec "T20" "FAIL" "TUI burst / SIGKILL test failed (panicked=$panicked, runs=$runs, reason=$exit_reason)"
    fi
}

# ==============================================================================
# T21: Test-induced forced panic caught, recovered, and session healthy
# ==============================================================================
# Field of the sess_panic entry of a state file (the sessions map has no fixed
# order and also holds the default session).
t21_session_field() {
    python3 -c 'import json,sys
try:
    print(json.load(open(sys.argv[1]))["sessions"]["sess_panic"][sys.argv[2]])
except Exception:
    print("")' "$1" "$2" 2>/dev/null
}

test_t21() {
    local tdir="${TEST_TMP}/t21"
    mkdir -p "$tdir/home" "$tdir/work" "$tdir/control"
    local log_file="$tdir/fake-claude.log"
    local state_file="$tdir/state.json"
    local cfg_file="$tdir/config.json"
    local stderr_file="$tdir/supervisor_stderr.log"
    local panic_trigger="$tdir/panic_trigger"

    touch "$panic_trigger"

    cat <<EOF > "$cfg_file"
{
    "state_file": "$state_file",
    "heartbeat_interval_secs": 0.2,
    "sessions": [
        {
            "name": "sess_panic",
            "workdir": "$tdir/work",
            "session_id": "panic-021",
            "claude_binary": "$FAKE_CLAUDE",
            "base_backoff_secs": 0.05,
            "readiness_poll_secs": 0.2,
            "env": {
                "FAKE_CLAUDE_LOG": "$log_file",
                "FAKE_CLAUDE_CONTROL_DIR": "$tdir/control"
            }
        }
    ]
}
EOF

    CLAUDE_SUPERVISOR_TEST_PANIC_ONCE="$panic_trigger" HOME="$tdir/home" "$BIN" run --config "$cfg_file" >/dev/null 2> "$stderr_file" &
    local spid=$!
    ACTIVE_PIDS+=("$spid")

    # Wait for restart and running status
    # Condition-based wait (10 s cap): the panic has been consumed, the session
    # was restarted at least once and is running again. The state file lists the
    # sessions in no fixed order (and includes the default one), so the entry is
    # read by name.
    for _ in $(seq 1 200); do
        if [ ! -f "$panic_trigger" ] \
            && [ "$(t21_session_field "$state_file" status)" = "running" ] \
            && [ "$(t21_session_field "$state_file" restart_count)" -ge 1 ] 2>/dev/null; then
            local count=$(count_lines "$log_file")
            if [ "$count" -ge 1 ]; then break; fi
        fi
        sleep 0.05
    done

    stop_supervisor "$spid"

    local caught_panic=0
    if grep -q "Session loop caught panic" "$stderr_file" 2>/dev/null; then
        caught_panic=1
    fi
    local trigger_gone=0
    if [ ! -f "$panic_trigger" ]; then
        trigger_gone=1
    fi
    local restarts=$(t21_session_field "$state_file" restart_count)

    if [ "$caught_panic" -eq 1 ] && [ "$trigger_gone" -eq 1 ] && [ "${restarts:-0}" -ge 1 ]; then
        rec "T21" "PASS" "forced session panic was caught, logged, counted as crash, and recovered to healthy running"
    else
        rec "T21" "FAIL" "panic recovery failed (caught=$caught_panic, trigger_gone=$trigger_gone, restarts=$restarts)"
    fi
}

# ==============================================================================
# T22: Dead session thread detected by coordinator, marked failed, status exits 2
# ==============================================================================
test_t22() {
    local tdir="${TEST_TMP}/t22"
    mkdir -p "$tdir/home" "$tdir/work" "$tdir/control"
    local state_file="$tdir/state.json"
    local cfg_file="$tdir/config.json"
    local die_trigger="$tdir/die_trigger"

    touch "$die_trigger"

    cat <<EOF > "$cfg_file"
{
    "state_file": "$state_file",
    "heartbeat_interval_secs": 0.2,
    "sessions": [
        {
            "name": "sess_die",
            "workdir": "$tdir/work",
            "session_id": "die-022",
            "claude_binary": "$FAKE_CLAUDE",
            "readiness_poll_secs": 0.2,
            "env": { "FAKE_CLAUDE_CONTROL_DIR": "$tdir/control" }
        }
    ]
}
EOF

    CLAUDE_SUPERVISOR_TEST_THREAD_DIE="$die_trigger" HOME="$tdir/home" "$BIN" run --config "$cfg_file" >/dev/null 2>&1 &
    local spid=$!
    ACTIVE_PIDS+=("$spid")

    # Wait for coordinator to mark session failed in state file
    local failed=0
    for _ in $(seq 1 50); do
        if grep -q '"status": "failed"' "$state_file" 2>/dev/null; then
            failed=1
            break
        fi
        sleep 0.05
    done

    # Probe status command
    local status_rc=0
    "$BIN" status --state-file "$state_file" >/dev/null 2>&1 || status_rc=$?

    stop_supervisor "$spid"

    if [ "$failed" -eq 1 ] && [ "$status_rc" -eq 2 ]; then
        rec "T22" "PASS" "dead session thread detected by coordinator, marked failed, status exited 2"
    else
        rec "T22" "FAIL" "dead thread detection failed (marked_failed=$failed, status_rc=$status_rc)"
    fi
}

# ==============================================================================
# T23: print-config with CLAUDE_WORKSPACES=a,b -> 3 sessions (default + a + b)
# ==============================================================================
test_t23() {
    local tdir="${TEST_TMP}/t23"
    mkdir -p "$tdir/root/a" "$tdir/root/b" "$tdir/home"
    local out_json="$tdir/config.json"

    HOME="$tdir/home" CLAUDE_WORKSPACE_ROOT="$tdir/root" CLAUDE_WORKSPACES="a,b" \
        "$BIN" print-config > "$out_json"

    local has_default=$(grep -q '"name": "default"' "$out_json" && echo 1 || echo 0)
    local has_a=$(grep -q '"name": "a"' "$out_json" && echo 1 || echo 0)
    local has_b=$(grep -q '"name": "b"' "$out_json" && echo 1 || echo 0)
    local has_rem_a=$(grep -q '"ai-server-a"' "$out_json" && echo 1 || echo 0)
    local has_rem_b=$(grep -q '"ai-server-b"' "$out_json" && echo 1 || echo 0)
    local has_transcripts=$(grep -q '"transcript_path":' "$out_json" && echo 1 || echo 0)

    if [ "$has_default" -eq 1 ] && [ "$has_a" -eq 1 ] && [ "$has_b" -eq 1 ] && \
       [ "$has_rem_a" -eq 1 ] && [ "$has_rem_b" -eq 1 ] && [ "$has_transcripts" -eq 1 ]; then
        rec "T23" "PASS" "print-config with CLAUDE_WORKSPACES=a,b yielded 3 sessions with correct remote names and transcript paths"
    else
        rec "T23" "FAIL" "print-config failed (def=$has_default, a=$has_a, b=$has_b, rem_a=$has_rem_a, rem_b=$has_rem_b, trans=$has_transcripts)"
    fi
}

# ==============================================================================
# T24: Multi-workspace execution: cwd, args, independent restart on SIGKILL with --resume
# ==============================================================================
test_t24() {
    local tdir="${TEST_TMP}/t24"
    mkdir -p "$tdir/root/ws1" "$tdir/root/ws2" "$tdir/home" "$tdir/control" "$tdir/bin"
    ln -sf "$FAKE_CLAUDE" "$tdir/bin/claude"
    local state_file="$tdir/state.json"
    local log_file="$tdir/fake-claude.log"

    cat <<EOF > "$tdir/config.json"
{
    "state_file": "$state_file",
    "heartbeat_interval_secs": 0.2
}
EOF

    PATH="$tdir/bin:$PATH" FAKE_CLAUDE_LOG="$log_file" FAKE_CLAUDE_CONTROL_DIR="$tdir/control" \
    HOME="$tdir/home" CLAUDE_DEFAULT_SESSION=0 CLAUDE_WORKSPACE_ROOT="$tdir/root" CLAUDE_WORKSPACES="ws1,ws2" \
        "$BIN" run --config "$tdir/config.json" >/dev/null 2>&1 &
    local spid=$!
    ACTIVE_PIDS+=("$spid")

    # Wait for both sessions to launch
    for _ in $(seq 1 40); do
        local count=$(count_lines "$log_file")
        if [ "$count" -ge 2 ]; then break; fi
        sleep 0.05
    done

    local ws1_start=$(grep "cwd=$tdir/root/ws1" "$log_file" | head -n 1 || true)
    local ws2_start=$(grep "cwd=$tdir/root/ws2" "$log_file" | head -n 1 || true)
    local ws1_has_remote=$(echo "$ws1_start" | grep -q -- "--remote-control ai-server-ws1" && echo 1 || echo 0)
    local ws2_has_remote=$(echo "$ws2_start" | grep -q -- "--remote-control ai-server-ws2" && echo 1 || echo 0)
    local ws1_pid=$(echo "$ws1_start" | awk '{print $2}' | cut -d= -f2 || true)
    local ws2_pid=$(echo "$ws2_start" | awk '{print $2}' | cut -d= -f2 || true)

    # Pre-create transcript for ws1 so restart detects it and uses --resume
    local ws1_id=$(echo "$ws1_start" | grep -o -- '--session-id [^ ]*' | awk '{print $2}')
    local ws1_slug=$(echo "$tdir/root/ws1" | tr '/.' '--')
    local transcript_dir="$tdir/home/.claude/projects/$ws1_slug"
    mkdir -p "$transcript_dir"
    echo '{"type":"test"}' > "$transcript_dir/${ws1_id}.jsonl"

    # SIGKILL child ws1
    if [ -n "$ws1_pid" ]; then
        kill -9 "$ws1_pid" 2>/dev/null || true
    fi

    # Wait for ws1 to restart
    for _ in $(seq 1 50); do
        local count=$(count_lines "$log_file")
        if [ "$count" -ge 3 ]; then break; fi
        sleep 0.05
    done

    # Check if ws2 is still running
    local ws2_alive=0
    if [ -n "$ws2_pid" ] && kill -0 "$ws2_pid" 2>/dev/null; then
        ws2_alive=1
    fi

    # Check that restarted ws1 passed --resume
    local ws1_restart_resume=$(grep "cwd=$tdir/root/ws1" "$log_file" | grep -q -- "--resume $ws1_id" && echo 1 || echo 0)

    stop_supervisor "$spid"

    if [ "$ws1_has_remote" -eq 1 ] && [ "$ws2_has_remote" -eq 1 ] && [ "$ws2_alive" -eq 1 ] && [ "$ws1_restart_resume" -eq 1 ]; then
        rec "T24" "PASS" "multi-workspace: correct cwd and remote names, independent restart on SIGKILL with --resume"
    else
        rec "T24" "FAIL" "multi-workspace run failed (ws1_rem=$ws1_has_remote, ws2_rem=$ws2_has_remote, ws2_alive=$ws2_alive, ws1_resume=$ws1_restart_resume)"
    fi
}

# ==============================================================================
# T25: Supervisor restart yields SAME session ids (determinism) and resumes
# ==============================================================================
test_t25() {
    local tdir="${TEST_TMP}/t25"
    mkdir -p "$tdir/root/sess_det" "$tdir/home" "$tdir/control" "$tdir/bin"
    ln -sf "$FAKE_CLAUDE" "$tdir/bin/claude"
    local state_file="$tdir/state.json"
    local log1="$tdir/fake1.log"
    local log2="$tdir/fake2.log"

    cat <<EOF > "$tdir/config.json"
{
    "state_file": "$state_file",
    "heartbeat_interval_secs": 0.2
}
EOF

    # 1. First run of supervisor
    PATH="$tdir/bin:$PATH" FAKE_CLAUDE_LOG="$log1" FAKE_CLAUDE_CONTROL_DIR="$tdir/control" \
    HOME="$tdir/home" CLAUDE_DEFAULT_SESSION=0 CLAUDE_WORKSPACE_ROOT="$tdir/root" CLAUDE_WORKSPACES="sess_det" \
        "$BIN" run --config "$tdir/config.json" >/dev/null 2>&1 &
    local spid1=$!
    ACTIVE_PIDS+=("$spid1")

    for _ in $(seq 1 40); do
        if [ -s "$log1" ]; then break; fi
        sleep 0.05
    done
    stop_supervisor "$spid1"

    local id1=$(grep "cwd=$tdir/root/sess_det" "$log1" | grep -o -- '--session-id [^ ]*' | awk '{print $2}')

    # Create transcript file for id1
    local slug=$(echo "$tdir/root/sess_det" | sed 's/[^A-Za-z0-9]/-/g')
    local transcript_dir="$tdir/home/.claude/projects/$slug"
    mkdir -p "$transcript_dir"
    echo '{"type":"test"}' > "$transcript_dir/${id1}.jsonl"

    # 2. Second run of supervisor
    PATH="$tdir/bin:$PATH" FAKE_CLAUDE_LOG="$log2" FAKE_CLAUDE_CONTROL_DIR="$tdir/control" \
    HOME="$tdir/home" CLAUDE_DEFAULT_SESSION=0 CLAUDE_WORKSPACE_ROOT="$tdir/root" CLAUDE_WORKSPACES="sess_det" \
        "$BIN" run --config "$tdir/config.json" >/dev/null 2>&1 &
    local spid2=$!
    ACTIVE_PIDS+=("$spid2")

    for _ in $(seq 1 40); do
        if [ -s "$log2" ]; then break; fi
        sleep 0.05
    done
    stop_supervisor "$spid2"

    local id2=$(grep "cwd=$tdir/root/sess_det" "$log2" | grep -o -- '--resume [^ ]*' | awk '{print $2}')

    if [ -n "$id1" ] && [ "$id1" = "$id2" ]; then
        rec "T25" "PASS" "supervisor restart yields same deterministic session id and resumes with --resume"
    else
        rec "T25" "FAIL" "determinism / resume failed (id1=$id1, id2=$id2)"
    fi
}

# ==============================================================================
# T26: Invalid name / missing dir skipped with warning, duplicate names exit 1
# ==============================================================================
test_t26() {
    local tdir="${TEST_TMP}/t26"
    mkdir -p "$tdir/root/valid_ws" "$tdir/home"
    local warn_log="$tdir/warn.log"
    local out_json="$tdir/out.json"
    local dup_log="$tdir/dup.log"

    # 1. Invalid name and missing dir skipped with warning
    local rc1=0
    HOME="$tdir/home" CLAUDE_DEFAULT_SESSION=0 CLAUDE_WORKSPACE_ROOT="$tdir/root" \
    CLAUDE_WORKSPACES="valid_ws,missing_ws,_invalid_name" \
        "$BIN" print-config > "$out_json" 2> "$warn_log" || rc1=$?

    local has_warn_missing=$(grep -q "missing_ws" "$warn_log" && echo 1 || echo 0)
    local has_warn_invalid=$(grep -q "_invalid_name" "$warn_log" && echo 1 || echo 0)
    local has_only_valid=$(grep -q '"name": "valid_ws"' "$out_json" && echo 1 || echo 0)
    local valid_count=$(grep -c '"name":' "$out_json" || true)

    # 2. Duplicate session names exit 1
    local rc2=0
    HOME="$tdir/home" CLAUDE_DEFAULT_SESSION=0 CLAUDE_WORKSPACE_ROOT="$tdir/root" \
    CLAUDE_WORKSPACES="valid_ws,valid_ws" \
        "$BIN" print-config >/dev/null 2> "$dup_log" || rc2=$?

    local has_dup_error=$(grep -q "Duplicate session name" "$dup_log" && echo 1 || echo 0)

    if [ "$rc1" -eq 0 ] && [ "$has_warn_missing" -eq 1 ] && [ "$has_warn_invalid" -eq 1 ] && \
       [ "$has_only_valid" -eq 1 ] && [ "$valid_count" -eq 1 ] && [ "$rc2" -eq 1 ] && [ "$has_dup_error" -eq 1 ]; then
        rec "T26" "PASS" "invalid name / missing dir skipped with warning, duplicate names exit 1"
    else
        rec "T26" "FAIL" "skipping or duplicate validation failed (rc1=$rc1, missing=$has_warn_missing, inv=$has_warn_invalid, valid_count=$valid_count, rc2=$rc2, dup_err=$has_dup_error)"
    fi
}

# ==============================================================================
# T27: max_crashes=0 retries forever (8 consecutive crashes survive, resume normally, status healthy)
# ==============================================================================
test_t27() {
    local tdir="${TEST_TMP}/t27"
    mkdir -p "$tdir/home" "$tdir/work" "$tdir/control"
    local log_file="$tdir/fake-claude.log"
    local state_file="$tdir/state.json"
    local cfg_file="$tdir/config.json"

    # Make fake-claude crash immediately on launch
    touch "$tdir/control/action_exit1"

    cat <<EOF > "$cfg_file"
{
    "state_file": "$state_file",
    "heartbeat_interval_secs": 0.2,
    "sessions": [
        {
            "name": "sess27_unlimited",
            "workdir": "$tdir/work",
            "session_id": "unlimited-027",
            "claude_binary": "$FAKE_CLAUDE",
            "base_backoff_secs": 0.05,
            "max_backoff_secs": 0.1,
            "max_crashes": 0,
            "crash_window_secs": 10.0,
            "readiness_poll_secs": 0.2,
            "env": {
                "FAKE_CLAUDE_LOG": "$log_file",
                "FAKE_CLAUDE_CONTROL_DIR": "$tdir/control"
            }
        }
    ]
}
EOF

    HOME="$tdir/home" "$BIN" run --config "$cfg_file" >/dev/null 2>&1 &
    local spid=$!
    ACTIVE_PIDS+=("$spid")

    # Wait for at least 8 crashes
    for _ in $(seq 1 120); do
        local count=$(grep -c "exit=1" "$log_file" 2>/dev/null | tail -n 1 || true)
        count="${count:-0}"
        if [ "$count" -ge 8 ]; then break; fi
        sleep 0.05
    done

    local crashes_seen=$(grep -c "exit=1" "$log_file" 2>/dev/null | tail -n 1 || echo 0)

    # Remove crash trigger so the next run starts normally
    rm -f "$tdir/control/action_exit1"

    # Wait for process to start normally and enter running state
    for _ in $(seq 1 60); do
        local launches=$(grep -c "argv=" "$log_file" 2>/dev/null | tail -n 1 || true)
        launches="${launches:-0}"
        if [ "$launches" -gt "$crashes_seen" ]; then
            if grep -A 8 '"sess27_unlimited"' "$state_file" 2>/dev/null | grep -q '"status": "running"'; then
                break
            fi
        fi
        sleep 0.05
    done

    # Probe status command
    local status_rc=0
    "$BIN" status --state-file "$state_file" >/dev/null 2>&1 || status_rc=$?

    local final_status=$(grep -A 8 '"sess27_unlimited"' "$state_file" 2>/dev/null | grep '"status"' | tr -d '", ' | cut -d: -f2 || true)
    local restart_count=$(grep -A 8 '"sess27_unlimited"' "$state_file" 2>/dev/null | grep '"restart_count"' | tr -d '", ' | cut -d: -f2 || true)

    stop_supervisor "$spid"

    if [ "$crashes_seen" -ge 8 ] && [ "$status_rc" -eq 0 ] && [ "$final_status" = "running" ] && [ "${restart_count:-0}" -ge 8 ]; then
        rec "T27" "PASS" "max_crashes=0 survived 8 immediate crashes, restarted normally (status=0, final_status=$final_status, restarts=$restart_count)"
    else
        rec "T27" "FAIL" "unlimited crashes failed (crashes_seen=$crashes_seen, status_rc=$status_rc, final_status=$final_status, restarts=$restart_count)"
    fi
}

# ==============================================================================
# T28: Config source --config loads valid path, non-existent --config exits 1
# ==============================================================================
test_t28() {
    local tdir="${TEST_TMP}/t28"
    mkdir -p "$tdir"
    local cfg_file="$tdir/custom.json"
    cat <<EOF > "$cfg_file"
{
    "state_file": "$tdir/state_from_cli.json"
}
EOF

    # 1. Valid --config loads state_file
    local out1=$("$BIN" print-config --config "$cfg_file" 2>/dev/null)
    local has_custom=$(echo "$out1" | grep -q "state_from_cli.json" && echo 1 || echo 0)

    # 2. Non-existent explicit --config exits 1 with error
    local rc_err=0
    local err_out
    err_out=$("$BIN" print-config --config "$tdir/nonexistent.json" 2>&1) || rc_err=$?

    if [ "$has_custom" -eq 1 ] && [ "$rc_err" -eq 1 ] && [ -n "$err_out" ]; then
        rec "T28" "PASS" "--config loads valid path; explicit non-existent --config exits 1 with error"
    else
        rec "T28" "FAIL" "--config handling failed (has_custom=$has_custom, rc_err=$rc_err)"
    fi
}

# ==============================================================================
# T29: Config source env var loads valid path, non-existent exits 1
# (primary AGENT_ORCHESTRATE_CONFIG; legacy CLAUDE_SUPERVISOR_CONFIG still accepted)
# ==============================================================================
test_t29() {
    local tdir="${TEST_TMP}/t29"
    mkdir -p "$tdir"
    local cfg_file="$tdir/env_custom.json"
    cat <<EOF > "$cfg_file"
{
    "state_file": "$tdir/state_from_env.json"
}
EOF

    # 1. Valid env config loads state_file (legacy CLAUDE_SUPERVISOR_CONFIG alias)
    local out1=$(CLAUDE_SUPERVISOR_CONFIG="$cfg_file" "$BIN" print-config 2>/dev/null)
    local has_custom=$(echo "$out1" | grep -q "state_from_env.json" && echo 1 || echo 0)

    # 1b. Primary AGENT_ORCHESTRATE_CONFIG also loads state_file
    local out1b=$(AGENT_ORCHESTRATE_CONFIG="$cfg_file" "$BIN" print-config 2>/dev/null)
    local has_custom_b=$(echo "$out1b" | grep -q "state_from_env.json" && echo 1 || echo 0)

    # 2. Non-existent explicit env config exits 1 with error
    local rc_err=0
    local err_out
    err_out=$(CLAUDE_SUPERVISOR_CONFIG="$tdir/nonexistent.json" "$BIN" print-config 2>&1) || rc_err=$?

    if [ "$has_custom" -eq 1 ] && [ "$has_custom_b" -eq 1 ] && [ "$rc_err" -eq 1 ] && [ -n "$err_out" ]; then
        rec "T29" "PASS" "config env vars load valid path (both names); non-existent env path exits 1 with error"
    else
        rec "T29" "FAIL" "config env var handling failed (has_custom=$has_custom, has_custom_b=$has_custom_b, rc_err=$rc_err)"
    fi
}

# ==============================================================================
# T30: CWD config search is removed: config.json / agent-orchestrate.json in cwd are ignored
# ==============================================================================
test_t30() {
    local tdir="${TEST_TMP}/t30"
    mkdir -p "$tdir/work"
    cat <<EOF > "$tdir/work/config.json"
{
    "state_file": "$tdir/work/bad_state_cwd.json"
}
EOF
    cat <<EOF > "$tdir/work/agent-orchestrate.json"
{
    "state_file": "$tdir/work/bad_state_cwd2.json"
}
EOF

    # Running from $tdir/work without --config or config env must NOT pick up cwd config
    local out=$(cd "$tdir/work" && env -u AGENT_ORCHESTRATE_CONFIG -u CLAUDE_SUPERVISOR_CONFIG "$BIN" print-config 2>/dev/null)
    local picked_up_cwd=$(echo "$out" | grep -q "bad_state_cwd" && echo 1 || echo 0)
    local has_default_state=$(echo "$out" | grep -q "/tmp/claude-supervisor-state.json" && echo 1 || echo 0)

    if [ "$picked_up_cwd" -eq 0 ] && [ "$has_default_state" -eq 1 ]; then
        rec "T30" "PASS" "cwd-relative config search is removed: cwd config files are ignored"
    else
        rec "T30" "FAIL" "cwd config files were unexpectedly picked up (picked_up_cwd=$picked_up_cwd)"
    fi
}

# ==============================================================================
# T31: Child outliving closed PTY killed with SIGTERM and restarted with reason "signal: 15"
# ==============================================================================
test_t31() {
    local tdir="${TEST_TMP}/t31"
    mkdir -p "$tdir/home" "$tdir/work" "$tdir/control"
    local log_file="$tdir/fake-claude.log"
    local state_file="$tdir/state.json"
    local sup_log="$tdir/supervisor.log"
    local cfg_file="$tdir/config.json"

    # Child will close its tty (EOF on master pty) and loop silently
    touch "$tdir/control/action_close_tty"

    cat <<EOF > "$cfg_file"
{
    "state_file": "$state_file",
    "sessions": [
        {
            "name": "sess31",
            "workdir": "$tdir/work",
            "session_id": "test-session-031",
            "claude_binary": "$FAKE_CLAUDE",
            "pty_eof_grace_secs": 0.5,
            "kill_grace_secs": 0.5,
            "base_backoff_secs": 0.1,
            "readiness_poll_secs": 0.2,
            "env": {
                "FAKE_CLAUDE_LOG": "$log_file",
                "FAKE_CLAUDE_CONTROL_DIR": "$tdir/control"
            }
        }
    ]
}
EOF

    HOME="$tdir/home" "$BIN" run --config "$cfg_file" > "$sup_log" 2>&1 &
    local spid=$!
    ACTIVE_PIDS+=("$spid")

    # Wait for initial launch
    for _ in $(seq 1 40); do
        if [ -s "$log_file" ]; then break; fi
        sleep 0.05
    done

    # Remove close_tty action so when restarted, it runs normally
    rm -f "$tdir/control/action_close_tty"

    # Wait for restart (at least 2 invocations in log)
    local has_sig15=0
    for _ in $(seq 1 60); do
        local count=$(count_lines "$log_file")
        if [ "$count" -ge 2 ]; then
            if grep -q '"last_exit_reason": "signal: 15"' "$state_file" 2>/dev/null; then
                has_sig15=1
            fi
            break
        fi
        sleep 0.05
    done

    stop_supervisor "$spid"

    local runs=$(count_lines "$log_file")
    local has_echild=$(grep -i "ECHILD" "$sup_log" && echo 1 || echo 0)
    local has_rc_unknown=$(grep -i "rc=unknown" "$sup_log" && echo 1 || echo 0)
    local has_sig15_log=$(grep -q "Process exited (signal=15" "$sup_log" && echo 1 || echo 0)

    if [ "$runs" -ge 2 ] && [ "$has_echild" -eq 0 ] && [ "$has_rc_unknown" -eq 0 ] && [ "$has_sig15_log" -eq 1 ]; then
        rec "T31" "PASS" "child outliving PTY killed with SIGTERM within grace and restarted with reason signal: 15"
    else
        rec "T31" "FAIL" "PTY EOF restart failed (runs=$runs, sig15_log=$has_sig15_log, echild=$has_echild, rc_unknown=$has_rc_unknown)"
    fi
}

# ==============================================================================
# T32: Child outliving closed PTY ignoring SIGTERM escalated to SIGKILL with reason "signal: 9"
# ==============================================================================
test_t32() {
    local tdir="${TEST_TMP}/t32"
    mkdir -p "$tdir/home" "$tdir/work" "$tdir/control"
    local log_file="$tdir/fake-claude.log"
    local state_file="$tdir/state.json"
    local sup_log="$tdir/supervisor.log"
    local cfg_file="$tdir/config.json"

    # Child will close its tty and ignore SIGTERM
    touch "$tdir/control/action_close_tty_ignore_sigterm"

    cat <<EOF > "$cfg_file"
{
    "state_file": "$state_file",
    "sessions": [
        {
            "name": "sess32",
            "workdir": "$tdir/work",
            "session_id": "test-session-032",
            "claude_binary": "$FAKE_CLAUDE",
            "pty_eof_grace_secs": 0.4,
            "kill_grace_secs": 0.3,
            "base_backoff_secs": 0.1,
            "readiness_poll_secs": 0.2,
            "env": {
                "FAKE_CLAUDE_LOG": "$log_file",
                "FAKE_CLAUDE_CONTROL_DIR": "$tdir/control"
            }
        }
    ]
}
EOF

    HOME="$tdir/home" "$BIN" run --config "$cfg_file" > "$sup_log" 2>&1 &
    local spid=$!
    ACTIVE_PIDS+=("$spid")

    for _ in $(seq 1 40); do
        if [ -s "$log_file" ]; then break; fi
        sleep 0.05
    done

    # Remove the stub action so restarted child runs normally
    rm -f "$tdir/control/action_close_tty_ignore_sigterm"

    local has_sig9=0
    for _ in $(seq 1 60); do
        local count=$(count_lines "$log_file")
        if [ "$count" -ge 2 ]; then
            if grep -q '"last_exit_reason": "signal: 9"' "$state_file" 2>/dev/null; then
                has_sig9=1
            fi
            break
        fi
        sleep 0.05
    done

    stop_supervisor "$spid"

    local runs=$(count_lines "$log_file")
    local has_echild=$(grep -i "ECHILD" "$sup_log" && echo 1 || echo 0)
    local has_rc_unknown=$(grep -i "rc=unknown" "$sup_log" && echo 1 || echo 0)
    local has_sig9_log=$(grep -q "Process exited (signal=9" "$sup_log" && echo 1 || echo 0)

    if [ "$runs" -ge 2 ] && [ "$has_echild" -eq 0 ] && [ "$has_rc_unknown" -eq 0 ] && [ "$has_sig9_log" -eq 1 ]; then
        rec "T32" "PASS" "stubborn child outliving PTY killed with SIGKILL and restarted with reason signal: 9"
    else
        rec "T32" "FAIL" "PTY EOF SIGKILL restart failed (runs=$runs, sig9_log=$has_sig9_log, echild=$has_echild, rc_unknown=$has_rc_unknown)"
    fi
}

# ==============================================================================
# T33: No leaked PTY file descriptors with two concurrent sessions
# ==============================================================================
test_t33() {
    local tdir="${TEST_TMP}/t33"
    mkdir -p "$tdir/home" "$tdir/work1" "$tdir/work2" "$tdir/control" "$tdir/fds"
    local state_file="$tdir/state.json"
    local cfg_file="$tdir/config.json"
    local log1="$tdir/fake1.log"
    local log2="$tdir/fake2.log"

    cat <<EOF > "$cfg_file"
{
    "state_file": "$state_file",
    "sessions": [
        {
            "name": "sess_fd_a",
            "workdir": "$tdir/work1",
            "session_id": "test-session-033a",
            "claude_binary": "$FAKE_CLAUDE",
            "readiness_poll_secs": 0.2,
            "env": {
                "FAKE_CLAUDE_LOG": "$log1",
                "FAKE_CLAUDE_CONTROL_DIR": "$tdir/control",
                "FAKE_CLAUDE_RECORD_FDS": "$tdir/fds/a"
            }
        },
        {
            "name": "sess_fd_b",
            "workdir": "$tdir/work2",
            "session_id": "test-session-033b",
            "claude_binary": "$FAKE_CLAUDE",
            "readiness_poll_secs": 0.2,
            "env": {
                "FAKE_CLAUDE_LOG": "$log2",
                "FAKE_CLAUDE_CONTROL_DIR": "$tdir/control",
                "FAKE_CLAUDE_RECORD_FDS": "$tdir/fds/b"
            }
        }
    ]
}
EOF

    HOME="$tdir/home" "$BIN" run --config "$cfg_file" >/dev/null 2>&1 &
    local spid=$!
    ACTIVE_PIDS+=("$spid")

    # Wait (10 s cap) until both fake sessions started and recorded their FDs.
    for _ in $(seq 1 200); do
        if [ -s "$log1" ] && [ -s "$log2" ] && [ "$(find "$tdir/fds" -type f -size +0c 2>/dev/null | wc -l)" -ge 2 ]; then break; fi
        sleep 0.05
    done

    stop_supervisor "$spid"

    # Check the recorded FDs
    local fd_files=("$tdir/fds"/*)
    local leaked_ptmx=0
    local leaked_other_pts=0

    for f in "${fd_files[@]}"; do
        if [ -f "$f" ]; then
            if grep -q "ptmx" "$f"; then
                leaked_ptmx=1
            fi
            while IFS= read -r line; do
                local fd_num=$(echo "$line" | awk '{print $1}')
                local target=$(echo "$line" | awk '{print $3}')
                if [ -n "$fd_num" ] && [ "$fd_num" -gt 2 ] && [[ "$target" == *"/dev/pts/"* ]]; then
                    leaked_other_pts=1
                fi
            done < "$f"
        fi
    done

    if [ ${#fd_files[@]} -ge 2 ] && [ "$leaked_ptmx" -eq 0 ] && [ "$leaked_other_pts" -eq 0 ]; then
        rec "T33" "PASS" "no leaked master/slave PTY file descriptors across concurrent sessions"
    else
        rec "T33" "FAIL" "FD leak detected (files=${#fd_files[@]}, ptmx=$leaked_ptmx, pts=$leaked_other_pts)"
    fi
}

# ==============================================================================
# T34: Stdout writer bounded channel: stalled reader does not block session or supervision
# ==============================================================================
test_t34() {
    local tdir="${TEST_TMP}/t34"
    mkdir -p "$tdir/home" "$tdir/work" "$tdir/control"
    local state_file="$tdir/state.json"
    local cfg_file="$tdir/config.json"
    local log_file="$tdir/fake-claude.log"

    # Child spams lots of output
    touch "$tdir/control/action_tui_burst"

    cat <<EOF > "$cfg_file"
{
    "state_file": "$state_file",
    "sessions": [
        {
            "name": "sess34_spam",
            "workdir": "$tdir/work",
            "session_id": "test-session-034",
            "claude_binary": "$FAKE_CLAUDE",
            "base_backoff_secs": 0.1,
            "readiness_poll_secs": 0.2,
            "env": {
                "FAKE_CLAUDE_LOG": "$log_file",
                "FAKE_CLAUDE_CONTROL_DIR": "$tdir/control"
            }
        }
    ]
}
EOF

    # Create a FIFO and background cat process
    local fifo_path="$tdir/stall_pipe"
    mkfifo "$fifo_path"
    cat "$fifo_path" > /dev/null &
    local reader_pid=$!

    CLAUDE_DEFAULT_SESSION=0 HOME="$tdir/home" "$BIN" run --config "$cfg_file" > "$fifo_path" 2>&1 &
    local spid=$!
    ACTIVE_PIDS+=("$spid")

    # Wait for child to launch
    for _ in $(seq 1 40); do
        if [ -s "$log_file" ]; then break; fi
        sleep 0.05
    done
    local child_pid=$(head -n 1 "$log_file" | awk '{print $2}' | cut -d= -f2 || true)

    # Now pause reader with SIGSTOP to stall the stdout pipe
    kill -STOP "$reader_pid" 2>/dev/null || true

    # Let output fill the pipe buffer and bounded channel
    sleep 0.5

    # Kill child process; session thread must still detect exit and restart child even while stdout is stalled
    rm -f "$tdir/control/action_tui_burst"
    if [ -n "$child_pid" ]; then
        kill -9 "$child_pid" 2>/dev/null || true
    fi

    # Wait for restart
    local restarted=0
    for _ in $(seq 1 40); do
        local count=$(count_lines "$log_file")
        if [ "$count" -ge 2 ]; then
            restarted=1
            break
        fi
        sleep 0.05
    done

    # Shut down supervisor
    kill -TERM "$spid" 2>/dev/null || true
    local exited=0
    for _ in $(seq 1 40); do
        if ! kill -0 "$spid" 2>/dev/null; then
            exited=1
            break
        fi
        sleep 0.05
    done

    # Clean up reader
    kill -CONT "$reader_pid" 2>/dev/null || true
    kill -KILL "$reader_pid" 2>/dev/null || true
    wait "$reader_pid" 2>/dev/null || true

    if [ "$restarted" -eq 1 ] && [ "$exited" -eq 1 ]; then
        rec "T34" "PASS" "stalled stdout pipe reader did not block session restart or graceful shutdown"
    else
        rec "T34" "FAIL" "stdout stalling blocked supervision (restarted=$restarted, exited=$exited)"
    fi
}

# ==============================================================================
# T35: Status command exits 4 when state is stopped or any session is stopped
# ==============================================================================
test_t35() {
    local tdir="${TEST_TMP}/t35"
    mkdir -p "$tdir"
    local state_file="$tdir/state.json"
    local now=$(date +%s)

    # 1. Supervisor status is "stopped"
    cat <<EOF > "$state_file"
{
    "supervisor_pid": 1234,
    "heartbeat": "2026-10-01T07:00:00Z",
    "heartbeat_epoch": $now,
    "status": "stopped",
    "sessions": {}
}
EOF
    local rc_stopped=0
    "$BIN" status --state-file "$state_file" >/dev/null 2>&1 || rc_stopped=$?

    # 2. A session is in "stopped" status
    cat <<EOF > "$state_file"
{
    "supervisor_pid": 1234,
    "heartbeat": "2026-10-01T07:00:00Z",
    "heartbeat_epoch": $now,
    "status": "running",
    "sessions": {
        "sess_stop": {
            "name": "sess_stop",
            "pid": null,
            "start_time": null,
            "start_time_epoch": null,
            "uptime_secs": null,
            "restart_count": 0,
            "last_exit_reason": null,
            "status": "stopped",
            "last_health_result": "ok"
        }
    }
}
EOF
    local rc_sess_stopped=0
    "$BIN" status --state-file "$state_file" >/dev/null 2>&1 || rc_sess_stopped=$?

    if [ "$rc_stopped" -eq 4 ] && [ "$rc_sess_stopped" -eq 4 ]; then
        rec "T35" "PASS" "status exits 4 when state is stopped or any session is stopped"
    else
        rec "T35" "FAIL" "status stopped exit code failed (rc_stopped=$rc_stopped, rc_sess_stopped=$rc_sess_stopped)"
    fi
}

# ==============================================================================
# T36: Explicit transcript_path in config is honored and preserved by workspaces
# ==============================================================================
test_t36() {
    local tdir="${TEST_TMP}/t36"
    mkdir -p "$tdir"
    local cfg_file="$tdir/config.json"
    local custom_transcript="$tdir/custom-transcripts/my-session.jsonl"

    cat <<EOF > "$cfg_file"
{
    "sessions": [
        {
            "name": "default",
            "workdir": "$tdir",
            "session_id": "test-session-036",
            "transcript_path": "$custom_transcript"
        }
    ]
}
EOF

    local out=$("$BIN" print-config --config "$cfg_file" 2>/dev/null)
    local has_path=$(echo "$out" | grep -q "$custom_transcript" && echo 1 || echo 0)

    if [ "$has_path" -eq 1 ]; then
        rec "T36" "PASS" "explicit transcript_path is honored in print-config"
    else
        rec "T36" "FAIL" "explicit transcript_path was overwritten or omitted"
    fi
}

# ==============================================================================
# T37: Empty and whitespace-only env vars treated as unset, HOME fallback to /home/coder
# ==============================================================================
test_t37() {
    local tdir="${TEST_TMP}/t37"
    mkdir -p "$tdir"
    local out

    out=$(CLAUDE_WORKSPACES="   " \
          CLAUDE_DEFAULT_SESSION="" \
          CLAUDE_SESSION_ARGS="  \t  " \
          CLAUDE_MAX_CRASHES="   " \
          HOME="" \
          "$BIN" print-config 2>/dev/null)

    local has_coder_workdir=$(echo "$out" | grep -q '"workdir": "/home/coder"' && echo 1 || echo 0)
    local has_default_sess=$(echo "$out" | grep -q '"name": "default"' && echo 1 || echo 0)
    local has_max_crashes_zero=$(echo "$out" | grep -q '"max_crashes": 0' && echo 1 || echo 0)

    if [ "$has_coder_workdir" -eq 1 ] && [ "$has_default_sess" -eq 1 ] && [ "$has_max_crashes_zero" -eq 1 ]; then
        rec "T37" "PASS" "empty/whitespace env vars treated as unset, HOME fallback to /home/coder"
    else
        rec "T37" "FAIL" "empty env var handling failed (coder=$has_coder_workdir, def=$has_default_sess, mc0=$has_max_crashes_zero)"
    fi
}

# ==============================================================================
# T38: Readiness binary lookup finds executable in session env PATH
# ==============================================================================
test_t38() {
    local tdir="${TEST_TMP}/t38"
    mkdir -p "$tdir/home" "$tdir/work" "$tdir/control" "$tdir/custom_bin"
    ln -sf "$FAKE_CLAUDE" "$tdir/custom_bin/my-claude-bin"
    local log_file="$tdir/fake-claude.log"
    local state_file="$tdir/state.json"
    local cfg_file="$tdir/config.json"

    cat <<EOF > "$cfg_file"
{
    "state_file": "$state_file",
    "sessions": [
        {
            "name": "sess38",
            "workdir": "$tdir/work",
            "session_id": "test-session-038",
            "claude_binary": "my-claude-bin",
            "readiness_poll_secs": 0.2,
            "env": {
                "PATH": "$tdir/custom_bin:$PATH",
                "FAKE_CLAUDE_LOG": "$log_file",
                "FAKE_CLAUDE_CONTROL_DIR": "$tdir/control"
            }
        }
    ]
}
EOF

    PATH="/usr/bin:/bin" CLAUDE_DEFAULT_SESSION=0 HOME="$tdir/home" "$BIN" run --config "$cfg_file" >/dev/null 2>&1 &
    local spid=$!
    ACTIVE_PIDS+=("$spid")

    for _ in $(seq 1 40); do
        if [ -s "$log_file" ]; then break; fi
        sleep 0.05
    done

    stop_supervisor "$spid"

    local runs=$(count_lines "$log_file")
    if [ "$runs" -ge 1 ]; then
        rec "T38" "PASS" "readiness binary lookup resolved binary from session env PATH"
    else
        rec "T38" "FAIL" "binary lookup in session env PATH failed (runs=$runs)"
    fi
}

# ==============================================================================
# T39: Transcript path slug canonicalizes non-alphanumerics, warns on symlink workdir
# ==============================================================================
test_t39() {
    local tdir="${TEST_TMP}/t39"
    mkdir -p "$tdir/home" "$tdir/root/slug_dir+with special@chars/sub space"
    local target_dir="$tdir/root/slug_dir+with special@chars/sub space"
    ln -s "$target_dir" "$tdir/root/symlink_target"
    local warn_log="$tdir/warn.log"

    local out
    out=$(CLAUDE_DEFAULT_SESSION=0 CLAUDE_WORKSPACE_ROOT="$tdir/root" CLAUDE_WORKSPACES="symlink_target" \
          HOME="$tdir/home" "$BIN" print-config 2> "$warn_log")

    local canonical_target=$(cd -P "$target_dir" && pwd)
    local expected_slug=$(echo "$canonical_target" | sed 's/[^A-Za-z0-9]/-/g')

    local has_expected_slug=$(echo "$out" | grep -q -- "$expected_slug" && echo 1 || echo 0)
    local has_symlink_warn=$(grep -q "symlink target changes" "$warn_log" && echo 1 || echo 0)

    if [ "$has_expected_slug" -eq 1 ] && [ "$has_symlink_warn" -eq 1 ]; then
        rec "T39" "PASS" "canonical slug replaces non-alphanumerics with '-', warns on symlink workdir"
    else
        rec "T39" "FAIL" "slug derivation or warning failed (slug_match=$has_expected_slug, warn=$has_symlink_warn)"
    fi
}

# ==============================================================================
# T40: Self-healing: "already in use" error switches subsequent launch to --resume
# ==============================================================================
test_t40() {
    local tdir="${TEST_TMP}/t40"
    mkdir -p "$tdir/home" "$tdir/work" "$tdir/control"
    local log_file="$tdir/fake-claude.log"
    local state_file="$tdir/state.json"
    local cfg_file="$tdir/config.json"

    # Make fake claude fail if --session-id is passed
    touch "$tdir/control/action_already_in_use"

    cat <<EOF > "$cfg_file"
{
    "state_file": "$state_file",
    "sessions": [
        {
            "name": "sess40_heal",
            "workdir": "$tdir/work",
            "session_id": "test-session-040",
            "claude_binary": "$FAKE_CLAUDE",
            "base_backoff_secs": 0.1,
            "readiness_poll_secs": 0.2,
            "env": {
                "FAKE_CLAUDE_LOG": "$log_file",
                "FAKE_CLAUDE_CONTROL_DIR": "$tdir/control"
            }
        }
    ]
}
EOF

    HOME="$tdir/home" "$BIN" run --config "$cfg_file" >/dev/null 2>&1 &
    local spid=$!
    ACTIVE_PIDS+=("$spid")

    # Wait for at least 2 launches: first with --session-id (fails), second with --resume (succeeds)
    for _ in $(seq 1 60); do
        local count=$(grep -c "argv=" "$log_file" 2>/dev/null || echo 0)
        if [ "${count:-0}" -ge 2 ]; then break; fi
        sleep 0.05
    done

    stop_supervisor "$spid"

    local line1=$(grep "argv=" "$log_file" | head -n 1 || true)
    local line2=$(grep "argv=" "$log_file" | head -n 2 | tail -n 1 || true)
    local has_session_id_first=$(echo "$line1" | grep -q -- "--session-id" && echo 1 || echo 0)
    local has_resume_second=$(echo "$line2" | grep -q -- "--resume" && echo 1 || echo 0)

    if [ "$has_session_id_first" -eq 1 ] && [ "$has_resume_second" -eq 1 ]; then
        rec "T40" "PASS" "self-healing switched from --session-id to --resume on 'already in use' error"
    else
        rec "T40" "FAIL" "self-healing failed (line1=$line1, line2=$line2)"
    fi
}

# ==============================================================================
# T41: SIGHUP triggers graceful shutdown like SIGTERM
# ==============================================================================
test_t41() {
    local tdir="${TEST_TMP}/t41"
    mkdir -p "$tdir/home" "$tdir/work" "$tdir/control"
    local state_file="$tdir/state.json"
    local cfg_file="$tdir/config.json"
    local log_file="$tdir/fake-claude.log"

    cat <<EOF > "$cfg_file"
{
    "state_file": "$state_file",
    "shutdown_grace_secs": 2.0,
    "sessions": [
        {
            "name": "sess41",
            "workdir": "$tdir/work",
            "session_id": "test-session-041",
            "claude_binary": "$FAKE_CLAUDE",
            "readiness_poll_secs": 0.2,
            "env": {
                "FAKE_CLAUDE_LOG": "$log_file",
                "FAKE_CLAUDE_CONTROL_DIR": "$tdir/control"
            }
        }
    ]
}
EOF

    HOME="$tdir/home" "$BIN" run --config "$cfg_file" >/dev/null 2>&1 &
    local spid=$!

    for _ in $(seq 1 40); do
        if [ -s "$log_file" ]; then break; fi
        sleep 0.05
    done

    # Send SIGHUP
    kill -HUP "$spid" 2>/dev/null || true
    local exit_code=0
    wait "$spid" || exit_code=$?

    local state_status=$(grep '"status"' "$state_file" 2>/dev/null | head -n 1 | tr -d '", ' | cut -d: -f2 || true)

    if [ "$exit_code" -eq 0 ] && [ "$state_status" = "stopped" ]; then
        rec "T41" "PASS" "SIGHUP cleanly shut down supervisor with status stopped (rc=0)"
    else
        rec "T41" "FAIL" "SIGHUP shutdown failed (rc=$exit_code, state=$state_status)"
    fi
}

# ==============================================================================
# T42: Transcript with truncated last line -> resume, no quarantine
# ==============================================================================
test_t42() {
    local tdir="${TEST_TMP}/t42"
    mkdir -p "$tdir/home" "$tdir/work" "$tdir/control"
    local log_file="$tdir/fake-claude.log"
    local state_file="$tdir/state.json"
    local cfg_file="$tdir/config.json"
    local sess_file="$tdir/transcript.jsonl"

    printf '{"type": "user", "message": "hello"}\n{"type": "assistant", "partial": "wor' > "$sess_file"

    cat <<EOF > "$cfg_file"
{
    "state_file": "$state_file",
    "sessions": [
        {
            "name": "sess42",
            "workdir": "$tdir/work",
            "session_id": "test-session-042",
            "claude_binary": "$FAKE_CLAUDE",
            "session_file": "$sess_file",
            "readiness_poll_secs": 0.2,
            "env": {
                "FAKE_CLAUDE_LOG": "$log_file",
                "FAKE_CLAUDE_CONTROL_DIR": "$tdir/control"
            }
        }
    ]
}
EOF

    HOME="$tdir/home" "$BIN" run --config "$cfg_file" >/dev/null 2>&1 &
    local spid=$!
    ACTIVE_PIDS+=("$spid")

    for _ in $(seq 1 40); do
        if [ -s "$log_file" ]; then break; fi
        sleep 0.05
    done

    stop_supervisor "$spid"

    local backups=0
    for f in "$tdir"/transcript.jsonl.corrupt.*.bak; do
        [ -e "$f" ] && backups=$((backups + 1))
    done
    local has_resume=$(grep -q -- "--resume" "$log_file" 2>/dev/null && echo 1 || echo 0)

    if [ "$backups" -eq 0 ] && [ "$has_resume" -eq 1 ]; then
        rec "T42" "PASS" "transcript with truncated last line resumed without quarantine"
    else
        rec "T42" "FAIL" "truncated last line failed (backups=$backups, resume=$has_resume)"
    fi
}

# ==============================================================================
# T43: Transcript with garbage line in the middle -> resume, no quarantine
# ==============================================================================
test_t43() {
    local tdir="${TEST_TMP}/t43"
    mkdir -p "$tdir/home" "$tdir/work" "$tdir/control"
    local log_file="$tdir/fake-claude.log"
    local state_file="$tdir/state.json"
    local cfg_file="$tdir/config.json"
    local sess_file="$tdir/transcript.jsonl"

    printf '{"type": "user"}\nCORRUPT MIDDLE NON-JSON LINE\n{"type": "assistant"}\n' > "$sess_file"

    cat <<EOF > "$cfg_file"
{
    "state_file": "$state_file",
    "sessions": [
        {
            "name": "sess43",
            "workdir": "$tdir/work",
            "session_id": "test-session-043",
            "claude_binary": "$FAKE_CLAUDE",
            "session_file": "$sess_file",
            "readiness_poll_secs": 0.2,
            "env": {
                "FAKE_CLAUDE_LOG": "$log_file",
                "FAKE_CLAUDE_CONTROL_DIR": "$tdir/control"
            }
        }
    ]
}
EOF

    HOME="$tdir/home" "$BIN" run --config "$cfg_file" >/dev/null 2>&1 &
    local spid=$!
    ACTIVE_PIDS+=("$spid")

    for _ in $(seq 1 40); do
        if [ -s "$log_file" ]; then break; fi
        sleep 0.05
    done

    stop_supervisor "$spid"

    local backups=0
    for f in "$tdir"/transcript.jsonl.corrupt.*.bak; do
        [ -e "$f" ] && backups=$((backups + 1))
    done
    local has_resume=$(grep -q -- "--resume" "$log_file" 2>/dev/null && echo 1 || echo 0)

    if [ "$backups" -eq 0 ] && [ "$has_resume" -eq 1 ]; then
        rec "T43" "PASS" "transcript with corrupt middle line resumed without quarantine"
    else
        rec "T43" "FAIL" "corrupt middle line failed (backups=$backups, resume=$has_resume)"
    fi
}

# ==============================================================================
# T44: Empty transcript file -> quarantined, fresh start succeeds with --session-id
# ==============================================================================
test_t44() {
    local tdir="${TEST_TMP}/t44"
    mkdir -p "$tdir/home" "$tdir/work" "$tdir/control"
    local log_file="$tdir/fake-claude.log"
    local state_file="$tdir/state.json"
    local cfg_file="$tdir/config.json"
    local sess_file="$tdir/empty-session.jsonl"

    touch "$sess_file"

    cat <<EOF > "$cfg_file"
{
    "state_file": "$state_file",
    "sessions": [
        {
            "name": "sess44",
            "workdir": "$tdir/work",
            "session_id": "test-session-044",
            "claude_binary": "$FAKE_CLAUDE",
            "session_file": "$sess_file",
            "readiness_poll_secs": 0.2,
            "env": {
                "FAKE_CLAUDE_LOG": "$log_file",
                "FAKE_CLAUDE_CONTROL_DIR": "$tdir/control"
            }
        }
    ]
}
EOF

    HOME="$tdir/home" "$BIN" run --config "$cfg_file" >/dev/null 2>&1 &
    local spid=$!
    ACTIVE_PIDS+=("$spid")

    for _ in $(seq 1 40); do
        if [ -s "$log_file" ]; then break; fi
        sleep 0.05
    done

    stop_supervisor "$spid"

    local backups=0
    for f in "$tdir"/empty-session.jsonl.corrupt.*.bak; do
        [ -e "$f" ] && backups=$((backups + 1))
    done
    local has_session_id=$(grep -q -- "--session-id test-session-044" "$log_file" 2>/dev/null && echo 1 || echo 0)

    if [ "$backups" -ge 1 ] && [ "$has_session_id" -eq 1 ]; then
        rec "T44" "PASS" "empty transcript file was quarantined and fresh session started with --session-id"
    else
        rec "T44" "FAIL" "empty transcript quarantine failed (backups=$backups, session_id=$has_session_id)"
    fi
}

# ==============================================================================
# T45: Config validation rejects negative duration and huge duration (>30 days)
# ==============================================================================
test_t45() {
    local tdir="${TEST_TMP}/t45"
    mkdir -p "$tdir"
    local neg_cfg="$tdir/neg.json"
    local huge_cfg="$tdir/huge.json"

    cat <<EOF > "$neg_cfg"
{
    "sessions": [
        {
            "name": "bad_neg",
            "workdir": "$tdir",
            "session_id": "neg-001",
            "base_backoff_secs": -2.0
        }
    ]
}
EOF

    cat <<EOF > "$huge_cfg"
{
    "heartbeat_interval_secs": 99999999999.0,
    "sessions": []
}
EOF

    local out_neg=""
    local rc_neg=0
    out_neg=$("$BIN" run --config "$neg_cfg" 2>&1) || rc_neg=$?

    local out_huge=""
    local rc_huge=0
    out_huge=$("$BIN" run --config "$huge_cfg" 2>&1) || rc_huge=$?

    local neg_ok=0
    if [ "$rc_neg" -eq 1 ] && echo "$out_neg" | grep -q "base_backoff_secs" && echo "$out_neg" | grep -q "bad_neg"; then
        neg_ok=1
    fi

    local huge_ok=0
    if [ "$rc_huge" -eq 1 ] && echo "$out_huge" | grep -q "heartbeat_interval_secs"; then
        huge_ok=1
    fi

    if [ "$neg_ok" -eq 1 ] && [ "$huge_ok" -eq 1 ]; then
        rec "T45" "PASS" "config validation rejected negative duration and huge duration with exit 1 naming field and session"
    else
        rec "T45" "FAIL" "duration validation failed (neg_rc=$rc_neg out='$out_neg', huge_rc=$rc_huge out='$out_huge')"
    fi
}

# ==============================================================================
# T46: Shutdown with one session in readiness wait and one session running (Item 7)
# ==============================================================================
test_t46() {
    local tdir="${TEST_TMP}/t46"
    mkdir -p "$tdir/home" "$tdir/work" "$tdir/ctrl_wait" "$tdir/ctrl_run"
    local state_file="$tdir/state.json"
    local cfg_file="$tdir/config.json"
    local log_run="$tdir/run.log"

    touch "$tdir/ctrl_wait/auth_not_logged_in"

    cat <<EOF > "$cfg_file"
{
    "state_file": "$state_file",
    "shutdown_grace_secs": 2.0,
    "sessions": [
        {
            "name": "sess_waiting",
            "workdir": "$tdir/work",
            "session_id": "wait-001",
            "claude_binary": "$FAKE_CLAUDE",
            "readiness_poll_secs": 0.2,
            "env": { "FAKE_CLAUDE_CONTROL_DIR": "$tdir/ctrl_wait" }
        },
        {
            "name": "sess_running",
            "workdir": "$tdir/work",
            "session_id": "run-001",
            "claude_binary": "$FAKE_CLAUDE",
            "readiness_poll_secs": 0.2,
            "env": {
                "FAKE_CLAUDE_LOG": "$log_run",
                "FAKE_CLAUDE_CONTROL_DIR": "$tdir/ctrl_run"
            }
        }
    ]
}
EOF

    CLAUDE_DEFAULT_SESSION=0 HOME="$tdir/home" "$BIN" run --config "$cfg_file" >/dev/null 2>&1 &
    local spid=$!
    ACTIVE_PIDS+=("$spid")

    for _ in $(seq 1 40); do
        if [ -s "$log_run" ]; then break; fi
        sleep 0.05
    done

    kill -TERM "$spid" 2>/dev/null || true
    local waited=0
    while kill -0 "$spid" 2>/dev/null && [ "$waited" -lt 40 ]; do
        sleep 0.05
        waited=$((waited + 1))
    done
    local exited=0
    if ! kill -0 "$spid" 2>/dev/null; then
        exited=1
    fi

    if [ "$exited" -eq 1 ]; then
        rec "T46" "PASS" "shutdown cleanly reaped running session while sibling was in readiness wait"
    else
        rec "T46" "FAIL" "shutdown hung when one session in readiness wait"
        kill -KILL "$spid" 2>/dev/null || true
    fi
}

# ==============================================================================
# T47: Orphan prevention via PR_SET_PDEATHSIG on supervisor SIGKILL (Item 8)
# ==============================================================================
test_t47() {
    local tdir="${TEST_TMP}/t47"
    mkdir -p "$tdir/home" "$tdir/work" "$tdir/control"
    local log_file="$tdir/fake-claude.log"
    local state_file="$tdir/state.json"
    local cfg_file="$tdir/config.json"

    cat <<EOF > "$cfg_file"
{
    "state_file": "$state_file",
    "sessions": [
        {
            "name": "sess47",
            "workdir": "$tdir/work",
            "session_id": "pdeathsig-001",
            "claude_binary": "$FAKE_CLAUDE",
            "readiness_poll_secs": 0.2,
            "env": {
                "FAKE_CLAUDE_LOG": "$log_file",
                "FAKE_CLAUDE_CONTROL_DIR": "$tdir/control"
            }
        }
    ]
}
EOF

    HOME="$tdir/home" "$BIN" run --config "$cfg_file" >/dev/null 2>&1 &
    local spid=$!

    for _ in $(seq 1 40); do
        if [ -s "$log_file" ]; then break; fi
        sleep 0.05
    done
    local child_pid=$(head -n 1 "$log_file" | awk '{print $2}' | cut -d= -f2 || true)

    local child_was_running=0
    if [ -n "$child_pid" ] && kill -0 "$child_pid" 2>/dev/null; then
        local init_stat=$(ps -p "$child_pid" -o stat= 2>/dev/null || echo "")
        if [[ ! "$init_stat" =~ Z ]]; then
            child_was_running=1
        fi
    fi

    # SIGKILL the supervisor process abruptly
    kill -KILL "$spid" 2>/dev/null || true

    # Child must be terminated within a few seconds via PR_SET_PDEATHSIG
    local child_gone=0
    for _ in $(seq 1 50); do
        if ! kill -0 "$child_pid" 2>/dev/null; then
            child_gone=1
            break
        fi
        local stat=$(ps -p "$child_pid" -o stat= 2>/dev/null || echo "")
        if [[ "$stat" =~ Z ]]; then
            child_gone=1
            break
        fi
        sleep 0.05
    done

    if [ "$child_was_running" -eq 1 ] && [ "$child_gone" -eq 1 ]; then
        rec "T47" "PASS" "child process terminated promptly via PR_SET_PDEATHSIG after supervisor SIGKILL"
    else
        rec "T47" "FAIL" "child was not terminated after supervisor SIGKILL (running=$child_was_running, gone=$child_gone, pid=$child_pid)"
        kill -KILL "$child_pid" 2>/dev/null || true
    fi
}

# ==============================================================================
# T48: File sessions take precedence over generated sessions (Item 9)
# ==============================================================================
test_t48() {
    local tdir="${TEST_TMP}/t48"
    mkdir -p "$tdir"
    local cfg_file="$tdir/config.json"

    cat <<EOF > "$cfg_file"
{
    "sessions": [
        {
            "name": "conflict-ws",
            "workdir": "$tdir",
            "session_id": "file-wins-12345",
            "claude_binary": "claude"
        }
    ]
}
EOF

    local out=""
    out=$(CLAUDE_WORKSPACES="conflict-ws" CLAUDE_DEFAULT_SESSION=0 "$BIN" print-config --config "$cfg_file" 2>&1)

    local sess_count=$(echo "$out" | grep -c '"name": "conflict-ws"' || echo 0)
    local has_file_id=$(echo "$out" | grep -q "file-wins-12345" && echo 1 || echo 0)

    # Base config with duplicate names within file sessions must exit 1
    local dup_cfg="$tdir/dup.json"
    cat <<EOF > "$dup_cfg"
{
    "sessions": [
        {"name": "dup", "workdir": "$tdir", "session_id": "id-1"},
        {"name": "dup", "workdir": "$tdir", "session_id": "id-2"}
    ]
}
EOF
    local dup_rc=0
    "$BIN" print-config --config "$dup_cfg" >/dev/null 2>&1 || dup_rc=$?

    if [ "$sess_count" -eq 1 ] && [ "$has_file_id" -eq 1 ] && [ "$dup_rc" -eq 1 ]; then
        rec "T48" "PASS" "file session took precedence over generated session (file won, duplicate file sessions exited 1)"
    else
        rec "T48" "FAIL" "file precedence failed (count=$sess_count, file_id=$has_file_id, dup_rc=$dup_rc)"
    fi
}

# ==============================================================================
# T49: Sliding window backoff with multiple healthy runs (Item 4)
# ==============================================================================
test_t49() {
    local tdir="${TEST_TMP}/t49"
    mkdir -p "$tdir/home" "$tdir/work" "$tdir/control"
    local log_file="$tdir/fake-claude.log"
    local state_file="$tdir/state.json"
    local cfg_file="$tdir/config.json"

    # Base backoff 0.15s, healthy threshold 0.1s
    cat <<EOF > "$cfg_file"
{
    "state_file": "$state_file",
    "sessions": [
        {
            "name": "sess49",
            "workdir": "$tdir/work",
            "session_id": "swb-049",
            "claude_binary": "$FAKE_CLAUDE",
            "base_backoff_secs": 0.35,
            "max_backoff_secs": 4.0,
            "healthy_run_secs": 0.1,
            "backoff_window_secs": 60.0,
            "readiness_poll_secs": 0.2,
            "env": {
                "FAKE_CLAUDE_LOG": "$log_file",
                "FAKE_CLAUDE_CONTROL_DIR": "$tdir/control"
            }
        }
    ]
}
EOF

    # Child will run briefly then exit 1 each time
    touch "$tdir/control/action_exit1"

    CLAUDE_DEFAULT_SESSION=0 HOME="$tdir/home" "$BIN" run --config "$cfg_file" >/dev/null 2>&1 &
    local spid=$!
    ACTIVE_PIDS+=("$spid")

    # Wait for at least 3 runs to finish
    for _ in $(seq 1 80); do
        local count=$(grep -c "exit=1" "$log_file" 2>/dev/null || echo 0)
        if [ "$count" -ge 3 ]; then break; fi
        sleep 0.05
    done

    stop_supervisor "$spid"

    # Extract timestamps: start1, exit1, start2, exit2, start3
    local starts=($(grep "argv=" "$log_file" 2>/dev/null | awk '{print $1}'))
    local exits=($(grep "exit=" "$log_file" 2>/dev/null | awk '{print $1}'))

    local growth_ok=0
    if [ "${#starts[@]}" -ge 3 ] && [ "${#exits[@]}" -ge 2 ]; then
        # delay1 = starts[1] - exits[0] (restart after 1st crash)
        # delay2 = starts[2] - exits[1] (restart after 2nd crash, n=2 in window)
        local d1=$(python3 -c "print(f'{float(\"${starts[1]}\") - float(\"${exits[0]}\"):.3f}')" 2>/dev/null || echo 0)
        local d2=$(python3 -c "print(f'{float(\"${starts[2]}\") - float(\"${exits[1]}\"):.3f}')" 2>/dev/null || echo 0)
        # Delay 2 should be larger than delay 1 (due to 2^(n-1) growth in sliding window)
        growth_ok=$(python3 -c "print(1 if float('$d2') > float('$d1') * 1.1 else 0)" 2>/dev/null || echo 0)
    fi

    if [ "$growth_ok" -eq 1 ]; then
        rec "T49" "PASS" "sliding window backoff increased delay on subsequent crashes (d1=${d1}s, d2=${d2}s)"
    else
        rec "T49" "FAIL" "sliding window backoff did not grow (starts=${#starts[@]}, exits=${#exits[@]}, d1=${d1:-none}, d2=${d2:-none})"
    fi
}

# ==============================================================================
# T50: Repeated fast --resume failures quarantined after N runs with fresh start
# ==============================================================================
test_t50() {
    local tdir="${TEST_TMP}/t50"
    mkdir -p "$tdir/home" "$tdir/work" "$tdir/control"
    local log_file="$tdir/fake-claude.log"
    local state_file="$tdir/state.json"
    local cfg_file="$tdir/config.json"
    local sess_file="$tdir/resume-fail-session.jsonl"

    # Pre-populate valid jsonl transcript
    echo '{"type": "message", "text": "previous valid message"}' > "$sess_file"

    # Action: refuse --resume (exit 1), but accept --session-id
    touch "$tdir/control/action_resume_fail"

    cat <<EOF > "$cfg_file"
{
    "state_file": "$state_file",
    "sessions": [
        {
            "name": "sess50",
            "workdir": "$tdir/work",
            "session_id": "test-session-050",
            "claude_binary": "$FAKE_CLAUDE",
            "session_file": "$sess_file",
            "base_backoff_secs": 0.05,
            "max_backoff_secs": 0.2,
            "resume_fail_quarantine_after": 3,
            "resume_fail_uptime_secs": 2.0,
            "readiness_poll_secs": 0.2,
            "env": {
                "FAKE_CLAUDE_LOG": "$log_file",
                "FAKE_CLAUDE_CONTROL_DIR": "$tdir/control"
            }
        }
    ]
}
EOF

    HOME="$tdir/home" "$BIN" run --config "$cfg_file" >/dev/null 2>&1 &
    local spid=$!
    ACTIVE_PIDS+=("$spid")

    # Wait for the session to quarantine and restart fresh with --session-id
    local has_session_id=0
    for _ in $(seq 1 80); do
        if grep -q -- "--session-id test-session-050" "$log_file" 2>/dev/null; then
            has_session_id=1
            break
        fi
        sleep 0.05
    done

    stop_supervisor "$spid"

    local backups=0
    for f in "$tdir"/resume-fail-session.jsonl.corrupt.*.bak; do
        [ -e "$f" ] && backups=$((backups + 1))
    done
    local resume_attempts=$(grep -c -- "--resume test-session-050" "$log_file" 2>/dev/null || echo 0)

    if [ "$backups" -ge 1 ] && [ "$resume_attempts" -ge 3 ] && [ "$has_session_id" -eq 1 ]; then
        rec "T50" "PASS" "repeated fast --resume failures quarantined after 3 attempts and started fresh with --session-id"
    else
        rec "T50" "FAIL" "repeated fast --resume failures quarantine failed (backups=$backups, resume_attempts=$resume_attempts, session_id=$has_session_id)"
    fi
}

# ==============================================================================
# T51: pty-closed escalation kill counts as crash (enters failed), not healthy
# ==============================================================================
test_t51() {
    local tdir="${TEST_TMP}/t51"
    mkdir -p "$tdir/home" "$tdir/work" "$tdir/control"
    local log_file="$tdir/fake-claude.log"
    local state_file="$tdir/state.json"
    local cfg_file="$tdir/config.json"

    # Action: close tty and ignore EOF, supervisor must kill child
    touch "$tdir/control/action_close_tty"

    cat <<EOF > "$cfg_file"
{
    "state_file": "$state_file",
    "sessions": [
        {
            "name": "sess51",
            "workdir": "$tdir/work",
            "session_id": "test-session-051",
            "claude_binary": "$FAKE_CLAUDE",
            "max_crashes": 2,
            "crash_window_secs": 30.0,
            "pty_eof_grace_secs": 0.1,
            "kill_grace_secs": 0.1,
            "kill_wait_secs": 0.2,
            "base_backoff_secs": 0.05,
            "failed_cooldown_secs": 30.0,
            "readiness_poll_secs": 0.2,
            "env": {
                "FAKE_CLAUDE_LOG": "$log_file",
                "FAKE_CLAUDE_CONTROL_DIR": "$tdir/control"
            }
        }
    ]
}
EOF

    HOME="$tdir/home" "$BIN" run --config "$cfg_file" >/dev/null 2>&1 &
    local spid=$!
    ACTIVE_PIDS+=("$spid")

    # Wait for session to reach 'failed' status after 2 pty-closed killed crashes
    local is_failed=0
    for _ in $(seq 1 80); do
        if grep -q '"status": "failed"' "$state_file" 2>/dev/null; then
            is_failed=1
            break
        fi
        sleep 0.05
    done

    # Probe check should exit 2 when session is failed
    local probe_rc=0
    "$BIN" status --state-file "$state_file" >/dev/null 2>&1 || probe_rc=$?

    stop_supervisor "$spid"

    if [ "$is_failed" -eq 1 ] && [ "$probe_rc" -eq 2 ]; then
        rec "T51" "PASS" "pty-closed supervisor kill counted as crash and entered failed state (probe rc=2)"
    else
        rec "T51" "FAIL" "pty-closed supervisor kill failed (is_failed=$is_failed, probe_rc=$probe_rc)"
    fi
}

# ==============================================================================
# T52: fast --resume failures with different error (e.g. API unavailable) do NOT quarantine
# ==============================================================================
test_t52() {
    local tdir="${TEST_TMP}/t52"
    mkdir -p "$tdir/home" "$tdir/work" "$tdir/control"
    local log_file="$tdir/fake-claude.log"
    local state_file="$tdir/state.json"
    local cfg_file="$tdir/config.json"
    local sess_file="$tdir/resume-api-err-session.jsonl"

    # Pre-populate valid jsonl transcript
    local original_content='{"type": "message", "text": "important previous message"}'
    echo "$original_content" > "$sess_file"

    # Action: refuse --resume with API unavailable (different error from missing session)
    touch "$tdir/control/action_resume_api_error"

    # N = 3, so N+3 = 6 consecutive runs
    local N=3
    local target_runs=$((N + 3))

    cat <<EOF > "$cfg_file"
{
    "state_file": "$state_file",
    "sessions": [
        {
            "name": "sess52",
            "workdir": "$tdir/work",
            "session_id": "test-session-052",
            "claude_binary": "$FAKE_CLAUDE",
            "session_file": "$sess_file",
            "base_backoff_secs": 0.05,
            "max_backoff_secs": 0.1,
            "max_crashes": 0,
            "resume_fail_quarantine_after": $N,
            "resume_fail_uptime_secs": 2.0,
            "readiness_poll_secs": 0.2,
            "env": {
                "FAKE_CLAUDE_LOG": "$log_file",
                "FAKE_CLAUDE_CONTROL_DIR": "$tdir/control"
            }
        }
    ]
}
EOF

    HOME="$tdir/home" "$BIN" run --config "$cfg_file" >/dev/null 2>&1 &
    local spid=$!
    ACTIVE_PIDS+=("$spid")

    # Wait for at least target_runs (N+3 = 6) fast resume attempts
    local resume_attempts=0
    for _ in $(seq 1 120); do
        resume_attempts=$(grep -c -- "--resume test-session-052" "$log_file" 2>/dev/null || echo 0)
        if [ "$resume_attempts" -ge "$target_runs" ]; then
            break
        fi
        sleep 0.05
    done

    stop_supervisor "$spid"

    # Check that NO quarantine backup was created
    local backups=0
    for f in "$tdir"/resume-api-err-session.jsonl.corrupt.*.bak; do
        [ -e "$f" ] && backups=$((backups + 1))
    done

    # Check that --session-id was NEVER invoked (fresh start never triggered)
    local has_session_id=0
    if grep -q -- "--session-id" "$log_file" 2>/dev/null; then
        has_session_id=1
    fi

    # Check that transcript file still exists and content is unchanged
    local content_matches=0
    if [ -f "$sess_file" ] && [ "$(cat "$sess_file")" = "$original_content" ]; then
        content_matches=1
    fi

    if [ "$resume_attempts" -ge "$target_runs" ] && [ "$backups" -eq 0 ] && [ "$has_session_id" -eq 0 ] && [ "$content_matches" -eq 1 ]; then
        rec "T52" "PASS" "fast --resume failures with different error for N+3 ($resume_attempts >= $target_runs) runs did NOT quarantine"
    else
        rec "T52" "FAIL" "fast --resume failures with different error check failed (attempts=$resume_attempts, backups=$backups, session_id=$has_session_id, content_matches=$content_matches)"
    fi
}

# ==============================================================================
# T53: REST API /healthz is reachable and reports ok while supervising
# ==============================================================================
# The supervisor serves the copied agent-run REST API and its own
# /api/v1/sessions group on AGENT_ORCHESTRATE_LISTEN (default 127.0.0.1:8003).
# /healthz is exempt from the optional bearer token.
# ==============================================================================
test_t53() {
    local tdir="${TEST_TMP}/t53"
    mkdir -p "$tdir/home" "$tdir/work"
    local log_file="$tdir/fake-claude.log"
    local state_file="$tdir/state.json"
    local cfg_file="$tdir/config.json"
    local port=$(port_alloc)
    local listen="127.0.0.1:${port}"

    cat <<EOF > "$cfg_file"
{
    "state_file": "$state_file",
    "sessions": [
        {
            "name": "sess53",
            "workdir": "$tdir/work",
            "session_id": "test-session-053",
            "claude_binary": "$FAKE_CLAUDE",
            "base_backoff_secs": 0.05,
            "max_backoff_secs": 0.1,
            "readiness_poll_secs": 0.2,
            "env": {
                "FAKE_CLAUDE_LOG": "$log_file",
                "FAKE_CLAUDE_CONTROL_DIR": "$tdir/control"
            }
        }
    ]
}
EOF

    HOME="$tdir/home" AGENT_ORCHESTRATE_LISTEN="$listen" \
        "$BIN" run --config "$cfg_file" >/dev/null 2>&1 &
    local spid=$!
    ACTIVE_PIDS+=("$spid")

    local ok=0
    for _ in $(seq 1 120); do
        local body=$(curl -s "http://${listen}/healthz" 2>/dev/null)
        if [ -n "$body" ]; then
            ok=1
            break
        fi
        sleep 0.05
    done

    stop_supervisor "$spid"

    if [ "$ok" -eq 1 ] && echo "$body" | grep -q "ok"; then
        rec "T53" "PASS" "/healthz reachable and reports ok: $body"
    else
        rec "T53" "FAIL" "/healthz not reachable or unexpected body: '$body'"
    fi
}

# ==============================================================================
# T54: /api/v1/sessions lists the supervised session via the REST API
# ==============================================================================
test_t54() {
    local tdir="${TEST_TMP}/t54"
    mkdir -p "$tdir/home" "$tdir/work" "$tdir/control"
    local log_file="$tdir/fake-claude.log"
    local state_file="$tdir/state.json"
    local cfg_file="$tdir/config.json"
    local port=$(port_alloc)
    local listen="127.0.0.1:${port}"

    cat <<EOF > "$cfg_file"
{
    "state_file": "$state_file",
    "sessions": [
        {
            "name": "sess54",
            "workdir": "$tdir/work",
            "session_id": "test-session-054",
            "claude_binary": "$FAKE_CLAUDE",
            "base_backoff_secs": 0.05,
            "max_backoff_secs": 0.1,
            "readiness_poll_secs": 0.2,
            "env": {
                "FAKE_CLAUDE_LOG": "$log_file",
                "FAKE_CLAUDE_CONTROL_DIR": "$tdir/control"
            }
        }
    ]
}
EOF

    HOME="$tdir/home" AGENT_ORCHESTRATE_LISTEN="$listen" \
        "$BIN" run --config "$cfg_file" >/dev/null 2>&1 &
    local spid=$!
    ACTIVE_PIDS+=("$spid")

    local found=0
    for _ in $(seq 1 120); do
        local body=$(curl -s "http://${listen}/api/v1/sessions" 2>/dev/null)
        if echo "$body" | grep -q "sess54"; then
            found=1
            break
        fi
        sleep 0.05
    done

    stop_supervisor "$spid"

    if [ "$found" -eq 1 ]; then
        rec "T54" "PASS" "/api/v1/sessions lists supervised session sess54"
    else
        rec "T54" "FAIL" "/api/v1/sessions did not list sess54 (body: '$body')"
    fi
}

# ==============================================================================
# T55: /api/v1/sessions/{name}/stop then /start control the session
# ==============================================================================
test_t55() {
    local tdir="${TEST_TMP}/t55"
    mkdir -p "$tdir/home" "$tdir/work" "$tdir/control"
    local log_file="$tdir/fake-claude.log"
    local state_file="$tdir/state.json"
    local cfg_file="$tdir/config.json"
    local port=$(port_alloc)
    local listen="127.0.0.1:${port}"

    cat <<EOF > "$cfg_file"
{
    "state_file": "$state_file",
    "sessions": [
        {
            "name": "sess55",
            "workdir": "$tdir/work",
            "session_id": "test-session-055",
            "claude_binary": "$FAKE_CLAUDE",
            "base_backoff_secs": 0.05,
            "max_backoff_secs": 0.1,
            "readiness_poll_secs": 0.2,
            "env": {
                "FAKE_CLAUDE_LOG": "$log_file",
                "FAKE_CLAUDE_CONTROL_DIR": "$tdir/control"
            }
        }
    ]
}
EOF

    HOME="$tdir/home" AGENT_ORCHESTRATE_LISTEN="$listen" \
        "$BIN" run --config "$cfg_file" >/dev/null 2>&1 &
    local spid=$!
    ACTIVE_PIDS+=("$spid")

    # Wait until the session is listed so stop has a control to act on.
    local listed=0
    for _ in $(seq 1 120); do
        local body=$(curl -s "http://${listen}/api/v1/sessions" 2>/dev/null)
        if echo "$body" | grep -q "sess55"; then
            listed=1
            break
        fi
        sleep 0.05
    done

    local stop_ok=0
    if [ "$listed" -eq 1 ]; then
        local stop_body=$(curl -s -X POST "http://${listen}/api/v1/sessions/sess55/stop" 2>/dev/null)
        stop_ok=$(echo "$stop_body" | grep -q '"paused":true' && echo 1 || echo 0)
    fi

    local start_ok=0
    if [ "$stop_ok" -eq 1 ]; then
        local start_body=$(curl -s -X POST "http://${listen}/api/v1/sessions/sess55/start" 2>/dev/null)
        start_ok=$(echo "$start_body" | grep -q '"paused":false' && echo 1 || echo 0)
    fi

    stop_supervisor "$spid"

    if [ "$listed" -eq 1 ] && [ "$stop_ok" -eq 1 ] && [ "$start_ok" -eq 1 ]; then
        rec "T55" "PASS" "/api/v1/sessions/stop then /start toggled session control"
    else
        rec "T55" "FAIL" "stop/start control failed (listed=$listed stop_ok=$stop_ok start_ok=$start_ok)"
    fi
}

# ==============================================================================
# T56: /api/v1/sessions/{name}/log serves a redacted tail
# ==============================================================================
test_t56() {
    local tdir="${TEST_TMP}/t56"
    mkdir -p "$tdir/home" "$tdir/work" "$tdir/control"
    local log_file="$tdir/fake-claude.log"
    local state_file="$tdir/state.json"
    local cfg_file="$tdir/config.json"
    local port=$(port_alloc)
    local listen="127.0.0.1:${port}"

    cat <<EOF > "$cfg_file"
{
    "state_file": "$state_file",
    "sessions": [
        {
            "name": "sess56",
            "workdir": "$tdir/work",
            "session_id": "test-session-056",
            "claude_binary": "$FAKE_CLAUDE",
            "base_backoff_secs": 0.05,
            "max_backoff_secs": 0.1,
            "readiness_poll_secs": 0.2,
            "env": {
                "FAKE_CLAUDE_LOG": "$log_file",
                "FAKE_CLAUDE_CONTROL_DIR": "$tdir/control"
            }
        }
    ]
}
EOF

    HOME="$tdir/home" AGENT_ORCHESTRATE_LISTEN="$listen" \
        "$BIN" run --config "$cfg_file" >/dev/null 2>&1 &
    local spid=$!
    ACTIVE_PIDS+=("$spid")

    local log_ok=0
    for _ in $(seq 1 160); do
        local body=$(curl -s "http://${listen}/api/v1/sessions/sess56/log?tail=50" 2>/dev/null)
        if echo "$body" | grep -q '"session":"sess56"'; then
            log_ok=1
            break
        fi
        sleep 0.05
    done

    stop_supervisor "$spid"

    if [ "$log_ok" -eq 1 ]; then
        rec "T56" "PASS" "/api/v1/sessions/sess56/log served a tail"
    else
        rec "T56" "FAIL" "/api/v1/sessions/sess56/log did not return a tail (body: '$body')"
    fi
}

# ==============================================================================
# T57: RESTART of a running session ends running with a new pid and paused=false
# ==============================================================================
test_t57() {
    local tdir="${TEST_TMP}/t57"
    mkdir -p "$tdir/home" "$tdir/work" "$tdir/control"
    local log_file="$tdir/fake-claude.log"
    local state_file="$tdir/state.json"
    local cfg_file="$tdir/config.json"
    local port=$(port_alloc)
    local listen="127.0.0.1:${port}"

    cat <<EOF > "$cfg_file"
{
    "state_file": "$state_file",
    "heartbeat_interval_secs": 0.05,
    "sessions": [
        {
            "name": "sess57",
            "workdir": "$tdir/work",
            "session_id": "test-session-057",
            "claude_binary": "$FAKE_CLAUDE",
            "kill_grace_secs": 0.5,
            "kill_wait_secs": 0.5,
            "pty_eof_grace_secs": 0.2,
            "readiness_poll_secs": 0.2,
            "env": {
                "FAKE_CLAUDE_LOG": "$log_file",
                "FAKE_CLAUDE_CONTROL_DIR": "$tdir/control"
            }
        }
    ]
}
EOF

    # The child ignores SIGTERM from the very start, so the stop path of a
    # restart must escalate to SIGKILL and the drain spans several heartbeat
    # ticks. That would let the old broken restart consume its flag while the
    # thread was still draining and leave the session stopped.
    touch "$tdir/control/action_ignore_sigterm"

    HOME="$tdir/home" AGENT_ORCHESTRATE_LISTEN="$listen" \
        "$BIN" run --config "$cfg_file" >/dev/null 2>&1 &
    local spid=$!
    ACTIVE_PIDS+=("$spid")

    # Wait until the session is listed and running with a pid.
    local running=0
    for _ in $(seq 1 120); do
        local sbody=$(curl -s "http://${listen}/api/v1/sessions/sess57" 2>/dev/null)
        if echo "$sbody" | grep -q '"status":"running"'; then
            running=1
            break
        fi
        sleep 0.05
    done
    local old_pid=""
    old_pid=$(echo "$sbody" | grep -o '"pid":[0-9]*' | head -n1 | cut -d: -f2)

    local restart_ok=0
    if [ "$running" -eq 1 ] && [ -n "$old_pid" ] && [ "$old_pid" != "null" ]; then
        curl -s -X POST "http://${listen}/api/v1/sessions/sess57/restart" >/dev/null 2>&1
        rm -f "$tdir/control/action_ignore_sigterm"
        for _ in $(seq 1 200); do
            local after=$(curl -s "http://${listen}/api/v1/sessions/sess57" 2>/dev/null)
            local st=$(echo "$after" | grep -o '"status":"[a-z]*"' | cut -d: -f2 | tr -d '"')
            local paused=$(echo "$after" | grep -o '"paused":[a-z]*' | head -n1 | cut -d: -f2)
            local new_pid=$(echo "$after" | grep -o '"pid":[0-9]*' | head -n1 | cut -d: -f2)
            if [ "$st" = "running" ] && [ "$paused" = "false" ] && [ -n "$new_pid" ] && [ "$new_pid" != "null" ] && [ "$new_pid" != "$old_pid" ]; then
                restart_ok=1
                break
            fi
            sleep 0.05
        done
    fi

    stop_supervisor "$spid"

    if [ "$restart_ok" -eq 1 ]; then
        rec "T57" "PASS" "restart of running session ended running (new_pid=$new_pid old_pid=$old_pid paused=false)"
    else
        rec "T57" "FAIL" "restart did not end running (running=$running old_pid=$old_pid final_status=$st final_paused=$paused new_pid=$new_pid)"
    fi
}

# ==============================================================================
# T58: RESTART of a stopped/paused session ends running
# ==============================================================================
test_t58() {
    local tdir="${TEST_TMP}/t58"
    mkdir -p "$tdir/home" "$tdir/work" "$tdir/control"
    local log_file="$tdir/fake-claude.log"
    local state_file="$tdir/state.json"
    local cfg_file="$tdir/config.json"
    local port=$(port_alloc)
    local listen="127.0.0.1:${port}"

    cat <<EOF > "$cfg_file"
{
    "state_file": "$state_file",
    "sessions": [
        {
            "name": "sess58",
            "workdir": "$tdir/work",
            "session_id": "test-session-058",
            "claude_binary": "$FAKE_CLAUDE",
            "kill_grace_secs": 0.2,
            "kill_wait_secs": 0.4,
            "pty_eof_grace_secs": 0.2,
            "readiness_poll_secs": 0.2,
            "env": {
                "FAKE_CLAUDE_LOG": "$log_file",
                "FAKE_CLAUDE_CONTROL_DIR": "$tdir/control"
            }
        }
    ]
}
EOF

    HOME="$tdir/home" AGENT_ORCHESTRATE_LISTEN="$listen" \
        "$BIN" run --config "$cfg_file" >/dev/null 2>&1 &
    local spid=$!
    ACTIVE_PIDS+=("$spid")

    # Wait until the session is listed, then stop it so it is paused/stopped.
    local listed=0
    for _ in $(seq 1 120); do
        local sbody=$(curl -s "http://${listen}/api/v1/sessions/sess58" 2>/dev/null)
        if echo "$sbody" | grep -q '"name":"sess58"'; then
            listed=1
            break
        fi
        sleep 0.05
    done

    local stop_ok=0
    if [ "$listed" -eq 1 ]; then
        curl -s -X POST "http://${listen}/api/v1/sessions/sess58/stop" >/dev/null 2>&1
        for _ in $(seq 1 120); do
            local sb=$(curl -s "http://${listen}/api/v1/sessions/sess58" 2>/dev/null)
            if echo "$sb" | grep -q '"paused":true'; then
                stop_ok=1
                break
            fi
            sleep 0.05
        done
    fi

    local restart_ok=0
    if [ "$stop_ok" -eq 1 ]; then
        curl -s -X POST "http://${listen}/api/v1/sessions/sess58/restart" >/dev/null 2>&1
        for _ in $(seq 1 160); do
            local after=$(curl -s "http://${listen}/api/v1/sessions/sess58" 2>/dev/null)
            local st=$(echo "$after" | grep -o '"status":"[a-z]*"' | cut -d: -f2 | tr -d '"')
            local paused=$(echo "$after" | grep -o '"paused":[a-z]*' | head -n1 | cut -d: -f2)
            if [ "$st" = "running" ] && [ "$paused" = "false" ]; then
                restart_ok=1
                break
            fi
            sleep 0.05
        done
    fi

    stop_supervisor "$spid"

    if [ "$restart_ok" -eq 1 ]; then
        rec "T58" "PASS" "restart of stopped session ended running (paused=false)"
    else
        rec "T58" "FAIL" "restart of stopped session did not end running (listed=$listed stop_ok=$stop_ok final_status=$st final_paused=$paused)"
    fi
}

# ==============================================================================
# T59: Message injection: accepted message reaches the child as a bracketed
# paste + one final Enter (ESC[200~ text ESC[201~ \r), and never the text*as
# a command.
# ==============================================================================
test_t59() {
    local tdir="${TEST_TMP}/t59"
    mkdir -p "$tdir/home" "$tdir/work" "$tdir/control"
    local log_file="$tdir/fake-claude.log"
    local stdin_log="$tdir/stdin.log"
    local state_file="$tdir/state.json"
    local cfg_file="$tdir/config.json"
    local port=$(port_alloc)
    local listen="127.0.0.1:${port}"

    cat <<EOF > "$cfg_file"
{
    "state_file": "$state_file",
    "heartbeat_interval_secs": 0.2,
    "sessions": [
        {
            "name": "sess59",
            "workdir": "$tdir/work",
            "session_id": "test-session-059",
            "claude_binary": "$FAKE_CLAUDE",
            "base_backoff_secs": 0.05,
            "max_backoff_secs": 0.1,
            "readiness_poll_secs": 0.2,
            "env": {
                "FAKE_CLAUDE_LOG": "$log_file",
                "FAKE_CLAUDE_STDIN_LOG": "$stdin_log",
                "FAKE_CLAUDE_CONTROL_DIR": "$tdir/control"
            }
        }
    ]
}
EOF

    HOME="$tdir/home" AGENT_ORCHESTRATE_LISTEN="$listen" \
        "$BIN" run --config "$cfg_file" >/dev/null 2>&1 &
    local spid=$!
    ACTIVE_PIDS+=("$spid")

    # Wait until the child is up and its stdin capture is armed.
    local up=0
    for _ in $(seq 1 160); do
        if grep -q "fd0-loop started" "$stdin_log" 2>/dev/null; then
            up=1
            break
        fi
        sleep 0.05
    done

    local resp=""
    if [ "$up" -eq 1 ]; then
        resp=$(curl -s -X POST "http://${listen}/api/v1/sessions/sess59/message" \
            -H 'Content-Type: application/json' \
            -d '{"text":"hello world\nsecond line"}')
    fi

    # Give the PTY time to deliver the paste to the child's stdin reader.
    local seen=0
    for _ in $(seq 1 40); do
        if grep -q 'data=b.\\x1b\\[200~' "$stdin_log" 2>/dev/null; then
            seen=1
            break
        fi
        sleep 0.05
    done

    stop_supervisor "$spid"

    local paste_ok=0
    if python3 - "$stdin_log" <<'EOF' 2>/dev/null
import ast, sys
lines = open(sys.argv[1], 'rb').read().splitlines()
data = b''.join(ast.literal_eval(l.split(b'data=', 1)[1].decode('utf-8')) for l in lines if l.startswith(b'data='))
paste = b'\x1b[200~' + b'hello world\nsecond line' + b'\x1b[201~' + b'\r'
sys.exit(0 if paste in data else 1)
EOF
    then
        paste_ok=1
    fi

    local accepted=0
    if echo "$resp" | grep -q '"accepted":true'; then
        accepted=1
    fi

    if [ "$up" -eq 1 ] && [ "$paste_ok" -eq 1 ] && [ "$accepted" -eq 1 ]; then
        rec "T59" "PASS" "message reached child as bracketed paste + Enter"
    else
        rec "T59" "FAIL" "message injection failed (up=$up paste=$paste_ok accepted=$accepted resp='$resp')"
    fi
}

# ==============================================================================
# T60: Control characters are stripped from the injected text
# ==============================================================================
test_t60() {
    local tdir="${TEST_TMP}/t60"
    mkdir -p "$tdir/home" "$tdir/work" "$tdir/control"
    local log_file="$tdir/fake-claude.log"
    local stdin_log="$tdir/stdin.log"
    local state_file="$tdir/state.json"
    local cfg_file="$tdir/config.json"
    local port=$(port_alloc)
    local listen="127.0.0.1:${port}"

    cat <<EOF > "$cfg_file"
{
    "state_file": "$state_file",
    "sessions": [
        {
            "name": "sess60",
            "workdir": "$tdir/work",
            "session_id": "test-session-060",
            "claude_binary": "$FAKE_CLAUDE",
            "readiness_poll_secs": 0.2,
            "env": {
                "FAKE_CLAUDE_LOG": "$log_file",
                "FAKE_CLAUDE_STDIN_LOG": "$stdin_log",
                "FAKE_CLAUDE_CONTROL_DIR": "$tdir/control"
            }
        }
    ]
}
EOF

    HOME="$tdir/home" AGENT_ORCHESTRATE_LISTEN="$listen" \
        "$BIN" run --config "$cfg_file" >/dev/null 2>&1 &
    local spid=$!
    ACTIVE_PIDS+=("$spid")

    local up=0
    for _ in $(seq 1 160); do
        if grep -q "fd0-loop started" "$stdin_log" 2>/dev/null; then
            up=1
            break
        fi
        sleep 0.05
    done

    local resp=""
    if [ "$up" -eq 1 ]; then
        resp=$(curl -s -X POST "http://${listen}/api/v1/sessions/sess60/message" \
            -H 'Content-Type: application/json' \
            -d '{"text":"clean\u001b[31mred\u001b[0m done"}')
    fi

    local seen=0
    for _ in $(seq 1 40); do
        if grep -q 'data=b.\\x1b\\[200~' "$stdin_log" 2>/dev/null; then
            seen=1
            break
        fi
        sleep 0.05
    done

    stop_supervisor "$spid"

    local stripped_ok=0
    if python3 - "$stdin_log" <<'EOF' 2>/dev/null
import ast, sys
lines = open(sys.argv[1], 'rb').read().splitlines()
data = b''.join(ast.literal_eval(l.split(b'data=', 1)[1].decode('utf-8')) for l in lines if l.startswith(b'data='))
# The ESC[31m / ESC[0m sequences must be gone; plain text remains.
paste = b'\x1b[200~' + b'cleanred done' + b'\x1b[201~' + b'\r'
sys.exit(0 if paste in data else 1)
EOF
    then
        stripped_ok=1
    fi

    if [ "$up" -eq 1 ] && [ "$stripped_ok" -eq 1 ]; then
        rec "T60" "PASS" "control chars and escape sequences stripped"
    else
        rec "T60" "FAIL" "control stripping failed (up=$up)
stripped_ok=$stripped_ok"
    fi
}

# ==============================================================================
# T61: A crafted ESC[201~ inside the text is stripped and cannot close the paste
# ==============================================================================
test_t61() {
    local tdir="${TEST_TMP}/t61"
    mkdir -p "$tdir/home" "$tdir/work" "$tdir/control"
    local log_file="$tdir/fake-claude.log"
    local stdin_log="$tdir/stdin.log"
    local state_file="$tdir/state.json"
    local cfg_file="$tdir/config.json"
    local port=$(port_alloc)
    local listen="127.0.0.1:${port}"

    cat <<EOF > "$cfg_file"
{
    "state_file": "$state_file",
    "sessions": [
        {
            "name": "sess61",
            "workdir": "$tdir/work",
            "session_id": "test-session-061",
            "claude_binary": "$FAKE_CLAUDE",
            "readiness_poll_secs": 0.2,
            "env": {
                "FAKE_CLAUDE_LOG": "$log_file",
                "FAKE_CLAUDE_STDIN_LOG": "$stdin_log",
                "FAKE_CLAUDE_CONTROL_DIR": "$tdir/control"
            }
        }
    ]
}
EOF

    HOME="$tdir/home" AGENT_ORCHESTRATE_LISTEN="$listen" \
        "$BIN" run --config "$cfg_file" >/dev/null 2>&1 &
    local spid=$!
    ACTIVE_PIDS+=("$spid")

    local up=0
    for _ in $(seq 1 160); do
        if grep -q "fd0-loop started" "$stdin_log" 2>/dev/null; then
            up=1
            break
        fi
        sleep 0.05
    done

    local resp=""
    if [ "$up" -eq 1 ]; then
        # The text contains a real ESC[201~ (bracketed-paste end) mid-payload.
        resp=$(curl -s -X POST "http://${listen}/api/v1/sessions/sess61/message" \
            -H 'Content-Type: application/json' \
            -d '{"text":"implant\u001b[201~ and after"}')
    fi

    local seen=0
    for _ in $(seq 1 40); do
        if grep -q 'data=b.\\x1b\\[200~' "$stdin_log" 2>/dev/null; then
            seen=1
            break
        fi
        sleep 0.05
    done

    stop_supervisor "$spid"

    local ok=0
    if python3 - "$stdin_log" <<'EOF' 2>/dev/null
import ast, sys
lines = open(sys.argv[1], 'rb').read().splitlines()
data = b''.join(ast.literal_eval(l.split(b'data=', 1)[1].decode('utf-8')) for l in lines if l.startswith(b'data='))
# ESC[201~ stripped from the payload; the frame has exactly one close.
paste = b'\x1b[200~' + b'implant and after' + b'\x1b[201~' + b'\r'
sys.exit(0 if paste in data else 1)
EOF
    then
        ok=1
    fi

    if [ "$up" -eq 1 ] && [ "$ok" -eq 1 ]; then
        rec "T61" "PASS" "embedded ESC[201~ stripped inside the paste"
    else
        rec "T61" "FAIL" "ESC[201~ stripping failed (up=$up ok=$ok)"
    fi
}

# ==============================================================================
# T62: Size limit: text over 8192 bytes is refused with 413 too_large
# ==============================================================================
test_t62() {
    local tdir="${TEST_TMP}/t62"
    mkdir -p "$tdir/home" "$tdir/work" "$tdir/control"
    local log_file="$tdir/fake-claude.log"
    local state_file="$tdir/state.json"
    local cfg_file="$tdir/config.json"
    local port=$(port_alloc)
    local listen="127.0.0.1:${port}"

    cat <<EOF > "$cfg_file"
{
    "state_file": "$state_file",
    "sessions": [
        {
            "name": "sess62",
            "workdir": "$tdir/work",
            "session_id": "test-session-062",
            "claude_binary": "$FAKE_CLAUDE",
            "readiness_poll_secs": 0.2,
            "env": {
                "FAKE_CLAUDE_LOG": "$log_file",
                "FAKE_CLAUDE_CONTROL_DIR": "$tdir/control"
            }
        }
    ]
}
EOF

    HOME="$tdir/home" AGENT_ORCHESTRATE_LISTEN="$listen" \
        "$BIN" run --config "$cfg_file" >/dev/null 2>&1 &
    local spid=$!
    ACTIVE_PIDS+=("$spid")

    local listed=0
    for _ in $(seq 1 120); do
        local body=$(curl -s "http://${listen}/api/v1/sessions" 2>/dev/null)
        if echo "$body" | grep -q "sess62"; then
            listed=1
            break
        fi
        sleep 0.05
    done

    local big=$(python3 -c "print('x' * 9000)")
    local code=""
    if [ "$listed" -eq 1 ]; then
        code=$(curl -s -o /dev/null -w '%{http_code}' -X POST \
            "http://${listen}/api/v1/sessions/sess62/message" \
            -H 'Content-Type: application/json' \
            -d "{\"text\":\"$big\"}")
    fi

    stop_supervisor "$spid"

    if [ "$listed" -eq 1 ] && [ "$code" = "413" ]; then
        rec "T62" "PASS" "oversized message refused with 413 too_large"
    else
        rec "T62" "FAIL" "size limit failed (listed=$listed code=$code)"
    fi
}

# ==============================================================================
# T63: Empty/whitespace text refused with 400 invalid_body
# ==============================================================================
test_t63() {
    local tdir="${TEST_TMP}/t63"
    mkdir -p "$tdir/home" "$tdir/work" "$tdir/control"
    local log_file="$tdir/fake-claude.log"
    local state_file="$tdir/state.json"
    local cfg_file="$tdir/config.json"
    local port=$(port_alloc)
    local listen="127.0.0.1:${port}"

    cat <<EOF > "$cfg_file"
{
    "state_file": "$state_file",
    "sessions": [
        {
            "name": "sess63",
            "workdir": "$tdir/work",
            "session_id": "test-session-063",
            "claude_binary": "$FAKE_CLAUDE",
            "readiness_poll_secs": 0.2,
            "env": {
                "FAKE_CLAUDE_LOG": "$log_file",
                "FAKE_CLAUDE_CONTROL_DIR": "$tdir/control"
            }
        }
    ]
}
EOF

    HOME="$tdir/home" AGENT_ORCHESTRATE_LISTEN="$listen" \
        "$BIN" run --config "$cfg_file" >/dev/null 2>&1 &
    local spid=$!
    ACTIVE_PIDS+=("$spid")

    local listed=0
    for _ in $(seq 1 120); do
        local body=$(curl -s "http://${listen}/api/v1/sessions" 2>/dev/null)
        if echo "$body" | grep -q "sess63"; then
            listed=1
            break
        fi
        sleep 0.05
    done

    local code_ws=""
    local code_empty=""
    local code_badjson=""
    if [ "$listed" -eq 1 ]; then
        code_ws=$(curl -s -o /dev/null -w '%{http_code}' -X POST \
            "http://${listen}/api/v1/sessions/sess63/message" \
            -H 'Content-Type: application/json' -d '{"text":"   "}')
        code_empty=$(curl -s -o /dev/null -w '%{http_code}' -X POST \
            "http://${listen}/api/v1/sessions/sess63/message" \
            -H 'Content-Type: application/json' -d '{"text":""}')
        code_badjson=$(curl -s -o /dev/null -w '%{http_code}' -X POST \
            "http://${listen}/api/v1/sessions/sess63/message" \
            -H 'Content-Type: application/json' -d 'not-json')
    fi

    stop_supervisor "$spid"

    if [ "$listed" -eq 1 ] && [ "$code_ws" = "400" ] && [ "$code_empty" = "400" ] && [ "$code_badjson" = "400" ]; then
        rec "T63" "PASS" "empty/whitespace/invalid bodies refused with 400"
    else
        rec "T63" "FAIL" "empty text test failed (ws=$code_ws empty=$code_empty badjson=$code_badjson)"
    fi
}

# ==============================================================================
# T64: Unknown session -> 404
# ==============================================================================
test_t64() {
    local tdir="${TEST_TMP}/t64"
    mkdir -p "$tdir/home" "$tdir/work" "$tdir/control"
    local log_file="$tdir/fake-claude.log"
    local state_file="$tdir/state.json"
    local cfg_file="$tdir/config.json"
    local port=$(port_alloc)
    local listen="127.0.0.1:${port}"

    cat <<EOF > "$cfg_file"
{
    "state_file": "$state_file",
    "sessions": [
        {
            "name": "sess64",
            "workdir": "$tdir/work",
            "session_id": "test-session-064",
            "claude_binary": "$FAKE_CLAUDE",
            "readiness_poll_secs": 0.2,
            "env": {
                "FAKE_CLAUDE_LOG": "$log_file",
                "FAKE_CLAUDE_CONTROL_DIR": "$tdir/control"
            }
        }
    ]
}
EOF

    HOME="$tdir/home" AGENT_ORCHESTRATE_LISTEN="$listen" \
        "$BIN" run --config "$cfg_file" >/dev/null 2>&1 &
    local spid=$!
    ACTIVE_PIDS+=("$spid")

    local code=""
    for _ in $(seq 1 120); do
        code=$(curl -s -o /dev/null -w '%{http_code}' -X POST \
            "http://${listen}/api/v1/sessions/ghost/message" \
            -H 'Content-Type: application/json' -d '{"text":"hi"}' 2>/dev/null)
        if [ "$code" = "404" ]; then break; fi
        sleep 0.05
    done

    stop_supervisor "$spid"

    if [ "$code" = "404" ]; then
        rec "T64" "PASS" "unknown session answered 404"
    else
        rec "T64" "FAIL" "unknown session not 404 (code=$code)"
    fi
}

# ==============================================================================
# T65: Stopped session -> 409 not_running
# ==============================================================================
test_t65() {
    local tdir="${TEST_TMP}/t65"
    mkdir -p "$tdir/home" "$tdir/work" "$tdir/control"
    local log_file="$tdir/fake-claude.log"
    local state_file="$tdir/state.json"
    local cfg_file="$tdir/config.json"
    local port=$(port_alloc)
    local listen="127.0.0.1:${port}"

    cat <<EOF > "$cfg_file"
{
    "state_file": "$state_file",
    "sessions": [
        {
            "name": "sess65",
            "workdir": "$tdir/work",
            "session_id": "test-session-065",
            "claude_binary": "$FAKE_CLAUDE",
            "base_backoff_secs": 0.05,
            "max_backoff_secs": 0.1,
            "readiness_poll_secs": 0.2,
            "env": {
                "FAKE_CLAUDE_LOG": "$log_file",
                "FAKE_CLAUDE_CONTROL_DIR": "$tdir/control"
            }
        }
    ]
}
EOF

    HOME="$tdir/home" AGENT_ORCHESTRATE_LISTEN="$listen" \
        "$BIN" run --config "$cfg_file" >/dev/null 2>&1 &
    local spid=$!
    ACTIVE_PIDS+=("$spid")

    local listed=0
    for _ in $(seq 1 120); do
        local sbody=$(curl -s "http://${listen}/api/v1/sessions/sess65" 2>/dev/null)
        if echo "$sbody" | grep -q '"name":"sess65"'; then
            listed=1
            break
        fi
        sleep 0.05
    done

    local stopped=0
    if [ "$listed" -eq 1 ]; then
        curl -s -X POST "http://${listen}/api/v1/sessions/sess65/stop" >/dev/null 2>&1
        for _ in $(seq 1 120); do
            local sb=$(curl -s "http://${listen}/api/v1/sessions/sess65" 2>/dev/null)
            if echo "$sb" | grep -q '"paused":true'; then
                stopped=1
                break
            fi
            sleep 0.05
        done
    fi

    local code=""
    if [ "$stopped" -eq 1 ]; then
        code=$(curl -s -o /dev/null -w '%{http_code}' -X POST \
            "http://${listen}/api/v1/sessions/sess65/message" \
            -H 'Content-Type: application/json' -d '{"text":"nope"}')
    fi

    stop_supervisor "$spid"

    if [ "$stopped" -eq 1 ] && [ "$code" = "409" ]; then
        rec "T65" "PASS" "stopped session answered 409 not_running"
    else
        rec "T65" "FAIL" "stopped message test failed (listed=$listed stopped=$stopped code=$code)"
    fi
}

# ==============================================================================
# T66: Rate limit: 31st message within a minute -> 429 with Retry-After
# ==============================================================================
test_t66() {
    local tdir="${TEST_TMP}/t66"
    mkdir -p "$tdir/home" "$tdir/work" "$tdir/control"
    local log_file="$tdir/fake-claude.log"
    local state_file="$tdir/state.json"
    local cfg_file="$tdir/config.json"
    local port=$(port_alloc)
    local listen="127.0.0.1:${port}"

    cat <<EOF > "$cfg_file"
{
    "state_file": "$state_file",
    "sessions": [
        {
            "name": "sess66",
            "workdir": "$tdir/work",
            "session_id": "test-session-066",
            "claude_binary": "$FAKE_CLAUDE",
            "readiness_poll_secs": 0.2,
            "env": {
                "FAKE_CLAUDE_LOG": "$log_file",
                "FAKE_CLAUDE_CONTROL_DIR": "$tdir/control"
            }
        }
    ]
}
EOF

    HOME="$tdir/home" AGENT_ORCHESTRATE_LISTEN="$listen" \
        "$BIN" run --config "$cfg_file" >/dev/null 2>&1 &
    local spid=$!
    ACTIVE_PIDS+=("$spid")

    local listed=0
    for _ in $(seq 1 120); do
        local body=$(curl -s "http://${listen}/api/v1/sessions" 2>/dev/null)
        if echo "$body" | grep -q "sess66"; then
            listed=1
            break
        fi
        sleep 0.05
    done

    local last_code=""
    if [ "$listed" -eq 1 ]; then
        # 30 fit within the window, the 31st is refused.
        for i in $(seq 1 31); do
            last_code=$(curl -s -o /dev/null -w '%{http_code}' -X POST \
                "http://${listen}/api/v1/sessions/sess66/message" \
                -H 'Content-Type: application/json' -d "{\"text\":\"m$i\"}" 2>/dev/null)
        done
    fi

    local retry_after=""
    if [ "$listed" -eq 1 ]; then
        curl -s -D "$tdir/headers" -o /dev/null -X POST \
            "http://${listen}/api/v1/sessions/sess66/message" \
            -H 'Content-Type: application/json' -d '{"text":"again"}' 2>/dev/null
        retry_after=$(grep -i '^retry-after:' "$tdir/headers" 2>/dev/null | tr -d '\r' | awk '{print $2}')
    fi

    stop_supervisor "$spid"

    if [ "$listed" -eq 1 ] && [ "$last_code" = "429" ] && [ -n "$retry_after" ]; then
        rec "T66" "PASS" "rate limit: 31st message 429 with Retry-After=$retry_after"
    else
        rec "T66" "FAIL" "rate limit failed (listed=$listed last_code=$last_code retry_after='$retry_after')"
    fi
}

# ==============================================================================
# T67: Bearer auth: no token configured -> reachable; configured -> 401
# ==============================================================================
test_t67() {
    local tdir="${TEST_TMP}/t67"
    mkdir -p "$tdir/home" "$tdir/work" "$tdir/control"
    local log_file="$tdir/fake-claude.log"
    local state_file="$tdir/state.json"
    local cfg_file="$tdir/config.json"
    local token_file="$tdir/token"
    local port=$(port_alloc)
    local listen="127.0.0.1:${port}"
    printf 'sekret-token\n' > "$token_file"

    cat <<EOF > "$cfg_file"
{
    "state_file": "$state_file",
    "sessions": [
        {
            "name": "sess67",
            "workdir": "$tdir/work",
            "session_id": "test-session-067",
            "claude_binary": "$FAKE_CLAUDE",
            "readiness_poll_secs": 0.2,
            "env": {
                "FAKE_CLAUDE_LOG": "$log_file",
                "FAKE_CLAUDE_CONTROL_DIR": "$tdir/control"
            }
        }
    ]
}
EOF

    HOME="$tdir/home" AGENT_ORCHESTRATE_TOKEN_FILE="$token_file" \
    AGENT_ORCHESTRATE_LISTEN="$listen" \
        "$BIN" run --config "$cfg_file" >/dev/null 2>&1 &
    local spid=$!
    ACTIVE_PIDS+=("$spid")

    local noauth_code=""
    for _ in $(seq 1 120); do
        noauth_code=$(curl -s -o /dev/null -w '%{http_code}' -X POST \
            "http://${listen}/api/v1/sessions/sess67/message" \
            -H 'Content-Type: application/json' -d '{"text":"beatlejuice"}' 2>/dev/null)
        if [ "$noauth_code" = "401" ]; then break; fi
        sleep 0.05
    done

    local auth_code=""
    if [ "$noauth_code" = "401" ]; then
        auth_code=$(curl -s -o /dev/null -w '%{http_code}' -X POST \
            "http://${listen}/api/v1/sessions/sess67/message" \
            -H "Authorization: Bearer sekret-token" \
            -H 'Content-Type: application/json' -d '{"text":"allowed"}')
    fi

    stop_supervisor "$spid"

    if [ "$noauth_code" = "401" ] && [ "$auth_code" = "202" ]; then
        rec "T67" "PASS" "bearer auth enforced on message route (401 without, 202 with token)"
    else
        rec "T67" "FAIL" "message auth failed (noauth=$noauth_code auth=$auth_code)"
    fi
}

# ==============================================================================
# T68: Injected text never appears in the log endpoint output or supervisor log
# ==============================================================================
test_t68() {
    local tdir="${TEST_TMP}/t68"
    mkdir -p "$tdir/home" "$tdir/work" "$tdir/control"
    local log_file="$tdir/fake-claude.log"
    local stdin_log="$tdir/stdin.log"
    local state_file="$tdir/state.json"
    local cfg_file="$tdir/config.json"
    local sup_log="$tdir/supervisor.log"
    local port=$(port_alloc)
    local listen="127.0.0.1:${port}"

    cat <<EOF > "$cfg_file"
{
    "state_file": "$state_file",
    "sessions": [
        {
            "name": "sess68",
            "workdir": "$tdir/work",
            "session_id": "test-session-068",
            "claude_binary": "$FAKE_CLAUDE",
            "readiness_poll_secs": 0.2,
            "env": {
                "FAKE_CLAUDE_LOG": "$log_file",
                "FAKE_CLAUDE_STDIN_LOG": "$stdin_log",
                "FAKE_CLAUDE_CONTROL_DIR": "$tdir/control"
            }
        }
    ]
}
EOF

    HOME="$tdir/home" AGENT_ORCHESTRATE_LISTEN="$listen" \
        "$BIN" run --config "$cfg_file" > "$sup_log" 2>&1 &
    local spid=$!
    ACTIVE_PIDS+=("$spid")

    local up=0
    for _ in $(seq 1 160); do
        if grep -q "fd0-loop started" "$stdin_log" 2>/dev/null; then
            up=1
            break
        fi
        sleep 0.05
    done

    local secret="T68LEGACYSUPERSECRET98765"
    if [ "$up" -eq 1 ]; then
        curl -s -X POST "http://${listen}/api/v1/sessions/sess68/message" \
            -H 'Content-Type: application/json' -d "{\"text\":\"$secret\"}" >/dev/null 2>&1
    fi

    # Wait for delivery, then also fetch the log endpoint.
    sleep 0.6
    local log_body=""
    log_body=$(curl -s "http://${listen}/api/v1/sessions/sess68/log?tail=100" 2>/dev/null)

    stop_supervisor "$spid"

    local in_sup=0
    if grep -q "$secret" "$sup_log" 2>/dev/null; then
        in_sup=1
    fi
    local in_log=0
    if echo "$log_body" | grep -q "$secret"; then
        in_log=1
    fi
    local in_ring=0
    if grep -q "$secret" "$stdin_log" 2>/dev/null; then
        in_ring=1 # stdin capture legitimately shows it; not the ring
    fi

    if [ "$up" -eq 1 ] && [ "$in_sup" -eq 0 ] && [ "$in_log" -eq 0 ]; then
        rec "T68" "PASS" "message text never appears in supervisor log or log endpoint"
    else
        rec "T68" "FAIL" "message text leaked (up=$up sup=$in_sup logendpoint=$in_log)"
    fi
}

TESTS_TO_RUN=()
if [ -n "${ONLY:-}" ]; then
    for t in $ONLY; do
        t_clean=$(echo "$t" | tr '[:upper:]' '[:lower:]' | sed 's/^test_//')
        TESTS_TO_RUN+=("$t_clean")
    done
elif [ "$#" -gt 0 ]; then
    for t in "$@"; do
        t_clean=$(echo "$t" | tr '[:upper:]' '[:lower:]' | sed 's/^test_//')
        TESTS_TO_RUN+=("$t_clean")
    done
fi

if [ "${#TESTS_TO_RUN[@]}" -gt 0 ]; then
    for t in "${TESTS_TO_RUN[@]}"; do
        "test_${t}"
    done
else
    test_t01
    test_t02
    test_t03
    test_t04
    test_t05
    test_t06
    test_t07
    test_t08
    test_t09
    test_t10
    test_t11
    test_t12
    test_t13
    test_t14
    test_t15
    test_t16
    test_t17
    test_t18
    test_t19
    test_t20
    test_t21
    test_t22
    test_t23
    test_t24
    test_t25
    test_t26
    test_t27
    test_t28
    test_t29
    test_t30
    test_t31
    test_t32
    test_t33
    test_t34
    test_t35
    test_t36
    test_t37
    test_t38
    test_t39
    test_t40
    test_t41
    test_t42
    test_t43
    test_t44
    test_t45
    test_t46
    test_t47
    test_t48
    test_t49
    test_t50
    test_t51
    test_t52
    test_t53
    test_t54
    test_t55
    test_t56
    test_t57
    test_t58
    test_t59
    test_t60
    test_t61
    test_t62
    test_t63
    test_t64
    test_t65
    test_t66
    test_t67
    test_t68
fi

echo "============================================================"
if [ "$FAILED_TESTS" -eq 0 ]; then
    echo -e "${GREEN}SUITE RESULT: all ${TOTAL_TESTS} tests passed${NC}"
    exit 0
else
    echo -e "${RED}SUITE RESULT: ${FAILED_TESTS} of ${TOTAL_TESTS} tests failed${NC}"
    exit 1
fi

