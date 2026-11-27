#!/usr/bin/env bash
# fake-opencode.sh: Mock opencode CLI for supervisor orchestrate tests.
# Supports both:
#  1. Interactive session mode (`run -i ...`) inside a PTY reading user lines.
#  2. Job mode (`run --format json ...`).
set -euo pipefail

# 1. Handle auth status check or version if ever called
if [ "${1:-}" = "auth" ]; then
    echo '{"loggedIn": true}'
    exit 0
fi

# 2. Record exit and invocation
record_exit() {
    local rc=$?
    if [ -n "${FAKE_OPENCODE_LOG:-}" ]; then
        local now=`date +%s.%N`
        printf '%s pid=%d exit=%d\n' "$now" "$$" "$rc" >> "$FAKE_OPENCODE_LOG"
    fi
}
trap record_exit EXIT

if [ -n "${FAKE_OPENCODE_LOG:-}" ]; then
    local log_dir=`dirname "$FAKE_OPENCODE_LOG"`
    mkdir -p "$log_dir"
    local now=`date +%s.%N`
    printf '%s pid=%d cwd=%s argv=%s\n' "$now" "$$" "$PWD" "$*" >> "$FAKE_OPENCODE_LOG"
fi

# 2b. stdin capture: record every byte that arrives on our tty/stdin while running.
if [ -n "${FAKE_OPENCODE_STDIN_LOG:-}" ]; then
    local stdin_dir=`dirname "$FAKE_OPENCODE_STDIN_LOG"`
    mkdir -p "$stdin_dir"
    python3 -c '
import os, sys, tty
log = sys.argv[1]
try:
    tty.setraw(0)
except Exception:
    pass
try:
    os.set_blocking(0, True)
except Exception:
    pass
out = open(log, "a", buffering=1)
out.write("fd0-loop started raw\n")
while True:
    try:
        data = os.read(0, 4096)
    except OSError:
        break
    if not data:
        break
    out.write("data=" + repr(data) + "\n")
out.close()
' "$FAKE_OPENCODE_STDIN_LOG" < /dev/tty &
fi

# 3. Actions / Markers control
check_action() {
    local action="${FAKE_OPENCODE_ACTION:-}"
    if [ -n "${FAKE_OPENCODE_CONTROL_DIR:-}" ]; then
        if [ -f "${FAKE_OPENCODE_CONTROL_DIR}/action_exit0" ]; then
            action="exit0"
        elif [ -f "${FAKE_OPENCODE_CONTROL_DIR}/action_exit1" ]; then
            action="exit1"
        elif [ -f "${FAKE_OPENCODE_CONTROL_DIR}/action_hang" ]; then
            action="hang"
        elif [ -f "${FAKE_OPENCODE_CONTROL_DIR}/action_ignore_sigterm" ]; then
            action="ignore_sigterm"
        elif [ -f "${FAKE_OPENCODE_CONTROL_DIR}/action_crash" ]; then
            action="crash"
        fi
    fi
    echo "$action"
}

ACTION=`check_action`

case "$ACTION" in
    exit0)
        echo "fake-opencode: exiting 0 immediately"
        exit 0
        ;;
    exit1)
        echo "fake-opencode: exiting 1 immediately"
        exit 1
        ;;
    crash)
        CRASH_SECS="${FAKE_OPENCODE_CRASH_SECS:-1}"
        echo "fake-opencode: running for ${CRASH_SECS}s then crashing..."
        sleep "$CRASH_SECS"
        echo "fake-opencode: crashed!" >&2
        exit 1
        ;;
    hang)
        while true; do
            sleep 1
        done
        ;;
    ignore_sigterm)
        trap '' TERM
        echo "fake-opencode: ignoring SIGTERM..."
        while true; do
            sleep 1
        done
        ;;
    *)
        # Normal running loop
        echo "fake-opencode: started successfully with args: $*"
        while true; do
            if [ -n "${FAKE_OPENCODE_CONTROL_DIR:-}" ]; then
                if [ -f "${FAKE_OPENCODE_CONTROL_DIR}/action_exit1" ]; then
                    echo "fake-opencode: dynamic exit 1 triggered"
                    exit 1
                elif [ -f "${FAKE_OPENCODE_CONTROL_DIR}/action_exit0" ]; then
                    echo "fake-opencode: dynamic exit 0 triggered"
                    exit 0
                fi
            fi
            local now=`date +%s`
            echo "fake-opencode: heartbeat $now"
            sleep 0.2
        done
        ;;
esac
