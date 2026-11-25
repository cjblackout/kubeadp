#!/usr/bin/env bash
# fake-claude.sh: Mock Claude CLI for supervisor tests.
set -euo pipefail

# 1. Handle auth status check
if [ "${1:-}" = "auth" ] && [ "${2:-}" = "status" ] && [ "${3:-}" = "--json" ]; then
    if [ -n "${FAKE_CLAUDE_CONTROL_DIR:-}" ] && [ -f "${FAKE_CLAUDE_CONTROL_DIR}/auth_not_logged_in" ]; then
        echo '{"loggedIn": false}'
        exit 0
    elif [ "${FAKE_CLAUDE_AUTH:-}" = "not-logged-in" ]; then
        echo '{"loggedIn": false}'
        exit 0
    elif [ -n "${FAKE_CLAUDE_CONTROL_DIR:-}" ] && [ -f "${FAKE_CLAUDE_CONTROL_DIR}/auth_error" ]; then
        echo 'auth error' >&2
        exit 1
    elif [ "${FAKE_CLAUDE_AUTH:-}" = "error" ]; then
        echo 'auth error' >&2
        exit 1
    else
        echo '{"loggedIn": true}'
        exit 0
    fi
fi

# 2. Record exit and invocation
record_exit() {
    local rc=$?
    if [ -n "${FAKE_CLAUDE_LOG:-}" ]; then
        printf '%s pid=%d exit=%d\n' "$(date +%s.%N)" "$$" "$rc" >> "$FAKE_CLAUDE_LOG"
    fi
}
trap record_exit EXIT

if [ -n "${FAKE_CLAUDE_LOG:-}" ]; then
    mkdir -p "$(dirname "$FAKE_CLAUDE_LOG")"
    printf '%s pid=%d cwd=%s argv=%s\n' "$(date +%s.%N)" "$$" "$PWD" "$*" >> "$FAKE_CLAUDE_LOG"
fi

# 2b. stdin capture: echo every byte that arrives on our tty while running.
# Used by the message-injection tests to prove exactly what reaches the child
# (the bracketed ESC[200~ ... ESC[201~ \r frame), independent of the PTY's
# normal echo/tty-line discipline.
if [ -n "${FAKE_CLAUDE_STDIN_LOG:-}" ]; then
    mkdir -p "$(dirname "$FAKE_CLAUDE_STDIN_LOG")"
    # Background jobs in a non-interactive shell get stdin from /dev/null, so
    # attach the reader to the controlling terminal explicitly. Raw mode
    # (tty.setraw) disables ECHO and canonical buffering so the exact frame is
    # captured and nothing is echoed back into the session output ring, which
    # is how a real TUI (raw-mode Claude) behaves.
    python3 -c '
import os, sys, tty
log = sys.argv[1]
tty.setraw(0)
os.set_blocking(0, True)
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
' "$FAKE_CLAUDE_STDIN_LOG" < /dev/tty &
fi

if [ -n "${FAKE_CLAUDE_RECORD_FDS:-}" ]; then
    mkdir -p "$(dirname "$FAKE_CLAUDE_RECORD_FDS")"
    for fd in /proc/$$/fd/*; do
        if [ -e "$fd" ] || [ -L "$fd" ]; then
            b="$(basename "$fd")"
            t="$(readlink "$fd" || true)"
            echo "$b -> $t"
        fi
    done | sort -n > "${FAKE_CLAUDE_RECORD_FDS}.${$}"
fi

# 3. Actions / Markers control
check_action() {
    local action="${FAKE_CLAUDE_ACTION:-}"
    if [ -n "${FAKE_CLAUDE_CONTROL_DIR:-}" ]; then
        if [ -f "${FAKE_CLAUDE_CONTROL_DIR}/action_exit0" ]; then
            action="exit0"
        elif [ -f "${FAKE_CLAUDE_CONTROL_DIR}/action_exit1" ]; then
            action="exit1"
        elif [ -f "${FAKE_CLAUDE_CONTROL_DIR}/action_hang" ]; then
            action="hang"
        elif [ -f "${FAKE_CLAUDE_CONTROL_DIR}/action_ignore_sigterm" ]; then
            action="ignore_sigterm"
        elif [ -f "${FAKE_CLAUDE_CONTROL_DIR}/action_close_tty" ]; then
            action="close_tty"
        elif [ -f "${FAKE_CLAUDE_CONTROL_DIR}/action_close_tty_exit" ]; then
            action="close_tty_exit"
        elif [ -f "${FAKE_CLAUDE_CONTROL_DIR}/action_close_tty_ignore_sigterm" ]; then
            action="close_tty_ignore_sigterm"
        elif [ -f "${FAKE_CLAUDE_CONTROL_DIR}/action_already_in_use" ] || [ -f "${FAKE_CLAUDE_CONTROL_DIR}/already_in_use" ]; then
            action="already_in_use"
        elif [ -f "${FAKE_CLAUDE_CONTROL_DIR}/action_resume_fail" ]; then
            action="resume_fail"
        elif [ -f "${FAKE_CLAUDE_CONTROL_DIR}/action_resume_api_error" ]; then
            action="resume_api_error"
        elif [ -f "${FAKE_CLAUDE_CONTROL_DIR}/action_tui_burst" ]; then
            action="tui_burst"
        elif [ -f "${FAKE_CLAUDE_CONTROL_DIR}/action_raw_burst" ]; then
            action="raw_burst"
        elif [ -f "${FAKE_CLAUDE_CONTROL_DIR}/action_crash" ]; then
            action="crash"
        fi
    fi
    echo "$action"
}

ACTION="$(check_action)"

case "$ACTION" in
    exit0)
        echo "fake-claude: exiting 0 immediately"
        exit 0
        ;;
    exit1)
        echo "fake-claude: exiting 1 immediately"
        exit 1
        ;;
    crash)
        CRASH_SECS="${FAKE_CLAUDE_CRASH_SECS:-1}"
        echo "fake-claude: running for ${CRASH_SECS}s then crashing..."
        sleep "$CRASH_SECS"
        echo "fake-claude: crashed!" >&2
        exit 1
        ;;
    hang)
        # Hang silently: no output, no exit, responds to signals normally
        while true; do
            sleep 1
        done
        ;;
    ignore_sigterm)
        # Ignore SIGTERM (proves SIGKILL escalation)
        trap '' TERM
        echo "fake-claude: ignoring SIGTERM..."
        while true; do
            sleep 1
        done
        ;;
    close_tty)
        # Close standard fds (EOF on PTY) and keep running with normal signal handling
        trap '' HUP
        exec >&- 2>&-
        exec 0<&-
        while true; do
            sleep 1
        done
        ;;
    close_tty_ignore_sigterm)
        # Close standard fds (EOF on PTY) and ignore SIGTERM
        trap '' HUP TERM
        exec >&- 2>&-
        exec 0<&-
        while true; do
            sleep 1
        done
        ;;
    close_tty_exit)
        # Close standard fds (EOF on PTY) and exit on its own shortly
        trap '' HUP
        exec >&- 2>&-
        exec 0<&-
        sleep 0.2
        exit 0
        ;;
    already_in_use)
        for arg in "$@"; do
            if [ "$arg" = "--session-id" ]; then
                echo "Error: Session ID 10000000-0000-4000-8000-000000000001 is already in use." >&2
                exit 1
            fi
        done
        echo "fake-claude: started successfully with args: $*"
        while true; do
            echo "fake-claude: heartbeat $(date +%s)"
            sleep 0.2
        done
        ;;
    resume_fail)
        for arg in "$@"; do
            if [ "$arg" = "--resume" ]; then
                echo "Error: No conversation found with session ID" >&2
                exit 1
            fi
        done
        echo "fake-claude: started successfully with args: $*"
        while true; do
            echo "fake-claude: heartbeat $(date +%s)"
            sleep 0.2
        done
        ;;
    resume_api_error)
        for arg in "$@"; do
            if [ "$arg" = "--resume" ]; then
                echo "Error: API unavailable" >&2
                exit 1
            fi
        done
        echo "fake-claude: started successfully with args: $*"
        while true; do
            echo "fake-claude: heartbeat $(date +%s)"
            sleep 0.2
        done
        ;;
    tui_burst)
        # Print a TUI-like burst of multi-byte chars (box-drawing, emoji, escape sequences, NO trailing newline)
        printf '\033[2J\033[H'
        for i in $(seq 1 40); do
            printf '┌───────────────────────────────┐\r\n'
            printf '│  🤖 Claude Code TUI Mode ✨   │\r\n'
            printf '└───────────────────────────────┘\r\n'
        done
        printf '┌───────────── Progress 50%% ───'
        while true; do
            sleep 1
        done
        ;;
    raw_burst)
        # Immediate raw burst with NO newline
        printf 'RAW_BURST_WITHOUT_NEWLINE_12345'
        while true; do
            sleep 1
        done
        ;;
    *)
        # Default normal running loop: emit output periodically so hang detection does not trigger
        echo "fake-claude: started successfully with args: $*"
        while true; do
            # Check if dynamic control was placed while running
            if [ -n "${FAKE_CLAUDE_CONTROL_DIR:-}" ]; then
                if [ -f "${FAKE_CLAUDE_CONTROL_DIR}/action_exit1" ]; then
                    echo "fake-claude: dynamic exit 1 triggered"
                    exit 1
                elif [ -f "${FAKE_CLAUDE_CONTROL_DIR}/action_exit0" ]; then
                    echo "fake-claude: dynamic exit 0 triggered"
                    exit 0
                fi
            fi
            echo "fake-claude: heartbeat $(date +%s)"
            sleep 0.2
        done
        ;;
esac
