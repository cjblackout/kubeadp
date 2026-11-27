#!/usr/bin/env bash
# fake-opencode.sh: a fake `opencode serve` for the supervisor orchestrate tests.
#
# Server-backed opencode sessions supervise `opencode serve --hostname 127.0.0.1
# --port <p>` and talk to it over HTTP; this fake serves the small part of that API
# the engine uses (shapes verified against opencode 1.19.4):
#   GET  /session/status               -> 200 {}
#   POST /session                      -> 200 {"id":"ses_fake<N>"}; a non-array
#                                         "permission" is rejected with 400 like the real server
#   GET  /session/<id>                 -> 200 for a known id, else 404
#   POST /session/<id>/prompt_async    -> 204; the text of the first part is appended to
#                                         $FAKE_OPENCODE_LOG as `PROMPT <text>`
# Every start logs `argv=<args>` and `listening port=<p>`, every exit `exit=<rc>`, to $FAKE_OPENCODE_LOG.
# Control: FAKE_OPENCODE_ACTION (exit0|exit1|crash) at start, or marker files
# action_exit0 / action_exit1 / action_crash in $FAKE_OPENCODE_CONTROL_DIR at any time.
set -euo pipefail

log() { [ -n "${FAKE_OPENCODE_LOG:-}" ] && printf '%s\n' "$*" >> "$FAKE_OPENCODE_LOG" || true; }
[ -n "${FAKE_OPENCODE_LOG:-}" ] && mkdir -p "$(dirname "$FAKE_OPENCODE_LOG")"
log "argv=$*"

if [ "${1:-}" != "serve" ]; then
    echo "fake-opencode: only 'serve' is supported (got: $*)" >&2
    exit 2
fi

PORT=""
args=("$@")
for ((i = 0; i < ${#args[@]}; i++)); do
    if [ "${args[$i]}" = "--port" ]; then PORT="${args[$((i + 1))]:-}"; fi
done
[ -n "$PORT" ] || { echo "fake-opencode: --port is required" >&2; exit 2; }

case "${FAKE_OPENCODE_ACTION:-}" in
    exit0) echo "fake-opencode: exiting 0 immediately"; log "exit=0"; exit 0 ;;
    exit1) echo "fake-opencode: exiting 1 immediately"; log "exit=1"; exit 1 ;;
    crash) sleep "${FAKE_OPENCODE_CRASH_SECS:-1}"; echo "fake-opencode: crashed!" >&2; log "exit=1"; exit 1 ;;
esac

exec python3 - "$PORT" <<'PY'
import http.server, json, os, re, sys, threading, time

port = int(sys.argv[1])
log_file = os.environ.get("FAKE_OPENCODE_LOG")
ctrl = os.environ.get("FAKE_OPENCODE_CONTROL_DIR")
sessions = {}
lock = threading.Lock()

def log(line):
    if log_file:
        with open(log_file, "a") as f:
            f.write(line + "\n")

class Handler(http.server.BaseHTTPRequestHandler):
    def log_message(self, *args):
        pass

    def _send(self, code, body=b"{}"):
        self.send_response(code)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(body)))
        self.send_header("Connection", "close")
        self.end_headers()
        if code != 204:
            self.wfile.write(body)

    def do_GET(self):
        if self.path == "/session/status":
            return self._send(200, b"{}")
        m = re.fullmatch(r"/session/(ses_[A-Za-z0-9_]+)", self.path)
        if m:
            with lock:
                known = m.group(1) in sessions
            return self._send(200 if known else 404, b"{}")
        self._send(404)

    def do_POST(self):
        length = int(self.headers.get("Content-Length") or 0)
        raw = self.rfile.read(length) if length else b""
        try:
            body = json.loads(raw or b"{}")
        except ValueError:
            return self._send(400, b'{"_tag":"BadRequest"}')
        if self.path == "/session":
            if "permission" in body and not isinstance(body["permission"], list):
                return self._send(400, b'{"_tag":"BadRequest"}')
            with lock:
                sid = "ses_fake%d" % (len(sessions) + 1)
                sessions[sid] = []
            return self._send(200, json.dumps({"id": sid}).encode())
        m = re.fullmatch(r"/session/(ses_[A-Za-z0-9_]+)/prompt_async", self.path)
        if m:
            with lock:
                if m.group(1) not in sessions:
                    return self._send(404)
            parts = body.get("parts") or [{}]
            log("PROMPT " + str(parts[0].get("text", "")))
            return self._send(204)
        self._send(404)

def watcher():
    while True:
        if ctrl:
            if os.path.exists(os.path.join(ctrl, "action_exit0")):
                log("exit=0")
                os._exit(0)
            for marker in ("action_exit1", "action_crash"):
                if os.path.exists(os.path.join(ctrl, marker)):
                    log("exit=1")
                    os._exit(1)
        time.sleep(0.1)

def heartbeat():
    while True:
        print("fake-opencode: heartbeat %d" % int(time.time()), flush=True)
        time.sleep(0.5)

threading.Thread(target=watcher, daemon=True).start()
threading.Thread(target=heartbeat, daemon=True).start()
server = http.server.ThreadingHTTPServer(("127.0.0.1", port), Handler)
log("listening port=%d" % port)
print("fake-opencode: serving on 127.0.0.1:%d" % port, flush=True)
server.serve_forever()
PY
