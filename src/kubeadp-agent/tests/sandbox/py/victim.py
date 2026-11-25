#!/usr/bin/env python3
# victim.py - the long-lived target agent (agent A).
#
# Lives in A's OWN rw dir (B cannot list it). On start:
#   - calls prctl(PR_SET_PTRACER, ANY). In the negative control (fake stub)
#     this lets the attacker ptrace/read us so the suite provably detects a
#     leak. In a real sandbox it changes nothing (namespaces / Landlock still
#     isolate us) but is harmless.
#   - maps a memory page at a FIXED virtual address (best-effort) holding a
#     random secret; reports mem_mapped to the harness so the attacker knows
#     whether /proc/.../mem + process_vm_readv probes are meaningful.
#   - writes info.json -> own state dir (pid, port, worktree marker, mem flag)
#   - touches a worktree marker file (nonce) in own rw dir
#   - listens on 127.0.0.1:0 (random) so R10 can connect, writes the port
#   - sleeps until SIGTERM; then exits 0 (R02/R08 signal propagation).
#
# The secret is NEVER put in argv or environ: it lives in the mapped page and
# (via the sandbox launcher's fd 3 handling) in <private tmp>/secret, which
# this process also reads back for R06's positive check.
#
# Standard library only.

import ctypes
import json
import mmap
import os
import secrets
import signal
import socket
import sys
import threading
import time

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
try:
    from sbcommon import verdict, FIXED_SECRET_ADDR
except Exception:
    FIXED_SECRET_ADDR = 0x7000000
    def verdict(cid, res, msg, extra=None):
        sys.stdout.write("VERDICT %s %s %s\n" % (cid, res, msg))
        sys.stdout.flush()

RW = os.environ.get("KUBEADP_TEST_RW", ".")
STATE = os.environ.get("KUBEADP_TEST_STATE", os.path.join(RW, "state"))
PORT_HINT = os.environ.get("KUBEADP_TEST_PORT", "")


def prctl_set_ptracer_any():
    try:
        libc = ctypes.CDLL(None, use_errno=True)
        libc.prctl(0x59616d61, -1)  # PR_SET_PTRACER = PR_SET_PTRACER_ANY
        return True
    except Exception:
        return False


def map_mem_secret():
    """Map a page at a fixed address and store a random secret there. Returns
    (mapped_bool, secret_bytes). Best-effort; on failure the harness will
    SKIP the /proc/mem + process_vm_readv probes instead of misreading them."""
    secret = secrets.token_bytes(64)
    try:
        libc = ctypes.CDLL(None, use_errno=True)
        MAP_PRIVATE = 0x02
        MAP_FIXED = 0x10
        MAP_ANONYMOUS = 0x20
        addr = libc.mmap(ctypes.c_void_p(FIXED_SECRET_ADDR), 4096,
                         ctypes.c_int(0x7),
                         ctypes.c_int(MAP_PRIVATE | MAP_ANONYMOUS | MAP_FIXED),
                         ctypes.c_int(-1), ctypes.c_int(0))
        if addr != FIXED_SECRET_ADDR:
            return False, secret
        # write the secret into the page
        libc.memcpy(ctypes.c_void_p(FIXED_SECRET_ADDR), ctypes.c_char_p(secret), len(secret))
        return True, secret
    except Exception:
        return False, secret


def main():
    pid = os.getpid()
    # argv[1] is the run tag (non-secret, for attacker discovery via cmdline)
    tag = sys.argv[1] if len(sys.argv) > 1 else os.environ.get("KUBEADP_TEST_TAG", "")
    prctl_set_ptracer_any()
    mapped, mem_secret = map_mem_secret()

    # TCP listener now (before state so R10 has a port quickly). A background
    # thread accepts one connection and sends the marker back (R10 verifies
    # that B can reach it AND read the service's reply).
    try:
        s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        host_port = int(PORT_HINT) if PORT_HINT.isdigit() else 0
        s.bind(("127.0.0.1", host_port))
        s.listen(4)
        port = s.getsockname()[1]

        def accept_loop():
            while True:
                try:
                    conn, _ = s.accept()
                    conn.sendall(b"hl86-listener-OK\n")
                    conn.close()
                except Exception:
                    try:
                        s.close()
                        return
                    except Exception:
                        return

        threading.Thread(target=accept_loop, daemon=True).start()
    except Exception:
        port = 0

    # R06: read our own private tmp secret back (positive check). If the real
    # sandbox mounted it, this yields a hash. In fake runs there is none -> the
    # harness marks control FAIL (expected).
    secret_file = os.environ.get("KUBEADP_TEST_SECRET_FILE", os.path.join(
        os.environ.get("TMPDIR", "/tmp"), "secret"))
    try:
        with open(secret_file, "rb") as f:
            data = f.read()
        import hashlib
        sys.stdout.write("INFOHASH A %s\n" % hashlib.sha256(data).hexdigest())
        try:
            mode = "%04o" % (os.stat(secret_file).st_mode & 0o7777)
            sys.stdout.write("INFOMODE A %s\n" % mode)
        except Exception:
            pass
        sys.stdout.flush()
    except Exception:
        pass

    # worktree marker
    nonce = secrets.token_hex(8)
    with open(os.path.join(RW, "victim-marker.txt"), "w") as f:
        f.write("hl86-victim %s %s\n" % (os.environ.get("KUBEADP_TEST_TAG", ""), nonce))

    os.makedirs(STATE, exist_ok=True)
    with open(os.path.join(STATE, "info.json"), "w") as f:
        json.dump({
            "pid": pid,
            "port": port,
            "worktree_marker": nonce,
            "mem_mapped": bool(mapped),
            "ready": True,
        }, f)
    with open(os.path.join(STATE, "ready"), "w") as f:
        f.write("1\n")

    if not mapped:
        verdict("R03.arm_mem", "INFO", "fixed-address mmap failed; /proc/mem+vmread probes will SKIP")

    # keep running until the harness tears down. Tolerate SIGTERM/SIGINT
    # (log only, keep serving) so a concurrent signal attack cannot kill the
    # shared victim out from under the other cases; the harness sends SIGKILL
    # at teardown. This mirrors the real sandbox where the attacker cannot
    # signal us at all (R02).
    def on_term(signum, frame):
        sys.stdout.write("VERDICT R08.victim_sigterm_seen INFO victim got signal %s (kept alive)\n" % signum)
        sys.stdout.flush()

    signal.signal(signal.SIGTERM, on_term)
    signal.signal(signal.SIGINT, on_term)
    while True:
        time.sleep(0.5)


if __name__ == "__main__":
    main()
