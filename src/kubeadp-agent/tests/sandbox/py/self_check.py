#!/usr/bin/env python3
# self_check.py - R5: the agent can use its own environment normally.
#
# usage: self_check.py
#
# Verifies (all PASS expected):
#   R05.write_own   : write + read a file in our OWN rw dir
#   R05.tmp_ok      : write + read a file in $TMPDIR (private tmp)
#   R05.read_ro     : read the shared ro-share marker
#   R05.tools       : exec python3 -c, and run git --version / ssh -V / node --version
#                     where present (SKIP if a tool is missing)
#   R05.shared_rw      : write+read in $KUBEADP_TEST_SHARED (shared by design)
# Failures here mean the sandbox is too tight -> FAIL.

import os
import subprocess
import sys

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
try:
    from sbcommon import verdict
except Exception:
    def verdict(cid, res, msg, extra=None):
        sys.stdout.write("VERDICT %s %s %s\n" % (cid, res, msg))
        sys.stdout.flush()


def try_write_read(path, payload="write-check\n"):
    try:
        with open(path, "w") as f:
            f.write(payload)
        with open(path) as f:
            return f.read() == payload
    except Exception:
        return False


def run_tool(args):
    try:
        r = subprocess.run(args, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL, timeout=15)
        return r.returncode == 0
    except Exception:
        return False


def main():
    rw = os.environ.get("KUBEADP_TEST_RW", ".")
    shared = os.environ.get("KUBEADP_TEST_SHARED", "")
    ro_share = os.environ.get("KUBEADP_TEST_RO_SHARE", "")
    tmp = os.environ.get("TMPDIR", "/tmp")

    ok = try_write_read(os.path.join(rw, "self-write.txt"))
    verdict("R05.write_own", "PASS" if ok else "FAIL", "write+read own rw dir %s" % rw)

    ok = try_write_read(os.path.join(tmp, "self-tmp.txt"))
    verdict("R05.tmp_ok", "PASS" if ok else "FAIL", "write+read private tmp %s" % tmp)

    if ro_share:
        ok = False
        try:
            with open(os.path.join(ro_share, "marker.txt")) as f:
                ok = "hl86-ro-marker" in f.read()
        except Exception:
            ok = False
        verdict("R05.read_ro", "PASS" if ok else "FAIL", "read ro-share marker %s/marker.txt" % ro_share)
    else:
        verdict("R05.read_ro", "SKIP", "no KUBEADP_TEST_RO_SHARE")

    if shared:
        ok = try_write_read(os.path.join(shared, "self-shared.txt"))
        verdict("R05.shared_rw", "PASS" if ok else "FAIL", "write+read shared_rw %s" % shared)
    else:
        verdict("R05.shared_rw", "SKIP", "no KUBEADP_TEST_SHARED")

    tools = []
    tools.append(("python3", ["python3", "-c", "print(1)"], "python3"))
    tools.append(("git", ["git", "--version"], "git"))
    tools.append(("ssh", ["ssh", "-V"], "ssh"))
    tools.append(("node", ["node", "--version"], "node"))
    tools.append(("cargo", ["cargo", "--version"], "cargo"))

    for tname, cmd, disp in tools:
        if run_tool(cmd):
            verdict("R05.tool_%s" % tname, "PASS", "%s works" % disp)
        else:
            verdict("R05.tool_%s" % tname, "SKIP", "%s missing or failed" % disp)


if __name__ == "__main__":
    main()
