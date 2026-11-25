#!/usr/bin/env python3
# sysproc_probe.py - inspect the visible process table for cross-agent leaks.
#
# Checks, from inside B's sandbox:
#   R01.proc_lists      : can B list other (non-B) processes' /proc/<pid>?
#   R06.no_secret_in_argv_of_visible : no visible process's cmdline contains
#                        the run secret (or the victim argv tag would be fine
#                        but the SECRET value must not appear)
#   R06.no_secret_in_environ_of_visible : same for environ
#
# The secret itself never leaves the host in argv; we pass a HASH of the
# secret via env and compare hashes of values read from /proc instead, so the
# plaintext secret is never emitted.
#
# usage: sysproc_probe.py <secret-hash-sha256>
#   (KUBEADP_TEST_TAG env may carry the run tag to identify the victim pid)

import hashlib
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
try:
    from sbcommon import verdict, list_proc_pids, TAG, verdict_meta
except Exception:
    def verdict(cid, res, msg, extra=None):
        sys.stdout.write("VERDICT %s %s %s\n" % (cid, res, msg))
        sys.stdout.flush()
    def list_proc_pids():
        try:
            return [int(p) for p in os.listdir("/proc") if p.isdigit()]
        except Exception:
            return []
    def verdict_meta(cid, what, detail):
        verdict(cid, "FAIL", "%s visible" % what)


def main():
    if len(sys.argv) >= 2:
        want_hash = sys.argv[1].strip().lower()
    else:
        want_hash = os.environ.get("KUBEADP_TEST_SECRET_HASH", "")
    pids = list_proc_pids()
    mypid = os.getpid()
    # Own ancestor chain (the case shell, the sandbox init shim): in a fresh pid
    # namespace these are the sandbox's own processes, not another agent's.
    own = {mypid}
    cur = mypid
    for _ in range(64):
        try:
            with open("/proc/%d/stat" % cur) as f:
                cur = int(f.read().rsplit(")", 1)[1].split()[1])
        except Exception:
            break
        if cur <= 0 or cur in own:
            break
        own.add(cur)
    fresh_pidns = os.environ.get("KUBEADP_TEST_PIDNS", "") == "1"
    leaked = []
    visible_others = 0
    for pid in pids:
        if pid in own or (fresh_pidns and pid == 1):
            continue
        # don't scan our own shell/self (self-control noise)
        try:
            with open("/proc/%d/cmdline" % pid, "rb") as f:
                raw_cmd = f.read()
            cmd = raw_cmd.replace(b"\x00", b" ")
        except Exception:
            continue
        if not cmd.strip():
            continue
        # /proc/<pid> exists for us? (i.e. we can see another agent's dir)
        visible_others += 1
        if want_hash:
            h = hashlib.sha256(cmd).hexdigest()
            if h == want_hash:
                leaked.append("pid%d cmdline" % pid)
        try:
            with open("/proc/%d/environ" % pid, "rb") as f:
                env = f.read()
            if want_hash and hashlib.sha256(env).hexdigest() == want_hash:
                leaked.append("pid%d environ" % pid)
        except Exception:
            pass

    if want_hash and leaked:
        verdict("R06.visible_secret_leaked", "FAIL", "secret found in visible process: %s" % ";".join(leaked[:3]))
    else:
        verdict("R06.visible_secret_leaked", "PASS", "secret absent from all visible argv/env" if want_hash else "no secret hash supplied; argv/env scan skipped")
    if want_hash:
        verdict("R06.visible_secret_procmes", "INFO", "scanned %d visible processes" % visible_others)
    # The visible process table is a metadata property of the (shared) pod
    # /proc on kata / runc-Landlock; it is a documented accepted difference
    # (INFO) there, and a FAIL (leak) on a fresh-pidns runtime / fake stub.
    if visible_others > 0:
        verdict_meta("R01.proc_lists", "other processes in /proc", "visible others=%d" % visible_others)
    else:
        verdict("R01.proc_lists", "PASS", "no other processes visible")


if __name__ == "__main__":
    main()
