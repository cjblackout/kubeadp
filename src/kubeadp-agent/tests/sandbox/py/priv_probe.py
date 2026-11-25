#!/usr/bin/env python3
# priv_probe.py - R11: setuid binaries must not elevate; NoNewPrivs set.
#
# usage: priv_probe.py [path-to-setuid-binary] [nosuid-path]
#
# Checks:
#   R11.no_new_privs   : /proc/self/status shows NoNewPrivs:\t1
#   R11.setuid_no_elev : run a setuid root binary (su/passwd/etc, with no
#                        args required) and confirm the effective uid does NOT
#                        become 0. We use <path> --help where possible and
#                        verify via os.geteuid() before/after via fork, or by
#                        inspecting /proc for the child's ruid/euid.
#
# The setuid binary is started as a child; we read /proc/<child>/status for
# Uid/Gid lines. If the child's effective uid is 0 while ours is 1000, the
# sandbox failed R11.

import os
import subprocess
import sys
import time

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
try:
    from sbcommon import verdict
except Exception:
    def verdict(cid, res, msg, extra=None):
        sys.stdout.write("VERDICT %s %s %s\n" % (cid, res, msg))
        sys.stdout.flush()


def status_uid(pid):
    try:
        with open("/proc/%d/status" % pid) as f:
            for line in f:
                if line.startswith("Uid:"):
                    return [int(x) for x in line.split()[1:]]
    except Exception:
        pass
    return None


def main():
    seteuid_bin = sys.argv[1] if len(sys.argv) > 1 else ""
    self_euid = os.geteuid()

    # NoNewPrivs
    np = None
    try:
        with open("/proc/self/status") as f:
            for line in f:
                if line.startswith("NoNewPrivs:"):
                    np = int(line.split()[1])
    except Exception:
        np = None
    if np == 1:
        verdict("R11.no_new_privs", "PASS", "NoNewPrivs set")
    else:
        verdict("R11.no_new_privs", "FAIL", "NoNewPrivs not set (value=%r)" % np)

    # setuid elevation probe
    if not seteuid_bin or not os.path.exists(seteuid_bin):
        verdict("R11.setuid_no_elev", "SKIP", "no setuid binary supplied or missing")
        return
    child = subprocess.Popen(
        [seteuid_bin, "--version"],
        stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL,
        preexec_fn=lambda: None)
    time.sleep(0.3)
    if child.poll() is not None:
        # already exited; --version not supported; try no-arg
        child = subprocess.Popen(
            [seteuid_bin],
            stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
        time.sleep(0.3)
    uid = status_uid(child.pid)
    child.kill() if child.poll() is None else None
    child.wait()
    if uid:
        ruid, euid, suid, fsuid = uid
        if euid == 0 and self_euid != 0:
            verdict("R11.setuid_no_elev", "FAIL", "%s elevated: euid 0" % seteuid_bin)
        else:
            verdict("R11.setuid_no_elev", "PASS", "%s did not elevate (euid=%s)" % (seteuid_bin, euid))
    else:
        verdict("R11.setuid_no_elev", "SKIP", "could not read child uid status")


if __name__ == "__main__":
    main()
