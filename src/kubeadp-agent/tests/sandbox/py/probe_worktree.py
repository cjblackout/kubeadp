#!/usr/bin/env python3
# probe_worktree.py - R04: reach the OTHER agent's worktree and private tmp.
#
# usage: probe_worktree.py <victim-pid> <victim-rw-host-path>
#
# Vectors:
#   1. direct path from env (RW_A) - hidden under the sandbox's hide rule
#      (ENOENT / not traversable), writable in fake runs (FAIL control)
#   2. /proc/<pid>/root/<rel> - Landlock does not allow this path
#   3. /tmp and /dev/shm enumeration - on kata/runc they are masked (INFO)
#      or shared (leak if we can see others' files)
#
# ATTACK semantics: successful read/write/list => FAIL (leak), blocked => PASS.

import errno
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
try:
    from sbcommon import verdict, errno_name
except Exception:
    def verdict(cid, res, msg, extra=None):
        sys.stdout.write("VERDICT %s %s %s\n" % (cid, res, msg))
        sys.stdout.flush()
    def errno_name(e):
        return "E%d" % e


def op(cid, action, path, payload=b"probe", expect_absent_ok=False):
    result = None
    info = ""
    if action == "read":
        try:
            with open(path, "rb") as f:
                d = f.read()
            result = "ok"; info = "%d bytes" % len(d)
        except OSError as e:
            result = "err"; info = errno_name(e.errno)
    elif action == "list":
        try:
            ents = os.listdir(path)
            result = "ok"; info = ",".join(ents[:6])
        except OSError as e:
            result = "err"; info = errno_name(e.errno)
    elif action == "write":
        p = os.path.join(path, ".hl86-r04-probe")
        try:
            with open(p, "wb") as f:
                f.write(payload)
            # verify round-trip
            with open(p, "rb") as f:
                back = f.read()
            os.remove(p)
            result = "ok" if back == payload else "err"
            info = "round-trip ok" if result == "ok" else "mismatch"
        except OSError as e:
            result = "err"; info = errno_name(e.errno)
    elif action == "stat":
        try:
            os.stat(path)
            result = "ok"; info = "exists"
        except OSError as e:
            result = "err"; info = errno_name(e.errno)
    if result == "ok":
        if expect_absent_ok:
            verdict(cid, "PASS", "%s %s (%s) - documented/shared" % (action, path, info))
        else:
            verdict(cid, "FAIL", "%s %s succeeded (leak) (%s)" % (action, path, info))
    else:
        verdict(cid, "PASS", "%s %s blocked: %s" % (action, path, info))


def main():
    if len(sys.argv) < 3:
        sys.stdout.write("VERDICT R04.usage SKIP usage: probe_worktree.py <pid> <victim-rw>\n")
        return
    pid = sys.argv[1] if sys.argv[1].isdigit() else ""
    victim_rw = sys.argv[2]

    # 1. direct host path
    if victim_rw:
        op("R04.read_victim_worktree", "read", os.path.join(victim_rw, "victim-marker.txt"))
        op("R04.list_victim_worktree", "list", victim_rw)
        op("R04.write_victim_worktree", "write", victim_rw)
        # probe private tmp through the hide-parent: nothing should resolve
        op("R04.stat_hidden_parent", "stat", os.path.split(victim_rw)[0], expect_absent_ok=True)
    else:
        sys.stdout.write("VERDICT R04.victim_rw SKIP no victim rw path\n")

    # 2. /proc/<pid>/root traversal
    if pid:
        root = "/proc/%s/root" % pid
        # reading another agent's root's proc environ is a real cross-agent
        # read; a sandbox must block it (Landlock: EACCES, gvisor: ENOENT
        # because the victim has its own fresh /proc)
        op("R04.via_root_proc_environ", "read",
           os.path.join(root, "proc", "self", "environ"))
        # its root's /tmp is the victim's private tmp parent on kata
        op("R04.via_root_tmp", "list", os.path.join(root, "tmp"))
        op("R04.via_root_devshm", "list", os.path.join(root, "dev", "shm"))
    else:
        sys.stdout.write("VERDICT R04.root_traversal SKIP no pid\n")

    # 3. shared /tmp + /dev/shm enumeration (INFO on kata where masked)
    my_tmp = os.environ.get("TMPDIR", "/tmp")
    op("R04.tmp_others", "list", my_tmp, expect_absent_ok=True)
    try:
        shm = os.listdir("/dev/shm")
        verdict("R04.devshm", "INFO" if not shm else "FAIL",
                "/dev/shm listed (%d entries)" % len(shm))
    except OSError as e:
        verdict("R04.devshm", "PASS", "/dev/shm blocked: %s" % errno_name(e.errno))


if __name__ == "__main__":
    main()
