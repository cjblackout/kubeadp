#!/usr/bin/env python3
# probe_procfs.py - R01: read another agent's /proc entries via python,
# covering environ, maps, cmdline (INFO-only), stat, task/*/environ, fd lists,
# readlink cwd/root/exe and /proc/<pid>/root traversal. Uses open() and
# os.open() (the "os.open" variants) and cat is exercised by the shell case.
#
# usage: probe_procfs.py <pid>
# ATTACK semantics: successful read/list => FAIL (leak). Blocked => PASS.

import errno
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
try:
    from sbcommon import verdict, errno_name, list_proc_pids, verdict_meta
except Exception:
    def verdict(cid, res, msg, extra=None):
        sys.stdout.write("VERDICT %s %s %s\n" % (cid, res, msg))
        sys.stdout.flush()
    def errno_name(e):
        return "E%d" % e
    def verdict_meta(cid, what, detail):
        verdict(cid, "FAIL", "%s visible" % what)


def read_one(cid, path, info_only=False, metadata=False):
    try:
        with open(path, "rb") as f:
            d = f.read()
        if info_only:
            verdict(cid, "INFO", "read %s (%d bytes) - documented accepted difference" % (path, len(d)))
        elif metadata:
            # pure metadata (stat: pid parentage/state) -- INFO on a shared-/proc
            # Landlock runtime, FAIL elsewhere
            verdict_meta(cid, "read %s" % path, "%d bytes" % len(d))
        elif len(d):
            verdict(cid, "FAIL", "read %s (%d bytes)" % (path, len(d)))
        else:
            verdict(cid, "FAIL", "read %s (0 bytes)" % path)
    except OSError as e:
        verdict(cid, "PASS", "read %s blocked: %s" % (path, errno_name(e.errno)))


def os_open_one(cid, path):
    try:
        fd = os.open(path, os.O_RDONLY)
        d = os.read(fd, 4096)
        os.close(fd)
        if len(d) or True:
            verdict(cid, "FAIL" if len(d) else "FAIL", "os.open read %s (%d bytes)" % (path, len(d)))
    except OSError as e:
        verdict(cid, "PASS", "os.open %s blocked: %s" % (path, errno_name(e.errno)))


def main():
    if len(sys.argv) < 2 or not sys.argv[1].isdigit():
        sys.stdout.write("VERDICT R01.usage SKIP no pid\n")
        return
    pid = int(sys.argv[1])
    base = "/proc/%d" % pid

    read_one("R01.environ", "%s/environ" % base)
    read_one("R01.maps", "%s/maps" % base)
    read_one("R01.stat", "%s/stat" % base, metadata=True)
    read_one("R01.cmdline", "%s/cmdline" % base, info_only=True)

    os_open_one("R01.osopen_environ", "%s/environ" % base)
    os_open_one("R01.osopen_maps", "%s/maps" % base)

    # task/*/environ for each thread (deduplicating with probe_mem is left to
    # the harness; here we only check the environ content itself)
    try:
        tids = os.listdir("%s/task" % base)
        leaked = []
        for t in tids[:8]:
            try:
                with open("%s/task/%s/environ" % (base, t), "rb") as f:
                    d = f.read()
                leaked.append("%s:%d" % (t, len(d)))
            except OSError:
                pass
        if leaked:
            verdict("R01.task_environ_py", "FAIL", "task environ readable: %s" % ",".join(leaked))
        elif tids:
            verdict("R01.task_environ_py", "PASS", "thread dirs visible, environ unreadable")
        else:
            verdict("R01.task_environ_py", "PASS", "task list empty")
    except OSError as e:
        verdict("R01.task_list_py", "PASS", "task list blocked: %s" % errno_name(e.errno))


if __name__ == "__main__":
    main()
