#!/usr/bin/env python3
# probe_mem.py - R01/R03: read /proc/<pid>/{mem} of the victim at the fixed
# secret address and several offsets, plus enumerate /proc/<pid>/fd and
# /proc/<pid>/task.
#
# usage: probe_mem.py <pid>
# ATTACK semantics: a successful read / fd listing is a FAIL (leak). Blocked /
# EACCES / empty after denial => PASS.

import errno
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
try:
    from sbcommon import verdict, errno_name, FIXED_SECRET_ADDR, FIXED_SECRET_LEN, verdict_meta
except Exception:
    FIXED_SECRET_ADDR = 0x10000000000
    FIXED_SECRET_LEN = 4096
    def verdict(cid, res, msg, extra=None):
        sys.stdout.write("VERDICT %s %s %s\n" % (cid, res, msg))
        sys.stdout.flush()
    def errno_name(e):
        return "E%d" % e
    def verdict_meta(cid, what, detail):
        verdict(cid, "FAIL", "%s visible" % what)


def safe_pread(path, addr, length):
    try:
        fd = os.open(path, os.O_RDONLY)
        try:
            os.lseek(fd, addr, os.SEEK_SET)
            return True, os.read(fd, length)
        finally:
            os.close(fd)
    except OSError as e:
        return False, e.errno


def main():
    if len(sys.argv) < 2 or not sys.argv[1].isdigit():
        sys.stdout.write("VERDICT R01.usage SKIP no pid\n")
        return
    pid = int(sys.argv[1])
    mem = "/proc/%d/mem" % pid

    ok, res = safe_pread(mem, FIXED_SECRET_ADDR, FIXED_SECRET_LEN)
    if ok:
        verdict("R01.read_mem_secret", "FAIL", "/proc/mem read %d bytes at secret addr" % len(res))
    else:
        verdict("R01.read_mem_secret", "PASS", "/proc/mem at secret addr blocked: %s" % errno_name(res))

    # fd listing
    try:
        fds = os.listdir("/proc/%d/fd" % pid)
        if fds:
            verdict_meta("R01.list_fd", "victim fd list", ",".join(fds[:8]))
        else:
            verdict("R01.list_fd", "PASS", "victim fd list empty")
    except OSError as e:
        verdict("R01.list_fd", "PASS", "list fd blocked: %s" % errno_name(e.errno))

    # task thread listing
    try:
        tids = os.listdir("/proc/%d/task" % pid)
        if tids:
            verdict_meta("R01.list_task", "victim thread list", ",".join(tids[:8]))
        else:
            verdict("R01.list_task", "PASS", "victim thread list empty")
    except OSError as e:
        verdict("R01.list_task", "PASS", "list task blocked: %s" % errno_name(e.errno))

    # each thread environ
    try:
        tids = os.listdir("/proc/%d/task" % pid)
        leaked = []
        for t in tids[:4]:
            p = "/proc/%d/task/%s/environ" % (pid, t)
            try:
                d = open(p, "rb").read()
                leaked.append("%s:%d" % (t, len(d)))
            except OSError:
                pass
        if leaked:
            verdict("R01.read_task_environ", "FAIL", "task environ readable: %s" % ",".join(leaked))
        else:
            verdict("R01.read_task_environ", "PASS", "task environ unreadable")
    except OSError as e:
        verdict("R01.read_task_environ", "PASS", "task list blocked: %s" % errno_name(e.errno))

    # cwd / root / exe link resolution. These go through the same
    # ptrace_may_access gate as environ/maps/mem (unlike stat/cmdline which are
    # free metadata), so a successful resolution is a real metadata leak and
    # stays a FAIL even on a shared-/proc Landlock runtime.
    for link in ("cwd", "root", "exe"):
        try:
            tgt = os.readlink("/proc/%d/%s" % (pid, link))
            verdict("R01.readlink_%s" % link, "FAIL", "resolved %s -> %s" % (link, tgt))
        except OSError as e:
            verdict("R01.readlink_%s" % link, "PASS", "%s readlink blocked: %s" % (link, errno_name(e.errno)))


if __name__ == "__main__":
    main()
