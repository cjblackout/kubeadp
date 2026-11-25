#!/usr/bin/env python3
# probe_signal.py - R02: attempt to signal the OTHER agent (victim A) and the
# parent daemon (the launcher's parent), using kill / tgkill / pidfd_send_signal.
#
# usage: probe_signal.py <victim-pid> [daemon-pid...]
#   victim-pid : host pid of agent A's root process
#   daemon-pid : extra targets (e.g. a marked sleeper child of the harness that
#                plays the role of the launcher's parent daemon). A delivered
#                signal to IT is just as much a leak.
#
# A delivered signal => FAIL (leak). Blocked => PASS.
# NOTE: we never SIGKILL the victim; SIGTERM is used for A and for the daemon
# we use SIGTERM too. The harness independently verifies A's liveness so we do
# not need to wait here.

import ctypes
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


def _kill(pid, sig, name):
    cid = "R02.kill_%s" % name
    try:
        os.kill(pid, sig)
        verdict(cid, "FAIL", "kill %s delivered to %d" % (name, pid))
    except OSError as e:
        verdict(cid, "PASS", "kill %s blocked: %s" % (name, errno_name(e.errno)))


def _tgkill(pid, sig):
    cid = "R02.tgkill"
    libc = ctypes.CDLL(None, use_errno=True)
    try:
        rc = libc.syscall(234, pid, pid, sig)  # __NR_tgkill x86_64
        if rc == 0:
            verdict(cid, "FAIL", "tgkill SIGTERM delivered to %d" % pid)
        else:
            verdict(cid, "PASS", "tgkill blocked: %s" % errno_name(ctypes.get_errno()))
    except Exception as e:
        verdict(cid, "SKIP", "tgkill syscall unavailable: %s" % e)


def _pidfd(pid, sig):
    cid = "R02.pidfd_send_signal"
    libc = ctypes.CDLL(None, use_errno=True)
    try:
        fd = libc.syscall(434, pid, 0)  # pidfd_open
        if fd < 0:
            verdict(cid, "PASS", "pidfd_open blocked: %s" % errno_name(ctypes.get_errno()))
            return
        meta = (ctypes.c_int * 1)(0)
        rc = libc.syscall(424, fd, sig, meta, 0)
        os.close(fd)
        if rc == 0:
            verdict(cid, "FAIL", "pidfd_send_signal delivered to %d" % pid)
        else:
            verdict(cid, "PASS", "pidfd_send_signal blocked: %s" % errno_name(ctypes.get_errno()))
    except Exception as e:
        verdict(cid, "SKIP", "pidfd unavailable: %s" % e)


def main():
    if len(sys.argv) < 2:
        sys.stdout.write("VERDICT R02.usage SKIP no pid\n")
        return
    victim_pid = sys.argv[1]
    daemons = [p for p in sys.argv[2:] if p.isdigit()]
    if victim_pid.isdigit():
        vp = int(victim_pid)
        # shared victim: use catchable SIGTERM only (the victim traps it and
        # survives); do NOT SIGKILL the shared victim or other cases lose it.
        _tgkill(vp, 15)
        _pidfd(vp, 15)
        # raw kill(SIGTERM)
        try:
            os.kill(vp, 15)
            verdict("R02.kill_SIGTERM", "FAIL", "kill SIGTERM delivered to %d" % vp)
        except OSError as e:
            verdict("R02.kill_SIGTERM", "PASS", "kill SIGTERM blocked: %s" % errno_name(e.errno))
    else:
        sys.stdout.write("VERDICT R02.discover SKIP no victim pid\n")
    # parent daemons (disposable): SIGKILL is fine to attempt here
    for dp in daemons:
        _kill(int(dp), 15, "daemon_SIGTERM")
        _kill(int(dp), 9, "daemon_SIGKILL")
        _tgkill(int(dp), 15)
        _pidfd(int(dp), 15)


if __name__ == "__main__":
    main()
