#!/usr/bin/env python3
# probe_ptrace.py - R03: try to ptrace / process_vm_readv the victim.
#
# usage: probe_ptrace.py <pid>
# Attempts (all must fail / be blocked in a sandbox):
#   PTRACE_ATTACH, PTRACE_SEIZE, process_vm_readv, pidfd_getfd (fd 2)
# Emits VERDICT lines. On a non-sandbox environment these succeed (FAIL), so
# this same script powers the negative control against the fake stub.

import ctypes
import errno
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
try:
    from sbcommon import verdict, errno_name, FIXED_SECRET_ADDR, FIXED_SECRET_LEN
except Exception:
    FIXED_SECRET_ADDR = 0x10000000000
    FIXED_SECRET_LEN = 4096
    def verdict(cid, res, msg, extra=None):
        sys.stdout.write("VERDICT %s %s %s\n" % (cid, res, msg))
        sys.stdout.flush()
    def errno_name(e):
        return "E%d" % e


PTRACE_SEIZE = 0x4206
PTRACE_ATTACH = 16
PTRACE_DETACH = 0x11


def main():
    if len(sys.argv) < 2 or not sys.argv[1].isdigit():
        sys.stdout.write("VERDICT R03.usage SKIP no pid\n")
        return
    pid = int(sys.argv[1])
    libc = ctypes.CDLL(None, use_errno=True)

    # PTRACE_ATTACH
    try:
        rc = libc.ptrace(PTRACE_ATTACH, pid, 0, 0)
        if rc == 0:
            os.waitpid(pid, os.WNOHANG)
            libc.ptrace(PTRACE_DETACH, pid, 0, 0)
            verdict("R03.ptrace_attach", "FAIL", "ptrace attach succeeded")
        else:
            e = ctypes.get_errno()
            verdict("R03.ptrace_attach", "PASS", "ptrace attach blocked: %s" % errno_name(e))
    except Exception as ex:
        verdict("R03.ptrace_attach", "SKIP", "ptrace unavailable: %s" % ex)

    # PTRACE_SEIZE
    try:
        rc = libc.ptrace(PTRACE_SEIZE, pid, 0, 0)
        if rc == 0:
            libc.ptrace(PTRACE_DETACH, pid, 0, 0)
            verdict("R03.ptrace_seize", "FAIL", "ptrace seize succeeded")
        else:
            e = ctypes.get_errno()
            verdict("R03.ptrace_seize", "PASS", "ptrace seize blocked: %s" % errno_name(e))
    except Exception as ex:
        verdict("R03.ptrace_seize", "SKIP", "ptrace seize unavailable: %s" % ex)

    # process_vm_readv at FIXED_SECRET_ADDR
    try:
        buf = ctypes.create_string_buffer(FIXED_SECRET_LEN)
        class iovec(ctypes.Structure):
            _fields_ = [("base", ctypes.c_void_p), ("len", ctypes.c_size_t)]
        local = (iovec * 1)(iovec(ctypes.cast(buf, ctypes.c_void_p), FIXED_SECRET_LEN))
        remote = (iovec * 1)(iovec(FIXED_SECRET_ADDR, FIXED_SECRET_LEN))
        rc = libc.process_vm_readv(pid, local, 1, remote, 1, 0)
        if rc > 0:
            verdict("R03.process_vm_readv", "FAIL", "process_vm_readv read %d bytes" % rc)
        else:
            e = ctypes.get_errno()
            verdict("R03.process_vm_readv", "PASS", "process_vm_readv blocked: %s" % errno_name(e))
    except Exception as ex:
        verdict("R03.process_vm_readv", "SKIP", "process_vm_readv unavailable: %s" % ex)

    # pidfd_getfd (dup fd 2 of victim)
    try:
        pidfd = libc.syscall(434, pid, 0)  # pidfd_open
        if pidfd < 0:
            e = ctypes.get_errno()
            verdict("R03.pidfd_getfd", "PASS", "pidfd_open blocked: %s" % errno_name(e))
        else:
            fd = libc.syscall(438, pidfd, 2, 0)  # pidfd_getfd
            if fd < 0:
                e = ctypes.get_errno()
                os.close(pidfd)
                verdict("R03.pidfd_getfd", "PASS", "pidfd_getfd blocked: %s" % errno_name(e))
            else:
                verdict("R03.pidfd_getfd", "FAIL", "pidfd_getfd dup'd victim fd 2 (%d)" % fd)
                os.close(fd)
            os.close(pidfd)
    except Exception as ex:
        verdict("R03.pidfd_getfd", "SKIP", "pidfd_getfd unavailable: %s" % ex)


if __name__ == "__main__":
    main()
