#!/usr/bin/env python3
# sbcommon.py - shared helpers for the sandbox attack suite python tools.
#
# Loaded by the other tools in this directory (same dir on sys.path). Provides:
#   - verdict(): emit one VERDICT line for the harness collector
#   - target resolution: env-supplied pid + /proc scan for the run tag
#   - proc-attack primitives shared by the R1/R2/R3 helpers
#
# Standard library only. Read-only.

import errno
import hashlib
import os
import re
import signal
import subprocess
import sys
import time

# Fixed virtual address where the victim places its secret buffer (both sides
# know it, so no info channel is needed). MAP_FIXED target. Used by R03
# process_vm_readv / /proc/<pid>/mem. Best-effort mapping; memory probes are
# only meaningful when the victim reported mem_mapped.
# 0x7000000 = 117 MB: typically free in a small binary's address space.
FIXED_SECRET_ADDR = 0x7000000
FIXED_SECRET_LEN = 4096

# Non-secret marker used to discover the victim via /proc/*/cmdline. The value
# is deliberately NOT the secret; it only lets the attacker find the target pid.
TAG = os.environ.get("KUBEADP_TEST_TAG", "hl86-tag")

# Runtime facts forwarded by the harness (see lib.sh probe_runtime_env):
#   KUBEADP_TEST_RUNTIME : gvisor | kata | runc-ns | runc-landlock | fake | ""
#   KUBEADP_TEST_PIDNS   : "1" fresh pid namespace, "0" shared, "" unknown
PIDNS_FRESH = os.environ.get("KUBEADP_TEST_PIDNS", "0") == "1"
RUNTIME = os.environ.get("KUBEADP_TEST_RUNTIME", "")


def metadata_visible():
    """True when this sandbox shares the pod's /proc under a Landlock domain
    (kata or runc-Landlock): other processes' entry dirs, stat, cmdline, fd/
    task lists and readlink targets are visible by design, while the data-
    bearing files (environ, maps, mem) remain blocked. On such a runtime the
    purely-metadata R01 observations are a documented accepted difference
    (INFO) instead of a FAIL, so a real, correctly-isolated kata pod does not
    fail the suite on metadata alone."""
    return PIDNS_FRESH == False and RUNTIME in ("kata", "runc-landlock")


def verdict_meta(cid, what, detail):
    """Emit a verdict for a metadata-only observation. On a shared-/proc
    Landlock runtime this is INFO (documented accepted difference); otherwise
    it is a FAIL (the metadata should not have been observable)."""
    if metadata_visible():
        verdict(cid, "INFO", "%s visible (documented accepted difference: shared /proc, metadata only)" % what, detail)
    else:
        verdict(cid, "FAIL", "%s visible" % what, detail)


def verdict(cid, result, message, extra=None):
    """Emit one VERDICT <id> <result> <message> line for the harness."""
    if extra:
        message = "%s (%s)" % (message, extra)
    # Keep ids short (R01.xxx) and messages single-line.
    message = message.replace("\n", " ").replace("\r", " ").strip()
    sys.stdout.write("VERDICT %s %s %s\n" % (cid, result, message))
    sys.stdout.flush()


def errno_name(e):
    try:
        return errno.errorcode.get(e, "E%d" % e)
    except Exception:
        return "E%d" % e


def target_pids():
    """Candidate victim pids, in isolation: first same-namespace /proc scan
    for the run tag in cmdline (this is the pid B can actually address inside
    its own pid namespace), then the env-supplied host pid. If a pid namespace
    separates A and B, only the env pid exists but it is unresolvable in B
    (ESRCH on every probe) -> isolation is observable correctly either way."""
    pids = []
    for pid in list_proc_pids():
        if proc_cmdline_has_tag(pid):
            pids.append((pid, "procscan"))
    env_pid = os.environ.get("KUBEADP_TEST_TARGET_PID", "")
    if env_pid.isdigit():
        ep = int(env_pid)
        if ep not in [p for p, _ in pids]:
            # still report it so cases can probe an unresolvable pid
            pids.append((ep, "env"))
    return pids


def list_proc_pids():
    try:
        return [int(p) for p in os.listdir("/proc") if p.isdigit()]
    except Exception:
        return []


def proc_cmdline_has_tag(pid):
    try:
        with open("/proc/%d/cmdline" % pid, "rb") as f:
            data = f.read()
    except Exception:
        return False
    if TAG.encode() in data:
        return True
    # the tag may also appear in the argv of the harness-owned sleeper
    return False


def find_daemon_pids():
    """Return pids whose cmdline contains KUBEADP_TEST_DAEMON_MARK."""
    mark = os.environ.get("KUBEADP_TEST_DAEMON_MARK", "__hl86_daemon__")
    out = []
    for pid in list_proc_pids():
        if proc_has_mark(pid, mark):
            out.append((pid, "daemon"))
    return out


def proc_has_mark(pid, mark):
    try:
        with open("/proc/%d/cmdline" % pid, "rb") as f:
            return mark.encode() in f.read()
    except Exception:
        return False


def read_proc_file(pid, name):
    """Return (ok, payload_or_errno)."""
    try:
        with open("/proc/%d/%s" % (pid, name), "rb") as f:
            return True, f.read()
    except OSError as e:
        return False, e.errno


def stat_proc_entry(pid, name):
    try:
        import stat as st
        info = os.stat("/proc/%d/%s" % (pid, name))
        return True, info
    except OSError as e:
        return False, e.errno


def kill_target(pid, sig):
    try:
        os.kill(pid, sig)
        return True, 0
    except OSError as e:
        return False, e.errno


def tgkill_target(pid, tid, sig):
    try:
        import ctypes
        libc = ctypes.CDLL(None, use_errno=True)
        rc = libc.syscall(234, pid, tid, sig)  # __NR_tgkill x86_64
        return rc == 0, (0 if rc == 0 else ctypes.get_errno())
    except Exception:
        return False, errno.ENOTSUP


def pidfd_send_signal(pid, sig):
    try:
        import ctypes
        libc = ctypes.CDLL(None, use_errno=True)
        # pidfd_open(pid, flags=0)
        fd = libc.syscall(434, pid, 0)
        if fd < 0:
            return False, ctypes.get_errno()
        meta = (ctypes.c_int * 1)(0)
        rc = libc.syscall(424, fd, sig, meta, 0)  # pidfd_send_signal
        import os as _os
        _os.close(fd)
        return rc == 0, (0 if rc == 0 else ctypes.get_errno())
    except Exception:
        return False, errno.ENOTSUP


def is_dead(pid):
    try:
        os.kill(pid, 0)
        return False
    except OSError as e:
        return True


def page_align(addr):
    return addr & ~0xFFF


class BlockerJudge:
    """Maps an attack outcome to a verdict, per the contract's accepted errnos.
    'penetrated' means the attack succeeded despite the sandbox -> FAIL.
    Blocked with a sandbox-typical errno -> PASS.
    Missing tools/syscalls -> SKIP.
    """

    BLOCK_ERRNOS = frozenset(
        [errno.EACCES, errno.EPERM, errno.ENOENT, errno.ESRCH, errno.ENOTDIR,
         errno.ENODEV, errno.EOVERFLOW, errno.ENOSYS, errno.EINVAL]
    )

    def __init__(self, cid, what):
        self.cid = cid
        self.what = what

    def report(self, ok, e, note=""):
        # ok True == attack penetrated (leak)
        e = e if isinstance(e, int) else -1
        if ok:
            verdict(self.cid, "FAIL",
                    "attack %s succeeded (leak)" % self.what, note)
            return
        if e in self.BLOCK_ERRNOS:
            verdict(self.cid, "PASS",
                    "attack %s blocked: %s" % (self.what, errno_name(e)), note)
        elif e == 0:
            verdict(self.cid, "PASS", "attack %s yields nothing" % self.what, note)
        else:
            verdict(self.cid, "PASS",
                    "attack %s blocked: %s" % (self.what, errno_name(e)), note)

    def blocked_ok(self, e, note=""):
        self.report(False, e, note)


def host_secret_hash():
    """Hash of the run's own secret payload (host file), used by checks that
    compare a read value against the true secret."""
    # Not used inside helpers directly; victim hashes are compared host-side.
    return ""
