#!/usr/bin/env python3
# exit_worker.py - produce a known exit/signal-death for R08.
#
# usage: exit_worker.py <zero|seven|sigterm|sigkill|sigsegv>
# The R08 case runs this inside the agent sandbox and checks the observed
# process exit status (via the sandbox-exec launcher's exit code) against:
#   0      -> 0
#   7      -> 7
#   SIGTERM-> 128+15 = 143
#   SIGKILL-> 128+9  = 137
#   SIGSEGV-> 128+11 = 139
# Death by signal is done self-inflicted (raise) so the sandbox must report
# 128+N; if it squashes the signal the code would differ.

import os
import signal
import sys


def main():
    kind = sys.argv[1] if len(sys.argv) > 1 else "zero"
    if kind == "zero":
        sys.exit(0)
    if kind == "seven":
        sys.exit(7)
    sigs = {
        "sigterm": signal.SIGTERM,
        "sigkill": signal.SIGKILL,
        "sigsegv": signal.SIGSEGV,
    }
    if kind in sigs:
        os.kill(os.getpid(), sigs[kind])
        # not reached: the process dies here; if it survives the sandbox
        # swallowed the signal -> print a marker for the harness to detect
        sys.stdout.write("SURVIVED %s\n" % kind)
        sys.stdout.flush()
        sys.exit(0)
    sys.stderr.write("exit_worker: unknown kind %s\n" % kind)
    sys.exit(2)


if __name__ == "__main__":
    main()
