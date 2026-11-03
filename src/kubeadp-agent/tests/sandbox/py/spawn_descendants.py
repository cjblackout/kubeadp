#!/usr/bin/env python3
# spawn_descendants.py - R07: create a deep descendant tree inside the sandbox
# that must be reaped when the agent's root exits.
#
# usage: spawn_descendants.py <logdir> <tag>
#
# Creates descendants of several kinds, all of which must be GONE within 5
# seconds after the agent root process exits (R07):
#   - a plain child
#   - a GRANDchild of a double fork (detached, reparented to init/subreaper)
#   - a setsid session leader
#   - a nohup-invoked process
#   - a python daemonize (double fork + chdir /)
#
# Every persisted descendant is a small python process that sleeps, and each
# carries a UNIQUE argv marker `__hl86_desc__<tag>-<name>` as one of its argv
# elements, so the marker shows up in /proc/<pid>/cmdline. The harness detects
# surviving descendants by scanning /proc/*/cmdline for that marker, NOT by
# trusting pid files:
#   - on a fresh-pidns runtime (gVisor) the poke-process pids the root saw are
#     namespace-local and disagree with the pod pids the harness sees;
#   - a descendant can re-parent/rename away from its marker file, but the
#     marker bytes in argv survive the process (cmdline is fixed at exec).
#
# The host-side scan uses the marker prefix, so collapse a descendant whose
# actual argv also contains the run tag (it would be picked up by a generic
# tag scan). The marker is deliberately NOT the run secret.

import os
import subprocess
import sys
import time


# code for a python that just sleeps; the marker is passed as an extra argv
# element after "-c CODE" so /proc/<pid>/cmdline carries it.
_SLEEP_CODE = "import time; time.sleep(300)"


def main():
    logdir = sys.argv[1] if len(sys.argv) > 1 else "."
    tag = sys.argv[2] if len(sys.argv) > 2 else "d"

    def marker(name):
        return "__hl86_desc__%s-%s" % (tag, name)

    def spawned(name, pid):
        try:
            with open(os.path.join(logdir, "desc-%s-%s" % (tag, name)), "w") as f:
                f.write("%s\n" % pid)
        except OSError:
            pass
        sys.stdout.write("DESC %s %s\n" % (name, pid))
        sys.stdout.flush()
        return True

    def spawn(kind, argv):
        # argv[0] is the program; marker travels as an argv element after it.
        try:
            p = subprocess.Popen(argv)
            return spawned(kind, p.pid), p
        except FileNotFoundError:
            sys.stdout.write("DESC %s SKIP(missing tool)\n" % kind)
            sys.stdout.flush()
            return False, None

    py = sys.executable

    # plain child: python sleeping, marker in argv
    ok1, p1 = spawn("child", [py, "-c", _SLEEP_CODE, marker("child")])

    # double-fork grandchild: detached, setsid'd, reparented to init/subreaper.
    # The double-forked orphan execs itself into a marker-carrying sleeper so
    # its cmdline carries `__hl86_desc__<tag>-grandchild` for the host-side
    # scan. The grandchild prints its pid (this wrapper's direct child) to
    # stdout so we also record a pid marker file.
    gf = None
    gf_code = (
        "import os,sys,time\n"
        "pid = os.fork()\n"
        "if pid > 0:\n"
        "    os._exit(0)\n"
        "os.setsid()\n"
        "pid2 = os.fork()\n"
        "if pid2 > 0:\n"
        "    os._exit(0)\n"
        "print(os.getpid(), flush=True)\n"
        "os.execv(sys.executable, [sys.executable, '-c', "
        "'import time; time.sleep(300)', " + repr(marker("grandchild")) + "])\n"
    )
    try:
        p2 = subprocess.Popen([py, "-c", gf_code],
                              stdout=subprocess.PIPE, text=True,
                              start_new_session=False)
        try:
            gf = int(p2.stdout.readline().strip())
        except Exception:
            gf = None
    except Exception:
        gf = None
    p2.wait(timeout=10)
    if gf:
        spawned("grandchild", gf)

    # setsid session leader: `setsid python -c CODE M` (setsid execs python)
    ok3, _ = spawn("setsid", ["setsid", py, "-c", _SLEEP_CODE, marker("setsid")])

    # nohup-invoked process: `nohup python -c CODE M`
    ok4, _ = spawn("nohup", ["nohup", py, "-c", _SLEEP_CODE, marker("nohup")])

    # python daemonize: multi-line double-fork body; the final orphan spawns
    # the marker-carrying sleeper and exits, so the sleeper is a fully
    # detached (setsid, chdir /) process reparented to init/subreaper,
    # carrying the marker in its own argv.
    daemon_code = (
        "import os,sys,subprocess,time\n"
        "pid = os.fork()\n"
        "if pid > 0:\n"
        "    os._exit(0)\n"
        "os.setsid()\n"
        "pid2 = os.fork()\n"
        "if pid2 > 0:\n"
        "    os._exit(0)\n"
        "os.chdir('/')\n"
        "os.umask(0)\n"
        "subprocess.Popen([sys.executable, '-c', 'import time; time.sleep(300)', "
        + repr(marker("daemon")) + "])\n"
        "os._exit(0)\n"
    )
    ok5, _ = spawn("daemon", [py, "-c", daemon_code])

    # keep this root alive; the harness will TERM us when done.
    sys.stdout.write("DESCSPAWNED-READY\n")
    sys.stdout.flush()
    try:
        while True:
            time.sleep(300)
    except KeyboardInterrupt:
        pass


if __name__ == "__main__":
    main()
