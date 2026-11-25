# kubeadp-agent sandbox attack suite

Black-box isolation tests for the `kubeadp-agent sandbox-exec` launcher,
covering the isolation requirements of the sandbox contract. The suite only
observes the binary's behaviour from the outside (exit codes, files created,
process tables) -- it never inspects the sandbox implementation.

## Layout

```
run.sh          harness (runs unsandboxed in the pod)
lib.sh          helpers sourced by run.sh (policy generation, launchers, verdicts)
lib_case.sh     helpers sourced by the in-sandbox case scripts
cases/          one script per requirement (R01..R11), run inside agent B
py/             python helpers run inside the agents (probes, victim, descendants)
fake/           kubeadp-agent stub used as a negative control (no isolation)
```

## Running against the real binary

Use `run.sh` with no extra flags inside a pod that has `kubeadp-agent` on PATH:

```
bash src/kubeadp-agent/tests/sandbox/run.sh
```

The contract command is `kubeadp-agent sandbox-exec --policy <file|-> -- <cmd>`.
There is no `--runtime` / `--mode` flag: the runtime (gvisor/kata/runc) is
auto-detected inside the pod and isolation is always applied. If isolation
cannot be established the launcher exits `125` and the command never runs
(fail closed). The suite verifies this directly (R09).

Optional flags:

- `--binary <path>` use another binary (e.g. the fake stub).
- `--only R1,R7` run a subset of cases.
- `--keep` keep the scratch directory instead of cleaning it in the trap
  (the scratch path is always printed at start, e.g. under `/tmp/hl86-*`).

## The Python tools

Copied into each agent's own rw dir at start (`tools/`). The `sandbox-exec`
launchers forward everything under `KUBEADP_TEST_*` as environment, so no
secret ever travels in argv; secrets travel on fd 3 like the real contract.

## Stub control (self-test without the real binary)

The negative control proves the suite can *detect* leaks. It needs no pod:

```
bash src/kubeadp-agent/tests/sandbox/run.sh \
  --binary src/kubeadp-agent/tests/sandbox/fake/kubeadp-agent
```

`fake/kubeadp-agent` implements just enough of the CLI (`sandbox-exec
--policy P -- CMD`, `sandbox-probe [--json]`) to run the suite: it validates
the policy, fails closed with `125` on an invalid policy (missing file,
malformed JSON, unreadable file, or an uncreatable `rw` path), sets up a
per-agent private tmp with a 0400 `secret` file, and then **plain-execs** the
command -- it provides NO isolation.

Against the stub the leak checks (R1, R2, R3, R4, R6, R7, R11) FAIL and are
marked `[CONTROL]` (expected; excluded from the exit code), while the positive
cases R5 (own env/dirs), R8 (exit codes) and R10 (network) PASS. The suite
then exits 0, which is what the header states.

## Known limitation (not tested as a leak)

R10 keeps the shared network namespace, so under the default `net: host` an agent can connect to
ABSTRACT unix-domain sockets of other processes in the pod (the Kata backend blocks this with
Landlock `LANDLOCK_SCOPE_ABSTRACT_UNIX_SOCKET`). Every backend closes this with the opt-in
policy value `"net": "relay"` (own loopback-only network namespace + unix-socket relay proxy;
the default `"net": "host"` and the legacy `"shared"` alias keep today's shared behaviour).
There is no isolation test for the shared-namespace case: it is a documented property of the
default network mode, and the pod must not run abstract socket listeners (or must set `net: relay`).

## Exit code semantics

The harness exits 0 iff there are no hard (non-control) FAILs; it exits 1
when a real leak is detected or the victim never becomes ready, and 2 on a
usage error (bad option, binary/python3 missing).

## Runtime awareness (gvisor vs kata/runc-Landlock)

The sandbox gives the agent different /proc views depending on the backend,
and the suite must not call a correctly-isolated pod a leak just because the
view differs:

- **gvisor / runc-namespaces**: the agent runs in a FRESH PID namespace with a
  fresh /proc mount. It cannot see the other agent's pids at all, and the host
  pid the harness would hand over is unresolvable inside B by definition. The
  harness sets `KUBEADP_TEST_PIDNS=1`, cases stop falling back to the env pid
  (`tp()`), and a missing target is PASS (isolation), not SKIP.
- **kata / runc-Landlock**: the agent shares the pod's /proc under a Landlock
  domain. Data-bearing files of other processes (environ, maps, mem,
  task/*/environ) stay blocked (Landlock rule against /proc does not grant the
  read data), so those stay hard PASS/FAIL checks. The purely-*metadata*
  observations (stat, cmdline, fd/task listings, readlink targets, the visible
  process table itself) are a DOCUMENTED accepted difference: they are emitted
  as INFO (`verdict_meta`), not FAIL, because Landlock cannot hide the shared
  /proc listing. The fake stub (no isolation) never gets this downgrade, so its
  metadata checks stay FAIL [CONTROL].

Both `KUBEADP_TEST_RUNTIME` and `KUBEADP_TEST_PIDNS` are derived once from the
binary's own `sandbox-probe --json` when the harness starts.

## R07: descendant reaping is detected by argv marker

`spawn_descendants.py` installs a unique per-descendant argv marker
`__hl86_desc__<tag>-<name>` in every process it spawns (including the
detached double-fork and setsid kinds). The host-side `case_R7` finds surviving
descendants by scanning `/proc/*/cmdline` for the marker, not by trusting the
pid marker files the spawner writes:

- on gVisor the pids the spawner saw are namespace-local and differ from the
  pod pids, so pid-file based liveness checks would be wrong;
- a descendant can reparent/rename away from its marker file, but the marker
  bytes in argv survive (cmdline is fixed at exec).

The cleanup that kills genuine leaks also matches by the marker prefix, so it
never touches the victim or other cases (which do not carry the prefix).

## R12 (described, not wired into the runner): net=relay closes the shared network

A black-box case for the opt-in `"net": "relay"` policy value (default off) —
a new numbered case description, not a runner change:

- Launch agent B with a policy carrying `"net": "relay"` and, on the host
  side, serve an ABSTRACT unix-domain socket: B must fail to connect it (B
  runs in its own network namespace).
- B must have no non-loopback interface: a raw TCP connect (and DNS) to an
  outside address must fail (no direct egress).
- A proxy-aware client in B (env `HTTP_PROXY`/`HTTPS_PROXY` =
  `http://127.0.0.1:3128`, or `GIT_SSH_COMMAND` with the shipped
  `relay-connect` ProxyCommand) must reach an outside TCP echo only through
  the relay path, while loopback (`127.0.0.1`, `::1`) and link-local
  (`169.254.0.0/16`) destinations are refused by the proxy.
- A second agent B2 with the default policy (`"net": "host"`) must behave
  exactly as today (R10): abstract sockets reachable, direct egress working.

The suite does not run this case; it needs a relay-capable build and a pod
whose backend supports `net: relay` (gVisor, or kata/runc-Landlock with
unprivileged user namespaces -- otherwise it fails closed, exit `125`).
