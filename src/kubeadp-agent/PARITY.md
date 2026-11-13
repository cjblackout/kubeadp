# Engine parity record: agy vs claude (HL-71 S2)

Side-by-side contract record for the two job engines of the kubeadp-agent
stack. It mirrors the capability tables in `src/engine/agy.rs` and
`src/engine/claude.rs` and the black-box suite in `tests/cli/run-tests.sh`.

## Contract

- Every `/api/v1/jobs/{id}/...` control endpoint answers a usable body for
  BOTH engines, gated on the engine's reported capability: `Native` runs in
  the engine, `Emulated` is fronted by the E4 emulation layer (`src/engine/
  emulate.rs`). An endpoint is reported `Unsupported` (501) only where the
  emulation layer cannot honestly front it — none of the shared control
  endpoints below are, for either engine.
- The worker arms every non-native control endpoint as emulated on startup
  (`engine::EngineRegistry::enable_all_emulated`, `worker/entry.rs`), so the
  live server presents the tables recorded here.
- The tables here are enforced end to end by the engine conformance suite
  (`tests/conformance.rs`, see DOCS.md §8), which drives the same endpoints
  through the real `/api/v1` routes and asserts the documented-contract
  parity plus the SPEC.md §5.5 KNOWN_GAPS.

## Per-operation record

| Operation | agy | claude | Backing implementation (emulated) | Test |
|---|---|---|---|---|
| `job.start` / `job.status` / `job.list` / `job.kill` / `job.retry` / `job.events` / `job.result` / `workspace.*` | native | native | — | T172–T181 |
| `interrupt` | emulated | emulated | process-group SIGTERM→SIGKILL via `core::commands::kill`, no kill bookkeeping (`[engine interrupt]` log lines) | T183 |
| `pause` / `resume` | emulated | emulated | per-run in-process flag + `.engine-message-queue` in the worktree | T183 (agy) |
| `a2a.send_message` (job) | emulated | emulated | queue to `.engine-message-queue`, drained on the next resume turn (`core/commands/retry.rs`, `[cfg(feature="engine")]`) | T184 |
| `fork` | emulated | emulated | worktree copy `<parent>/<id>-fork-<seq>` | T185 |
| `compact` | emulated | emulated | checkpoint + summarise-and-write, then restart via `core::commands::retry` | T185 |
| `revert` (non-empty commit) / unrevert (empty) | emulated | emulated | `engine/emulate/revert.rs`: checkpoint refs, `reset --hard`, `clean`; unrevert-without-revert is a graceful no-op | T185 |
| `todo` | emulated | emulated | structured todo list collected from the run's event stream | T186 |

`permissions` is deliberately absent (no `Engine` trait method yet): an engine
without a native permission surface reports it `Unsupported`, it is never
claimed emulated.

## Route surface

`POST /api/v1/jobs/{id}/interrupt|pause|message|fork|compact|revert` and
`GET /api/v1/jobs/{id}/todo` (plus the `agent-run`
`interrupt/pause/message/fork/compact/revert/todo` subcommands) all resolve the
job row on demand through the same emulation wrapper the run surface uses, so
agy and claude answer identically never a 501 because of the engine.

## Rust unit parity test

`src/engine/agy.rs::agy_claude_job_control_parity_after_enable_all_emulated`
asserts that, after the worker's `enable_all_emulated`, the agy and claude
capability tables agree on the emulated status of every shared job-control
endpoint.
