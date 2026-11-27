# Engine parity record: agy vs claude vs opencode (HL-71 S2, KA-7)

Side-by-side contract record for the three job engines of the kubeadp-agent
stack. It mirrors the capability tables in `src/engine/agy.rs`,
`src/engine/claude.rs`, and `src/engine/opencode.rs` and the black-box suite in
`tests/cli/run-tests.sh`.

## Contract

- Every `/api/v1/jobs/{id}/...` control endpoint answers a usable body for
  ALL THREE engines, gated on the engine's reported capability: `Native` runs in
  the engine, `Emulated` is fronted by the E4 emulation layer (`src/engine/
  emulate.rs`). An endpoint is reported `Unsupported` (501) only where the
  emulation layer cannot honestly front it — none of the shared control
  endpoints below are, for any engine.
- The worker arms every non-native control endpoint as emulated on startup
  (`engine::EngineRegistry::enable_all_emulated`, `worker/entry.rs`), so the
  live server presents the tables recorded here.
- The tables here are enforced end to end by the engine conformance suite
  (`tests/conformance.rs`, see DOCS.md §8), which drives the same endpoints
  through the real `/api/v1` routes and asserts the documented-contract
  parity plus the SPEC.md §5.5 KNOWN_GAPS.

## Per-operation record

| Operation | agy | claude | opencode | Backing implementation (emulated) | Test |
|---|---|---|---|---|---|
| `job.start` / `job.status` / `job.list` / `job.kill` / `job.retry` / `job.events` / `job.result` / `workspace.*` | native | native | native | — | T172–T181 |
| `interrupt` | emulated | emulated | emulated | process-group SIGTERM→SIGKILL via `core::commands::kill`, no kill bookkeeping (`[engine interrupt]` log lines) | T183 |
| `pause` / `resume` | emulated | emulated | emulated | per-run in-process flag + `.engine-message-queue` in the worktree | T183 (agy) |
| `a2a.send_message` (job) | emulated | emulated | emulated | queue to `.engine-message-queue`, drained on the next resume turn (`core/commands/retry.rs`, `[cfg(feature="engine")]`) | T184 |
| `fork` | emulated | emulated | emulated | worktree copy `<parent>/<id>-fork-<seq>` | T185 |
| `compact` | emulated | emulated | emulated | checkpoint + summarise-and-write, then restart via `core::commands::retry` | T185 |
| `revert` (non-empty commit) / unrevert (empty) | emulated | emulated | emulated | `engine/emulate/revert.rs`: checkpoint refs, `reset --hard`, `clean`; unrevert-without-revert is a graceful no-op | T185 |
| `todo` | emulated | emulated | emulated | structured todo list collected from the run's event stream | T186 |

## A2A Injection and Session Mode Verification

- **A2A message injection**:
  - `hook` (`experimental.chat.messages.transform` plugin hook) and `tool_result` (`mcp.a2a` config) are materialised into `~/.config/opencode/` by `worker::opencode_config`. These channels are verified by unit tests, but because they have NOT been exercised against a real live opencode binary in this repository's test suite, their status is documented as partial.
  - `idle_turn`: For sessions, `idle_turn` delivers the message through the opencode server API (`prompt_async`). For running jobs, opencode behaves like claude: a RUNNING job is not idle and has no live stdin channel (`idle_turn` answers `Unsupported`); the `hook` and `tool_result` channels cover the mid-run window, while a finished job is resumed as a retry.
- **Session mode**:
  - Sessions supervise `opencode serve` and deliver messages through `prompt_async`. Proven by the orchestrator tests T74–T78 (fake server) and by a live run with the real opencode 1.19.4 and a mock LLM: user message and A2A-framed message reached the model, and the same opencode session continued after stop/restart. Not yet proven: sandboxed sessions and the in-CI black-box run.

`permissions` is deliberately absent (no `Engine` trait method yet): an engine
without a native permission surface reports it `Unsupported`, it is never
claimed emulated.

## Route surface

`POST /api/v1/jobs/{id}/interrupt|pause|message|fork|compact|revert` and
`GET /api/v1/jobs/{id}/todo` (plus the `agent-run`
`interrupt/pause/message/fork/compact/revert/todo` subcommands) all resolve the
job row on demand through the same emulation wrapper the run surface uses, so
agy, claude, and opencode answer identically never a 501 because of the engine.

## Rust unit parity test

`src/engine/agy.rs::agy_claude_job_control_parity_after_enable_all_emulated`
asserts that, after the worker's `enable_all_emulated`, the agy, claude, and
opencode capability tables agree on the emulated status of every shared
job-control endpoint.
