# agent-run Documentation

This document outlines the architecture, components, navigation structure, and terminology of the agent-run system.

agent-run provides background execution of agy coding agents inside isolated Git worktrees against local repositories. The architecture decouples client invocation from server execution over an HTTP API, providing job isolation, process supervision, and status inspection.

## Components

| Crate | Role | Binary |
| :--- | :--- | :--- |
| `cli` module (formerly `agent-run-cli`) | Command-line HTTP client shim | `agent-run` |
| `core` module (formerly `agent-core`) | Shared execution engine, Git worktree manager, and state repository | None (library) |
| `agent-run-api` | HTTP API daemon, process monitor, and zombie reaper | `agent-run-api` |

## Documentation Map

| Page | Target audience | Content |
| :--- | :--- | :--- |
| [quickstart.md](quickstart.md) | Users | End-to-end command walkthrough from job launch to patch export |
| [installation.md](installation.md) | Operators, Developers | Build instructions, container images, and Kubernetes deployment |
| [architecture.md](architecture.md) | Systems engineers | Process supervision, job state machine, worktrees, and Hindsight |
| [cli-reference.md](cli-reference.md) | Users, Script authors | Command syntax, arguments, options, exit codes, and examples |
| [api-reference.md](api-reference.md) | Integration engineers | HTTP endpoints, wire schemas, SSE events, and error taxonomy |
| [configuration.md](configuration.md) | Operators | Settings, precedence hierarchy, defaults, and proxy configuration |
| [operations.md](operations.md) | Operators | Health monitoring, retention policies, garbage collection, and recovery |
| [security.md](security.md) | Security engineers | Threat boundaries, network policies, credential storage, and sandboxing |
| [limitations.md](limitations.md) | Architects, Users | Verified architectural constraints and workarounds |
| [troubleshooting.md](troubleshooting.md) | Operators, Users | Diagnostic table, error codes, and operational recovery procedures |
| [development.md](development.md) | Contributors | Source layout, test harnesses, and documentation contracts |

## Terminology

- **Job**: A single execution attempt of `agy` identified by an 8-character hexadecimal string, tracking prompt text, process metadata, and lifecycle status.
- **Worktree**: A linked Git worktree created under `<workspace_root>/.agent-jobs/<project>/<job_id>` dedicated to an individual job.
- **Server**: The `agent-run-api` HTTP daemon executing `agy` jobs, managing the SQLite state database, and supervising processes.
- **Client**: The `agent-run` command-line utility dispatching requests to the server over HTTP.
