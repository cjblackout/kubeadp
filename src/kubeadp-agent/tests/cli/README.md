# agent-run tests

This directory contains integration test scripts and the end-to-end black-box test harness for `agent-run`. The suite validates client-server interactions, job lifecycles, Git worktree isolation, and failure recovery against a mock agent runner (`fake-agy.sh`). Run the full verification suite with `bash tests/cli/run-all.sh`, or execute the black-box harness directly via `bash tests/cli/run-tests.sh`. Comprehensive testing workflows and environment options are documented in [docs/agent-run/development.md](../../../docs/agent-run/development.md).
