# Quickstart

This document provides a sequential walkthrough of executing an agent task from start to completion using the agent-run CLI.

## Prerequisites
- Executable `agent-run` client binary.
- Network access to `agent-run-api` (`export AGENT_RUN_REMOTE="http://kubeadp-agent-worker-service.opencode.svc:8002"`).
- Authenticated `agy` credentials on the server instance.

## Execution Workflow
### 1. Job Launch
Submit a task targeting a repository project under the workspace root:
```bash
agent-run start project-a "Update documentation links"
```
Output:
```text
Creating git worktree at /workspace/.agent-jobs/project-a/a1b2c3d4 on branch 'agent-run/a1b2c3d4' (from 'HEAD')...
Memory: recall on, retain on, bank project-a
Job 'a1b2c3d4' started in background.
  PID:      1042
  Worktree: /workspace/.agent-jobs/project-a/a1b2c3d4
  Log:      /home/agent/.agent-run/logs/a1b2c3d4.log
Run 'agent-run status a1b2c3d4' or 'agent-run tail a1b2c3d4' to monitor.
```

### 2. Status Inspection
Inspect execution state and elapsed runtime:
```bash
agent-run status a1b2c3d4
```
Output:
```text
Job ID:       a1b2c3d4
Project:      project-a
Status:       running
Elapsed:      15s
Worktree:     /workspace/.agent-jobs/project-a/a1b2c3d4
Branch:       agent-run/a1b2c3d4
Base Ref:     <commit-sha>
PID:          1042
Model:        <model-id>
Effort:       <low|medium|high>
Conversation: <agy-conversation-uuid>
Memory:       recall on, retain on, bank project-a
Prompt:       <prompt text>
```
`Model`, `Effort` and `Conversation` are printed only when recorded for the job.

### 3. Log Following and Waiting
Follow log output live until a terminal state is reached:
```bash
agent-run tail a1b2c3d4
```
Alternatively, block synchronously until completion:
```bash
agent-run wait a1b2c3d4 --timeout 15m
```

### 4. Agent Response Retrieval
Retrieve the final structured result text:
```bash
agent-run result a1b2c3d4
```

### 5. Worktree Modifications
Review changes within the job worktree:
```bash
agent-run diff a1b2c3d4 --stat
```
Output (Git status code, size in bytes, path):
```text
M  21688  apps/ai-server/DOCS.md
A  14739  docs/agent-run/api-reference.md
```

### 6. Commit Creation
Create a Git commit inside the job worktree:
```bash
agent-run commit a1b2c3d4 -m "docs: update documentation links"
```

### 7. Patch Export
Export committed changes to a local patch file on the client host:
```bash
agent-run export a1b2c3d4 --out ./update.patch
```
Output:
```text
  Wrote ./update.patch  (1 commit(s), 1 file(s), +1 -1, 412 B)
```
Apply locally: `git am ./update.patch && git push origin HEAD`.

### 8. Resource Reclamation
Remove the worktree, branch, and metadata row on the server:
```bash
agent-run clean a1b2c3d4
```
