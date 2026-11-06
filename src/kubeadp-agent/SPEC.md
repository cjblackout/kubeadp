# kubeadp-agent specification

Normative design of the agent mesh. Behaviour and operation of what exists are described in [DOCS.md](DOCS.md); this file states what the system must do.

## 1. Mesh (jobset)

A **mesh** is a jobset: the set of sessions and jobs, of any engine, that talk to each other in both directions.

1. **Two-way.** Every member can send to every other member, directly into the receiver's context.
2. **Send side.** The MCP tools of `kubeadp-agent mcp-a2a`: `list_agents`, `send_message`, `reply`, `get_task`.
3. **Receive side.** The wrapper harness delivers the message as the receiver's next input and frames it as data (claimed sender, hop count, per-message nonce), never as an instruction:
   - an orchestrator session receives it as a bracketed paste into its terminal;
   - a running job receives it as its next turn (a running attempt is stopped cleanly and resumed with the message);
   - a **finished** job (`done`, `failed`, `self-terminated`, `incomplete`) that has a conversation id is resumed automatically, as a retry whose prompt is the framed message.
4. **Addressing.** A session is addressed by its session name (A2A context id, for example `homelab`), a job by its job id (A2A task id). The per-job sender is `<pod>/<job id>`. Sessions and jobs register in the registry under exactly these ids.

## 2. Membership

1. A jobset is created by the process that starts its first member. A process started by a member joins that member's jobset (`agent-run start --jobset`, default `KUBEADP_JOBSET_ID`). An operator may name a jobset.
2. A job records the jobset it was started into with its state; every retry and every resume rejoins the same jobset.
3. Ending a jobset bumps its epoch and revokes every credential of it.
4. Membership is the only credential. The sender name in a message is claimed, not authenticated.

## 3. Security

1. **Credential.** The per-jobset secret is `HMAC(root key, jobset id, epoch)`. It is handed to members through the sandbox secret channel (a 0400 file whose path is in `KUBEADP_JOBSET_SECRET_FILE`), never in argv or the environment. It is compared in constant time.
2. **Hidden material.** The jobset root key file and the operator API token are hidden from every sandboxed agent. Jobs never hold the operator token.
3. **Verification.** The credential is verified against the addressed target: the target must be a live member of the presented jobset (or a finished job whose remembered jobset is the presented one), at the current epoch. A request that names no target needs a valid credential of a live member of the presented jobset.
4. **Authentication runs first.** On every route except the public agent card, authentication runs before any body parsing; an unknown target, a wrong secret, a missing credential and an ended jobset all answer `401` (`application/problem+json`, `WWW-Authenticate`). The body read for target resolution is capped at 64 KiB.
5. **Limits.** Hop limit 4, per-sender and per-job rate limits, size cap before framing.
6. **Outer layer.** Network policy and SPIRE mutual authentication.

## 4. Discovery

1. Agents with a ServiceAccount token discover peers by Service label `a2a.homelab/agent=true`.
2. Sandboxed agents without a token use a static peer list (`A2A_PEERS`, `name=url`, validated).
3. `send_message` and `get_task` accept registry names only, never arbitrary URLs.
4. The agent card name is the pod name (`A2A_SELF_NAME`, from the downward API).

## 5. Sandbox

1. Backends: `gvisor`, `kata`, `runc`, `trusted`. Network modes: `host`, `shared`, `relay` (fails closed).
2. The mesh works under every backend and every network mode: the credential channel, the peer list and the relay must not depend on the backend.

## 6. Acceptance

1. A session starts a job in its jobset; they exchange 10 messages each way. Each side's messages arrive framed in the other's context; replies sent with the job's `send_message` arrive in the session without polling.
2. A message to the job after it finished resumes it, and the job answers.
3. A non-member is refused with `401`: a job started without the jobset, a wrong secret, an ended jobset, an unknown target.
4. The same run passes with every sandbox backend and network mode.
