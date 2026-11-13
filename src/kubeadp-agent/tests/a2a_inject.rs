//! Parity contract tests of the harness-injected A2A delivery (design
//! HL-113 §7.14): the same suite against both engines through the
//! engine capability vocabulary, with fake engines / fakes, hermetic (no
//! Docker, no live engine process, passing without Landlock).
//!
//! The contracts (a)-(h) of the design, as far as phase 1 reaches (the
//! delivery state machine itself is phase 2):
//! (a) a member blocked in `wait_message` gets the message within 2 s and
//!     nothing is killed;
//! (b) a busy member is not killed and gets the message at the next boundary
//!     (the hook after the tool call; the tool result of its next A2A tool);
//! (c) an idle member gets an `idle_turn` (the channel is declared per
//!     engine and injects; the ladder timing is phase 2);
//! (d) kill-and-resume only with `interrupt: true` or the force timeout
//!     (nothing in phase 1 kills: the channels deliver without a kill);
//! (e) exactly once under retries and concurrent senders;
//! (f) queue bound and TTL;
//! (g) sender task state submitted -> working on injection (the inbox state
//!     machine that backs it);
//! (h) `a2a-hook` output format per event.
//!
//! The phase-2 contracts (the escalation ladder, design §7.14):
//! (i) an idle member gets an `idle_turn` after `a2a_ack_secs` (the ladder
//!     timing half of (c)), for both engines;
//! (j) kill-and-resume only with `interrupt: true` or the force timeout (the
//!     ladder timing half of (d); the default never kills);
//! (k) TTL expiry fails the sender task with a clear reason;
//! (l) the sender task state mapping (submitted -> working on injection);
//! (m) the agy hook JSON per event and the plugin/hook-settings writer
//!     structures (covered in the module unit tests of inject.rs,
//!     agy_plugin.rs and claude_config.rs).
#![cfg(feature = "a2a")]

use std::sync::{Arc, Mutex};
use std::time::Duration;

use a2a::TaskState;
use async_trait::async_trait;
use kubeadp_agent::core::a2a::client::build_ctx;
use kubeadp_agent::core::a2a::inbox::{
    EnqueueOutcome, Inbox, QueuedMessage, DEFAULT_TTL_SECS, MAX_QUEUED_BYTES, MAX_QUEUED_MSGS,
    NOTE_SENDER,
};
use kubeadp_agent::core::a2a::inject::{join_framed, run_a2a_hook};
use kubeadp_agent::engine::capability::{inject_channels, CapabilityTable, InjectChannel};
use kubeadp_agent::engine::{
    Engine, EngineCtx, EngineError, JobSpec, Run, RunId, RunStatus, SessionSpec, Transport,
};
use kubeadp_agent::mcp_a2a::A2aMcpServer;
use rmcp::model::CallToolRequestParams;
use rmcp::ServiceExt;
use tempfile::TempDir;

// ---------------------------------------------------------------------------
// Fake engines (the parity suite runs the same assertions against both)
// ---------------------------------------------------------------------------

/// A fake engine that records every kill/interrupt: the "nothing is killed"
/// half of the parity contracts.
struct FakeEngine {
    id: &'static str,
    kills: Mutex<Vec<String>>,
}

impl FakeEngine {
    fn new(id: &'static str) -> Arc<Self> {
        Arc::new(Self {
            id,
            kills: Mutex::new(Vec::new()),
        })
    }

    fn kill_count(&self) -> usize {
        self.kills.lock().unwrap().len()
    }
}

#[async_trait]
impl Engine for FakeEngine {
    fn id(&self) -> kubeadp_agent::engine::EngineId {
        self.id.to_string()
    }

    async fn start_job(
        &self,
        _spec: JobSpec,
        _ctx: &mut dyn EngineCtx,
    ) -> Result<RunId, EngineError> {
        Ok(format!("{}-job", self.id))
    }

    async fn resume_job(&self, _run: &Run, _prompt: &str) -> Result<(), EngineError> {
        Ok(())
    }

    async fn job_status(&self, run: &Run) -> Result<RunStatus, EngineError> {
        Ok(run.status)
    }

    async fn start_session(
        &self,
        _spec: SessionSpec,
        _ctx: &mut dyn EngineCtx,
    ) -> Result<RunId, EngineError> {
        Ok(format!("{}-session", self.id))
    }

    async fn send_message(&self, _run: &Run, _text: &str) -> Result<(), EngineError> {
        Ok(())
    }

    async fn stop_session(&self, _run: &Run, _restart: bool) -> Result<(), EngineError> {
        Ok(())
    }

    async fn interrupt(&self, run: &Run) -> Result<(), EngineError> {
        self.kills.lock().unwrap().push(run.id.clone());
        Ok(())
    }

    fn capabilities(&self) -> CapabilityTable {
        CapabilityTable::builder(self.id)
            .transport(Transport::StreamJson)
            .native(&["a2a.inject"])
            .build()
    }
}

fn queued_msg(id: &str, text: &str) -> QueuedMessage {
    QueuedMessage {
        message_id: id.to_string(),
        sender: "peer-agent".to_string(),
        hop: 1,
        context_id: Some("ctx-1".to_string()),
        task_id: None,
        framed: format!("--- A2A INCOMING MESSAGE ---\n{text}"),
        queued_at: kubeadp_agent::core::a2a::inbox::now_unix(),
    }
}

/// The MCP server under test, with an explicit inbox (hermetic; `new` reads
/// the env) and a fake-discovery client context.
async fn mcp_server_with_inbox(inbox: Inbox) -> A2aMcpServer {
    let list_services = |_base: &str, _token: Option<&str>| {
        let peer = serde_json::json!({
            "metadata": { "name": "peer", "namespace": "testns", "labels": {
                "a2a.homelab/agent": "true" } },
            "spec": { "ports": [ { "name": "a2a", "port": 8002 } ] }
        });
        async move { Ok(vec![peer]) }
    };
    let ctx = build_ctx(None, list_services).await.expect("build ctx");
    A2aMcpServer::with_inbox(
        ctx,
        Some(kubeadp_agent::core::a2a::receive::ReceiveInbox::Local(
            inbox,
        )),
    )
}

/// Drive the in-memory MCP transport around a server.
async fn mcp_client(
    server: A2aMcpServer,
) -> rmcp::service::RunningService<rmcp::service::RoleClient, ()> {
    let (server_transport, client_transport) = tokio::io::duplex(8192);
    tokio::spawn(async move {
        if let Ok(running) = server.serve(server_transport).await {
            let _ = running.waiting().await;
        }
    });
    ().serve(client_transport).await.expect("client handshake")
}

/// The first text content block of a tool result, parsed as JSON.
fn result_json(result: &rmcp::model::CallToolResult) -> serde_json::Value {
    let text = result
        .content
        .first()
        .map(|c| c.as_text().map(|t| t.text.clone()).unwrap_or_default())
        .unwrap_or_default();
    serde_json::from_str(&text).expect("tool result is JSON")
}

/// One env-var lock for the tests that touch `KUBEADP_A2A_INBOX_DIR`.
fn env_lock() -> std::sync::MutexGuard<'static, ()> {
    static LOCK: Mutex<()> = Mutex::new(());
    LOCK.lock().unwrap_or_else(|e| e.into_inner())
}

/// Ages the stored message past the TTL by rewriting its `queued_at` in the
/// inbox store (the shared state database or the in-memory mirror).
fn age_message_file(inbox: &Inbox, message_id: &str, seconds: u64) {
    let now = kubeadp_agent::core::a2a::inbox::now_unix();
    let (run_id,) = (inbox.run_id(),);
    let _ = inbox.with_conn(|conn| {
        conn.execute(
            "UPDATE a2a_message SET queued_at=?1 WHERE run_id=?2 AND message_id=?3 AND fake=?4",
            rusqlite::params![
                now.saturating_sub(seconds),
                run_id,
                message_id,
                i64::from(inbox.fake())
            ],
        )
        .map_err(|e| format!("failed to age the A2A message: {e}"))?;
        Ok(())
    });
}

// ---------------------------------------------------------------------------
// (a) blocked in wait_message: the message wakes it within 2 s, no kill
// ---------------------------------------------------------------------------

#[tokio::test]
async fn a_wait_message_is_woken_by_the_message_within_2s() {
    let tmp = TempDir::new().unwrap();
    let inbox = Inbox::new(tmp.path());
    let engine = FakeEngine::new("agy");

    let client = mcp_client(mcp_server_with_inbox(inbox.clone()).await).await;

    // The member blocks in wait_message BEFORE the message arrives.
    let wait = tokio::spawn(async move {
        client
            .call_tool(
                CallToolRequestParams::new("wait_message").with_arguments(
                    serde_json::json!({ "timeout_s": 25 })
                        .as_object()
                        .unwrap()
                        .clone(),
                ),
            )
            .await
            .expect("wait_message")
    });
    tokio::time::sleep(Duration::from_millis(100)).await;
    let start = std::time::Instant::now();
    inbox.enqueue(queued_msg("m1", "wake up")).unwrap();

    let value = result_json(&wait.await.unwrap());
    let elapsed = start.elapsed();
    assert_eq!(value["count"], 1, "the message arrives: {value}");
    assert!(
        value["messages"][0]["framed"]
            .as_str()
            .unwrap()
            .contains("wake up"),
        "the RESULT is the framed message: {value}"
    );
    assert!(
        elapsed < Duration::from_secs(2),
        "the wait must return the moment the message is queued, took {elapsed:?}"
    );
    // Nothing is killed.
    assert_eq!(engine.kill_count(), 0);
}

// ---------------------------------------------------------------------------
// (b) busy member: the message lands at the next boundary, nothing is killed
// ---------------------------------------------------------------------------

#[tokio::test]
async fn b_busy_member_gets_the_message_at_the_next_boundary() {
    let tmp = TempDir::new().unwrap();
    let inbox = Inbox::new(tmp.path());
    let agy = FakeEngine::new("agy");
    let claude = FakeEngine::new("claude");

    // A "busy" member (a long command in flight) has the message queued while
    // it works; nothing delivers mid-command and nothing kills.
    inbox
        .enqueue(queued_msg("m1", "for the busy member"))
        .unwrap();
    assert_eq!(agy.kill_count(), 0);
    assert_eq!(claude.kill_count(), 0);

    // agy boundary: the tool_result injection appends the FULL framed text to
    // the result of its next A2A tool call.
    let client = mcp_client(mcp_server_with_inbox(inbox.clone()).await).await;
    let result = client
        .call_tool(CallToolRequestParams::new("list_agents"))
        .await
        .expect("list_agents");
    let value = result_json(&result);
    assert!(
        value["a2a_incoming"]
            .as_str()
            .unwrap()
            .contains("for the busy member"),
        "the tool result carries the full framed message: {value}"
    );
    assert_eq!(agy.kill_count(), 0, "tool_result never kills");

    // Claude boundary: the hook after the tool call carries the next message.
    inbox
        .enqueue(queued_msg("m2", "for the claude member"))
        .unwrap();
    let _guard = env_lock();
    std::env::set_var("KUBEADP_A2A_INBOX_DIR", tmp.path());
    let code = run_a2a_hook(&["PostToolUse".to_string()]);
    std::env::remove_var("KUBEADP_A2A_INBOX_DIR");
    drop(_guard);
    assert_eq!(code, 0);
    assert_eq!(claude.kill_count(), 0, "hook never kills");
    // Both boundaries consumed their message (the injection is the ack).
    assert!(inbox.pending().is_empty());
}

// ---------------------------------------------------------------------------
// (c) idle member: the idle_turn channel is declared and injects (ladder: ph2)
// ---------------------------------------------------------------------------

#[test]
fn c_idle_turn_is_declared_for_both_engines_and_injects() {
    let tmp = TempDir::new().unwrap();
    let inbox = Inbox::new(tmp.path());

    // The parity requirement (design §7.14): both engines support hook +
    // tool_result + idle_turn (the agy hook channel is the verified native
    // agy plugin PreInvocation hook).
    for engine in ["claude", "agy"] {
        let channels = inject_channels(engine);
        assert!(
            channels.contains(&InjectChannel::IdleTurn)
                && channels.contains(&InjectChannel::ToolResult)
                && channels.contains(&InjectChannel::Hook),
            "{engine} must support hook + idle_turn + tool_result, got {channels:?}"
        );
        assert_eq!(
            CapabilityTable::builder(engine).build().inject_channels,
            channels.to_vec(),
            "the capability table lists the channels per engine"
        );
    }

    // The channel injects: a message marked idle_turn is acknowledged.
    inbox.enqueue(queued_msg("m1", "idle")).unwrap();
    assert!(inbox.mark_injected("m1", InjectChannel::IdleTurn.as_str()));
    assert!(inbox.injected("m1"));
    assert!(inbox.pending().is_empty());
}

// ---------------------------------------------------------------------------
// (d) kill-and-resume only with interrupt (phase 1: no channel kills)
// ---------------------------------------------------------------------------

#[tokio::test]
async fn d_no_channel_kills_by_default() {
    let tmp = TempDir::new().unwrap();
    let inbox = Inbox::new(tmp.path());
    let agy = FakeEngine::new("agy");
    let claude = FakeEngine::new("claude");

    // Three messages delivered through the phase-1 boundaries: the fake
    // engines are never interrupted (kill-and-resume is the explicit last
    // resort, only for `interrupt: true` messages or a force timeout; the
    // ladder wiring is phase 2).
    for i in 0..3 {
        inbox
            .enqueue(queued_msg(&format!("m{i}"), "hello"))
            .unwrap();
    }
    let client = mcp_client(mcp_server_with_inbox(inbox.clone()).await).await;
    let result = client
        .call_tool(
            CallToolRequestParams::new("wait_message").with_arguments(
                serde_json::json!({ "timeout_s": 0 })
                    .as_object()
                    .unwrap()
                    .clone(),
            ),
        )
        .await
        .expect("wait_message");
    let value = result_json(&result);
    assert_eq!(value["count"], 3);

    let _guard = env_lock();
    std::env::set_var("KUBEADP_A2A_INBOX_DIR", tmp.path());
    let code = run_a2a_hook(&["UserPromptSubmit".to_string()]);
    std::env::remove_var("KUBEADP_A2A_INBOX_DIR");
    drop(_guard);
    assert_eq!(code, 0);

    assert_eq!(agy.kill_count(), 0, "no channel may kill by default");
    assert_eq!(claude.kill_count(), 0, "no channel may kill by default");
}

// ---------------------------------------------------------------------------
// (e) exactly once under retries and concurrent senders
// ---------------------------------------------------------------------------

#[tokio::test]
async fn e_exactly_once_under_retries_and_concurrent_senders() {
    let tmp = TempDir::new().unwrap();
    let inbox = Arc::new(Inbox::new(tmp.path()));

    // Two concurrent senders retry the same id: one copy wins.
    let s1 = Arc::clone(&inbox);
    let s2 = Arc::clone(&inbox);
    let (r1, r2) = tokio::join!(
        tokio::task::spawn_blocking(move || s1.enqueue(queued_msg("dup", "copy 1"))),
        tokio::task::spawn_blocking(move || s2.enqueue(queued_msg("dup", "copy 2"))),
    );
    let outcomes = [r1.unwrap().unwrap(), r2.unwrap().unwrap()];
    assert_eq!(
        outcomes
            .iter()
            .filter(|o| **o == EnqueueOutcome::Stored)
            .count(),
        1,
        "exactly one copy is stored: {outcomes:?}"
    );
    assert_eq!(inbox.pending().len(), 1);

    // Distinct concurrent senders all land.
    let mut senders = Vec::new();
    for i in 0..4 {
        let inbox = Arc::clone(&inbox);
        senders.push(tokio::task::spawn_blocking(move || {
            inbox.enqueue(queued_msg(&format!("m{i}"), "text")).unwrap()
        }));
    }
    for h in senders {
        h.await.unwrap();
    }
    assert_eq!(inbox.pending().len(), 5);

    // Two consumers (wait_message and a direct hook boundary) race: each
    // message is injected exactly once.
    let client = mcp_client(mcp_server_with_inbox((*inbox).clone()).await).await;
    let consumer1 = tokio::spawn(async move {
        client
            .call_tool(
                CallToolRequestParams::new("wait_message").with_arguments(
                    serde_json::json!({ "timeout_s": 0 })
                        .as_object()
                        .unwrap()
                        .clone(),
                ),
            )
            .await
            .expect("wait_message")
    });
    let consumer2 = tokio::task::spawn_blocking({
        let inbox = Arc::clone(&inbox);
        move || inbox.drain("hook").len()
    });
    let (mcp_result, hook_count) = (consumer1.await.unwrap(), consumer2.await.unwrap());
    let value = result_json(&mcp_result);
    let via_mcp = value["count"].as_u64().unwrap() as usize;
    assert_eq!(
        via_mcp + hook_count,
        5,
        "each message is injected exactly once (mcp {via_mcp} + hook {hook_count})"
    );
    assert!(inbox.pending().is_empty());
}

// ---------------------------------------------------------------------------
// (f) queue bound and TTL
// ---------------------------------------------------------------------------

#[test]
fn f_queue_bound_and_ttl() {
    let tmp = TempDir::new().unwrap();
    let inbox = Inbox::new(tmp.path());
    for i in 0..20 {
        inbox
            .enqueue(queued_msg(&format!("m{i}"), &format!("text {i}")))
            .unwrap();
    }
    assert!(
        inbox.pending().len() <= MAX_QUEUED_MSGS,
        "the queue stays within {} entries",
        MAX_QUEUED_MSGS
    );
    assert!(
        inbox
            .pending()
            .iter()
            .any(|m| m.framed.contains("dropped by the A2A inbox")),
        "overflow is recorded with the durable drop note"
    );
    let bytes: usize = inbox.pending().iter().map(|m| m.framed.len()).sum();
    assert!(bytes <= MAX_QUEUED_BYTES, "byte bound holds");

    // Uninjected messages expire after the TTL and stop being served (and the
    // host's next enqueue prunes the expired file).
    let ttl_inbox = Inbox::with_ttl(tmp.path().join("ttl"), 10);
    ttl_inbox.enqueue(queued_msg("t1", "expire")).unwrap();
    assert_eq!(ttl_inbox.pending().len(), 1);
    age_message_file(&ttl_inbox, "t1", 11);
    assert!(ttl_inbox.pending().is_empty(), "expired: not served");
    ttl_inbox.enqueue(queued_msg("t2", "next")).unwrap();
    assert_eq!(ttl_inbox.pending().len(), 1, "the fresh message survives");
    // Sanity: the default TTL is an hour and the drop note is never mistaken
    // for A2A traffic.
    assert_eq!(DEFAULT_TTL_SECS, 3600);
    assert_eq!(NOTE_SENDER, "inbox");
}

// ---------------------------------------------------------------------------
// (g) sender task state submitted -> working on injection
// ---------------------------------------------------------------------------

#[test]
fn g_sender_task_state_submitted_then_working() {
    let tmp = TempDir::new().unwrap();
    let inbox = Inbox::new(tmp.path());

    // While queued, the message is pending: the sender's task is `submitted`
    // (visible as queued work on the target run).
    inbox.enqueue(queued_msg("m1", "state check")).unwrap();
    assert_eq!(inbox.pending().len(), 1, "queued == submitted");
    assert!(!inbox.injected("m1"));

    // Once injected, the message is acknowledged: the sender's task is
    // `working` (the run is processing it) and the message is never queued
    // again.
    let drained = inbox.drain(InjectChannel::ToolResult.as_str());
    assert_eq!(drained.len(), 1);
    assert!(inbox.injected("m1"), "injected == working");
    assert!(inbox.pending().is_empty());
    assert_eq!(
        inbox.enqueue(queued_msg("m1", "state check")).unwrap(),
        EnqueueOutcome::Duplicate,
        "an injected id is never re-queued"
    );
}

// ---------------------------------------------------------------------------
// (h) a2a-hook output format per event
// ---------------------------------------------------------------------------

#[test]
fn h_a2a_hook_output_format_per_event() {
    let tmp = TempDir::new().unwrap();
    let inbox = Inbox::new(tmp.path());
    inbox.enqueue(queued_msg("m1", "format check")).unwrap();

    let framed = join_framed(&inbox.pending());
    // PostToolUse and UserPromptSubmit: additionalContext in the event's
    // hookSpecificOutput.
    for event in ["PostToolUse", "UserPromptSubmit"] {
        let v = kubeadp_agent::core::a2a::inject::hook_json(event, &framed).unwrap();
        assert_eq!(
            v["hookSpecificOutput"]["hookEventName"], event,
            "the hook names its event"
        );
        assert_eq!(
            v["hookSpecificOutput"]["additionalContext"], framed,
            "the hook carries the full framed message"
        );
    }
    // Stop: block with the messages as the reason.
    let v = kubeadp_agent::core::a2a::inject::hook_json("Stop", &framed).unwrap();
    assert_eq!(v["decision"], "block");
    assert_eq!(v["reason"], framed);

    // agy's verified native pre-invocation: the injectSteps system message.
    let v = kubeadp_agent::core::a2a::inject::hook_json("pre-invocation", &framed).unwrap();
    assert_eq!(
        v["injectSteps"][0]["systemMessage"]["systemMessage"], framed,
        "the agy PreInvocation hook carries the full framed message"
    );

    // An unknown event is refused, never guessed.
    assert!(kubeadp_agent::core::a2a::inject::hook_json("SessionStart", "x").is_err());
}

#[test]
fn h_a2a_hook_subcommand_marks_injected_and_is_silent_when_empty() {
    let tmp = TempDir::new().unwrap();
    let inbox = Inbox::new(tmp.path());

    // The subcommand injects through the hook channel; a second run with an
    // empty inbox is a silent no-op (exit 0, nothing printed).
    inbox.enqueue(queued_msg("m1", "subcommand")).unwrap();
    {
        let _guard = env_lock();
        std::env::set_var("KUBEADP_A2A_INBOX_DIR", tmp.path());
        let first = run_a2a_hook(&["Stop".to_string()]);
        let second = run_a2a_hook(&["Stop".to_string()]);
        std::env::remove_var("KUBEADP_A2A_INBOX_DIR");
        drop(_guard);
        assert_eq!(first, 0);
        assert_eq!(second, 0, "empty inbox: no output, exit 0");
    }
    assert!(inbox.injected("m1"));
    assert!(inbox.pending().is_empty());

    // A wrong event or missing arguments are usage errors.
    assert_eq!(run_a2a_hook(&["Nope".to_string()]), 2);
    assert_eq!(run_a2a_hook(&[]), 2);
}

// ---------------------------------------------------------------------------
// Task-state sanity: the A2A state mapping is untouched by phase 1 (the
// delivery paths keep their behaviour).
// ---------------------------------------------------------------------------

#[test]
fn delivery_state_mapping_is_untouched() {
    // A running job/session still maps to Working via the existing backends;
    // phase 1 added no state-mapping change.
    use kubeadp_agent::core::a2a::map_job_status;
    use kubeadp_agent::core::job::JobStatus;
    assert_eq!(map_job_status(JobStatus::Running), TaskState::Working);
    assert_eq!(map_job_status(JobStatus::Done), TaskState::Completed);
}

// ---------------------------------------------------------------------------
// (i)-(l) the escalation ladder, phase 2, for BOTH engines (the full
//     contract table is pinned in the delivery.rs unit tests)
// ---------------------------------------------------------------------------

#[test]
fn i_l_ladder_contracts_both_engines() {
    use kubeadp_agent::core::a2a::delivery::{next_step, LadderConfig, LadderInput, LadderStep};
    let agy = FakeEngine::new("agy");
    let claude = FakeEngine::new("claude");
    let d = LadderConfig {
        ack_secs: 45,
        force_secs: 0,
        ttl_secs: 3600,
    };
    fn input<'a>(
        queued_at: u64,
        now: u64,
        interrupt: bool,
        idle: bool,
        cfg: &'a LadderConfig,
    ) -> LadderInput<'a> {
        LadderInput {
            now,
            queued_at,
            interrupt,
            idle_turn_available: idle,
            expired: false,
            config: cfg,
        }
    }
    for engine in ["agy", "claude"] {
        // Inside the ack window: queued; past it with the run idle:
        // idle_turn (never a kill). A busy run: queued however late (the
        // default never escalates to interrupt). interrupt:true escalates
        // immediately. Past the TTL the message is never delivered.
        assert_eq!(
            next_step(&input(1000, 1044, false, true, &d)),
            LadderStep::Queued
        );
        assert_eq!(
            next_step(&input(1000, 1045, false, true, &d)),
            LadderStep::IdleTurn
        );
        assert_eq!(
            next_step(&input(1000, 1000 + 3590, false, false, &d)),
            LadderStep::Queued,
            "{engine}: default never kills inside the ttl"
        );
        assert_eq!(
            next_step(&input(1000, 1000, true, true, &d)),
            LadderStep::Interrupt
        );
        assert!(matches!(
            next_step(&input(1000, 1000 + 3600, true, true, &d)),
            LadderStep::Expired { .. }
        ));
        // The fake engines were never driven to a kill by this suite.
        assert_eq!(agy.kill_count(), 0, "{engine}");
        assert_eq!(claude.kill_count(), 0, "{engine}");
    }
    let tmp = TempDir::new().unwrap();
    let inbox = Inbox::new(tmp.path());
    inbox.enqueue(queued_msg("m1", "idle member")).unwrap();
    assert!(inbox.mark_injected("m1", InjectChannel::IdleTurn.as_str()));
    assert!(inbox.pending().is_empty());
}
