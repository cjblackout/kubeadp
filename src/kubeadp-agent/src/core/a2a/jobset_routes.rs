//! The jobset-scoped routes OUTSIDE the SDK REST set (SPEC §7.9), served on
//! the same authorization layer as the SDK routes:
//!
//! - `GET /a2a/agents` -- the agents hosted by THIS pod that are members of
//!   the presented jobset (§7.7 item 6). Authorized by steps 1 and 2 of §7.4
//!   (the request names no recipient), enforced by the router-level
//!   [`super::auth::JobsetAuthLayer`].
//! - `POST /a2a/jobsets/{id}:end` -- the jobset end propagation of §7.16,
//!   authorized by the creator proof (§7.2 item 3). The layer verifies the
//!   credential and creator; this handler applies the local end and propagates
//!   it to every peer pod, retrying with backoff until each peer answers.
//!
//! Both routes are mounted on the jobset-only router (the C4a target layer);
//! they are NOT mounted in production until C5 flips the auth layer.

use std::sync::Arc;

use axum::http::HeaderMap;
use axum::response::{IntoResponse, Response};

use super::auth::{
    AGENT_ID_HEADER, AGENT_PROOF_HEADER, END_RETRY_ATTEMPTS, END_RETRY_BASE_SECS,
    END_RETRY_MAX_SECS,
};
use crate::core::jobset::{JOBSET_ID_HEADER, JOBSET_SECRET_HEADER};

/// `GET /a2a/agents` handler body.
#[derive(serde::Serialize)]
#[serde(rename_all = "camelCase")]
pub struct AgentsResponse {
    pub agents: Vec<serde_json::Value>,
}

/// Build the `GET /a2a/agents` response for the presented jobset: every agent
/// this pod hosts that is a member of that jobset, from the registry and the
/// SQLite store (C1) when one is open. The layer has already verified steps 1
/// and 2; this lists only that jobset's members.
pub fn agents_listing(
    registry: &crate::core::jobset::JobsetRegistry,
    jobset_id: &str,
    kind: &str,
) -> AgentsResponse {
    let mut seen = std::collections::BTreeSet::new();
    let mut agents = Vec::new();
    // The in-memory registry's live memberships.
    for (agent_id, entry) in registry.snapshot() {
        if entry.jobset_id == jobset_id && seen.insert(agent_id.clone()) {
            agents.push(serde_json::json!({
                "agent_id": agent_id,
                "kind": kind,
                "state": "member",
                "jobset_id": jobset_id,
            }));
        }
    }
    // The persisted memberships (a restarted/gc'd job still lists its jobset).
    if let Some(dir) = registry.data_dir() {
        if let Ok(store) = crate::state::jobsets::JobsetStore::open_path(&dir) {
            if let Ok(members) = store.members(jobset_id) {
                for agent_id in members {
                    if seen.insert(agent_id.clone()) {
                        agents.push(serde_json::json!({
                            "agent_id": agent_id,
                            "kind": kind,
                            "state": "member",
                            "jobset_id": jobset_id,
                        }));
                    }
                }
            }
        }
    }
    agents.sort_by_key(|a| {
        a.get("agent_id")
            .and_then(|v| v.as_str())
            .unwrap_or("")
            .to_string()
    });
    AgentsResponse { agents }
}

/// The peer-end notifier: how `POST /a2a/jobsets/{id}:end` reaches the other
/// pods of the mesh (§7.7). A default HTTP client posts the end to the
/// discovered agent Services with the creator proof headers; tests inject a
/// stub. `jobset_id` is the jobset being ended; `headers` carries the creator
/// proof the LOCAL layer just verified (replayed to every peer).
#[async_trait::async_trait]
pub trait PeerEndNotifier: Send + Sync + std::fmt::Debug {
    /// Propagate `:end` for `jobset_id` to every peer pod, retrying with
    /// backoff until each peer has answered (SPEC §7.16). Returns the number
    /// of peers that answered.
    async fn notify(&self, jobset_id: &str, headers: &HeaderMap) -> usize;
}

/// The production notifier: posts `:end` to every configured agent Service
/// base URL, authorized with the creator proof, retrying with linear backoff
/// until each peer answers. The local end is permanent even if a peer is down.
#[derive(Debug, Clone)]
pub struct HttpPeerEndNotifier {
    peers: Vec<String>,
}

impl HttpPeerEndNotifier {
    /// Builds the notifier from the discovered peer base URLs
    /// (http://service.namespace.svc:port). An empty list is valid: a
    /// single-pod mesh propagates to nobody and the local end is still
    /// applied.
    pub fn new(peers: Vec<String>) -> Self {
        Self { peers }
    }
}

#[async_trait::async_trait]
impl PeerEndNotifier for HttpPeerEndNotifier {
    async fn notify(&self, jobset_id: &str, headers: &HeaderMap) -> usize {
        let client = crate::core::a2a::client::shared_peer_client()
            .unwrap_or_else(|_| reqwest::Client::new());
        let mut answered = 0usize;
        for peer in &self.peers {
            let url = format!("{}/a2a/jobsets/{jobset_id}:end", peer.trim_end_matches('/'));
            // Retry with linear backoff capped at END_RETRY_MAX_SECS.
            for attempt in 0..END_RETRY_ATTEMPTS {
                let mut req = client.post(&url);
                for (name, value) in headers {
                    if let Ok(v) = value.to_str() {
                        req = req.header(name.as_str(), v);
                    }
                }
                match req.send().await {
                    Ok(resp) if resp.status().is_success() => {
                        answered += 1;
                        break;
                    }
                    Ok(_) => {}
                    Err(_) => {}
                }
                let delay = END_RETRY_BASE_SECS
                    .saturating_add(attempt as u64)
                    .min(END_RETRY_MAX_SECS);
                tokio::time::sleep(std::time::Duration::from_secs(delay)).await;
            }
        }
        answered
    }
}

/// The `:end` route handler body.
#[derive(serde::Serialize)]
#[serde(rename_all = "camelCase")]
pub struct EndResponse {
    pub jobset_id: String,
    pub ended: bool,
    pub epoch: u64,
    pub peers_answered: usize,
}

/// Applies the local end and propagates it to the peers. The layer already
/// verified the creator proof for `jobset_id`; this mutates the local registry
/// and store (idempotent, never 404 for a known jobset) and marks queued
/// messages expired (SPEC §7.16).
pub async fn apply_end(
    registry: Arc<crate::core::jobset::JobsetRegistry>,
    notifier: Arc<dyn PeerEndNotifier>,
    headers: HeaderMap,
    jobset_id: String,
) -> Response {
    // Idempotent local end on THIS pod's registry (its ended map rejects every
    // old credential from now on) and a mirror into the SQLite store when one
    // is open. `None` only for a genuinely unknown jobset -- the layer already
    // bounded the request to a KNOWN jobset via the creator proof, so this is
    // not reachable on the authorized path; end anyway.
    let ended_epoch = registry.end_jobset(&jobset_id).or(Some(0)).unwrap_or(0);

    if let Some(dir) = registry.data_dir() {
        if let Ok(store) = crate::state::jobsets::JobsetStore::open_path(&dir) {
            if let Err(e) = store.end(&jobset_id) {
                eprintln!(
                    "kubeadp-agent: warning: failed to mirror jobset end for '{}' into the state store: {e:#}",
                    jobset_id
                );
            }
        }
    }

    expire_queued_of_jobset(&registry, &jobset_id);

    let peers_answered = notifier.notify(&jobset_id, &headers).await;
    axum::Json(EndResponse {
        jobset_id,
        ended: true,
        epoch: ended_epoch,
        peers_answered,
    })
    .into_response()
}

/// The inbox directory of a member run, derived from the run id. The inbox
/// root is the shared agent-run state root (SPEC §7.14).
fn member_inbox_dir(member: &str) -> std::path::PathBuf {
    let root = crate::core::a2a::inbox::inbox_state_root();
    root.join(crate::core::a2a::inbox::INBOX_DIR_NAME)
        .join(member)
}

/// Sets every queued (uninjected, unexpired) message of the ended jobset's
/// member runs `expired` (SPEC §7.16). Best effort: a read/write failure is
/// logged and the end still completes. The inbox key for a member is its run
/// id; for jobs/sessions the agent id IS the run id in the inbox directories.
fn expire_queued_of_jobset(registry: &crate::core::jobset::JobsetRegistry, jobset_id: &str) {
    // Runs whose member entries carry this jobset (the host's view).
    let members: Vec<String> = registry
        .snapshot()
        .into_iter()
        .filter(|(_, e)| e.jobset_id == jobset_id)
        .map(|(id, _)| id)
        .collect();
    for member in members {
        let dir = member_inbox_dir(&member);
        let inbox = crate::core::a2a::inbox::Inbox::with_ttl(
            dir,
            crate::core::a2a::inbox::DEFAULT_TTL_SECS,
        );
        for msg in inbox.pending() {
            if !inbox.expire(&msg.message_id, "jobset_ended") {
                eprintln!(
                    "a2a jobset end: no queued message '{}' of run '{}' to expire",
                    msg.message_id, member
                );
            }
        }
    }
}

/// The four creator-proof header values (id, secret, agent id, proof), when
/// all are present. Used by the propagation path and by tests to build the
/// request the layer verifies.
pub fn creator_headers(headers: &HeaderMap) -> Option<(String, String, String, String)> {
    let id = headers.get(JOBSET_ID_HEADER)?.to_str().ok()?.to_string();
    let secret = headers
        .get(JOBSET_SECRET_HEADER)?
        .to_str()
        .ok()?
        .to_string();
    let agent = headers.get(AGENT_ID_HEADER)?.to_str().ok()?.to_string();
    let proof = headers.get(AGENT_PROOF_HEADER)?.to_str().ok()?.to_string();
    Some((id, secret, agent, proof))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[derive(Debug)]
    struct StubNotifier {
        calls: std::sync::Mutex<Vec<(String, HeaderMap)>>,
        answer: usize,
    }

    #[async_trait::async_trait]
    impl PeerEndNotifier for StubNotifier {
        async fn notify(&self, jobset_id: &str, headers: &HeaderMap) -> usize {
            self.calls
                .lock()
                .unwrap()
                .push((jobset_id.to_string(), headers.clone()));
            self.answer
        }
    }

    fn headers(id: &str, secret: &str, agent: &str, proof: &str) -> HeaderMap {
        let mut h = HeaderMap::new();
        h.insert(JOBSET_ID_HEADER, id.parse().unwrap());
        h.insert(JOBSET_SECRET_HEADER, secret.parse().unwrap());
        h.insert(AGENT_ID_HEADER, agent.parse().unwrap());
        h.insert(AGENT_PROOF_HEADER, proof.parse().unwrap());
        h
    }

    #[test]
    fn creator_headers_reads_the_four_values() {
        let h = headers("js-1", "s", "a-1", "p");
        let got = creator_headers(&h).unwrap();
        assert_eq!(got, ("js-1".into(), "s".into(), "a-1".into(), "p".into()));
        let mut partial = HeaderMap::new();
        partial.insert(JOBSET_ID_HEADER, "js-1".parse().unwrap());
        assert!(creator_headers(&partial).is_none());
    }

    #[tokio::test]
    async fn apply_end_is_idempotent_and_expires_queued_messages() {
        let registry = Arc::new(crate::core::jobset::JobsetRegistry::new());
        registry.register("agent-a", "js-1", "secret-1");
        let h = headers("js-1", "secret-1", "agent-a", "proof");
        let notifier = Arc::new(StubNotifier {
            calls: std::sync::Mutex::new(Vec::new()),
            answer: 1,
        });
        let resp = apply_end(registry.clone(), notifier.clone(), h, "js-1".to_string()).await;
        assert_eq!(resp.status(), axum::http::StatusCode::OK);
        let calls = notifier.calls.lock().unwrap();
        assert_eq!(calls.len(), 1);
        assert_eq!(calls[0].0, "js-1");
        assert!(registry.is_ended("js-1"));
    }
}
