//! Public agent card type (`A2aModel`) and its conversion to the A2A
//! [`AgentCard`]; served by the SDK's `agent_card_router`.
//!
//! The card identifies the RUNNING POD (not a fixed image identity): its
//! `name` is the pod's own name (see [`pod_name`]), its `description` and
//! `skills` name the role the pod serves (`worker` or `orchestrator`), its
//! `version` is the crate version, and its `supportedInterfaces[].url` is the
//! address clients should reach the HTTP+JSON binding on (see
//! [`advertised_url`]).

use a2a::agent_card::{
    AgentCapabilities, AgentCard, AgentInterface, AgentSkill, HttpAuthSecurityScheme,
    SecurityScheme,
};
use a2a::TRANSPORT_PROTOCOL_HTTP_JSON;
use std::collections::HashMap;

/// Well-known path for the public agent card (A2A §8.6).
pub const WELL_KNOWN_AGENT_CARD_PATH: &str = "/.well-known/agent-card.json";

/// The `worker` role: runs agent jobs, one A2A task per job (task id = job id).
pub const ROLE_WORKER: &str = "worker";
/// The `orchestrator` role: sends messages into agent sessions.
pub const ROLE_ORCHESTRATOR: &str = "orchestrator";

/// Env override for the agent card `name` (highest precedence in
/// [`pod_name`]).
pub const A2A_NAME_ENV: &str = "KUBEADP_A2A_NAME";
/// Env carrying the pod's own name (the manifests set it from the downward
/// API); the second precedence in [`pod_name`].
pub const A2A_SELF_NAME_ENV: &str = "A2A_SELF_NAME";
/// The advertised base URL of the HTTP+JSON binding (highest precedence in
/// [`advertised_url`]).
pub const A2A_URL_ENV: &str = "KUBEADP_A2A_URL";

/// Identifies an agent for its card.
#[derive(Debug, Clone)]
pub struct A2aModel {
    pub name: String,
    pub description: String,
    pub version: String,
    /// The `skills` advertised in the card (built with [`role_skills`]).
    pub skills: Vec<AgentSkill>,
    /// The base URL where the HTTP+JSON (REST) binding is served; used as the
    /// `supportedInterfaces[].url`.
    pub url: String,
}

/// Resolves the agent card `name` from the running pod.
///
/// Precedence: `KUBEADP_A2A_NAME` (explicit override), then `A2A_SELF_NAME`
/// (set from the downward API in the manifests to the pod's own name), then
/// `POD_NAME`, then `HOSTNAME`; when none of those is set the role default
/// `kubeadp-agent-<role>` is used so the card still carries a meaningful
/// identity (e.g. outside a pod).
pub fn pod_name(role: &str) -> String {
    let first = |vars: &[&str]| -> Option<String> {
        vars.iter().find_map(|var| {
            std::env::var(var)
                .ok()
                .map(|v| v.trim().to_owned())
                .filter(|s| !s.is_empty())
        })
    };
    first(&[A2A_NAME_ENV, A2A_SELF_NAME_ENV, "POD_NAME", "HOSTNAME"])
        .unwrap_or_else(|| format!("kubeadp-agent-{role}"))
}

/// The pure core of [`pod_name`]: the first non-empty of `names` (in order),
/// else the role default. Exposed for hermetic unit tests.
#[cfg_attr(not(test), allow(dead_code))]
pub(crate) fn pod_name_from(names: &[&str], role: &str) -> String {
    names
        .iter()
        .find(|s| !s.trim().is_empty())
        .map(|s| s.trim().to_string())
        .unwrap_or_else(|| format!("kubeadp-agent-{role}"))
}

/// Resolves the base URL advertised in the card's `supportedInterfaces[].url`.
///
/// `KUBEADP_A2A_URL` (when set to a non-empty value) is authoritative -- the
/// manifests point it at the pod's cluster Service DNS (e.g.
/// `http://kubeadp-agent-worker-service.opencode.svc:8002`) so the advertised
/// address is reachable by clients. When it is unset the configured listen
/// address is used as the documented fallback.
pub fn advertised_url(listen: &str) -> String {
    match std::env::var(A2A_URL_ENV) {
        Ok(value) if !value.trim().is_empty() => value.trim().to_string(),
        _ => listen.to_string(),
    }
}

/// The role-specific description (it names the role).
pub fn role_description(role: &str) -> &'static str {
    match role {
        ROLE_WORKER => {
            "Kubeadp-agent worker: runs agent jobs as A2A tasks, one task per job, task id = job id"
        }
        ROLE_ORCHESTRATOR => "Kubeadp-agent orchestrator: sends messages into agent sessions",
        _ => "Kubeadp-agent A2A endpoint",
    }
}

/// The role-specific `skills` list (never empty).
pub fn role_skills(role: &str) -> Vec<AgentSkill> {
    match role {
        ROLE_WORKER => vec![AgentSkill {
            id: "run-job".to_string(),
            name: "run-job".to_string(),
            description: "Run an agent job".to_string(),
            tags: Vec::new(),
            examples: Some(vec![
                "{\"jobId\":\"job-1234\"}".to_string(),
                "{\"jobId\":\"job-1234\",\"prompt\":\"fix the failing test\"}".to_string(),
            ]),
            input_modes: Some(vec!["text/plain".to_string()]),
            output_modes: Some(vec!["text/plain".to_string()]),
            security_requirements: None,
        }],
        ROLE_ORCHESTRATOR => vec![AgentSkill {
            id: "session-message".to_string(),
            name: "session-message".to_string(),
            description: "Send a message into an agent session".to_string(),
            tags: Vec::new(),
            examples: Some(vec![
                "{\"sessionId\":\"project-a\",\"text\":\"hello\"}".to_string()
            ]),
            input_modes: Some(vec!["text/plain".to_string()]),
            output_modes: Some(vec!["text/plain".to_string()]),
            security_requirements: None,
        }],
        _ => Vec::new(),
    }
}

impl A2aModel {
    /// Builds the [`AgentCard`] for this model:
    /// streaming=true, push notifications off, extended card off, one bearer
    /// security scheme (A2A §8.6.3).
    pub fn to_agent_card(&self) -> AgentCard {
        let capabilities = AgentCapabilities {
            streaming: Some(true),
            push_notifications: Some(false),
            extensions: None,
            extended_agent_card: Some(false),
        };

        let mut security_schemes = HashMap::new();
        security_schemes.insert(
            "bearer".to_string(),
            SecurityScheme::HttpAuth(HttpAuthSecurityScheme {
                scheme: "bearer".to_string(),
                description: None,
                bearer_format: None,
            }),
        );
        let mut security_requirements = Vec::new();
        let mut requirement = HashMap::new();
        requirement.insert("bearer".to_string(), Vec::<String>::new());
        security_requirements.push(requirement);

        AgentCard {
            name: self.name.clone(),
            description: self.description.clone(),
            version: self.version.clone(),
            supported_interfaces: vec![AgentInterface::new(
                self.url.clone(),
                TRANSPORT_PROTOCOL_HTTP_JSON,
            )],
            capabilities,
            default_input_modes: vec!["text/plain".to_string()],
            default_output_modes: vec!["text/plain".to_string()],
            skills: self.skills.clone(),
            provider: None,
            documentation_url: None,
            icon_url: None,
            security_schemes: Some(security_schemes),
            security_requirements: Some(security_requirements),
            signatures: None,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::core::a2a::testenv;

    #[test]
    fn pod_name_from_prefers_first_non_empty_else_role_default() {
        // No candidate set -> the role default keeps a meaningful identity.
        assert_eq!(pod_name_from(&[], ROLE_WORKER), "kubeadp-agent-worker");
        assert_eq!(
            pod_name_from(&[], ROLE_ORCHESTRATOR),
            "kubeadp-agent-orchestrator"
        );
        // The first non-empty candidate wins (blank/whitespace is skipped).
        assert_eq!(
            pod_name_from(
                &["", "   ", "kubeadp-agent-worker-9b45797-w76s6"],
                ROLE_WORKER
            ),
            "kubeadp-agent-worker-9b45797-w76s6"
        );
        assert_eq!(
            pod_name_from(&["override", "pod-a", "pod-b", "host-c"], ROLE_WORKER),
            "override"
        );
        // Precedence: KUBEADP_A2A_NAME > A2A_SELF_NAME > POD_NAME > HOSTNAME.
        let names = [
            "kubeadp-agent-orchestrator-8675cbf44f-dbwcn",
            "pod-b",
            "host-c",
        ];
        assert_eq!(
            pod_name_from(&names, ROLE_ORCHESTRATOR),
            "kubeadp-agent-orchestrator-8675cbf44f-dbwcn"
        );
    }

    #[test]
    fn pod_name_env_wrapper_reads_the_env() {
        // The env wrapper applies the documented precedence over the process
        // environment. Each guard holds the testenv lock, so it is dropped
        // (scoped) before the next one is taken.
        {
            let _g = testenv::Guard::set(&[
                ("KUBEADP_A2A_NAME", None),
                ("A2A_SELF_NAME", Some("kubeadp-agent-worker-9b45797-w76s6")),
                ("POD_NAME", Some("pod-b")),
                ("HOSTNAME", Some("host-c")),
                ("KUBEADP_A2A_URL", None),
            ]);
            assert_eq!(pod_name(ROLE_WORKER), "kubeadp-agent-worker-9b45797-w76s6");
        }
        {
            let _g = testenv::Guard::set(&[("KUBEADP_A2A_NAME", Some("override-name"))]);
            assert_eq!(pod_name(ROLE_WORKER), "override-name");
        }
        {
            let _g = testenv::Guard::set(&[("HOSTNAME", Some("host-c"))]);
            assert_eq!(pod_name(ROLE_WORKER), "host-c");
        }
        {
            let _g = testenv::Guard::set(&[]);
            assert_eq!(pod_name(ROLE_ORCHESTRATOR), "kubeadp-agent-orchestrator");
        }
    }

    #[test]
    fn advertised_url_prefers_env_then_listen_fallback() {
        // The KUBEADP_A2A_URL override wins when set (the cluster Service DNS).
        {
            let _g = testenv::Guard::set(&[(
                "KUBEADP_A2A_URL",
                Some("http://kubeadp-agent-worker-service.opencode.svc:8002"),
            )]);
            assert_eq!(
                advertised_url("http://0.0.0.0:8002"),
                "http://kubeadp-agent-worker-service.opencode.svc:8002"
            );
        }
        // Unset -> the documented listen fallback (never a pod hostname).
        let _g = testenv::Guard::set(&[]);
        assert_eq!(
            advertised_url("http://10.0.0.5:8002"),
            "http://10.0.0.5:8002"
        );
    }

    #[test]
    fn role_descriptions_name_the_role() {
        assert!(role_description(ROLE_WORKER).contains("worker"));
        assert!(role_description(ROLE_WORKER).contains("job"));
        assert!(role_description(ROLE_ORCHESTRATOR).contains("orchestrator"));
        assert!(role_description(ROLE_ORCHESTRATOR).contains("session"));
    }

    #[test]
    fn role_skills_are_non_empty_and_correct_per_role() {
        let worker = role_skills(ROLE_WORKER);
        assert_eq!(worker.len(), 1);
        assert_eq!(worker[0].id, "run-job");
        assert_eq!(worker[0].name, "run-job");
        assert_eq!(worker[0].description, "Run an agent job");
        assert_eq!(
            worker[0].input_modes.as_deref(),
            Some(&["text/plain".to_string()][..])
        );
        assert_eq!(
            worker[0].output_modes.as_deref(),
            Some(&["text/plain".to_string()][..])
        );
        assert!(worker[0].examples.as_deref().is_some_and(|e| !e.is_empty()));

        let orchestrator = role_skills(ROLE_ORCHESTRATOR);
        assert_eq!(orchestrator.len(), 1);
        assert_eq!(orchestrator[0].id, "session-message");
        assert_eq!(orchestrator[0].name, "session-message");
        assert_eq!(
            orchestrator[0].description,
            "Send a message into an agent session"
        );
        assert_eq!(
            orchestrator[0].input_modes.as_deref(),
            Some(&["text/plain".to_string()][..])
        );
        assert_eq!(
            orchestrator[0].output_modes.as_deref(),
            Some(&["text/plain".to_string()][..])
        );
    }

    #[test]
    fn worker_card_carries_pod_name_role_and_skills() {
        let _g =
            testenv::Guard::set(&[("A2A_SELF_NAME", Some("kubeadp-agent-worker-9b45797-w76s6"))]);
        let model = A2aModel {
            name: pod_name(ROLE_WORKER),
            description: role_description(ROLE_WORKER).to_string(),
            version: env!("CARGO_PKG_VERSION").to_string(),
            skills: role_skills(ROLE_WORKER),
            url: advertised_url("http://0.0.0.0:8002"),
        };
        let card = model.to_agent_card();
        assert_eq!(card.name, "kubeadp-agent-worker-9b45797-w76s6");
        assert!(card.description.contains("worker"));
        assert_eq!(card.version, env!("CARGO_PKG_VERSION"));
        assert_eq!(card.skills.len(), 1);
        assert_eq!(card.skills[0].id, "run-job");
        assert_eq!(card.supported_interfaces[0].url, "http://0.0.0.0:8002");
    }

    #[test]
    fn orchestrator_card_carries_pod_name_role_and_skills() {
        let _g = testenv::Guard::set(&[(
            "A2A_SELF_NAME",
            Some("kubeadp-agent-orchestrator-8675cbf44f-dbwcn"),
        )]);
        let model = A2aModel {
            name: pod_name(ROLE_ORCHESTRATOR),
            description: role_description(ROLE_ORCHESTRATOR).to_string(),
            version: env!("CARGO_PKG_VERSION").to_string(),
            skills: role_skills(ROLE_ORCHESTRATOR),
            url: advertised_url("http://127.0.0.1:8002"),
        };
        let card = model.to_agent_card();
        assert_eq!(card.name, "kubeadp-agent-orchestrator-8675cbf44f-dbwcn");
        assert!(card.description.contains("orchestrator"));
        assert_eq!(card.version, env!("CARGO_PKG_VERSION"));
        assert_eq!(card.skills.len(), 1);
        assert_eq!(card.skills[0].id, "session-message");
        assert_eq!(card.supported_interfaces[0].url, "http://127.0.0.1:8002");
    }
}
