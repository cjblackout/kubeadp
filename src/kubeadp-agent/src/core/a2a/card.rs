//! Public agent card (`GET /.well-known/agent-card.json`).

use a2a::agent_card::{
    AgentCapabilities, AgentCard, AgentInterface, AgentSkill, HttpAuthSecurityScheme,
    SecurityScheme,
};
use a2a::TRANSPORT_PROTOCOL_JSONRPC;
use axum::extract::State;
use axum::response::{IntoResponse, Response};
use axum::Json;
use serde_json::json;
use std::collections::HashMap;

/// Well-known path for the public agent card (A2A §8.6).
pub const WELL_KNOWN_AGENT_CARD_PATH: &str = "/.well-known/agent-card.json";

/// Identifies an agent for its card.
#[derive(Debug, Clone)]
pub struct A2aModel {
    pub name: String,
    pub description: String,
    pub version: String,
    /// `skills` in the card = the project/agent names the caller passes.
    pub skills: Vec<String>,
    /// The base URL (without the JSON-RPC path) where the JSON-RPC binding
    /// is served; used as the `supportedInterfaces[].url`.
    pub json_rpc_url: String,
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

        let skills = self
            .skills
            .iter()
            .map(|name| AgentSkill {
                id: name.clone(),
                name: name.clone(),
                description: String::new(),
                tags: Vec::new(),
                examples: None,
                input_modes: None,
                output_modes: None,
                security_requirements: None,
            })
            .collect();

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
                self.json_rpc_url.clone(),
                TRANSPORT_PROTOCOL_JSONRPC,
            )],
            capabilities,
            default_input_modes: vec!["text/plain".to_string()],
            default_output_modes: vec!["text/plain".to_string()],
            skills,
            provider: None,
            documentation_url: None,
            icon_url: None,
            security_schemes: Some(security_schemes),
            security_requirements: Some(security_requirements),
            signatures: None,
        }
    }
}

/// Serves the agent card without authentication (A2A §8.6.1).
pub async fn agent_card(State(state): State<super::A2aState>) -> Response {
    let card = state.model.to_agent_card();
    // The card is built from a fixed, trusted struct -- never from request
    // input -- so serialization cannot fail.
    let body = serde_json::to_value(&card).unwrap_or_else(|_| json!({}));
    Json(body).into_response()
}
