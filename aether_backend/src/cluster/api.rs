//! The internal API (`cluster.listen`): what nodes ask each other. Every
//! request must be signed with the cluster key ([`super::auth`]). It serves
//! only health for now; Raft, block transfers and read-index requests come in
//! the next steps (docs/CLUSTERING.md).

use std::sync::Arc;
use std::time::Instant;

use axum::body::{Body, to_bytes};
use axum::extract::{Request, State};
use axum::http::StatusCode;
use axum::middleware::{self, Next};
use axum::response::{IntoResponse, Response};
use axum::routing::get;
use axum::{Extension, Json, Router};
use serde::{Deserialize, Serialize};
use uuid::Uuid;

use super::auth::{AuthError, ClusterKey, ReplayGuard};
use crate::engine::{Engine, now_millis};
use crate::observe::RequestFacts;

/// Largest request body checked before it is handled. Block transfers will
/// verify their bodies while streaming instead.
const MAX_BODY: usize = 1024 * 1024;

pub struct ClusterSettings {
    pub key: ClusterKey,
    pub zone: Option<String>,
}

struct ClusterState {
    engine: Arc<Engine>,
    key: ClusterKey,
    replays: ReplayGuard,
    zone: Option<String>,
    started: Instant,
}

/// The node a request came from, as it signed it.
#[derive(Clone, Debug)]
pub struct Peer(pub String);

/// `GET /internal/v1/health`.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct NodeHealth {
    pub node_id: Uuid,
    pub zone: Option<String>,
    pub version: String,
    pub uptime_secs: u64,
    /// Whether the metadata store answers.
    pub ready: bool,
}

pub fn router(engine: Arc<Engine>, settings: ClusterSettings) -> Router {
    let state = Arc::new(ClusterState {
        engine,
        key: settings.key,
        replays: ReplayGuard::default(),
        zone: settings.zone,
        started: Instant::now(),
    });
    Router::new()
        .route("/internal/v1/health", get(health))
        .fallback(not_found)
        .layer(middleware::from_fn_with_state(Arc::clone(&state), authenticate))
        .with_state(state)
}

async fn authenticate(State(state): State<Arc<ClusterState>>, request: Request, next: Next) -> Response {
    let (mut parts, body) = request.into_parts();
    let facts = parts.extensions.get::<RequestFacts>().cloned();
    let refuse = |e: AuthError| {
        if let Some(facts) = &facts {
            facts.record_operation("unauthenticated");
        }
        error(StatusCode::UNAUTHORIZED, "Unauthorized", &e.to_string())
    };
    // The signature first: an unsigned request's body is never read.
    let target = parts.uri.path_and_query().map_or("/", |p| p.as_str());
    let signed = match state.key.verify_headers(parts.method.as_str(), target, &parts.headers, now_millis()) {
        Ok(signed) => signed,
        Err(e) => return refuse(e),
    };
    let Ok(body) = to_bytes(body, MAX_BODY).await else {
        return error(StatusCode::PAYLOAD_TOO_LARGE, "TooLarge", "the request body is too large");
    };
    match signed.verify_body(&body, now_millis(), &state.replays) {
        Ok(node) => {
            if let Some(facts) = &facts {
                facts.record_access_key(&format!("node:{node}"));
            }
            parts.extensions.insert(Peer(node));
            next.run(Request::from_parts(parts, Body::from(body))).await
        }
        Err(e) => refuse(e),
    }
}

async fn health(State(state): State<Arc<ClusterState>>, facts: Option<Extension<RequestFacts>>) -> Json<NodeHealth> {
    if let Some(Extension(facts)) = facts {
        facts.record_operation("health");
    }
    Json(NodeHealth {
        node_id: state.engine.node_id(),
        zone: state.zone.clone(),
        version: env!("CARGO_PKG_VERSION").to_owned(),
        uptime_secs: state.started.elapsed().as_secs(),
        ready: state.engine.list_buckets().await.is_ok(),
    })
}

async fn not_found() -> Response {
    error(StatusCode::NOT_FOUND, "NotFound", "no such internal API route")
}

fn error(status: StatusCode, code: &str, message: &str) -> Response {
    (status, Json(serde_json::json!({ "error": { "code": code, "message": message } }))).into_response()
}
