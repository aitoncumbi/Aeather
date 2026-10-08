//! Per-request bookkeeping for both listeners: a request id (returned in a
//! response header and attached to every log line of the request), an access
//! log line, and HTTP metrics.

use std::net::SocketAddr;
use std::sync::{Arc, Mutex};
use std::time::Instant;

use axum::body::{Body, HttpBody};
use axum::extract::{ConnectInfo, Request, State};
use axum::http::{HeaderName, HeaderValue, header};
use axum::middleware::Next;
use axum::response::Response;
use tracing::{Instrument, info, info_span};

use crate::metrics::Metrics;

/// Details learned while a request is handled; the S3 permission check
/// records which operation ran and which access key signed it.
#[derive(Clone, Debug, Default)]
pub struct RequestFacts(Arc<Mutex<Facts>>);

#[derive(Clone, Debug, Default)]
pub struct Facts {
    pub operation: Option<String>,
    pub access_key: Option<String>,
}

impl RequestFacts {
    pub fn record(&self, operation: &str, access_key: &str) {
        let mut facts = self.0.lock().expect("request facts lock poisoned");
        facts.operation = Some(operation.to_owned());
        facts.access_key = Some(access_key.to_owned());
    }

    /// Records only the operation, for requests that carry no access key.
    pub fn record_operation(&self, operation: &str) {
        self.0.lock().expect("request facts lock poisoned").operation = Some(operation.to_owned());
    }

    /// Records only the caller (admin API requests keep their route label).
    pub fn record_access_key(&self, access_key: &str) {
        self.0.lock().expect("request facts lock poisoned").access_key = Some(access_key.to_owned());
    }

    fn take(&self) -> Facts {
        std::mem::take(&mut self.0.lock().expect("request facts lock poisoned"))
    }
}

#[derive(Clone)]
pub struct Observer {
    /// `s3`, `admin` or `cluster`: the metrics label and log field.
    pub api: &'static str,
    /// `x-amz-request-id` for S3 (what SDKs report in errors), `x-request-id` otherwise.
    pub id_header: HeaderName,
    pub metrics: Arc<Metrics>,
    pub access_log: bool,
}

pub async fn observe(State(observer): State<Observer>, mut request: Request, next: Next) -> Response {
    let id = request_id();
    let facts = RequestFacts::default();
    request.extensions_mut().insert(facts.clone());
    let method = request.method().clone();
    let path = request.uri().path().to_owned();
    let peer = request.extensions().get::<ConnectInfo<SocketAddr>>().map(|info| info.0);

    let span = info_span!("request", id = %id);
    let started = Instant::now();
    let mut response = next.run(request).instrument(span.clone()).await;
    let elapsed = started.elapsed();

    if let Ok(value) = HeaderValue::from_str(&id) {
        response.headers_mut().insert(observer.id_header.clone(), value);
    }
    if observer.api == "s3" && (response.status().is_client_error() || response.status().is_server_error()) {
        response = add_request_id_to_error(response, &id).await;
    }
    let Facts { operation, access_key } = facts.take();
    let operation = operation.unwrap_or_else(|| default_operation(observer.api, &path, method.as_str()));
    let status = response.status().as_u16();
    observer.metrics.observe_request(observer.api, &operation, status, elapsed);
    if observer.access_log {
        // The query string is never logged: presigned URLs carry signatures in it.
        info!(
            target: "aether::access",
            parent: &span,
            api = observer.api,
            method = %method,
            path = %path,
            status,
            operation = %operation,
            access_key = access_key.as_deref().unwrap_or("-"),
            peer = %peer.map_or_else(|| "-".to_owned(), |p| p.to_string()),
            ms = elapsed.as_millis() as u64,
            "request"
        );
    }
    response
}

/// S3 error documents carry the request id too (`<RequestId>`), which is what
/// SDKs show in error messages. Errors come from s3s as well as from
/// handlers, so the id is added here, to the small, fully buffered XML body.
async fn add_request_id_to_error(response: Response, id: &str) -> Response {
    const MAX_ERROR_BODY: u64 = 64 * 1024;
    let is_xml =
        response.headers().get(header::CONTENT_TYPE).and_then(|v| v.to_str().ok()).is_some_and(|v| v.contains("xml"));
    let small = response.body().size_hint().exact().is_some_and(|n| n > 0 && n <= MAX_ERROR_BODY);
    if !is_xml || !small {
        return response;
    }
    let (mut parts, body) = response.into_parts();
    let Ok(bytes) = axum::body::to_bytes(body, MAX_ERROR_BODY as usize).await else {
        return Response::from_parts(parts, Body::empty());
    };
    let body = match std::str::from_utf8(&bytes) {
        Ok(xml) if !xml.contains("<RequestId>") && xml.trim_end().ends_with("</Error>") => {
            let end = xml.rfind("</Error>").expect("checked above");
            format!("{}<RequestId>{id}</RequestId>{}", &xml[..end], &xml[end..]).into_bytes().into()
        }
        _ => bytes,
    };
    parts.headers.insert(header::CONTENT_LENGTH, HeaderValue::from(body.len()));
    Response::from_parts(parts, Body::from(body))
}

/// Label for requests that never reached an S3 operation (health checks,
/// signature failures) and for admin API requests. Labels must come from a
/// small set: arbitrary paths would grow the metrics without bound.
fn default_operation(api: &str, path: &str, method: &str) -> String {
    if api == "cluster" {
        // Its handlers record their own; anything else is not a route.
        "other".into()
    } else if path.starts_with("/_aether/") || path == "/metrics" {
        "internal".into()
    } else if api == "s3" {
        "unauthenticated".into()
    } else if path.starts_with("/api/") {
        format!("{method} {}", path.split('/').take(4).collect::<Vec<_>>().join("/"))
    } else {
        "console".into()
    }
}

/// 16 upper-case hex digits, like S3's request ids.
fn request_id() -> String {
    let id = uuid::Uuid::new_v4();
    format!("{:016X}", u64::from_be_bytes(id.as_bytes()[..8].try_into().expect("8 bytes")))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn request_ids_look_like_s3_ones() {
        let id = request_id();
        assert_eq!(id.len(), 16);
        assert!(id.bytes().all(|b| b.is_ascii_digit() || b.is_ascii_uppercase()));
        assert_ne!(id, request_id());
    }

    #[test]
    fn admin_operations_are_labelled_by_route_prefix() {
        assert_eq!(default_operation("admin", "/api/v1/keys/AKX/buckets/b", "PUT"), "PUT /api/v1/keys");
        assert_eq!(default_operation("s3", "/_aether/health/ready", "GET"), "internal");
        assert_eq!(default_operation("s3", "/bucket/key", "GET"), "unauthenticated");
        assert_eq!(default_operation("admin", "/_next/static/chunks/x.js", "GET"), "console");
        assert_eq!(default_operation("cluster", "/internal/v1/anything/at/all", "GET"), "other");
    }
}
