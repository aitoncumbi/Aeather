//! JSON admin API: access keys, permissions, buckets and usage.
//!
//! Served on its own port. Every request authenticates with HTTP Basic auth
//! using an access key id and secret of the root key or an admin key. The
//! web console (v1.1) and the `aether` CLI are clients of this API.

use std::collections::BTreeMap;
use std::sync::Arc;
use std::time::Instant;

use axum::extract::rejection::JsonRejection;
use axum::extract::{Path, Request, State};
use axum::http::{HeaderMap, HeaderValue, StatusCode, header};
use axum::middleware::{self, Next};
use axum::response::{IntoResponse, Response};
use axum::routing::{get, put};
use axum::{Json, Router};
use base64::Engine as _;
use base64::engine::general_purpose::STANDARD;
use serde::{Deserialize, Serialize};
use tracing::error;

use crate::auth::Authorizer;
use crate::engine::{Engine, NewKey};
use crate::error::Error;
use crate::meta::{AccessKeyRecord, BucketGrant, BucketRecord, BucketUsage, KeyUpdate, Millis};

#[derive(Clone)]
struct AdminState {
    engine: Arc<Engine>,
    auth: Arc<Authorizer>,
    started: Instant,
}

pub fn router(engine: Arc<Engine>, auth: Arc<Authorizer>) -> Router {
    let state = AdminState { engine, auth, started: Instant::now() };
    Router::new()
        .route("/api/v1/status", get(status))
        .route("/api/v1/keys", get(list_keys).post(create_key))
        .route("/api/v1/keys/{id}", get(get_key).patch(update_key).delete(delete_key))
        .route("/api/v1/keys/{id}/buckets/{bucket}", put(set_grant).delete(revoke_grant))
        .route("/api/v1/buckets", get(list_buckets).post(create_bucket))
        .route("/api/v1/buckets/{name}", get(get_bucket).delete(delete_bucket))
        .route_layer(middleware::from_fn_with_state(state.clone(), require_admin))
        .fallback(|| async { ApiError::new(StatusCode::NOT_FOUND, "NotFound", "no such endpoint") })
        .with_state(state)
}

// ---- request and response bodies ----

#[derive(Debug, Serialize, Deserialize)]
pub struct KeyView {
    pub access_key_id: String,
    pub name: String,
    pub created_at: String,
    pub admin: bool,
    pub can_create_buckets: bool,
    pub disabled: bool,
    pub buckets: BTreeMap<String, BucketGrant>,
}

/// Returned once, when a key is created: the only time the secret is shown.
#[derive(Debug, Serialize, Deserialize)]
pub struct CreatedKey {
    #[serde(flatten)]
    pub key: KeyView,
    pub secret_key: String,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CreateKeyRequest {
    pub name: String,
    #[serde(default)]
    pub admin: bool,
    #[serde(default)]
    pub can_create_buckets: bool,
    #[serde(default)]
    pub buckets: BTreeMap<String, BucketGrant>,
    /// Import existing credentials instead of generating them.
    pub access_key_id: Option<String>,
    pub secret_key: Option<String>,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct UpdateKeyRequest {
    pub name: Option<String>,
    pub admin: Option<bool>,
    pub can_create_buckets: Option<bool>,
    pub disabled: Option<bool>,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct BucketView {
    pub name: String,
    pub created_at: String,
    pub objects: u64,
    pub bytes: u64,
    /// Keys with explicit permissions; only in single-bucket responses.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub keys: Option<Vec<BucketKeyView>>,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct BucketKeyView {
    pub access_key_id: String,
    pub name: String,
    #[serde(flatten)]
    pub grant: BucketGrant,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CreateBucketRequest {
    pub name: String,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct StatusView {
    pub version: String,
    pub uptime_secs: u64,
    pub buckets: usize,
    pub objects: u64,
    pub bytes: u64,
    pub blocks: u64,
    pub access_keys: usize,
}

fn key_view(key: AccessKeyRecord) -> KeyView {
    KeyView {
        access_key_id: key.access_key_id,
        name: key.name,
        created_at: rfc3339(key.created_at),
        admin: key.admin,
        can_create_buckets: key.can_create_buckets,
        disabled: key.disabled,
        buckets: key.buckets,
    }
}

fn bucket_view(bucket: BucketRecord, usage: BucketUsage) -> BucketView {
    BucketView {
        name: bucket.name,
        created_at: rfc3339(bucket.created_at),
        objects: usage.objects,
        bytes: usage.bytes,
        keys: None,
    }
}

fn rfc3339(millis: Millis) -> String {
    let nanos = i128::from(millis) * 1_000_000;
    let time = time::OffsetDateTime::from_unix_timestamp_nanos(nanos).unwrap_or(time::OffsetDateTime::UNIX_EPOCH);
    time.format(&time::format_description::well_known::Rfc3339).unwrap_or_default()
}

// ---- handlers ----

type ApiResult<T> = Result<T, ApiError>;

async fn status(State(s): State<AdminState>) -> ApiResult<Json<StatusView>> {
    let buckets = s.engine.list_bucket_usage().await?;
    Ok(Json(StatusView {
        version: env!("CARGO_PKG_VERSION").to_owned(),
        uptime_secs: s.started.elapsed().as_secs(),
        buckets: buckets.len(),
        objects: buckets.iter().map(|(_, u)| u.objects).sum(),
        bytes: buckets.iter().map(|(_, u)| u.bytes).sum(),
        blocks: s.engine.block_count().await?,
        access_keys: s.engine.access_keys().len(),
    }))
}

async fn list_keys(State(s): State<AdminState>) -> Json<Vec<KeyView>> {
    Json(s.engine.access_keys().into_iter().map(key_view).collect())
}

async fn create_key(
    State(s): State<AdminState>,
    body: Result<Json<CreateKeyRequest>, JsonRejection>,
) -> ApiResult<(StatusCode, Json<CreatedKey>)> {
    let Json(req) = body?;
    if req.access_key_id.as_deref().is_some_and(|id| s.auth.is_root_key(id)) {
        return Err(Error::AccessKeyExists.into());
    }
    if req.access_key_id.is_some() != req.secret_key.is_some() {
        return Err(ApiError::bad_request("access_key_id and secret_key must be given together"));
    }
    let new = NewKey {
        name: req.name,
        admin: req.admin,
        can_create_buckets: req.can_create_buckets,
        buckets: req.buckets,
        access_key_id: req.access_key_id,
        secret_key: req.secret_key,
    };
    let mut key = s.engine.create_key(new).await?;
    let secret_key = std::mem::take(&mut key.secret_key);
    Ok((StatusCode::CREATED, Json(CreatedKey { key: key_view(key), secret_key })))
}

async fn get_key(State(s): State<AdminState>, Path(id): Path<String>) -> ApiResult<Json<KeyView>> {
    let key = s.engine.access_key(&id).ok_or(Error::NoSuchAccessKey)?;
    Ok(Json(key_view(key)))
}

async fn update_key(
    State(s): State<AdminState>,
    Path(id): Path<String>,
    body: Result<Json<UpdateKeyRequest>, JsonRejection>,
) -> ApiResult<Json<KeyView>> {
    let Json(req) = body?;
    let update = KeyUpdate {
        name: req.name,
        admin: req.admin,
        can_create_buckets: req.can_create_buckets,
        disabled: req.disabled,
    };
    Ok(Json(key_view(s.engine.update_key(&id, update).await?)))
}

async fn delete_key(State(s): State<AdminState>, Path(id): Path<String>) -> ApiResult<StatusCode> {
    s.engine.delete_key(&id).await?;
    Ok(StatusCode::NO_CONTENT)
}

async fn set_grant(
    State(s): State<AdminState>,
    Path((id, bucket)): Path<(String, String)>,
    body: Result<Json<BucketGrant>, JsonRejection>,
) -> ApiResult<Json<KeyView>> {
    let Json(grant) = body?;
    Ok(Json(key_view(s.engine.set_grant(&id, &bucket, grant).await?)))
}

async fn revoke_grant(
    State(s): State<AdminState>,
    Path((id, bucket)): Path<(String, String)>,
) -> ApiResult<StatusCode> {
    s.engine.set_grant(&id, &bucket, BucketGrant::default()).await?;
    Ok(StatusCode::NO_CONTENT)
}

async fn list_buckets(State(s): State<AdminState>) -> ApiResult<Json<Vec<BucketView>>> {
    let buckets = s.engine.list_bucket_usage().await?;
    Ok(Json(buckets.into_iter().map(|(bucket, usage)| bucket_view(bucket, usage)).collect()))
}

async fn create_bucket(
    State(s): State<AdminState>,
    body: Result<Json<CreateBucketRequest>, JsonRejection>,
) -> ApiResult<(StatusCode, Json<BucketView>)> {
    let Json(req) = body?;
    let bucket = s.engine.create_bucket(&req.name).await?;
    Ok((StatusCode::CREATED, Json(bucket_view(bucket, BucketUsage::default()))))
}

async fn get_bucket(State(s): State<AdminState>, Path(name): Path<String>) -> ApiResult<Json<BucketView>> {
    let (bucket, usage) = s.engine.bucket_usage(&name).await?;
    let keys = s
        .engine
        .access_keys()
        .into_iter()
        .filter_map(|key| {
            let grant = *key.buckets.get(&name)?;
            Some(BucketKeyView { access_key_id: key.access_key_id, name: key.name, grant })
        })
        .collect();
    Ok(Json(BucketView { keys: Some(keys), ..bucket_view(bucket, usage) }))
}

async fn delete_bucket(State(s): State<AdminState>, Path(name): Path<String>) -> ApiResult<StatusCode> {
    s.engine.delete_bucket(&name).await?;
    Ok(StatusCode::NO_CONTENT)
}

// ---- authentication ----

async fn require_admin(State(s): State<AdminState>, req: Request, next: Next) -> Response {
    let Some((id, secret)) = basic_credentials(req.headers()) else {
        return ApiError::unauthorized().into_response();
    };
    match s.auth.authenticate(&id, &secret) {
        Some(identity) if identity.is_admin() => next.run(req).await,
        Some(_) => {
            ApiError::new(StatusCode::FORBIDDEN, "AccessDenied", "this access key is not an admin key").into_response()
        }
        None => ApiError::unauthorized().into_response(),
    }
}

/// `Authorization: Basic base64(access_key_id:secret_key)`.
fn basic_credentials(headers: &HeaderMap) -> Option<(String, String)> {
    let value = headers.get(header::AUTHORIZATION)?.to_str().ok()?;
    let (scheme, encoded) = value.split_once(' ')?;
    if !scheme.eq_ignore_ascii_case("basic") {
        return None;
    }
    let decoded = String::from_utf8(STANDARD.decode(encoded.trim()).ok()?).ok()?;
    let (id, secret) = decoded.split_once(':')?;
    Some((id.to_owned(), secret.to_owned()))
}

// ---- errors ----

#[derive(Debug)]
pub struct ApiError {
    status: StatusCode,
    code: &'static str,
    message: String,
}

#[derive(Serialize)]
struct ErrorBody<'a> {
    error: ErrorDetail<'a>,
}

#[derive(Serialize)]
struct ErrorDetail<'a> {
    code: &'a str,
    message: &'a str,
}

impl ApiError {
    fn new(status: StatusCode, code: &'static str, message: impl Into<String>) -> Self {
        Self { status, code, message: message.into() }
    }

    fn bad_request(message: impl Into<String>) -> Self {
        Self::new(StatusCode::BAD_REQUEST, "InvalidRequest", message)
    }

    fn unauthorized() -> Self {
        Self::new(StatusCode::UNAUTHORIZED, "Unauthorized", "use HTTP Basic auth with an admin access key and secret")
    }
}

impl IntoResponse for ApiError {
    fn into_response(self) -> Response {
        let body = ErrorBody { error: ErrorDetail { code: self.code, message: &self.message } };
        let mut response = (self.status, Json(body)).into_response();
        if self.status == StatusCode::UNAUTHORIZED {
            response.headers_mut().insert(header::WWW_AUTHENTICATE, HeaderValue::from_static("Basic realm=\"aether\""));
        }
        response
    }
}

impl From<JsonRejection> for ApiError {
    fn from(rejection: JsonRejection) -> Self {
        Self::bad_request(rejection.body_text())
    }
}

impl From<Error> for ApiError {
    fn from(e: Error) -> Self {
        let (status, code) = match &e {
            Error::NoSuchBucket => (StatusCode::NOT_FOUND, "NoSuchBucket"),
            Error::NoSuchAccessKey => (StatusCode::NOT_FOUND, "NoSuchAccessKey"),
            Error::BucketAlreadyExists => (StatusCode::CONFLICT, "BucketAlreadyExists"),
            Error::BucketNotEmpty => (StatusCode::CONFLICT, "BucketNotEmpty"),
            Error::AccessKeyExists => (StatusCode::CONFLICT, "AccessKeyExists"),
            Error::InvalidBucketName(_) => (StatusCode::BAD_REQUEST, "InvalidBucketName"),
            Error::InvalidArgument(_) => (StatusCode::BAD_REQUEST, "InvalidArgument"),
            _ => {
                error!(error = %e, "admin API internal error");
                (StatusCode::INTERNAL_SERVER_ERROR, "InternalError")
            }
        };
        Self::new(status, code, e.to_string())
    }
}
