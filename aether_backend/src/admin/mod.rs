//! JSON admin API: access keys, permissions, buckets, objects and usage,
//! plus the web console that uses it.
//!
//! Served on its own port. Requests authenticate with HTTP Basic auth using
//! the access key id and secret of the root key or an admin key (the `aether`
//! CLI), or with a session cookie from signing in to the console.

mod console;
mod objects;
mod presign;
mod session;

use std::collections::BTreeMap;
use std::sync::Arc;
use std::time::{Duration, Instant};

use axum::extract::rejection::JsonRejection;
use axum::extract::{DefaultBodyLimit, Extension, Path, Query, Request, State};
use axum::http::{HeaderMap, HeaderValue, Method, StatusCode, Uri, header};
use axum::middleware::{self, Next};
use axum::response::{IntoResponse, Response};
use axum::routing::{delete, get, post, put};
use axum::{Json, Router};
use base64::Engine as _;
use base64::engine::general_purpose::STANDARD;
use serde::{Deserialize, Serialize};
use tracing::error;

pub use self::objects::{
    ObjectDetail, ObjectItem, ObjectList, PartView, ShareLink, UploadView, VersionItem, VersionList,
};
use self::session::Sessions;
use crate::auth::{Authorizer, Identity};
use crate::config::Config;
use crate::engine::{Engine, NewKey, ScrubReport, now_millis};
use crate::error::Error;
use crate::history::{self, History};
use crate::meta::{
    AccessKeyRecord, BucketGrant, BucketRecord, BucketUsage, CorsRule, KeyUpdate, Millis, Quota, Versioning,
};
use crate::metrics::{self, Metrics};
use crate::observe::RequestFacts;

/// What the admin API needs to know about the rest of the configuration.
#[derive(Clone, Debug)]
pub struct AdminSettings {
    /// Serve the web console at `/`.
    pub console: bool,
    /// The admin listener uses TLS (session cookies are then `Secure`).
    pub tls: bool,
    pub region: String,
    /// `s3.public_url`, if set.
    pub s3_public_url: Option<String>,
    pub s3_port: u16,
    pub s3_tls: bool,
    pub max_object_size: u64,
}

impl AdminSettings {
    pub fn from_config(config: &Config) -> Self {
        let s3_tls = config.tls.files().is_some();
        Self {
            console: config.admin.console,
            tls: s3_tls && config.tls.admin,
            region: config.s3.region.clone(),
            s3_public_url: config.s3.public_url.clone(),
            s3_port: config.s3.listen.port(),
            s3_tls,
            max_object_size: config.s3.max_object_size,
        }
    }

    /// The URL clients reach the S3 API at: `s3.public_url`, or else the
    /// host this request was addressed to, with the S3 port.
    pub fn s3_endpoint(&self, headers: &HeaderMap) -> String {
        if let Some(url) = &self.s3_public_url {
            return url.trim_end_matches('/').to_owned();
        }
        let host = headers.get(header::HOST).and_then(|h| h.to_str().ok()).unwrap_or("localhost");
        // Strip the admin port, minding IPv6 literals like `[::1]:9001`.
        let host = match host.rsplit_once(':') {
            Some((name, port)) if port.bytes().all(|b| b.is_ascii_digit()) && !name.ends_with(':') => name,
            _ => host,
        };
        format!("{}://{host}:{}", if self.s3_tls { "https" } else { "http" }, self.s3_port)
    }
}

#[derive(Clone)]
pub(crate) struct AdminState {
    engine: Arc<Engine>,
    auth: Arc<Authorizer>,
    history: Arc<History>,
    sessions: Arc<Sessions>,
    settings: Arc<AdminSettings>,
    started: Instant,
}

/// The authenticated caller, set by [`require_admin`].
#[derive(Clone, Debug)]
pub(crate) struct Caller {
    access_key_id: String,
}

pub fn router(
    engine: Arc<Engine>,
    auth: Arc<Authorizer>,
    metrics: Arc<Metrics>,
    history: Arc<History>,
    settings: AdminSettings,
) -> Router {
    let state = AdminState {
        engine,
        auth,
        history,
        sessions: Arc::default(),
        settings: Arc::new(settings),
        started: Instant::now(),
    };
    let api = Router::new()
        .route("/status", get(status))
        .route("/stats", get(stats))
        .route("/scrub", post(start_scrub))
        .route("/backup", post(backup))
        .route("/session", get(current_session))
        .route("/keys", get(list_keys).post(create_key))
        .route("/keys/{id}", get(get_key).patch(update_key).delete(delete_key))
        .route("/keys/{id}/buckets/{bucket}", put(set_grant).delete(revoke_grant))
        .route("/buckets", get(list_buckets).post(create_bucket))
        .route("/buckets/{name}", get(get_bucket).delete(delete_bucket))
        .route("/buckets/{name}/quota", put(set_quota))
        .route("/buckets/{name}/versioning", put(set_versioning))
        .route("/buckets/{name}/objects", get(objects::list))
        .route("/buckets/{name}/object", get(objects::head).put(objects::put).delete(objects::delete))
        .route("/buckets/{name}/object/download", get(objects::download))
        .route("/buckets/{name}/object/versions", get(objects::versions))
        .route("/buckets/{name}/object/restore", post(objects::restore))
        .route("/buckets/{name}/object/share", post(objects::share))
        .route("/buckets/{name}/uploads", post(objects::create_upload))
        .route("/buckets/{name}/uploads/{upload_id}", delete(objects::abort_upload))
        .route("/buckets/{name}/uploads/{upload_id}/parts/{part}", put(objects::upload_part))
        .route("/buckets/{name}/uploads/{upload_id}/complete", post(objects::complete_upload))
        .layer(DefaultBodyLimit::disable())
        .route_layer(middleware::from_fn_with_state(state.clone(), require_admin))
        // Without credentials:
        .route("/health", get(health))
        .route("/login", post(login))
        .route("/logout", post(logout));
    // Prometheus scrapes without credentials; the listener is loopback-only by default.
    let metrics = Router::new().route("/metrics", get(metrics::handler)).with_state(metrics);
    Router::new().nest("/api/v1", api).fallback(fallback).with_state(state).merge(metrics)
}

/// Unknown API paths get a JSON 404; everything else is the console's.
async fn fallback(State(s): State<AdminState>, method: Method, uri: Uri) -> Response {
    if uri.path().starts_with("/api/") || !s.settings.console {
        return ApiError::new(StatusCode::NOT_FOUND, "NotFound", "no such endpoint").into_response();
    }
    console::serve(method, uri).await
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
    /// How many keys have explicit permissions (admin keys reach every bucket
    /// and are not counted).
    #[serde(default)]
    pub key_count: usize,
    #[serde(default)]
    pub quota: Quota,
    /// `Unversioned`, `Enabled` or `Suspended`.
    #[serde(default)]
    pub versioning: Versioning,
    /// Noncurrent versions kept by versioning (counted in the size quota).
    #[serde(default)]
    pub noncurrent_objects: u64,
    #[serde(default)]
    pub noncurrent_bytes: u64,
    /// Keys with explicit permissions; only in single-bucket responses.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub keys: Option<Vec<BucketKeyView>>,
    /// CORS rules (set through the S3 API); only in single-bucket responses.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub cors: Option<Vec<CorsRule>>,
    /// Lifecycle rules (set through the S3 API); only in single-bucket responses.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub lifecycle: Option<Vec<crate::meta::LifecycleRule>>,
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
    /// This node's identity (kept in `node.redb`).
    #[serde(default)]
    pub node_id: uuid::Uuid,
    pub uptime_secs: u64,
    pub buckets: usize,
    pub objects: u64,
    pub bytes: u64,
    pub blocks: u64,
    pub access_keys: usize,
    /// Free and total space of the data directory's file system.
    pub disk_available_bytes: Option<u64>,
    pub disk_total_bytes: Option<u64>,
    /// The running scrub's progress, or the last one's result.
    pub scrub: Option<ScrubReport>,
    #[serde(default)]
    pub data_dir: String,
    #[serde(default)]
    pub block_size: u64,
    #[serde(default)]
    pub inline_threshold: u64,
    /// Blocks waiting for garbage collection.
    #[serde(default)]
    pub gc_pending_blocks: u64,
    #[serde(default)]
    pub gc_grace_secs: u64,
    /// How long reading the bucket list from the metadata store took.
    #[serde(default)]
    pub metadata_latency_ms: f64,
    /// Where clients reach the S3 API, and whether `s3.public_url` set it.
    #[serde(default)]
    pub s3_endpoint: String,
    #[serde(default)]
    pub s3_endpoint_configured: bool,
    #[serde(default)]
    pub region: String,
    #[serde(default)]
    pub admin_tls: bool,
    /// Fingerprint of the master key new objects are encrypted with; `null`
    /// when encryption at rest is off.
    #[serde(default)]
    pub encryption_key: Option<String>,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct SessionView {
    pub access_key_id: String,
    pub name: String,
    /// Signed in with the root key from the configuration file.
    pub root: bool,
    pub root_access_key_id: String,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct LoginRequest {
    pub access_key_id: String,
    pub secret_key: String,
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

fn bucket_view(bucket: BucketRecord, usage: BucketUsage, keys: &[AccessKeyRecord]) -> BucketView {
    let key_count = keys.iter().filter(|k| !k.admin && k.buckets.contains_key(&bucket.name)).count();
    BucketView {
        name: bucket.name,
        created_at: rfc3339(bucket.created_at),
        objects: usage.objects,
        bytes: usage.bytes,
        key_count,
        quota: bucket.quota,
        versioning: bucket.versioning,
        noncurrent_objects: usage.noncurrent_objects,
        noncurrent_bytes: usage.noncurrent_bytes,
        keys: None,
        cors: None,
        lifecycle: None,
    }
}

pub(crate) fn rfc3339(millis: Millis) -> String {
    let nanos = i128::from(millis) * 1_000_000;
    let time = time::OffsetDateTime::from_unix_timestamp_nanos(nanos).unwrap_or(time::OffsetDateTime::UNIX_EPOCH);
    time.format(&time::format_description::well_known::Rfc3339).unwrap_or_default()
}

// ---- handlers ----

pub(crate) type ApiResult<T> = Result<T, ApiError>;

async fn status(State(s): State<AdminState>, headers: HeaderMap) -> ApiResult<Json<StatusView>> {
    let timer = Instant::now();
    s.engine.list_buckets().await?;
    let metadata_latency_ms = timer.elapsed().as_secs_f64() * 1000.0;
    let buckets = s.engine.list_bucket_usage().await?;
    let config = s.engine.config();
    let disk = metrics::disk_space(&config.data_dir).ok();
    Ok(Json(StatusView {
        version: env!("CARGO_PKG_VERSION").to_owned(),
        node_id: s.engine.node_id(),
        uptime_secs: s.started.elapsed().as_secs(),
        buckets: buckets.len(),
        objects: buckets.iter().map(|(_, u)| u.objects).sum(),
        bytes: buckets.iter().map(|(_, u)| u.bytes).sum(),
        blocks: s.engine.block_count().await?,
        access_keys: s.engine.access_keys().len(),
        disk_available_bytes: disk.map(|(available, _)| available),
        disk_total_bytes: disk.map(|(_, total)| total),
        scrub: s.engine.scrub_report(),
        data_dir: config.data_dir.display().to_string(),
        block_size: config.block_size,
        inline_threshold: config.inline_threshold as u64,
        gc_pending_blocks: s.engine.gc_pending().await?,
        gc_grace_secs: config.gc_grace.as_secs(),
        metadata_latency_ms,
        s3_endpoint: s.settings.s3_endpoint(&headers),
        s3_endpoint_configured: s.settings.s3_public_url.is_some(),
        region: s.settings.region.clone(),
        admin_tls: s.settings.tls,
        encryption_key: config.keyring.as_ref().map(|ring| ring.current_id().to_owned()),
    }))
}

#[derive(Debug, Deserialize)]
struct StatsParams {
    range: Option<String>,
}

/// Request rates, latency and bandwidth for the console's charts.
async fn stats(State(s): State<AdminState>, Query(params): Query<StatsParams>) -> ApiResult<Json<history::Report>> {
    let range = params.range.as_deref().unwrap_or("24h");
    let range =
        history::Range::parse(range).ok_or_else(|| ApiError::bad_request("range must be one of 1h, 24h, 7d"))?;
    Ok(Json(s.history.report(range, now_millis())))
}

/// For the sign-in page: whether the node is up, without credentials.
async fn health(State(s): State<AdminState>) -> (StatusCode, Json<serde_json::Value>) {
    let ready = s.engine.list_buckets().await.is_ok();
    let status = if ready { StatusCode::OK } else { StatusCode::SERVICE_UNAVAILABLE };
    let body = serde_json::json!({
        "status": if ready { "ready" } else { "unavailable" },
        "version": env!("CARGO_PKG_VERSION"),
        "tls": s.settings.tls,
    });
    (status, Json(body))
}

fn session_view(s: &AdminState, access_key_id: &str) -> Option<SessionView> {
    let (name, root) = match s.auth.identify(access_key_id)? {
        Identity::Root => ("Root key".to_owned(), true),
        Identity::Key(key) => (key.name, false),
    };
    Some(SessionView {
        access_key_id: access_key_id.to_owned(),
        name,
        root,
        root_access_key_id: s.auth.root_access_key().to_owned(),
    })
}

async fn current_session(
    State(s): State<AdminState>,
    Extension(caller): Extension<Caller>,
) -> ApiResult<Json<SessionView>> {
    session_view(&s, &caller.access_key_id).map(Json).ok_or_else(|| ApiError::unauthorized(true))
}

/// Signs in to the console: checks the key and secret once and sets the
/// session cookie.
async fn login(
    State(s): State<AdminState>,
    headers: HeaderMap,
    body: Result<Json<LoginRequest>, JsonRejection>,
) -> ApiResult<Response> {
    require_console_header(&headers)?;
    let Json(req) = body?;
    let identity = match s.auth.authenticate(&req.access_key_id, &req.secret_key) {
        Some(identity) => identity,
        None => {
            // Slows down guessing; the secrets themselves are long.
            tokio::time::sleep(Duration::from_millis(250)).await;
            return Err(ApiError::new(
                StatusCode::UNAUTHORIZED,
                "InvalidCredentials",
                "the access key ID or secret key is wrong, or the key is disabled",
            ));
        }
    };
    if !identity.is_admin() {
        return Err(ApiError::new(
            StatusCode::FORBIDDEN,
            "AccessDenied",
            "only the root key and admin keys can sign in",
        ));
    }
    let token = s.sessions.create(&req.access_key_id)?;
    let view = session_view(&s, &req.access_key_id).ok_or_else(|| ApiError::unauthorized(true))?;
    let mut response = Json(view).into_response();
    response.headers_mut().insert(header::SET_COOKIE, session::set_cookie(&token, s.settings.tls));
    Ok(response)
}

async fn logout(State(s): State<AdminState>, headers: HeaderMap) -> ApiResult<Response> {
    require_console_header(&headers)?;
    if let Some(token) = session::token(&headers) {
        s.sessions.remove(&token);
    }
    let mut response = StatusCode::NO_CONTENT.into_response();
    response.headers_mut().insert(header::SET_COOKIE, session::clear_cookie(s.settings.tls));
    Ok(response)
}

fn require_console_header(headers: &HeaderMap) -> ApiResult<()> {
    if session::from_console(headers) { Ok(()) } else { Err(console_header_missing()) }
}

fn console_header_missing() -> ApiError {
    let message = format!("console requests must carry the {} header", session::CONSOLE_HEADER);
    ApiError::new(StatusCode::FORBIDDEN, "AccessDenied", message)
}

/// Starts a scrub in the background; progress shows in `/api/v1/status`.
async fn start_scrub(State(s): State<AdminState>) -> ApiResult<(StatusCode, Json<serde_json::Value>)> {
    if s.engine.scrub_report().is_some_and(|report| report.finished_at.is_none()) {
        return Err(Error::Conflict("a scrub is already running".into()).into());
    }
    let engine = Arc::clone(&s.engine);
    tokio::spawn(async move {
        if let Err(e) = engine.scrub().await {
            tracing::warn!(error = %e, "scrub failed");
        }
    });
    Ok((StatusCode::ACCEPTED, Json(serde_json::json!({ "started": true }))))
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct BackupRequest {
    /// An absolute path on the server, new or empty, outside the data directory.
    dir: std::path::PathBuf,
}

/// Backs the node up to a directory on the server and answers when done.
/// The backup runs to the end even if the client goes away.
async fn backup(
    State(s): State<AdminState>,
    body: Result<Json<BackupRequest>, JsonRejection>,
) -> ApiResult<Json<crate::engine::BackupReport>> {
    let Json(req) = body?;
    let engine = Arc::clone(&s.engine);
    let report = tokio::spawn(async move { engine.backup(&req.dir).await }).await.map_err(Error::from)??;
    Ok(Json(report))
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
    let keys = s.engine.access_keys();
    Ok(Json(buckets.into_iter().map(|(bucket, usage)| bucket_view(bucket, usage, &keys)).collect()))
}

async fn create_bucket(
    State(s): State<AdminState>,
    body: Result<Json<CreateBucketRequest>, JsonRejection>,
) -> ApiResult<(StatusCode, Json<BucketView>)> {
    let Json(req) = body?;
    let bucket = s.engine.create_bucket(&req.name).await?;
    Ok((StatusCode::CREATED, Json(bucket_view(bucket, BucketUsage::default(), &[]))))
}

async fn get_bucket(State(s): State<AdminState>, Path(name): Path<String>) -> ApiResult<Json<BucketView>> {
    let (bucket, usage) = s.engine.bucket_usage(&name).await?;
    let cors = bucket.cors.clone();
    let lifecycle = bucket.lifecycle.clone();
    let all_keys = s.engine.access_keys();
    let keys = all_keys
        .iter()
        .filter(|key| !key.admin)
        .filter_map(|key| {
            let grant = *key.buckets.get(&name)?;
            Some(BucketKeyView { access_key_id: key.access_key_id.clone(), name: key.name.clone(), grant })
        })
        .collect();
    let view = bucket_view(bucket, usage, &all_keys);
    Ok(Json(BucketView { keys: Some(keys), cors: Some(cors), lifecycle: Some(lifecycle), ..view }))
}

/// `{"max_bytes": n | null, "max_objects": n | null}`; null is no limit.
async fn set_quota(
    State(s): State<AdminState>,
    Path(name): Path<String>,
    body: Result<Json<Quota>, JsonRejection>,
) -> ApiResult<Json<BucketView>> {
    let Json(quota) = body?;
    s.engine.set_bucket_quota(&name, quota).await?;
    let (bucket, usage) = s.engine.bucket_usage(&name).await?;
    Ok(Json(bucket_view(bucket, usage, &s.engine.access_keys())))
}

#[derive(Debug, Deserialize)]
pub struct VersioningRequest {
    status: Versioning,
}

/// `{"status": "Enabled" | "Suspended"}`. Versioning cannot be removed once enabled.
async fn set_versioning(
    State(s): State<AdminState>,
    Path(name): Path<String>,
    body: Result<Json<VersioningRequest>, JsonRejection>,
) -> ApiResult<Json<BucketView>> {
    let Json(req) = body?;
    s.engine.set_bucket_versioning(&name, req.status).await?;
    let (bucket, usage) = s.engine.bucket_usage(&name).await?;
    Ok(Json(bucket_view(bucket, usage, &s.engine.access_keys())))
}

async fn delete_bucket(State(s): State<AdminState>, Path(name): Path<String>) -> ApiResult<StatusCode> {
    s.engine.delete_bucket(&name).await?;
    Ok(StatusCode::NO_CONTENT)
}

// ---- authentication ----

/// Basic auth (the CLI) or a console session cookie, from the root key or an
/// admin key. Cookie-authenticated writes must also carry the console header.
async fn require_admin(State(s): State<AdminState>, mut req: Request, next: Next) -> Response {
    let console = session::from_console(req.headers());
    // Requests with a (perhaps expired) session cookie come from the console
    // even without its header, e.g. download links; they must not get a Basic
    // challenge either, or the browser shows its own sign-in dialog.
    let browser = console || session::token(req.headers()).is_some();
    let (id, identity) = if let Some((id, secret)) = basic_credentials(req.headers()) {
        match s.auth.authenticate(&id, &secret) {
            Some(identity) => (id, identity),
            None => return ApiError::unauthorized(browser).into_response(),
        }
    } else if let Some(token) = session::token(req.headers()) {
        let Some(id) = s.sessions.resolve(&token, !session::is_background(req.headers())) else {
            return ApiError::unauthorized(browser).into_response();
        };
        let safe = matches!(*req.method(), Method::GET | Method::HEAD);
        if !safe && !console {
            return console_header_missing().into_response();
        }
        match s.auth.identify(&id) {
            Some(identity) => (id, identity),
            None => {
                // The key was deleted or disabled since signing in.
                s.sessions.remove(&token);
                return ApiError::unauthorized(browser).into_response();
            }
        }
    } else {
        return ApiError::unauthorized(browser).into_response();
    };
    if let Some(facts) = req.extensions().get::<RequestFacts>() {
        facts.record_access_key(&id);
    }
    if !identity.is_admin() {
        return ApiError::new(StatusCode::FORBIDDEN, "AccessDenied", "this access key is not an admin key")
            .into_response();
    }
    req.extensions_mut().insert(Caller { access_key_id: id });
    next.run(req).await
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
    /// Ask for Basic auth. Never for the console: browsers would pop up
    /// their own sign-in dialog.
    challenge: bool,
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
    pub(crate) fn new(status: StatusCode, code: &'static str, message: impl Into<String>) -> Self {
        Self { status, code, message: message.into(), challenge: false }
    }

    pub(crate) fn bad_request(message: impl Into<String>) -> Self {
        Self::new(StatusCode::BAD_REQUEST, "InvalidRequest", message)
    }

    fn unauthorized(console: bool) -> Self {
        if console {
            return Self::new(StatusCode::UNAUTHORIZED, "Unauthorized", "sign in again");
        }
        Self {
            challenge: true,
            ..Self::new(
                StatusCode::UNAUTHORIZED,
                "Unauthorized",
                "use HTTP Basic auth with an admin access key and secret",
            )
        }
    }
}

impl IntoResponse for ApiError {
    fn into_response(self) -> Response {
        let body = ErrorBody { error: ErrorDetail { code: self.code, message: &self.message } };
        let mut response = (self.status, Json(body)).into_response();
        if self.challenge {
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
            Error::NoSuchKey => (StatusCode::NOT_FOUND, "NoSuchKey"),
            Error::NoSuchVersion => (StatusCode::NOT_FOUND, "NoSuchVersion"),
            Error::MethodNotAllowed(_) => (StatusCode::METHOD_NOT_ALLOWED, "MethodNotAllowed"),
            Error::CustomerKey(_) => (StatusCode::BAD_REQUEST, "CustomerKeyRequired"),
            Error::NoSuchUpload => (StatusCode::NOT_FOUND, "NoSuchUpload"),
            Error::InvalidPart(_) => (StatusCode::BAD_REQUEST, "InvalidPart"),
            Error::InvalidPartOrder => (StatusCode::BAD_REQUEST, "InvalidPartOrder"),
            Error::EntityTooSmall(_) => (StatusCode::BAD_REQUEST, "EntityTooSmall"),
            Error::PreconditionFailed => (StatusCode::PRECONDITION_FAILED, "PreconditionFailed"),
            Error::BodyTimeout => (StatusCode::REQUEST_TIMEOUT, "RequestTimeout"),
            Error::Body(_) => (StatusCode::BAD_REQUEST, "IncompleteBody"),
            Error::NoSuchAccessKey => (StatusCode::NOT_FOUND, "NoSuchAccessKey"),
            Error::BucketAlreadyExists => (StatusCode::CONFLICT, "BucketAlreadyExists"),
            Error::BucketNotEmpty => (StatusCode::CONFLICT, "BucketNotEmpty"),
            Error::AccessKeyExists => (StatusCode::CONFLICT, "AccessKeyExists"),
            Error::Conflict(_) => (StatusCode::CONFLICT, "Conflict"),
            Error::InvalidBucketName(_) => (StatusCode::BAD_REQUEST, "InvalidBucketName"),
            Error::InvalidArgument(_) => (StatusCode::BAD_REQUEST, "InvalidArgument"),
            Error::QuotaExceeded(_) => (StatusCode::FORBIDDEN, "QuotaExceeded"),
            _ => {
                error!(error = %e, "admin API internal error");
                (StatusCode::INTERNAL_SERVER_ERROR, "InternalError")
            }
        };
        Self::new(status, code, e.to_string())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn settings(public_url: Option<&str>) -> AdminSettings {
        AdminSettings {
            console: true,
            tls: false,
            region: "us-east-1".into(),
            s3_public_url: public_url.map(str::to_owned),
            s3_port: 9000,
            s3_tls: false,
            max_object_size: 1,
        }
    }

    #[test]
    fn s3_endpoint_follows_the_console_host_unless_configured() {
        let endpoint = |host: &str| {
            let mut headers = HeaderMap::new();
            headers.insert(header::HOST, HeaderValue::from_str(host).unwrap());
            settings(None).s3_endpoint(&headers)
        };
        assert_eq!(endpoint("storage.lan:9001"), "http://storage.lan:9000");
        assert_eq!(endpoint("localhost"), "http://localhost:9000");
        assert_eq!(endpoint("[::1]:9001"), "http://[::1]:9000");
        assert_eq!(settings(Some("https://s3.internal/")).s3_endpoint(&HeaderMap::new()), "https://s3.internal");
    }
}
