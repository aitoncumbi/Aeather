//! The admin API and the `aether` CLI, against a running server.

use std::net::SocketAddr;
use std::sync::Arc;

use aether_backend::config::{Config, Secret};
use aether_backend::engine::Engine;
use aether_backend::server;
use aws_sdk_s3::config::{BehaviorVersion, Credentials, Region};
use aws_sdk_s3::primitives::ByteStream;
use reqwest::StatusCode;
use serde_json::{Value, json};
use tokio::net::TcpListener;

const ROOT_KEY: &str = "root-access";
const ROOT_SECRET: &str = "root-secret-key";

struct TestServer {
    s3: SocketAddr,
    admin: SocketAddr,
    http: reqwest::Client,
    _dir: tempfile::TempDir,
}

async fn start() -> TestServer {
    let dir = tempfile::tempdir().unwrap();
    let mut config = Config::default();
    config.storage.data_dir = dir.path().to_path_buf();
    config.s3.access_key = ROOT_KEY.into();
    config.s3.secret_key = Secret::from(ROOT_SECRET);
    let engine = Arc::new(Engine::open(config.engine_config()).await.unwrap());
    let routers = server::routers(engine, &config);

    let s3 = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let admin = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let (s3_addr, admin_addr) = (s3.local_addr().unwrap(), admin.local_addr().unwrap());
    tokio::spawn(async move { axum::serve(s3, routers.s3).await.unwrap() });
    tokio::spawn(async move { axum::serve(admin, routers.admin).await.unwrap() });
    TestServer { s3: s3_addr, admin: admin_addr, http: reqwest::Client::new(), _dir: dir }
}

impl TestServer {
    /// Calls the admin API as root.
    async fn call(&self, method: reqwest::Method, path: &str, body: Option<Value>) -> (StatusCode, Value) {
        self.call_as(ROOT_KEY, ROOT_SECRET, method, path, body).await
    }

    async fn call_as(
        &self,
        key: &str,
        secret: &str,
        method: reqwest::Method,
        path: &str,
        body: Option<Value>,
    ) -> (StatusCode, Value) {
        let mut request =
            self.http.request(method, format!("http://{}{path}", self.admin)).basic_auth(key, Some(secret));
        if let Some(body) = body {
            request = request.json(&body);
        }
        let response = request.send().await.unwrap();
        let status = response.status();
        let text = response.text().await.unwrap();
        (status, if text.is_empty() { Value::Null } else { serde_json::from_str(&text).unwrap() })
    }

    fn s3_client(&self, key: &str, secret: &str) -> aws_sdk_s3::Client {
        let config = aws_sdk_s3::Config::builder()
            .behavior_version(BehaviorVersion::latest())
            .region(Region::new("us-east-1"))
            .endpoint_url(format!("http://{}", self.s3))
            .credentials_provider(Credentials::new(key, secret, None, None, "test"))
            .force_path_style(true)
            .build();
        aws_sdk_s3::Client::from_conf(config)
    }

    /// Runs the `aether` binary against this server.
    async fn cli(&self, args: &[&str]) -> (bool, String, String) {
        let url = format!("http://{}", self.admin);
        let output = tokio::process::Command::new(env!("CARGO_BIN_EXE_aether"))
            .args(args)
            .env("AETHER_ADMIN_URL", &url)
            .env("AETHER_ADMIN_ACCESS_KEY", ROOT_KEY)
            .env("AETHER_ADMIN_SECRET_KEY", ROOT_SECRET)
            .output()
            .await
            .unwrap();
        let text = |b: Vec<u8>| String::from_utf8(b).unwrap();
        (output.status.success(), text(output.stdout), text(output.stderr))
    }
}

use reqwest::Method;

#[tokio::test]
async fn requests_must_come_from_an_admin() {
    let s = start().await;
    let response = s.http.get(format!("http://{}/api/v1/status", s.admin)).send().await.unwrap();
    assert_eq!(response.status(), StatusCode::UNAUTHORIZED);
    assert!(response.headers().contains_key("www-authenticate"));

    let (status, body) = s.call_as(ROOT_KEY, "wrong", Method::GET, "/api/v1/status", None).await;
    assert_eq!((status, body["error"]["code"].as_str()), (StatusCode::UNAUTHORIZED, Some("Unauthorized")));

    let (_, app) = s.call(Method::POST, "/api/v1/keys", Some(json!({ "name": "app" }))).await;
    let (app_id, app_secret) = (app["access_key_id"].as_str().unwrap(), app["secret_key"].as_str().unwrap());
    let (status, _) = s.call_as(app_id, app_secret, Method::GET, "/api/v1/keys", None).await;
    assert_eq!(status, StatusCode::FORBIDDEN);

    let (_, admin) = s.call(Method::POST, "/api/v1/keys", Some(json!({ "name": "ops", "admin": true }))).await;
    let (admin_id, admin_secret) = (admin["access_key_id"].as_str().unwrap(), admin["secret_key"].as_str().unwrap());
    let (status, _) = s.call_as(admin_id, admin_secret, Method::GET, "/api/v1/keys", None).await;
    assert_eq!(status, StatusCode::OK);

    let (status, _) = s.call(Method::GET, "/api/v1/nope", None).await;
    assert_eq!(status, StatusCode::NOT_FOUND);
}

#[tokio::test]
async fn key_lifecycle() {
    let s = start().await;
    s.call(Method::POST, "/api/v1/buckets", Some(json!({ "name": "invoices" }))).await;

    let body = json!({ "name": "billing", "buckets": { "invoices": { "read": true, "write": true } } });
    let (status, created) = s.call(Method::POST, "/api/v1/keys", Some(body)).await;
    assert_eq!(status, StatusCode::CREATED);
    let id = created["access_key_id"].as_str().unwrap().to_owned();
    let secret = created["secret_key"].as_str().unwrap().to_owned();
    assert_eq!(created["buckets"]["invoices"], json!({ "read": true, "write": true, "owner": false }));

    // The new key works against the S3 API right away.
    let app = s.s3_client(&id, &secret);
    app.put_object().bucket("invoices").key("2026/10.pdf").body(ByteStream::from_static(b"%PDF")).send().await.unwrap();

    // The secret is never shown again.
    let (_, keys) = s.call(Method::GET, "/api/v1/keys", None).await;
    assert!(!keys.to_string().contains(&secret));
    let (_, key) = s.call(Method::GET, &format!("/api/v1/keys/{id}"), None).await;
    assert_eq!(key["name"], "billing");
    assert!(key.get("secret_key").is_none());

    let (status, key) =
        s.call(Method::PUT, &format!("/api/v1/keys/{id}/buckets/invoices"), Some(json!({ "read": true }))).await;
    assert_eq!((status, key["buckets"]["invoices"]["write"].as_bool()), (StatusCode::OK, Some(false)));
    let err = app.put_object().bucket("invoices").key("x").body(ByteStream::from_static(b"x")).send().await;
    assert!(err.is_err(), "write was revoked");

    let (status, key) = s.call(Method::PATCH, &format!("/api/v1/keys/{id}"), Some(json!({ "disabled": true }))).await;
    assert_eq!((status, key["disabled"].as_bool()), (StatusCode::OK, Some(true)));
    assert!(app.list_buckets().send().await.is_err());

    let (status, _) = s.call(Method::DELETE, &format!("/api/v1/keys/{id}/buckets/invoices"), None).await;
    assert_eq!(status, StatusCode::NO_CONTENT);
    let (status, _) = s.call(Method::DELETE, &format!("/api/v1/keys/{id}"), None).await;
    assert_eq!(status, StatusCode::NO_CONTENT);
    let (status, body) = s.call(Method::GET, &format!("/api/v1/keys/{id}"), None).await;
    assert_eq!((status, body["error"]["code"].as_str()), (StatusCode::NOT_FOUND, Some("NoSuchAccessKey")));
}

#[tokio::test]
async fn key_requests_are_validated() {
    let s = start().await;
    let post = |body: Value| s.call(Method::POST, "/api/v1/keys", Some(body));

    let (status, body) = post(json!({ "name": "x", "buckets": { "missing": { "read": true } } })).await;
    assert_eq!((status, body["error"]["code"].as_str()), (StatusCode::NOT_FOUND, Some("NoSuchBucket")));
    let (status, _) = post(json!({ "name": "x", "unknown_field": 1 })).await;
    assert_eq!(status, StatusCode::BAD_REQUEST);
    let (status, _) = post(json!({ "name": "" })).await;
    assert_eq!(status, StatusCode::BAD_REQUEST);
    let (status, _) = post(json!({ "name": "x", "access_key_id": "only-id-given" })).await;
    assert_eq!(status, StatusCode::BAD_REQUEST);
    let (status, body) = post(json!({ "name": "x", "access_key_id": ROOT_KEY, "secret_key": "something-long" })).await;
    assert_eq!((status, body["error"]["code"].as_str()), (StatusCode::CONFLICT, Some("AccessKeyExists")));

    // Imported credentials keep working after a migration from another store.
    let (status, _) =
        post(json!({ "name": "legacy", "access_key_id": "legacy-app", "secret_key": "legacy-secret", "admin": true }))
            .await;
    assert_eq!(status, StatusCode::CREATED);
    s.s3_client("legacy-app", "legacy-secret").list_buckets().send().await.unwrap();
}

#[tokio::test]
async fn buckets_and_usage() {
    let s = start().await;
    let (status, bucket) = s.call(Method::POST, "/api/v1/buckets", Some(json!({ "name": "media" }))).await;
    assert_eq!((status, bucket["objects"].as_u64()), (StatusCode::CREATED, Some(0)));
    let (status, _) = s.call(Method::POST, "/api/v1/buckets", Some(json!({ "name": "media" }))).await;
    assert_eq!(status, StatusCode::CONFLICT);
    let (status, _) = s.call(Method::POST, "/api/v1/buckets", Some(json!({ "name": "Bad_Name" }))).await;
    assert_eq!(status, StatusCode::BAD_REQUEST);

    let root = s.s3_client(ROOT_KEY, ROOT_SECRET);
    for (key, size) in [("a", 1000), ("b", 24)] {
        root.put_object().bucket("media").key(key).body(ByteStream::from(vec![0u8; size])).send().await.unwrap();
    }
    let (_, buckets) = s.call(Method::GET, "/api/v1/buckets", None).await;
    assert_eq!((buckets[0]["objects"].as_u64(), buckets[0]["bytes"].as_u64()), (Some(2), Some(1024)));

    let body = json!({ "name": "viewer", "buckets": { "media": { "read": true } } });
    let (_, viewer) = s.call(Method::POST, "/api/v1/keys", Some(body)).await;
    let (_, detail) = s.call(Method::GET, "/api/v1/buckets/media", None).await;
    assert_eq!(detail["keys"][0]["access_key_id"], viewer["access_key_id"]);
    assert_eq!(detail["keys"][0]["read"], true);

    let (_, status_view) = s.call(Method::GET, "/api/v1/status", None).await;
    assert_eq!((status_view["buckets"].as_u64(), status_view["objects"].as_u64()), (Some(1), Some(2)));
    assert_eq!(status_view["access_keys"].as_u64(), Some(1));

    let (status, body) = s.call(Method::DELETE, "/api/v1/buckets/media", None).await;
    assert_eq!((status, body["error"]["code"].as_str()), (StatusCode::CONFLICT, Some("BucketNotEmpty")));
    for key in ["a", "b"] {
        root.delete_object().bucket("media").key(key).send().await.unwrap();
    }
    let (status, _) = s.call(Method::DELETE, "/api/v1/buckets/media", None).await;
    assert_eq!(status, StatusCode::NO_CONTENT);
    // Deleting the bucket removed the viewer's grant.
    let (_, viewer) =
        s.call(Method::GET, &format!("/api/v1/keys/{}", viewer["access_key_id"].as_str().unwrap()), None).await;
    assert_eq!(viewer["buckets"], json!({}));
}

#[tokio::test]
async fn cli_manages_keys_and_buckets() {
    let s = start().await;
    let (ok, out, err) = s.cli(&["bucket", "create", "reports"]).await;
    assert!(ok, "{err}");
    assert_eq!(out, "Created bucket reports.\n");

    let (ok, out, err) = s.cli(&["key", "create", "reporting job", "--grant", "reports=rw"]).await;
    assert!(ok, "{err}");
    let field = |name: &str| {
        let line = out.lines().find(|l| l.starts_with(name)).unwrap_or_else(|| panic!("no {name} in {out}"));
        line[name.len()..].trim().to_owned()
    };
    let (id, secret) = (field("Access key id:"), field("Secret key:"));
    assert_eq!(field("Buckets:"), "reports:rw");
    let app = s.s3_client(&id, &secret);
    app.put_object().bucket("reports").key("q3.csv").body(ByteStream::from_static(b"a,b\n")).send().await.unwrap();

    let (ok, out, _) = s.cli(&["key", "list"]).await;
    assert!(ok);
    assert!(out.starts_with("ACCESS KEY ID") && out.contains(&id) && out.contains("reporting job"), "{out}");

    let (ok, out, _) = s.cli(&["key", "grant", &id, "reports", "r"]).await;
    assert!(ok);
    assert!(out.contains("reports:r\n"), "{out}");
    let (ok, out, _) = s.cli(&["key", "disable", &id]).await;
    assert!(ok && out.contains("DISABLED"), "{out}");

    let (ok, out, _) = s.cli(&["bucket", "list", "--json"]).await;
    assert!(ok);
    let buckets: Value = serde_json::from_str(&out).unwrap();
    assert_eq!((buckets[0]["name"].as_str(), buckets[0]["objects"].as_u64()), (Some("reports"), Some(1)));
    let (ok, out, _) = s.cli(&["bucket", "show", "reports"]).await;
    assert!(ok && out.contains(&id), "{out}");
    let (ok, out, _) = s.cli(&["status"]).await;
    assert!(ok && out.contains("Objects:     1"), "{out}");

    let (ok, _, err) = s.cli(&["key", "grant", &id, "missing", "rw"]).await;
    assert!(!ok);
    assert!(err.contains("NoSuchBucket"), "{err}");
    let (ok, _, err) = s.cli(&["key", "grant", &id, "reports", "rx"]).await;
    assert!(!ok && err.contains("unknown permission"), "{err}");

    let (ok, out, _) = s.cli(&["key", "delete", &id]).await;
    assert!(ok);
    assert_eq!(out, format!("Deleted access key {id}.\n"));
}
