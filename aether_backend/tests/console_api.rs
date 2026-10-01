//! What the web console relies on: sign-in sessions, object browsing and
//! transfers through the admin API, share links and the charts' statistics.

use std::net::SocketAddr;
use std::sync::Arc;

use aether_backend::config::{Config, Secret};
use aether_backend::engine::Engine;
use aether_backend::server;
use reqwest::header::{CONTENT_DISPOSITION, CONTENT_TYPE, COOKIE, SET_COOKIE, WWW_AUTHENTICATE};
use reqwest::{Method, StatusCode};
use serde_json::{Value, json};
use tokio::net::TcpListener;

const ROOT_KEY: &str = "root-access";
const ROOT_SECRET: &str = "root-secret-key";
const MIB: usize = 1024 * 1024;

struct TestServer {
    s3: SocketAddr,
    admin: SocketAddr,
    http: reqwest::Client,
    _dir: tempfile::TempDir,
}

async fn start() -> TestServer {
    aether_backend::http::tls::install_crypto_provider();
    let dir = tempfile::tempdir().unwrap();
    let s3 = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let admin = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let (s3_addr, admin_addr) = (s3.local_addr().unwrap(), admin.local_addr().unwrap());
    let mut config = Config::default();
    config.storage.data_dir = dir.path().to_path_buf();
    config.s3.access_key = ROOT_KEY.into();
    config.s3.secret_key = Secret::from(ROOT_SECRET);
    config.s3.public_url = Some(format!("http://{s3_addr}"));
    let engine = Arc::new(Engine::open(config.engine_config()).await.unwrap());
    let routers = server::routers(engine, &config);
    tokio::spawn(async move { axum::serve(s3, routers.s3).await.unwrap() });
    tokio::spawn(async move { axum::serve(admin, routers.admin).await.unwrap() });
    TestServer { s3: s3_addr, admin: admin_addr, http: reqwest::Client::new(), _dir: dir }
}

impl TestServer {
    fn url(&self, path: &str) -> String {
        format!("http://{}{path}", self.admin)
    }

    /// Signs in like the console does; returns the session cookie.
    async fn login(&self, key: &str, secret: &str) -> Result<String, StatusCode> {
        let response = self
            .http
            .post(self.url("/api/v1/login"))
            .header("x-aether-console", "1")
            .json(&json!({ "access_key_id": key, "secret_key": secret }))
            .send()
            .await
            .unwrap();
        if response.status() != StatusCode::OK {
            assert!(response.headers().get(WWW_AUTHENTICATE).is_none(), "no browser sign-in dialog");
            return Err(response.status());
        }
        let cookie = response.headers()[SET_COOKIE].to_str().unwrap().to_owned();
        assert!(cookie.contains("HttpOnly") && cookie.contains("SameSite=Strict"), "{cookie}");
        Ok(cookie.split(';').next().unwrap().to_owned())
    }

    /// A console request: session cookie and console header.
    fn console(&self, cookie: &str, method: Method, path: &str) -> reqwest::RequestBuilder {
        self.http.request(method, self.url(path)).header(COOKIE, cookie).header("x-aether-console", "1")
    }

    async fn json(&self, request: reqwest::RequestBuilder) -> (StatusCode, Value) {
        let response = request.send().await.unwrap();
        let status = response.status();
        let text = response.text().await.unwrap();
        (status, if text.is_empty() { Value::Null } else { serde_json::from_str(&text).unwrap() })
    }
}

fn pattern(len: usize) -> Vec<u8> {
    (0..len).map(|i| (i * 7 % 251) as u8).collect()
}

#[tokio::test]
async fn console_sessions() {
    let s = start().await;
    let no_header =
        s.http.post(s.url("/api/v1/login")).json(&json!({"access_key_id": ROOT_KEY, "secret_key": ROOT_SECRET}));
    assert_eq!(no_header.send().await.unwrap().status(), StatusCode::FORBIDDEN, "login needs the console header");
    assert_eq!(s.login(ROOT_KEY, "wrong secret").await, Err(StatusCode::UNAUTHORIZED));

    let cookie = s.login(ROOT_KEY, ROOT_SECRET).await.unwrap();
    let (status, me) = s.json(s.console(&cookie, Method::GET, "/api/v1/session")).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!((me["root"].as_bool(), me["root_access_key_id"].as_str()), (Some(true), Some(ROOT_KEY)));

    // Reads work with the cookie alone; writes also need the console header.
    let get = s.http.get(s.url("/api/v1/buckets")).header(COOKIE, &cookie);
    assert_eq!(get.send().await.unwrap().status(), StatusCode::OK);
    let forged = s.http.post(s.url("/api/v1/buckets")).header(COOKIE, &cookie).json(&json!({"name": "forged"}));
    assert_eq!(forged.send().await.unwrap().status(), StatusCode::FORBIDDEN);
    let create = s.console(&cookie, Method::POST, "/api/v1/buckets").json(&json!({"name": "real"}));
    assert_eq!(create.send().await.unwrap().status(), StatusCode::CREATED);

    // A non-admin key cannot sign in; an admin key can until it is disabled.
    let (_, app) = s
        .json(
            s.console(&cookie, Method::POST, "/api/v1/keys")
                .json(&json!({"name": "app", "buckets": {"real": {"read": true}}})),
        )
        .await;
    let app_secret = app["secret_key"].as_str().unwrap();
    assert_eq!(s.login(app["access_key_id"].as_str().unwrap(), app_secret).await, Err(StatusCode::FORBIDDEN));
    let (_, ops) =
        s.json(s.console(&cookie, Method::POST, "/api/v1/keys").json(&json!({"name": "ops", "admin": true}))).await;
    let ops_id = ops["access_key_id"].as_str().unwrap();
    let ops_cookie = s.login(ops_id, ops["secret_key"].as_str().unwrap()).await.unwrap();
    let (_, me) = s.json(s.console(&ops_cookie, Method::GET, "/api/v1/session")).await;
    assert_eq!((me["name"].as_str(), me["root"].as_bool()), (Some("ops"), Some(false)));
    let disable = s.console(&cookie, Method::PATCH, &format!("/api/v1/keys/{ops_id}")).json(&json!({"disabled": true}));
    assert_eq!(disable.send().await.unwrap().status(), StatusCode::OK);
    let (status, _) = s.json(s.console(&ops_cookie, Method::GET, "/api/v1/session")).await;
    assert_eq!(status, StatusCode::UNAUTHORIZED, "disabling a key ends its sessions");

    let logout = s.console(&cookie, Method::POST, "/api/v1/logout").send().await.unwrap();
    assert_eq!(logout.status(), StatusCode::NO_CONTENT);
    assert!(logout.headers()[SET_COOKIE].to_str().unwrap().contains("Max-Age=0"));
    let (status, _) = s.json(s.console(&cookie, Method::GET, "/api/v1/session")).await;
    assert_eq!(status, StatusCode::UNAUTHORIZED);

    // The CLI's Basic auth still works, and still gets a Basic challenge.
    let basic = s.http.get(s.url("/api/v1/status")).basic_auth(ROOT_KEY, Some(ROOT_SECRET)).send().await.unwrap();
    assert_eq!(basic.status(), StatusCode::OK);
    let anonymous = s.http.get(s.url("/api/v1/status")).send().await.unwrap();
    assert!(anonymous.headers().contains_key(WWW_AUTHENTICATE));
}

#[tokio::test]
async fn browsing_uploading_and_downloading_objects() {
    let s = start().await;
    let cookie = s.login(ROOT_KEY, ROOT_SECRET).await.unwrap();
    s.console(&cookie, Method::POST, "/api/v1/buckets").json(&json!({"name": "docs"})).send().await.unwrap();

    for key in ["2026/q3.pdf", "2026/q2.pdf", "2026/monthly/09.csv", "readme.txt"] {
        let put = s
            .console(&cookie, Method::PUT, "/api/v1/buckets/docs/object")
            .query(&[("key", key)])
            .header(CONTENT_TYPE, "application/pdf")
            .body(format!("contents of {key}"));
        let (status, item) = s.json(put).await;
        assert_eq!(status, StatusCode::CREATED);
        assert_eq!(item["key"], key);
    }

    let list = |prefix: &str, max: Option<&str>, continuation: Option<&str>| {
        let mut query = vec![("prefix", prefix.to_owned()), ("delimiter", "/".to_owned())];
        query.extend(max.map(|m| ("max_keys", m.to_owned())));
        query.extend(continuation.map(|c| ("continuation", c.to_owned())));
        s.console(&cookie, Method::GET, "/api/v1/buckets/docs/objects").query(&query)
    };
    let (_, root) = s.json(list("", None, None)).await;
    assert_eq!(root["prefixes"], json!(["2026/"]));
    assert_eq!(root["objects"][0]["key"], "readme.txt");
    let (_, page) = s.json(list("2026/", Some("1"), None)).await;
    assert_eq!(page["prefixes"], json!(["2026/monthly/"]));
    let next = page["continuation"].as_str().unwrap().to_owned();
    let (_, page) = s.json(list("2026/", Some("5"), Some(&next))).await;
    let keys: Vec<_> = page["objects"].as_array().unwrap().iter().map(|o| o["key"].as_str().unwrap()).collect();
    assert_eq!(keys, ["2026/q2.pdf", "2026/q3.pdf"]);
    assert!(page["continuation"].is_null());

    let (status, detail) =
        s.json(s.console(&cookie, Method::GET, "/api/v1/buckets/docs/object").query(&[("key", "2026/q3.pdf")])).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(
        (detail["content_type"].as_str(), detail["storage"].as_str()),
        (Some("application/pdf"), Some("inline"))
    );

    // Downloads are attachments, never rendered in the console's origin.
    let download = s
        .console(&cookie, Method::GET, "/api/v1/buckets/docs/object/download")
        .query(&[("key", "2026/q3.pdf")])
        .send()
        .await
        .unwrap();
    assert_eq!(download.status(), StatusCode::OK);
    assert_eq!(download.headers()[CONTENT_TYPE], "application/octet-stream");
    assert!(download.headers()[CONTENT_DISPOSITION].to_str().unwrap().starts_with("attachment; filename=\"q3.pdf\""));
    assert_eq!(download.text().await.unwrap(), "contents of 2026/q3.pdf");

    // Multipart: two parts, the first at the 5 MiB minimum.
    let (status, upload) = s
        .json(
            s.console(&cookie, Method::POST, "/api/v1/buckets/docs/uploads")
                .json(&json!({"key": "big/data.bin", "content_type": "application/octet-stream"})),
        )
        .await;
    assert_eq!(status, StatusCode::CREATED);
    let upload_id = upload["upload_id"].as_str().unwrap();
    let data = pattern(5 * MIB + 1234);
    let mut parts = Vec::new();
    for (number, chunk) in [(1, &data[..5 * MIB]), (2, &data[5 * MIB..])] {
        let put = s
            .console(&cookie, Method::PUT, &format!("/api/v1/buckets/docs/uploads/{upload_id}/parts/{number}"))
            .query(&[("key", "big/data.bin")])
            .body(chunk.to_vec());
        let (status, part) = s.json(put).await;
        assert_eq!(status, StatusCode::OK, "{part}");
        parts.push(json!({"part_number": number, "etag": part["etag"]}));
    }
    let complete = s
        .console(&cookie, Method::POST, &format!("/api/v1/buckets/docs/uploads/{upload_id}/complete"))
        .json(&json!({"key": "big/data.bin", "parts": parts}));
    let (status, object) = s.json(complete).await;
    assert_eq!(status, StatusCode::OK, "{object}");
    assert!(object["etag"].as_str().unwrap().ends_with("-2"));
    let bytes = s
        .console(&cookie, Method::GET, "/api/v1/buckets/docs/object/download")
        .query(&[("key", "big/data.bin")])
        .send()
        .await
        .unwrap()
        .bytes()
        .await
        .unwrap();
    assert!(bytes == data, "multipart upload round trip");

    // An aborted upload is gone.
    let (_, upload) =
        s.json(s.console(&cookie, Method::POST, "/api/v1/buckets/docs/uploads").json(&json!({"key": "tmp"}))).await;
    let path = format!("/api/v1/buckets/docs/uploads/{}", upload["upload_id"].as_str().unwrap());
    let abort = s.console(&cookie, Method::DELETE, &path).query(&[("key", "tmp")]).send().await.unwrap();
    assert_eq!(abort.status(), StatusCode::NO_CONTENT);
    let late = s.console(&cookie, Method::PUT, &format!("{path}/parts/1")).query(&[("key", "tmp")]).body("x");
    assert_eq!(s.json(late).await.0, StatusCode::NOT_FOUND);

    let delete = s.console(&cookie, Method::DELETE, "/api/v1/buckets/docs/object").query(&[("key", "readme.txt")]);
    assert_eq!(delete.send().await.unwrap().status(), StatusCode::NO_CONTENT);
    let (status, error) =
        s.json(s.console(&cookie, Method::GET, "/api/v1/buckets/docs/object").query(&[("key", "readme.txt")])).await;
    assert_eq!((status, error["error"]["code"].as_str()), (StatusCode::NOT_FOUND, Some("NoSuchKey")));
}

#[tokio::test]
async fn share_links_are_presigned_s3_urls() {
    let s = start().await;
    let cookie = s.login(ROOT_KEY, ROOT_SECRET).await.unwrap();
    s.console(&cookie, Method::POST, "/api/v1/buckets").json(&json!({"name": "media"})).send().await.unwrap();
    let key = "photos/a b+ü.txt";
    s.console(&cookie, Method::PUT, "/api/v1/buckets/media/object")
        .query(&[("key", key)])
        .body("shared")
        .send()
        .await
        .unwrap();

    let share = s.console(&cookie, Method::POST, "/api/v1/buckets/media/object/share");
    let (status, link) = s.json(share.json(&json!({"key": key, "expires_secs": 3600}))).await;
    assert_eq!(status, StatusCode::OK, "{link}");
    let url = link["url"].as_str().unwrap();
    assert!(url.starts_with(&format!("http://{}/media/photos/a%20b%2B%C3%BC.txt?", s.s3)), "{url}");
    // Anyone with the link can download, without credentials.
    let fetched = reqwest::get(url).await.unwrap();
    assert_eq!(fetched.status(), StatusCode::OK);
    assert_eq!(fetched.text().await.unwrap(), "shared");
    let tampered = url.replace("X-Amz-Expires=3600", "X-Amz-Expires=3601");
    assert_eq!(reqwest::get(&tampered).await.unwrap().status(), StatusCode::FORBIDDEN);

    let too_long = s.console(&cookie, Method::POST, "/api/v1/buckets/media/object/share");
    let (status, _) = s.json(too_long.json(&json!({"key": key, "expires_secs": 8 * 24 * 3600}))).await;
    assert_eq!(status, StatusCode::BAD_REQUEST);
    let missing = s.console(&cookie, Method::POST, "/api/v1/buckets/media/object/share");
    let (status, _) = s.json(missing.json(&json!({"key": "nope", "expires_secs": 60}))).await;
    assert_eq!(status, StatusCode::NOT_FOUND);
}

#[tokio::test]
async fn overview_status_and_stats() {
    let s = start().await;
    let cookie = s.login(ROOT_KEY, ROOT_SECRET).await.unwrap();
    let (status, view) = s.json(s.console(&cookie, Method::GET, "/api/v1/status")).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(view["s3_endpoint"].as_str(), Some(format!("http://{}", s.s3).as_str()));
    assert_eq!(view["s3_endpoint_configured"], true);
    assert_eq!(view["block_size"], 64 * MIB);
    assert_eq!(view["gc_pending_blocks"], 0);

    for range in ["1h", "24h", "7d"] {
        let (status, stats) = s.json(s.console(&cookie, Method::GET, "/api/v1/stats").query(&[("range", range)])).await;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(stats["range"], range);
        assert!(stats["bars"].as_array().unwrap().len() >= 12);
        assert!(stats["current"]["requests_per_sec"].is_number());
    }
    let (status, _) = s.json(s.console(&cookie, Method::GET, "/api/v1/stats").query(&[("range", "2d")])).await;
    assert_eq!(status, StatusCode::BAD_REQUEST);

    // The sign-in page asks without credentials.
    let (status, health) = s.json(s.http.get(s.url("/api/v1/health"))).await;
    assert_eq!((status, health["status"].as_str()), (StatusCode::OK, Some("ready")));
}

#[tokio::test]
async fn the_console_is_served_with_strict_headers() {
    let s = start().await;
    let page = s.http.get(s.url("/")).send().await.unwrap();
    assert_eq!(page.status(), StatusCode::OK);
    assert!(page.headers()[CONTENT_TYPE].to_str().unwrap().starts_with("text/html"));
    let csp = page.headers()["content-security-policy"].to_str().unwrap().to_owned();
    assert!(csp.contains("frame-ancestors 'none'") && csp.contains("default-src 'self'"), "{csp}");
    assert_eq!(page.headers()["x-frame-options"], "DENY");

    let (status, error) = s.json(s.http.get(s.url("/api/v1/nope"))).await;
    assert_eq!((status, error["error"]["code"].as_str()), (StatusCode::NOT_FOUND, Some("NotFound")));
    let post = s.http.post(s.url("/keys/")).send().await.unwrap();
    assert_eq!(post.status(), StatusCode::METHOD_NOT_ALLOWED);
}
