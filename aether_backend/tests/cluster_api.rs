//! The internal API between nodes: signed requests, node identity and TLS.

use std::net::SocketAddr;
use std::path::Path;
use std::sync::Arc;
use std::time::Duration;

use aether_backend::cluster::auth::{self, Signed};
use aether_backend::cluster::{ClusterClient, ClusterKey};
use aether_backend::config::{Config, Secret};
use aether_backend::engine::{Engine, now_millis};
use aether_backend::http::tls::{ReloadingCert, install_crypto_provider};
use aether_backend::http::{self, Limits};
use aether_backend::server;
use reqwest::StatusCode;
use serde_json::Value;
use tokio::net::TcpListener;
use tokio::sync::oneshot;
use uuid::Uuid;

const SECRET: &str = "a-cluster-secret-of-32-characters!";

struct Node {
    addr: SocketAddr,
    id: Uuid,
    stop: Option<oneshot::Sender<()>>,
    done: tokio::task::JoinHandle<()>,
}

impl Node {
    async fn stop(mut self) {
        let _ = self.stop.take().unwrap().send(());
        self.done.await.unwrap();
    }
}

fn config(data_dir: &Path) -> Config {
    let mut config = Config::default();
    config.storage.data_dir = data_dir.to_path_buf();
    config.s3.access_key = "root-access".into();
    config.s3.secret_key = Secret::from("root-secret-key");
    config.cluster.enabled = true;
    config.cluster.secret = Secret::from(SECRET);
    config.cluster.zone = Some("rack-1".into());
    config
}

/// Serves a node's internal API, as `server::run` does.
async fn start(data_dir: &Path, tls: Option<Arc<rustls::ServerConfig>>) -> Node {
    install_crypto_provider();
    let config = config(data_dir);
    let engine = Arc::new(Engine::open(config.engine_config()).await.unwrap());
    let id = engine.node_id();
    let routers = server::routers(engine, &config);
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let (stop, stopped) = oneshot::channel::<()>();
    let limits = Limits {
        header_timeout: Duration::from_secs(5),
        handshake_timeout: Duration::from_secs(5),
        shutdown_timeout: Duration::from_millis(300),
    };
    let cluster = routers.cluster.expect("cluster.enabled");
    let done = tokio::spawn(async move {
        http::serve(listener, cluster, tls, limits, async move {
            let _ = stopped.await;
        })
        .await
        .unwrap();
    });
    Node { addr, id, stop: Some(stop), done }
}

fn client(secret: &str) -> ClusterClient {
    ClusterClient::new(ClusterKey::derive(secret), "test-node", None).unwrap()
}

#[tokio::test]
async fn health_needs_the_cluster_secret() {
    let dir = tempfile::tempdir().unwrap();
    let node = start(dir.path(), None).await;
    let base = format!("http://{}", node.addr);

    let health = client(SECRET).health(&base).await.unwrap();
    assert_eq!(health.node_id, node.id);
    assert_eq!(health.zone.as_deref(), Some("rack-1"));
    assert!(health.ready);

    let wrong = client("another-secret-also-32-characters!").health(&base).await.unwrap_err();
    assert!(format!("{wrong:#}").contains("cluster.secret must be the same"), "{wrong:#}");

    // Unsigned requests, over HTTP/1.1 or HTTP/2, learn nothing, not even which routes exist.
    for http in [reqwest::Client::new(), reqwest::Client::builder().http2_prior_knowledge().build().unwrap()] {
        for path in ["/internal/v1/health", "/internal/v1/nothing-here", "/"] {
            let response = http.get(format!("{base}{path}")).send().await.unwrap();
            assert_eq!(response.status(), StatusCode::UNAUTHORIZED, "{path}");
            let body: Value = response.json().await.unwrap();
            assert_eq!(body["error"]["code"], "Unauthorized");
        }
    }
    // Refused before its body is read: 401, not 413 for the oversized body.
    // (HTTP/2 lets the server answer before the upload ends.)
    let response = reqwest::Client::builder()
        .http2_prior_knowledge()
        .build()
        .unwrap()
        .post(format!("{base}/internal/v1/health"))
        .body(vec![0u8; 2 * 1024 * 1024])
        .send()
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::UNAUTHORIZED);
    node.stop().await;
}

/// Sends one hand-signed request twice: the second time it is refused.
#[tokio::test]
async fn signed_requests_cannot_be_replayed_or_altered() {
    let dir = tempfile::tempdir().unwrap();
    let node = start(dir.path(), None).await;
    let key = ClusterKey::derive(SECRET);
    let (target, date, nonce, content) =
        ("/internal/v1/health", now_millis(), auth::new_nonce(), auth::body_sha256(b""));
    let signature =
        key.sign(&Signed { method: "GET", target, node: "node-x", date, nonce: &nonce, content_sha256: &content });
    let http = reqwest::Client::builder().http2_prior_knowledge().build().unwrap();
    let send = |path: &str| {
        http.get(format!("http://{}{path}", node.addr))
            .header(auth::NODE, "node-x")
            .header(auth::DATE, date.to_string())
            .header(auth::NONCE, &nonce)
            .header(auth::CONTENT_SHA256, &content)
            .header(auth::SIGNATURE, &signature)
            .send()
    };
    // Signed for another path: refused, and the nonce is still unused.
    assert_eq!(send("/internal/v1/health?x=1").await.unwrap().status(), StatusCode::UNAUTHORIZED);
    assert_eq!(send(target).await.unwrap().status(), StatusCode::OK);
    let replayed = send(target).await.unwrap();
    assert_eq!(replayed.status(), StatusCode::UNAUTHORIZED);
    let body: Value = replayed.json().await.unwrap();
    assert!(body["error"]["message"].as_str().unwrap().contains("already received"));
    node.stop().await;
}

#[tokio::test]
async fn unknown_routes_are_not_found_once_signed() {
    let dir = tempfile::tempdir().unwrap();
    let node = start(dir.path(), None).await;
    let key = ClusterKey::derive(SECRET);
    let (target, date, nonce, content) =
        ("/internal/v1/raft/vote", now_millis(), auth::new_nonce(), auth::body_sha256(b""));
    let signature =
        key.sign(&Signed { method: "GET", target, node: "node-x", date, nonce: &nonce, content_sha256: &content });
    let response = reqwest::Client::new()
        .get(format!("http://{}{target}", node.addr))
        .header(auth::NODE, "node-x")
        .header(auth::DATE, date.to_string())
        .header(auth::NONCE, &nonce)
        .header(auth::CONTENT_SHA256, &content)
        .header(auth::SIGNATURE, &signature)
        .send()
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::NOT_FOUND);
    node.stop().await;
}

#[tokio::test]
async fn a_node_keeps_its_identity_across_restarts() {
    let dir = tempfile::tempdir().unwrap();
    let first = start(dir.path(), None).await;
    let id = first.id;
    first.stop().await;
    let again = start(dir.path(), None).await;
    let health = client(SECRET).health(&format!("http://{}", again.addr)).await.unwrap();
    assert_eq!(health.node_id, id);
    again.stop().await;

    let other = tempfile::tempdir().unwrap();
    let different = start(other.path(), None).await;
    assert_ne!(different.id, id);
    different.stop().await;
}

#[tokio::test]
async fn the_internal_api_uses_tls_when_configured() {
    install_crypto_provider();
    let certs = tempfile::tempdir().unwrap();
    let generated = rcgen::generate_simple_self_signed(vec!["localhost".to_string()]).unwrap();
    let (cert_file, key_file, ca_file) =
        (certs.path().join("node.crt"), certs.path().join("node.key"), certs.path().join("ca.pem"));
    std::fs::write(&cert_file, generated.cert.pem()).unwrap();
    std::fs::write(&key_file, generated.signing_key.serialize_pem()).unwrap();
    std::fs::write(&ca_file, generated.cert.pem()).unwrap();
    let tls = ReloadingCert::load(&cert_file, &key_file).unwrap().server_config().unwrap();

    let dir = tempfile::tempdir().unwrap();
    let node = start(dir.path(), Some(tls)).await;
    let base = format!("https://localhost:{}", node.addr.port());
    let trusting = ClusterClient::new(ClusterKey::derive(SECRET), "test-node", Some(&ca_file)).unwrap();
    assert_eq!(trusting.health(&base).await.unwrap().node_id, node.id);
    // Without the CA, the self-signed certificate is not trusted.
    assert!(client(SECRET).health(&base).await.is_err());
    node.stop().await;
}

#[tokio::test]
async fn clustering_is_off_by_default() {
    let dir = tempfile::tempdir().unwrap();
    let mut config = config(dir.path());
    config.cluster = Default::default();
    let engine = Arc::new(Engine::open(config.engine_config()).await.unwrap());
    assert!(server::routers(engine, &config).cluster.is_none());
}
