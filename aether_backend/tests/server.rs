//! The HTTP layer: TLS, certificate reloading, request ids, metrics,
//! timeouts and shutdown.

use std::net::SocketAddr;
use std::path::Path;
use std::sync::Arc;
use std::time::{Duration, SystemTime};

use aether_backend::config::{Config, Secret};
use aether_backend::engine::Engine;
use aether_backend::http::tls::{ReloadingCert, install_crypto_provider};
use aether_backend::http::{self, Limits};
use aether_backend::server;
use aws_sdk_s3::config::{BehaviorVersion, Credentials, Region};
use aws_sdk_s3::primitives::ByteStream;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::oneshot;

const ROOT_KEY: &str = "root-access";
const ROOT_SECRET: &str = "root-secret-key";

fn limits() -> Limits {
    Limits {
        header_timeout: Duration::from_millis(300),
        handshake_timeout: Duration::from_secs(2),
        shutdown_timeout: Duration::from_millis(300),
    }
}

struct Server {
    s3: SocketAddr,
    admin: SocketAddr,
    stop: Option<oneshot::Sender<()>>,
    done: tokio::task::JoinHandle<()>,
    _dir: tempfile::TempDir,
}

/// Starts both listeners through `http::serve`, optionally with TLS.
async fn start(tls: Option<Arc<rustls::ServerConfig>>) -> Server {
    install_crypto_provider();
    let dir = tempfile::tempdir().unwrap();
    let mut config = Config::default();
    config.storage.data_dir = dir.path().join("data");
    config.s3.access_key = ROOT_KEY.into();
    config.s3.secret_key = Secret::from(ROOT_SECRET);
    let engine = Arc::new(Engine::open(config.engine_config()).await.unwrap());
    let routers = server::routers(engine, &config);

    let s3 = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let admin = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let (s3_addr, admin_addr) = (s3.local_addr().unwrap(), admin.local_addr().unwrap());
    let (stop, stopped) = oneshot::channel::<()>();
    let stopped = futures::FutureExt::shared(async move {
        let _ = stopped.await;
    });
    let done = tokio::spawn(async move {
        let s3 = http::serve(s3, routers.s3, tls.clone(), limits(), stopped.clone());
        let admin = http::serve(admin, routers.admin, tls, limits(), stopped);
        tokio::try_join!(s3, admin).unwrap();
    });
    Server { s3: s3_addr, admin: admin_addr, stop: Some(stop), done, _dir: dir }
}

fn s3_client(addr: SocketAddr) -> aws_sdk_s3::Client {
    let config = aws_sdk_s3::Config::builder()
        .behavior_version(BehaviorVersion::latest())
        .region(Region::new("us-east-1"))
        .endpoint_url(format!("http://{addr}"))
        .credentials_provider(Credentials::new(ROOT_KEY, ROOT_SECRET, None, None, "test"))
        .force_path_style(true)
        .build();
    aws_sdk_s3::Client::from_conf(config)
}

/// A self-signed certificate for localhost and 127.0.0.1, written as PEM files.
fn write_cert(dir: &Path, name: &str) -> (std::path::PathBuf, std::path::PathBuf, String) {
    let generated = rcgen::generate_simple_self_signed(vec!["localhost".to_string(), "127.0.0.1".to_string()]).unwrap();
    let (cert, key) = (dir.join(format!("{name}.crt")), dir.join(format!("{name}.key")));
    std::fs::write(&cert, generated.cert.pem()).unwrap();
    std::fs::write(&key, generated.signing_key.serialize_pem()).unwrap();
    (cert, key, generated.cert.pem())
}

fn https_client(ca_pem: &str) -> reqwest::Client {
    install_crypto_provider();
    reqwest::Client::builder()
        .add_root_certificate(reqwest::Certificate::from_pem(ca_pem.as_bytes()).unwrap())
        .build()
        .unwrap()
}

#[tokio::test]
async fn serves_https_and_reloads_renewed_certificates() {
    install_crypto_provider();
    let certs = tempfile::tempdir().unwrap();
    let (cert_file, key_file, first_pem) = write_cert(certs.path(), "server");
    let cert = ReloadingCert::load(&cert_file, &key_file).unwrap();
    let server = start(Some(cert.server_config().unwrap())).await;

    let client = https_client(&first_pem);
    let response =
        client.get(format!("https://127.0.0.1:{}/_aether/health/ready", server.s3.port())).send().await.unwrap();
    assert_eq!(response.status(), 200);
    let status = client
        .get(format!("https://127.0.0.1:{}/api/v1/status", server.admin.port()))
        .basic_auth(ROOT_KEY, Some(ROOT_SECRET))
        .send()
        .await
        .unwrap();
    assert_eq!(status.status(), 200);

    // Plain HTTP on the TLS port gets nowhere.
    assert!(reqwest::get(format!("http://127.0.0.1:{}/_aether/health/live", server.s3.port())).await.is_err());

    // Renew: a client that only trusts the new certificate fails until the reload.
    let renewed = tempfile::tempdir().unwrap();
    let (new_cert, new_key, second_pem) = write_cert(renewed.path(), "renewed");
    let new_client = https_client(&second_pem);
    let url = format!("https://127.0.0.1:{}/_aether/health/live", server.s3.port());
    assert!(new_client.get(&url).send().await.is_err());
    std::fs::copy(&new_cert, &cert_file).unwrap();
    std::fs::copy(&new_key, &key_file).unwrap();
    let later = SystemTime::now() + Duration::from_secs(5);
    for file in [&cert_file, &key_file] {
        std::fs::File::options().write(true).open(file).unwrap().set_modified(later).unwrap();
    }
    assert!(cert.reload_if_changed().unwrap());
    assert!(!cert.reload_if_changed().unwrap(), "unchanged files are not reloaded");
    assert_eq!(new_client.get(&url).send().await.unwrap().status(), 200);

    // A broken renewal keeps the working certificate.
    std::fs::write(&cert_file, "not a certificate").unwrap();
    std::fs::File::options()
        .write(true)
        .open(&cert_file)
        .unwrap()
        .set_modified(later + Duration::from_secs(5))
        .unwrap();
    assert!(cert.reload_if_changed().is_err());
    assert_eq!(new_client.get(&url).send().await.unwrap().status(), 200);
}

#[tokio::test]
async fn invalid_certificates_are_rejected_at_startup() {
    let certs = tempfile::tempdir().unwrap();
    let (cert_file, _, _) = write_cert(certs.path(), "a");
    let (_, other_key, _) = write_cert(certs.path(), "b");
    let err = ReloadingCert::load(&cert_file, &other_key).unwrap_err();
    assert!(err.to_string().contains("does not match"), "{err}");
    assert!(ReloadingCert::load(Path::new("/nonexistent.crt"), &other_key).is_err());
}

#[tokio::test]
async fn responses_carry_request_ids() {
    let server = start(None).await;
    let s3 = reqwest::get(format!("http://{}/bkt/key", server.s3)).await.unwrap();
    assert_eq!(s3.status(), 403);
    let id = s3.headers()["x-amz-request-id"].to_str().unwrap().to_owned();
    assert_eq!(id.len(), 16);
    let body = s3.text().await.unwrap();
    assert!(body.contains(&format!("<RequestId>{id}</RequestId></Error>")), "{body}");
    let admin = reqwest::get(format!("http://{}/api/v1/status", server.admin)).await.unwrap();
    assert_eq!(admin.status(), 401);
    assert_ne!(admin.headers()["x-request-id"].to_str().unwrap(), id);
}

#[tokio::test]
async fn metrics_describe_traffic_and_usage() {
    let server = start(None).await;
    let client = s3_client(server.s3);
    client.create_bucket().bucket("bkt").send().await.unwrap();
    client.put_object().bucket("bkt").key("k").body(ByteStream::from(vec![7u8; 10_000])).send().await.unwrap();
    client.get_object().bucket("bkt").key("k").send().await.unwrap().body.collect().await.unwrap();
    assert!(client.get_object().bucket("bkt").key("missing").send().await.is_err());
    let quota = reqwest::Client::new()
        .put(format!("http://{}/api/v1/buckets/bkt/quota", server.admin))
        .basic_auth(ROOT_KEY, Some(ROOT_SECRET))
        .json(&serde_json::json!({ "max_bytes": 50_000 }))
        .send()
        .await
        .unwrap();
    assert_eq!(quota.status(), 200);

    let response = reqwest::get(format!("http://{}/metrics", server.admin)).await.unwrap();
    assert_eq!(response.status(), 200, "no credentials needed");
    assert!(response.headers()["content-type"].to_str().unwrap().starts_with("application/openmetrics-text"));
    let text = response.text().await.unwrap();
    for expected in [
        r#"aether_http_requests_total{api="s3",operation="PutObject",status="200"} 1"#,
        r#"aether_http_requests_total{api="s3",operation="GetObject",status="404"} 1"#,
        r#"aether_http_request_duration_seconds_count{api="s3",operation="GetObject"} 2"#,
        r#"aether_bucket_objects{bucket="bkt"} 1"#,
        r#"aether_bucket_bytes{bucket="bkt"} 10000"#,
        r#"aether_bucket_quota_bytes{bucket="bkt"} 50000"#,
        "aether_received_bytes_total 10000",
        "aether_sent_bytes_total 10000",
        "aether_buckets 1",
        "aether_disk_available_bytes ",
        r#"aether_build_info{version=""#,
    ] {
        assert!(text.contains(expected), "missing {expected:?} in:\n{text}");
    }
}

#[tokio::test]
async fn slow_clients_and_shutdown_are_bounded() {
    let mut server = start(None).await;

    // A client that never finishes its headers is disconnected.
    let mut idle = TcpStream::connect(server.s3).await.unwrap();
    idle.write_all(b"GET /_aether/health/live HTTP/1.1\r\n").await.unwrap();
    let mut buf = Vec::new();
    let read = tokio::time::timeout(Duration::from_secs(5), idle.read_to_end(&mut buf)).await;
    assert!(read.is_ok(), "the server should close the connection after the header timeout");

    // Shutdown finishes even while a connection is stuck.
    let _stuck = TcpStream::connect(server.s3).await.unwrap();
    server.stop.take().unwrap().send(()).unwrap();
    tokio::time::timeout(Duration::from_secs(5), &mut server.done).await.expect("shutdown is bounded").unwrap();
    assert!(TcpStream::connect(server.s3).await.is_err(), "no longer accepting");
}

#[tokio::test]
async fn scrub_runs_on_request() {
    let server = start(None).await;
    let client = s3_client(server.s3);
    client.create_bucket().bucket("bkt").send().await.unwrap();
    client.put_object().bucket("bkt").key("k").body(ByteStream::from(vec![1u8; 100_000])).send().await.unwrap();

    let http = reqwest::Client::new();
    let admin = |path: &str| format!("http://{}{path}", server.admin);
    let started = http.post(admin("/api/v1/scrub")).basic_auth(ROOT_KEY, Some(ROOT_SECRET)).send().await.unwrap();
    assert_eq!(started.status(), 202);
    let mut report = serde_json::Value::Null;
    for _ in 0..100 {
        let status: serde_json::Value = http
            .get(admin("/api/v1/status"))
            .basic_auth(ROOT_KEY, Some(ROOT_SECRET))
            .send()
            .await
            .unwrap()
            .json()
            .await
            .unwrap();
        report = status["scrub"].clone();
        if !report["finished_at"].is_null() {
            break;
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    assert_eq!((report["objects_checked"].as_u64(), report["blocks_checked"].as_u64()), (Some(1), Some(1)), "{report}");
    assert_eq!(report["corrupt"], serde_json::json!([]));
}
