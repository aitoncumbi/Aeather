//! End-to-end tests: the real HTTP server, driven by the official AWS SDK.

use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;

use aether_backend::config::{Config, Secret};
use aether_backend::engine::Engine;
use aether_backend::server;
use aws_sdk_s3::Client;
use aws_sdk_s3::config::{BehaviorVersion, Credentials, Region};
use aws_sdk_s3::error::ProvideErrorMetadata;
use aws_sdk_s3::presigning::PresigningConfig;
use aws_sdk_s3::primitives::ByteStream;
use aws_sdk_s3::types::{ChecksumMode, Delete, ObjectIdentifier};
use base64::Engine as _;
use base64::engine::general_purpose::STANDARD;
use md5::{Digest, Md5};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};

const ACCESS_KEY: &str = "test-access";
const SECRET_KEY: &str = "test-secret-key";

struct TestServer {
    client: Client,
    addr: SocketAddr,
    _dir: tempfile::TempDir,
}

async fn start() -> TestServer {
    let dir = tempfile::tempdir().unwrap();
    let mut config = Config::default();
    config.storage.data_dir = dir.path().to_path_buf();
    config.storage.block_size = 1024 * 1024;
    config.s3.access_key = ACCESS_KEY.into();
    config.s3.secret_key = Secret::from(SECRET_KEY);

    let engine = Arc::new(Engine::open(config.engine_config()).await.unwrap());
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let app = server::router(engine, &config);
    tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });

    TestServer { client: client(addr, SECRET_KEY), addr, _dir: dir }
}

fn client(addr: SocketAddr, secret: &str) -> Client {
    let config = aws_sdk_s3::Config::builder()
        .behavior_version(BehaviorVersion::latest())
        .region(Region::new("us-east-1"))
        .endpoint_url(format!("http://{addr}"))
        .credentials_provider(Credentials::new(ACCESS_KEY, secret, None, None, "test"))
        .force_path_style(true)
        .build();
    Client::from_conf(config)
}

fn pattern(len: usize) -> Vec<u8> {
    (0..len).map(|i| (i * 31 % 251) as u8).collect()
}

fn md5_hex(data: &[u8]) -> String {
    hex::encode(Md5::digest(data))
}

/// Minimal HTTP/1.1 GET, for requests the SDK cannot make (unsigned, presigned).
async fn raw_get(addr: SocketAddr, path_and_query: &str) -> (u16, Vec<u8>) {
    let mut stream = TcpStream::connect(addr).await.unwrap();
    let request = format!("GET {path_and_query} HTTP/1.1\r\nHost: {addr}\r\nConnection: close\r\n\r\n");
    stream.write_all(request.as_bytes()).await.unwrap();
    let mut response = Vec::new();
    stream.read_to_end(&mut response).await.unwrap();
    let split = response.windows(4).position(|w| w == b"\r\n\r\n").unwrap();
    let head = String::from_utf8_lossy(&response[..split]).to_string();
    let status = head.split(' ').nth(1).unwrap().parse().unwrap();
    (status, response[split + 4..].to_vec())
}

#[tokio::test]
async fn bucket_operations() {
    let s = start().await;
    let c = &s.client;
    c.create_bucket().bucket("alpha").send().await.unwrap();
    c.create_bucket().bucket("beta").send().await.unwrap();

    let err = c.create_bucket().bucket("alpha").send().await.unwrap_err();
    assert_eq!(err.code(), Some("BucketAlreadyOwnedByYou"));

    let names: Vec<_> =
        c.list_buckets().send().await.unwrap().buckets().iter().filter_map(|b| b.name()).map(str::to_owned).collect();
    assert_eq!(names, ["alpha", "beta"]);

    c.head_bucket().bucket("alpha").send().await.unwrap();
    assert!(c.head_bucket().bucket("missing").send().await.is_err());
    c.get_bucket_location().bucket("alpha").send().await.unwrap();

    c.put_object().bucket("alpha").key("x").body(ByteStream::from_static(b"x")).send().await.unwrap();
    let err = c.delete_bucket().bucket("alpha").send().await.unwrap_err();
    assert_eq!(err.code(), Some("BucketNotEmpty"));
    c.delete_object().bucket("alpha").key("x").send().await.unwrap();
    c.delete_bucket().bucket("alpha").send().await.unwrap();

    let err = c.get_object().bucket("alpha").key("x").send().await.unwrap_err();
    assert_eq!(err.code(), Some("NoSuchBucket"));
}

#[tokio::test]
async fn small_object_round_trip() {
    let s = start().await;
    let c = &s.client;
    c.create_bucket().bucket("docs").send().await.unwrap();

    let put = c
        .put_object()
        .bucket("docs")
        .key("notes/hello world.txt")
        .content_type("text/plain")
        .cache_control("max-age=60")
        .metadata("owner", "ops")
        .body(ByteStream::from_static(b"hello world"))
        .send()
        .await
        .unwrap();
    let etag = format!("\"{}\"", md5_hex(b"hello world"));
    assert_eq!(put.e_tag(), Some(etag.as_str()));

    let got = c.get_object().bucket("docs").key("notes/hello world.txt").send().await.unwrap();
    assert_eq!(got.content_type(), Some("text/plain"));
    assert_eq!(got.cache_control(), Some("max-age=60"));
    assert_eq!(got.metadata().unwrap().get("owner").map(String::as_str), Some("ops"));
    assert_eq!(got.e_tag(), Some(etag.as_str()));
    assert_eq!(got.content_length(), Some(11));
    assert_eq!(got.body.collect().await.unwrap().into_bytes().as_ref(), b"hello world");

    let head = c.head_object().bucket("docs").key("notes/hello world.txt").send().await.unwrap();
    assert_eq!(head.content_length(), Some(11));
    assert_eq!(head.e_tag(), Some(etag.as_str()));

    let range = c.get_object().bucket("docs").key("notes/hello world.txt").range("bytes=6-").send().await.unwrap();
    assert_eq!(range.content_range(), Some("bytes 6-10/11"));
    assert_eq!(range.body.collect().await.unwrap().into_bytes().as_ref(), b"world");

    let response_override = c
        .get_object()
        .bucket("docs")
        .key("notes/hello world.txt")
        .response_content_type("application/octet-stream")
        .send()
        .await
        .unwrap();
    assert_eq!(response_override.content_type(), Some("application/octet-stream"));

    c.put_object().bucket("docs").key("empty").body(ByteStream::from_static(b"")).send().await.unwrap();
    let empty = c.get_object().bucket("docs").key("empty").send().await.unwrap();
    assert_eq!(empty.content_length(), Some(0));
    assert!(empty.body.collect().await.unwrap().into_bytes().is_empty());

    let err = c.get_object().bucket("docs").key("missing").send().await.unwrap_err();
    assert_eq!(err.code(), Some("NoSuchKey"));
    let err = c.get_object().bucket("docs").key("empty").range("bytes=0-0").send().await.unwrap_err();
    assert_eq!(err.code(), Some("InvalidRange"));
}

#[tokio::test]
async fn large_object_with_sdk_default_checksums() {
    let s = start().await;
    let c = &s.client;
    c.create_bucket().bucket("media").send().await.unwrap();

    // Spans several 1 MiB blocks. The SDK sends it aws-chunked with a CRC32 trailer.
    let data = pattern(5 * 1024 * 1024 + 123);
    let put =
        c.put_object().bucket("media").key("video.bin").body(ByteStream::from(data.clone())).send().await.unwrap();
    let sent_crc32 = put.checksum_crc32().expect("SDK sends a CRC32 by default").to_owned();

    let got =
        c.get_object().bucket("media").key("video.bin").checksum_mode(ChecksumMode::Enabled).send().await.unwrap();
    assert_eq!(got.checksum_crc32(), Some(sent_crc32.as_str()));
    assert_eq!(got.content_length(), Some(data.len() as i64));
    assert_eq!(got.body.collect().await.unwrap().into_bytes().as_ref(), data.as_slice());

    let start = 1024 * 1024 - 10;
    let end = 3 * 1024 * 1024 + 5;
    let part =
        c.get_object().bucket("media").key("video.bin").range(format!("bytes={start}-{end}")).send().await.unwrap();
    assert_eq!(part.body.collect().await.unwrap().into_bytes().as_ref(), &data[start..=end]);
}

#[tokio::test]
async fn integrity_checks_reject_corrupt_uploads() {
    let s = start().await;
    let c = &s.client;
    c.create_bucket().bucket("bkt").send().await.unwrap();

    let wrong_sha256 = STANDARD.encode([0u8; 32]);
    let err = c
        .put_object()
        .bucket("bkt")
        .key("k")
        .checksum_sha256(wrong_sha256)
        .body(ByteStream::from_static(b"data"))
        .send()
        .await
        .unwrap_err();
    assert_eq!(err.code(), Some("BadDigest"));

    let wrong_md5 = STANDARD.encode([0u8; 16]);
    let err = c
        .put_object()
        .bucket("bkt")
        .key("k")
        .content_md5(wrong_md5)
        .body(ByteStream::from_static(b"data"))
        .send()
        .await
        .unwrap_err();
    assert_eq!(err.code(), Some("BadDigest"));

    let right_md5 = STANDARD.encode(Md5::digest(b"data"));
    c.put_object()
        .bucket("bkt")
        .key("k")
        .content_md5(right_md5)
        .body(ByteStream::from_static(b"data"))
        .send()
        .await
        .unwrap();

    // Nothing from the failed uploads became visible.
    let listed = c.list_objects_v2().bucket("bkt").send().await.unwrap();
    assert_eq!(listed.key_count(), Some(1));
}

#[tokio::test]
async fn conditional_requests() {
    let s = start().await;
    let c = &s.client;
    c.create_bucket().bucket("bkt").send().await.unwrap();
    let put = c.put_object().bucket("bkt").key("k").body(ByteStream::from_static(b"v1")).send().await.unwrap();
    let etag = put.e_tag().unwrap().to_owned();

    let err = c
        .put_object()
        .bucket("bkt")
        .key("k")
        .if_none_match("*")
        .body(ByteStream::from_static(b"v2"))
        .send()
        .await
        .unwrap_err();
    assert_eq!(err.code(), Some("PreconditionFailed"));
    c.put_object()
        .bucket("bkt")
        .key("new")
        .if_none_match("*")
        .body(ByteStream::from_static(b"v1"))
        .send()
        .await
        .unwrap();

    let err = c
        .put_object()
        .bucket("bkt")
        .key("k")
        .if_match("\"nope\"")
        .body(ByteStream::from_static(b"v2"))
        .send()
        .await
        .unwrap_err();
    assert_eq!(err.code(), Some("PreconditionFailed"));
    c.put_object().bucket("bkt").key("k").if_match(&etag).body(ByteStream::from_static(b"v2")).send().await.unwrap();

    let current = c.head_object().bucket("bkt").key("k").send().await.unwrap();
    let current_etag = current.e_tag().unwrap();
    let err = c.get_object().bucket("bkt").key("k").if_none_match(current_etag).send().await.unwrap_err();
    assert_eq!(err.raw_response().map(|r| r.status().as_u16()), Some(304));
    let err = c.get_object().bucket("bkt").key("k").if_match(&etag).send().await.unwrap_err();
    assert_eq!(err.raw_response().map(|r| r.status().as_u16()), Some(412));
    c.get_object().bucket("bkt").key("k").if_match(current_etag).send().await.unwrap();
}

#[tokio::test]
async fn listing_with_pagination_and_delimiter() {
    let s = start().await;
    let c = &s.client;
    c.create_bucket().bucket("bkt").send().await.unwrap();
    let keys = ["a.txt", "dir/1", "dir/2", "dir/sub/3", "other/x", "z y+z.txt"];
    for key in keys {
        c.put_object().bucket("bkt").key(key).body(ByteStream::from_static(b"x")).send().await.unwrap();
    }

    let mut all = Vec::new();
    let mut pages = c.list_objects_v2().bucket("bkt").max_keys(2).into_paginator().send();
    while let Some(page) = pages.next().await {
        all.extend(page.unwrap().contents().iter().map(|o| o.key().unwrap().to_owned()));
    }
    assert_eq!(all, keys);

    let mut objects = Vec::new();
    let mut prefixes = Vec::new();
    let mut pages = c.list_objects_v2().bucket("bkt").delimiter("/").max_keys(1).into_paginator().send();
    while let Some(page) = pages.next().await {
        let page = page.unwrap();
        objects.extend(page.contents().iter().map(|o| o.key().unwrap().to_owned()));
        prefixes.extend(page.common_prefixes().iter().map(|p| p.prefix().unwrap().to_owned()));
    }
    assert_eq!(objects, ["a.txt", "z y+z.txt"]);
    assert_eq!(prefixes, ["dir/", "other/"]);

    let under_dir = c.list_objects_v2().bucket("bkt").prefix("dir/").delimiter("/").send().await.unwrap();
    let keys: Vec<_> = under_dir.contents().iter().map(|o| o.key().unwrap()).collect();
    assert_eq!(keys, ["dir/1", "dir/2"]);
    assert_eq!(under_dir.common_prefixes()[0].prefix(), Some("dir/sub/"));

    let after = c.list_objects_v2().bucket("bkt").start_after("dir/2").send().await.unwrap();
    assert_eq!(after.contents()[0].key(), Some("dir/sub/3"));

    // ListObjects v1, paging with NextMarker.
    let mut marker: Option<String> = None;
    let mut seen = Vec::new();
    loop {
        let page =
            c.list_objects().bucket("bkt").delimiter("/").max_keys(1).set_marker(marker.clone()).send().await.unwrap();
        seen.extend(page.contents().iter().map(|o| o.key().unwrap().to_owned()));
        seen.extend(page.common_prefixes().iter().map(|p| p.prefix().unwrap().to_owned()));
        if page.is_truncated() != Some(true) {
            break;
        }
        marker = page.next_marker().map(str::to_owned);
    }
    assert_eq!(seen, ["a.txt", "dir/", "other/", "z y+z.txt"]);
}

#[tokio::test]
async fn batch_delete() {
    let s = start().await;
    let c = &s.client;
    c.create_bucket().bucket("bkt").send().await.unwrap();
    for key in ["a", "b", "c"] {
        c.put_object().bucket("bkt").key(key).body(ByteStream::from_static(b"x")).send().await.unwrap();
    }
    let ids = ["a", "b", "missing"].map(|k| ObjectIdentifier::builder().key(k).build().unwrap());
    let delete = Delete::builder().set_objects(Some(ids.to_vec())).build().unwrap();
    let out = c.delete_objects().bucket("bkt").delete(delete).send().await.unwrap();
    assert_eq!(out.deleted().len(), 3);
    assert!(out.errors().is_empty());

    let left = c.list_objects_v2().bucket("bkt").send().await.unwrap();
    assert_eq!(left.contents().iter().map(|o| o.key().unwrap()).collect::<Vec<_>>(), ["c"]);
}

#[tokio::test]
async fn authentication() {
    let s = start().await;
    s.client.create_bucket().bucket("bkt").send().await.unwrap();
    s.client.put_object().bucket("bkt").key("k").body(ByteStream::from_static(b"secret")).send().await.unwrap();

    let err = client(s.addr, "wrong-secret").list_buckets().send().await.unwrap_err();
    assert_eq!(err.code(), Some("SignatureDoesNotMatch"));

    let (status, _) = raw_get(s.addr, "/bkt/k").await;
    assert_eq!(status, 403, "unsigned requests are rejected");

    let presigned = s
        .client
        .get_object()
        .bucket("bkt")
        .key("k")
        .presigned(PresigningConfig::expires_in(Duration::from_secs(60)).unwrap())
        .await
        .unwrap();
    let uri: axum::http::Uri = presigned.uri().parse().unwrap();
    let (status, body) = raw_get(s.addr, uri.path_and_query().unwrap().as_str()).await;
    assert_eq!(status, 200);
    assert_eq!(body, b"secret");
}

#[tokio::test]
async fn health_endpoints() {
    let s = start().await;
    assert_eq!(raw_get(s.addr, "/_aether/health/live").await, (200, b"ok".to_vec()));
    assert_eq!(raw_get(s.addr, "/_aether/health/ready").await, (200, b"ready".to_vec()));
}
