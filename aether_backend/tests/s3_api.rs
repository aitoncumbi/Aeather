//! End-to-end tests: the real HTTP server, driven by the official AWS SDK.

use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;

use aether_backend::config::{Config, Secret};
use aether_backend::engine::{Engine, NewKey};
use aether_backend::meta::{BucketGrant, KeyUpdate};
use aether_backend::server;
use aws_sdk_s3::Client;
use aws_sdk_s3::config::{BehaviorVersion, Credentials, Region};
use aws_sdk_s3::error::ProvideErrorMetadata;
use aws_sdk_s3::presigning::PresigningConfig;
use aws_sdk_s3::primitives::ByteStream;
use aws_sdk_s3::types::{
    ChecksumAlgorithm, ChecksumMode, ChecksumType, CompletedMultipartUpload, CompletedPart, Delete, MetadataDirective,
    ObjectIdentifier,
};
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
    engine: Arc<Engine>,
    _dir: tempfile::TempDir,
}

async fn start() -> TestServer {
    start_with(|_| {}).await
}

async fn start_with(configure: impl FnOnce(&mut Config)) -> TestServer {
    let dir = tempfile::tempdir().unwrap();
    let mut config = Config::default();
    config.storage.data_dir = dir.path().to_path_buf();
    config.storage.block_size = 1024 * 1024;
    config.s3.access_key = ACCESS_KEY.into();
    config.s3.secret_key = Secret::from(SECRET_KEY);
    configure(&mut config);

    let engine = Arc::new(Engine::open(config.engine_config()).await.unwrap());
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let app = server::routers(Arc::clone(&engine), &config).s3;
    tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });

    TestServer { client: client(addr, SECRET_KEY), addr, engine, _dir: dir }
}

fn client(addr: SocketAddr, secret: &str) -> Client {
    client_for(addr, ACCESS_KEY, secret)
}

fn client_for(addr: SocketAddr, access_key: &str, secret: &str) -> Client {
    let config = aws_sdk_s3::Config::builder()
        .behavior_version(BehaviorVersion::latest())
        .region(Region::new("us-east-1"))
        .endpoint_url(format!("http://{addr}"))
        .credentials_provider(Credentials::new(access_key, secret, None, None, "test"))
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
    // A 304 repeats the validators.
    let headers = err_headers(c.get_object().bucket("bkt").key("k").if_none_match(current_etag).send().await);
    assert_eq!(headers.get("etag").map(String::as_str), Some(current_etag));
    assert!(headers.contains_key("last-modified"));

    // If-None-Match: <etag> writes unless the key holds that object.
    let put_v3 = |cond: &str| {
        c.put_object().bucket("bkt").key("k").if_none_match(cond).body(ByteStream::from_static(b"v3")).send()
    };
    assert_eq!(put_v3(current_etag).await.unwrap_err().code(), Some("PreconditionFailed"));
    put_v3("\"other\"").await.unwrap();
    // If-Match: * writes only over an existing object.
    let put_star = |key: &'static str| {
        c.put_object().bucket("bkt").key(key).if_match("*").body(ByteStream::from_static(b"v4")).send()
    };
    put_star("k").await.unwrap();
    assert_eq!(put_star("absent").await.unwrap_err().code(), Some("NoSuchKey"));

    // Conditional deletes; a missing key is deleted whatever the condition.
    let latest = c.head_object().bucket("bkt").key("k").send().await.unwrap();
    let err = c.delete_object().bucket("bkt").key("k").if_match("\"other\"").send().await.unwrap_err();
    assert_eq!(err.code(), Some("PreconditionFailed"));
    let err = c.delete_object().bucket("bkt").key("k").if_match_size(99).send().await.unwrap_err();
    assert_eq!(err.code(), Some("PreconditionFailed"));
    c.delete_object()
        .bucket("bkt")
        .key("k")
        .if_match(latest.e_tag().unwrap())
        .if_match_size(2)
        .if_match_last_modified_time(*latest.last_modified().unwrap())
        .send()
        .await
        .unwrap();
    c.delete_object().bucket("bkt").key("k").if_match("\"other\"").send().await.unwrap();
}

/// Response headers of a request that failed with an HTTP error.
fn err_headers<T, E: std::fmt::Debug>(
    result: Result<T, aws_sdk_s3::error::SdkError<E>>,
) -> std::collections::HashMap<String, String> {
    let Err(err) = result else { panic!("the request succeeded") };
    let response = err.raw_response().expect("an HTTP response");
    response.headers().iter().map(|(k, v)| (k.to_owned(), v.to_owned())).collect()
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

    let v1 = c.list_objects().bucket("bkt").delimiter("").send().await.unwrap();
    assert_eq!(v1.marker(), Some(""), "v1 always returns Marker");
    assert_eq!(v1.delimiter(), None, "an empty delimiter is not echoed");
    assert_eq!(v1.contents().len(), 6);
    let none = c.list_objects_v2().bucket("bkt").max_keys(0).send().await.unwrap();
    assert_eq!((none.key_count(), none.is_truncated()), (Some(0), Some(false)));
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

    // Per-key conditions: a mismatch is reported for that key only.
    for key in ["d", "e"] {
        c.put_object().bucket("bkt").key(key).body(ByteStream::from_static(b"x")).send().await.unwrap();
    }
    let ids = [
        ObjectIdentifier::builder().key("c").e_tag("\"wrong\"").build().unwrap(),
        ObjectIdentifier::builder().key("d").e_tag(format!("\"{}\"", md5_hex(b"x"))).build().unwrap(),
        ObjectIdentifier::builder().key("e").size(1).build().unwrap(),
    ];
    let delete = Delete::builder().set_objects(Some(ids.to_vec())).build().unwrap();
    let out = c.delete_objects().bucket("bkt").delete(delete).send().await.unwrap();
    let deleted: Vec<_> = out.deleted().iter().map(|d| d.key().unwrap()).collect();
    assert_eq!(deleted, ["d", "e"]);
    assert_eq!(out.errors().len(), 1);
    assert_eq!((out.errors()[0].key(), out.errors()[0].code()), (Some("c"), Some("PreconditionFailed")));
}

#[tokio::test]
async fn list_buckets_in_pages() {
    let s = start().await;
    let c = &s.client;
    for name in ["b-one", "b-two", "b-three", "other"] {
        c.create_bucket().bucket(name).send().await.unwrap();
    }
    let mut names = Vec::new();
    let mut token = None;
    loop {
        let page = c.list_buckets().max_buckets(2).prefix("b-").set_continuation_token(token).send().await.unwrap();
        names.extend(page.buckets().iter().map(|b| b.name().unwrap().to_owned()));
        token = page.continuation_token().map(str::to_owned);
        if token.is_none() {
            break;
        }
    }
    assert_eq!(names, ["b-one", "b-three", "b-two"]);
    assert_eq!(c.list_buckets().send().await.unwrap().buckets().len(), 4);
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

/// A legacy SigV2 presigned GET URL (what boto3 makes by default).
fn sig_v2_presigned_path(bucket_and_key: &str) -> String {
    use hmac::{KeyInit, Mac};
    let expires = std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).unwrap().as_secs() + 300;
    let mut mac = hmac::Hmac::<sha1::Sha1>::new_from_slice(SECRET_KEY.as_bytes()).unwrap();
    mac.update(format!("GET\n\n\n{expires}\n/{bucket_and_key}").as_bytes());
    let signature = STANDARD.encode(mac.finalize().into_bytes());
    let signature = signature.replace('+', "%2B").replace('/', "%2F").replace('=', "%3D");
    format!("/{bucket_and_key}?AWSAccessKeyId={ACCESS_KEY}&Expires={expires}&Signature={signature}")
}

#[tokio::test]
async fn sig_v2_is_accepted_only_when_enabled() {
    for allow in [false, true] {
        let s = start_with(|config| config.s3.allow_sig_v2 = allow).await;
        s.client.create_bucket().bucket("bkt").send().await.unwrap();
        s.client.put_object().bucket("bkt").key("k").body(ByteStream::from_static(b"v2")).send().await.unwrap();
        let (status, body) = raw_get(s.addr, &sig_v2_presigned_path("bkt/k")).await;
        if allow {
            assert_eq!((status, body.as_slice()), (200, b"v2".as_slice()));
        } else {
            assert_eq!(status, 403, "{}", String::from_utf8_lossy(&body));
        }
    }
}

#[tokio::test]
async fn health_endpoints() {
    let s = start().await;
    assert_eq!(raw_get(s.addr, "/_aether/health/live").await, (200, b"ok".to_vec()));
    assert_eq!(raw_get(s.addr, "/_aether/health/ready").await, (200, b"ready".to_vec()));
}

// ---- multipart uploads and copies ----

const MIB: usize = 1024 * 1024;

fn other_pattern(len: usize) -> Vec<u8> {
    (0..len).map(|i| (i * 7 % 13) as u8).collect()
}

async fn get_bytes(c: &Client, bucket: &str, key: &str) -> Vec<u8> {
    let out = c.get_object().bucket(bucket).key(key).send().await.unwrap();
    out.body.collect().await.unwrap().into_bytes().to_vec()
}

fn completed(parts: &[(i32, String)]) -> CompletedMultipartUpload {
    let parts = parts.iter().map(|(n, etag)| CompletedPart::builder().part_number(*n).e_tag(etag).build()).collect();
    CompletedMultipartUpload::builder().set_parts(Some(parts)).build()
}

#[tokio::test]
async fn multipart_upload_round_trip() {
    let s = start().await;
    let c = &s.client;
    c.create_bucket().bucket("bkt").send().await.unwrap();

    let created = c
        .create_multipart_upload()
        .bucket("bkt")
        .key("videos/big.bin")
        .content_type("video/mp4")
        .metadata("camera", "front")
        .send()
        .await
        .unwrap();
    let upload_id = created.upload_id().unwrap().to_owned();

    let data = [pattern(5 * MIB), other_pattern(5 * MIB), pattern(123_457)];
    let mut parts = Vec::new();
    // Upload out of order, as parallel clients do.
    for n in [2, 1, 3] {
        let out = c
            .upload_part()
            .bucket("bkt")
            .key("videos/big.bin")
            .upload_id(&upload_id)
            .part_number(n)
            .body(ByteStream::from(data[n as usize - 1].clone()))
            .send()
            .await
            .unwrap();
        assert_eq!(out.e_tag().unwrap(), format!("\"{}\"", md5_hex(&data[n as usize - 1])));
        parts.push((n, out.e_tag().unwrap().to_owned()));
    }
    parts.sort();

    let listed = c.list_parts().bucket("bkt").key("videos/big.bin").upload_id(&upload_id).send().await.unwrap();
    let sizes: Vec<_> = listed.parts().iter().map(|p| (p.part_number().unwrap(), p.size().unwrap())).collect();
    assert_eq!(sizes, [(1, 5 * MIB as i64), (2, 5 * MIB as i64), (3, 123_457)]);
    let uploads = c.list_multipart_uploads().bucket("bkt").send().await.unwrap();
    assert_eq!(uploads.uploads()[0].upload_id(), Some(upload_id.as_str()));

    let done = c
        .complete_multipart_upload()
        .bucket("bkt")
        .key("videos/big.bin")
        .upload_id(&upload_id)
        .multipart_upload(completed(&parts))
        .send()
        .await
        .unwrap();
    assert!(done.e_tag().unwrap().ends_with("-3\""), "{:?}", done.e_tag());

    let whole = data.concat();
    let got = c.get_object().bucket("bkt").key("videos/big.bin").send().await.unwrap();
    assert_eq!(got.content_type(), Some("video/mp4"));
    assert_eq!(got.metadata().unwrap().get("camera").map(String::as_str), Some("front"));
    assert_eq!(got.e_tag(), done.e_tag());
    assert_eq!(got.body.collect().await.unwrap().into_bytes().as_ref(), whole.as_slice());

    let head = c.head_object().bucket("bkt").key("videos/big.bin").part_number(2).send().await.unwrap();
    assert_eq!((head.content_length(), head.parts_count()), (Some(5 * MIB as i64), Some(3)));
    let part3 = c.get_object().bucket("bkt").key("videos/big.bin").part_number(3).send().await.unwrap();
    assert_eq!(part3.content_range(), Some(format!("bytes {}-{}/{}", 10 * MIB, whole.len() - 1, whole.len()).as_str()));
    assert_eq!(part3.body.collect().await.unwrap().into_bytes().as_ref(), data[2].as_slice());
    let err = c.get_object().bucket("bkt").key("videos/big.bin").part_number(4).send().await.unwrap_err();
    assert_eq!(err.code(), Some("InvalidPartNumber"));

    assert!(c.list_multipart_uploads().bucket("bkt").send().await.unwrap().uploads().is_empty());
}

#[tokio::test]
async fn multipart_full_object_crc32_is_verified_by_the_sdk() {
    let s = start().await;
    let c = &s.client;
    c.create_bucket().bucket("bkt").send().await.unwrap();
    let created = c
        .create_multipart_upload()
        .bucket("bkt")
        .key("k")
        .checksum_algorithm(ChecksumAlgorithm::Crc32)
        .checksum_type(ChecksumType::FullObject)
        .send()
        .await
        .unwrap();
    assert_eq!(created.checksum_type(), Some(&ChecksumType::FullObject));
    let upload_id = created.upload_id().unwrap();

    let data = [pattern(5 * MIB + 17), other_pattern(MIB)];
    let mut parts = Vec::new();
    for (i, part) in data.iter().enumerate() {
        let out = c
            .upload_part()
            .bucket("bkt")
            .key("k")
            .upload_id(upload_id)
            .part_number(i as i32 + 1)
            .checksum_algorithm(ChecksumAlgorithm::Crc32)
            .body(ByteStream::from(part.clone()))
            .send()
            .await
            .unwrap();
        let completed = CompletedPart::builder()
            .part_number(i as i32 + 1)
            .e_tag(out.e_tag().unwrap())
            .checksum_crc32(out.checksum_crc32().unwrap())
            .build();
        parts.push(completed);
    }
    let done = c
        .complete_multipart_upload()
        .bucket("bkt")
        .key("k")
        .upload_id(upload_id)
        .multipart_upload(CompletedMultipartUpload::builder().set_parts(Some(parts)).build())
        .send()
        .await
        .unwrap();

    let whole = data.concat();
    let expected =
        STANDARD.encode((crc_fast::checksum(crc_fast::CrcAlgorithm::Crc32IsoHdlc, &whole) as u32).to_be_bytes());
    assert_eq!(done.checksum_crc32(), Some(expected.as_str()));
    assert_eq!(done.checksum_type(), Some(&ChecksumType::FullObject));

    // The SDK validates this checksum against the body it receives.
    let got = c.get_object().bucket("bkt").key("k").checksum_mode(ChecksumMode::Enabled).send().await.unwrap();
    assert_eq!(got.checksum_crc32(), Some(expected.as_str()));
    assert_eq!(got.body.collect().await.unwrap().into_bytes().as_ref(), whole.as_slice());
}

#[tokio::test]
async fn multipart_composite_checksum() {
    let s = start().await;
    let c = &s.client;
    c.create_bucket().bucket("bkt").send().await.unwrap();
    let created = c
        .create_multipart_upload()
        .bucket("bkt")
        .key("k")
        .checksum_algorithm(ChecksumAlgorithm::Sha256)
        .send()
        .await
        .unwrap();
    assert_eq!(created.checksum_type(), Some(&ChecksumType::Composite));
    let upload_id = created.upload_id().unwrap();

    let mut parts = Vec::new();
    for (n, data) in [(1, pattern(5 * MIB)), (2, pattern(10))] {
        let out = c
            .upload_part()
            .bucket("bkt")
            .key("k")
            .upload_id(upload_id)
            .part_number(n)
            .checksum_algorithm(ChecksumAlgorithm::Sha256)
            .body(ByteStream::from(data))
            .send()
            .await
            .unwrap();
        parts.push((n, out.e_tag().unwrap().to_owned()));
    }
    let done = c
        .complete_multipart_upload()
        .bucket("bkt")
        .key("k")
        .upload_id(upload_id)
        .multipart_upload(completed(&parts))
        .send()
        .await
        .unwrap();
    assert!(done.checksum_sha256().unwrap().ends_with("-2"));

    let got = c.get_object().bucket("bkt").key("k").checksum_mode(ChecksumMode::Enabled).send().await.unwrap();
    assert_eq!(got.checksum_sha256(), done.checksum_sha256());
    assert_eq!(got.checksum_type(), Some(&ChecksumType::Composite));
    assert_eq!(got.body.collect().await.unwrap().into_bytes().len(), 5 * MIB + 10);
}

#[tokio::test]
async fn multipart_errors() {
    let s = start().await;
    let c = &s.client;
    c.create_bucket().bucket("bkt").send().await.unwrap();
    let upload_id =
        c.create_multipart_upload().bucket("bkt").key("k").send().await.unwrap().upload_id().unwrap().to_owned();
    let mut parts = Vec::new();
    for n in [1, 2] {
        let out = c
            .upload_part()
            .bucket("bkt")
            .key("k")
            .upload_id(&upload_id)
            .part_number(n)
            .body(ByteStream::from(pattern(MIB)))
            .send()
            .await
            .unwrap();
        parts.push((n, out.e_tag().unwrap().to_owned()));
    }
    let complete = |parts: Vec<(i32, String)>| {
        c.complete_multipart_upload()
            .bucket("bkt")
            .key("k")
            .upload_id(&upload_id)
            .multipart_upload(completed(&parts))
            .send()
    };
    assert_eq!(complete(parts.clone()).await.unwrap_err().code(), Some("EntityTooSmall"));
    assert_eq!(complete(vec![(1, "\"0000\"".into())]).await.unwrap_err().code(), Some("InvalidPart"));
    assert_eq!(complete(vec![parts[1].clone(), parts[0].clone()]).await.unwrap_err().code(), Some("InvalidPartOrder"));
    assert_eq!(complete(vec![]).await.unwrap_err().code(), Some("MalformedXML"));

    c.abort_multipart_upload().bucket("bkt").key("k").upload_id(&upload_id).send().await.unwrap();
    let err = c
        .upload_part()
        .bucket("bkt")
        .key("k")
        .upload_id(&upload_id)
        .part_number(3)
        .body(ByteStream::from_static(b"late"))
        .send()
        .await
        .unwrap_err();
    assert_eq!(err.code(), Some("NoSuchUpload"));
    let err = c.list_parts().bucket("bkt").key("k").upload_id("not-an-id").send().await.unwrap_err();
    assert_eq!(err.code(), Some("NoSuchUpload"));
}

#[tokio::test]
async fn retried_completion_succeeds() {
    let s = start().await;
    let c = &s.client;
    c.create_bucket().bucket("bkt").send().await.unwrap();
    let upload_id =
        c.create_multipart_upload().bucket("bkt").key("k").send().await.unwrap().upload_id().unwrap().to_owned();
    let part = c
        .upload_part()
        .bucket("bkt")
        .key("k")
        .upload_id(&upload_id)
        .part_number(1)
        .body(ByteStream::from_static(b"only part"))
        .send()
        .await
        .unwrap();
    let complete = || {
        c.complete_multipart_upload()
            .bucket("bkt")
            .key("k")
            .upload_id(&upload_id)
            .multipart_upload(completed(&[(1, part.e_tag().unwrap().to_owned())]))
            .send()
    };
    let first = complete().await.unwrap();
    // The response was lost and the client retries: same answer.
    let again = complete().await.unwrap();
    assert_eq!(first.e_tag(), again.e_tag());

    // Once the key holds another object, the upload is gone for good.
    c.put_object().bucket("bkt").key("k").body(ByteStream::from_static(b"newer")).send().await.unwrap();
    assert_eq!(complete().await.unwrap_err().code(), Some("NoSuchUpload"));
}

#[tokio::test]
async fn copy_object() {
    let s = start().await;
    let c = &s.client;
    c.create_bucket().bucket("src-bkt").send().await.unwrap();
    c.create_bucket().bucket("dst-bkt").send().await.unwrap();
    let data = pattern(3 * MIB);
    c.put_object()
        .bucket("src-bkt")
        .key("dir/a b+c.bin")
        .content_type("application/x-a")
        .metadata("origin", "camera")
        .body(ByteStream::from(data.clone()))
        .send()
        .await
        .unwrap();
    // The copy source is URL-encoded by the caller.
    let source = "src-bkt/dir/a%20b%2Bc.bin";

    let copied = c.copy_object().bucket("dst-bkt").key("copy.bin").copy_source(source).send().await.unwrap();
    let result = copied.copy_object_result().unwrap();
    assert_eq!(result.e_tag(), Some(format!("\"{}\"", md5_hex(&data)).as_str()));
    let got = c.get_object().bucket("dst-bkt").key("copy.bin").send().await.unwrap();
    assert_eq!(got.content_type(), Some("application/x-a"));
    assert_eq!(got.metadata().unwrap().get("origin").map(String::as_str), Some("camera"));
    assert_eq!(got.body.collect().await.unwrap().into_bytes().as_ref(), data.as_slice());

    c.copy_object()
        .bucket("dst-bkt")
        .key("replaced.bin")
        .copy_source(source)
        .metadata_directive(MetadataDirective::Replace)
        .content_type("text/plain")
        .metadata("origin", "edited")
        .send()
        .await
        .unwrap();
    let head = c.head_object().bucket("dst-bkt").key("replaced.bin").send().await.unwrap();
    assert_eq!(head.content_type(), Some("text/plain"));
    assert_eq!(head.metadata().unwrap().get("origin").map(String::as_str), Some("edited"));

    // Copying an object onto itself is only allowed when something changes.
    let err =
        c.copy_object().bucket("dst-bkt").key("copy.bin").copy_source("dst-bkt/copy.bin").send().await.unwrap_err();
    assert_eq!(err.code(), Some("InvalidRequest"));
    c.copy_object()
        .bucket("dst-bkt")
        .key("copy.bin")
        .copy_source("dst-bkt/copy.bin")
        .metadata_directive(MetadataDirective::Replace)
        .content_type("image/png")
        .send()
        .await
        .unwrap();
    assert_eq!(get_bytes(c, "dst-bkt", "copy.bin").await, data);

    let err = c
        .copy_object()
        .bucket("dst-bkt")
        .key("x")
        .copy_source(source)
        .copy_source_if_match("\"nope\"")
        .send()
        .await
        .unwrap_err();
    assert_eq!(err.code(), Some("PreconditionFailed"));
    let err = c.copy_object().bucket("dst-bkt").key("x").copy_source("src-bkt/missing").send().await.unwrap_err();
    assert_eq!(err.code(), Some("NoSuchKey"));

    // Deleting the source leaves the copy intact.
    c.delete_object().bucket("src-bkt").key("dir/a b+c.bin").send().await.unwrap();
    assert_eq!(get_bytes(c, "dst-bkt", "replaced.bin").await, data);
}

#[tokio::test]
async fn upload_part_copy_assembles_ranges() {
    let s = start().await;
    let c = &s.client;
    c.create_bucket().bucket("bkt").send().await.unwrap();
    let source = [pattern(6 * MIB), other_pattern(MIB)].concat();
    c.put_object().bucket("bkt").key("source").body(ByteStream::from(source.clone())).send().await.unwrap();
    c.put_object().bucket("bkt").key("tail").body(ByteStream::from_static(b"tail")).send().await.unwrap();

    let upload_id =
        c.create_multipart_upload().bucket("bkt").key("joined").send().await.unwrap().upload_id().unwrap().to_owned();
    let first = c
        .upload_part_copy()
        .bucket("bkt")
        .key("joined")
        .upload_id(&upload_id)
        .part_number(1)
        .copy_source("bkt/source")
        .copy_source_range(format!("bytes={}-{}", MIB, 7 * MIB - 1))
        .send()
        .await
        .unwrap();
    let first_etag = first.copy_part_result().unwrap().e_tag().unwrap().to_owned();
    assert_eq!(first_etag, format!("\"{}\"", md5_hex(&source[MIB..])));
    let second = c
        .upload_part_copy()
        .bucket("bkt")
        .key("joined")
        .upload_id(&upload_id)
        .part_number(2)
        .copy_source("bkt/tail")
        .send()
        .await
        .unwrap();
    let second_etag = second.copy_part_result().unwrap().e_tag().unwrap().to_owned();

    let err = c
        .upload_part_copy()
        .bucket("bkt")
        .key("joined")
        .upload_id(&upload_id)
        .part_number(3)
        .copy_source("bkt/tail")
        .copy_source_range("bytes=0-100")
        .send()
        .await
        .unwrap_err();
    assert_eq!(err.code(), Some("InvalidRange"));

    c.complete_multipart_upload()
        .bucket("bkt")
        .key("joined")
        .upload_id(&upload_id)
        .multipart_upload(completed(&[(1, first_etag), (2, second_etag)]))
        .send()
        .await
        .unwrap();
    assert_eq!(get_bytes(c, "bkt", "joined").await, [&source[MIB..], b"tail"].concat());
}

// ---- access keys ----

const READ: BucketGrant = BucketGrant { read: true, write: false, owner: false };
const WRITE: BucketGrant = BucketGrant { read: false, write: true, owner: false };
const READ_WRITE: BucketGrant = BucketGrant { read: true, write: true, owner: false };

/// Creates an access key and a client that signs with it.
async fn key_client(
    s: &TestServer,
    grants: &[(&str, BucketGrant)],
    configure: impl FnOnce(&mut NewKey),
) -> (Client, String) {
    let mut new = NewKey {
        name: "test app".into(),
        buckets: grants.iter().map(|(b, g)| (b.to_string(), *g)).collect(),
        ..Default::default()
    };
    configure(&mut new);
    let key = s.engine.create_key(new).await.unwrap();
    (client_for(s.addr, &key.access_key_id, &key.secret_key), key.access_key_id)
}

async fn put(c: &Client, bucket: &str, key: &str) -> Result<(), String> {
    c.put_object().bucket(bucket).key(key).body(ByteStream::from_static(b"data")).send().await.map(|_| ()).map_err(code)
}

async fn get(c: &Client, bucket: &str, key: &str) -> Result<(), String> {
    c.get_object().bucket(bucket).key(key).send().await.map(|_| ()).map_err(code)
}

fn code<E: ProvideErrorMetadata>(e: E) -> String {
    e.code().unwrap_or("<none>").to_owned()
}

fn denied() -> Result<(), String> {
    Err("AccessDenied".into())
}

#[tokio::test]
async fn keys_only_reach_their_buckets() {
    let s = start().await;
    for bucket in ["app", "other"] {
        s.client.create_bucket().bucket(bucket).send().await.unwrap();
        put(&s.client, bucket, "x").await.unwrap();
    }
    let (app, _) = key_client(&s, &[("app", READ_WRITE)], |_| {}).await;

    assert_eq!(put(&app, "app", "k").await, Ok(()));
    assert_eq!(get(&app, "app", "k").await, Ok(()));
    app.list_objects_v2().bucket("app").send().await.unwrap();
    app.copy_object().bucket("app").key("copy").copy_source("app/k").send().await.unwrap();

    assert_eq!(get(&app, "other", "x").await, denied());
    assert_eq!(put(&app, "other", "k").await, denied());
    // Like S3, a missing bucket is reported as missing, not as forbidden.
    assert_eq!(get(&app, "missing", "x").await, Err("NoSuchBucket".into()));
    let err = app.copy_object().bucket("app").key("c").copy_source("missing/x").send().await.unwrap_err();
    assert_eq!(code(err), "NoSuchBucket");
    assert_eq!(code(app.list_objects_v2().bucket("other").send().await.unwrap_err()), "AccessDenied");
    assert!(app.head_bucket().bucket("other").send().await.is_err());
    let err = app.copy_object().bucket("app").key("stolen").copy_source("other/x").send().await.unwrap_err();
    assert_eq!(code(err), "AccessDenied");

    let names: Vec<_> =
        app.list_buckets().send().await.unwrap().buckets().iter().filter_map(|b| b.name()).map(str::to_owned).collect();
    assert_eq!(names, ["app"]);
    assert_eq!(code(app.delete_bucket().bucket("app").send().await.unwrap_err()), "AccessDenied");
    assert_eq!(code(app.create_bucket().bucket("new").send().await.unwrap_err()), "AccessDenied");
    // "Create if missing" at startup gets the answer apps already handle.
    assert_eq!(code(app.create_bucket().bucket("app").send().await.unwrap_err()), "BucketAlreadyOwnedByYou");
}

#[tokio::test]
async fn read_only_and_write_only_keys() {
    let s = start().await;
    s.client.create_bucket().bucket("bkt").send().await.unwrap();
    put(&s.client, "bkt", "existing").await.unwrap();
    let (reader, _) = key_client(&s, &[("bkt", READ)], |_| {}).await;
    let (writer, _) = key_client(&s, &[("bkt", WRITE)], |_| {}).await;

    assert_eq!(get(&reader, "bkt", "existing").await, Ok(()));
    reader.list_objects_v2().bucket("bkt").send().await.unwrap();
    assert_eq!(put(&reader, "bkt", "k").await, denied());
    assert_eq!(code(reader.delete_object().bucket("bkt").key("existing").send().await.unwrap_err()), "AccessDenied");
    let err = reader.create_multipart_upload().bucket("bkt").key("k").send().await.unwrap_err();
    assert_eq!(code(err), "AccessDenied");

    assert_eq!(put(&writer, "bkt", "upload").await, Ok(()));
    assert_eq!(get(&writer, "bkt", "existing").await, denied());
    assert_eq!(code(writer.list_objects_v2().bucket("bkt").send().await.unwrap_err()), "AccessDenied");
    // Uploaders can run multipart uploads end to end, including ListParts.
    let upload_id =
        writer.create_multipart_upload().bucket("bkt").key("mp").send().await.unwrap().upload_id().unwrap().to_owned();
    let part = writer
        .upload_part()
        .bucket("bkt")
        .key("mp")
        .upload_id(&upload_id)
        .part_number(1)
        .body(ByteStream::from_static(b"part"))
        .send()
        .await
        .unwrap();
    writer.list_parts().bucket("bkt").key("mp").upload_id(&upload_id).send().await.unwrap();
    writer
        .complete_multipart_upload()
        .bucket("bkt")
        .key("mp")
        .upload_id(&upload_id)
        .multipart_upload(completed(&[(1, part.e_tag().unwrap().to_owned())]))
        .send()
        .await
        .unwrap();
    assert_eq!(get_bytes(&s.client, "bkt", "mp").await, b"part");
}

#[tokio::test]
async fn keys_that_create_buckets_own_them() {
    let s = start().await;
    s.client.create_bucket().bucket("taken").send().await.unwrap();
    let (creator, _) = key_client(&s, &[], |k| k.can_create_buckets = true).await;

    creator.create_bucket().bucket("mine").send().await.unwrap();
    assert_eq!(put(&creator, "mine", "k").await, Ok(()));
    assert_eq!(get(&creator, "mine", "k").await, Ok(()));
    let err = creator.create_bucket().bucket("taken").send().await.unwrap_err();
    assert_eq!(code(err), "BucketAlreadyExists", "someone else's bucket");
    assert_eq!(get(&creator, "taken", "k").await, denied());

    creator.delete_object().bucket("mine").key("k").send().await.unwrap();
    creator.delete_bucket().bucket("mine").send().await.unwrap();
}

#[tokio::test]
async fn disabled_and_deleted_keys_are_rejected() {
    let s = start().await;
    s.client.create_bucket().bucket("bkt").send().await.unwrap();
    let (app, id) = key_client(&s, &[("bkt", READ_WRITE)], |_| {}).await;
    app.list_buckets().send().await.unwrap();

    s.engine.update_key(&id, KeyUpdate { disabled: Some(true), ..Default::default() }).await.unwrap();
    assert_eq!(code(app.list_buckets().send().await.unwrap_err()), "InvalidAccessKeyId");
    s.engine.update_key(&id, KeyUpdate { disabled: Some(false), ..Default::default() }).await.unwrap();
    app.list_buckets().send().await.unwrap();

    s.engine.delete_key(&id).await.unwrap();
    assert_eq!(code(app.list_buckets().send().await.unwrap_err()), "InvalidAccessKeyId");
}

#[tokio::test]
async fn admin_keys_have_full_access() {
    let s = start().await;
    s.client.create_bucket().bucket("root-made").send().await.unwrap();
    let (admin, _) = key_client(&s, &[], |k| k.admin = true).await;
    admin.create_bucket().bucket("admin-made").send().await.unwrap();
    assert_eq!(put(&admin, "root-made", "k").await, Ok(()));
    let names: Vec<_> = admin
        .list_buckets()
        .send()
        .await
        .unwrap()
        .buckets()
        .iter()
        .filter_map(|b| b.name())
        .map(str::to_owned)
        .collect();
    assert_eq!(names, ["admin-made", "root-made"]);
    admin.delete_bucket().bucket("admin-made").send().await.unwrap();
}

#[tokio::test]
async fn bucket_cors_lets_browsers_upload_with_presigned_urls() {
    use aws_sdk_s3::types::{CorsConfiguration, CorsRule};

    aether_backend::http::tls::install_crypto_provider();
    let s = start().await;
    s.client.create_bucket().bucket("web").send().await.unwrap();
    let err = s.client.get_bucket_cors().bucket("web").send().await.unwrap_err();
    assert_eq!(code(err), "NoSuchCORSConfiguration");

    let http = reqwest::Client::new();
    let url = |path: &str| format!("http://{}{path}", s.addr);
    let preflight = |origin: &'static str, method: &'static str, headers: &'static str| {
        let req = http
            .request(reqwest::Method::OPTIONS, url("/web/upload.txt"))
            .header("Origin", origin)
            .header("Access-Control-Request-Method", method);
        let req = if headers.is_empty() { req } else { req.header("Access-Control-Request-Headers", headers) };
        req.send()
    };
    // No rules yet: browsers are refused.
    assert_eq!(preflight("https://app.example.com", "PUT", "").await.unwrap().status(), 403);

    let rule = |origins: &[&str], methods: &[&str]| {
        let mut rule = CorsRule::builder().allowed_headers("Content-Type").expose_headers("ETag").max_age_seconds(600);
        for o in origins {
            rule = rule.allowed_origins(*o);
        }
        for m in methods {
            rule = rule.allowed_methods(*m);
        }
        rule.build().unwrap()
    };
    let config = CorsConfiguration::builder()
        .cors_rules(rule(&["https://app.example.com"], &["GET", "PUT"]))
        .cors_rules(rule(&["*"], &["GET"]))
        .build()
        .unwrap();
    s.client.put_bucket_cors().bucket("web").cors_configuration(config).send().await.unwrap();
    let stored = s.client.get_bucket_cors().bucket("web").send().await.unwrap();
    assert_eq!(stored.cors_rules().len(), 2);
    assert_eq!(stored.cors_rules()[0].allowed_methods(), ["GET", "PUT"]);
    assert_eq!(stored.cors_rules()[0].allowed_headers(), ["content-type"]);

    // The app's origin may upload with Content-Type...
    let res = preflight("https://app.example.com", "PUT", "content-type").await.unwrap();
    assert_eq!(res.status(), 200);
    let h = res.headers();
    assert_eq!(h["access-control-allow-origin"], "https://app.example.com");
    assert_eq!(h["access-control-allow-methods"], "GET, PUT");
    assert_eq!(h["access-control-allow-headers"], "content-type");
    assert_eq!(h["access-control-max-age"], "600");
    assert_eq!(h["access-control-allow-credentials"], "true");
    // ...but not send headers the rule does not list, and others may only read.
    let res = preflight("https://app.example.com", "PUT", "x-custom").await.unwrap();
    assert_eq!(res.status(), 403);
    assert_eq!(preflight("https://evil.example.net", "PUT", "").await.unwrap().status(), 403);
    let res = preflight("https://evil.example.net", "GET", "").await.unwrap();
    assert_eq!(res.headers()["access-control-allow-origin"], "*");

    // The browser then sends the presigned PUT; the response carries the CORS
    // headers so the page can read it, including the exposed ETag.
    let presigned = s
        .client
        .put_object()
        .bucket("web")
        .key("upload.txt")
        .presigned(PresigningConfig::expires_in(Duration::from_secs(300)).unwrap())
        .await
        .unwrap();
    let res = http
        .put(presigned.uri())
        .header("Origin", "https://app.example.com")
        .body("from the browser")
        .send()
        .await
        .unwrap();
    assert_eq!(res.status(), 200);
    assert_eq!(res.headers()["access-control-allow-origin"], "https://app.example.com");
    assert_eq!(res.headers()["access-control-expose-headers"], "ETag");
    assert!(res.headers().get_all("vary").iter().any(|v| v == "Origin"));
    assert_eq!(get_bytes(&s.client, "web", "upload.txt").await, b"from the browser");

    // CORS never replaces the signature: unsigned requests are still refused,
    // with headers that let the page see why.
    let res = http.get(url("/web/upload.txt")).header("Origin", "https://app.example.com").send().await.unwrap();
    assert_eq!(res.status(), 403);
    assert_eq!(res.headers()["access-control-allow-origin"], "https://app.example.com");

    // Only owners change the configuration; DeleteBucketCors removes it.
    let (writer, _) = key_client(&s, &[("web", WRITE)], |_| {}).await;
    let err = writer.delete_bucket_cors().bucket("web").send().await.unwrap_err();
    assert_eq!(code(err), "AccessDenied");
    s.client.delete_bucket_cors().bucket("web").send().await.unwrap();
    assert_eq!(preflight("https://app.example.com", "PUT", "").await.unwrap().status(), 403);

    // Invalid configurations are refused, and missing buckets refuse preflights.
    let bad = CorsConfiguration::builder().cors_rules(rule(&["*"], &["PATCH"])).build().unwrap();
    let err = s.client.put_bucket_cors().bucket("web").cors_configuration(bad).send().await.unwrap_err();
    assert_eq!(code(err), "InvalidRequest");
    let res = http
        .request(reqwest::Method::OPTIONS, url("/missing/k"))
        .header("Origin", "https://app.example.com")
        .header("Access-Control-Request-Method", "GET")
        .send()
        .await
        .unwrap();
    assert_eq!(res.status(), 403);
    let res = http.request(reqwest::Method::OPTIONS, url("/web/k")).send().await.unwrap();
    assert_eq!(res.status(), 400);
}

#[tokio::test]
async fn virtual_hosted_style_requests_address_buckets() {
    aether_backend::http::tls::install_crypto_provider();
    let s = start_with(|c| c.s3.domains = vec!["s3.test".into()]).await;
    let port = s.addr.port();
    // The SDK addresses buckets as subdomains; a resolver override sends them here.
    let vhost = aws_sdk_s3::Config::builder()
        .behavior_version(BehaviorVersion::latest())
        .region(Region::new("us-east-1"))
        .endpoint_url(format!("http://s3.test:{port}"))
        .credentials_provider(Credentials::new(ACCESS_KEY, SECRET_KEY, None, None, "test"))
        .build();
    let vhost = Client::from_conf(vhost);
    let http = reqwest::Client::builder()
        .resolve("s3.test", s.addr)
        .resolve("bkt.s3.test", s.addr)
        .resolve("other.s3.test", s.addr)
        .build()
        .unwrap();
    let presign = || PresigningConfig::expires_in(Duration::from_secs(300)).unwrap();

    s.client.create_bucket().bucket("bkt").send().await.unwrap();
    let put = vhost.put_object().bucket("bkt").key("a/b.txt").presigned(presign()).await.unwrap();
    assert!(put.uri().starts_with(&format!("http://bkt.s3.test:{port}/a/b.txt")), "{}", put.uri());
    let res = http.put(put.uri()).body("virtual").send().await.unwrap();
    assert_eq!(res.status(), 200, "{}", res.text().await.unwrap());

    // The same object is reachable path-style, by IP and through the base domain.
    assert_eq!(get_bytes(&s.client, "bkt", "a/b.txt").await, b"virtual");
    let get = vhost.get_object().bucket("bkt").key("a/b.txt").presigned(presign()).await.unwrap();
    assert_eq!(http.get(get.uri()).send().await.unwrap().text().await.unwrap(), "virtual");
    // Path-style through the base domain itself.
    let base_get = Client::from_conf(
        aws_sdk_s3::Config::builder()
            .behavior_version(BehaviorVersion::latest())
            .region(Region::new("us-east-1"))
            .endpoint_url(format!("http://s3.test:{port}"))
            .credentials_provider(Credentials::new(ACCESS_KEY, SECRET_KEY, None, None, "test"))
            .force_path_style(true)
            .build(),
    )
    .get_object()
    .bucket("bkt")
    .key("a/b.txt")
    .presigned(presign())
    .await
    .unwrap();
    assert_eq!(http.get(base_get.uri()).send().await.unwrap().text().await.unwrap(), "virtual");

    // A missing bucket in the host is NoSuchBucket, as in S3.
    let missing = vhost.get_object().bucket("other").key("k").presigned(presign()).await.unwrap();
    let res = http.get(missing.uri()).send().await.unwrap();
    assert_eq!(res.status(), 404);
    assert!(res.text().await.unwrap().contains("NoSuchBucket"));

    // CORS finds the bucket in the host too.
    use aws_sdk_s3::types::{CorsConfiguration, CorsRule};
    let rule = CorsRule::builder().allowed_origins("https://app.example.com").allowed_methods("PUT").build().unwrap();
    let config = CorsConfiguration::builder().cors_rules(rule).build().unwrap();
    s.client.put_bucket_cors().bucket("bkt").cors_configuration(config).send().await.unwrap();
    let res = http
        .request(reqwest::Method::OPTIONS, format!("http://bkt.s3.test:{port}/a/b.txt"))
        .header("Origin", "https://app.example.com")
        .header("Access-Control-Request-Method", "PUT")
        .send()
        .await
        .unwrap();
    assert_eq!(res.status(), 200);
    assert_eq!(res.headers()["access-control-allow-origin"], "https://app.example.com");
}

#[tokio::test]
async fn multipart_uploads_stop_at_the_first_part_past_the_quota() {
    let s = start().await;
    s.client.create_bucket().bucket("capped").send().await.unwrap();
    let quota = aether_backend::meta::Quota { max_bytes: Some(8 * 1024 * 1024), max_objects: None };
    s.engine.set_bucket_quota("capped", quota).await.unwrap();
    let upload = s.client.create_multipart_upload().bucket("capped").key("big").send().await.unwrap();
    let id = upload.upload_id().unwrap();
    let part = |n: i32| {
        s.client
            .upload_part()
            .bucket("capped")
            .key("big")
            .upload_id(id)
            .part_number(n)
            .body(ByteStream::from(pattern(5 * 1024 * 1024)))
            .send()
    };
    part(1).await.unwrap();
    let err = part(2).await.unwrap_err();
    assert_eq!(code(err), "QuotaExceeded");
    let parts = s.client.list_parts().bucket("capped").key("big").upload_id(id).send().await.unwrap();
    assert_eq!(parts.parts().len(), 1);
}

#[tokio::test]
async fn object_tags_are_stored_copied_and_permission_checked() {
    use aws_sdk_s3::types::{Tag, Tagging, TaggingDirective};
    let s = start().await;
    s.client.create_bucket().bucket("bkt").send().await.unwrap();
    let tags_of = |key: &'static str| {
        let client = s.client.clone();
        async move {
            let out = client.get_object_tagging().bucket("bkt").key(key).send().await.unwrap();
            let mut tags: Vec<_> = out.tag_set().iter().map(|t| format!("{}={}", t.key(), t.value())).collect();
            tags.sort();
            tags
        }
    };

    // On PUT, from the URL-encoded header.
    s.client
        .put_object()
        .bucket("bkt")
        .key("a")
        .tagging("project=aether&team=storage%20ops")
        .body(ByteStream::from_static(b"a"))
        .send()
        .await
        .unwrap();
    assert_eq!(tags_of("a").await, ["project=aether", "team=storage ops"]);
    let got = s.client.get_object().bucket("bkt").key("a").send().await.unwrap();
    assert_eq!(got.tag_count(), Some(2));

    // Replaced and removed with the tagging calls.
    let tagging =
        Tagging::builder().tag_set(Tag::builder().key("stage").value("final").build().unwrap()).build().unwrap();
    s.client.put_object_tagging().bucket("bkt").key("a").tagging(tagging).send().await.unwrap();
    assert_eq!(tags_of("a").await, ["stage=final"]);

    // Copies keep the source's tags unless told to replace them.
    let copy = |dst: &'static str| s.client.copy_object().bucket("bkt").key(dst).copy_source("bkt/a");
    copy("kept").send().await.unwrap();
    assert_eq!(tags_of("kept").await, ["stage=final"]);
    copy("replaced").tagging_directive(TaggingDirective::Replace).tagging("x=1").send().await.unwrap();
    assert_eq!(tags_of("replaced").await, ["x=1"]);

    // Multipart uploads take their tags at creation.
    let up = s.client.create_multipart_upload().bucket("bkt").key("mp").tagging("kind=video").send().await.unwrap();
    let id = up.upload_id().unwrap();
    let part = s.client.upload_part().bucket("bkt").key("mp").upload_id(id).part_number(1);
    let part = part.body(ByteStream::from_static(b"data")).send().await.unwrap();
    s.client
        .complete_multipart_upload()
        .bucket("bkt")
        .key("mp")
        .upload_id(id)
        .multipart_upload(completed(&[(1, part.e_tag().unwrap().to_owned())]))
        .send()
        .await
        .unwrap();
    assert_eq!(tags_of("mp").await, ["kind=video"]);

    s.client.delete_object_tagging().bucket("bkt").key("a").send().await.unwrap();
    assert!(tags_of("a").await.is_empty());
    assert_eq!(s.client.get_object().bucket("bkt").key("a").send().await.unwrap().tag_count(), None);

    // Limits and missing objects.
    let eleven = (0..11).map(|i| format!("k{i}=v")).collect::<Vec<_>>().join("&");
    let err = s.client.put_object().bucket("bkt").key("b").tagging(eleven).send().await.unwrap_err();
    assert_eq!(code(err), "InvalidTag");
    let err = s.client.get_object_tagging().bucket("bkt").key("missing").send().await.unwrap_err();
    assert_eq!(code(err), "NoSuchKey");

    // Readers read tags, writers change them.
    let (reader, _) = key_client(&s, &[("bkt", READ)], |_| {}).await;
    reader.get_object_tagging().bucket("bkt").key("mp").send().await.unwrap();
    let err = reader.delete_object_tagging().bucket("bkt").key("mp").send().await.unwrap_err();
    assert_eq!(code(err), "AccessDenied");
    let (writer, _) = key_client(&s, &[("bkt", WRITE)], |_| {}).await;
    writer.delete_object_tagging().bucket("bkt").key("mp").send().await.unwrap();
}

#[tokio::test]
async fn sse_s3_encrypts_objects_when_a_master_key_is_set() {
    use aws_sdk_s3::types::ServerSideEncryption;
    let key = STANDARD.encode([42u8; 32]);
    let s = start_with(|c| c.encryption.master_key = Some(Secret::from(key.as_str()))).await;
    s.client.create_bucket().bucket("bkt").send().await.unwrap();

    // Every object is encrypted, whether or not the client asks.
    let data = pattern(3 * 1024 * 1024 + 17);
    let put = s.client.put_object().bucket("bkt").key("big").body(ByteStream::from(data.clone())).send().await.unwrap();
    assert_eq!(put.server_side_encryption(), Some(&ServerSideEncryption::Aes256));
    let asked = s
        .client
        .put_object()
        .bucket("bkt")
        .key("small")
        .server_side_encryption(ServerSideEncryption::Aes256)
        .body(ByteStream::from_static(b"tiny"))
        .send()
        .await
        .unwrap();
    assert_eq!(asked.server_side_encryption(), Some(&ServerSideEncryption::Aes256));
    let got = s.client.get_object().bucket("bkt").key("big").range("bytes=1048570-2097160").send().await.unwrap();
    assert_eq!(got.server_side_encryption(), Some(&ServerSideEncryption::Aes256));
    assert_eq!(got.body.collect().await.unwrap().into_bytes(), data[1_048_570..=2_097_160]);
    assert_eq!(get_bytes(&s.client, "bkt", "small").await, b"tiny");
    let head = s.client.head_object().bucket("bkt").key("big").send().await.unwrap();
    assert_eq!(head.server_side_encryption(), Some(&ServerSideEncryption::Aes256));

    let copy = s.client.copy_object().bucket("bkt").key("copy").copy_source("bkt/big").send().await.unwrap();
    assert_eq!(copy.server_side_encryption(), Some(&ServerSideEncryption::Aes256));
    assert_eq!(get_bytes(&s.client, "bkt", "copy").await, data);

    // Keys created now have sealed secrets, and still sign requests.
    let (app, _) = key_client(&s, &[("bkt", READ_WRITE)], |_| {}).await;
    app.put_object().bucket("bkt").key("by-app").body(ByteStream::from_static(b"ok")).send().await.unwrap();
    assert_eq!(get_bytes(&app, "bkt", "by-app").await, b"ok");

    let enc = s.client.get_bucket_encryption().bucket("bkt").send().await.unwrap();
    let rule = &enc.server_side_encryption_configuration().unwrap().rules()[0];
    assert_eq!(rule.apply_server_side_encryption_by_default().unwrap().sse_algorithm(), &ServerSideEncryption::Aes256);

    // SSE-KMS and customer keys are refused rather than ignored.
    let err = s
        .client
        .put_object()
        .bucket("bkt")
        .key("kms")
        .server_side_encryption(ServerSideEncryption::AwsKms)
        .body(ByteStream::from_static(b"x"))
        .send()
        .await
        .unwrap_err();
    assert_eq!(code(err), "NotImplemented");

    // Without a master key, asking for SSE-S3 is refused and nothing is encrypted.
    let plain = start().await;
    plain.client.create_bucket().bucket("bkt").send().await.unwrap();
    let err = plain
        .client
        .put_object()
        .bucket("bkt")
        .key("k")
        .server_side_encryption(ServerSideEncryption::Aes256)
        .body(ByteStream::from_static(b"x"))
        .send()
        .await
        .unwrap_err();
    assert_eq!(code(err), "NotImplemented");
    let put =
        plain.client.put_object().bucket("bkt").key("k").body(ByteStream::from_static(b"x")).send().await.unwrap();
    assert_eq!(put.server_side_encryption(), None);
    let err = plain.client.get_bucket_encryption().bucket("bkt").send().await.unwrap_err();
    assert_eq!(code(err), "ServerSideEncryptionConfigurationNotFoundError");
}

#[tokio::test]
async fn versioning_through_the_s3_api() {
    use aws_sdk_s3::types::{BucketVersioningStatus, VersioningConfiguration};
    let s = start().await;
    let c = &s.client;
    c.create_bucket().bucket("bkt").send().await.unwrap();
    assert_eq!(c.get_bucket_versioning().bucket("bkt").send().await.unwrap().status(), None);
    let enable = VersioningConfiguration::builder().status(BucketVersioningStatus::Enabled).build();
    c.put_bucket_versioning().bucket("bkt").versioning_configuration(enable).send().await.unwrap();
    let status = c.get_bucket_versioning().bucket("bkt").send().await.unwrap();
    assert_eq!(status.status(), Some(&BucketVersioningStatus::Enabled));

    let put = |body: &'static str| {
        c.put_object().bucket("bkt").key("doc").body(ByteStream::from_static(body.as_bytes())).send()
    };
    let v1 = put("first").await.unwrap().version_id().unwrap().to_owned();
    let v2 = put("second").await.unwrap().version_id().unwrap().to_owned();
    assert_ne!(v1, v2);
    let get_version = |v: &str| c.get_object().bucket("bkt").key("doc").version_id(v).send();
    let first = get_version(&v1).await.unwrap();
    assert_eq!(first.version_id(), Some(v1.as_str()));
    assert_eq!(first.body.collect().await.unwrap().into_bytes(), "first".as_bytes());
    let head = c.head_object().bucket("bkt").key("doc").send().await.unwrap();
    assert_eq!(head.version_id(), Some(v2.as_str()));

    // A plain delete adds a marker; reading the key then is a 404 with the marker headers.
    let del = c.delete_object().bucket("bkt").key("doc").send().await.unwrap();
    assert_eq!(del.delete_marker(), Some(true));
    let marker = del.version_id().unwrap().to_owned();
    let err = c.get_object().bucket("bkt").key("doc").send().await.unwrap_err();
    let raw = err.raw_response().unwrap();
    assert_eq!(raw.status().as_u16(), 404);
    assert_eq!(raw.headers().get("x-amz-delete-marker"), Some("true"));
    // Asking for the marker itself is a 405.
    let err = c.head_object().bucket("bkt").key("doc").version_id(&marker).send().await.unwrap_err();
    assert_eq!(err.raw_response().unwrap().status().as_u16(), 405);

    // Copy an older version to a new key.
    let copy = c
        .copy_object()
        .bucket("bkt")
        .key("restored")
        .copy_source(format!("bkt/doc?versionId={v1}"))
        .send()
        .await
        .unwrap();
    assert_eq!(copy.copy_source_version_id(), Some(v1.as_str()));
    assert_eq!(get_bytes(c, "bkt", "restored").await, b"first");

    // Listing versions, a page at a time.
    let mut seen = Vec::new();
    let (mut key_marker, mut version_marker) = (None, None);
    loop {
        let page = c
            .list_object_versions()
            .bucket("bkt")
            .max_keys(1)
            .set_key_marker(key_marker.clone())
            .set_version_id_marker(version_marker.clone())
            .send()
            .await
            .unwrap();
        for v in page.versions() {
            seen.push((v.key().unwrap().to_owned(), v.version_id().unwrap().to_owned(), v.is_latest().unwrap()));
        }
        for m in page.delete_markers() {
            seen.push((
                m.key().unwrap().to_owned(),
                format!("marker {}", m.version_id().unwrap()),
                m.is_latest().unwrap(),
            ));
        }
        if !page.is_truncated().unwrap() {
            break;
        }
        key_marker = page.next_key_marker().map(str::to_owned);
        version_marker = page.next_version_id_marker().map(str::to_owned);
    }
    let restored = seen.iter().find(|(k, _, _)| k == "restored").unwrap().1.clone();
    assert_eq!(
        seen,
        [
            ("doc".into(), format!("marker {marker}"), true),
            ("doc".into(), v2.clone(), false),
            ("doc".into(), v1.clone(), false),
            ("restored".into(), restored, true),
        ]
    );

    // Batch delete of specific versions, including the marker: the key comes back as v1.
    let objects = [&marker, &v2]
        .iter()
        .map(|v| ObjectIdentifier::builder().key("doc").version_id(*v).build().unwrap())
        .collect::<Vec<_>>();
    let out = c
        .delete_objects()
        .bucket("bkt")
        .delete(Delete::builder().set_objects(Some(objects)).build().unwrap())
        .send()
        .await
        .unwrap();
    assert_eq!(out.deleted().len(), 2);
    assert!(out.deleted().iter().any(|d| d.delete_marker() == Some(true) && d.version_id() == Some(marker.as_str())));
    assert_eq!(get_bytes(c, "bkt", "doc").await, b"first");

    let err = c.get_object().bucket("bkt").key("doc").version_id("not-a-version").send().await.unwrap_err();
    assert_eq!(code(err), "InvalidArgument");
    let missing = "0".repeat(32);
    let err = get_version(&missing).await.unwrap_err();
    assert_eq!(code(err), "NoSuchVersion");
    // A versioned bucket is only deleted once every version is gone.
    let err = c.delete_bucket().bucket("bkt").send().await.unwrap_err();
    assert_eq!(code(err), "BucketNotEmpty");
}

#[tokio::test]
async fn lifecycle_configuration_through_the_s3_api() {
    use aws_sdk_s3::types::{
        AbortIncompleteMultipartUpload, BucketLifecycleConfiguration, ExpirationStatus, LifecycleExpiration,
        LifecycleRule, LifecycleRuleFilter, NoncurrentVersionExpiration, StorageClass, Transition,
    };
    let s = start().await;
    let c = &s.client;
    c.create_bucket().bucket("bkt").send().await.unwrap();
    let err = c.get_bucket_lifecycle_configuration().bucket("bkt").send().await.unwrap_err();
    assert_eq!(code(err), "NoSuchLifecycleConfiguration");

    let logs = LifecycleRule::builder()
        .id("logs")
        .status(ExpirationStatus::Enabled)
        .filter(LifecycleRuleFilter::builder().prefix("logs/").build())
        .expiration(LifecycleExpiration::builder().days(30).build())
        .noncurrent_version_expiration(
            NoncurrentVersionExpiration::builder().noncurrent_days(7).newer_noncurrent_versions(2).build(),
        )
        .build()
        .unwrap();
    let uploads = LifecycleRule::builder()
        .status(ExpirationStatus::Enabled)
        .filter(LifecycleRuleFilter::builder().prefix("").build())
        .abort_incomplete_multipart_upload(AbortIncompleteMultipartUpload::builder().days_after_initiation(3).build())
        .build()
        .unwrap();
    let config = BucketLifecycleConfiguration::builder().rules(logs).rules(uploads).build().unwrap();
    c.put_bucket_lifecycle_configuration().bucket("bkt").lifecycle_configuration(config).send().await.unwrap();

    let got = c.get_bucket_lifecycle_configuration().bucket("bkt").send().await.unwrap();
    let rules = got.rules();
    assert_eq!(rules.len(), 2);
    assert_eq!(rules[0].id(), Some("logs"));
    assert_eq!(rules[0].expiration().unwrap().days(), Some(30));
    assert_eq!(rules[0].noncurrent_version_expiration().unwrap().newer_noncurrent_versions(), Some(2));
    assert!(rules[1].id().is_some_and(|id| !id.is_empty()), "rules without an ID get one");
    assert_eq!(rules[1].abort_incomplete_multipart_upload().unwrap().days_after_initiation(), Some(3));

    // Invalid rules are refused; transitions need storage classes Aether does not have.
    let bad = |rule: LifecycleRule| {
        c.put_bucket_lifecycle_configuration()
            .bucket("bkt")
            .lifecycle_configuration(BucketLifecycleConfiguration::builder().rules(rule).build().unwrap())
            .send()
    };
    let base = || {
        LifecycleRule::builder()
            .status(ExpirationStatus::Enabled)
            .filter(LifecycleRuleFilter::builder().prefix("").build())
    };
    let err =
        bad(base().expiration(LifecycleExpiration::builder().days(0).build()).build().unwrap()).await.unwrap_err();
    assert_eq!(code(err), "InvalidArgument");
    let err = bad(base().build().unwrap()).await.unwrap_err();
    assert_eq!(code(err), "InvalidRequest");
    let transition = Transition::builder().days(30).storage_class(StorageClass::Glacier.as_str().into()).build();
    let err = bad(base().transitions(transition).build().unwrap()).await.unwrap_err();
    assert_eq!(code(err), "NotImplemented");

    c.delete_bucket_lifecycle().bucket("bkt").send().await.unwrap();
    let err = c.get_bucket_lifecycle_configuration().bucket("bkt").send().await.unwrap_err();
    assert_eq!(code(err), "NoSuchLifecycleConfiguration");
}
