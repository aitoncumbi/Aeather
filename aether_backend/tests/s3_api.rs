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
    let dir = tempfile::tempdir().unwrap();
    let mut config = Config::default();
    config.storage.data_dir = dir.path().to_path_buf();
    config.storage.block_size = 1024 * 1024;
    config.s3.access_key = ACCESS_KEY.into();
    config.s3.secret_key = Secret::from(SECRET_KEY);

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
    assert_eq!(err.code(), Some("InvalidArgument"));

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
