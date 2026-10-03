use std::time::Duration;

use futures::TryStreamExt;

use super::*;
use crate::meta::{BucketGrant, KeyUpdate};

/// Tiny blocks and inline threshold so multi-block paths run on small inputs.
async fn test_engine() -> (Engine, tempfile::TempDir) {
    let dir = tempfile::tempdir().unwrap();
    let config = EngineConfig {
        data_dir: dir.path().to_path_buf(),
        block_size: 1000,
        inline_threshold: 100,
        gc_grace: Duration::ZERO,
        orphan_grace: Duration::ZERO,
        min_part_size: 1000,
        ..EngineConfig::default()
    };
    (Engine::open(config).await.unwrap(), dir)
}

fn body(data: &[u8], chunk: usize) -> impl Stream<Item = Result<Bytes, io::Error>> + Unpin + use<> {
    let chunks: Vec<_> = data.chunks(chunk.max(1)).map(|c| Ok(Bytes::copy_from_slice(c))).collect();
    stream::iter(chunks)
}

fn pattern(len: usize) -> Vec<u8> {
    (0..len).map(|i| (i * 31 % 251) as u8).collect()
}

async fn put(engine: &Engine, bucket: &str, key: &str, data: &[u8]) -> ObjectRecord {
    engine.put_object(bucket, key, body(data, 333), ObjectAttrs::default(), WriteCondition::None).await.unwrap()
}

async fn read_all(engine: &Engine, record: &ObjectRecord, range: Range<u64>) -> Vec<u8> {
    engine
        .read_object(record, range)
        .try_fold(Vec::new(), |mut out, chunk| async move {
            out.extend_from_slice(&chunk);
            Ok(out)
        })
        .await
        .unwrap()
}

async fn block_files(engine: &Engine) -> usize {
    engine.blocks.scan().await.unwrap().len()
}

/// Waits for the async cleanup spawned by a dropped `StagedObject`.
async fn wait_for_block_files(engine: &Engine, expected: usize) {
    for _ in 0..100 {
        if block_files(engine).await == expected {
            return;
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    assert_eq!(block_files(engine).await, expected);
}

#[tokio::test]
async fn bucket_lifecycle() {
    let (engine, _dir) = test_engine().await;
    engine.create_bucket("photos").await.unwrap();
    engine.create_bucket("docs").await.unwrap();
    assert!(matches!(engine.create_bucket("photos").await, Err(Error::BucketAlreadyExists)));
    assert!(matches!(engine.create_bucket("Bad_Name").await, Err(Error::InvalidBucketName(_))));
    assert!(matches!(engine.create_bucket("../escape").await, Err(Error::InvalidBucketName(_))));
    for name in ["foo.-bar", "foo-.bar", "a..b", "-ab", "192.168.5.4"] {
        assert!(matches!(engine.create_bucket(name).await, Err(Error::InvalidBucketName(_))), "{name}");
    }

    let names: Vec<_> = engine.list_buckets().await.unwrap().into_iter().map(|b| b.name).collect();
    assert_eq!(names, ["docs", "photos"]);

    put(&engine, "photos", "cat.jpg", b"meow").await;
    assert!(matches!(engine.delete_bucket("photos").await, Err(Error::BucketNotEmpty)));
    engine.delete_object("photos", "cat.jpg").await.unwrap();
    engine.delete_bucket("photos").await.unwrap();
    assert!(matches!(engine.get_bucket("photos").await, Err(Error::NoSuchBucket)));
    assert!(matches!(engine.delete_bucket("photos").await, Err(Error::NoSuchBucket)));
}

#[tokio::test]
async fn small_objects_are_inline() {
    let (engine, _dir) = test_engine().await;
    engine.create_bucket("bkt").await.unwrap();
    let attrs = ObjectAttrs {
        content_type: Some("text/plain".into()),
        user_metadata: [("owner".to_string(), "ops".to_string())].into(),
        ..Default::default()
    };
    let written =
        engine.put_object("bkt", "hello.txt", body(b"hello", 2), attrs.clone(), WriteCondition::None).await.unwrap();

    assert!(matches!(written.data, ObjectData::Inline(_)));
    assert_eq!(written.etag, "5d41402abc4b2a76b9719d911017c592");
    assert_eq!(block_files(&engine).await, 0);

    let record = engine.get_object("bkt", "hello.txt").await.unwrap();
    assert_eq!(record, written);
    assert_eq!(record.attrs, attrs);
    assert_eq!(read_all(&engine, &record, 0..5).await, b"hello");
    assert_eq!(read_all(&engine, &record, 1..3).await, b"el");

    let empty = put(&engine, "bkt", "empty", b"").await;
    assert_eq!(empty.size, 0);
    assert_eq!(read_all(&engine, &empty, 0..0).await, b"");
}

#[tokio::test]
async fn large_objects_span_blocks() {
    let (engine, _dir) = test_engine().await;
    engine.create_bucket("bkt").await.unwrap();
    let data = pattern(4500);
    let record = put(&engine, "bkt", "big.bin", &data).await;

    let blocks = record.data.blocks();
    assert_eq!(blocks.iter().map(|b| b.len).collect::<Vec<_>>(), [1000, 1000, 1000, 1000, 500]);
    for (i, block) in blocks.iter().enumerate() {
        let start = i * 1000;
        let end = (start + 1000).min(data.len());
        assert_eq!(block.crc32c, crc32c::crc32c(&data[start..end]));
    }
    assert_eq!(block_files(&engine).await, 5);
    assert_eq!(read_all(&engine, &record, 0..4500).await, data);

    for (start, end) in [(0, 1), (999, 1001), (1000, 2000), (1500, 3700), (4499, 4500), (0, 4500), (2000, 2000)] {
        assert_eq!(read_all(&engine, &record, start..end).await, &data[start as usize..end as usize], "{start}..{end}");
    }
}

#[tokio::test]
async fn overwrite_and_delete_release_blocks() {
    let (engine, _dir) = test_engine().await;
    engine.create_bucket("bkt").await.unwrap();
    put(&engine, "bkt", "k", &pattern(2500)).await;
    let newer = put(&engine, "bkt", "k", &pattern(1200)).await;
    assert_eq!(block_files(&engine).await, 5);

    assert_eq!(engine.collect_garbage().await.unwrap(), 3);
    assert_eq!(block_files(&engine).await, 2);
    assert_eq!(engine.meta.block_count().unwrap(), 2);
    assert_eq!(read_all(&engine, &newer, 0..1200).await, pattern(1200));

    // Unversioned: the object is gone, no delete marker.
    let deleted = engine.delete_object("bkt", "k").await.unwrap();
    assert_eq!(deleted, Deleted::default());
    assert!(matches!(engine.get_object("bkt", "k").await, Err(Error::NoSuchKey)));
    assert_eq!(engine.collect_garbage().await.unwrap(), 2);
    assert_eq!(block_files(&engine).await, 0);
    assert_eq!(engine.meta.block_count().unwrap(), 0);

    assert_eq!(engine.delete_object("bkt", "k").await.unwrap(), Deleted::default());
    assert!(matches!(engine.delete_object("nope", "k").await, Err(Error::NoSuchBucket)));
}

#[tokio::test]
async fn batch_delete() {
    let (engine, _dir) = test_engine().await;
    engine.create_bucket("bkt").await.unwrap();
    put(&engine, "bkt", "a", &pattern(1500)).await;
    put(&engine, "bkt", "b", b"small").await;
    put(&engine, "bkt", "c", b"keep").await;

    let keys = vec!["a".to_string(), "b".to_string(), "missing".to_string()];
    engine.delete_objects("bkt", keys).await.unwrap();
    let page = engine.list_objects("bkt", ListQuery { max_keys: 10, ..Default::default() }).await.unwrap();
    assert_eq!(page.objects.iter().map(|o| o.key.as_str()).collect::<Vec<_>>(), ["c"]);
    assert_eq!(engine.collect_garbage().await.unwrap(), 2);
    assert!(matches!(engine.delete_objects("nope", vec![]).await, Err(Error::NoSuchBucket)));
}

#[tokio::test]
async fn gc_waits_for_grace_period() {
    let dir = tempfile::tempdir().unwrap();
    let config = EngineConfig {
        data_dir: dir.path().to_path_buf(),
        block_size: 1000,
        inline_threshold: 100,
        gc_grace: Duration::from_secs(3600),
        ..EngineConfig::default()
    };
    let engine = Engine::open(config).await.unwrap();
    engine.create_bucket("bkt").await.unwrap();
    let record = put(&engine, "bkt", "k", &pattern(1500)).await;
    engine.delete_object("bkt", "k").await.unwrap();

    assert_eq!(engine.collect_garbage().await.unwrap(), 0);
    // A download that started before the delete can still finish.
    assert_eq!(read_all(&engine, &record, 0..1500).await, pattern(1500));
}

#[tokio::test]
async fn conditional_writes() {
    let (engine, _dir) = test_engine().await;
    engine.create_bucket("bkt").await.unwrap();
    let first = put(&engine, "bkt", "k", &pattern(1500)).await;
    let before = block_files(&engine).await;

    // Fails the early check without reading the body.
    let err = engine
        .put_object("bkt", "k", body(&pattern(1500), 500), ObjectAttrs::default(), WriteCondition::IfNoneMatchAny)
        .await
        .unwrap_err();
    assert!(matches!(err, Error::PreconditionFailed));

    // Passes the early check, then loses the race inside the commit: the staged blocks are removed.
    let staged = engine.stage(body(&pattern(1500), 500)).await.unwrap();
    assert_eq!(block_files(&engine).await, before + 2);
    let err = engine
        .commit_put("bkt", "k", staged, ObjectAttrs::default(), WriteCondition::IfMatch("wrong".into()))
        .await
        .unwrap_err();
    assert!(matches!(err, Error::PreconditionFailed));
    wait_for_block_files(&engine, before).await;

    let err = engine
        .put_object(
            "bkt",
            "missing",
            body(b"x", 1),
            ObjectAttrs::default(),
            WriteCondition::IfMatch(first.etag.clone()),
        )
        .await
        .unwrap_err();
    assert!(matches!(err, Error::NoSuchKey));

    let second = engine
        .put_object("bkt", "k", body(b"new", 1), ObjectAttrs::default(), WriteCondition::IfMatch(first.etag.clone()))
        .await
        .unwrap();
    assert_eq!(engine.get_object("bkt", "k").await.unwrap(), second);

    engine
        .put_object("bkt", "fresh", body(b"x", 1), ObjectAttrs::default(), WriteCondition::IfNoneMatchAny)
        .await
        .unwrap();
}

#[tokio::test]
async fn failed_uploads_leave_no_blocks() {
    let (engine, _dir) = test_engine().await;
    engine.create_bucket("bkt").await.unwrap();

    // The client disconnects after 2500 bytes.
    let mut chunks: Vec<Result<Bytes, io::Error>> =
        pattern(2500).chunks(500).map(|c| Ok(Bytes::copy_from_slice(c))).collect();
    chunks.push(Err(io::Error::new(io::ErrorKind::ConnectionReset, "client went away")));
    let err = engine.stage(stream::iter(chunks)).await.unwrap_err();
    assert!(matches!(err, Error::Body(_)));
    wait_for_block_files(&engine, 0).await;

    // Staged, then abandoned (e.g. checksum mismatch).
    let staged = engine.stage(body(&pattern(2500), 500)).await.unwrap();
    assert_eq!(block_files(&engine).await, 3);
    drop(staged);
    wait_for_block_files(&engine, 0).await;

    assert!(matches!(
        engine.put_object("nope", "k", body(b"x", 1), ObjectAttrs::default(), WriteCondition::None).await,
        Err(Error::NoSuchBucket)
    ));
    assert!(matches!(
        engine.put_object("bkt", "", body(b"x", 1), ObjectAttrs::default(), WriteCondition::None).await,
        Err(Error::InvalidArgument(_))
    ));
}

#[tokio::test]
async fn sweeper_removes_only_orphans() {
    let (engine, _dir) = test_engine().await;
    engine.create_bucket("bkt").await.unwrap();
    let record = put(&engine, "bkt", "k", &pattern(1500)).await;

    // A block from an upload that crashed before committing.
    let mut writer = engine.blocks.create(None).await.unwrap();
    writer.write(b"orphan").await.unwrap();
    let orphan = writer.finish().await.unwrap();
    engine.blocks.unstage(&[orphan.id]);

    // A block from an upload still in progress.
    let mut writer = engine.blocks.create(None).await.unwrap();
    writer.write(b"in flight").await.unwrap();
    let in_flight = writer.finish().await.unwrap();

    assert_eq!(engine.sweep_orphans().await.unwrap(), 1);
    assert!(engine.blocks.open_range(&orphan.id, 0, 1).await.is_err());
    assert!(engine.blocks.open_range(&in_flight.id, 0, 1).await.is_ok());
    assert_eq!(read_all(&engine, &record, 0..1500).await, pattern(1500));
}

#[tokio::test]
async fn reopen_keeps_data() {
    let dir = tempfile::tempdir().unwrap();
    let config = EngineConfig {
        data_dir: dir.path().to_path_buf(),
        block_size: 1000,
        inline_threshold: 100,
        ..Default::default()
    };
    {
        let engine = Engine::open(config.clone()).await.unwrap();
        engine.create_bucket("bkt").await.unwrap();
        put(&engine, "bkt", "k", &pattern(2500)).await;
    }
    let engine = Engine::open(config).await.unwrap();
    let record = engine.get_object("bkt", "k").await.unwrap();
    assert_eq!(read_all(&engine, &record, 0..2500).await, pattern(2500));
}

#[tokio::test]
async fn second_process_cannot_open_same_data_dir() {
    let (_engine, dir) = test_engine().await;
    let config = EngineConfig { data_dir: dir.path().to_path_buf(), ..Default::default() };
    assert!(Engine::open(config).await.is_err());
}

const LIST_KEYS: &[&str] = &[
    "a",
    "a/b",
    "a/b/c",
    "a/c",
    "b",
    "b/",
    "b/x",
    "c/d/e",
    "photos/2024/feb.jpg",
    "photos/2024/jan.jpg",
    "photos/2025/x.jpg",
    "photos/readme",
    "z",
    "zz/日本/1",
];

/// Reference model: every matching key in order, rolled up by delimiter.
fn expected_listing(prefix: &str, delimiter: Option<&str>) -> (Vec<String>, Vec<String>) {
    let mut keys = Vec::new();
    let mut prefixes: Vec<String> = Vec::new();
    for key in LIST_KEYS.iter().filter(|k| k.starts_with(prefix)) {
        let rest = &key[prefix.len()..];
        match delimiter.and_then(|d| rest.find(d).map(|i| &key[..prefix.len() + i + d.len()])) {
            Some(common) => {
                if prefixes.last().map(String::as_str) != Some(common) {
                    prefixes.push(common.to_string());
                }
            }
            None => keys.push(key.to_string()),
        }
    }
    (keys, prefixes)
}

#[tokio::test]
async fn listing_matches_reference_model_at_every_page_size() {
    let (engine, _dir) = test_engine().await;
    engine.create_bucket("bkt").await.unwrap();
    engine.create_bucket("other").await.unwrap();
    for key in LIST_KEYS {
        put(&engine, "bkt", key, key.as_bytes()).await;
    }
    // Keys in a neighbouring bucket must never leak into the listing.
    put(&engine, "other", "a/zzz", b"x").await;

    for prefix in ["", "a", "a/", "b/", "photos/", "photos/2024/", "zz/", "missing"] {
        for delimiter in [None, Some("/"), Some("/2")] {
            let (want_keys, want_prefixes) = expected_listing(prefix, delimiter);
            for max_keys in 1..=LIST_KEYS.len() + 1 {
                let mut keys = Vec::new();
                let mut prefixes = Vec::new();
                let mut start = None;
                loop {
                    let query = ListQuery {
                        prefix: prefix.to_string(),
                        delimiter: delimiter.map(str::to_string),
                        start: start.clone(),
                        max_keys,
                    };
                    let page = engine.list_objects("bkt", query).await.unwrap();
                    let count = page.objects.len() + page.common_prefixes.len();
                    assert!(count <= max_keys);
                    keys.extend(page.objects.into_iter().map(|o| o.key));
                    prefixes.extend(page.common_prefixes);
                    match page.next_start {
                        Some(next) => {
                            assert_eq!(count, max_keys, "truncated pages are full");
                            start = Some(next);
                        }
                        None => break,
                    }
                }
                let ctx = format!("prefix={prefix:?} delimiter={delimiter:?} max_keys={max_keys}");
                assert_eq!(keys, want_keys, "{ctx}");
                assert_eq!(prefixes, want_prefixes, "{ctx}");
            }
        }
    }
}

#[tokio::test]
async fn listing_details() {
    let (engine, _dir) = test_engine().await;
    engine.create_bucket("bkt").await.unwrap();
    let record = put(&engine, "bkt", "a", b"abc").await;
    put(&engine, "bkt", "b", b"").await;

    let page = engine.list_objects("bkt", ListQuery { max_keys: 10, ..Default::default() }).await.unwrap();
    assert_eq!(page.objects[0].size, 3);
    assert_eq!(page.objects[0].etag, record.etag);
    assert_eq!(page.objects[0].last_modified, record.last_modified);

    let page = engine.list_objects("bkt", ListQuery { max_keys: 0, ..Default::default() }).await.unwrap();
    assert!(page.objects.is_empty());
    assert_eq!(page.next_start, Some(b"a".to_vec()));

    // start-after "a" is expressed as the inclusive start "a\0".
    let query = ListQuery { start: Some(b"a\0".to_vec()), max_keys: 10, ..Default::default() };
    let page = engine.list_objects("bkt", query).await.unwrap();
    assert_eq!(page.objects.iter().map(|o| o.key.as_str()).collect::<Vec<_>>(), ["b"]);

    let query = ListQuery { max_keys: 5000, ..Default::default() };
    assert_eq!(engine.list_objects("bkt", query).await.unwrap().objects.len(), 2);

    assert!(matches!(engine.list_objects("nope", ListQuery::default()).await, Err(Error::NoSuchBucket)));
}

// ---- multipart uploads ----

async fn upload_part(engine: &Engine, key: &str, upload_id: Uuid, number: u32, data: &[u8]) -> PartRecord {
    let upload = engine.get_upload("bkt", key, upload_id).await.unwrap();
    let staged = engine.stage_part(body(data, 333), &upload).await.unwrap();
    engine.commit_part("bkt", key, upload_id, number, staged, BTreeMap::new()).await.unwrap()
}

async fn complete(engine: &Engine, key: &str, upload_id: Uuid, parts: &[(u32, &PartRecord)]) -> Result<ObjectRecord> {
    let requested: Vec<_> = parts.iter().map(|(n, p)| (*n, p.etag.clone())).collect();
    let completion = engine.prepare_completion("bkt", key, upload_id, &requested).await?;
    engine.complete_upload("bkt", key, completion, BTreeMap::new(), None, WriteCondition::None).await
}

fn other_pattern(len: usize) -> Vec<u8> {
    (0..len).map(|i| (i * 7 % 13) as u8).collect()
}

#[tokio::test]
async fn multipart_upload_round_trip() {
    let (engine, _dir) = test_engine().await;
    engine.create_bucket("bkt").await.unwrap();
    let attrs = ObjectAttrs { content_type: Some("video/mp4".into()), ..Default::default() };
    let upload = engine.create_upload("bkt", "video.mp4", attrs, None, None).await.unwrap();
    let id = upload.upload_id;

    let (d1, d2, d3) = (pattern(1500), other_pattern(1500), pattern(700));
    // Parts may arrive in any order.
    let p2 = upload_part(&engine, "video.mp4", id, 2, &d2).await;
    let p1 = upload_part(&engine, "video.mp4", id, 1, &d1).await;
    let p3 = upload_part(&engine, "video.mp4", id, 3, &d3).await;
    assert_eq!(p1.etag, hex::encode(Md5::digest(&d1)));

    let record = complete(&engine, "video.mp4", id, &[(1, &p1), (2, &p2), (3, &p3)]).await.unwrap();
    let whole = [d1.clone(), d2.clone(), d3.clone()].concat();
    let mut md5s = Md5::new();
    for part in [&d1, &d2, &d3] {
        md5s.update(Md5::digest(part));
    }
    assert_eq!(record.etag, format!("{}-3", hex::encode(md5s.finalize())));
    assert_eq!(record.size, 3700);
    assert_eq!(record.part_sizes, [1500, 1500, 700]);
    assert_eq!(record.attrs.content_type.as_deref(), Some("video/mp4"));
    assert_eq!(engine.get_object("bkt", "video.mp4").await.unwrap(), record);
    assert_eq!(read_all(&engine, &record, 0..3700).await, whole);
    assert_eq!(read_all(&engine, &record, 1400..3100).await, &whole[1400..3100]);

    // Completing moved the parts' blocks into the object: nothing to collect.
    assert!(matches!(engine.get_upload("bkt", "video.mp4", id).await, Err(Error::NoSuchUpload)));
    assert_eq!(engine.collect_garbage().await.unwrap(), 0);
    assert_eq!(engine.meta.block_count().unwrap(), 5);
    assert_eq!(block_files(&engine).await, 5);
}

#[tokio::test]
async fn replaced_and_unused_parts_are_released() {
    let (engine, _dir) = test_engine().await;
    engine.create_bucket("bkt").await.unwrap();
    let id = engine.create_upload("bkt", "k", ObjectAttrs::default(), None, None).await.unwrap().upload_id;

    upload_part(&engine, "k", id, 1, &pattern(1500)).await;
    let p1 = upload_part(&engine, "k", id, 1, &other_pattern(1200)).await;
    assert_eq!(engine.collect_garbage().await.unwrap(), 2);
    let p2 = upload_part(&engine, "k", id, 2, &pattern(500)).await;
    upload_part(&engine, "k", id, 3, &pattern(500)).await;

    let record = complete(&engine, "k", id, &[(1, &p1), (2, &p2)]).await.unwrap();
    assert_eq!(engine.collect_garbage().await.unwrap(), 1, "part 3 was not used");
    assert_eq!(engine.meta.block_count().unwrap(), 3);
    assert_eq!(read_all(&engine, &record, 0..1700).await, [other_pattern(1200), pattern(500)].concat());
}

#[tokio::test]
async fn completion_is_validated() {
    let (engine, _dir) = test_engine().await;
    engine.create_bucket("bkt").await.unwrap();
    let upload = engine.create_upload("bkt", "k", ObjectAttrs::default(), None, None).await.unwrap();
    let id = upload.upload_id;
    let p1 = upload_part(&engine, "k", id, 1, &pattern(500)).await;
    let p2 = upload_part(&engine, "k", id, 2, &pattern(1500)).await;
    let p3 = upload_part(&engine, "k", id, 3, &pattern(100)).await;

    let prepare = |parts: Vec<(u32, String)>| {
        let engine = &engine;
        async move { engine.prepare_completion("bkt", "k", id, &parts).await }
    };
    assert!(matches!(prepare(vec![(2, p2.etag.clone()), (1, p1.etag.clone())]).await, Err(Error::InvalidPartOrder)));
    assert!(matches!(prepare(vec![(2, p2.etag.clone()), (2, p2.etag.clone())]).await, Err(Error::InvalidPartOrder)));
    assert!(matches!(prepare(vec![(1, "nope".into())]).await, Err(Error::InvalidPart(_))));
    assert!(matches!(prepare(vec![(4, p1.etag.clone())]).await, Err(Error::InvalidPart(_))));
    assert!(matches!(prepare(vec![]).await, Err(Error::InvalidPart(_))));
    assert!(matches!(prepare(vec![(1, p1.etag.clone()), (2, p2.etag.clone())]).await, Err(Error::EntityTooSmall(1))));
    // Only the last part may be small; quoted ETags are accepted.
    let ok = prepare(vec![(2, format!("\"{}\"", p2.etag)), (3, p3.etag.clone())]).await.unwrap();
    assert_eq!(ok.size(), 1600);

    for bad in [0, MAX_PARTS + 1] {
        let staged = engine.stage_part(body(b"x", 1), &upload).await.unwrap();
        let err = engine.commit_part("bkt", "k", id, bad, staged, BTreeMap::new()).await.unwrap_err();
        assert!(matches!(err, Error::InvalidArgument(_)));
    }
    // An upload id only works with the key it was created for.
    assert!(matches!(engine.get_upload("bkt", "other", id).await, Err(Error::NoSuchUpload)));
    assert!(matches!(
        engine.create_upload("bkt", "nul\0key", ObjectAttrs::default(), None, None).await,
        Err(Error::InvalidArgument(_))
    ));
}

#[tokio::test]
async fn completion_detects_a_part_replaced_concurrently() {
    let (engine, _dir) = test_engine().await;
    engine.create_bucket("bkt").await.unwrap();
    let id = engine.create_upload("bkt", "k", ObjectAttrs::default(), None, None).await.unwrap().upload_id;
    let p1 = upload_part(&engine, "k", id, 1, &pattern(1500)).await;
    let completion = engine.prepare_completion("bkt", "k", id, &[(1, p1.etag.clone())]).await.unwrap();

    // Same content, so the same ETag, but new blocks: the stale completion must not commit.
    upload_part(&engine, "k", id, 1, &pattern(1500)).await;
    let err =
        engine.complete_upload("bkt", "k", completion, BTreeMap::new(), None, WriteCondition::None).await.unwrap_err();
    assert!(matches!(err, Error::Conflict(_)));

    let record = complete(&engine, "k", id, &[(1, &p1)]).await.unwrap();
    engine.collect_garbage().await.unwrap();
    assert_eq!(read_all(&engine, &record, 0..1500).await, pattern(1500));
}

#[tokio::test]
async fn complete_honours_write_conditions() {
    let (engine, _dir) = test_engine().await;
    engine.create_bucket("bkt").await.unwrap();
    put(&engine, "bkt", "k", b"existing").await;
    let id = engine.create_upload("bkt", "k", ObjectAttrs::default(), None, None).await.unwrap().upload_id;
    let p1 = upload_part(&engine, "k", id, 1, &pattern(10)).await;
    let completion = engine.prepare_completion("bkt", "k", id, &[(1, p1.etag.clone())]).await.unwrap();
    let err = engine
        .complete_upload("bkt", "k", completion, BTreeMap::new(), None, WriteCondition::IfNoneMatchAny)
        .await
        .unwrap_err();
    assert!(matches!(err, Error::PreconditionFailed));
    // The upload survives a failed condition and can still be completed.
    complete(&engine, "k", id, &[(1, &p1)]).await.unwrap();
}

#[tokio::test]
async fn abort_releases_parts() {
    let (engine, _dir) = test_engine().await;
    engine.create_bucket("bkt").await.unwrap();
    let upload = engine.create_upload("bkt", "k", ObjectAttrs::default(), None, None).await.unwrap();
    let id = upload.upload_id;
    upload_part(&engine, "k", id, 1, &pattern(1500)).await;
    upload_part(&engine, "k", id, 2, &pattern(500)).await;

    engine.abort_upload("bkt", "k", id).await.unwrap();
    assert_eq!(engine.collect_garbage().await.unwrap(), 3);
    assert_eq!(engine.meta.block_count().unwrap(), 0);
    assert!(matches!(engine.abort_upload("bkt", "k", id).await, Err(Error::NoSuchUpload)));

    // A part uploaded after the abort is rejected and its blocks removed.
    let staged = engine.stage_part(body(&pattern(1500), 500), &upload).await.unwrap();
    let err = engine.commit_part("bkt", "k", id, 3, staged, BTreeMap::new()).await.unwrap_err();
    assert!(matches!(err, Error::NoSuchUpload));
    wait_for_block_files(&engine, 0).await;
}

#[tokio::test]
async fn abandoned_uploads_expire() {
    let dir = tempfile::tempdir().unwrap();
    let config = EngineConfig {
        data_dir: dir.path().to_path_buf(),
        gc_grace: Duration::ZERO,
        upload_expiry: Duration::from_millis(1),
        ..EngineConfig::default()
    };
    let engine = Engine::open(config).await.unwrap();
    engine.create_bucket("bkt").await.unwrap();
    let id = engine.create_upload("bkt", "k", ObjectAttrs::default(), None, None).await.unwrap().upload_id;
    upload_part(&engine, "k", id, 1, &pattern(10)).await;
    tokio::time::sleep(Duration::from_millis(5)).await;

    assert_eq!(engine.abort_stale_uploads().await.unwrap(), 1);
    assert!(matches!(engine.get_upload("bkt", "k", id).await, Err(Error::NoSuchUpload)));
    assert_eq!(engine.collect_garbage().await.unwrap(), 1);

    let (fresh, _dir) = test_engine().await;
    fresh.create_bucket("bkt").await.unwrap();
    fresh.create_upload("bkt", "k", ObjectAttrs::default(), None, None).await.unwrap();
    assert_eq!(fresh.abort_stale_uploads().await.unwrap(), 0, "a week has not passed");
}

#[tokio::test]
async fn deleting_a_bucket_aborts_its_uploads() {
    let (engine, _dir) = test_engine().await;
    engine.create_bucket("bkt").await.unwrap();
    let id = engine.create_upload("bkt", "k", ObjectAttrs::default(), None, None).await.unwrap().upload_id;
    upload_part(&engine, "k", id, 1, &pattern(1500)).await;

    engine.delete_bucket("bkt").await.unwrap();
    assert_eq!(engine.collect_garbage().await.unwrap(), 2);
    assert_eq!(engine.meta.block_count().unwrap(), 0);
}

#[tokio::test]
async fn list_parts_pages() {
    let (engine, _dir) = test_engine().await;
    engine.create_bucket("bkt").await.unwrap();
    let id = engine.create_upload("bkt", "k", ObjectAttrs::default(), None, None).await.unwrap().upload_id;
    for n in [5, 1, 3, 2, 4] {
        upload_part(&engine, "k", id, n, &pattern(n as usize)).await;
    }
    let numbers = |parts: &[(u32, PartRecord)]| parts.iter().map(|(n, _)| *n).collect::<Vec<_>>();
    let page = engine.list_parts("bkt", "k", id, 0, 2).await.unwrap();
    assert_eq!((numbers(&page.parts), page.truncated), (vec![1, 2], true));
    let page = engine.list_parts("bkt", "k", id, 2, 2).await.unwrap();
    assert_eq!((numbers(&page.parts), page.truncated), (vec![3, 4], true));
    let page = engine.list_parts("bkt", "k", id, 4, 2).await.unwrap();
    assert_eq!((numbers(&page.parts), page.truncated), (vec![5], false));
    assert_eq!(page.parts[0].1.size, 5);
}

#[tokio::test]
async fn list_uploads_groups_and_pages() {
    let (engine, _dir) = test_engine().await;
    engine.create_bucket("bkt").await.unwrap();
    let mut ids = Vec::new();
    for key in ["c/x/y", "b", "a/1", "b", "a/2"] {
        ids.push((key, engine.create_upload("bkt", key, ObjectAttrs::default(), None, None).await.unwrap().upload_id));
    }
    let (b_first, b_second) = (ids[1].1, ids[3].1);
    let list = |query: UploadListQuery| {
        let engine = &engine;
        async move { engine.list_uploads("bkt", query).await.unwrap() }
    };
    let entries = |page: &UploadPage| page.uploads.iter().map(|u| u.key.clone()).collect::<Vec<_>>();

    let all = list(UploadListQuery { max_uploads: 10, ..Default::default() }).await;
    assert_eq!(entries(&all), ["a/1", "a/2", "b", "b", "c/x/y"]);
    assert_eq!(all.uploads[2].upload_id, b_first, "same key: oldest first");
    assert!(!all.truncated);

    let grouped = list(UploadListQuery { delimiter: Some("/".into()), max_uploads: 10, ..Default::default() }).await;
    assert_eq!(entries(&grouped), ["b", "b"]);
    assert_eq!(grouped.common_prefixes, ["a/", "c/"]);

    let page1 = list(UploadListQuery { delimiter: Some("/".into()), max_uploads: 2, ..Default::default() }).await;
    assert_eq!((page1.common_prefixes.clone(), entries(&page1)), (vec!["a/".to_string()], vec!["b".to_string()]));
    assert!(page1.truncated);
    assert_eq!((page1.next_key_marker.as_deref(), page1.next_upload_id_marker), (Some("b"), Some(b_first)));
    let page2 = list(UploadListQuery {
        delimiter: Some("/".into()),
        key_marker: page1.next_key_marker,
        upload_id_marker: page1.next_upload_id_marker,
        max_uploads: 2,
        ..Default::default()
    })
    .await;
    assert_eq!(page2.uploads[0].upload_id, b_second);
    assert_eq!(page2.common_prefixes, ["c/"]);
    assert!(!page2.truncated);

    // A key marker that is a common prefix skips everything under it.
    let after_a = list(UploadListQuery {
        delimiter: Some("/".into()),
        key_marker: Some("a/".into()),
        max_uploads: 10,
        ..Default::default()
    })
    .await;
    assert_eq!(entries(&after_a), ["b", "b"]);
    assert_eq!(after_a.common_prefixes, ["c/"]);

    // One at a time, without a delimiter, visits every upload once.
    let mut seen = Vec::new();
    let (mut key_marker, mut upload_id_marker) = (None, None);
    loop {
        let page = list(UploadListQuery { key_marker, upload_id_marker, max_uploads: 1, ..Default::default() }).await;
        seen.extend(page.uploads.iter().map(|u| u.upload_id));
        if !page.truncated {
            break;
        }
        (key_marker, upload_id_marker) = (page.next_key_marker, page.next_upload_id_marker);
    }
    assert_eq!(seen, all.uploads.iter().map(|u| u.upload_id).collect::<Vec<_>>());

    let prefixed = list(UploadListQuery { prefix: "a/".into(), max_uploads: 10, ..Default::default() }).await;
    assert_eq!(entries(&prefixed), ["a/1", "a/2"]);
}

// ---- copies ----

#[tokio::test]
async fn copies_share_blocks() {
    let (engine, _dir) = test_engine().await;
    engine.create_bucket("bkt").await.unwrap();
    engine.create_bucket("other").await.unwrap();
    let source = put(&engine, "bkt", "src", &pattern(2500)).await;

    let attrs = ObjectAttrs { content_type: Some("text/plain".into()), ..Default::default() };
    let copy =
        engine.copy_object("bkt", "src", &source, "other", "dst", attrs.clone(), WriteCondition::None).await.unwrap();
    assert_eq!((copy.etag.as_str(), copy.size, &copy.data), (source.etag.as_str(), 2500, &source.data));
    assert_eq!(copy.attrs, attrs);
    assert_ne!(copy.version_id, source.version_id);
    assert_eq!(engine.meta.block_count().unwrap(), 3, "no data was copied");

    engine.delete_object("bkt", "src").await.unwrap();
    assert_eq!(engine.collect_garbage().await.unwrap(), 0, "the copy still uses the blocks");
    assert_eq!(read_all(&engine, &copy, 0..2500).await, pattern(2500));
    engine.delete_object("other", "dst").await.unwrap();
    assert_eq!(engine.collect_garbage().await.unwrap(), 3);
}

#[tokio::test]
async fn copy_onto_itself_replaces_metadata() {
    let (engine, _dir) = test_engine().await;
    engine.create_bucket("bkt").await.unwrap();
    let source = put(&engine, "bkt", "k", &pattern(1500)).await;
    let attrs = ObjectAttrs { content_type: Some("image/png".into()), ..Default::default() };
    let copy = engine.copy_object("bkt", "k", &source, "bkt", "k", attrs, WriteCondition::None).await.unwrap();
    assert_eq!(engine.collect_garbage().await.unwrap(), 0);
    assert_eq!(engine.get_object("bkt", "k").await.unwrap().attrs.content_type.as_deref(), Some("image/png"));
    assert_eq!(read_all(&engine, &copy, 0..1500).await, pattern(1500));
}

#[tokio::test]
async fn copy_checks_source_and_destination() {
    let (engine, _dir) = test_engine().await;
    engine.create_bucket("bkt").await.unwrap();
    let stale = put(&engine, "bkt", "src", &pattern(1500)).await;
    put(&engine, "bkt", "src", &pattern(1200)).await;
    let copy = |source: ObjectRecord, dst_bucket: &'static str, condition: WriteCondition| {
        let engine = &engine;
        async move { engine.copy_object("bkt", "src", &source, dst_bucket, "dst", ObjectAttrs::default(), condition).await }
    };
    assert!(matches!(copy(stale, "bkt", WriteCondition::None).await, Err(Error::Conflict(_))));

    let current = engine.get_object("bkt", "src").await.unwrap();
    assert!(matches!(copy(current.clone(), "nope", WriteCondition::None).await, Err(Error::NoSuchBucket)));
    copy(current.clone(), "bkt", WriteCondition::None).await.unwrap();
    assert!(matches!(
        copy(current.clone(), "bkt", WriteCondition::IfNoneMatchAny).await,
        Err(Error::PreconditionFailed)
    ));

    let small = put(&engine, "bkt", "small", b"inline").await;
    let copied = engine
        .copy_object("bkt", "small", &small, "bkt", "small-copy", ObjectAttrs::default(), WriteCondition::None)
        .await
        .unwrap();
    assert_eq!(read_all(&engine, &copied, 0..6).await, b"inline");
}

// ---- access keys and usage ----

fn grant(read: bool, write: bool, owner: bool) -> BucketGrant {
    BucketGrant { read, write, owner }
}

#[tokio::test]
async fn keys_and_grants() {
    let (engine, _dir) = test_engine().await;
    engine.create_bucket("alpha").await.unwrap();
    engine.create_bucket("beta").await.unwrap();

    let new = NewKey {
        name: "billing".into(),
        buckets: [("alpha".to_string(), grant(true, false, false))].into(),
        ..Default::default()
    };
    let key = engine.create_key(new).await.unwrap();
    assert!(key.access_key_id.starts_with("AK"));
    assert_eq!(key.secret_key.len(), 40);
    assert_eq!(engine.access_key(&key.access_key_id), Some(key.clone()));
    let id = key.access_key_id.as_str();

    let updated = engine.set_grant(id, "beta", grant(false, true, false)).await.unwrap();
    assert_eq!(updated.buckets["beta"], grant(false, true, false));
    let updated = engine.set_grant(id, "beta", BucketGrant::default()).await.unwrap();
    assert!(!updated.buckets.contains_key("beta"));
    assert!(matches!(engine.set_grant(id, "missing", BucketGrant::FULL).await, Err(Error::NoSuchBucket)));

    let update = KeyUpdate { name: Some("billing-v2".into()), disabled: Some(true), ..Default::default() };
    engine.update_key(id, update).await.unwrap();
    let cached = engine.access_key(id).unwrap();
    assert_eq!((cached.name.as_str(), cached.disabled, cached.buckets.len()), ("billing-v2", true, 1));

    // A key granted on a missing bucket is not created at all.
    let bad =
        NewKey { name: "x".into(), buckets: [("nope".to_string(), BucketGrant::FULL)].into(), ..Default::default() };
    assert!(matches!(engine.create_key(bad).await, Err(Error::NoSuchBucket)));
    assert_eq!(engine.access_keys().len(), 1);

    let imported = NewKey {
        name: "legacy".into(),
        access_key_id: Some("legacy-app".into()),
        secret_key: Some("legacy-secret".into()),
        ..Default::default()
    };
    engine.create_key(imported.clone()).await.unwrap();
    assert!(matches!(engine.create_key(imported).await, Err(Error::AccessKeyExists)));
    assert_eq!(engine.access_key("legacy-app").unwrap().secret_key, "legacy-secret");

    engine.delete_key(id).await.unwrap();
    assert_eq!(engine.access_key(id), None);
    assert!(matches!(engine.delete_key(id).await, Err(Error::NoSuchAccessKey)));
    assert!(matches!(engine.update_key(id, KeyUpdate::default()).await, Err(Error::NoSuchAccessKey)));
}

#[tokio::test]
async fn buckets_carry_their_grants() {
    let (engine, _dir) = test_engine().await;
    let new = NewKey { name: "app".into(), can_create_buckets: true, ..Default::default() };
    let id = engine.create_key(new).await.unwrap().access_key_id;

    engine.create_bucket_for("mine", &id).await.unwrap();
    assert_eq!(engine.access_key(&id).unwrap().buckets["mine"], BucketGrant::FULL);

    // Deleting the bucket revokes the grant, so a new bucket with the same name starts clean.
    engine.delete_bucket("mine").await.unwrap();
    assert!(engine.access_key(&id).unwrap().buckets.is_empty());
    engine.create_bucket("mine").await.unwrap();
    assert!(engine.access_key(&id).unwrap().buckets.is_empty());

    assert!(matches!(engine.create_bucket_for("orphan", "no-such-key").await, Err(Error::NoSuchAccessKey)));
    assert!(matches!(engine.get_bucket("orphan").await, Err(Error::NoSuchBucket)));
}

async fn bkt_usage(engine: &Engine) -> BucketUsage {
    engine.bucket_usage("bkt").await.unwrap().1
}

#[tokio::test]
async fn usage_tracks_every_write() {
    let (engine, _dir) = test_engine().await;
    engine.create_bucket("bkt").await.unwrap();
    assert_eq!(bkt_usage(&engine).await, BucketUsage::default());

    put(&engine, "bkt", "a", &pattern(100)).await;
    put(&engine, "bkt", "b", &pattern(2500)).await;
    put(&engine, "bkt", "a", &pattern(50)).await;
    assert_eq!(bkt_usage(&engine).await, BucketUsage { objects: 2, bytes: 2550, ..Default::default() });

    engine.delete_object("bkt", "b").await.unwrap();
    assert_eq!(bkt_usage(&engine).await, BucketUsage { objects: 1, bytes: 50, ..Default::default() });

    let a = engine.get_object("bkt", "a").await.unwrap();
    engine.copy_object("bkt", "a", &a, "bkt", "c", ObjectAttrs::default(), WriteCondition::None).await.unwrap();
    assert_eq!(bkt_usage(&engine).await, BucketUsage { objects: 2, bytes: 100, ..Default::default() });

    let id = engine.create_upload("bkt", "d", ObjectAttrs::default(), None, None).await.unwrap().upload_id;
    let p1 = upload_part(&engine, "d", id, 1, &pattern(1500)).await;
    let p2 = upload_part(&engine, "d", id, 2, &pattern(700)).await;
    assert_eq!(
        bkt_usage(&engine).await,
        BucketUsage { objects: 2, bytes: 100, ..Default::default() },
        "parts do not count until completed"
    );
    complete(&engine, "d", id, &[(1, &p1), (2, &p2)]).await.unwrap();
    assert_eq!(bkt_usage(&engine).await, BucketUsage { objects: 3, bytes: 2300, ..Default::default() });

    engine.delete_objects("bkt", vec!["a".into(), "c".into(), "missing".into()]).await.unwrap();
    assert_eq!(bkt_usage(&engine).await, BucketUsage { objects: 1, bytes: 2200, ..Default::default() });

    let all = engine.list_bucket_usage().await.unwrap();
    assert_eq!(all.len(), 1);
    assert_eq!(
        (all[0].0.name.as_str(), all[0].1),
        ("bkt", BucketUsage { objects: 1, bytes: 2200, ..Default::default() })
    );
}

#[tokio::test]
async fn schema_migration_rebuilds_usage() {
    let dir = tempfile::tempdir().unwrap();
    let config = EngineConfig { data_dir: dir.path().to_path_buf(), ..Default::default() };
    {
        let engine = Engine::open(config.clone()).await.unwrap();
        engine.create_bucket("bkt").await.unwrap();
        engine.create_bucket("empty").await.unwrap();
        put(&engine, "bkt", "a", &pattern(10)).await;
        put(&engine, "bkt", "b", &pattern(20)).await;
        engine.meta.downgrade_to_v1().unwrap();
        assert_eq!(engine.bucket_usage("bkt").await.unwrap().1, BucketUsage::default());
    }
    let engine = Engine::open(config).await.unwrap();
    assert_eq!(
        engine.bucket_usage("bkt").await.unwrap().1,
        BucketUsage { objects: 2, bytes: 30, ..Default::default() }
    );
    assert_eq!(engine.bucket_usage("empty").await.unwrap().1, BucketUsage::default());
}

// ---- scrubbing and timeouts ----

#[tokio::test]
async fn scrub_finds_damaged_and_missing_blocks() {
    let (engine, _dir) = test_engine().await;
    engine.create_bucket("bkt").await.unwrap();
    put(&engine, "bkt", "good", &pattern(2500)).await;
    let bad = put(&engine, "bkt", "bad", &pattern(1500)).await;
    let gone = put(&engine, "bkt", "gone", &pattern(1500)).await;
    put(&engine, "bkt", "small", b"inline").await;

    // Flip one byte of a block, and lose another block entirely.
    let damaged = engine.blocks.block_path(&bad.data.blocks()[1].id);
    let mut bytes = std::fs::read(&damaged).unwrap();
    bytes[10] ^= 0xFF;
    std::fs::write(&damaged, bytes).unwrap();
    std::fs::remove_file(engine.blocks.block_path(&gone.data.blocks()[0].id)).unwrap();

    let report = engine.scrub().await.unwrap();
    assert_eq!(report.objects_checked, 3, "inline objects have no blocks");
    assert_eq!(report.blocks_checked, 3 + 2 + 2);
    assert!(report.finished_at.is_some());
    let mut found: Vec<_> = report.corrupt.iter().map(|c| (c.key.as_str(), c.problem.as_str())).collect();
    found.sort();
    assert_eq!(found.len(), 2, "{found:?}");
    assert_eq!(found[0].0, "bad");
    assert!(found[0].1.contains("checksum mismatch"), "{}", found[0].1);
    assert_eq!(found[1], ("gone", "block file is missing"));
    assert!(report.corrupt.iter().all(|c| c.bucket == "bkt"));

    let stats = engine.stats();
    assert_eq!(stats.scrub_corrupt_blocks.load(std::sync::atomic::Ordering::Relaxed), 2);
    assert_eq!(engine.scrub_report().unwrap().blocks_checked, 7);
}

#[tokio::test]
async fn stalled_uploads_time_out() {
    let dir = tempfile::tempdir().unwrap();
    let config = EngineConfig {
        data_dir: dir.path().to_path_buf(),
        body_timeout: Duration::from_millis(50),
        ..EngineConfig::default()
    };
    let engine = Engine::open(config).await.unwrap();
    let first = stream::iter([Ok::<_, io::Error>(Bytes::from_static(b"some data"))]);
    let stalled = first.chain(stream::pending());
    assert!(matches!(engine.stage(Box::pin(stalled)).await, Err(Error::BodyTimeout)));
}

#[tokio::test]
async fn traffic_is_counted() {
    let (engine, _dir) = test_engine().await;
    engine.create_bucket("bkt").await.unwrap();
    let record = put(&engine, "bkt", "k", &pattern(1500)).await;
    read_all(&engine, &record, 0..1000).await;
    let stats = engine.stats();
    assert_eq!(stats.bytes_received.load(std::sync::atomic::Ordering::Relaxed), 1500);
    assert_eq!(stats.bytes_sent.load(std::sync::atomic::Ordering::Relaxed), 1000);
}

#[tokio::test]
async fn quotas_limit_growth_but_not_shrinking() {
    let (engine, _dir) = test_engine().await;
    engine.create_bucket("bkt").await.unwrap();
    engine.set_bucket_quota("bkt", Quota { max_bytes: Some(3000), max_objects: Some(2) }).await.unwrap();
    let put_result = |key: &'static str, len: usize| {
        let engine = &engine;
        async move {
            engine.put_object("bkt", key, body(&pattern(len), 333), ObjectAttrs::default(), WriteCondition::None).await
        }
    };
    let quota_exceeded = |r: Result<ObjectRecord>| matches!(r, Err(Error::QuotaExceeded(_)));

    put_result("a", 1000).await.unwrap();
    put_result("b", 1500).await.unwrap();
    // A third object, or more bytes than the limit, is refused...
    assert!(quota_exceeded(put_result("c", 10).await));
    assert!(quota_exceeded(put_result("b", 2500).await));
    // ...and nothing changed: usage and the stored object are as before.
    let (_, usage) = engine.bucket_usage("bkt").await.unwrap();
    assert_eq!((usage.objects, usage.bytes), (2, 2500));
    assert_eq!(engine.get_object("bkt", "b").await.unwrap().size, 1500);
    // Overwrites that fit are fine, and deleting makes room.
    put_result("b", 2000).await.unwrap();
    engine.delete_object("bkt", "a").await.unwrap();
    put_result("c", 500).await.unwrap();

    // Lowered below current usage: shrinking writes still pass, growth does not.
    engine.set_bucket_quota("bkt", Quota { max_bytes: Some(500), max_objects: None }).await.unwrap();
    put_result("b", 100).await.unwrap();
    assert!(quota_exceeded(put_result("b", 200).await));
    assert!(quota_exceeded(put_result("d", 1).await));
    // Early check, before any body is read, with a declared size.
    let early = engine.precheck_put("bkt", "d", &WriteCondition::None, Some(5000)).await;
    assert!(matches!(early, Err(Error::QuotaExceeded(_))));
    engine.precheck_put("bkt", "d", &WriteCondition::None, None).await.unwrap();

    // Copies and completed multipart uploads count too.
    engine.create_bucket("src").await.unwrap();
    let big = put(&engine, "src", "big", &pattern(1500)).await;
    let copy =
        engine.copy_object("src", "big", &big, "bkt", "copy", ObjectAttrs::default(), WriteCondition::None).await;
    assert!(quota_exceeded(copy));
    let upload = engine.create_upload("bkt", "mp", ObjectAttrs::default(), None, None).await.unwrap();
    let part = upload_part(&engine, "mp", upload.upload_id, 1, &pattern(1500)).await;
    assert!(quota_exceeded(complete(&engine, "mp", upload.upload_id, &[(1, &part)]).await));

    // No limits: anything goes.
    engine.set_bucket_quota("bkt", Quota::default()).await.unwrap();
    put_result("d", 5000).await.unwrap();
}

#[tokio::test]
async fn a_batch_gives_each_command_its_own_result() {
    let (engine, _dir) = test_engine().await;
    let create =
        |name: &str| Command::CreateBucket { name: name.into(), id: Uuid::new_v4(), created_at: 1, owner: None };
    let results = engine.meta.apply_batch(vec![
        create("a"),
        create("a"),
        create("b"),
        Command::DeleteBucket { name: "missing".into(), gc_due_at: 0 },
        Command::SetBucketQuota { bucket: "b".into(), quota: Quota { max_bytes: Some(10), max_objects: None } },
    ]);
    let summary: Vec<_> = results.iter().map(|r| r.as_ref().map(|_| ()).map_err(|e| e.to_string())).collect();
    assert!(summary[0].is_ok() && summary[2].is_ok() && summary[4].is_ok(), "{summary:?}");
    assert!(matches!(results[1], Err(Error::BucketAlreadyExists)));
    assert!(matches!(results[3], Err(Error::NoSuchBucket)));
    // The state is what applying them one by one would have left.
    let names: Vec<_> = engine.list_buckets().await.unwrap().into_iter().map(|b| (b.name, b.quota.max_bytes)).collect();
    assert_eq!(names, [("a".to_string(), None), ("b".to_string(), Some(10))]);
}

#[tokio::test]
async fn concurrent_writes_are_committed_together_and_correctly() {
    let (engine, _dir) = test_engine().await;
    let engine = Arc::new(engine);
    engine.create_bucket("bkt").await.unwrap();
    let put = |key: String, condition: WriteCondition| {
        let engine = Arc::clone(&engine);
        tokio::spawn(async move {
            engine.put_object("bkt", &key, body(b"data", 4), ObjectAttrs::default(), condition).await
        })
    };

    // Many writers at once: all land, in fewer commits than writes, and
    // usage counts every one.
    let commits = engine.stats.meta_commits.load(Ordering::Relaxed);
    let tasks: Vec<_> = (0..64).map(|i| put(format!("k{i}"), WriteCondition::None)).collect();
    for task in tasks {
        task.await.unwrap().unwrap();
    }
    let commits = engine.stats.meta_commits.load(Ordering::Relaxed) - commits;
    assert!(commits < 64, "{commits} commits for 64 concurrent writes");
    let (_, usage) = engine.bucket_usage("bkt").await.unwrap();
    assert_eq!((usage.objects, usage.bytes), (64, 256));

    // Racing create-only writes to one key, likely in one batch: exactly one wins.
    let tasks: Vec<_> = (0..32).map(|_| put("once".into(), WriteCondition::IfNoneMatchAny)).collect();
    let mut won = 0;
    for task in tasks {
        match task.await.unwrap() {
            Ok(_) => won += 1,
            Err(Error::PreconditionFailed) => {}
            Err(e) => panic!("unexpected {e}"),
        }
    }
    assert_eq!(won, 1);

    // A quota holds under concurrency, and refused uploads leave no blocks.
    engine.create_bucket("small").await.unwrap();
    engine.set_bucket_quota("small", Quota { max_bytes: None, max_objects: Some(10) }).await.unwrap();
    let tasks: Vec<_> = (0..40)
        .map(|i| {
            let engine = Arc::clone(&engine);
            tokio::spawn(async move {
                let data = pattern(2500);
                engine
                    .put_object(
                        "small",
                        &format!("k{i}"),
                        body(&data, 1000),
                        ObjectAttrs::default(),
                        WriteCondition::None,
                    )
                    .await
            })
        })
        .collect();
    let mut stored = 0;
    for task in tasks {
        match task.await.unwrap() {
            Ok(_) => stored += 1,
            Err(Error::QuotaExceeded(_)) => {}
            Err(e) => panic!("unexpected {e}"),
        }
    }
    assert_eq!(stored, 10);
    assert_eq!(engine.bucket_usage("small").await.unwrap().1.objects, 10);
    wait_for_block_files(&engine, 10 * 3).await;
}

#[tokio::test]
async fn multipart_parts_are_checked_against_the_quota_as_they_arrive() {
    let (engine, _dir) = test_engine().await;
    engine.create_bucket("bkt").await.unwrap();
    put(&engine, "bkt", "existing", &pattern(1000)).await;
    engine.set_bucket_quota("bkt", Quota { max_bytes: Some(5000), max_objects: None }).await.unwrap();
    let id = engine.create_upload("bkt", "mp", ObjectAttrs::default(), None, None).await.unwrap().upload_id;
    let check = |number: u32, size: u64| engine.precheck_part("bkt", "mp", id, number, Some(size));

    check(1, 2000).await.unwrap();
    upload_part(&engine, "mp", id, 1, &pattern(2000)).await;
    // 1000 stored + 2000 uploaded + 2500 more is past 5000: refused before the body is read.
    assert!(matches!(check(2, 2500).await, Err(Error::QuotaExceeded(_))));
    check(2, 2000).await.unwrap();
    // Re-sending part 1 replaces it rather than adding to it.
    check(1, 3500).await.unwrap();
    assert!(matches!(check(1, 4500).await, Err(Error::QuotaExceeded(_))));
    // Overwriting an existing object counts only the difference.
    let over = engine.create_upload("bkt", "existing", ObjectAttrs::default(), None, None).await.unwrap().upload_id;
    engine.precheck_part("bkt", "existing", over, 1, Some(3000)).await.unwrap();
    // Without a quota, nothing is checked.
    engine.set_bucket_quota("bkt", Quota::default()).await.unwrap();
    check(2, 1 << 40).await.unwrap();
}

async fn engine_with_keys(dir: &std::path::Path, current: u8, previous: &[u8]) -> Engine {
    let previous: Vec<Vec<u8>> = previous.iter().map(|b| vec![*b; 32]).collect();
    let config = EngineConfig {
        data_dir: dir.to_path_buf(),
        block_size: 150_000,
        inline_threshold: 100,
        min_part_size: 1000,
        keyring: Some(Arc::new(crate::crypto::Keyring::new(&[current; 32], &previous).unwrap())),
        ..EngineConfig::default()
    };
    Engine::open(config).await.unwrap()
}

/// Every block file's bytes, concatenated.
async fn stored_bytes(engine: &Engine) -> Vec<u8> {
    let mut out = Vec::new();
    for (_, _, path) in engine.blocks.scan().await.unwrap() {
        out.extend(tokio::fs::read(path).await.unwrap());
    }
    out
}

#[tokio::test]
async fn objects_are_encrypted_at_rest_and_read_back_by_range() {
    let dir = tempfile::tempdir().unwrap();
    let engine = engine_with_keys(dir.path(), 1, &[]).await;
    engine.create_bucket("bkt").await.unwrap();
    // Recognizable text, so it would show up in the files if stored in the clear.
    let text = |len: usize| -> Vec<u8> { b"plaintext marker ".iter().copied().cycle().take(len).collect() };

    let small = put(&engine, "bkt", "small", &text(50)).await;
    assert!(small.sse.is_some());
    let ObjectData::Inline(stored) = &small.data else { panic!("inline") };
    assert_eq!(stored.len() as u64, crate::crypto::stored_len(50));
    assert_eq!(read_all(&engine, &small, 5..40).await, text(50)[5..40]);

    // Three blocks of 150 000 bytes (several frames each) plus a partial one.
    let data = text(500_000);
    let big = put(&engine, "bkt", "big", &data).await;
    assert_eq!(big.etag, hex::encode(Md5::digest(&data)), "ETag is the plaintext MD5");
    assert_eq!(big.data.blocks().len(), 4);
    assert!(big.data.blocks().iter().all(|b| b.stored_len == Some(crate::crypto::stored_len(b.len))));
    assert_eq!(read_all(&engine, &big, 0..500_000).await, data);
    for range in [0..1, 65_535..65_537, 149_990..150_010, 131_000..400_000, 499_999..500_000] {
        assert_eq!(read_all(&engine, &big, range.clone()).await, data[range.start as usize..range.end as usize]);
    }
    let disk = stored_bytes(&engine).await;
    assert!(!disk.windows(17).any(|w| w == b"plaintext marker "), "plaintext found on disk");

    // Multipart parts share the upload's key; copies share the source's.
    let upload = engine.create_upload("bkt", "mp", ObjectAttrs::default(), None, None).await.unwrap();
    assert!(upload.sse.is_some());
    let p1 = upload_part(&engine, "mp", upload.upload_id, 1, &text(1500)).await;
    let p2 = upload_part(&engine, "mp", upload.upload_id, 2, &pattern(700)).await;
    let mp = complete(&engine, "mp", upload.upload_id, &[(1, &p1), (2, &p2)]).await.unwrap();
    assert_eq!(mp.sse, upload.sse);
    assert_eq!(read_all(&engine, &mp, 1400..1600).await, [&text(1500)[1400..], &pattern(700)[..100]].concat());
    let copy = engine
        .copy_object("bkt", "big", &big, "bkt", "copy", ObjectAttrs::default(), WriteCondition::None)
        .await
        .unwrap();
    assert_eq!(read_all(&engine, &copy, 0..500_000).await, data);

    // The scrubber checks encrypted blocks as stored.
    let report = engine.scrub().await.unwrap();
    assert!(report.corrupt.is_empty(), "{:?}", report.corrupt);
    drop(engine);

    // After rotation the old key still reads; new objects use the new one.
    let rotated = engine_with_keys(dir.path(), 2, &[1]).await;
    let big = rotated.get_object("bkt", "big").await.unwrap();
    assert_eq!(read_all(&rotated, &big, 0..500_000).await, data);
    let fresh = put(&rotated, "bkt", "fresh", &text(10)).await;
    assert_ne!(fresh.sse.unwrap().key_id, big.sse.as_ref().unwrap().key_id);
    drop(rotated);

    // Without the keys that encrypted stored data, the engine refuses to open.
    let keyless = EngineConfig { data_dir: dir.path().to_path_buf(), ..EngineConfig::default() };
    let err = Engine::open(keyless).await.err().expect("refused");
    assert!(err.to_string().contains(&crate::crypto::fingerprint(&[1u8; 32])), "{err}");
    let ring = crate::crypto::Keyring::new(&[2u8; 32], &[]).unwrap();
    let only_new = EngineConfig { keyring: Some(Arc::new(ring)), ..EngineConfig::default() };
    let only_new = EngineConfig { data_dir: dir.path().to_path_buf(), ..only_new };
    assert!(Engine::open(only_new).await.is_err());
}

#[tokio::test]
async fn access_key_secrets_are_sealed_under_the_master_key() {
    let dir = tempfile::tempdir().unwrap();
    let plain = EngineConfig { data_dir: dir.path().to_path_buf(), ..EngineConfig::default() };
    let engine = Engine::open(plain).await.unwrap();
    let new = |name: &str| NewKey { name: name.into(), ..NewKey::default() };
    let before = engine.create_key(new("before")).await.unwrap();
    assert!(engine.meta.list_keys().unwrap()[0].sealed_secret.is_none(), "no master key: stored as before");
    drop(engine);

    // Adding a master key seals existing secrets at startup; new ones are sealed when created.
    let engine = engine_with_keys(dir.path(), 1, &[]).await;
    let after = engine.create_key(new("after")).await.unwrap();
    for stored in engine.meta.list_keys().unwrap() {
        assert!(stored.secret_key.is_empty(), "{} stored in plaintext", stored.access_key_id);
        assert_eq!(stored.sealed_secret.unwrap().key_id, crate::crypto::fingerprint(&[1u8; 32]));
    }
    assert_eq!(engine.access_key(&before.access_key_id).unwrap().secret_key, before.secret_key);
    assert_eq!(engine.access_key(&after.access_key_id).unwrap().secret_key, after.secret_key);
    drop(engine);

    // Rotation re-seals them under the new key.
    let engine = engine_with_keys(dir.path(), 2, &[1]).await;
    assert!(
        engine
            .meta
            .list_keys()
            .unwrap()
            .iter()
            .all(|k| k.sealed_secret.as_ref().unwrap().key_id == crate::crypto::fingerprint(&[2u8; 32]))
    );
    assert_eq!(engine.access_key(&after.access_key_id).unwrap().secret_key, after.secret_key);

    // A sealed secret is bound to its key id: copied to another key, it does not open.
    let stored =
        engine.meta.list_keys().unwrap().into_iter().find(|k| k.access_key_id == before.access_key_id).unwrap();
    let ring = engine.config.keyring.clone().unwrap();
    assert!(ring.unseal(stored.sealed_secret.as_ref().unwrap(), after.access_key_id.as_bytes()).is_err());
    drop(engine);

    // Without the master key, the server does not start.
    let keyless = EngineConfig { data_dir: dir.path().to_path_buf(), ..EngineConfig::default() };
    assert!(Engine::open(keyless).await.is_err());
}

async fn listing(engine: &Engine, bucket: &str) -> Vec<(String, String, bool, bool)> {
    let query = VersionsQuery { max_keys: 1000, ..VersionsQuery::default() };
    let page = engine.list_versions(bucket, query).await.unwrap();
    page.versions
        .into_iter()
        .map(|e| (e.key, e.record.version().to_string(), e.is_latest, e.record.delete_marker))
        .collect()
}

#[tokio::test]
async fn versioning_keeps_versions_and_delete_markers() {
    let (engine, _dir) = test_engine().await;
    engine.create_bucket("bkt").await.unwrap();
    let old = put(&engine, "bkt", "k", b"null version").await;
    assert!(!old.versioned);
    engine.set_bucket_versioning("bkt", Versioning::Enabled).await.unwrap();

    let v1 = put(&engine, "bkt", "k", b"one").await;
    let v2 = put(&engine, "bkt", "k", &pattern(1500)).await;
    assert!(v1.versioned && v2.versioned);
    let list = listing(&engine, "bkt").await;
    assert_eq!(list.len(), 3, "{list:?}");
    assert_eq!((list[0].1.clone(), list[0].2), (v2.version().to_string(), true));
    assert_eq!((list[2].1.as_str(), list[2].2), ("null", false));
    // Every version reads back.
    let read = |v: VersionRef| engine.get_version("bkt", "k", Some(v));
    assert_eq!(read_all(&engine, &read(v1.version()).await.unwrap(), 0..3).await, b"one");
    assert_eq!(read_all(&engine, &read(VersionRef::Null).await.unwrap(), 0..12).await, b"null version");
    let (_, usage) = engine.bucket_usage("bkt").await.unwrap();
    assert_eq!((usage.objects, usage.bytes, usage.noncurrent_objects, usage.noncurrent_bytes), (1, 1500, 2, 15));

    // A plain delete adds a marker: the key disappears, its versions stay.
    let deleted = engine.delete_object("bkt", "k").await.unwrap();
    assert!(deleted.delete_marker);
    assert!(matches!(engine.get_object("bkt", "k").await, Err(Error::NoSuchKey)));
    assert!(engine.get_version("bkt", "k", None).await.unwrap().delete_marker);
    assert!(
        engine
            .list_objects("bkt", ListQuery { max_keys: 10, ..ListQuery::default() })
            .await
            .unwrap()
            .objects
            .is_empty()
    );
    assert_eq!(listing(&engine, "bkt").await.len(), 4);

    // Removing the marker brings the newest version back.
    engine.delete_object_if("bkt", "k", deleted.version, DeleteCondition::default()).await.unwrap();
    assert_eq!(engine.get_object("bkt", "k").await.unwrap().version_id, v2.version_id);
    // Removing the current version promotes the previous one.
    engine.delete_object_if("bkt", "k", Some(v2.version()), DeleteCondition::default()).await.unwrap();
    assert_eq!(engine.get_object("bkt", "k").await.unwrap().version_id, v1.version_id);
    let list = listing(&engine, "bkt").await;
    assert_eq!(list.iter().map(|e| e.1.clone()).collect::<Vec<_>>(), [v1.version().to_string(), "null".into()]);
    assert_eq!(engine.collect_garbage().await.unwrap(), 2, "v2's blocks are freed");

    // Suspended: writes replace the null version, other versions stay.
    engine.set_bucket_versioning("bkt", Versioning::Suspended).await.unwrap();
    let s = put(&engine, "bkt", "k", b"suspended").await;
    assert!(!s.versioned);
    let list = listing(&engine, "bkt").await;
    assert_eq!(list.iter().map(|e| e.1.as_str()).collect::<Vec<_>>(), ["null", &v1.version().to_string()]);
    assert_eq!(read_all(&engine, &read(VersionRef::Null).await.unwrap(), 0..9).await, b"suspended");

    // The bucket cannot be deleted while versions remain.
    for (_, version, _, _) in listing(&engine, "bkt").await {
        engine.delete_object_if("bkt", "k", VersionRef::parse(&version), DeleteCondition::default()).await.unwrap();
    }
    assert!(listing(&engine, "bkt").await.is_empty());
    let (_, usage) = engine.bucket_usage("bkt").await.unwrap();
    assert_eq!(usage, BucketUsage::default());
    engine.delete_bucket("bkt").await.unwrap();
}

#[tokio::test]
async fn lifecycle_rules_expire_objects_versions_markers_and_uploads() {
    use crate::meta::{LifecycleFilter, LifecycleRule};
    let dir = tempfile::tempdir().unwrap();
    let config = EngineConfig {
        data_dir: dir.path().to_path_buf(),
        block_size: 1000,
        inline_threshold: 100,
        gc_grace: Duration::ZERO,
        min_part_size: 1000,
        lifecycle_day: Duration::from_secs(1),
        ..EngineConfig::default()
    };
    let engine = Engine::open(config).await.unwrap();
    let day = 1000;
    let rule = |prefix: &str| LifecycleRule {
        enabled: true,
        filter: LifecycleFilter { prefix: prefix.into(), ..LifecycleFilter::default() },
        ..LifecycleRule::default()
    };

    // Unversioned: expiry removes objects; tag and size filters narrow it down.
    engine.create_bucket("plain").await.unwrap();
    let tagged = ObjectAttrs { tags: [("temp".to_string(), "yes".to_string())].into(), ..ObjectAttrs::default() };
    engine.put_object("plain", "tmp/a", body(b"aaaa", 4), tagged, WriteCondition::None).await.unwrap();
    put(&engine, "plain", "tmp/b", b"bbbb").await;
    put(&engine, "plain", "tmp/big", &pattern(5000)).await;
    put(&engine, "plain", "keep", b"kept").await;
    let mut tags_only = rule("tmp/");
    tags_only.filter.tags = [("temp".to_string(), "yes".to_string())].into();
    tags_only.expiration_days = Some(1);
    let mut big_only = rule("tmp/");
    big_only.filter.size_greater_than = Some(1000);
    big_only.expiration_days = Some(1);
    let mut disabled = rule("");
    disabled.enabled = false;
    disabled.expiration_days = Some(1);
    engine.set_bucket_lifecycle("plain", vec![tags_only, big_only, disabled]).await.unwrap();

    let now = now_millis();
    assert_eq!(engine.apply_lifecycle(now).await.unwrap(), LifecycleReport::default(), "nothing is old enough yet");
    let report = engine.apply_lifecycle(now + 2 * day).await.unwrap();
    assert_eq!(report.expired_objects, 2);
    let left: Vec<_> =
        engine.list_objects("plain", ListQuery { max_keys: 10, ..ListQuery::default() }).await.unwrap().objects;
    assert_eq!(left.iter().map(|o| o.key.as_str()).collect::<Vec<_>>(), ["keep", "tmp/b"]);

    // Versioned: current objects get delete markers, old versions go, the newest is kept.
    engine.create_bucket("ver").await.unwrap();
    engine.set_bucket_versioning("ver", Versioning::Enabled).await.unwrap();
    for i in 0..4 {
        put(&engine, "ver", "doc", format!("v{i}").as_bytes()).await;
    }
    put(&engine, "ver", "logs/x", b"log").await;
    let upload = engine.create_upload("ver", "half", ObjectAttrs::default(), None, None).await.unwrap();
    let mut logs = rule("logs/");
    logs.expiration_days = Some(1);
    logs.noncurrent_days = Some(1);
    let mut history = rule("doc");
    history.noncurrent_days = Some(1);
    history.newer_noncurrent_versions = Some(1);
    let mut markers = rule("");
    markers.expired_object_delete_marker = true;
    let mut uploads = rule("");
    uploads.abort_upload_days = Some(1);
    engine.set_bucket_lifecycle("ver", vec![logs, history, markers, uploads]).await.unwrap();

    let report = engine.apply_lifecycle(now_millis() + 2 * day).await.unwrap();
    assert_eq!(report.expired_objects, 1, "logs/x gets a delete marker");
    assert_eq!(report.expired_versions, 2, "doc keeps its current and newest noncurrent version");
    assert_eq!(report.aborted_uploads, 1);
    assert!(matches!(engine.get_upload("ver", "half", upload.upload_id).await, Err(Error::NoSuchUpload)));
    let versions_of = |key: &'static str| {
        let engine = &engine;
        async move { listing(engine, "ver").await.into_iter().filter(|e| e.0 == key).count() }
    };
    assert_eq!(versions_of("doc").await, 2);
    assert_eq!(versions_of("logs/x").await, 2, "the marker and the old version");

    // Later: the old log version expires, then its marker is left alone and removed.
    let report = engine.apply_lifecycle(now_millis() + 4 * day).await.unwrap();
    assert_eq!(report.expired_versions, 1);
    assert_eq!(versions_of("logs/x").await, 1);
    let report = engine.apply_lifecycle(now_millis() + 4 * day).await.unwrap();
    assert_eq!(report.expired_markers, 1);
    assert_eq!(versions_of("logs/x").await, 0);

    // Expiry by date.
    let mut dated = rule("");
    dated.expiration_date = Some(now_millis() + day);
    engine.set_bucket_lifecycle("plain", vec![dated]).await.unwrap();
    assert_eq!(engine.apply_lifecycle(now_millis()).await.unwrap().expired_objects, 0);
    assert_eq!(engine.apply_lifecycle(now_millis() + 2 * day).await.unwrap().expired_objects, 2);
}
