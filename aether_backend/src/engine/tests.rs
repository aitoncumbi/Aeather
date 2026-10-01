use std::time::Duration;

use futures::TryStreamExt;

use super::*;

/// Tiny blocks and inline threshold so multi-block paths run on small inputs.
async fn test_engine() -> (Engine, tempfile::TempDir) {
    let dir = tempfile::tempdir().unwrap();
    let config = EngineConfig {
        data_dir: dir.path().to_path_buf(),
        block_size: 1000,
        inline_threshold: 100,
        gc_grace: Duration::ZERO,
        orphan_grace: Duration::ZERO,
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

    let deleted = engine.delete_object("bkt", "k").await.unwrap();
    assert_eq!(deleted, Some(newer));
    assert!(matches!(engine.get_object("bkt", "k").await, Err(Error::NoSuchKey)));
    assert_eq!(engine.collect_garbage().await.unwrap(), 2);
    assert_eq!(block_files(&engine).await, 0);
    assert_eq!(engine.meta.block_count().unwrap(), 0);

    assert_eq!(engine.delete_object("bkt", "k").await.unwrap(), None);
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
    let mut writer = engine.blocks.create().await.unwrap();
    writer.write(b"orphan").await.unwrap();
    let orphan = writer.finish().await.unwrap();
    engine.blocks.unstage(&[orphan.id]);

    // A block from an upload still in progress.
    let mut writer = engine.blocks.create().await.unwrap();
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
