//! Object semantics on top of the metadata store and block store.
//!
//! The engine is the only place that coordinates the two, so it owns the
//! write ordering that makes uploads crash-safe:
//!
//! 1. stream the body into blocks (each one durable on disk),
//! 2. let the caller verify checksums,
//! 3. commit one metadata command that references the blocks.
//!
//! Blocks that never get committed are deleted when their [`StagedObject`] is
//! dropped, or by the orphan sweeper after a crash.

use std::collections::BTreeMap;
use std::io;
use std::net::Ipv4Addr;
use std::ops::Range;
use std::path::PathBuf;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use bytes::{Bytes, BytesMut};
use futures::stream::{self, BoxStream};
use futures::{Stream, StreamExt};
use md5::{Digest, Md5};
use tokio::io::AsyncReadExt;
use tokio::task::JoinHandle;
use tracing::{info, warn};
use uuid::Uuid;

use self::keys::KeyCache;
pub use self::keys::NewKey;
use crate::blocks::{BlockStore, BlockWriter};
use crate::error::{BoxError, Error, Result};
use crate::meta::{
    BlockRef, BucketRecord, BucketUsage, Command, ListPage, ListQuery, MetaStore, Millis, ObjectAttrs, ObjectData,
    ObjectRecord, Outcome, PartRecord, PartsPage, UploadListQuery, UploadPage, UploadRecord, WriteCondition,
};

/// S3's limit for one ListObjects page.
pub const MAX_LIST_KEYS: usize = 1000;
/// S3's limit on object key length, in bytes.
pub const MAX_KEY_LEN: usize = 1024;
/// S3's limit on parts per multipart upload (and the highest part number).
pub const MAX_PARTS: u32 = 10_000;
/// S3's limit for one ListParts or ListMultipartUploads page.
pub const MAX_LIST_PARTS: usize = 1000;

const READ_CHUNK: u64 = 256 * 1024;
const GC_BATCH: usize = 1000;

#[derive(Clone, Debug)]
pub struct EngineConfig {
    pub data_dir: PathBuf,
    /// Uploads are split into blocks of at most this many bytes.
    pub block_size: u64,
    /// Objects up to this size are stored inside their metadata record.
    pub inline_threshold: usize,
    /// How long blocks of deleted or overwritten objects are kept, so that
    /// downloads already in progress can finish.
    pub gc_grace: Duration,
    pub gc_interval: Duration,
    /// Unreferenced block files younger than this are left alone by the sweeper.
    pub orphan_grace: Duration,
    pub sweep_interval: Duration,
    /// Every part of a multipart upload except the last must be at least this big.
    pub min_part_size: u64,
    /// Multipart uploads not completed within this time are aborted. Zero disables.
    pub upload_expiry: Duration,
    /// An upload whose body sends nothing for this long fails with a timeout.
    pub body_timeout: Duration,
    /// How often every block is re-read and its checksum verified. Zero disables.
    pub scrub_interval: Duration,
    /// Read rate limit for scrubbing, in bytes per second. Zero means unlimited.
    pub scrub_rate: u64,
}

impl Default for EngineConfig {
    fn default() -> Self {
        Self {
            data_dir: PathBuf::from("data"),
            block_size: 64 * 1024 * 1024,
            inline_threshold: 4 * 1024,
            gc_grace: Duration::from_secs(15 * 60),
            gc_interval: Duration::from_secs(30),
            orphan_grace: Duration::from_secs(60 * 60),
            sweep_interval: Duration::from_secs(60 * 60),
            min_part_size: 5 * 1024 * 1024,
            upload_expiry: Duration::from_secs(7 * 24 * 60 * 60),
            body_timeout: Duration::from_secs(60),
            scrub_interval: Duration::from_secs(7 * 24 * 60 * 60),
            scrub_rate: 64 * 1024 * 1024,
        }
    }
}

pub struct Engine {
    meta: Arc<MetaStore>,
    blocks: Arc<BlockStore>,
    keys: Arc<KeyCache>,
    stats: Arc<EngineStats>,
    scrub: scrub::ScrubState,
    config: EngineConfig,
}

/// Counters since startup, exported as metrics.
#[derive(Debug, Default)]
pub struct EngineStats {
    /// Object data received from clients (uploads, parts, part copies).
    pub bytes_received: AtomicU64,
    /// Object data sent to clients.
    pub bytes_sent: AtomicU64,
    pub gc_blocks_deleted: AtomicU64,
    pub orphans_removed: AtomicU64,
    pub uploads_aborted: AtomicU64,
    pub scrub_blocks_checked: AtomicU64,
    pub scrub_bytes_checked: AtomicU64,
    pub scrub_corrupt_blocks: AtomicU64,
}

fn count(counter: &AtomicU64, n: u64) {
    counter.fetch_add(n, Ordering::Relaxed);
}

impl Engine {
    pub async fn open(config: EngineConfig) -> Result<Self> {
        tokio::fs::create_dir_all(&config.data_dir).await?;
        // Open metadata first: redb holds a lock on its file, so a second
        // process on the same data directory fails here, before the block
        // store clears the staging directory.
        let meta_path = config.data_dir.join("meta.redb");
        let meta = tokio::task::spawn_blocking(move || MetaStore::open(&meta_path)).await??;
        let blocks = BlockStore::open(&config.data_dir).await?;
        let keys = KeyCache::default();
        KeyCache::load(&meta, &keys)?;
        Ok(Self {
            meta: Arc::new(meta),
            blocks: Arc::new(blocks),
            keys: Arc::new(keys),
            stats: Arc::default(),
            scrub: scrub::ScrubState::default(),
            config,
        })
    }

    pub fn config(&self) -> &EngineConfig {
        &self.config
    }

    pub fn stats(&self) -> &EngineStats {
        &self.stats
    }

    // ---- buckets ----

    pub async fn create_bucket(&self, name: &str) -> Result<BucketRecord> {
        validate_bucket_name(name)?;
        let command =
            Command::CreateBucket { name: name.to_owned(), id: Uuid::new_v4(), created_at: now_millis(), owner: None };
        match self.apply(command).await? {
            Outcome::Bucket(bucket) => Ok(bucket),
            other => unreachable!("CreateBucket returned {other:?}"),
        }
    }

    /// Fails with `BucketNotEmpty` unless the bucket has no objects.
    /// Multipart uploads still in progress are aborted, and every key loses
    /// its permissions on the bucket.
    pub async fn delete_bucket(&self, name: &str) -> Result<()> {
        let command = Command::DeleteBucket { name: name.to_owned(), gc_due_at: self.gc_due_at() };
        self.apply_to_keys(command).await?;
        Ok(())
    }

    pub async fn bucket_usage(&self, name: &str) -> Result<(BucketRecord, BucketUsage)> {
        let name = name.to_owned();
        self.read(move |meta| meta.bucket_usage(&name)).await
    }

    /// Every bucket with its usage, in name order.
    pub async fn list_bucket_usage(&self) -> Result<Vec<(BucketRecord, BucketUsage)>> {
        self.read(|meta| meta.list_bucket_usage()).await
    }

    /// Number of block files referenced or awaiting deletion.
    pub async fn block_count(&self) -> Result<u64> {
        self.read(|meta| meta.block_count()).await
    }

    pub async fn get_bucket(&self, name: &str) -> Result<BucketRecord> {
        let name = name.to_owned();
        self.read(move |meta| meta.get_bucket(&name)?.ok_or(Error::NoSuchBucket)).await
    }

    pub async fn list_buckets(&self) -> Result<Vec<BucketRecord>> {
        self.read(|meta| meta.list_buckets()).await
    }

    // ---- writes ----

    /// Cheap early check so a doomed upload is rejected before its body is read.
    /// The authoritative check happens again inside the commit.
    pub async fn precheck_put(&self, bucket: &str, key: &str, condition: &WriteCondition) -> Result<()> {
        validate_key(key)?;
        let (bucket, key, condition) = (bucket.to_owned(), key.to_owned(), condition.clone());
        self.read(move |meta| {
            if meta.get_bucket(&bucket)?.is_none() {
                return Err(Error::NoSuchBucket);
            }
            let existing = match meta.get_object(&bucket, &key) {
                Ok(record) => Some(record),
                Err(Error::NoSuchKey) => None,
                Err(e) => return Err(e),
            };
            match (&condition, existing) {
                (WriteCondition::IfNoneMatchAny, Some(_)) => Err(Error::PreconditionFailed),
                (WriteCondition::IfMatch(_), None) => Err(Error::NoSuchKey),
                (WriteCondition::IfMatch(etag), Some(record)) if *etag != record.etag => Err(Error::PreconditionFailed),
                _ => Ok(()),
            }
        })
        .await
    }

    /// Streams a body into durable blocks (or an inline buffer) without
    /// committing it. Dropping the result deletes the blocks.
    pub async fn stage<S, E>(&self, body: S) -> Result<StagedObject>
    where
        S: Stream<Item = Result<Bytes, E>> + Unpin,
        E: Into<BoxError>,
    {
        self.stage_with(body, self.config.inline_threshold).await
    }

    /// Like [`stage`](Self::stage), for a part of a multipart upload. Parts are
    /// never inlined, since the completed object is a sequence of blocks.
    pub async fn stage_part<S, E>(&self, body: S) -> Result<StagedObject>
    where
        S: Stream<Item = Result<Bytes, E>> + Unpin,
        E: Into<BoxError>,
    {
        self.stage_with(body, 0).await
    }

    async fn stage_with<S, E>(&self, mut body: S, inline_threshold: usize) -> Result<StagedObject>
    where
        S: Stream<Item = Result<Bytes, E>> + Unpin,
        E: Into<BoxError>,
    {
        let mut staged = StagedObject {
            store: Arc::clone(&self.blocks),
            size: 0,
            md5: [0; 16],
            data: ObjectData::Inline(Vec::new()),
            block_ids: Vec::new(),
            committed: false,
        };
        let mut md5 = Md5::new();
        let mut inline = Some(Vec::new());
        let mut sink = BlockSink { engine: self, writer: None, blocks: Vec::new() };

        loop {
            let next = tokio::time::timeout(self.config.body_timeout, body.next()).await;
            let Some(chunk) = next.map_err(|_| Error::BodyTimeout)? else { break };
            let chunk = chunk.map_err(|e| Error::Body(e.into()))?;
            md5.update(&chunk);
            staged.size += chunk.len() as u64;
            count(&self.stats.bytes_received, chunk.len() as u64);
            if let Some(buf) = &mut inline {
                if buf.len() + chunk.len() <= inline_threshold {
                    buf.extend_from_slice(&chunk);
                    continue;
                }
                let buf = inline.take().expect("checked above");
                sink.write(&buf, &mut staged).await?;
            }
            sink.write(&chunk, &mut staged).await?;
        }
        sink.finish().await?;

        staged.md5 = md5.finalize().into();
        staged.data = match inline {
            Some(buf) => ObjectData::Inline(buf),
            None => ObjectData::Blocks(sink.blocks),
        };
        Ok(staged)
    }

    /// Commits a staged body as the current version of `bucket/key`.
    pub async fn commit_put(
        &self,
        bucket: &str,
        key: &str,
        staged: StagedObject,
        attrs: ObjectAttrs,
        condition: WriteCondition,
    ) -> Result<ObjectRecord> {
        validate_key(key)?;
        let now = now_millis();
        let record = ObjectRecord {
            version_id: Uuid::now_v7(),
            size: staged.size,
            etag: staged.etag(),
            last_modified: now,
            attrs,
            data: staged.data.clone(),
            part_sizes: Vec::new(),
        };
        let command = Command::PutObject {
            bucket: bucket.to_owned(),
            key: key.to_owned(),
            record: record.clone(),
            condition,
            gc_due_at: now + self.gc_grace_millis(),
        };
        self.commit_staged(staged, command).await?;
        Ok(record)
    }

    /// Stage and commit in one call.
    pub async fn put_object<S, E>(
        &self,
        bucket: &str,
        key: &str,
        body: S,
        attrs: ObjectAttrs,
        condition: WriteCondition,
    ) -> Result<ObjectRecord>
    where
        S: Stream<Item = Result<Bytes, E>> + Unpin,
        E: Into<BoxError>,
    {
        self.precheck_put(bucket, key, &condition).await?;
        let staged = self.stage(body).await?;
        self.commit_put(bucket, key, staged, attrs, condition).await
    }

    /// Deleting a key that does not exist succeeds, as in S3.
    pub async fn delete_object(&self, bucket: &str, key: &str) -> Result<Option<ObjectRecord>> {
        let command = Command::DeleteObject {
            bucket: bucket.to_owned(),
            key: key.to_owned(),
            gc_due_at: now_millis() + self.gc_grace_millis(),
        };
        match self.apply(command).await? {
            Outcome::Object(previous) => Ok(previous),
            other => unreachable!("DeleteObject returned {other:?}"),
        }
    }

    /// Deletes up to 1000 keys in one commit. Missing keys are not an error.
    pub async fn delete_objects(&self, bucket: &str, keys: Vec<String>) -> Result<()> {
        let command = Command::DeleteObjects {
            bucket: bucket.to_owned(),
            keys,
            gc_due_at: now_millis() + self.gc_grace_millis(),
        };
        self.apply(command).await?;
        Ok(())
    }

    /// Makes `dst_bucket/dst_key` a copy of `source`, which the caller read from
    /// `src_bucket/src_key`. The copy shares the source's blocks, so no data is
    /// written. Fails with `Conflict` if the source changed in the meantime.
    #[allow(clippy::too_many_arguments)]
    pub async fn copy_object(
        &self,
        src_bucket: &str,
        src_key: &str,
        source: &ObjectRecord,
        dst_bucket: &str,
        dst_key: &str,
        attrs: ObjectAttrs,
        condition: WriteCondition,
    ) -> Result<ObjectRecord> {
        validate_key(dst_key)?;
        let now = now_millis();
        let record = ObjectRecord {
            version_id: Uuid::now_v7(),
            size: source.size,
            etag: source.etag.clone(),
            last_modified: now,
            attrs,
            data: source.data.clone(),
            part_sizes: source.part_sizes.clone(),
        };
        let command = Command::CopyObject {
            src_bucket: src_bucket.to_owned(),
            src_key: src_key.to_owned(),
            src_version: source.version_id,
            dst_bucket: dst_bucket.to_owned(),
            dst_key: dst_key.to_owned(),
            record: record.clone(),
            condition,
            gc_due_at: now + self.gc_grace_millis(),
        };
        self.apply(command).await?;
        Ok(record)
    }

    // ---- multipart uploads ----

    pub async fn create_upload(
        &self,
        bucket: &str,
        key: &str,
        attrs: ObjectAttrs,
        checksum_algorithm: Option<String>,
        checksum_type: Option<String>,
    ) -> Result<UploadRecord> {
        validate_key(key)?;
        let upload = UploadRecord {
            upload_id: Uuid::now_v7(),
            key: key.to_owned(),
            initiated: now_millis(),
            attrs,
            checksum_algorithm,
            checksum_type,
        };
        self.apply(Command::CreateUpload { bucket: bucket.to_owned(), upload: upload.clone() }).await?;
        Ok(upload)
    }

    /// Fails with `NoSuchBucket` or `NoSuchUpload`.
    pub async fn get_upload(&self, bucket: &str, key: &str, upload_id: Uuid) -> Result<UploadRecord> {
        let (bucket, key) = (bucket.to_owned(), key.to_owned());
        self.read(move |meta| meta.get_upload(&bucket, &key, &upload_id)).await
    }

    /// Commits a body staged with [`stage_part`](Self::stage_part) as part
    /// `part_number`, replacing an earlier upload of the same part.
    pub async fn commit_part(
        &self,
        bucket: &str,
        key: &str,
        upload_id: Uuid,
        part_number: u32,
        staged: StagedObject,
        checksums: BTreeMap<String, String>,
    ) -> Result<PartRecord> {
        validate_part_number(part_number)?;
        if matches!(&staged.data, ObjectData::Inline(data) if !data.is_empty()) {
            return Err(Error::InvalidArgument("parts must be staged with stage_part".into()));
        }
        let now = now_millis();
        let part = PartRecord {
            size: staged.size,
            etag: staged.etag(),
            last_modified: now,
            blocks: staged.data.blocks().to_vec(),
            checksums,
        };
        let command = Command::PutPart {
            bucket: bucket.to_owned(),
            key: key.to_owned(),
            upload_id,
            part_number,
            part: part.clone(),
            gc_due_at: now + self.gc_grace_millis(),
        };
        self.commit_staged(staged, command).await?;
        Ok(part)
    }

    /// Parts numbered above `after`, up to `max` (capped at 1000).
    pub async fn list_parts(
        &self,
        bucket: &str,
        key: &str,
        upload_id: Uuid,
        after: u32,
        max: usize,
    ) -> Result<PartsPage> {
        let (bucket, key) = (bucket.to_owned(), key.to_owned());
        let max = max.min(MAX_LIST_PARTS);
        self.read(move |meta| meta.list_parts(&bucket, &key, &upload_id, after, max)).await
    }

    /// Validates the part list of a CompleteMultipartUpload request (part
    /// number and ETag pairs, as sent by the client).
    pub async fn prepare_completion(
        &self,
        bucket: &str,
        key: &str,
        upload_id: Uuid,
        requested: &[(u32, String)],
    ) -> Result<Completion> {
        if requested.is_empty() {
            return Err(Error::InvalidPart("at least one part must be specified".into()));
        }
        if requested.windows(2).any(|w| w[0].0 >= w[1].0) {
            return Err(Error::InvalidPartOrder);
        }
        let (bucket, key) = (bucket.to_owned(), key.to_owned());
        let PartsPage { upload, parts: uploaded, .. } =
            self.read(move |meta| meta.list_parts(&bucket, &key, &upload_id, 0, usize::MAX)).await?;

        let mut parts = Vec::with_capacity(requested.len());
        for (i, (number, etag)) in requested.iter().enumerate() {
            let Ok(found) = uploaded.binary_search_by_key(number, |(n, _)| *n) else {
                return Err(Error::InvalidPart(format!("part {number} has not been uploaded")));
            };
            let part = &uploaded[found].1;
            if etag.trim_matches('"') != part.etag {
                return Err(Error::InvalidPart(format!("the ETag of part {number} does not match")));
            }
            if i + 1 < requested.len() && part.size < self.config.min_part_size {
                return Err(Error::EntityTooSmall(*number));
            }
            parts.push((*number, part.clone()));
        }
        Ok(Completion { upload, parts })
    }

    /// Turns a validated upload into an object. Only metadata is written: the
    /// object takes over the parts' blocks.
    pub async fn complete_upload(
        &self,
        bucket: &str,
        key: &str,
        completion: Completion,
        checksums: BTreeMap<String, String>,
        checksum_type: Option<String>,
        condition: WriteCondition,
    ) -> Result<ObjectRecord> {
        let now = now_millis();
        let mut attrs = completion.upload.attrs.clone();
        attrs.checksums = checksums;
        attrs.checksum_type = checksum_type;
        let record = ObjectRecord {
            version_id: Uuid::now_v7(),
            size: completion.size(),
            etag: completion.etag()?,
            last_modified: now,
            attrs,
            data: ObjectData::Blocks(completion.parts.iter().flat_map(|(_, p)| p.blocks.iter().copied()).collect()),
            part_sizes: completion.parts.iter().map(|(_, p)| p.size).collect(),
        };
        let command = Command::CompleteUpload {
            bucket: bucket.to_owned(),
            key: key.to_owned(),
            upload_id: completion.upload.upload_id,
            part_numbers: completion.parts.iter().map(|(n, _)| *n).collect(),
            record: record.clone(),
            condition,
            gc_due_at: now + self.gc_grace_millis(),
        };
        self.apply(command).await?;
        Ok(record)
    }

    pub async fn abort_upload(&self, bucket: &str, key: &str, upload_id: Uuid) -> Result<()> {
        let command = Command::AbortUpload {
            bucket: bucket.to_owned(),
            key: key.to_owned(),
            upload_id,
            gc_due_at: self.gc_due_at(),
        };
        self.apply(command).await?;
        Ok(())
    }

    pub async fn list_uploads(&self, bucket: &str, mut query: UploadListQuery) -> Result<UploadPage> {
        query.max_uploads = query.max_uploads.min(MAX_LIST_PARTS);
        let bucket = bucket.to_owned();
        self.read(move |meta| meta.list_uploads(&bucket, &query)).await
    }

    /// Aborts uploads older than `upload_expiry`. Returns how many.
    pub async fn abort_stale_uploads(&self) -> Result<usize> {
        if self.config.upload_expiry.is_zero() {
            return Ok(0);
        }
        let cutoff = now_millis() - self.config.upload_expiry.as_millis() as Millis;
        let stale = self.read(move |meta| meta.uploads_started_before(cutoff)).await?;
        let mut aborted = 0;
        for (bucket, key, upload_id) in stale {
            match self.abort_upload(&bucket, &key, upload_id).await {
                Ok(()) => {
                    aborted += 1;
                    count(&self.stats.uploads_aborted, 1);
                }
                // Completed, aborted or deleted with its bucket in the meantime.
                Err(Error::NoSuchUpload | Error::NoSuchBucket) => {}
                Err(e) => return Err(e),
            }
        }
        Ok(aborted)
    }

    // ---- reads ----

    pub async fn get_object(&self, bucket: &str, key: &str) -> Result<ObjectRecord> {
        let (bucket, key) = (bucket.to_owned(), key.to_owned());
        self.read(move |meta| meta.get_object(&bucket, &key)).await
    }

    /// Streams `range` (already validated against `record.size`) of an object.
    pub fn read_object(&self, record: &ObjectRecord, range: Range<u64>) -> BoxStream<'static, io::Result<Bytes>> {
        match &record.data {
            ObjectData::Inline(data) => {
                let bytes = Bytes::copy_from_slice(&data[range.start as usize..range.end as usize]);
                count(&self.stats.bytes_sent, bytes.len() as u64);
                stream::once(async move { Ok(bytes) }).boxed()
            }
            ObjectData::Blocks(blocks) => {
                let state = ReadState {
                    store: Arc::clone(&self.blocks),
                    stats: Arc::clone(&self.stats),
                    segments: segments(blocks, range).into_iter(),
                    current: None,
                };
                stream::try_unfold(state, ReadState::next).boxed()
            }
        }
    }

    pub async fn list_objects(&self, bucket: &str, mut query: ListQuery) -> Result<ListPage> {
        query.max_keys = query.max_keys.min(MAX_LIST_KEYS);
        let bucket = bucket.to_owned();
        self.read(move |meta| meta.list_objects(&bucket, &query)).await
    }

    // ---- background work ----

    /// Deletes blocks whose grace period has passed. Returns how many.
    pub async fn collect_garbage(&self) -> Result<usize> {
        let now = now_millis();
        let mut total = 0;
        loop {
            let due = self.read(move |meta| meta.due_gc(now, GC_BATCH)).await?;
            let batch = due.len();
            if batch == 0 {
                break;
            }
            for (_, id) in &due {
                self.blocks.delete(id).await?;
            }
            self.apply(Command::ForgetBlocks { entries: due }).await?;
            count(&self.stats.gc_blocks_deleted, batch as u64);
            total += batch;
            if batch < GC_BATCH {
                break;
            }
        }
        Ok(total)
    }

    /// Deletes block files no metadata knows about: leftovers of uploads that
    /// were interrupted by a crash. Returns how many.
    pub async fn sweep_orphans(&self) -> Result<usize> {
        let cutoff = SystemTime::now() - self.config.orphan_grace;
        let files = self.blocks.scan().await?;
        let mut removed = 0;
        for chunk in files.chunks(GC_BATCH) {
            // Check the staged set before metadata: uploads commit and only
            // then unstage, so a block is always in at least one of the two.
            let candidates: Vec<_> = chunk
                .iter()
                .filter(|(id, modified, _)| *modified <= cutoff && !self.blocks.is_staged(id))
                .cloned()
                .collect();
            let unknown = self
                .read(move |meta| {
                    let mut unknown = Vec::new();
                    for (id, _, path) in candidates {
                        if !meta.block_known(&id)? {
                            unknown.push(path);
                        }
                    }
                    Ok(unknown)
                })
                .await?;
            for path in unknown {
                match tokio::fs::remove_file(&path).await {
                    Ok(()) => {
                        removed += 1;
                        count(&self.stats.orphans_removed, 1);
                    }
                    Err(e) if e.kind() == io::ErrorKind::NotFound => {}
                    Err(e) => return Err(e.into()),
                }
            }
        }
        Ok(removed)
    }

    /// Runs GC, the orphan sweeper and abandoned-upload cleanup until the
    /// returned handle is dropped.
    pub fn start_background_tasks(self: &Arc<Self>) -> BackgroundTasks {
        let gc = {
            let engine = Arc::clone(self);
            tokio::spawn(async move {
                let mut tick = tokio::time::interval(engine.config.gc_interval);
                loop {
                    tick.tick().await;
                    match engine.collect_garbage().await {
                        Ok(0) => {}
                        Ok(n) => info!(blocks = n, "garbage collected"),
                        Err(e) => warn!(error = %e, "garbage collection failed"),
                    }
                }
            })
        };
        let sweeper = {
            let engine = Arc::clone(self);
            tokio::spawn(async move {
                let mut tick = tokio::time::interval(engine.config.sweep_interval);
                loop {
                    tick.tick().await;
                    match engine.sweep_orphans().await {
                        Ok(0) => {}
                        Ok(n) => info!(files = n, "removed orphaned block files"),
                        Err(e) => warn!(error = %e, "orphan sweep failed"),
                    }
                    match engine.abort_stale_uploads().await {
                        Ok(0) => {}
                        Ok(n) => info!(uploads = n, "aborted abandoned multipart uploads"),
                        Err(e) => warn!(error = %e, "aborting abandoned uploads failed"),
                    }
                }
            })
        };
        let mut handles = vec![gc, sweeper];
        if !self.config.scrub_interval.is_zero() {
            handles.push(tokio::spawn(scrub::run_periodically(Arc::clone(self))));
        }
        BackgroundTasks { handles }
    }

    // ---- helpers ----

    async fn apply(&self, command: Command) -> Result<Outcome> {
        let meta = Arc::clone(&self.meta);
        tokio::task::spawn_blocking(move || meta.apply(command)).await?
    }

    /// Applies a command that references a staged body's blocks. The staged
    /// body moves into the blocking task, so it outlives the commit even if
    /// this future is cancelled (the client disconnects); otherwise a cancelled
    /// request could delete the blocks of a committed object.
    async fn commit_staged(&self, staged: StagedObject, command: Command) -> Result<Outcome> {
        let meta = Arc::clone(&self.meta);
        tokio::task::spawn_blocking(move || {
            let mut staged = staged;
            let result = meta.apply(command);
            staged.committed = result.is_ok();
            result
        })
        .await?
    }

    async fn read<T, F>(&self, f: F) -> Result<T>
    where
        T: Send + 'static,
        F: FnOnce(&MetaStore) -> Result<T> + Send + 'static,
    {
        let meta = Arc::clone(&self.meta);
        tokio::task::spawn_blocking(move || f(&meta)).await?
    }

    fn gc_grace_millis(&self) -> Millis {
        self.config.gc_grace.as_millis() as Millis
    }

    fn gc_due_at(&self) -> Millis {
        now_millis() + self.gc_grace_millis()
    }
}

/// A validated CompleteMultipartUpload request.
#[derive(Clone, Debug)]
pub struct Completion {
    pub upload: UploadRecord,
    /// The parts the object will consist of, in order.
    pub parts: Vec<(u32, PartRecord)>,
}

impl Completion {
    pub fn size(&self) -> u64 {
        self.parts.iter().map(|(_, p)| p.size).sum()
    }

    /// S3's multipart ETag: the MD5 of the parts' binary MD5s, then `-<count>`.
    pub fn etag(&self) -> Result<String> {
        let mut md5 = Md5::new();
        for (number, part) in &self.parts {
            let digest = hex::decode(&part.etag).map_err(|_| Error::Corrupt(format!("bad ETag on part {number}")))?;
            md5.update(&digest);
        }
        Ok(format!("{}-{}", hex::encode(md5.finalize()), self.parts.len()))
    }
}

/// A body written to disk but not yet visible. Dropping it without a
/// successful commit deletes its blocks.
pub struct StagedObject {
    store: Arc<BlockStore>,
    pub size: u64,
    pub md5: [u8; 16],
    data: ObjectData,
    block_ids: Vec<Uuid>,
    committed: bool,
}

impl StagedObject {
    /// The S3 ETag of a single-part upload: the hex MD5 of the content.
    pub fn etag(&self) -> String {
        hex::encode(self.md5)
    }
}

impl std::fmt::Debug for StagedObject {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("StagedObject")
            .field("size", &self.size)
            .field("etag", &self.etag())
            .field("blocks", &self.block_ids.len())
            .finish()
    }
}

impl Drop for StagedObject {
    fn drop(&mut self) {
        let ids = std::mem::take(&mut self.block_ids);
        if self.committed || ids.is_empty() {
            self.store.unstage(&ids);
            return;
        }
        match tokio::runtime::Handle::try_current() {
            Ok(handle) => {
                let store = Arc::clone(&self.store);
                handle.spawn(async move {
                    for id in &ids {
                        if let Err(e) = store.delete(id).await {
                            warn!(block = %id, error = %e, "failed to delete uncommitted block");
                        }
                    }
                    store.unstage(&ids);
                });
            }
            // No runtime (shutdown): the orphan sweeper removes them on next start.
            Err(_) => self.store.unstage(&ids),
        }
    }
}

struct BlockSink<'a> {
    engine: &'a Engine,
    writer: Option<BlockWriter>,
    blocks: Vec<BlockRef>,
}

impl BlockSink<'_> {
    async fn write(&mut self, mut data: &[u8], staged: &mut StagedObject) -> Result<()> {
        let block_size = self.engine.config.block_size;
        while !data.is_empty() {
            if self.writer.is_none() {
                let writer = self.engine.blocks.create().await?;
                staged.block_ids.push(writer.id());
                self.writer = Some(writer);
            }
            let writer = self.writer.as_mut().expect("set above");
            let room = (block_size - writer.bytes_written()) as usize;
            let (now, rest) = data.split_at(room.min(data.len()));
            writer.write(now).await?;
            data = rest;
            if writer.bytes_written() == block_size {
                self.finish().await?;
            }
        }
        Ok(())
    }

    async fn finish(&mut self) -> Result<()> {
        if let Some(writer) = self.writer.take() {
            self.blocks.push(writer.finish().await?);
        }
        Ok(())
    }
}

/// `(block id, offset in block, length)` for each block overlapping `range`.
fn segments(blocks: &[BlockRef], range: Range<u64>) -> Vec<(Uuid, u64, u64)> {
    let mut out = Vec::new();
    let mut block_start = 0;
    for block in blocks {
        let block_end = block_start + block.len;
        if block_end > range.start && block_start < range.end {
            let start = range.start.max(block_start);
            let end = range.end.min(block_end);
            out.push((block.id, start - block_start, end - start));
        }
        if block_end >= range.end {
            break;
        }
        block_start = block_end;
    }
    out
}

struct ReadState {
    store: Arc<BlockStore>,
    stats: Arc<EngineStats>,
    segments: std::vec::IntoIter<(Uuid, u64, u64)>,
    current: Option<(tokio::io::Take<tokio::fs::File>, u64)>,
}

impl ReadState {
    async fn next(mut self) -> io::Result<Option<(Bytes, Self)>> {
        loop {
            if self.current.is_none() {
                let Some((id, offset, len)) = self.segments.next() else {
                    return Ok(None);
                };
                self.current = Some((self.store.open_range(&id, offset, len).await?, len));
            }
            let (file, remaining) = self.current.as_mut().expect("set above");
            if *remaining == 0 {
                self.current = None;
                continue;
            }
            let mut buf = BytesMut::with_capacity(READ_CHUNK.min(*remaining) as usize);
            let n = file.read_buf(&mut buf).await?;
            if n == 0 {
                return Err(io::Error::new(io::ErrorKind::UnexpectedEof, "block file is shorter than its metadata"));
            }
            *remaining -= n as u64;
            count(&self.stats.bytes_sent, n as u64);
            return Ok(Some((buf.freeze(), self)));
        }
    }
}

/// Aborts the background tasks when dropped.
pub struct BackgroundTasks {
    handles: Vec<JoinHandle<()>>,
}

impl BackgroundTasks {
    /// Adds another task to stop with the others.
    pub fn push(&mut self, handle: JoinHandle<()>) {
        self.handles.push(handle);
    }
}

impl Drop for BackgroundTasks {
    fn drop(&mut self) {
        for handle in &self.handles {
            handle.abort();
        }
    }
}

pub fn now_millis() -> Millis {
    SystemTime::now().duration_since(UNIX_EPOCH).map_or(0, |d| d.as_millis() as Millis)
}

/// S3 bucket naming rules for new buckets.
pub fn validate_bucket_name(name: &str) -> Result<()> {
    let bytes = name.as_bytes();
    let valid = (3..=63).contains(&bytes.len())
        && bytes.iter().all(|&b| b.is_ascii_lowercase() || b.is_ascii_digit() || b == b'-' || b == b'.')
        && bytes[0].is_ascii_alphanumeric()
        && bytes[bytes.len() - 1].is_ascii_alphanumeric()
        && !name.contains("..")
        && name.parse::<Ipv4Addr>().is_err();
    if valid { Ok(()) } else { Err(Error::InvalidBucketName(name.to_owned())) }
}

pub fn validate_key(key: &str) -> Result<()> {
    if key.is_empty() || key.len() > MAX_KEY_LEN {
        return Err(Error::InvalidArgument(format!("object keys must be 1 to {MAX_KEY_LEN} bytes")));
    }
    // NUL cannot appear in S3's XML responses, and the uploads table uses it as a separator.
    if key.contains('\0') {
        return Err(Error::InvalidArgument("object keys must not contain NUL characters".into()));
    }
    Ok(())
}

pub fn validate_part_number(part_number: u32) -> Result<()> {
    if !(1..=MAX_PARTS).contains(&part_number) {
        return Err(Error::InvalidArgument(format!("part numbers must be between 1 and {MAX_PARTS}")));
    }
    Ok(())
}

mod keys;
mod scrub;

pub use self::scrub::{CorruptBlock, ScrubReport};

#[cfg(test)]
mod tests;
