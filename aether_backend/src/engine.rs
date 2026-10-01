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

use std::io;
use std::net::Ipv4Addr;
use std::ops::Range;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use bytes::{Bytes, BytesMut};
use futures::stream::{self, BoxStream};
use futures::{Stream, StreamExt};
use md5::{Digest, Md5};
use tokio::io::AsyncReadExt;
use tokio::task::JoinHandle;
use tracing::{info, warn};
use uuid::Uuid;

use crate::blocks::{BlockStore, BlockWriter};
use crate::error::{BoxError, Error, Result};
use crate::meta::{
    BlockRef, BucketRecord, Command, ListPage, ListQuery, MetaStore, Millis, ObjectAttrs, ObjectData, ObjectRecord,
    Outcome, WriteCondition,
};

/// S3's limit for one ListObjects page.
pub const MAX_LIST_KEYS: usize = 1000;
/// S3's limit on object key length, in bytes.
pub const MAX_KEY_LEN: usize = 1024;

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
        }
    }
}

pub struct Engine {
    meta: Arc<MetaStore>,
    blocks: Arc<BlockStore>,
    config: EngineConfig,
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
        Ok(Self { meta: Arc::new(meta), blocks: Arc::new(blocks), config })
    }

    pub fn config(&self) -> &EngineConfig {
        &self.config
    }

    // ---- buckets ----

    pub async fn create_bucket(&self, name: &str) -> Result<BucketRecord> {
        validate_bucket_name(name)?;
        let command = Command::CreateBucket { name: name.to_owned(), id: Uuid::new_v4(), created_at: now_millis() };
        match self.apply(command).await? {
            Outcome::Bucket(bucket) => Ok(bucket),
            other => unreachable!("CreateBucket returned {other:?}"),
        }
    }

    /// Fails with `BucketNotEmpty` unless the bucket has no objects.
    pub async fn delete_bucket(&self, name: &str) -> Result<()> {
        self.apply(Command::DeleteBucket { name: name.to_owned() }).await?;
        Ok(())
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
    pub async fn stage<S, E>(&self, mut body: S) -> Result<StagedObject>
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

        while let Some(chunk) = body.next().await {
            let chunk = chunk.map_err(|e| Error::Body(e.into()))?;
            md5.update(&chunk);
            staged.size += chunk.len() as u64;
            if let Some(buf) = &mut inline {
                if buf.len() + chunk.len() <= self.config.inline_threshold {
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
        };
        let command = Command::PutObject {
            bucket: bucket.to_owned(),
            key: key.to_owned(),
            record: record.clone(),
            condition,
            gc_due_at: now + self.gc_grace_millis(),
        };
        // The staged blocks move into the blocking task, so they outlive the
        // commit even if this future is cancelled (the client disconnects).
        // Otherwise a cancelled request could delete blocks of a committed object.
        let meta = Arc::clone(&self.meta);
        tokio::task::spawn_blocking(move || {
            let mut staged = staged;
            let result = meta.apply(command);
            staged.committed = result.is_ok();
            result
        })
        .await??;
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
                stream::once(async move { Ok(bytes) }).boxed()
            }
            ObjectData::Blocks(blocks) => {
                let state = ReadState {
                    store: Arc::clone(&self.blocks),
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
                    Ok(()) => removed += 1,
                    Err(e) if e.kind() == io::ErrorKind::NotFound => {}
                    Err(e) => return Err(e.into()),
                }
            }
        }
        Ok(removed)
    }

    /// Runs GC and the orphan sweeper until the returned handle is dropped.
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
                }
            })
        };
        BackgroundTasks { handles: vec![gc, sweeper] }
    }

    // ---- helpers ----

    async fn apply(&self, command: Command) -> Result<Outcome> {
        let meta = Arc::clone(&self.meta);
        tokio::task::spawn_blocking(move || meta.apply(command)).await?
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
            return Ok(Some((buf.freeze(), self)));
        }
    }
}

/// Aborts the background tasks when dropped.
pub struct BackgroundTasks {
    handles: Vec<JoinHandle<()>>,
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
    Ok(())
}

#[cfg(test)]
mod tests;
