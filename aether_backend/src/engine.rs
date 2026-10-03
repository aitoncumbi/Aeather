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
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
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
use crate::crypto::{CustomerKey, DataKey, FRAME, FrameCipher, Keyring, SealedKey, SseCustomer, TAG};
pub use crate::error::CustomerKeyProblem;
use crate::error::{BoxError, Error, Result};
use crate::meta::{
    BlockRef, BucketRecord, BucketUsage, Command, CorsRule, DeleteCondition, ListPage, ListQuery, MetaStore, Millis,
    ObjectAttrs, ObjectData, ObjectRecord, Outcome, PartRecord, PartsPage, Quota, UploadListQuery, UploadPage,
    UploadRecord, WriteCondition, usage_after,
};
pub use crate::meta::{Deleted, VersionRef, Versioning, VersionsPage, VersionsQuery};

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
    /// Master keys for encryption at rest. Set: every new object is encrypted.
    pub keyring: Option<Arc<Keyring>>,
    /// How often lifecycle rules are applied. Zero disables the job.
    pub lifecycle_interval: Duration,
    /// The length of a lifecycle "day" (shorter only for testing).
    pub lifecycle_day: Duration,
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
            keyring: None,
            lifecycle_interval: Duration::from_secs(60 * 60),
            lifecycle_day: Duration::from_secs(24 * 60 * 60),
        }
    }
}

pub struct Engine {
    meta: Arc<MetaStore>,
    writer: writer::Writer,
    blocks: Arc<BlockStore>,
    keys: Arc<KeyCache>,
    stats: Arc<EngineStats>,
    scrub: scrub::ScrubState,
    /// Running backups: garbage collection waits while there are any.
    backups: AtomicUsize,
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
    /// Objects, versions and delete markers removed by lifecycle rules.
    pub lifecycle_expired: AtomicU64,
    /// Metadata write transactions committed; each carries one or more writes.
    pub meta_commits: AtomicU64,
    pub meta_writes: AtomicU64,
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
        let missing: Vec<String> = meta
            .sealing_key_ids()?
            .into_iter()
            .filter(|id| !config.keyring.as_ref().is_some_and(|ring| ring.has(id)))
            .collect();
        if !missing.is_empty() {
            return Err(Error::InvalidArgument(format!(
                "stored data is encrypted with master key(s) {} (fingerprints), which encryption.master_key and \
                 encryption.previous_keys do not include; without them it cannot be read",
                missing.join(", ")
            )));
        }
        let keys = KeyCache::default();
        KeyCache::load(&meta, &keys, config.keyring.as_deref())?;
        let meta = Arc::new(meta);
        let stats = Arc::new(EngineStats::default());
        let engine = Self {
            writer: writer::Writer::start(Arc::clone(&meta), Arc::clone(&stats)),
            meta,
            blocks: Arc::new(blocks),
            keys: Arc::new(keys),
            stats,
            scrub: scrub::ScrubState::default(),
            backups: AtomicUsize::new(0),
            config,
        };
        let sealed = engine.seal_key_secrets().await?;
        if sealed > 0 {
            tracing::info!(keys = sealed, "sealed access key secrets under the current master key");
        }
        Ok(engine)
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

    /// Blocks no object uses any more, waiting for garbage collection.
    pub async fn gc_pending(&self) -> Result<u64> {
        self.read(|meta| meta.gc_queue_len()).await
    }

    pub async fn get_bucket(&self, name: &str) -> Result<BucketRecord> {
        let name = name.to_owned();
        self.read(move |meta| meta.get_bucket(&name)?.ok_or(Error::NoSuchBucket)).await
    }

    pub async fn list_buckets(&self) -> Result<Vec<BucketRecord>> {
        self.read(|meta| meta.list_buckets()).await
    }

    /// Replaces the bucket's quota. Usage already over a new limit stays;
    /// only writes that would grow it further are refused.
    pub async fn set_bucket_quota(&self, name: &str, quota: Quota) -> Result<BucketRecord> {
        match self.apply(Command::SetBucketQuota { bucket: name.to_owned(), quota }).await? {
            Outcome::Bucket(bucket) => Ok(bucket),
            other => unreachable!("SetBucketQuota returned {other:?}"),
        }
    }

    /// Replaces an object's tags; an empty map removes them.
    pub async fn set_object_tags(
        &self,
        bucket: &str,
        key: &str,
        version: Option<VersionRef>,
        tags: BTreeMap<String, String>,
    ) -> Result<()> {
        let command = Command::SetObjectTags { bucket: bucket.to_owned(), key: key.to_owned(), version, tags };
        self.apply(command).await?;
        Ok(())
    }

    /// Replaces the bucket's CORS rules; an empty list removes them.
    pub async fn set_bucket_cors(&self, name: &str, rules: Vec<CorsRule>) -> Result<()> {
        self.apply(Command::SetBucketCors { bucket: name.to_owned(), rules }).await?;
        Ok(())
    }

    // ---- writes ----

    /// Cheap early check so a doomed upload is rejected before its body is
    /// read: the condition, and the quota when the size is known. The
    /// authoritative checks happen again inside the commit.
    pub async fn precheck_put(
        &self,
        bucket: &str,
        key: &str,
        condition: &WriteCondition,
        size: Option<u64>,
    ) -> Result<()> {
        validate_key(key)?;
        let (bucket, key, condition) = (bucket.to_owned(), key.to_owned(), condition.clone());
        self.read(move |meta| {
            let (record, used) = meta.bucket_usage(&bucket)?;
            let existing = match meta.get_object(&bucket, &key) {
                Ok(record) => Some(record),
                Err(Error::NoSuchKey) => None,
                Err(e) => return Err(e),
            };
            condition.check(existing.as_ref())?;
            if let Some(size) = size {
                record.quota.check(used, usage_after(used, Some(size), existing.map(|r| r.size)))?;
            }
            Ok(())
        })
        .await
    }

    /// Early quota check for one part of a multipart upload: would the object,
    /// with this part and the parts already uploaded, fit? Refuses an upload
    /// that cannot fit at the first part past the limit, not at completion,
    /// which stays the authoritative check.
    pub async fn precheck_part(
        &self,
        bucket: &str,
        key: &str,
        upload_id: Uuid,
        part_number: u32,
        size: Option<u64>,
    ) -> Result<()> {
        let (bucket, key) = (bucket.to_owned(), key.to_owned());
        self.read(move |meta| {
            let (record, used) = meta.bucket_usage(&bucket)?;
            meta.get_upload(&bucket, &key, &upload_id)?;
            if record.quota == Quota::default() {
                return Ok(());
            }
            let existing = match meta.get_object(&bucket, &key) {
                Ok(object) => Some(object.size),
                Err(Error::NoSuchKey) => None,
                Err(e) => return Err(e),
            };
            let total = meta.upload_bytes(&upload_id, part_number)? + size.unwrap_or(0);
            record.quota.check(used, usage_after(used, Some(total), existing))
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
        self.stage_with_customer_key(body, None).await
    }

    /// Like `stage`, encrypted with the client's key instead (SSE-C) if given.
    pub async fn stage_with_customer_key<S, E>(&self, body: S, customer: Option<&CustomerKey>) -> Result<StagedObject>
    where
        S: Stream<Item = Result<Bytes, E>> + Unpin,
        E: Into<BoxError>,
    {
        let protection = match customer {
            Some(customer) => Protection::Customer(customer.data_key().clone(), customer.check()?),
            None => match self.new_data_key()? {
                Some((key, sealed)) => Protection::Sealed(key, sealed),
                None => Protection::None,
            },
        };
        self.stage_with(body, self.config.inline_threshold, protection).await
    }

    /// A new data key, if encryption at rest is configured.
    fn new_data_key(&self) -> Result<Option<(DataKey, SealedKey)>> {
        self.config.keyring.as_ref().map(|ring| ring.new_data_key()).transpose()
    }

    /// The data key of an encrypted object or upload.
    fn open_key(&self, sealed: &SealedKey) -> Result<DataKey> {
        let ring = self.config.keyring.as_ref().ok_or_else(|| {
            Error::Corrupt(format!("data is encrypted with master key {}, but none is configured", sealed.key_id))
        })?;
        ring.open(sealed)
    }

    /// Like [`stage`](Self::stage), for a part of a multipart upload. Parts are
    /// never inlined, since the completed object is a sequence of blocks.
    pub async fn stage_part<S, E>(&self, body: S, upload: &UploadRecord) -> Result<StagedObject>
    where
        S: Stream<Item = Result<Bytes, E>> + Unpin,
        E: Into<BoxError>,
    {
        self.stage_part_with(body, upload, None).await
    }

    /// Parts of an SSE-C upload need the upload's customer key.
    pub async fn stage_part_with<S, E>(
        &self,
        body: S,
        upload: &UploadRecord,
        customer: Option<&CustomerKey>,
    ) -> Result<StagedObject>
    where
        S: Stream<Item = Result<Bytes, E>> + Unpin,
        E: Into<BoxError>,
    {
        let protection = match (&upload.sse_customer, customer) {
            (Some(stored), Some(customer)) if stored.matches(customer) => {
                Protection::Customer(customer.data_key().clone(), stored.clone())
            }
            (Some(_), Some(_)) => return Err(Error::CustomerKey(CustomerKeyProblem::Mismatch)),
            (Some(_), None) => return Err(Error::CustomerKey(CustomerKeyProblem::Missing)),
            (None, Some(_)) => return Err(Error::CustomerKey(CustomerKeyProblem::NotEncrypted)),
            (None, None) => match &upload.sse {
                Some(sealed) => Protection::Sealed(self.open_key(sealed)?, sealed.clone()),
                None => Protection::None,
            },
        };
        self.stage_with(body, 0, protection).await
    }

    async fn stage_with<S, E>(
        &self,
        mut body: S,
        inline_threshold: usize,
        protection: Protection,
    ) -> Result<StagedObject>
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
            sse: None,
            sse_customer: None,
        };
        let data_key = match protection {
            Protection::None => None,
            Protection::Sealed(key, sealed) => {
                staged.sse = Some(sealed);
                Some(key)
            }
            Protection::Customer(key, stored) => {
                staged.sse_customer = Some(stored);
                Some(key)
            }
        };
        let mut md5 = Md5::new();
        let mut inline = Some(Vec::new());
        let mut sink = BlockSink { engine: self, writer: None, blocks: Vec::new(), key: data_key.as_ref() };

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
        staged.data = match (inline, &data_key) {
            // Inline data uses the nil id: the data key is already unique to the object.
            (Some(buf), Some(data_key)) => ObjectData::Inline(data_key.for_block(&Uuid::nil()).seal_all(&buf)),
            (Some(buf), None) => ObjectData::Inline(buf),
            (None, _) => ObjectData::Blocks(sink.blocks),
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
            versioned: false,
            delete_marker: false,
            size: staged.size,
            etag: staged.etag(),
            last_modified: now,
            attrs,
            data: staged.data.clone(),
            part_sizes: Vec::new(),
            upload_id: None,
            sse: staged.sse.clone(),
            sse_customer: staged.sse_customer.clone(),
            part_checksums: Vec::new(),
        };
        let command = Command::PutObject {
            bucket: bucket.to_owned(),
            key: key.to_owned(),
            record: record.clone(),
            condition,
            gc_due_at: now + self.gc_grace_millis(),
        };
        match self.commit_staged(staged, command).await? {
            Outcome::Written { record, .. } => Ok(record),
            other => unreachable!("a write returned {other:?}"),
        }
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
        self.precheck_put(bucket, key, &condition, None).await?;
        let staged = self.stage(body).await?;
        self.commit_put(bucket, key, staged, attrs, condition).await
    }

    /// Deleting a key that does not exist succeeds, as in S3. In a versioned
    /// bucket this adds a delete marker.
    pub async fn delete_object(&self, bucket: &str, key: &str) -> Result<Deleted> {
        self.delete_object_if(bucket, key, None, DeleteCondition::default()).await
    }

    /// Deletes the key if it holds an object meeting `condition`; fails with
    /// `PreconditionFailed` if it holds another one.
    /// With `version`, permanently removes that version instead.
    pub async fn delete_object_if(
        &self,
        bucket: &str,
        key: &str,
        version: Option<VersionRef>,
        condition: DeleteCondition,
    ) -> Result<Deleted> {
        let now = now_millis();
        let command = Command::DeleteObject {
            bucket: bucket.to_owned(),
            key: key.to_owned(),
            version,
            condition,
            marker_id: Uuid::now_v7(),
            now,
            gc_due_at: now + self.gc_grace_millis(),
        };
        match self.apply(command).await? {
            Outcome::Deleted(deleted) => Ok(deleted),
            other => unreachable!("DeleteObject returned {other:?}"),
        }
    }

    /// Deletes up to 1000 keys in one commit. Missing keys are not an error.
    pub async fn delete_objects(&self, bucket: &str, keys: Vec<String>) -> Result<()> {
        let objects = keys.into_iter().map(|key| (key, None, DeleteCondition::default())).collect();
        self.delete_objects_if(bucket, objects).await.map(drop)
    }

    /// Like `delete_objects`, with an optional version and a condition per
    /// key. Each entry gets what was deleted, or `None` if its precondition failed.
    pub async fn delete_objects_if(
        &self,
        bucket: &str,
        objects: Vec<(String, Option<VersionRef>, DeleteCondition)>,
    ) -> Result<Vec<Option<Deleted>>> {
        let now = now_millis();
        let objects = objects.into_iter().map(|(key, version, cond)| (key, version, cond, Uuid::now_v7())).collect();
        let command =
            Command::DeleteObjects { bucket: bucket.to_owned(), objects, now, gc_due_at: now + self.gc_grace_millis() };
        match self.apply(command).await? {
            Outcome::DeletedMany(results) => Ok(results),
            other => unreachable!("DeleteObjects returned {other:?}"),
        }
    }

    /// Replaces the bucket's lifecycle rules; an empty list removes them.
    pub async fn set_bucket_lifecycle(&self, name: &str, rules: Vec<crate::meta::LifecycleRule>) -> Result<()> {
        self.apply(Command::SetBucketLifecycle { bucket: name.to_owned(), rules }).await?;
        Ok(())
    }

    /// Enables or suspends versioning (it cannot be turned off once enabled).
    pub async fn set_bucket_versioning(&self, bucket: &str, versioning: Versioning) -> Result<()> {
        self.apply(Command::SetBucketVersioning { bucket: bucket.to_owned(), versioning }).await?;
        Ok(())
    }

    /// One page of every version and delete marker (ListObjectVersions).
    pub async fn list_versions(&self, bucket: &str, query: VersionsQuery) -> Result<VersionsPage> {
        let bucket = bucket.to_owned();
        self.read(move |meta| meta.list_versions(&bucket, &query)).await
    }

    /// A version of `key`: the current one (possibly a delete marker) without
    /// `version`. Fails with `NoSuchKey` or `NoSuchVersion`.
    pub async fn get_version(&self, bucket: &str, key: &str, version: Option<VersionRef>) -> Result<ObjectRecord> {
        let (bucket, key) = (bucket.to_owned(), key.to_owned());
        self.read(move |meta| meta.get_version(&bucket, &key, version)).await
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
            versioned: false,
            delete_marker: false,
            size: source.size,
            etag: source.etag.clone(),
            last_modified: now,
            attrs,
            data: source.data.clone(),
            part_sizes: source.part_sizes.clone(),
            upload_id: None,
            // The copy shares the source's blocks, so it shares their key too.
            sse: source.sse.clone(),
            sse_customer: source.sse_customer.clone(),
            part_checksums: source.part_checksums.clone(),
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
        match self.apply(command).await? {
            Outcome::Written { record, .. } => Ok(record),
            other => unreachable!("a write returned {other:?}"),
        }
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
        self.create_upload_with(bucket, key, attrs, checksum_algorithm, checksum_type, None).await
    }

    /// With a customer key (SSE-C), every part must be sent with that key.
    pub async fn create_upload_with(
        &self,
        bucket: &str,
        key: &str,
        attrs: ObjectAttrs,
        checksum_algorithm: Option<String>,
        checksum_type: Option<String>,
        customer: Option<&CustomerKey>,
    ) -> Result<UploadRecord> {
        validate_key(key)?;
        let upload = UploadRecord {
            upload_id: Uuid::now_v7(),
            key: key.to_owned(),
            initiated: now_millis(),
            attrs,
            checksum_algorithm,
            checksum_type,
            sse: match customer {
                Some(_) => None,
                None => self.new_data_key()?.map(|(_, sealed)| sealed),
            },
            sse_customer: customer.map(CustomerKey::check).transpose()?,
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
            versioned: false,
            delete_marker: false,
            size: completion.size(),
            etag: completion.etag()?,
            last_modified: now,
            attrs,
            data: ObjectData::Blocks(completion.parts.iter().flat_map(|(_, p)| p.blocks.iter().copied()).collect()),
            part_sizes: completion.parts.iter().map(|(_, p)| p.size).collect(),
            upload_id: Some(completion.upload.upload_id),
            sse: completion.upload.sse.clone(),
            sse_customer: completion.upload.sse_customer.clone(),
            part_checksums: completion.parts.iter().map(|(_, p)| p.checksums.clone()).collect(),
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
        match self.apply(command).await? {
            Outcome::Written { record, .. } => Ok(record),
            other => unreachable!("a write returned {other:?}"),
        }
    }

    /// The object at `key` if `upload_id` created it: lets a retried
    /// CompleteMultipartUpload succeed after the first attempt did.
    pub async fn completed_upload(&self, bucket: &str, key: &str, upload_id: Uuid) -> Result<Option<ObjectRecord>> {
        match self.get_object(bucket, key).await {
            Ok(record) => Ok((record.upload_id == Some(upload_id)).then_some(record)),
            Err(Error::NoSuchKey) => Ok(None),
            Err(e) => Err(e),
        }
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
        self.read_object_with(record, range, None)
    }

    /// Reads an SSE-C object with its customer key (callers check the key first).
    pub fn read_object_with(
        &self,
        record: &ObjectRecord,
        range: Range<u64>,
        customer: Option<&CustomerKey>,
    ) -> BoxStream<'static, io::Result<Bytes>> {
        let key = match (&record.sse_customer, customer) {
            (Some(stored), Some(customer)) if stored.matches(customer) => Ok(Some(customer.data_key().clone())),
            (Some(_), _) => Err(Error::CustomerKey(CustomerKeyProblem::Missing)),
            (None, _) => record.sse.as_ref().map(|sealed| self.open_key(sealed)).transpose(),
        };
        let key = match key {
            Ok(key) => key,
            Err(e) => return stream::once(async move { Err(io::Error::other(e.to_string())) }).boxed(),
        };
        match &record.data {
            ObjectData::Inline(data) => {
                let plain = match &key {
                    Some(key) => match key.for_block(&Uuid::nil()).open_all(data) {
                        Ok(plain) => plain,
                        Err(e) => return stream::once(async move { Err(e) }).boxed(),
                    },
                    None => data.clone(),
                };
                let bytes = Bytes::copy_from_slice(&plain[range.start as usize..range.end as usize]);
                count(&self.stats.bytes_sent, bytes.len() as u64);
                stream::once(async move { Ok(bytes) }).boxed()
            }
            ObjectData::Blocks(blocks) => {
                let state = ReadState {
                    store: Arc::clone(&self.blocks),
                    stats: Arc::clone(&self.stats),
                    segments: segments(blocks, range).into_iter(),
                    key,
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
        // A backup copies blocks after snapshotting the metadata: keep them all.
        while !self.backup_running() {
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
        if !self.config.lifecycle_interval.is_zero() {
            handles.push(tokio::spawn(lifecycle::run_periodically(Arc::clone(self), self.config.lifecycle_interval)));
        }
        BackgroundTasks { handles }
    }

    // ---- helpers ----

    /// Every metadata write goes through the writer, which commits waiting
    /// writes together (group commit).
    async fn apply(&self, command: Command) -> Result<Outcome> {
        self.writer.apply(command, None).await
    }

    /// Applies a command that references a staged body's blocks. The staged
    /// body moves into the blocking task, so it outlives the commit even if
    /// this future is cancelled (the client disconnects); otherwise a cancelled
    /// request could delete the blocks of a committed object.
    async fn commit_staged(&self, staged: StagedObject, command: Command) -> Result<Outcome> {
        self.writer.apply(command, Some(staged)).await
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
    sse: Option<SealedKey>,
    sse_customer: Option<SseCustomer>,
}

/// How staged data is encrypted: not at all, under a sealed data key
/// (SSE-S3), or under the client's key (SSE-C).
enum Protection {
    None,
    Sealed(DataKey, SealedKey),
    Customer(DataKey, SseCustomer),
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
    key: Option<&'a DataKey>,
}

impl BlockSink<'_> {
    async fn write(&mut self, mut data: &[u8], staged: &mut StagedObject) -> Result<()> {
        let block_size = self.engine.config.block_size;
        while !data.is_empty() {
            if self.writer.is_none() {
                let writer = self.engine.blocks.create(self.key).await?;
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

/// `(block, offset in block, length)` for each block overlapping `range`,
/// in plaintext bytes.
fn segments(blocks: &[BlockRef], range: Range<u64>) -> Vec<(BlockRef, u64, u64)> {
    let mut out = Vec::new();
    let mut block_start = 0;
    for block in blocks {
        let block_end = block_start + block.len;
        if block_end > range.start && block_start < range.end {
            let start = range.start.max(block_start);
            let end = range.end.min(block_end);
            out.push((*block, start - block_start, end - start));
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
    segments: std::vec::IntoIter<(BlockRef, u64, u64)>,
    key: Option<DataKey>,
    current: Option<Segment>,
}

enum Segment {
    Plain {
        file: tokio::io::Take<tokio::fs::File>,
        remaining: u64,
    },
    /// Encrypted: whole frames are read and opened, then trimmed to the range.
    Sealed {
        file: tokio::io::Take<tokio::fs::File>,
        cipher: Box<FrameCipher>,
        frame: u64,
        skip: usize,
        remaining: u64,
    },
}

impl ReadState {
    async fn open(&self, (block, offset, len): (BlockRef, u64, u64)) -> io::Result<Segment> {
        let Some(key) = &self.key else {
            return Ok(Segment::Plain { file: self.store.open_range(&block.id, offset, len).await?, remaining: len });
        };
        let (first, last) = (offset / FRAME, (offset + len - 1) / FRAME);
        let start = first * (FRAME + TAG);
        let end = ((last + 1) * (FRAME + TAG)).min(block.stored_len());
        Ok(Segment::Sealed {
            file: self.store.open_range(&block.id, start, end - start).await?,
            cipher: Box::new(key.for_block(&block.id)),
            frame: first,
            skip: (offset % FRAME) as usize,
            remaining: len,
        })
    }

    async fn next(mut self) -> io::Result<Option<(Bytes, Self)>> {
        loop {
            if self.current.is_none() {
                let Some(segment) = self.segments.next() else {
                    return Ok(None);
                };
                self.current = Some(self.open(segment).await?);
            }
            let chunk = match self.current.as_mut().expect("set above") {
                Segment::Plain { remaining: 0, .. } | Segment::Sealed { remaining: 0, .. } => None,
                Segment::Plain { file, remaining } => {
                    let mut buf = BytesMut::with_capacity(READ_CHUNK.min(*remaining) as usize);
                    let n = file.read_buf(&mut buf).await?;
                    if n == 0 {
                        return Err(short_block());
                    }
                    *remaining -= n as u64;
                    Some(buf.freeze())
                }
                Segment::Sealed { file, cipher, frame, skip, remaining } => {
                    let mut buf = Vec::with_capacity((FRAME + TAG) as usize);
                    file.take(FRAME + TAG).read_to_end(&mut buf).await?;
                    if buf.len() as u64 <= TAG {
                        return Err(short_block());
                    }
                    let n = cipher.open(*frame, &mut buf)?;
                    let take = (n - *skip).min(*remaining as usize);
                    let out = Bytes::copy_from_slice(&buf[*skip..*skip + take]);
                    *frame += 1;
                    *skip = 0;
                    *remaining -= take as u64;
                    Some(out)
                }
            };
            match chunk {
                Some(bytes) => {
                    count(&self.stats.bytes_sent, bytes.len() as u64);
                    return Ok(Some((bytes, self)));
                }
                None => self.current = None,
            }
        }
    }
}

fn short_block() -> io::Error {
    io::Error::new(io::ErrorKind::UnexpectedEof, "block file is shorter than its metadata")
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

/// S3 bucket naming rules for new buckets: DNS-compatible, so that
/// virtual-hosted-style addressing works. Every dot-separated label starts
/// and ends with a letter or digit.
pub fn validate_bucket_name(name: &str) -> Result<()> {
    let label = |l: &str| {
        let b = l.as_bytes();
        !b.is_empty() && b[0].is_ascii_alphanumeric() && b[b.len() - 1].is_ascii_alphanumeric()
    };
    let valid = (3..=63).contains(&name.len())
        && name.bytes().all(|b| b.is_ascii_lowercase() || b.is_ascii_digit() || b == b'-' || b == b'.')
        && name.split('.').all(label)
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

mod backup;
mod keys;
mod lifecycle;
mod scrub;
mod writer;

pub use self::backup::BackupReport;
pub use self::lifecycle::LifecycleReport;

pub use self::scrub::{CorruptBlock, ScrubReport};

#[cfg(test)]
mod tests;
