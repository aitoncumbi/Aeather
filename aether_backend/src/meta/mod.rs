//! Metadata store backed by redb.
//!
//! Every mutation is a [`Command`] applied by [`MetaStore::apply`] in a single
//! write transaction. Commands carry all non-deterministic inputs (ids,
//! timestamps), so applying the same sequence on another machine produces the
//! same state. That is the seam where Raft replication plugs in for clustering.

mod codec;
mod keys;
mod types;
mod usage;

use std::ops::Bound;
use std::path::Path;

use redb::{Database, ReadableDatabase, ReadableTable, ReadableTableMetadata, TableDefinition};
use serde::{Deserialize, Serialize};
use uuid::Uuid;

pub use self::keys::prefix_successor;
use self::keys::{gc_key, object_key, parse_gc_key, parse_part_number, parse_upload_key, part_key, upload_key};
pub use self::types::*;
use crate::error::{Error, Result};

/// bucket name → `BucketRecord`
const BUCKETS: TableDefinition<&str, &[u8]> = TableDefinition::new("buckets");
/// bucket id ‖ object key → `ObjectRecord`
const OBJECTS: TableDefinition<&[u8], &[u8]> = TableDefinition::new("objects");
/// block id → number of objects referencing it. Entries at zero are waiting in `gc_queue`.
const BLOCKS: TableDefinition<&[u8], u64> = TableDefinition::new("blocks");
/// bucket id ‖ object key ‖ 0x00 ‖ upload id → `UploadRecord`
const UPLOADS: TableDefinition<&[u8], &[u8]> = TableDefinition::new("uploads");
/// upload id ‖ part number → `PartRecord`
const PARTS: TableDefinition<&[u8], &[u8]> = TableDefinition::new("parts");
/// due time ‖ block id → ()
const GC_QUEUE: TableDefinition<&[u8], ()> = TableDefinition::new("gc_queue");
/// access key id → `AccessKeyRecord`
const ACCESS_KEYS: TableDefinition<&str, &[u8]> = TableDefinition::new("access_keys");
/// bucket id → `BucketUsage`
const BUCKET_USAGE: TableDefinition<&[u8], &[u8]> = TableDefinition::new("bucket_usage");
/// name → value
const SYSTEM: TableDefinition<&str, &[u8]> = TableDefinition::new("system");

/// 1: initial layout. 2: adds `access_keys` and `bucket_usage`.
const SCHEMA_VERSION: u32 = 2;

// Commands and outcomes are short-lived; boxing the large variants buys nothing.
#[allow(clippy::large_enum_variant)]
#[derive(Clone, Debug, Serialize, Deserialize)]
pub enum Command {
    /// `owner`, if set, is the access key that created the bucket and gets full
    /// permissions on it.
    CreateBucket {
        name: String,
        id: Uuid,
        created_at: Millis,
        owner: Option<String>,
    },
    /// Fails if the bucket has objects. Aborts its in-progress uploads and
    /// removes every key's permissions on it.
    DeleteBucket {
        name: String,
        gc_due_at: Millis,
    },
    PutObject {
        bucket: String,
        key: String,
        record: ObjectRecord,
        condition: WriteCondition,
        /// When blocks released by an overwrite may be deleted.
        gc_due_at: Millis,
    },
    /// Fails with `PreconditionFailed` if the key exists and does not meet `condition`.
    DeleteObject {
        bucket: String,
        key: String,
        condition: DeleteCondition,
        gc_due_at: Millis,
    },
    /// Deletes many keys in one transaction (S3 DeleteObjects). Keys whose
    /// condition fails are left in place and listed in `Outcome::Failed`.
    DeleteObjects {
        bucket: String,
        objects: Vec<(String, DeleteCondition)>,
        gc_due_at: Millis,
    },
    CreateUpload {
        bucket: String,
        upload: UploadRecord,
    },
    /// Stores a part, replacing any earlier upload of the same part number.
    PutPart {
        bucket: String,
        key: String,
        upload_id: Uuid,
        part_number: u32,
        part: PartRecord,
        gc_due_at: Millis,
    },
    /// Turns an upload into an object. `record` was assembled from the parts
    /// named in `part_numbers`; if any of them changed since, the command fails
    /// with `Conflict` rather than reference blocks that may be gone.
    CompleteUpload {
        bucket: String,
        key: String,
        upload_id: Uuid,
        part_numbers: Vec<u32>,
        record: ObjectRecord,
        condition: WriteCondition,
        gc_due_at: Millis,
    },
    AbortUpload {
        bucket: String,
        key: String,
        upload_id: Uuid,
        gc_due_at: Millis,
    },
    /// Makes `dst` share the source's blocks. Fails with `Conflict` if the
    /// source is no longer the version `record` was built from.
    CopyObject {
        src_bucket: String,
        src_key: String,
        src_version: Uuid,
        dst_bucket: String,
        dst_key: String,
        record: ObjectRecord,
        condition: WriteCondition,
        gc_due_at: Millis,
    },
    /// Drop GC queue entries whose block files have been deleted.
    ForgetBlocks {
        entries: Vec<(Millis, Uuid)>,
    },
    /// Fails with `AccessKeyExists` if the id is taken, or `NoSuchBucket` if
    /// a granted bucket does not exist.
    CreateKey {
        key: AccessKeyRecord,
    },
    UpdateKey {
        access_key_id: String,
        update: KeyUpdate,
    },
    DeleteKey {
        access_key_id: String,
    },
    /// Sets a key's permissions on a bucket; an empty grant removes them.
    SetGrant {
        access_key_id: String,
        bucket: String,
        grant: BucketGrant,
    },
}

#[allow(clippy::large_enum_variant)]
#[derive(Clone, Debug)]
pub enum Outcome {
    Done,
    Bucket(BucketRecord),
    /// The object record that was replaced or deleted, if any.
    Object(Option<ObjectRecord>),
    Key(AccessKeyRecord),
    /// Indexes of the entries a batch command skipped because their
    /// precondition failed.
    Failed(Vec<usize>),
}

pub struct MetaStore {
    db: Database,
}

impl MetaStore {
    pub fn open(path: &Path) -> Result<Self> {
        let db = Database::create(path)?;
        let txn = db.begin_write()?;
        {
            txn.open_table(BUCKETS)?;
            txn.open_table(OBJECTS)?;
            txn.open_table(BLOCKS)?;
            txn.open_table(GC_QUEUE)?;
            txn.open_table(UPLOADS)?;
            txn.open_table(PARTS)?;
            txn.open_table(ACCESS_KEYS)?;
            txn.open_table(BUCKET_USAGE)?;
            let mut system = txn.open_table(SYSTEM)?;
            let stored = system.get("schema_version")?.map(|v| codec::decode::<u32>(v.value())).transpose()?;
            match stored {
                None | Some(SCHEMA_VERSION) => {}
                Some(1) => {
                    tracing::info!("migrating metadata schema 1 -> 2: computing bucket usage");
                    usage::rebuild(&txn)?;
                }
                Some(other) => {
                    return Err(Error::Corrupt(format!(
                        "metadata schema version {other} is not supported (expected {SCHEMA_VERSION})"
                    )));
                }
            }
            system.insert("schema_version", codec::encode(&SCHEMA_VERSION).as_slice())?;
        }
        txn.commit()?;
        Ok(Self { db })
    }

    pub fn apply(&self, command: Command) -> Result<Outcome> {
        let txn = self.db.begin_write()?;
        let outcome = match command {
            Command::CreateBucket { name, id, created_at, owner } => {
                let mut buckets = txn.open_table(BUCKETS)?;
                if buckets.get(name.as_str())?.is_some() {
                    return Err(Error::BucketAlreadyExists);
                }
                let record = BucketRecord { id, name, created_at };
                buckets.insert(record.name.as_str(), codec::encode(&record).as_slice())?;
                usage::adjust(&txn, &record.id, None, None)?;
                if let Some(owner) = owner {
                    let mut keys = txn.open_table(ACCESS_KEYS)?;
                    let mut key = load_key(&keys, &owner)?;
                    key.buckets.insert(record.name.clone(), BucketGrant::FULL);
                    keys.insert(owner.as_str(), codec::encode(&key).as_slice())?;
                }
                Outcome::Bucket(record)
            }
            Command::DeleteBucket { name, gc_due_at } => {
                let mut buckets = txn.open_table(BUCKETS)?;
                let bucket: BucketRecord = match buckets.get(name.as_str())? {
                    Some(v) => codec::decode(v.value())?,
                    None => return Err(Error::NoSuchBucket),
                };
                let objects = txn.open_table(OBJECTS)?;
                if objects.range::<&[u8]>(as_slice_bounds(&bucket_bounds(&bucket.id)))?.next().is_some() {
                    return Err(Error::BucketNotEmpty);
                }
                let uploads: Vec<Vec<u8>> = {
                    let table = txn.open_table(UPLOADS)?;
                    let range = table.range::<&[u8]>(as_slice_bounds(&bucket_bounds(&bucket.id)))?;
                    range.map(|entry| Ok(entry?.0.value().to_vec())).collect::<Result<_>>()?
                };
                for ukey in uploads {
                    let (_, upload_id) = parse_upload_key(&ukey[16..]).ok_or_else(bad_upload_key)?;
                    remove_upload(&txn, &ukey, &upload_id, &[], gc_due_at)?;
                }
                buckets.remove(name.as_str())?;
                usage::remove(&txn, &bucket.id)?;
                // Grants are by name: drop them so a future bucket with the same
                // name does not inherit them.
                let mut keys = txn.open_table(ACCESS_KEYS)?;
                let mut changed = Vec::new();
                for entry in keys.iter()? {
                    let mut key: AccessKeyRecord = codec::decode(entry?.1.value())?;
                    if key.buckets.remove(&name).is_some() {
                        changed.push(key);
                    }
                }
                for key in changed {
                    keys.insert(key.access_key_id.as_str(), codec::encode(&key).as_slice())?;
                }
                Outcome::Done
            }
            Command::PutObject { bucket, key, record, condition, gc_due_at } => {
                let bucket = load_bucket(&txn.open_table(BUCKETS)?, &bucket)?;
                let mut objects = txn.open_table(OBJECTS)?;
                let okey = object_key(&bucket.id, key.as_bytes());
                let previous: Option<ObjectRecord> =
                    objects.get(okey.as_slice())?.map(|v| codec::decode(v.value())).transpose()?;
                condition.check(previous.as_ref())?;
                objects.insert(okey.as_slice(), codec::encode(&record).as_slice())?;
                usage::adjust(&txn, &bucket.id, Some(&record), previous.as_ref())?;

                let mut blocks = txn.open_table(BLOCKS)?;
                let mut gc = txn.open_table(GC_QUEUE)?;
                for block in record.data.blocks() {
                    add_block_ref(&mut blocks, &block.id)?;
                }
                if let Some(prev) = &previous {
                    release_blocks(&mut blocks, &mut gc, prev.data.blocks(), gc_due_at)?;
                }
                Outcome::Object(previous)
            }
            Command::CreateUpload { bucket, upload } => {
                let bucket = load_bucket(&txn.open_table(BUCKETS)?, &bucket)?;
                let ukey = upload_key(&bucket.id, upload.key.as_bytes(), &upload.upload_id);
                txn.open_table(UPLOADS)?.insert(ukey.as_slice(), codec::encode(&upload).as_slice())?;
                Outcome::Done
            }
            Command::PutPart { bucket, key, upload_id, part_number, part, gc_due_at } => {
                let bucket = load_bucket(&txn.open_table(BUCKETS)?, &bucket)?;
                load_upload(&txn.open_table(UPLOADS)?, &bucket, &key, &upload_id)?;
                let mut parts = txn.open_table(PARTS)?;
                let pkey = part_key(&upload_id, part_number);
                let previous: Option<PartRecord> =
                    parts.get(pkey.as_slice())?.map(|v| codec::decode(v.value())).transpose()?;
                parts.insert(pkey.as_slice(), codec::encode(&part).as_slice())?;

                let mut blocks = txn.open_table(BLOCKS)?;
                let mut gc = txn.open_table(GC_QUEUE)?;
                for block in &part.blocks {
                    add_block_ref(&mut blocks, &block.id)?;
                }
                if let Some(prev) = &previous {
                    release_blocks(&mut blocks, &mut gc, &prev.blocks, gc_due_at)?;
                }
                Outcome::Done
            }
            Command::CompleteUpload { bucket, key, upload_id, part_numbers, record, condition, gc_due_at } => {
                let bucket = load_bucket(&txn.open_table(BUCKETS)?, &bucket)?;
                load_upload(&txn.open_table(UPLOADS)?, &bucket, &key, &upload_id)?;
                {
                    let parts = txn.open_table(PARTS)?;
                    let mut current = Vec::new();
                    for number in &part_numbers {
                        let part: PartRecord = match parts.get(part_key(&upload_id, *number).as_slice())? {
                            Some(v) => codec::decode(v.value())?,
                            None => return Err(Error::Conflict(format!("part {number} was removed"))),
                        };
                        current.extend(part.blocks);
                    }
                    if current != record.data.blocks() {
                        return Err(Error::Conflict("a part was replaced while the upload was completing".into()));
                    }
                }

                let mut objects = txn.open_table(OBJECTS)?;
                let okey = object_key(&bucket.id, key.as_bytes());
                let previous: Option<ObjectRecord> =
                    objects.get(okey.as_slice())?.map(|v| codec::decode(v.value())).transpose()?;
                condition.check(previous.as_ref())?;
                objects.insert(okey.as_slice(), codec::encode(&record).as_slice())?;
                drop(objects);
                usage::adjust(&txn, &bucket.id, Some(&record), previous.as_ref())?;

                // The object takes over the references held by the parts it uses.
                let ukey = upload_key(&bucket.id, key.as_bytes(), &upload_id);
                remove_upload(&txn, &ukey, &upload_id, &part_numbers, gc_due_at)?;
                if let Some(prev) = &previous {
                    let mut blocks = txn.open_table(BLOCKS)?;
                    let mut gc = txn.open_table(GC_QUEUE)?;
                    release_blocks(&mut blocks, &mut gc, prev.data.blocks(), gc_due_at)?;
                }
                Outcome::Object(previous)
            }
            Command::AbortUpload { bucket, key, upload_id, gc_due_at } => {
                let bucket = load_bucket(&txn.open_table(BUCKETS)?, &bucket)?;
                load_upload(&txn.open_table(UPLOADS)?, &bucket, &key, &upload_id)?;
                let ukey = upload_key(&bucket.id, key.as_bytes(), &upload_id);
                remove_upload(&txn, &ukey, &upload_id, &[], gc_due_at)?;
                Outcome::Done
            }
            Command::CopyObject {
                src_bucket,
                src_key,
                src_version,
                dst_bucket,
                dst_key,
                record,
                condition,
                gc_due_at,
            } => {
                let buckets = txn.open_table(BUCKETS)?;
                let src_bucket = load_bucket(&buckets, &src_bucket)?;
                let dst_bucket = load_bucket(&buckets, &dst_bucket)?;
                let mut objects = txn.open_table(OBJECTS)?;
                let source: ObjectRecord =
                    match objects.get(object_key(&src_bucket.id, src_key.as_bytes()).as_slice())? {
                        Some(v) => codec::decode(v.value())?,
                        None => return Err(Error::NoSuchKey),
                    };
                if source.version_id != src_version || source.data != record.data {
                    return Err(Error::Conflict("the source object changed during the copy".into()));
                }
                let okey = object_key(&dst_bucket.id, dst_key.as_bytes());
                let previous: Option<ObjectRecord> =
                    objects.get(okey.as_slice())?.map(|v| codec::decode(v.value())).transpose()?;
                condition.check(previous.as_ref())?;
                objects.insert(okey.as_slice(), codec::encode(&record).as_slice())?;
                usage::adjust(&txn, &dst_bucket.id, Some(&record), previous.as_ref())?;

                // Add references before releasing the previous object: when copying
                // an object onto itself they are the same blocks.
                let mut blocks = txn.open_table(BLOCKS)?;
                let mut gc = txn.open_table(GC_QUEUE)?;
                for block in record.data.blocks() {
                    add_block_ref(&mut blocks, &block.id)?;
                }
                if let Some(prev) = &previous {
                    release_blocks(&mut blocks, &mut gc, prev.data.blocks(), gc_due_at)?;
                }
                Outcome::Object(previous)
            }
            Command::DeleteObject { bucket, key, condition, gc_due_at } => {
                let bucket = load_bucket(&txn.open_table(BUCKETS)?, &bucket)?;
                Outcome::Object(delete_object(&txn, &bucket, &key, &condition, gc_due_at)?)
            }
            Command::DeleteObjects { bucket, objects, gc_due_at } => {
                let bucket = load_bucket(&txn.open_table(BUCKETS)?, &bucket)?;
                let mut failed = Vec::new();
                for (i, (key, condition)) in objects.iter().enumerate() {
                    match delete_object(&txn, &bucket, key, condition, gc_due_at) {
                        Ok(_) => {}
                        Err(Error::PreconditionFailed) => failed.push(i),
                        Err(e) => return Err(e),
                    }
                }
                Outcome::Failed(failed)
            }
            Command::CreateKey { key } => {
                let mut keys = txn.open_table(ACCESS_KEYS)?;
                if keys.get(key.access_key_id.as_str())?.is_some() {
                    return Err(Error::AccessKeyExists);
                }
                let buckets = txn.open_table(BUCKETS)?;
                for bucket in key.buckets.keys() {
                    load_bucket(&buckets, bucket)?;
                }
                keys.insert(key.access_key_id.as_str(), codec::encode(&key).as_slice())?;
                Outcome::Key(key)
            }
            Command::UpdateKey { access_key_id, update } => {
                let mut keys = txn.open_table(ACCESS_KEYS)?;
                let mut key = load_key(&keys, &access_key_id)?;
                let KeyUpdate { name, admin, can_create_buckets, disabled } = update;
                key.name = name.unwrap_or(key.name);
                key.admin = admin.unwrap_or(key.admin);
                key.can_create_buckets = can_create_buckets.unwrap_or(key.can_create_buckets);
                key.disabled = disabled.unwrap_or(key.disabled);
                keys.insert(access_key_id.as_str(), codec::encode(&key).as_slice())?;
                Outcome::Key(key)
            }
            Command::DeleteKey { access_key_id } => {
                if txn.open_table(ACCESS_KEYS)?.remove(access_key_id.as_str())?.is_none() {
                    return Err(Error::NoSuchAccessKey);
                }
                Outcome::Done
            }
            Command::SetGrant { access_key_id, bucket, grant } => {
                let mut keys = txn.open_table(ACCESS_KEYS)?;
                let mut key = load_key(&keys, &access_key_id)?;
                load_bucket(&txn.open_table(BUCKETS)?, &bucket)?;
                if grant.is_empty() {
                    key.buckets.remove(&bucket);
                } else {
                    key.buckets.insert(bucket, grant);
                }
                keys.insert(access_key_id.as_str(), codec::encode(&key).as_slice())?;
                Outcome::Key(key)
            }
            Command::ForgetBlocks { entries } => {
                let mut blocks = txn.open_table(BLOCKS)?;
                let mut gc = txn.open_table(GC_QUEUE)?;
                for (due_at, id) in entries {
                    gc.remove(gc_key(due_at, &id).as_slice())?;
                    let refs = blocks.get(id.as_bytes().as_slice())?.map(|v| v.value());
                    if refs == Some(0) {
                        blocks.remove(id.as_bytes().as_slice())?;
                    }
                }
                Outcome::Done
            }
        };
        txn.commit()?;
        Ok(outcome)
    }

    pub fn get_bucket(&self, name: &str) -> Result<Option<BucketRecord>> {
        let txn = self.db.begin_read()?;
        let buckets = txn.open_table(BUCKETS)?;
        buckets.get(name)?.map(|v| codec::decode(v.value())).transpose()
    }

    pub fn list_buckets(&self) -> Result<Vec<BucketRecord>> {
        let txn = self.db.begin_read()?;
        let buckets = txn.open_table(BUCKETS)?;
        buckets.iter()?.map(|entry| codec::decode(entry?.1.value())).collect()
    }

    /// Fails with `NoSuchBucket` or `NoSuchKey`.
    pub fn get_object(&self, bucket: &str, key: &str) -> Result<ObjectRecord> {
        let txn = self.db.begin_read()?;
        let bucket = load_bucket(&txn.open_table(BUCKETS)?, bucket)?;
        let objects = txn.open_table(OBJECTS)?;
        match objects.get(object_key(&bucket.id, key.as_bytes()).as_slice())? {
            Some(v) => codec::decode(v.value()),
            None => Err(Error::NoSuchKey),
        }
    }

    /// One page of a bucket listing, read from a single snapshot.
    ///
    /// Common prefixes count toward `max_keys`, as in S3. When a common prefix
    /// is found the scan jumps past every key under it.
    pub fn list_objects(&self, bucket: &str, query: &ListQuery) -> Result<ListPage> {
        let txn = self.db.begin_read()?;
        let bucket = load_bucket(&txn.open_table(BUCKETS)?, bucket)?;
        let objects = txn.open_table(OBJECTS)?;

        let base = bucket.id.as_bytes();
        let prefix = query.prefix.as_bytes();
        let delimiter = query.delimiter.as_deref().filter(|d| !d.is_empty()).map(str::as_bytes);

        let upper = match prefix_successor(&object_key(&bucket.id, prefix)) {
            Some(end) => Bound::Excluded(end),
            None => Bound::Unbounded,
        };
        let mut lower = match &query.start {
            Some(start) if start.as_slice() > prefix => start.clone(),
            _ => prefix.to_vec(),
        };

        let mut page = ListPage::default();
        let mut count = 0;
        'scan: loop {
            let range = (Bound::Included(object_key(&bucket.id, &lower)), upper.clone());
            let mut restart_at = None;
            for entry in objects.range::<&[u8]>(as_slice_bounds(&range))? {
                let (k, v) = entry?;
                let key = &k.value()[base.len()..];
                if count == query.max_keys {
                    page.next_start = Some(key.to_vec());
                    break 'scan;
                }
                if let Some(delim) = delimiter
                    && let Some(pos) = memchr::memmem::find(&key[prefix.len()..], delim)
                {
                    let common = &key[..prefix.len() + pos + delim.len()];
                    page.common_prefixes.push(utf8(common)?);
                    count += 1;
                    match prefix_successor(common) {
                        Some(next) => {
                            restart_at = Some(next);
                            break;
                        }
                        None => break 'scan,
                    }
                }
                let record: ObjectRecord = codec::decode(v.value())?;
                page.objects.push(ObjectSummary {
                    key: utf8(key)?,
                    size: record.size,
                    etag: record.etag,
                    last_modified: record.last_modified,
                });
                count += 1;
            }
            match restart_at {
                Some(next) => lower = next,
                None => break,
            }
        }
        Ok(page)
    }

    pub fn get_key(&self, access_key_id: &str) -> Result<Option<AccessKeyRecord>> {
        let txn = self.db.begin_read()?;
        let keys = txn.open_table(ACCESS_KEYS)?;
        keys.get(access_key_id)?.map(|v| codec::decode(v.value())).transpose()
    }

    pub fn list_keys(&self) -> Result<Vec<AccessKeyRecord>> {
        let txn = self.db.begin_read()?;
        let keys = txn.open_table(ACCESS_KEYS)?;
        keys.iter()?.map(|entry| codec::decode(entry?.1.value())).collect()
    }

    /// Every bucket with its usage, in name order.
    pub fn list_bucket_usage(&self) -> Result<Vec<(BucketRecord, BucketUsage)>> {
        let txn = self.db.begin_read()?;
        let usage = txn.open_table(BUCKET_USAGE)?;
        let mut out = Vec::new();
        for entry in txn.open_table(BUCKETS)?.iter()? {
            let bucket: BucketRecord = codec::decode(entry?.1.value())?;
            let used = usage::get(&usage, &bucket.id)?;
            out.push((bucket, used));
        }
        Ok(out)
    }

    pub fn bucket_usage(&self, bucket: &str) -> Result<(BucketRecord, BucketUsage)> {
        let txn = self.db.begin_read()?;
        let bucket = load_bucket(&txn.open_table(BUCKETS)?, bucket)?;
        let used = usage::get(&txn.open_table(BUCKET_USAGE)?, &bucket.id)?;
        Ok((bucket, used))
    }

    /// Fails with `NoSuchBucket` or `NoSuchUpload`.
    pub fn get_upload(&self, bucket: &str, key: &str, upload_id: &Uuid) -> Result<UploadRecord> {
        let txn = self.db.begin_read()?;
        let bucket = load_bucket(&txn.open_table(BUCKETS)?, bucket)?;
        load_upload(&txn.open_table(UPLOADS)?, &bucket, key, upload_id)
    }

    /// Parts with numbers greater than `after`, in order, up to `max`.
    pub fn list_parts(&self, bucket: &str, key: &str, upload_id: &Uuid, after: u32, max: usize) -> Result<PartsPage> {
        let txn = self.db.begin_read()?;
        let bucket = load_bucket(&txn.open_table(BUCKETS)?, bucket)?;
        let upload = load_upload(&txn.open_table(UPLOADS)?, &bucket, key, upload_id)?;
        let parts = txn.open_table(PARTS)?;
        let Some(first) = after.checked_add(1) else {
            return Ok(PartsPage { upload, parts: Vec::new(), truncated: false });
        };
        let start = part_key(upload_id, first);
        let end = part_key(upload_id, u32::MAX);
        let mut out = Vec::new();
        let mut truncated = false;
        for entry in parts.range::<&[u8]>(start.as_slice()..=end.as_slice())? {
            let (k, v) = entry?;
            if out.len() == max {
                truncated = true;
                break;
            }
            let number = parse_part_number(k.value()).ok_or_else(|| Error::Corrupt("bad parts key".into()))?;
            out.push((number, codec::decode(v.value())?));
        }
        Ok(PartsPage { upload, parts: out, truncated })
    }

    /// In-progress uploads of a bucket, ordered by key and then by start time.
    /// Common prefixes count toward `max_uploads`.
    pub fn list_uploads(&self, bucket: &str, query: &UploadListQuery) -> Result<UploadPage> {
        let txn = self.db.begin_read()?;
        let bucket = load_bucket(&txn.open_table(BUCKETS)?, bucket)?;
        let uploads = txn.open_table(UPLOADS)?;

        let base = bucket.id.as_bytes();
        let prefix = query.prefix.as_bytes();
        let delimiter = query.delimiter.as_deref().filter(|d| !d.is_empty()).map(str::as_bytes);
        let upper = match prefix_successor(&object_key(&bucket.id, prefix)) {
            Some(end) => Bound::Excluded(end),
            None => Bound::Unbounded,
        };
        let marker_start = match (&query.key_marker, &query.upload_id_marker) {
            (Some(key), Some(id)) => {
                let mut start = upload_key(&bucket.id, key.as_bytes(), id)[16..].to_vec();
                start.push(0);
                Some(start)
            }
            (Some(key), None) => Some(
                common_prefix_of(key.as_bytes(), prefix, delimiter)
                    .filter(|common| common.len() == key.len())
                    .and_then(prefix_successor)
                    .unwrap_or_else(|| [key.as_bytes(), &[1u8][..]].concat()),
            ),
            (None, _) => None,
        };
        let mut lower = match marker_start {
            Some(start) if start.as_slice() > prefix => start,
            _ => prefix.to_vec(),
        };

        let mut page = UploadPage::default();
        let mut count = 0;
        'scan: loop {
            let range = (Bound::Included(object_key(&bucket.id, &lower)), upper.clone());
            let mut restart_at = None;
            for entry in uploads.range::<&[u8]>(as_slice_bounds(&range))? {
                let (k, v) = entry?;
                let (key, upload_id) = parse_upload_key(&k.value()[base.len()..]).ok_or_else(bad_upload_key)?;
                if count == query.max_uploads {
                    page.truncated = true;
                    break 'scan;
                }
                if let Some(common) = common_prefix_of(key, prefix, delimiter) {
                    page.common_prefixes.push(utf8(common)?);
                    page.next_key_marker = Some(utf8(common)?);
                    page.next_upload_id_marker = None;
                    count += 1;
                    match prefix_successor(common) {
                        Some(next) => {
                            restart_at = Some(next);
                            break;
                        }
                        None => break 'scan,
                    }
                }
                let upload: UploadRecord = codec::decode(v.value())?;
                page.next_key_marker = Some(upload.key.clone());
                page.next_upload_id_marker = Some(upload_id);
                page.uploads.push(upload);
                count += 1;
            }
            match restart_at {
                Some(next) => lower = next,
                None => break,
            }
        }
        if !page.truncated {
            page.next_key_marker = None;
            page.next_upload_id_marker = None;
        }
        Ok(page)
    }

    /// Uploads (bucket, key, id) started before `cutoff`, across all buckets.
    pub fn uploads_started_before(&self, cutoff: Millis) -> Result<Vec<(String, String, Uuid)>> {
        let txn = self.db.begin_read()?;
        let mut names = std::collections::HashMap::new();
        for entry in txn.open_table(BUCKETS)?.iter()? {
            let bucket: BucketRecord = codec::decode(entry?.1.value())?;
            names.insert(bucket.id, bucket.name);
        }
        let mut out = Vec::new();
        for entry in txn.open_table(UPLOADS)?.iter()? {
            let (k, v) = entry?;
            let upload: UploadRecord = codec::decode(v.value())?;
            let bucket_id = Uuid::from_slice(&k.value()[..16]).map_err(|_| bad_upload_key())?;
            if upload.initiated < cutoff
                && let Some(name) = names.get(&bucket_id)
            {
                out.push((name.clone(), upload.key, upload.upload_id));
            }
        }
        Ok(out)
    }

    /// Objects stored in blocks, after the raw `objects` key `after`, for the
    /// scrubber. Returns (raw key, record) pairs; inline objects are skipped.
    pub fn objects_with_blocks(&self, after: Option<&[u8]>, limit: usize) -> Result<Vec<(Vec<u8>, ObjectRecord)>> {
        let txn = self.db.begin_read()?;
        let objects = txn.open_table(OBJECTS)?;
        let start = match after {
            Some(after) => Bound::Excluded(after),
            None => Bound::Unbounded,
        };
        let mut out = Vec::new();
        for entry in objects.range::<&[u8]>((start, Bound::Unbounded))? {
            let (k, v) = entry?;
            let record: ObjectRecord = codec::decode(v.value())?;
            if matches!(record.data, ObjectData::Blocks(_)) {
                out.push((k.value().to_vec(), record));
                if out.len() == limit {
                    break;
                }
            }
        }
        Ok(out)
    }

    /// The object stored under a raw `objects` key.
    pub fn object_at(&self, raw_key: &[u8]) -> Result<Option<ObjectRecord>> {
        let txn = self.db.begin_read()?;
        let objects = txn.open_table(OBJECTS)?;
        objects.get(raw_key)?.map(|v| codec::decode(v.value())).transpose()
    }

    /// A node-local value in the `system` table. These are not commands: they
    /// describe this node (e.g. when it last scrubbed), not the shared state.
    pub fn system_value(&self, name: &str) -> Result<Option<i64>> {
        let txn = self.db.begin_read()?;
        let system = txn.open_table(SYSTEM)?;
        system.get(name)?.map(|v| codec::decode(v.value())).transpose()
    }

    pub fn set_system_value(&self, name: &str, value: i64) -> Result<()> {
        let txn = self.db.begin_write()?;
        txn.open_table(SYSTEM)?.insert(name, codec::encode(&value).as_slice())?;
        txn.commit()?;
        Ok(())
    }

    /// GC queue entries due at or before `now`, oldest first.
    pub fn due_gc(&self, now: Millis, limit: usize) -> Result<Vec<(Millis, Uuid)>> {
        let txn = self.db.begin_read()?;
        let gc = txn.open_table(GC_QUEUE)?;
        let end = gc_key(now.saturating_add(1), &Uuid::nil());
        let mut out = Vec::new();
        for entry in gc.range::<&[u8]>(..end.as_slice())?.take(limit) {
            let (k, _) = entry?;
            out.push(parse_gc_key(k.value()).ok_or_else(|| Error::Corrupt("bad gc_queue key".into()))?);
        }
        Ok(out)
    }

    /// Whether a block is referenced or waiting for GC (as opposed to orphaned).
    pub fn block_known(&self, id: &Uuid) -> Result<bool> {
        let txn = self.db.begin_read()?;
        let blocks = txn.open_table(BLOCKS)?;
        Ok(blocks.get(id.as_bytes().as_slice())?.is_some())
    }

    /// Puts the database back into its schema-1 state, to test the migration.
    #[cfg(test)]
    pub fn downgrade_to_v1(&self) -> Result<()> {
        let txn = self.db.begin_write()?;
        txn.open_table(BUCKET_USAGE)?.retain(|_, _| false)?;
        txn.open_table(SYSTEM)?.insert("schema_version", codec::encode(&1u32).as_slice())?;
        txn.commit()?;
        Ok(())
    }

    pub fn block_count(&self) -> Result<u64> {
        let txn = self.db.begin_read()?;
        Ok(txn.open_table(BLOCKS)?.len()?)
    }
}

fn load_bucket(buckets: &impl ReadableTable<&'static str, &'static [u8]>, name: &str) -> Result<BucketRecord> {
    match buckets.get(name)? {
        Some(v) => codec::decode(v.value()),
        None => Err(Error::NoSuchBucket),
    }
}

fn load_key(keys: &impl ReadableTable<&'static str, &'static [u8]>, access_key_id: &str) -> Result<AccessKeyRecord> {
    match keys.get(access_key_id)? {
        Some(v) => codec::decode(v.value()),
        None => Err(Error::NoSuchAccessKey),
    }
}

fn load_upload(
    uploads: &impl ReadableTable<&'static [u8], &'static [u8]>,
    bucket: &BucketRecord,
    key: &str,
    upload_id: &Uuid,
) -> Result<UploadRecord> {
    match uploads.get(upload_key(&bucket.id, key.as_bytes(), upload_id).as_slice())? {
        Some(v) => codec::decode(v.value()),
        None => Err(Error::NoSuchUpload),
    }
}

/// Deletes an upload and its parts. Parts listed in `kept` (sorted) hand their
/// block references to a completed object; all others release them.
fn remove_upload(
    txn: &redb::WriteTransaction,
    ukey: &[u8],
    upload_id: &Uuid,
    kept: &[u32],
    gc_due_at: Millis,
) -> Result<()> {
    txn.open_table(UPLOADS)?.remove(ukey)?;
    let mut parts = txn.open_table(PARTS)?;
    let mut blocks = txn.open_table(BLOCKS)?;
    let mut gc = txn.open_table(GC_QUEUE)?;
    let start = part_key(upload_id, 0);
    let end = part_key(upload_id, u32::MAX);
    for entry in parts.extract_from_if::<&[u8], _>(start.as_slice()..=end.as_slice(), |_, _| true)? {
        let (k, v) = entry?;
        let number = parse_part_number(k.value()).ok_or_else(|| Error::Corrupt("bad parts key".into()))?;
        if kept.binary_search(&number).is_err() {
            let part: PartRecord = codec::decode(v.value())?;
            release_blocks(&mut blocks, &mut gc, &part.blocks, gc_due_at)?;
        }
    }
    Ok(())
}

/// The common prefix `key` rolls up into under `prefix` and `delimiter`, if any.
fn common_prefix_of<'k>(key: &'k [u8], prefix: &[u8], delimiter: Option<&[u8]>) -> Option<&'k [u8]> {
    let delim = delimiter?;
    let rest = key.strip_prefix(prefix)?;
    let pos = memchr::memmem::find(rest, delim)?;
    Some(&key[..prefix.len() + pos + delim.len()])
}

fn bad_upload_key() -> Error {
    Error::Corrupt("bad uploads key".into())
}

fn delete_object(
    txn: &redb::WriteTransaction,
    bucket: &BucketRecord,
    key: &str,
    condition: &DeleteCondition,
    gc_due_at: Millis,
) -> Result<Option<ObjectRecord>> {
    let mut objects = txn.open_table(OBJECTS)?;
    let okey = object_key(&bucket.id, key.as_bytes());
    let previous: Option<ObjectRecord> = objects.get(okey.as_slice())?.map(|v| codec::decode(v.value())).transpose()?;
    if let Some(prev) = &previous {
        condition.check(prev)?;
        objects.remove(okey.as_slice())?;
    }
    if let Some(prev) = &previous {
        usage::adjust(txn, &bucket.id, None, Some(prev))?;
        let mut blocks = txn.open_table(BLOCKS)?;
        let mut gc = txn.open_table(GC_QUEUE)?;
        release_blocks(&mut blocks, &mut gc, prev.data.blocks(), gc_due_at)?;
    }
    Ok(previous)
}

fn add_block_ref(blocks: &mut redb::Table<&[u8], u64>, id: &Uuid) -> Result<()> {
    let refs = blocks.get(id.as_bytes().as_slice())?.map_or(0, |v| v.value());
    blocks.insert(id.as_bytes().as_slice(), refs + 1)?;
    Ok(())
}

/// Drops one reference to each block; blocks left unreferenced are queued for GC.
fn release_blocks(
    blocks: &mut redb::Table<&[u8], u64>,
    gc: &mut redb::Table<&[u8], ()>,
    released: &[BlockRef],
    due_at: Millis,
) -> Result<()> {
    for block in released {
        let key = block.id.as_bytes().as_slice();
        let refs = blocks.get(key)?.map_or(0, |v| v.value());
        let remaining = refs.saturating_sub(1);
        blocks.insert(key, remaining)?;
        if remaining == 0 {
            gc.insert(gc_key(due_at, &block.id).as_slice(), ())?;
        }
    }
    Ok(())
}

/// Key range covering every object in a bucket.
fn bucket_bounds(id: &Uuid) -> (Bound<Vec<u8>>, Bound<Vec<u8>>) {
    let start = Bound::Included(id.as_bytes().to_vec());
    let end = prefix_successor(id.as_bytes()).map_or(Bound::Unbounded, Bound::Excluded);
    (start, end)
}

fn as_slice_bounds(range: &(Bound<Vec<u8>>, Bound<Vec<u8>>)) -> (Bound<&[u8]>, Bound<&[u8]>) {
    (range.0.as_ref().map(Vec::as_slice), range.1.as_ref().map(Vec::as_slice))
}

fn utf8(bytes: &[u8]) -> Result<String> {
    String::from_utf8(bytes.to_vec()).map_err(|_| Error::Corrupt("object key is not UTF-8".into()))
}
