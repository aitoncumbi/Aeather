//! Metadata store backed by redb.
//!
//! Every mutation is a [`Command`] applied by [`MetaStore::apply`] in a single
//! write transaction. Commands carry all non-deterministic inputs (ids,
//! timestamps), so applying the same sequence on another machine produces the
//! same state. That is the seam where Raft replication plugs in for clustering.

mod codec;
mod keys;
mod types;

use std::ops::Bound;
use std::path::Path;

use redb::{Database, ReadableDatabase, ReadableTable, ReadableTableMetadata, TableDefinition};
use serde::{Deserialize, Serialize};
use uuid::Uuid;

pub use self::keys::prefix_successor;
use self::keys::{gc_key, object_key, parse_gc_key};
pub use self::types::*;
use crate::error::{Error, Result};

/// bucket name → `BucketRecord`
const BUCKETS: TableDefinition<&str, &[u8]> = TableDefinition::new("buckets");
/// bucket id ‖ object key → `ObjectRecord`
const OBJECTS: TableDefinition<&[u8], &[u8]> = TableDefinition::new("objects");
/// block id → number of objects referencing it. Entries at zero are waiting in `gc_queue`.
const BLOCKS: TableDefinition<&[u8], u64> = TableDefinition::new("blocks");
/// due time ‖ block id → ()
const GC_QUEUE: TableDefinition<&[u8], ()> = TableDefinition::new("gc_queue");
/// name → value
const SYSTEM: TableDefinition<&str, &[u8]> = TableDefinition::new("system");

const SCHEMA_VERSION: u32 = 1;

// Commands and outcomes are short-lived; boxing the large variants buys nothing.
#[allow(clippy::large_enum_variant)]
#[derive(Clone, Debug, Serialize, Deserialize)]
pub enum Command {
    CreateBucket {
        name: String,
        id: Uuid,
        created_at: Millis,
    },
    DeleteBucket {
        name: String,
    },
    PutObject {
        bucket: String,
        key: String,
        record: ObjectRecord,
        condition: WriteCondition,
        /// When blocks released by an overwrite may be deleted.
        gc_due_at: Millis,
    },
    DeleteObject {
        bucket: String,
        key: String,
        gc_due_at: Millis,
    },
    /// Deletes many keys in one transaction (S3 DeleteObjects).
    DeleteObjects {
        bucket: String,
        keys: Vec<String>,
        gc_due_at: Millis,
    },
    /// Drop GC queue entries whose block files have been deleted.
    ForgetBlocks {
        entries: Vec<(Millis, Uuid)>,
    },
}

#[allow(clippy::large_enum_variant)]
#[derive(Clone, Debug)]
pub enum Outcome {
    Done,
    Bucket(BucketRecord),
    /// The object record that was replaced or deleted, if any.
    Object(Option<ObjectRecord>),
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
            let mut system = txn.open_table(SYSTEM)?;
            let stored = system.get("schema_version")?.map(|v| codec::decode::<u32>(v.value())).transpose()?;
            match stored {
                None => {
                    system.insert("schema_version", codec::encode(&SCHEMA_VERSION).as_slice())?;
                }
                Some(SCHEMA_VERSION) => {}
                Some(other) => {
                    return Err(Error::Corrupt(format!(
                        "metadata schema version {other} is not supported (expected {SCHEMA_VERSION})"
                    )));
                }
            }
        }
        txn.commit()?;
        Ok(Self { db })
    }

    pub fn apply(&self, command: Command) -> Result<Outcome> {
        let txn = self.db.begin_write()?;
        let outcome = match command {
            Command::CreateBucket { name, id, created_at } => {
                let mut buckets = txn.open_table(BUCKETS)?;
                if buckets.get(name.as_str())?.is_some() {
                    return Err(Error::BucketAlreadyExists);
                }
                let record = BucketRecord { id, name, created_at };
                buckets.insert(record.name.as_str(), codec::encode(&record).as_slice())?;
                Outcome::Bucket(record)
            }
            Command::DeleteBucket { name } => {
                let mut buckets = txn.open_table(BUCKETS)?;
                let bucket: BucketRecord = match buckets.get(name.as_str())? {
                    Some(v) => codec::decode(v.value())?,
                    None => return Err(Error::NoSuchBucket),
                };
                let objects = txn.open_table(OBJECTS)?;
                if objects.range::<&[u8]>(as_slice_bounds(&bucket_bounds(&bucket.id)))?.next().is_some() {
                    return Err(Error::BucketNotEmpty);
                }
                buckets.remove(name.as_str())?;
                Outcome::Done
            }
            Command::PutObject { bucket, key, record, condition, gc_due_at } => {
                let bucket = load_bucket(&txn.open_table(BUCKETS)?, &bucket)?;
                let mut objects = txn.open_table(OBJECTS)?;
                let okey = object_key(&bucket.id, key.as_bytes());
                let previous: Option<ObjectRecord> =
                    objects.get(okey.as_slice())?.map(|v| codec::decode(v.value())).transpose()?;
                match (&condition, &previous) {
                    (WriteCondition::None, _) => {}
                    (WriteCondition::IfNoneMatchAny, None) => {}
                    (WriteCondition::IfNoneMatchAny, Some(_)) => return Err(Error::PreconditionFailed),
                    (WriteCondition::IfMatch(_), None) => return Err(Error::NoSuchKey),
                    (WriteCondition::IfMatch(etag), Some(prev)) if *etag == prev.etag => {}
                    (WriteCondition::IfMatch(_), Some(_)) => return Err(Error::PreconditionFailed),
                }
                objects.insert(okey.as_slice(), codec::encode(&record).as_slice())?;

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
            Command::DeleteObject { bucket, key, gc_due_at } => {
                let bucket = load_bucket(&txn.open_table(BUCKETS)?, &bucket)?;
                Outcome::Object(delete_object(&txn, &bucket, &key, gc_due_at)?)
            }
            Command::DeleteObjects { bucket, keys, gc_due_at } => {
                let bucket = load_bucket(&txn.open_table(BUCKETS)?, &bucket)?;
                for key in keys {
                    delete_object(&txn, &bucket, &key, gc_due_at)?;
                }
                Outcome::Done
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

fn delete_object(
    txn: &redb::WriteTransaction,
    bucket: &BucketRecord,
    key: &str,
    gc_due_at: Millis,
) -> Result<Option<ObjectRecord>> {
    let mut objects = txn.open_table(OBJECTS)?;
    let okey = object_key(&bucket.id, key.as_bytes());
    let previous: Option<ObjectRecord> =
        objects.remove(okey.as_slice())?.map(|v| codec::decode(v.value())).transpose()?;
    if let Some(prev) = &previous {
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
