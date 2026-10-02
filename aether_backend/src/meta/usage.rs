//! Per-bucket object count and size, maintained inside each write command.

use std::collections::HashMap;

use redb::{ReadableTable, WriteTransaction};
use uuid::Uuid;

use super::{BUCKET_USAGE, BUCKETS, OBJECTS, codec};
use crate::error::{Error, Result};
use crate::meta::{BucketRecord, BucketUsage, ObjectRecord};

/// Accounts for `added` replacing `removed` (either may be absent). Fails
/// with `QuotaExceeded`, before anything is stored, if the bucket's quota
/// does not allow the change; the caller's transaction is then dropped.
pub(super) fn adjust(
    txn: &WriteTransaction,
    bucket: &BucketRecord,
    added: Option<&ObjectRecord>,
    removed: Option<&ObjectRecord>,
) -> Result<()> {
    let mut table = txn.open_table(BUCKET_USAGE)?;
    let before = get(&table, &bucket.id)?;
    let after = replaced(before, added.map(|r| r.size), removed.map(|r| r.size));
    bucket.quota.check(before, after)?;
    table.insert(bucket.id.as_bytes().as_slice(), codec::encode(&after).as_slice())?;
    Ok(())
}

/// Usage after an object of size `added` replaces one of size `removed`.
pub fn replaced(mut usage: BucketUsage, added: Option<u64>, removed: Option<u64>) -> BucketUsage {
    if let Some(size) = added {
        usage.objects += 1;
        usage.bytes += size;
    }
    if let Some(size) = removed {
        usage.objects = usage.objects.saturating_sub(1);
        usage.bytes = usage.bytes.saturating_sub(size);
    }
    usage
}

pub(super) fn remove(txn: &WriteTransaction, bucket_id: &Uuid) -> Result<()> {
    txn.open_table(BUCKET_USAGE)?.remove(bucket_id.as_bytes().as_slice())?;
    Ok(())
}

pub(super) fn get(table: &impl ReadableTable<&'static [u8], &'static [u8]>, bucket_id: &Uuid) -> Result<BucketUsage> {
    Ok(table.get(bucket_id.as_bytes().as_slice())?.map(|v| codec::decode(v.value())).transpose()?.unwrap_or_default())
}

/// Recomputes every bucket's usage from its objects (schema migration 1 → 2).
pub(super) fn rebuild(txn: &WriteTransaction) -> Result<()> {
    let mut totals: HashMap<Uuid, BucketUsage> = HashMap::new();
    for entry in txn.open_table(BUCKETS)?.iter()? {
        let bucket: BucketRecord = codec::decode(entry?.1.value())?;
        totals.insert(bucket.id, BucketUsage::default());
    }
    for entry in txn.open_table(OBJECTS)?.iter()? {
        let (k, v) = entry?;
        let bucket_id = Uuid::from_slice(&k.value()[..16]).map_err(|_| Error::Corrupt("bad objects key".into()))?;
        let record: ObjectRecord = codec::decode(v.value())?;
        let usage = totals.entry(bucket_id).or_default();
        usage.objects += 1;
        usage.bytes += record.size;
    }
    let mut table = txn.open_table(BUCKET_USAGE)?;
    table.retain(|_, _| false)?;
    for (bucket_id, usage) in totals {
        table.insert(bucket_id.as_bytes().as_slice(), codec::encode(&usage).as_slice())?;
    }
    Ok(())
}
