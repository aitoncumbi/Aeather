//! Bucket versioning.
//!
//! `objects` holds each key's current version when it is an object, so plain
//! listings never see anything else. `versions` holds noncurrent versions and
//! delete markers, keyed by bucket id ‖ key ‖ 0x00 ‖ inverted version id, so a
//! key's versions sort newest first (version ids are time-ordered UUIDv7).
//! The current version is the `objects` entry if there is one, otherwise the
//! newest `versions` entry, which is then always a delete marker: removing a
//! current version promotes the newest older one.

use redb::{ReadableTable, WriteTransaction};
use uuid::Uuid;

use std::ops::Bound;

use super::keys::{object_key, prefix_successor};
use super::{BLOCKS, GC_QUEUE, OBJECTS, VERSIONS, codec, release_blocks, usage};
use crate::error::{Error, Result};
use crate::meta::{
    BucketRecord, Millis, ObjectRecord, VersionEntry, VersionRef, Versioning, VersionsPage, VersionsQuery,
};

/// bucket id ‖ key ‖ 0x00 ‖ inverted version id.
pub fn version_key(bucket_id: &Uuid, key: &[u8], version_id: &Uuid) -> Vec<u8> {
    let mut out = object_key(bucket_id, key);
    out.push(0);
    out.extend(version_id.as_bytes().iter().map(|b| !b));
    out
}

/// The range of `key`'s entries in `versions`.
pub fn key_range(bucket_id: &Uuid, key: &[u8]) -> (Vec<u8>, Vec<u8>) {
    let start = [object_key(bucket_id, key), vec![0]].concat();
    let end = [object_key(bucket_id, key), vec![1]].concat();
    (start, end)
}

/// The key of a `versions` entry, without the bucket id and version suffix.
pub fn key_of(raw: &[u8]) -> &[u8] {
    &raw[16..raw.len() - 17]
}

/// `key`'s entries in `versions`, newest first.
pub fn noncurrent(
    table: &impl ReadableTable<&'static [u8], &'static [u8]>,
    bucket_id: &Uuid,
    key: &str,
) -> Result<Vec<ObjectRecord>> {
    let (start, end) = key_range(bucket_id, key.as_bytes());
    table.range::<&[u8]>(start.as_slice()..end.as_slice())?.map(|e| codec::decode(e?.1.value())).collect()
}

/// What a delete did, for the S3 response.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct Deleted {
    /// The version removed, or the delete marker created.
    pub version: Option<VersionRef>,
    pub delete_marker: bool,
}

/// Makes `record` the current version of `key`, as the bucket's versioning
/// state says: unversioned, the previous object is replaced; enabled, it is
/// kept as noncurrent; suspended, the `null` version is replaced and other
/// versions are kept. Delete markers are only placed in versioned buckets.
/// The caller has added references for `record`'s blocks; references of
/// replaced versions are released here. Returns the placed record and the
/// previous current object, if any.
pub(super) fn place(
    txn: &WriteTransaction,
    bucket: &BucketRecord,
    key: &str,
    mut record: ObjectRecord,
    gc_due_at: Millis,
) -> Result<(ObjectRecord, Option<ObjectRecord>)> {
    record.versioned = bucket.versioning == Versioning::Enabled;
    let okey = object_key(&bucket.id, key.as_bytes());
    let previous: Option<ObjectRecord> =
        txn.open_table(OBJECTS)?.get(okey.as_slice())?.map(|v| codec::decode(v.value())).transpose()?;

    if bucket.versioning == Versioning::Unversioned {
        if record.delete_marker {
            return Err(Error::Corrupt("delete markers need versioning".into()));
        }
        txn.open_table(OBJECTS)?.insert(okey.as_slice(), codec::encode(&record).as_slice())?;
        usage::adjust(txn, bucket, Some(&record), previous.as_ref())?;
        if let Some(prev) = &previous {
            release(txn, prev, gc_due_at)?;
        }
        return Ok((record, previous));
    }

    if bucket.versioning == Versioning::Suspended {
        remove_null_versions(txn, bucket, key, gc_due_at)?;
    }
    // Whatever is still current becomes noncurrent.
    let current: Option<ObjectRecord> =
        txn.open_table(OBJECTS)?.remove(okey.as_slice())?.map(|v| codec::decode(v.value())).transpose()?;
    if let Some(current) = &current {
        txn.open_table(VERSIONS)?.insert(
            version_key(&bucket.id, key.as_bytes(), &current.version_id).as_slice(),
            codec::encode(current).as_slice(),
        )?;
        usage::shift(txn, bucket, current, true)?;
    }
    if record.delete_marker {
        txn.open_table(VERSIONS)?.insert(
            version_key(&bucket.id, key.as_bytes(), &record.version_id).as_slice(),
            codec::encode(&record).as_slice(),
        )?;
    } else {
        txn.open_table(OBJECTS)?.insert(okey.as_slice(), codec::encode(&record).as_slice())?;
        usage::adjust(txn, bucket, Some(&record), None)?;
    }
    Ok((record, previous))
}

/// Suspended buckets keep one `null` version per key: drop the existing one.
fn remove_null_versions(txn: &WriteTransaction, bucket: &BucketRecord, key: &str, gc_due_at: Millis) -> Result<()> {
    let okey = object_key(&bucket.id, key.as_bytes());
    let current: Option<ObjectRecord> =
        txn.open_table(OBJECTS)?.get(okey.as_slice())?.map(|v| codec::decode(v.value())).transpose()?;
    if let Some(current) = current.filter(|c| !c.versioned) {
        txn.open_table(OBJECTS)?.remove(okey.as_slice())?;
        usage::adjust(txn, bucket, None, Some(&current))?;
        release(txn, &current, gc_due_at)?;
    }
    let nulls: Vec<ObjectRecord> =
        noncurrent(&txn.open_table(VERSIONS)?, &bucket.id, key)?.into_iter().filter(|v| !v.versioned).collect();
    for null in nulls {
        txn.open_table(VERSIONS)?.remove(version_key(&bucket.id, key.as_bytes(), &null.version_id).as_slice())?;
        usage::adjust_noncurrent(txn, bucket, None, Some(&null))?;
        release(txn, &null, gc_due_at)?;
    }
    Ok(())
}

/// Permanently removes one version. Removing the current one promotes the
/// newest older version. Removing a version that does not exist succeeds
/// and removes nothing.
pub(super) fn delete_version(
    txn: &WriteTransaction,
    bucket: &BucketRecord,
    key: &str,
    version: VersionRef,
    gc_due_at: Millis,
) -> Result<Deleted> {
    let okey = object_key(&bucket.id, key.as_bytes());
    let current: Option<ObjectRecord> =
        txn.open_table(OBJECTS)?.get(okey.as_slice())?.map(|v| codec::decode(v.value())).transpose()?;
    let removed = if let Some(current) = current.filter(|c| version.matches(c)) {
        txn.open_table(OBJECTS)?.remove(okey.as_slice())?;
        usage::adjust(txn, bucket, None, Some(&current))?;
        Some(current)
    } else {
        let found = noncurrent(&txn.open_table(VERSIONS)?, &bucket.id, key)?.into_iter().find(|v| version.matches(v));
        if let Some(found) = &found {
            txn.open_table(VERSIONS)?.remove(version_key(&bucket.id, key.as_bytes(), &found.version_id).as_slice())?;
            usage::adjust_noncurrent(txn, bucket, None, Some(found))?;
        }
        found
    };
    let Some(removed) = removed else {
        return Ok(Deleted { version: Some(version), delete_marker: false });
    };
    release(txn, &removed, gc_due_at)?;
    promote(txn, bucket, key)?;
    Ok(Deleted { version: Some(version), delete_marker: removed.delete_marker })
}

/// With no current object, the newest older version becomes current; a
/// delete marker stays in `versions`, where it is current by being newest.
fn promote(txn: &WriteTransaction, bucket: &BucketRecord, key: &str) -> Result<()> {
    let okey = object_key(&bucket.id, key.as_bytes());
    if txn.open_table(OBJECTS)?.get(okey.as_slice())?.is_some() {
        return Ok(());
    }
    let newest = noncurrent(&txn.open_table(VERSIONS)?, &bucket.id, key)?.into_iter().next();
    if let Some(newest) = newest.filter(|v| !v.delete_marker) {
        txn.open_table(VERSIONS)?.remove(version_key(&bucket.id, key.as_bytes(), &newest.version_id).as_slice())?;
        txn.open_table(OBJECTS)?.insert(okey.as_slice(), codec::encode(&newest).as_slice())?;
        usage::shift(txn, bucket, &newest, false)?;
    }
    Ok(())
}

fn release(txn: &WriteTransaction, record: &ObjectRecord, gc_due_at: Millis) -> Result<()> {
    let mut blocks = txn.open_table(BLOCKS)?;
    let mut gc = txn.open_table(GC_QUEUE)?;
    release_blocks(&mut blocks, &mut gc, record.data.blocks(), gc_due_at)
}

type Table = redb::ReadOnlyTable<&'static [u8], &'static [u8]>;

/// The smallest key (without the bucket id) of either table in `lower..upper`.
fn next_key(
    objects: &Table,
    versions: &Table,
    bucket_id: &Uuid,
    lower: &[u8],
    upper: &Bound<Vec<u8>>,
) -> Result<Option<Vec<u8>>> {
    let start = object_key(bucket_id, lower);
    let bounds = |start: Vec<u8>| (Bound::Included(start), upper.clone());
    let from_objects = objects.range::<&[u8]>(super::as_slice_bounds(&bounds(start.clone())))?.next();
    let a = from_objects.transpose()?.map(|(k, _)| k.value()[16..].to_vec());
    let from_versions = versions.range::<&[u8]>(super::as_slice_bounds(&bounds(start)))?.next();
    let b = from_versions.transpose()?.map(|(k, _)| key_of(k.value()).to_vec());
    Ok(match (a, b) {
        (Some(a), Some(b)) => Some(a.min(b)),
        (a, b) => a.or(b),
    })
}

/// A key's versions, current first: the `objects` entry, then `versions`.
fn all_versions(objects: &Table, versions: &Table, bucket_id: &Uuid, key: &[u8]) -> Result<Vec<ObjectRecord>> {
    let mut out = Vec::new();
    if let Some(v) = objects.get(object_key(bucket_id, key).as_slice())? {
        out.push(codec::decode(v.value())?);
    }
    let (start, end) = key_range(bucket_id, key);
    for entry in versions.range::<&[u8]>(start.as_slice()..end.as_slice())? {
        out.push(codec::decode(entry?.1.value())?);
    }
    Ok(out)
}

pub(super) fn list(
    objects: &Table,
    versions: &Table,
    bucket: &BucketRecord,
    q: &VersionsQuery,
) -> Result<VersionsPage> {
    let prefix = q.prefix.as_bytes();
    let delimiter = q.delimiter.as_deref().filter(|d| !d.is_empty()).map(str::as_bytes);
    let upper = match prefix_successor(&object_key(&bucket.id, prefix)) {
        Some(end) => Bound::Excluded(end),
        None => Bound::Unbounded,
    };
    // Where to start: within the marker key after a version; after a marker
    // that is a common prefix, past its whole group; after the marker key.
    let mut resume = None;
    let mut lower = match (&q.key_marker, q.version_marker) {
        (Some(key), Some(version)) => {
            resume = Some((key.as_bytes().to_vec(), version));
            key.as_bytes().to_vec()
        }
        (Some(key), None) if delimiter.is_some_and(|d| key.as_bytes().ends_with(d)) => {
            prefix_successor(key.as_bytes()).unwrap_or_default()
        }
        // Keys never contain NUL, and `key ‖ 0x00…` is where `key`'s own versions live.
        (Some(key), None) => [key.as_bytes(), &[1]].concat(),
        (None, _) => prefix.to_vec(),
    };
    if lower.as_slice() < prefix {
        lower = prefix.to_vec();
    }

    let mut page = VersionsPage::default();
    // The last item returned: where the next page continues.
    let mut last: Option<(String, Option<VersionRef>)> = None;
    while let Some(key) = next_key(objects, versions, &bucket.id, &lower, &upper)? {
        if let Some(delim) = delimiter
            && let Some(pos) = memchr::memmem::find(&key[prefix.len()..], delim)
        {
            let common = super::utf8(&key[..prefix.len() + pos + delim.len()])?;
            if page.versions.len() + page.common_prefixes.len() == q.max_keys {
                page.next = last;
                break;
            }
            match prefix_successor(common.as_bytes()) {
                Some(next) => lower = next,
                None => lower = Vec::new(),
            }
            last = Some((common.clone(), None));
            page.common_prefixes.push(common);
            if lower.is_empty() {
                break;
            }
            continue;
        }

        let key_str = super::utf8(&key)?;
        let mut entries = all_versions(objects, versions, &bucket.id, &key)?.into_iter().enumerate();
        if let Some((resume_key, after)) = resume.take()
            && resume_key == key
        {
            // Skip up to and including the marker version.
            for (_, record) in entries.by_ref() {
                if after.matches(&record) {
                    break;
                }
            }
        }
        for (i, record) in entries {
            if page.versions.len() + page.common_prefixes.len() == q.max_keys {
                page.next = last;
                return Ok(page);
            }
            last = Some((key_str.clone(), Some(record.version())));
            page.versions.push(VersionEntry { key: key_str.clone(), record, is_latest: i == 0 });
        }
        lower = [key.as_slice(), &[1]].concat();
    }
    Ok(page)
}
