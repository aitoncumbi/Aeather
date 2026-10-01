//! Byte encodings for table keys. Ordering of these encodings is what makes
//! range scans (listing, GC) work, so changes here are format changes.

use uuid::Uuid;

use super::types::Millis;

/// `objects` key: bucket id (16 bytes) followed by the object key's UTF-8 bytes.
pub fn object_key(bucket_id: &Uuid, key: &[u8]) -> Vec<u8> {
    let mut out = Vec::with_capacity(16 + key.len());
    out.extend_from_slice(bucket_id.as_bytes());
    out.extend_from_slice(key);
    out
}

/// `uploads` key: bucket id ‖ object key ‖ 0x00 ‖ upload id. Object keys never
/// contain NUL (see `engine::validate_key`), so uploads sort by key, then by
/// upload id, which is time-ordered.
pub fn upload_key(bucket_id: &Uuid, key: &[u8], upload_id: &Uuid) -> Vec<u8> {
    let mut out = object_key(bucket_id, key);
    out.push(0);
    out.extend_from_slice(upload_id.as_bytes());
    out
}

/// Splits the part of an `uploads` key after the bucket id into (object key, upload id).
pub fn parse_upload_key(rest: &[u8]) -> Option<(&[u8], Uuid)> {
    let split = rest.len().checked_sub(17)?;
    let (key, tail) = rest.split_at(split);
    (tail[0] == 0).then_some(())?;
    Some((key, Uuid::from_slice(&tail[1..]).ok()?))
}

/// `parts` key: upload id ‖ part number (big-endian).
pub fn part_key(upload_id: &Uuid, part_number: u32) -> [u8; 20] {
    let mut out = [0u8; 20];
    out[..16].copy_from_slice(upload_id.as_bytes());
    out[16..].copy_from_slice(&part_number.to_be_bytes());
    out
}

pub fn parse_part_number(key: &[u8]) -> Option<u32> {
    Some(u32::from_be_bytes(key.get(16..20)?.try_into().ok()?))
}

/// `gc_queue` key: due time (big-endian, so earlier sorts first), then block id.
pub fn gc_key(due_at: Millis, block: &Uuid) -> [u8; 24] {
    let mut out = [0u8; 24];
    // Times before the epoch never occur; clamp so the cast is order-preserving.
    out[..8].copy_from_slice(&(due_at.max(0) as u64).to_be_bytes());
    out[8..].copy_from_slice(block.as_bytes());
    out
}

pub fn parse_gc_key(key: &[u8]) -> Option<(Millis, Uuid)> {
    let due: [u8; 8] = key.get(..8)?.try_into().ok()?;
    let id = Uuid::from_slice(key.get(8..24)?).ok()?;
    Some((u64::from_be_bytes(due) as Millis, id))
}

/// The smallest byte string greater than every string starting with `prefix`.
/// `None` when no such string exists (empty or all-`0xFF` prefix).
pub fn prefix_successor(prefix: &[u8]) -> Option<Vec<u8>> {
    let mut out = prefix.to_vec();
    while let Some(last) = out.pop() {
        if last < 0xFF {
            out.push(last + 1);
            return Some(out);
        }
    }
    None
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn successor_increments_last_byte() {
        assert_eq!(prefix_successor(b"a/"), Some(b"a0".to_vec()));
        assert_eq!(prefix_successor(b"ab"), Some(b"ac".to_vec()));
    }

    #[test]
    fn successor_carries_over_ff() {
        assert_eq!(prefix_successor(&[0x61, 0xFF, 0xFF]), Some(vec![0x62]));
        assert_eq!(prefix_successor(&[0xFF]), None);
        assert_eq!(prefix_successor(b""), None);
    }

    #[test]
    fn upload_keys_round_trip_and_sort() {
        let bucket = Uuid::new_v4();
        let first = Uuid::now_v7();
        let second = Uuid::now_v7();
        let k1 = upload_key(&bucket, b"photos/a", &first);
        assert_eq!(parse_upload_key(&k1[16..]), Some((&b"photos/a"[..], first)));
        assert!(k1 < upload_key(&bucket, b"photos/a", &second));
        assert!(upload_key(&bucket, b"photos/a", &second) < upload_key(&bucket, b"photos/a0", &first));
        assert!(upload_key(&bucket, b"photos/a", &second) < upload_key(&bucket, b"photos/a/b", &first));
        assert_eq!(parse_part_number(&part_key(&first, 7)), Some(7));
        assert!(part_key(&first, 2) < part_key(&first, 10));
    }

    #[test]
    fn gc_keys_sort_by_time() {
        let a = Uuid::new_v4();
        let b = Uuid::new_v4();
        assert!(gc_key(5, &a) < gc_key(6, &b));
        assert!(gc_key(255, &a) < gc_key(256, &b));
        assert_eq!(parse_gc_key(&gc_key(1234, &a)), Some((1234, a)));
    }
}
