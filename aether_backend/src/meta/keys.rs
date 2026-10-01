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
    fn gc_keys_sort_by_time() {
        let a = Uuid::new_v4();
        let b = Uuid::new_v4();
        assert!(gc_key(5, &a) < gc_key(6, &b));
        assert!(gc_key(255, &a) < gc_key(256, &b));
        assert_eq!(parse_gc_key(&gc_key(1234, &a)), Some((1234, a)));
    }
}
