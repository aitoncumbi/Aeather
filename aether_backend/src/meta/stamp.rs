//! Commit-order stamps.
//!
//! The writer that commits a batch (with clustering: the Raft leader that
//! proposes it) gives every command that creates a version its Last-Modified
//! time and version id, just before the batch is applied. Times never go
//! backwards and ids increase strictly, so the order of versions, which
//! listings and lifecycle rules rely on, is the order of their commits,
//! whatever the clock of the node that took the request did. Replicas apply
//! stamped commands as they are.

use uuid::Uuid;

use super::{Command, Millis};

/// Bits of the counter that orders ids within one millisecond.
const SEQ_BITS: u32 = 42;
const MAX_SEQ: u64 = (1 << SEQ_BITS) - 1;

#[derive(Debug)]
pub struct CommitClock {
    ms: Millis,
    seq: u64,
}

impl CommitClock {
    /// Continues after `last`, the latest stamp already committed: the next
    /// stamp is later than every id issued at `last`.
    pub fn after(last: Millis) -> Self {
        Self { ms: last, seq: MAX_SEQ }
    }

    /// The next stamp: `wall`, unless that is not after the previous stamp,
    /// and a version id that sorts after every earlier one.
    pub fn next(&mut self, wall: Millis) -> (Millis, Uuid) {
        if wall > self.ms {
            self.ms = wall;
            self.seq = 0;
        } else if self.seq == MAX_SEQ {
            self.ms += 1;
            self.seq = 0;
        } else {
            self.seq += 1;
        }
        (self.ms, version_id(self.ms, self.seq))
    }
}

/// A UUIDv7 (RFC 9562) whose 48-bit timestamp is `ms`, followed by `seq` as a
/// 42-bit counter (the "fixed-length dedicated counter" method) and 32 random
/// bits. Byte order is (ms, seq, random), so ids sort in stamp order.
fn version_id(ms: Millis, seq: u64) -> Uuid {
    let ms = u64::try_from(ms).unwrap_or(0) & ((1 << 48) - 1);
    let random = Uuid::new_v4().as_u128() as u32;
    let rand_a = (seq >> 30) & 0xfff; // the counter's top 12 bits
    let rand_b = ((seq & 0x3fff_ffff) << 32) | u64::from(random); // 62 bits
    let high = (ms << 16) | (0x7 << 12) | rand_a;
    let low = (0b10 << 62) | rand_b;
    Uuid::from_u64_pair(high, low)
}

impl Command {
    /// Stamps a command that creates a version (or a delete marker) and
    /// returns its time; leaves other commands alone.
    pub fn stamp(&mut self, clock: &mut CommitClock, wall: Millis) -> Option<Millis> {
        match self {
            Command::PutObject { record, .. }
            | Command::CompleteUpload { record, .. }
            | Command::CopyObject { record, .. } => {
                let (time, id) = clock.next(wall);
                record.last_modified = time;
                record.version_id = id;
                Some(time)
            }
            Command::DeleteObject { marker_id, now, .. } => {
                let (time, id) = clock.next(wall);
                *now = time;
                *marker_id = id;
                Some(time)
            }
            Command::DeleteObjects { objects, now, .. } => {
                let (time, _) = clock.next(wall);
                *now = time;
                for (_, _, _, marker_id) in objects.iter_mut() {
                    *marker_id = clock.next(wall).1;
                }
                Some(time)
            }
            _ => None,
        }
    }

    /// The stamp of a command that [`stamp`](Command::stamp) would stamp.
    pub(super) fn stamped_at(&self) -> Option<Millis> {
        match self {
            Command::PutObject { record, .. }
            | Command::CompleteUpload { record, .. }
            | Command::CopyObject { record, .. } => Some(record.last_modified),
            Command::DeleteObject { now, .. } | Command::DeleteObjects { now, .. } => Some(*now),
            _ => None,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn ids_follow_commit_order_when_the_clock_goes_back() {
        let mut clock = CommitClock::after(0);
        let mut stamps = Vec::new();
        for wall in [1000, 1000, 1005, 990, 990, 2000, 10] {
            stamps.push(clock.next(wall));
        }
        let times: Vec<Millis> = stamps.iter().map(|s| s.0).collect();
        assert_eq!(times, [1000, 1000, 1005, 1005, 1005, 2000, 2000]);
        assert!(stamps.windows(2).all(|w| w[0].1 < w[1].1), "ids must increase: {stamps:?}");
        for (time, id) in &stamps {
            assert_eq!(id.get_version_num(), 7);
            assert_eq!(id.get_variant(), uuid::Variant::RFC4122);
            let (secs, nanos) = id.get_timestamp().unwrap().to_unix();
            assert_eq!(secs as i64 * 1000 + i64::from(nanos / 1_000_000), *time);
        }
    }

    #[test]
    fn resuming_continues_after_the_last_stamp() {
        let mut before = CommitClock::after(0);
        let (last, last_id) = before.next(5000);
        // After a restart with a clock that is now behind.
        let mut clock = CommitClock::after(last);
        let (time, id) = clock.next(4000);
        assert!(time > last && id > last_id);
        // With a clock that is ahead, the wall time wins.
        let mut clock = CommitClock::after(last);
        assert_eq!(clock.next(9000).0, 9000);
    }

    #[test]
    fn many_ids_in_one_millisecond_stay_ordered() {
        let mut clock = CommitClock::after(0);
        let ids: Vec<Uuid> = (0..10_000).map(|_| clock.next(7).1).collect();
        assert!(ids.windows(2).all(|w| w[0] < w[1]));
    }
}
