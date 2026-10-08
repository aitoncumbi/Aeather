//! The scrubber: re-reads every block and checks it against the CRC32C
//! recorded when it was written, to find bit rot and lost files before a
//! client does.
//!
//! On a single node a damaged block cannot be repaired, only reported (logs,
//! metrics, admin status). Once blocks are replicated (v3), the same pass will
//! repair from a healthy copy.

use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use serde::{Deserialize, Serialize};
use tokio::io::AsyncReadExt;
use tracing::{error, info, warn};
use uuid::Uuid;

use super::{Engine, count, now_millis};
use crate::error::{Error, Result};
use crate::meta::{BlockRef, Millis};
use crate::node::LAST_SCRUB;

const BATCH: usize = 256;
const READ_BUFFER: usize = 1024 * 1024;
/// Corrupt blocks listed in a report; the counters keep counting past this.
const MAX_REPORTED: usize = 1000;

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ScrubReport {
    pub started_at: Millis,
    /// `None` while the scrub is still running.
    pub finished_at: Option<Millis>,
    pub objects_checked: u64,
    pub blocks_checked: u64,
    pub bytes_checked: u64,
    pub corrupt: Vec<CorruptBlock>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct CorruptBlock {
    pub bucket: String,
    pub key: String,
    pub block_id: Uuid,
    pub problem: String,
}

#[derive(Default)]
pub(super) struct ScrubState {
    /// Held while a scrub runs, so only one runs at a time.
    running: tokio::sync::Mutex<()>,
    /// The running scrub's progress, or the last finished one's result.
    report: Mutex<Option<ScrubReport>>,
}

impl Engine {
    /// The current scrub's progress, or the last one's result.
    pub fn scrub_report(&self) -> Option<ScrubReport> {
        self.scrub.report.lock().expect("scrub report lock poisoned").clone()
    }

    /// Verifies every block of every object. Fails with `Conflict` if a scrub
    /// is already running.
    pub async fn scrub(&self) -> Result<ScrubReport> {
        let _running =
            self.scrub.running.try_lock().map_err(|_| Error::Conflict("a scrub is already running".into()))?;
        let started = now_millis();
        let report = ScrubReport {
            started_at: started,
            finished_at: None,
            objects_checked: 0,
            bytes_checked: 0,
            blocks_checked: 0,
            corrupt: Vec::new(),
        };
        self.set_scrub_report(report.clone());
        info!("scrub started");

        let buckets: HashMap<Uuid, String> =
            self.list_buckets().await?.into_iter().map(|bucket| (bucket.id, bucket.name)).collect();
        let mut limiter = RateLimiter::new(self.config.scrub_rate);
        let mut report = report;
        // Current versions, then noncurrent ones kept by versioning.
        for noncurrent in [false, true] {
            let mut after: Option<Vec<u8>> = None;
            loop {
                let cursor = after.clone();
                let batch =
                    self.read(move |meta| meta.objects_with_blocks(noncurrent, cursor.as_deref(), BATCH)).await?;
                let Some((last, _)) = batch.last() else { break };
                after = Some(last.clone());

                for (raw_key, record) in batch {
                    for block in record.data.blocks() {
                        let problem = self.verify_block(block, &mut limiter).await;
                        report.blocks_checked += 1;
                        report.bytes_checked += block.len;
                        count(&self.stats.scrub_blocks_checked, 1);
                        count(&self.stats.scrub_bytes_checked, block.len);
                        let Some(problem) = problem else { continue };
                        // The object may have been overwritten or deleted since the
                        // batch was read; only blocks it still uses are corrupt.
                        let raw = raw_key.clone();
                        let current = self.read(move |meta| meta.object_at(noncurrent, &raw)).await?;
                        if !current.is_some_and(|r| r.data.blocks().iter().any(|b| b.id == block.id)) {
                            continue;
                        }
                        let bucket_id = Uuid::from_slice(&raw_key[..16]).unwrap_or_default();
                        let bucket = buckets.get(&bucket_id).cloned().unwrap_or_else(|| bucket_id.to_string());
                        let key = match noncurrent {
                            false => String::from_utf8_lossy(&raw_key[16..]).into_owned(),
                            true => format!(
                                "{} (noncurrent version)",
                                String::from_utf8_lossy(crate::meta::version_key_of(&raw_key))
                            ),
                        };
                        error!(%bucket, %key, block = %block.id, %problem, "scrub found a damaged block");
                        count(&self.stats.scrub_corrupt_blocks, 1);
                        if report.corrupt.len() < MAX_REPORTED {
                            report.corrupt.push(CorruptBlock { bucket, key, block_id: block.id, problem });
                        }
                    }
                    report.objects_checked += 1;
                }
                self.set_scrub_report(report.clone());
            }
        }

        report.finished_at = Some(now_millis());
        self.set_scrub_report(report.clone());
        let finished = now_millis();
        self.set_node_value(LAST_SCRUB, finished).await?;
        info!(
            objects = report.objects_checked,
            blocks = report.blocks_checked,
            bytes = report.bytes_checked,
            corrupt = report.corrupt.len(),
            seconds = (finished - started) / 1000,
            "scrub finished"
        );
        Ok(report)
    }

    /// `None` if the block is intact, otherwise what is wrong with it.
    async fn verify_block(&self, block: &BlockRef, limiter: &mut RateLimiter) -> Option<String> {
        let path = self.blocks.block_path(&block.id);
        let mut file = match tokio::fs::File::open(&path).await {
            Ok(file) => file,
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Some("block file is missing".into()),
            Err(e) => return Some(format!("cannot open block file: {e}")),
        };
        let mut buf = vec![0u8; READ_BUFFER];
        let (mut crc, mut len) = (0u32, 0u64);
        loop {
            match file.read(&mut buf).await {
                Ok(0) => break,
                Ok(n) => {
                    crc = crc32c::crc32c_append(crc, &buf[..n]);
                    len += n as u64;
                    limiter.consume(n as u64).await;
                }
                Err(e) => return Some(format!("cannot read block file: {e}")),
            }
        }
        if len != block.stored_len() {
            Some(format!("block file has {len} bytes, expected {}", block.stored_len()))
        } else if crc != block.crc32c {
            Some(format!("checksum mismatch: CRC32C {crc:08x}, expected {:08x}", block.crc32c))
        } else {
            None
        }
    }

    fn set_scrub_report(&self, report: ScrubReport) {
        *self.scrub.report.lock().expect("scrub report lock poisoned") = Some(report);
    }
}

/// Runs a scrub every `scrub_interval`, counted from the last finished one
/// (remembered across restarts), so frequent restarts do not cause rescans.
pub(super) async fn run_periodically(engine: Arc<Engine>) {
    let interval = engine.config.scrub_interval;
    loop {
        let last = match engine.node_value(LAST_SCRUB).await {
            Ok(Some(last)) => last,
            Ok(None) => {
                // A new data directory: start counting from now.
                let now = now_millis();
                if let Err(e) = engine.set_node_value(LAST_SCRUB, now).await {
                    warn!(error = %e, "cannot record scrub time");
                }
                now
            }
            Err(e) => {
                warn!(error = %e, "cannot read last scrub time");
                now_millis()
            }
        };
        let due = last.saturating_add(interval.as_millis() as Millis);
        let wait = u64::try_from(due - now_millis()).unwrap_or(0);
        tokio::time::sleep(Duration::from_millis(wait)).await;
        match engine.scrub().await {
            Ok(_) => {}
            Err(Error::Conflict(_)) => tokio::time::sleep(Duration::from_secs(60)).await,
            Err(e) => {
                warn!(error = %e, "scrub failed");
                tokio::time::sleep(Duration::from_secs(60 * 60)).await;
            }
        }
    }
}

/// Keeps the average read rate at or below `rate` bytes per second.
struct RateLimiter {
    rate: u64,
    started: Instant,
    consumed: u64,
}

impl RateLimiter {
    fn new(rate: u64) -> Self {
        Self { rate, started: Instant::now(), consumed: 0 }
    }

    async fn consume(&mut self, bytes: u64) {
        if self.rate == 0 {
            return;
        }
        self.consumed += bytes;
        let target = Duration::from_secs_f64(self.consumed as f64 / self.rate as f64);
        if let Some(ahead) = target.checked_sub(self.started.elapsed()) {
            tokio::time::sleep(ahead).await;
        }
    }
}
