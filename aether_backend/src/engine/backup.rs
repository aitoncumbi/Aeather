//! Online backups: a consistent copy of a running node, laid out as a data
//! directory, so restoring is starting Aether on it.
//!
//! The metadata is copied from one read transaction (a point-in-time view),
//! then every block it references. Blocks are immutable, so copying them
//! later is safe as long as none is deleted meanwhile: garbage collection
//! pauses while a backup runs.

use std::path::{Path, PathBuf};
use std::sync::atomic::Ordering;
use std::time::Instant;

use serde::{Deserialize, Serialize};
use tokio::io::AsyncWriteExt;
use tracing::info;

use super::Engine;
use crate::error::{Error, Result};

/// The metadata file of a data directory. A backup has it only once its
/// blocks are all copied, so a directory with it is a complete backup.
const META_FILE: &str = "meta.redb";

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct BackupReport {
    pub dir: PathBuf,
    pub blocks: u64,
    /// Block files copied, plus the metadata database.
    pub bytes: u64,
    pub duration_ms: u64,
}

/// Holds garbage collection off while it lives.
struct GcPause<'a>(&'a Engine);

impl Drop for GcPause<'_> {
    fn drop(&mut self) {
        self.0.backups.fetch_sub(1, Ordering::SeqCst);
    }
}

impl Engine {
    /// Whether a backup is running (garbage collection waits for it).
    pub(super) fn backup_running(&self) -> bool {
        self.backups.load(Ordering::SeqCst) > 0
    }

    /// Copies the node to `dir`, which must not exist or be empty.
    pub async fn backup(&self, dir: &Path) -> Result<BackupReport> {
        let started = Instant::now();
        prepare(dir, &self.config.data_dir).await?;
        self.backups.fetch_add(1, Ordering::SeqCst);
        let _pause = GcPause(self);

        let partial = dir.join(format!("{META_FILE}.partial"));
        let target = partial.clone();
        let ids = self.read(move |meta| meta.snapshot(&target)).await?;
        let mut bytes = tokio::fs::metadata(&partial).await?.len();

        let blocks_dir = dir.join("blocks");
        for id in &ids {
            let hex = id.simple().to_string();
            let to = blocks_dir.join(&hex[0..2]).join(&hex[2..4]).join(&hex);
            tokio::fs::create_dir_all(to.parent().expect("block path has a parent")).await?;
            let from = self.blocks.block_path(id);
            bytes += tokio::fs::copy(&from, &to)
                .await
                .map_err(|e| Error::Corrupt(format!("cannot copy block {} ({}): {e}", hex, from.display())))?;
            tokio::fs::File::open(&to).await?.sync_all().await?;
        }
        // Only now does the directory look like a data directory.
        tokio::fs::rename(&partial, dir.join(META_FILE)).await?;
        let mut marker = tokio::fs::File::create(dir.join("BACKUP")).await?;
        marker
            .write_all(
                format!(
                    "Aether {} backup, {} blocks. Start Aether with this directory as storage.data_dir to restore it.\n",
                    env!("CARGO_PKG_VERSION"),
                    ids.len()
                )
                .as_bytes(),
            )
            .await?;
        marker.sync_all().await?;
        tokio::fs::File::open(dir).await?.sync_all().await?;

        let report = BackupReport {
            dir: dir.to_owned(),
            blocks: ids.len() as u64,
            bytes,
            duration_ms: started.elapsed().as_millis() as u64,
        };
        info!(dir = %dir.display(), blocks = report.blocks, bytes = report.bytes, "backup finished");
        Ok(report)
    }
}

/// Creates `dir`, or accepts it empty: a backup never overwrites files. It
/// must be outside the data directory, whose sweeper would delete its blocks.
async fn prepare(dir: &Path, data_dir: &Path) -> Result<()> {
    if !dir.is_absolute() {
        return Err(Error::InvalidArgument("the backup directory must be an absolute path".into()));
    }
    let created = match tokio::fs::read_dir(dir).await {
        Ok(mut entries) => {
            if entries.next_entry().await?.is_some() {
                return Err(Error::InvalidArgument(format!(
                    "{} is not empty: back up to a new or empty directory",
                    dir.display()
                )));
            }
            false
        }
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => {
            tokio::fs::create_dir_all(dir).await?;
            true
        }
        Err(e) => return Err(e.into()),
    };
    let real = tokio::fs::canonicalize(dir).await?;
    if real.starts_with(tokio::fs::canonicalize(data_dir).await?) {
        if created {
            let _ = tokio::fs::remove_dir(dir).await;
        }
        return Err(Error::InvalidArgument("the backup directory must be outside the data directory".into()));
    }
    Ok(())
}
