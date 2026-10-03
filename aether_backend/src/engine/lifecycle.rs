//! Lifecycle rules, applied by a background job: expiring current objects
//! (a plain delete, so a delete marker in versioned buckets), removing lone
//! delete markers, expiring noncurrent versions and aborting old uploads.
//!
//! Every removal is conditional on what was read, so a version written or
//! changed while the job runs is left alone.

use std::sync::Arc;
use std::time::Duration;

use tracing::{info, warn};

use super::{Engine, count, now_millis};
use crate::error::{Error, Result};
use crate::meta::{BucketRecord, DeleteCondition, LifecycleRule, Millis, UploadListQuery, VersionEntry, VersionsQuery};

const PAGE: usize = 1000;

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct LifecycleReport {
    pub expired_objects: u64,
    pub expired_markers: u64,
    pub expired_versions: u64,
    pub aborted_uploads: u64,
}

impl Engine {
    fn days(&self, n: u32) -> Millis {
        i64::from(n) * self.config.lifecycle_day.as_millis() as Millis
    }

    /// Applies every bucket's lifecycle rules as of `now`.
    pub async fn apply_lifecycle(&self, now: Millis) -> Result<LifecycleReport> {
        let mut report = LifecycleReport::default();
        for bucket in self.list_buckets().await? {
            let rules: Vec<&LifecycleRule> = bucket.lifecycle.iter().filter(|r| r.enabled).collect();
            if rules.is_empty() {
                continue;
            }
            match self.apply_bucket_lifecycle(&bucket, &rules, now, &mut report).await {
                // The bucket went away while the job ran.
                Ok(()) | Err(Error::NoSuchBucket) => {}
                Err(e) => return Err(e),
            }
        }
        let stats = &self.stats;
        count(&stats.lifecycle_expired, report.expired_objects + report.expired_markers + report.expired_versions);
        count(&stats.uploads_aborted, report.aborted_uploads);
        Ok(report)
    }

    async fn apply_bucket_lifecycle(
        &self,
        bucket: &BucketRecord,
        rules: &[&LifecycleRule],
        now: Millis,
        report: &mut LifecycleReport,
    ) -> Result<()> {
        // Every version of every key, a key's versions together (they may span pages).
        let mut query = VersionsQuery { max_keys: PAGE, ..VersionsQuery::default() };
        let mut group: Vec<VersionEntry> = Vec::new();
        loop {
            let page = self.list_versions(&bucket.name, query.clone()).await?;
            for entry in page.versions {
                if group.first().is_some_and(|g| g.key != entry.key) {
                    self.apply_to_key(bucket, rules, std::mem::take(&mut group), now, report).await?;
                }
                group.push(entry);
            }
            let Some((key, version)) = page.next else { break };
            query.key_marker = Some(key);
            query.version_marker = version;
        }
        if !group.is_empty() {
            self.apply_to_key(bucket, rules, group, now, report).await?;
        }

        // Multipart uploads started too long ago.
        let mut uploads = UploadListQuery { max_uploads: PAGE, ..UploadListQuery::default() };
        loop {
            let page = self.list_uploads(&bucket.name, uploads.clone()).await?;
            for upload in &page.uploads {
                let due = rules.iter().any(|r| {
                    r.abort_upload_days.is_some_and(|d| upload.initiated + self.days(d) <= now)
                        && upload.key.starts_with(&r.filter.prefix)
                });
                if due {
                    match self.abort_upload(&bucket.name, &upload.key, upload.upload_id).await {
                        Ok(()) => report.aborted_uploads += 1,
                        Err(Error::NoSuchUpload) => {}
                        Err(e) => return Err(e),
                    }
                }
            }
            if !page.truncated {
                break;
            }
            uploads.key_marker = page.next_key_marker;
            uploads.upload_id_marker = page.next_upload_id_marker;
        }
        Ok(())
    }

    /// One key's versions, newest first.
    async fn apply_to_key(
        &self,
        bucket: &BucketRecord,
        rules: &[&LifecycleRule],
        versions: Vec<VersionEntry>,
        now: Millis,
        report: &mut LifecycleReport,
    ) -> Result<()> {
        let key = &versions[0].key;
        let current = versions.first().filter(|v| v.is_latest).map(|v| &v.record);

        if let Some(current) = current.filter(|c| !c.delete_marker) {
            let due = rules.iter().any(|r| {
                r.filter.matches(key, current)
                    && (r.expiration_days.is_some_and(|d| current.last_modified + self.days(d) <= now)
                        || r.expiration_date.is_some_and(|date| date <= now))
            });
            if due {
                // Only the version that was read: if it changed since, leave it.
                let condition = DeleteCondition {
                    etag: Some(current.etag.clone()),
                    last_modified: Some(current.last_modified),
                    size: Some(current.size),
                };
                match self.delete_object_if(&bucket.name, key, None, condition).await {
                    Ok(_) => report.expired_objects += 1,
                    Err(Error::PreconditionFailed) => {}
                    Err(e) => return Err(e),
                }
            }
        }

        // Noncurrent versions: each became noncurrent when the next newer one was written.
        for (noncurrent_seen, pair) in (0u32..).zip(versions.windows(2)) {
            let (newer, older) = (&pair[0].record, &pair[1].record);
            let since = newer.last_modified;
            let due = rules.iter().any(|r| {
                key.starts_with(&r.filter.prefix)
                    && r.noncurrent_days.is_some_and(|d| since + self.days(d) <= now)
                    && r.newer_noncurrent_versions.is_none_or(|keep| noncurrent_seen >= keep)
                    && (older.delete_marker || r.filter.matches(key, older))
            });
            if due {
                self.delete_object_if(&bucket.name, key, Some(older.version()), DeleteCondition::default()).await?;
                report.expired_versions += 1;
            }
        }

        // A delete marker with nothing left behind it.
        if let Some(marker) = current.filter(|c| c.delete_marker)
            && versions.len() == 1
            && rules.iter().any(|r| r.expired_object_delete_marker && key.starts_with(&r.filter.prefix))
        {
            self.delete_object_if(&bucket.name, key, Some(marker.version()), DeleteCondition::default()).await?;
            report.expired_markers += 1;
        }
        Ok(())
    }
}

/// Applies lifecycle rules every `interval`.
pub(super) async fn run_periodically(engine: Arc<Engine>, interval: Duration) {
    let mut tick = tokio::time::interval(interval);
    loop {
        tick.tick().await;
        match engine.apply_lifecycle(now_millis()).await {
            Ok(r) if r == LifecycleReport::default() => {}
            Ok(r) => info!(
                objects = r.expired_objects,
                delete_markers = r.expired_markers,
                versions = r.expired_versions,
                uploads = r.aborted_uploads,
                "lifecycle rules applied"
            ),
            Err(e) => warn!(error = %e, "applying lifecycle rules failed"),
        }
    }
}
