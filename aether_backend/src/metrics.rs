//! Prometheus metrics, served at `/metrics` on the admin listener.
//!
//! Request counters and latency histograms are recorded as requests finish;
//! everything else (usage, disk space, background work) is read from the
//! engine when Prometheus scrapes.

use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant};

use axum::extract::State;
use axum::http::{StatusCode, header};
use axum::response::{IntoResponse, Response};
use prometheus_client::encoding::EncodeLabelSet;
use prometheus_client::encoding::text::encode;
use prometheus_client::metrics::counter::{ConstCounter, Counter};
use prometheus_client::metrics::family::Family;
use prometheus_client::metrics::gauge::{ConstGauge, Gauge};
use prometheus_client::metrics::histogram::Histogram;
use prometheus_client::registry::Registry;
use tracing::warn;

use crate::engine::Engine;

#[derive(Clone, Debug, Hash, PartialEq, Eq, EncodeLabelSet)]
struct RequestLabels {
    api: &'static str,
    operation: String,
    status: u16,
}

#[derive(Clone, Debug, Hash, PartialEq, Eq, EncodeLabelSet)]
struct OperationLabels {
    api: &'static str,
    operation: String,
}

#[derive(Clone, Debug, Hash, PartialEq, Eq, EncodeLabelSet)]
struct BucketLabels {
    bucket: String,
}

#[derive(Clone, Debug, Hash, PartialEq, Eq, EncodeLabelSet)]
struct BuildLabels {
    version: &'static str,
}

pub struct Metrics {
    engine: Arc<Engine>,
    started: Instant,
    requests: Family<RequestLabels, Counter>,
    durations: Family<OperationLabels, Histogram, fn() -> Histogram>,
    activity: Activity,
}

/// Upper bounds, in milliseconds, of the latency buckets the console's
/// percentiles are computed from (finer than the Prometheus histogram).
pub const LATENCY_BOUNDS_MS: [f64; 25] = [
    0.25, 0.5, 1.0, 2.0, 3.0, 5.0, 7.5, 10.0, 15.0, 20.0, 30.0, 50.0, 75.0, 100.0, 150.0, 200.0, 300.0, 500.0, 750.0,
    1000.0, 2000.0, 5000.0, 10_000.0, 30_000.0, 60_000.0,
];
/// One bucket per bound, plus one for anything slower.
pub const LATENCY_BUCKETS: usize = LATENCY_BOUNDS_MS.len() + 1;

/// What S3 clients asked for, in four kinds, for the console's charts.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum OperationKind {
    Read,
    Write,
    List,
    Other,
}

impl OperationKind {
    pub fn of(operation: &str) -> Self {
        match operation {
            "GetObject" | "HeadObject" | "GetObjectAttributes" => Self::Read,
            "PutObject"
            | "CopyObject"
            | "UploadPart"
            | "UploadPartCopy"
            | "CreateMultipartUpload"
            | "CompleteMultipartUpload"
            | "AbortMultipartUpload"
            | "DeleteObject"
            | "DeleteObjects" => Self::Write,
            "ListObjects"
            | "ListObjectsV2"
            | "ListObjectVersions"
            | "ListBuckets"
            | "ListMultipartUploads"
            | "ListParts" => Self::List,
            _ => Self::Other,
        }
    }
}

/// Cumulative S3 request counters since startup, sampled into
/// [`crate::history`].
#[derive(Debug, Default)]
pub struct Activity {
    /// Indexed by [`OperationKind`].
    pub requests: [AtomicU64; 4],
    pub client_errors: AtomicU64,
    pub server_errors: AtomicU64,
    pub latency: [AtomicU64; LATENCY_BUCKETS],
}

impl Activity {
    fn record(&self, operation: &str, status: u16, elapsed: Duration) {
        let kind = OperationKind::of(operation) as usize;
        self.requests[kind].fetch_add(1, Ordering::Relaxed);
        match status {
            400..=499 => self.client_errors.fetch_add(1, Ordering::Relaxed),
            500..=599 => self.server_errors.fetch_add(1, Ordering::Relaxed),
            _ => 0,
        };
        let ms = elapsed.as_secs_f64() * 1000.0;
        let bucket = LATENCY_BOUNDS_MS.iter().position(|&bound| ms <= bound).unwrap_or(LATENCY_BOUNDS_MS.len());
        self.latency[bucket].fetch_add(1, Ordering::Relaxed);
    }
}

fn latency_histogram() -> Histogram {
    Histogram::new([0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0, 10.0, 30.0, 60.0, 300.0])
}

impl Metrics {
    pub fn new(engine: Arc<Engine>) -> Arc<Self> {
        Arc::new(Self {
            engine,
            started: Instant::now(),
            requests: Family::default(),
            durations: Family::new_with_constructor(latency_histogram),
            activity: Activity::default(),
        })
    }

    pub fn engine(&self) -> &Arc<Engine> {
        &self.engine
    }

    pub fn activity(&self) -> &Activity {
        &self.activity
    }

    pub fn observe_request(&self, api: &'static str, operation: &str, status: u16, elapsed: Duration) {
        // Health checks and the like are not client traffic.
        if api == "s3" && operation != "internal" {
            self.activity.record(operation, status, elapsed);
        }
        let operation = operation.to_owned();
        self.requests.get_or_create(&RequestLabels { api, operation: operation.clone(), status }).inc();
        self.durations.get_or_create(&OperationLabels { api, operation }).observe(elapsed.as_secs_f64());
    }

    /// The OpenMetrics text exposition of every metric.
    pub async fn render(&self) -> crate::error::Result<String> {
        let mut registry = Registry::with_prefix("aether");
        registry.register("http_requests", "HTTP requests by API, operation and status", self.requests.clone());
        registry.register(
            "http_request_duration_seconds",
            "Time from receiving a request to sending its response headers",
            self.durations.clone(),
        );

        let build = Family::<BuildLabels, Gauge>::default();
        build.get_or_create(&BuildLabels { version: env!("CARGO_PKG_VERSION") }).set(1);
        registry.register("build_info", "Version of the running server", build);
        registry.register(
            "uptime_seconds",
            "Seconds since the server started",
            gauge(self.started.elapsed().as_secs()),
        );

        let stats = self.engine.stats();
        let counters = [
            ("received_bytes", "Object data received from clients", &stats.bytes_received),
            ("sent_bytes", "Object data sent to clients", &stats.bytes_sent),
            ("gc_deleted_blocks", "Block files deleted by garbage collection", &stats.gc_blocks_deleted),
            ("orphan_removed_blocks", "Unreferenced block files removed after crashes", &stats.orphans_removed),
            (
                "aborted_uploads",
                "Abandoned multipart uploads aborted (expiry and lifecycle rules)",
                &stats.uploads_aborted,
            ),
            (
                "lifecycle_expired",
                "Objects, versions and delete markers removed by lifecycle rules",
                &stats.lifecycle_expired,
            ),
            ("scrub_checked_blocks", "Blocks verified by the scrubber", &stats.scrub_blocks_checked),
            ("scrub_checked_bytes", "Bytes verified by the scrubber", &stats.scrub_bytes_checked),
            ("scrub_corrupt_blocks", "Damaged or missing blocks found by the scrubber", &stats.scrub_corrupt_blocks),
            (
                "metadata_commits",
                "Metadata write transactions (each one fsync), several writes each under load",
                &stats.meta_commits,
            ),
            ("metadata_writes", "Metadata writes (commands) committed or refused", &stats.meta_writes),
        ];
        for (name, help, value) in counters {
            registry.register(name, help, ConstCounter::new(value.load(Ordering::Relaxed)));
        }

        let usage = self.engine.list_bucket_usage().await?;
        let (objects, bytes) = (Family::<BucketLabels, Gauge>::default(), Family::<BucketLabels, Gauge>::default());
        // Only buckets with that limit, so `bucket_bytes / bucket_quota_bytes` alerts work as is.
        let (quota_objects, quota_bytes) =
            (Family::<BucketLabels, Gauge>::default(), Family::<BucketLabels, Gauge>::default());
        for (bucket, used) in &usage {
            let labels = BucketLabels { bucket: bucket.name.clone() };
            objects.get_or_create(&labels).set(clamp(used.objects));
            bytes.get_or_create(&labels).set(clamp(used.bytes));
            if let Some(max) = bucket.quota.max_objects {
                quota_objects.get_or_create(&labels).set(clamp(max));
            }
            if let Some(max) = bucket.quota.max_bytes {
                quota_bytes.get_or_create(&labels).set(clamp(max));
            }
        }
        registry.register("buckets", "Number of buckets", gauge(usage.len() as u64));
        registry.register("bucket_objects", "Objects per bucket", objects);
        registry.register("bucket_bytes", "Stored bytes per bucket (object sizes)", bytes);
        registry.register("bucket_quota_objects", "Object limit of buckets that have one", quota_objects);
        registry.register("bucket_quota_bytes", "Size limit in bytes of buckets that have one", quota_bytes);
        registry.register(
            "blocks",
            "Block files referenced or awaiting deletion",
            gauge(self.engine.block_count().await?),
        );
        registry.register("access_keys", "Number of access keys", gauge(self.engine.access_keys().len() as u64));

        if let Some(report) = self.engine.scrub_report() {
            let finished = report.finished_at.map_or(0, |ms| ms / 1000);
            registry.register(
                "scrub_last_finished_timestamp_seconds",
                "When the last complete scrub finished (0 while the first one runs)",
                gauge(u64::try_from(finished).unwrap_or(0)),
            );
        }
        match disk_space(&self.engine.config().data_dir) {
            Ok((available, total)) => {
                registry.register("disk_available_bytes", "Free space for the data directory", gauge(available));
                registry.register("disk_total_bytes", "Size of the data directory's file system", gauge(total));
            }
            Err(e) => warn!(error = %e, "cannot read disk space"),
        }

        let mut out = String::new();
        encode(&mut out, &registry).map_err(|e| crate::error::Error::Io(std::io::Error::other(e)))?;
        Ok(out)
    }
}

fn clamp(n: u64) -> i64 {
    i64::try_from(n).unwrap_or(i64::MAX)
}

fn gauge(n: u64) -> ConstGauge {
    ConstGauge::new(clamp(n))
}

/// (available, total) bytes on the file system holding `path`.
pub fn disk_space(path: &std::path::Path) -> std::io::Result<(u64, u64)> {
    let stats = rustix::fs::statvfs(path)?;
    Ok((stats.f_bavail.saturating_mul(stats.f_frsize), stats.f_blocks.saturating_mul(stats.f_frsize)))
}

pub async fn handler(State(metrics): State<Arc<Metrics>>) -> Response {
    match metrics.render().await {
        Ok(body) => ([(header::CONTENT_TYPE, "application/openmetrics-text; version=1.0.0; charset=utf-8")], body)
            .into_response(),
        Err(e) => {
            warn!(error = %e, "rendering metrics failed");
            (StatusCode::INTERNAL_SERVER_ERROR, "metrics unavailable").into_response()
        }
    }
}
