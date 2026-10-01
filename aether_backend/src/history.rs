//! Recent activity for the web console's charts: S3 request rates by kind,
//! latency percentiles, bandwidth and the object count. Kept in memory at
//! three resolutions (10 s for an hour, 1 min for a day, 10 min for a week)
//! and lost on restart; Prometheus is the place for long-term history.

use std::collections::VecDeque;
use std::sync::atomic::Ordering;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use serde::Serialize;
use tokio::task::JoinHandle;

use crate::engine::now_millis;
use crate::meta::Millis;
use crate::metrics::{LATENCY_BOUNDS_MS, LATENCY_BUCKETS, Metrics};

/// How often counters are sampled.
pub const TICK: Duration = Duration::from_secs(10);

/// Resolution and retention of each tier: (ticks per sample, samples kept).
const TIERS: [(u32, usize); 3] = [(1, 360), (6, 1440), (60, 1008)];
/// The object count is read from metadata once a minute, and kept for a week.
const OBJECTS_EVERY: u32 = 6;
const OBJECT_READINGS: usize = 7 * 24 * 60;

/// Counter values over some interval (or, before subtraction, since startup).
#[derive(Clone, Copy, Debug, Default, PartialEq)]
pub struct Counters {
    /// By [`crate::metrics::OperationKind`]: reads, writes, lists, other.
    pub requests: [u64; 4],
    pub client_errors: u64,
    pub server_errors: u64,
    pub latency: [u64; LATENCY_BUCKETS],
    pub bytes_in: u64,
    pub bytes_out: u64,
}

impl Counters {
    fn read(metrics: &Metrics) -> Self {
        let activity = metrics.activity();
        let stats = metrics.engine().stats();
        let load = |c: &std::sync::atomic::AtomicU64| c.load(Ordering::Relaxed);
        Self {
            requests: std::array::from_fn(|i| load(&activity.requests[i])),
            client_errors: load(&activity.client_errors),
            server_errors: load(&activity.server_errors),
            latency: std::array::from_fn(|i| load(&activity.latency[i])),
            bytes_in: load(&stats.bytes_received),
            bytes_out: load(&stats.bytes_sent),
        }
    }

    fn since(&self, earlier: &Self) -> Self {
        Self {
            requests: std::array::from_fn(|i| self.requests[i].saturating_sub(earlier.requests[i])),
            client_errors: self.client_errors.saturating_sub(earlier.client_errors),
            server_errors: self.server_errors.saturating_sub(earlier.server_errors),
            latency: std::array::from_fn(|i| self.latency[i].saturating_sub(earlier.latency[i])),
            bytes_in: self.bytes_in.saturating_sub(earlier.bytes_in),
            bytes_out: self.bytes_out.saturating_sub(earlier.bytes_out),
        }
    }

    fn add(&mut self, other: &Self) {
        for i in 0..4 {
            self.requests[i] += other.requests[i];
        }
        for i in 0..LATENCY_BUCKETS {
            self.latency[i] += other.latency[i];
        }
        self.client_errors += other.client_errors;
        self.server_errors += other.server_errors;
        self.bytes_in += other.bytes_in;
        self.bytes_out += other.bytes_out;
    }

    fn total_requests(&self) -> u64 {
        self.requests.iter().sum()
    }
}

/// What happened during one interval ending at `end`.
#[derive(Clone, Debug)]
struct Sample {
    end: Millis,
    millis: Millis,
    counters: Counters,
}

impl Sample {
    fn absorb(&mut self, other: &Sample) {
        self.end = other.end;
        self.millis += other.millis;
        self.counters.add(&other.counters);
    }
}

#[derive(Debug)]
struct Tier {
    ticks: u32,
    capacity: usize,
    samples: VecDeque<Sample>,
    /// The sample being filled, and how many ticks it holds.
    pending: Option<(Sample, u32)>,
}

impl Tier {
    fn push(&mut self, sample: &Sample) {
        let (pending, n) = match self.pending.take() {
            Some((mut pending, n)) => {
                pending.absorb(sample);
                (pending, n + 1)
            }
            None => (sample.clone(), 1),
        };
        if n < self.ticks {
            self.pending = Some((pending, n));
            return;
        }
        if self.samples.len() == self.capacity {
            self.samples.pop_front();
        }
        self.samples.push_back(pending);
    }

    /// Finished samples, then the one being filled.
    fn all(&self) -> impl Iterator<Item = &Sample> {
        self.samples.iter().chain(self.pending.as_ref().map(|(sample, _)| sample))
    }
}

#[derive(Debug)]
struct State {
    started: Option<Millis>,
    last: Option<(Millis, Counters)>,
    tiers: [Tier; 3],
    /// (time, objects stored), oldest first.
    objects: VecDeque<(Millis, u64)>,
}

pub struct History {
    metrics: Arc<Metrics>,
    state: Mutex<State>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Range {
    Hour,
    Day,
    Week,
}

impl Range {
    pub fn parse(s: &str) -> Option<Self> {
        match s {
            "1h" => Some(Self::Hour),
            "24h" => Some(Self::Day),
            "7d" => Some(Self::Week),
            _ => None,
        }
    }

    fn label(self) -> &'static str {
        match self {
            Self::Hour => "1h",
            Self::Day => "24h",
            Self::Week => "7d",
        }
    }

    /// (span, tier, chart point width, bandwidth bar width, bar count), in ms.
    fn shape(self) -> (Millis, usize, Millis, Millis, usize) {
        const MIN: Millis = 60_000;
        match self {
            Self::Hour => (60 * MIN, 0, MIN, 5 * MIN, 12),
            Self::Day => (24 * 60 * MIN, 1, 15 * MIN, 60 * MIN, 24),
            Self::Week => (7 * 24 * 60 * MIN, 2, 120 * MIN, 12 * 60 * MIN, 14),
        }
    }
}

#[derive(Debug, Serialize)]
pub struct Report {
    pub range: &'static str,
    pub now: Millis,
    /// The chart's left edge.
    pub start: Millis,
    /// When sampling began (server start); nothing before it is known.
    pub history_start: Option<Millis>,
    pub point_secs: u64,
    pub bar_secs: u64,
    pub points: Vec<Point>,
    pub bars: Vec<Bar>,
    pub current: Current,
    pub day: Day,
    pub objects_change: Option<ObjectsChange>,
    /// Objects stored over the range, at most [`MAX_OBJECT_POINTS`] readings.
    pub objects: Vec<ObjectsPoint>,
}

const MAX_OBJECT_POINTS: usize = 60;

#[derive(Debug, Serialize)]
pub struct ObjectsPoint {
    pub t: Millis,
    pub objects: u64,
}

/// Requests per second by kind, and latency percentiles, over one interval.
#[derive(Debug, Serialize)]
pub struct Point {
    pub t: Millis,
    pub reads: f64,
    pub writes: f64,
    pub lists: f64,
    pub other: f64,
    pub p50_ms: Option<f64>,
    pub p99_ms: Option<f64>,
}

#[derive(Debug, Serialize)]
pub struct Bar {
    pub start: Millis,
    pub end: Millis,
    pub bytes_in: u64,
    pub bytes_out: u64,
    /// False for intervals before the history began.
    pub covered: bool,
}

/// The last minute's request rate and the last five minutes' latency.
#[derive(Debug, Serialize)]
pub struct Current {
    pub requests_per_sec: f64,
    pub p50_ms: Option<f64>,
    pub p99_ms: Option<f64>,
}

/// Totals over the last 24 hours, or since `since` if the history is shorter.
#[derive(Debug, Serialize)]
pub struct Day {
    pub since: Option<Millis>,
    pub requests: u64,
    pub client_errors: u64,
    pub server_errors: u64,
}

#[derive(Debug, Serialize)]
pub struct ObjectsChange {
    pub since: Millis,
    pub from: u64,
    pub to: u64,
}

impl History {
    pub fn new(metrics: Arc<Metrics>) -> Arc<Self> {
        let tiers = TIERS.map(|(ticks, capacity)| Tier { ticks, capacity, samples: VecDeque::new(), pending: None });
        let state = State { started: None, last: None, tiers, objects: VecDeque::new() };
        Arc::new(Self { metrics, state: Mutex::new(state) })
    }

    /// Samples the counters every [`TICK`] until the task is aborted.
    pub fn spawn_sampler(self: &Arc<Self>) -> JoinHandle<()> {
        let history = Arc::clone(self);
        tokio::spawn(async move {
            let mut interval = tokio::time::interval(TICK);
            interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
            let mut tick: u32 = 0;
            loop {
                interval.tick().await;
                let objects = if tick.is_multiple_of(OBJECTS_EVERY) {
                    let usage = history.metrics.engine().list_bucket_usage().await;
                    usage.ok().map(|buckets| buckets.iter().map(|(_, u)| u.objects).sum())
                } else {
                    None
                };
                history.record(now_millis(), Counters::read(&history.metrics), objects);
                tick = tick.wrapping_add(1);
            }
        })
    }

    /// Adds what changed since the previous call. The first call only sets
    /// the baseline.
    pub fn record(&self, now: Millis, counters: Counters, objects: Option<u64>) {
        let mut state = self.state.lock().expect("history lock poisoned");
        if let Some(objects) = objects {
            if state.objects.len() == OBJECT_READINGS {
                state.objects.pop_front();
            }
            state.objects.push_back((now, objects));
        }
        let Some((last_time, last)) = state.last.replace((now, counters)) else {
            state.started = Some(now);
            return;
        };
        let sample = Sample { end: now, millis: (now - last_time).max(1), counters: counters.since(&last) };
        for tier in &mut state.tiers {
            tier.push(&sample);
        }
    }

    pub fn report(&self, range: Range, now: Millis) -> Report {
        let state = self.state.lock().expect("history lock poisoned");
        let (span, tier, point_ms, bar_ms, bar_count) = range.shape();
        let start = now - span;
        let samples: Vec<&Sample> = state.tiers[tier].all().filter(|s| s.end > start && s.millis > 0).collect();

        let mut points = Vec::new();
        for group in samples.chunk_by(|a, b| window(a.end, point_ms) == window(b.end, point_ms)) {
            let (merged, millis) = merge(group.iter().copied());
            let per_sec = |n: u64| n as f64 * 1000.0 / millis as f64;
            points.push(Point {
                t: group.last().expect("chunks are not empty").end,
                reads: per_sec(merged.requests[0]),
                writes: per_sec(merged.requests[1]),
                lists: per_sec(merged.requests[2]),
                other: per_sec(merged.requests[3]),
                p50_ms: percentile(&merged.latency, 0.5),
                p99_ms: percentile(&merged.latency, 0.99),
            });
        }

        let last_bar = window(now, bar_ms);
        let bars = (0..bar_count as i64)
            .rev()
            .map(|back| {
                let bar_start = last_bar - back * bar_ms;
                let inside = samples.iter().filter(|s| window(s.end, bar_ms) == bar_start);
                let (merged, millis) = merge(inside.copied());
                Bar {
                    start: bar_start,
                    end: (bar_start + bar_ms).min(now),
                    bytes_in: merged.bytes_in,
                    bytes_out: merged.bytes_out,
                    covered: millis > 0,
                }
            })
            .collect();

        let recent = |within: Millis| state.tiers[0].all().filter(move |s| s.end > now - within && s.millis > 0);
        let (minute, minute_ms) = merge(recent(60_000));
        let (five, _) = merge(recent(5 * 60_000));
        let current = Current {
            requests_per_sec: if minute_ms > 0 {
                minute.total_requests() as f64 * 1000.0 / minute_ms as f64
            } else {
                0.0
            },
            p50_ms: percentile(&five.latency, 0.5),
            p99_ms: percentile(&five.latency, 0.99),
        };

        let day_start = now - 24 * 60 * 60_000;
        let (day_totals, _) = merge(state.tiers[1].all().filter(|s| s.end > day_start));
        let day = Day {
            since: state.started.map(|t| t.max(day_start)),
            requests: day_totals.total_requests(),
            client_errors: day_totals.client_errors,
            server_errors: day_totals.server_errors,
        };
        let mut readings = state.objects.iter().filter(|(t, _)| *t >= day_start);
        let objects_change = readings
            .next()
            .zip(state.objects.back())
            .map(|(&(since, from), &(_, to))| ObjectsChange { since, from, to });

        let in_range: Vec<&(Millis, u64)> = state.objects.iter().filter(|(t, _)| *t >= start).collect();
        let step = in_range.len().div_ceil(MAX_OBJECT_POINTS).max(1);
        let mut objects: Vec<ObjectsPoint> =
            in_range.iter().step_by(step).map(|&&(t, objects)| ObjectsPoint { t, objects }).collect();
        if let Some(&&(t, n)) = in_range.last()
            && objects.last().is_none_or(|p| p.t != t)
        {
            objects.push(ObjectsPoint { t, objects: n });
        }

        Report {
            range: range.label(),
            now,
            start,
            history_start: state.started,
            point_secs: (point_ms / 1000) as u64,
            bar_secs: (bar_ms / 1000) as u64,
            points,
            bars,
            current,
            day,
            objects_change,
            objects,
        }
    }
}

/// The start of the `width`-wide window holding the interval that ends at `end`.
fn window(end: Millis, width: Millis) -> Millis {
    (end - 1).div_euclid(width) * width
}

fn merge<'a>(samples: impl Iterator<Item = &'a Sample>) -> (Counters, Millis) {
    let mut total = Counters::default();
    let mut millis = 0;
    for sample in samples {
        total.add(&sample.counters);
        millis += sample.millis;
    }
    (total, millis)
}

/// The `q` quantile of a latency histogram, interpolated within its bucket.
pub fn percentile(histogram: &[u64; LATENCY_BUCKETS], q: f64) -> Option<f64> {
    let total: u64 = histogram.iter().sum();
    if total == 0 {
        return None;
    }
    let target = q * total as f64;
    let mut below = 0.0;
    for (i, &count) in histogram.iter().enumerate() {
        if count == 0 {
            continue;
        }
        let count = count as f64;
        if below + count >= target {
            let lower = if i == 0 { 0.0 } else { LATENCY_BOUNDS_MS[i - 1] };
            let Some(&upper) = LATENCY_BOUNDS_MS.get(i) else { return Some(lower) };
            return Some(lower + (upper - lower) * ((target - below) / count).clamp(0.0, 1.0));
        }
        below += count;
    }
    LATENCY_BOUNDS_MS.last().copied()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::engine::{Engine, EngineConfig};

    const TICK_MS: Millis = 10_000;

    fn counters_for_test(reads: u64, writes: u64, latency_bucket: usize, bytes_in: u64) -> Counters {
        let mut counters = Counters { requests: [reads, writes, 0, 0], bytes_in, ..Default::default() };
        counters.latency[latency_bucket] = reads + writes;
        counters
    }

    async fn history() -> (Arc<History>, tempfile::TempDir) {
        let dir = tempfile::tempdir().unwrap();
        let config = EngineConfig { data_dir: dir.path().to_path_buf(), ..EngineConfig::default() };
        let engine = Arc::new(Engine::open(config).await.unwrap());
        (History::new(Metrics::new(engine)), dir)
    }

    #[test]
    fn percentiles_interpolate_within_buckets() {
        let mut h = [0u64; LATENCY_BUCKETS];
        assert_eq!(percentile(&h, 0.5), None);
        // 100 requests between 5 and 7.5 ms (bucket 6).
        h[6] = 100;
        let p50 = percentile(&h, 0.5).unwrap();
        assert!((p50 - 6.25).abs() < 1e-9, "{p50}");
        // One very slow request moves p99 only a little; p100 lands past the last bound.
        h[LATENCY_BUCKETS - 1] = 1;
        assert!(percentile(&h, 0.99).unwrap() < 7.5);
        assert_eq!(percentile(&h, 1.0), Some(60_000.0));
    }

    #[tokio::test]
    async fn rates_and_totals_come_from_counter_deltas() {
        let (history, _dir) = history().await;
        let t0 = 1_000 * TICK_MS;
        history.record(t0, Counters::default(), Some(10));
        // Six 10-second ticks, each with 50 reads and 10 writes at ~6 ms,
        // and 1 MB received.
        for i in 1..=6u64 {
            let counters = counters_for_test(50 * i, 10 * i, 6, 1_000_000 * i);
            history.record(t0 + i as i64 * TICK_MS, counters, (i == 6).then_some(25));
        }
        let now = t0 + 6 * TICK_MS;
        let report = history.report(Range::Hour, now);
        assert_eq!(report.history_start, Some(t0));
        assert!((report.current.requests_per_sec - 6.0).abs() < 1e-9, "{}", report.current.requests_per_sec);
        assert!(report.current.p50_ms.unwrap() > 5.0 && report.current.p50_ms.unwrap() < 7.5);
        let reads: f64 = report.points.iter().map(|p| p.reads).sum::<f64>() / report.points.len() as f64;
        assert!((reads - 5.0).abs() < 1e-9, "{reads}");
        assert_eq!(report.bars.len(), 12);
        assert_eq!(report.bars.iter().map(|b| b.bytes_in).sum::<u64>(), 6_000_000);
        assert!(report.bars.last().unwrap().covered);
        assert!(!report.bars.first().unwrap().covered, "before the history began");

        let day = history.report(Range::Day, now);
        assert_eq!(day.day.requests, 360);
        let change = day.objects_change.unwrap();
        assert_eq!((change.from, change.to, change.since), (10, 25, t0));
        let series: Vec<u64> = day.objects.iter().map(|p| p.objects).collect();
        assert_eq!(series, [10, 25]);
    }

    #[tokio::test]
    async fn tiers_keep_a_bounded_number_of_samples() {
        let (history, _dir) = history().await;
        let mut counters = Counters::default();
        for i in 0..2000i64 {
            counters.requests[0] += 1;
            history.record(i * TICK_MS, counters, None);
        }
        let state = history.state.lock().unwrap();
        assert_eq!(state.tiers[0].samples.len(), 360);
        assert_eq!(state.tiers[1].samples.len(), 1999 / 6);
        assert_eq!(state.tiers[2].samples.len(), 1999 / 60);
    }
}
