//! Load generator for an S3 endpoint (Aether, or anything else to compare).
//!
//!   cargo run --release --example bench -- --endpoint http://127.0.0.1:9000 \
//!       --access-key KEY --secret-key SECRET
//!
//! Runs a fixed sequence of phases in a scratch bucket, prints one Markdown
//! table row per phase (operations per second, throughput, latency
//! percentiles), then deletes everything it wrote. Small-object phases run
//! for a fixed time; large-object phases move a fixed amount of data.

use std::sync::Arc;
use std::time::{Duration, Instant};

use anyhow::{Context, Result, bail};
use aws_sdk_s3::Client;
use aws_sdk_s3::config::{BehaviorVersion, Credentials, Region};
use aws_sdk_s3::primitives::ByteStream;
use aws_sdk_s3::types::{Delete, ObjectIdentifier};
use bytes::Bytes;
use clap::Parser;
use futures::future::try_join_all;

const MIB: f64 = 1024.0 * 1024.0;

#[derive(Parser)]
struct Args {
    #[arg(long, default_value = "http://127.0.0.1:9000")]
    endpoint: String,
    #[arg(long, env = "AWS_ACCESS_KEY_ID")]
    access_key: String,
    #[arg(long, env = "AWS_SECRET_ACCESS_KEY")]
    secret_key: String,
    #[arg(long, default_value = "us-east-1")]
    region: String,
    /// Scratch bucket; created, then deleted at the end. Must not exist.
    #[arg(long, default_value = "aether-bench")]
    bucket: String,
    /// Concurrent requests in the small-object phases.
    #[arg(long, default_value_t = 32)]
    concurrency: usize,
    /// Seconds each small-object phase runs.
    #[arg(long, default_value_t = 15)]
    duration: u64,
    /// Small object sizes, in bytes.
    #[arg(long, value_delimiter = ',', default_values_t = [1024, 65536])]
    small_sizes: Vec<usize>,
    /// Size of each large object, in MiB.
    #[arg(long, default_value_t = 64)]
    large_mib: usize,
    /// Number of large objects to write (and then read).
    #[arg(long, default_value_t = 16)]
    large_count: usize,
    /// Concurrent requests in the large-object phases.
    #[arg(long, default_value_t = 4)]
    large_concurrency: usize,
}

/// What one phase measured.
#[derive(Default)]
struct Sample {
    latencies: Vec<Duration>,
    bytes: u64,
    errors: u64,
    first_error: Option<String>,
}

impl Sample {
    fn merge(&mut self, other: Sample) {
        self.latencies.extend(other.latencies);
        self.bytes += other.bytes;
        self.errors += other.errors;
        self.first_error = self.first_error.take().or(other.first_error);
    }

    fn record<T, E: std::fmt::Display>(&mut self, started: Instant, result: Result<T, E>) -> Option<T> {
        match result {
            Ok(value) => {
                self.latencies.push(started.elapsed());
                Some(value)
            }
            Err(e) => {
                self.errors += 1;
                self.first_error.get_or_insert_with(|| e.to_string());
                None
            }
        }
    }
}

fn print_header() {
    println!("| phase | ops | ops/s | MiB/s | p50 ms | p90 ms | p99 ms | max ms | errors |");
    println!("|---|--:|--:|--:|--:|--:|--:|--:|--:|");
}

fn print_row(phase: &str, mut sample: Sample, elapsed: Duration) {
    sample.latencies.sort_unstable();
    let ms = |q: f64| {
        let lat = &sample.latencies;
        if lat.is_empty() {
            return "-".to_owned();
        }
        let i = ((lat.len() as f64 * q).ceil() as usize).clamp(1, lat.len()) - 1;
        format!("{:.1}", lat[i].as_secs_f64() * 1000.0)
    };
    let secs = elapsed.as_secs_f64();
    let ops = sample.latencies.len();
    println!(
        "| {phase} | {ops} | {:.0} | {:.1} | {} | {} | {} | {} | {} |",
        ops as f64 / secs,
        sample.bytes as f64 / MIB / secs,
        ms(0.5),
        ms(0.9),
        ms(0.99),
        ms(1.0),
        sample.errors
    );
    if let Some(e) = sample.first_error {
        eprintln!("  first error in {phase}: {e}");
    }
}

/// Deterministic, cheap pseudo-random numbers (xorshift64*), one per worker.
struct Rng(u64);

impl Rng {
    fn next(&mut self, below: usize) -> usize {
        self.0 ^= self.0 >> 12;
        self.0 ^= self.0 << 25;
        self.0 ^= self.0 >> 27;
        (self.0.wrapping_mul(0x2545_F491_4F6C_DD1D) % below as u64) as usize
    }
}

fn payload(len: usize) -> Bytes {
    (0..len).map(|i| (i.wrapping_mul(2_654_435_761) >> 13) as u8).collect::<Vec<_>>().into()
}

/// Runs `concurrency` workers until `duration` has passed. Each worker gets
/// its index and calls `op` with a running counter. Also returns the fewest
/// successful operations any one worker made.
async fn timed<F, Fut>(concurrency: usize, duration: Duration, op: F) -> Result<(Sample, Duration, usize)>
where
    F: Fn(usize, usize, Sample) -> Fut + Send + Sync + 'static,
    Fut: std::future::Future<Output = Sample> + Send,
{
    let op = Arc::new(op);
    let started = Instant::now();
    let deadline = started + duration;
    let workers = (0..concurrency).map(|worker| {
        let op = Arc::clone(&op);
        tokio::spawn(async move {
            let mut sample = Sample::default();
            let mut i = 0;
            while Instant::now() < deadline {
                sample = op(worker, i, sample).await;
                i += 1;
            }
            sample
        })
    });
    let mut total = Sample::default();
    let mut fewest = usize::MAX;
    for sample in try_join_all(workers).await? {
        fewest = fewest.min(sample.latencies.len());
        total.merge(sample);
    }
    Ok((total, started.elapsed(), fewest))
}

/// Runs `op` on every item, `concurrency` at a time.
async fn each<T, F, Fut>(items: Vec<T>, concurrency: usize, op: F) -> Result<(Sample, Duration)>
where
    T: Send + 'static,
    F: Fn(T, Sample) -> Fut + Send + Sync + 'static,
    Fut: std::future::Future<Output = Sample> + Send,
{
    let op = Arc::new(op);
    let queue = Arc::new(std::sync::Mutex::new(items));
    let started = Instant::now();
    let workers = (0..concurrency).map(|_| {
        let (op, queue) = (Arc::clone(&op), Arc::clone(&queue));
        tokio::spawn(async move {
            let mut sample = Sample::default();
            loop {
                let next = queue.lock().expect("queue lock").pop();
                let Some(item) = next else { break };
                sample = op(item, sample).await;
            }
            sample
        })
    });
    let mut total = Sample::default();
    for sample in try_join_all(workers).await? {
        total.merge(sample);
    }
    Ok((total, started.elapsed()))
}

#[tokio::main]
async fn main() -> Result<()> {
    let args = Args::parse();
    let config = aws_sdk_s3::Config::builder()
        .behavior_version(BehaviorVersion::latest())
        .region(Region::new(args.region.clone()))
        .endpoint_url(&args.endpoint)
        .credentials_provider(Credentials::new(&args.access_key, &args.secret_key, None, None, "bench"))
        .force_path_style(true)
        .build();
    let client = Client::from_conf(config);
    let bucket: Arc<str> = args.bucket.clone().into();
    let duration = Duration::from_secs(args.duration);

    client.create_bucket().bucket(&*bucket).send().await.context("creating the scratch bucket")?;
    let result = run(&args, &client, &bucket, duration).await;
    eprintln!("cleaning up {bucket}...");
    cleanup(&client, &bucket).await?;
    result
}

async fn run(args: &Args, client: &Client, bucket: &Arc<str>, duration: Duration) -> Result<()> {
    print_header();
    for &size in &args.small_sizes {
        let body = payload(size);
        let label = if size >= 1024 { format!("{} KiB", size / 1024) } else { format!("{size} B") };

        // PUT: each worker writes its own sequence of keys.
        let (c, b, data) = (client.clone(), Arc::clone(bucket), body.clone());
        let (sample, elapsed, fewest) = timed(args.concurrency, duration, move |worker, i, mut sample| {
            let (c, b, data) = (c.clone(), Arc::clone(&b), data.clone());
            async move {
                let started = Instant::now();
                let len = data.len() as u64;
                let key = format!("small-{size}/{worker:03}/{i:08}");
                let result = c.put_object().bucket(&*b).key(key).body(ByteStream::from(data)).send().await;
                if sample.record(started, result).is_some() {
                    sample.bytes += len;
                }
                sample
            }
        })
        .await?;
        let sample_errors = sample.errors;
        print_row(&format!("PUT {label}"), sample, elapsed);
        // Keys 0..fewest exist for every writer.
        if fewest == 0 || sample_errors > 0 {
            bail!("the PUT phase failed");
        }
        let (writers, per_writer) = (args.concurrency, fewest);

        // GET: random reads of the objects just written.
        let (c, b) = (client.clone(), Arc::clone(bucket));
        let (sample, elapsed, _) = timed(args.concurrency, duration, move |worker, i, mut sample| {
            let (c, b) = (c.clone(), Arc::clone(&b));
            async move {
                let mut rng = Rng(0x9E37_79B9_7F4A_7C15 ^ ((worker as u64) << 32 | i as u64));
                let key = format!("small-{size}/{:03}/{:08}", rng.next(writers), rng.next(per_writer));
                let started = Instant::now();
                let result = match c.get_object().bucket(&*b).key(key).send().await {
                    Ok(out) => out.body.collect().await.map(|data| data.into_bytes().len()).map_err(|e| e.to_string()),
                    Err(e) => Err(e.to_string()),
                };
                if let Some(len) = sample.record(started, result) {
                    sample.bytes += len as u64;
                }
                sample
            }
        })
        .await?;
        print_row(&format!("GET {label}"), sample, elapsed);
    }

    // LIST: page through the first small-object prefix, 1000 keys a page.
    let prefix = format!("small-{}/", args.small_sizes[0]);
    let (c, b) = (client.clone(), Arc::clone(bucket));
    let (sample, elapsed, _) = timed(args.concurrency.min(8), duration, move |_, _, mut sample| {
        let (c, b, prefix) = (c.clone(), Arc::clone(&b), prefix.clone());
        async move {
            let mut token = None;
            loop {
                let started = Instant::now();
                let result = c.list_objects_v2().bucket(&*b).prefix(&prefix).set_continuation_token(token).send().await;
                let Some(page) = sample.record(started, result) else { break };
                token = page.next_continuation_token().map(str::to_owned);
                if token.is_none() {
                    break;
                }
            }
            sample
        }
    })
    .await?;
    print_row("LIST 1000 keys/page", sample, elapsed);

    // Large objects: a fixed number of single PUTs, then GETs of each.
    let data = payload(args.large_mib << 20);
    let keys: Vec<String> = (0..args.large_count).map(|i| format!("large/{i:04}")).collect();
    let (c, b, body) = (client.clone(), Arc::clone(bucket), data.clone());
    let (sample, elapsed) = each(keys.clone(), args.large_concurrency, move |key, mut sample| {
        let (c, b, body) = (c.clone(), Arc::clone(&b), body.clone());
        async move {
            let len = body.len() as u64;
            let started = Instant::now();
            let result = c.put_object().bucket(&*b).key(key).body(ByteStream::from(body)).send().await;
            if sample.record(started, result).is_some() {
                sample.bytes += len;
            }
            sample
        }
    })
    .await?;
    print_row(&format!("PUT {} MiB", args.large_mib), sample, elapsed);

    let (c, b) = (client.clone(), Arc::clone(bucket));
    let (sample, elapsed) = each(keys, args.large_concurrency, move |key, mut sample| {
        let (c, b) = (c.clone(), Arc::clone(&b));
        async move {
            let started = Instant::now();
            let result = match c.get_object().bucket(&*b).key(key).send().await {
                Ok(out) => out.body.collect().await.map(|data| data.into_bytes().len()).map_err(|e| e.to_string()),
                Err(e) => Err(e.to_string()),
            };
            if let Some(len) = sample.record(started, result) {
                sample.bytes += len as u64;
            }
            sample
        }
    })
    .await?;
    print_row(&format!("GET {} MiB", args.large_mib), sample, elapsed);
    Ok(())
}

async fn cleanup(client: &Client, bucket: &str) -> Result<()> {
    loop {
        let page = client.list_objects_v2().bucket(bucket).send().await?;
        let ids: Vec<_> = page
            .contents()
            .iter()
            .filter_map(|o| o.key())
            .map(|k| ObjectIdentifier::builder().key(k).build())
            .collect::<Result<_, _>>()?;
        if ids.is_empty() {
            break;
        }
        let delete = Delete::builder().set_objects(Some(ids)).quiet(true).build()?;
        client.delete_objects().bucket(bucket).delete(delete).send().await?;
    }
    client.delete_bucket().bucket(bucket).send().await?;
    Ok(())
}
