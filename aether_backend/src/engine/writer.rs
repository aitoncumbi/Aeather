//! Group commit: one thread applies every metadata write.
//!
//! redb runs one write transaction at a time and each commit is an fsync, so
//! committing writes one by one caps small PUTs at the disk's fsync rate. The
//! writer instead takes every command that is waiting and applies them in one
//! transaction ([`MetaStore::apply_batch`]): while one batch is being synced,
//! the next one gathers. Alone, a write commits as soon as it arrives.
//!
//! Just before applying a batch, the writer stamps the commands that create
//! versions ([`Command::stamp`]), so versions are ordered by commit, not by
//! the clock at the time each request arrived.

use std::sync::Arc;
use std::sync::mpsc;
use std::thread::JoinHandle;

use tokio::sync::oneshot;

use super::{EngineStats, StagedObject, WallClock, count};
use crate::error::{Error, Result};
use crate::meta::{Command, CommitClock, MetaStore, Outcome};

/// Most commands in one transaction, so one batch cannot hold the others' replies for long.
const MAX_BATCH: usize = 256;

struct Job {
    command: Command,
    /// Blocks the command references. The writer marks them committed (or lets
    /// them be deleted) itself, so a caller that goes away mid-commit cannot
    /// have committed blocks deleted.
    staged: Option<StagedObject>,
    reply: oneshot::Sender<Result<Outcome>>,
}

pub(super) struct Writer {
    jobs: Option<mpsc::Sender<Job>>,
    thread: Option<JoinHandle<()>>,
}

impl Writer {
    /// `clock` continues after the latest stamp `meta` has committed.
    pub(super) fn start(
        meta: Arc<MetaStore>,
        stats: Arc<EngineStats>,
        mut clock: CommitClock,
        wall: WallClock,
    ) -> Self {
        let (jobs, queue) = mpsc::channel::<Job>();
        // Dropped uncommitted blocks are deleted by a task on the runtime.
        let runtime = tokio::runtime::Handle::try_current().ok();
        let thread = std::thread::Builder::new()
            .name("aether-meta-writer".into())
            .spawn(move || {
                let _runtime = runtime.as_ref().map(|r| r.enter());
                while let Ok(first) = queue.recv() {
                    let mut batch = vec![first];
                    batch.extend(queue.try_iter().take(MAX_BATCH - 1));
                    let (mut commands, rest): (Vec<_>, Vec<_>) =
                        batch.into_iter().map(|job| (job.command, (job.staged, job.reply))).unzip();
                    let now = wall.now();
                    for command in &mut commands {
                        command.stamp(&mut clock, now);
                    }
                    count(&stats.meta_writes, commands.len() as u64);
                    count(&stats.meta_commits, 1);
                    let results = meta.apply_batch(commands);
                    for (result, (staged, reply)) in results.into_iter().zip(rest) {
                        if let Some(mut staged) = staged {
                            staged.committed = result.is_ok();
                        }
                        let _ = reply.send(result);
                    }
                }
            })
            .expect("start the metadata writer thread");
        Self { jobs: Some(jobs), thread: Some(thread) }
    }

    pub(super) async fn apply(&self, command: Command, staged: Option<StagedObject>) -> Result<Outcome> {
        let (reply, result) = oneshot::channel();
        let job = Job { command, staged, reply };
        self.jobs.as_ref().expect("writer running").send(job).map_err(|_| stopped())?;
        result.await.map_err(|_| stopped())?
    }
}

fn stopped() -> Error {
    Error::Io(std::io::Error::other("the metadata writer stopped"))
}

impl Drop for Writer {
    /// Waits for the thread, so the database is closed when the engine is
    /// gone (a new engine can then open the same data directory).
    fn drop(&mut self) {
        drop(self.jobs.take());
        if let Some(thread) = self.thread.take() {
            let _ = thread.join();
        }
    }
}
