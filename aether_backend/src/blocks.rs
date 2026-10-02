//! Immutable block files on local disk.
//!
//! Layout: `<root>/blocks/<aa>/<bb>/<block id>`, staging in `<root>/tmp/`.
//! A block is written to `tmp/`, fsynced, renamed into `blocks/`, and its
//! directory fsynced, so every file under `blocks/` is complete and durable.

use std::collections::HashSet;
use std::io;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};
use std::time::SystemTime;

use tokio::fs::{self, File};
use tokio::io::{AsyncReadExt, AsyncSeekExt, AsyncWriteExt, BufWriter};
use uuid::Uuid;

use crate::crypto::{DataKey, FRAME, FrameCipher};
use crate::meta::BlockRef;

const WRITE_BUFFER: usize = 1 << 20;

pub struct BlockStore {
    blocks_dir: PathBuf,
    tmp_dir: PathBuf,
    /// Blocks being written by uploads that have not committed yet. The orphan
    /// sweeper must not touch these even though no metadata references them.
    staged: Mutex<HashSet<Uuid>>,
}

impl BlockStore {
    /// Opens the store, discarding partial writes left by a previous process.
    pub async fn open(root: &Path) -> io::Result<Self> {
        let blocks_dir = root.join("blocks");
        let tmp_dir = root.join("tmp");
        fs::create_dir_all(&blocks_dir).await?;
        if fs::try_exists(&tmp_dir).await? {
            fs::remove_dir_all(&tmp_dir).await?;
        }
        fs::create_dir_all(&tmp_dir).await?;
        sync_dir(root).await?;
        Ok(Self { blocks_dir, tmp_dir, staged: Mutex::new(HashSet::new()) })
    }

    pub fn block_path(&self, id: &Uuid) -> PathBuf {
        let hex = id.simple().to_string();
        self.blocks_dir.join(&hex[0..2]).join(&hex[2..4]).join(hex)
    }

    /// Starts a new block, encrypted under `key` if given. Its id stays marked
    /// as staged until [`unstage`](Self::unstage).
    pub async fn create(self: &Arc<Self>, key: Option<&DataKey>) -> io::Result<BlockWriter> {
        let id = Uuid::new_v4();
        self.staged.lock().unwrap().insert(id);
        let tmp_path = self.tmp_dir.join(id.simple().to_string());
        let file = match File::create(&tmp_path).await {
            Ok(file) => file,
            Err(e) => {
                self.unstage(&[id]);
                return Err(e);
            }
        };
        Ok(BlockWriter {
            store: Arc::clone(self),
            id,
            tmp_path,
            file: BufWriter::with_capacity(WRITE_BUFFER, file),
            len: 0,
            stored: 0,
            crc32c: 0,
            cipher: key.map(|k| k.for_block(&id)),
            frame: Vec::new(),
            frames: 0,
            finished: false,
        })
    }

    pub fn unstage(&self, ids: &[Uuid]) {
        let mut staged = self.staged.lock().unwrap();
        for id in ids {
            staged.remove(id);
        }
    }

    pub fn is_staged(&self, id: &Uuid) -> bool {
        self.staged.lock().unwrap().contains(id)
    }

    /// Opens a block positioned at `offset`, limited to `len` bytes.
    pub async fn open_range(&self, id: &Uuid, offset: u64, len: u64) -> io::Result<tokio::io::Take<File>> {
        let mut file = File::open(self.block_path(id)).await?;
        if offset > 0 {
            file.seek(io::SeekFrom::Start(offset)).await?;
        }
        Ok(file.take(len))
    }

    /// Deletes a block. Deleting a block that is already gone succeeds.
    pub async fn delete(&self, id: &Uuid) -> io::Result<()> {
        match fs::remove_file(self.block_path(id)).await {
            Err(e) if e.kind() != io::ErrorKind::NotFound => Err(e),
            _ => Ok(()),
        }
    }

    /// Every block file with its modification time, plus any staging files.
    /// Runs on a blocking thread; intended for the orphan sweeper.
    pub async fn scan(self: &Arc<Self>) -> io::Result<Vec<(Uuid, SystemTime, PathBuf)>> {
        let store = Arc::clone(self);
        tokio::task::spawn_blocking(move || {
            let mut out = Vec::new();
            collect_block_files(&store.blocks_dir, 3, &mut out)?;
            collect_block_files(&store.tmp_dir, 1, &mut out)?;
            Ok(out)
        })
        .await?
    }
}

/// Walks `depth` directory levels below `dir` and collects files named by a block id.
fn collect_block_files(dir: &Path, depth: u32, out: &mut Vec<(Uuid, SystemTime, PathBuf)>) -> io::Result<()> {
    for entry in std::fs::read_dir(dir)? {
        let entry = entry?;
        let path = entry.path();
        if depth > 1 {
            if entry.file_type()?.is_dir() {
                collect_block_files(&path, depth - 1, out)?;
            }
            continue;
        }
        let Some(id) = path.file_name().and_then(|n| n.to_str()).and_then(|n| Uuid::try_parse(n).ok()) else {
            continue;
        };
        out.push((id, entry.metadata()?.modified()?, path));
    }
    Ok(())
}

pub struct BlockWriter {
    store: Arc<BlockStore>,
    id: Uuid,
    tmp_path: PathBuf,
    file: BufWriter<File>,
    /// Plaintext bytes written.
    len: u64,
    /// Bytes in the file.
    stored: u64,
    crc32c: u32,
    /// Encryption: the block's cipher, the frame being filled, frames sealed.
    cipher: Option<FrameCipher>,
    frame: Vec<u8>,
    frames: u64,
    finished: bool,
}

impl BlockWriter {
    pub fn id(&self) -> Uuid {
        self.id
    }

    pub fn bytes_written(&self) -> u64 {
        self.len
    }

    pub async fn write(&mut self, mut data: &[u8]) -> io::Result<()> {
        self.len += data.len() as u64;
        if self.cipher.is_none() {
            return self.write_stored(data).await;
        }
        while !data.is_empty() {
            let room = FRAME as usize - self.frame.len();
            let (now, rest) = data.split_at(room.min(data.len()));
            self.frame.extend_from_slice(now);
            data = rest;
            if self.frame.len() == FRAME as usize {
                self.seal_frame().await?;
            }
        }
        Ok(())
    }

    async fn seal_frame(&mut self) -> io::Result<()> {
        let cipher = self.cipher.as_ref().expect("only called when encrypting");
        let sealed = cipher.seal(self.frames, &self.frame);
        self.frames += 1;
        self.frame.clear();
        self.write_stored(&sealed).await
    }

    async fn write_stored(&mut self, data: &[u8]) -> io::Result<()> {
        self.file.write_all(data).await?;
        self.crc32c = crc32c::crc32c_append(self.crc32c, data);
        self.stored += data.len() as u64;
        Ok(())
    }

    /// Makes the block durable at its final path. The id stays staged; the
    /// caller unstages it once metadata references it (or it is discarded).
    pub async fn finish(mut self) -> io::Result<BlockRef> {
        if !self.frame.is_empty() {
            self.seal_frame().await?;
        }
        self.file.flush().await?;
        self.file.get_ref().sync_all().await?;
        let path = self.store.block_path(&self.id);
        let dir = path.parent().expect("block path has a parent");
        if !fs::try_exists(dir).await? {
            fs::create_dir_all(dir).await?;
            sync_dir(dir.parent().expect("fan-out dir has a parent")).await?;
            sync_dir(&self.store.blocks_dir).await?;
        }
        fs::rename(&self.tmp_path, &path).await?;
        self.finished = true;
        sync_dir(dir).await?;
        let stored_len = self.cipher.is_some().then_some(self.stored);
        Ok(BlockRef { id: self.id, len: self.len, crc32c: self.crc32c, stored_len })
    }
}

/// A writer dropped before `finish` (error, or the upload future was cancelled
/// because the client went away) removes its partial file.
impl Drop for BlockWriter {
    fn drop(&mut self) {
        if !self.finished {
            let _ = std::fs::remove_file(&self.tmp_path);
            self.store.unstage(&[self.id]);
        }
    }
}

async fn sync_dir(dir: &Path) -> io::Result<()> {
    File::open(dir).await?.sync_all().await
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn write_read_delete() {
        let dir = tempfile::tempdir().unwrap();
        let store = Arc::new(BlockStore::open(dir.path()).await.unwrap());

        let mut w = store.create(None).await.unwrap();
        w.write(b"hello ").await.unwrap();
        w.write(b"world").await.unwrap();
        let block = w.finish().await.unwrap();
        assert_eq!(block.len, 11);
        assert_eq!(block.crc32c, crc32c::crc32c(b"hello world"));
        assert!(store.is_staged(&block.id));

        let mut buf = String::new();
        store.open_range(&block.id, 6, 3).await.unwrap().read_to_string(&mut buf).await.unwrap();
        assert_eq!(buf, "wor");

        let scanned = store.scan().await.unwrap();
        assert_eq!(scanned.len(), 1);
        assert_eq!(scanned[0].0, block.id);

        store.delete(&block.id).await.unwrap();
        store.delete(&block.id).await.unwrap();
        assert!(store.open_range(&block.id, 0, 1).await.is_err());
    }

    #[tokio::test]
    async fn dropped_writer_removes_partial_file() {
        let dir = tempfile::tempdir().unwrap();
        let store = Arc::new(BlockStore::open(dir.path()).await.unwrap());
        let mut w = store.create(None).await.unwrap();
        let id = w.id();
        w.write(b"partial").await.unwrap();
        drop(w);
        assert!(!store.is_staged(&id));
        assert!(store.scan().await.unwrap().is_empty());
    }

    #[tokio::test]
    async fn open_discards_partial_writes() {
        let dir = tempfile::tempdir().unwrap();
        let store = Arc::new(BlockStore::open(dir.path()).await.unwrap());
        let mut w = store.create(None).await.unwrap();
        w.write(b"partial").await.unwrap();
        w.file.flush().await.unwrap();
        // Simulate a crash: the writer never runs its cleanup.
        std::mem::forget(w);
        assert_eq!(store.scan().await.unwrap().len(), 1);

        let store = Arc::new(BlockStore::open(dir.path()).await.unwrap());
        assert!(store.scan().await.unwrap().is_empty());
    }
}
