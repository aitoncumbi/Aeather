//! Node-local state, in `node.redb` next to `meta.redb`.
//!
//! `meta.redb` holds only state that every replica of the metadata shares:
//! it changes only through [`Command`](crate::meta::Command)s, so applying
//! the same commands anywhere gives the same tables. What describes this node
//! rather than the stored data (when it last scrubbed, and with clustering its
//! node id, repair queue and staging leases) lives here instead, and is never
//! replicated or included in metadata snapshots.

use std::path::Path;

use redb::{Database, ReadableDatabase, ReadableTable, TableDefinition};
use uuid::Uuid;

use crate::error::{Error, Result};

/// name → value
const VALUES: TableDefinition<&str, i64> = TableDefinition::new("values");
/// name → bytes (the node id)
const BYTES: TableDefinition<&str, &[u8]> = TableDefinition::new("bytes");
const NODE_ID: &str = "node_id";

/// Values that older versions kept in `meta.redb`'s `system` table; they move
/// here when a data directory is opened.
pub const MOVED_FROM_META: &[&str] = &[LAST_SCRUB];

/// When the last scrub finished (or when the data directory was created).
pub const LAST_SCRUB: &str = "last_scrub_finished";

pub struct NodeStore {
    db: Database,
}

impl NodeStore {
    pub fn open(path: &Path) -> Result<Self> {
        let db = Database::create(path)?;
        let txn = db.begin_write()?;
        txn.open_table(VALUES)?;
        txn.open_table(BYTES)?;
        txn.commit()?;
        Ok(Self { db })
    }

    /// This node's identity: a random id made on first start and kept, so a
    /// node stays the same node across restarts and address changes.
    pub fn node_id(&self) -> Result<Uuid> {
        let txn = self.db.begin_write()?;
        let id = {
            let mut table = txn.open_table(BYTES)?;
            let stored = table.get(NODE_ID)?.map(|v| v.value().to_vec());
            match stored {
                Some(bytes) => Uuid::from_slice(&bytes).map_err(|_| Error::Corrupt("bad node id".into()))?,
                None => {
                    let id = Uuid::new_v4();
                    table.insert(NODE_ID, id.as_bytes().as_slice())?;
                    id
                }
            }
        };
        txn.commit()?;
        Ok(id)
    }

    pub fn get(&self, name: &str) -> Result<Option<i64>> {
        let txn = self.db.begin_read()?;
        Ok(txn.open_table(VALUES)?.get(name)?.map(|v| v.value()))
    }

    pub fn set(&self, name: &str, value: i64) -> Result<()> {
        let txn = self.db.begin_write()?;
        txn.open_table(VALUES)?.insert(name, value)?;
        txn.commit()?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn values_persist() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("node.redb");
        {
            let node = NodeStore::open(&path).unwrap();
            assert_eq!(node.get(LAST_SCRUB).unwrap(), None);
            node.set(LAST_SCRUB, 42).unwrap();
            node.set(LAST_SCRUB, 43).unwrap();
        }
        let node = NodeStore::open(&path).unwrap();
        assert_eq!(node.get(LAST_SCRUB).unwrap(), Some(43));
    }

    #[test]
    fn the_node_id_is_made_once() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("node.redb");
        let id = NodeStore::open(&path).unwrap().node_id().unwrap();
        assert_eq!(NodeStore::open(&path).unwrap().node_id().unwrap(), id);
        let other = NodeStore::open(&dir.path().join("other.redb")).unwrap().node_id().unwrap();
        assert_ne!(other, id);
    }
}
