//! Node-local state, in `node.redb` next to `meta.redb`.
//!
//! `meta.redb` holds only state that every replica of the metadata shares:
//! it changes only through [`Command`](crate::meta::Command)s, so applying
//! the same commands anywhere gives the same tables. What describes this node
//! rather than the stored data (when it last scrubbed, and with clustering its
//! node id, repair queue and staging leases) lives here instead, and is never
//! replicated or included in metadata snapshots.

use std::path::Path;

use redb::{Database, ReadableDatabase, TableDefinition};

use crate::error::Result;

/// name → value
const VALUES: TableDefinition<&str, i64> = TableDefinition::new("values");

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
        txn.commit()?;
        Ok(Self { db })
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
}
