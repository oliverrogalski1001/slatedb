//! Default [`WalStore`] backend: writes each flush batch as a WAL-formatted
//! SST into an object store via [`TableStore`].
//!
//! This preserves the pre-refactor WAL layout bit-for-bit so existing
//! deployments (tests, benchmarks, checkpoints) keep working with no format
//! change. All of the SST-specific logic is now confined to this module —
//! the rest of the database talks to the [`WalStore`] trait.

use async_trait::async_trait;
use std::sync::Arc;

use crate::db_state::SsTableId;
use crate::error::SlateDBError;
use crate::iter::KeyValueIterator;
use crate::sst_iter::{SstIterator, SstIteratorOptions};
use crate::tablestore::TableStore;
use crate::types::RowEntry;
use crate::wal::wal_store::{WalBatch, WalEntryReader, WalStore};
use crate::wal_id::WalIdStore;

/// WAL store that serializes each batch through [`crate::wal::wal_sst_builder`]
/// and writes it to the WAL object store.
pub(crate) struct ObjectStoreWalStore {
    table_store: Arc<TableStore>,
    /// None for read-only stores (e.g. `DbReader`), which never invoke
    /// `append_wal` — they only enumerate and stream existing WALs for
    /// replay. Writing on a read-only store panics loudly.
    wal_id_incrementor: Option<Arc<dyn WalIdStore>>,
}

impl ObjectStoreWalStore {
    pub(crate) fn new(
        table_store: Arc<TableStore>,
        wal_id_incrementor: Arc<dyn WalIdStore>,
    ) -> Self {
        Self {
            table_store,
            wal_id_incrementor: Some(wal_id_incrementor),
        }
    }

    /// Construct a read-only store for `DbReader` and other non-writer
    /// contexts.
    pub(crate) fn new_read_only(table_store: Arc<TableStore>) -> Self {
        Self {
            table_store,
            wal_id_incrementor: None,
        }
    }
}

#[async_trait]
impl WalStore for ObjectStoreWalStore {
    async fn append_wal(&self, batch: WalBatch<'_>) -> Result<u64, SlateDBError> {
        // IDs are allocated before the write because the manifest's
        // `next_wal_sst_id` counter is the system of record for ordering
        // compacted state against live WALs; swapping to post-allocation
        // would be observable in the manifest.
        let wal_id = self
            .wal_id_incrementor
            .as_ref()
            .expect("append_wal called on a read-only ObjectStoreWalStore")
            .next_wal_id();
        let mut sst_builder = self.table_store.wal_table_builder();
        for entry in batch.entries {
            sst_builder.add(entry.clone()).await?;
        }
        let encoded_sst = sst_builder.build().await?;
        self.table_store
            .write_sst(&SsTableId::Wal(wal_id), encoded_sst, false)
            .await?;
        Ok(wal_id)
    }

    async fn list_wals(&self, after: u64) -> Result<Vec<u64>, SlateDBError> {
        let metadata = self.table_store.list_wal_ssts((after + 1)..).await?;
        Ok(metadata
            .into_iter()
            .map(|md| md.id.unwrap_wal_id())
            .collect())
    }

    async fn open_wal(
        &self,
        wal_id: u64,
    ) -> Result<Box<dyn WalEntryReader>, SlateDBError> {
        let handle = self
            .table_store
            .open_sst(&SsTableId::Wal(wal_id))
            .await?;
        let options = SstIteratorOptions {
            eager_spawn: true,
            ..SstIteratorOptions::default()
        };
        let maybe_iter = SstIterator::new_owned_initialized(
            ..,
            handle,
            Arc::clone(&self.table_store),
            options,
        )
        .await?;
        Ok(Box::new(SstWalReader {
            inner: maybe_iter,
        }))
    }

    async fn tail(&self) -> Result<u64, SlateDBError> {
        self.table_store.last_seen_wal_id().await
    }

    async fn trim(&self, up_to: u64) -> Result<(), SlateDBError> {
        // Compaction already deletes flushed WAL SSTs via its own bookkeeping;
        // explicit prefix-trim would race with that path. Keep this as a no-op
        // for the object-store backend — Corfu provides a real implementation.
        let _ = up_to;
        Ok(())
    }

    fn estimate_encoded_size(&self, entry_count: usize, size_bytes: usize) -> usize {
        self.table_store
            .estimate_encoded_size_wal(entry_count, size_bytes)
    }
}

/// Adapts an owned `SstIterator` to the [`WalEntryReader`] trait.
///
/// Empty WALs (used to fence zombie writers) open as `Ok(None)` from
/// `new_owned_initialized`; the adapter surfaces that as an immediately-
/// exhausted reader so the replay iterator still records the WAL id.
struct SstWalReader {
    inner: Option<SstIterator<'static>>,
}

#[async_trait]
impl WalEntryReader for SstWalReader {
    async fn next_entry(&mut self) -> Result<Option<RowEntry>, SlateDBError> {
        match self.inner.as_mut() {
            Some(iter) => iter.next_entry().await,
            None => Ok(None),
        }
    }
}
