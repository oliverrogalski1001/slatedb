//! Pluggable Write-Ahead Log storage.
//!
//! The [`WalStore`] trait is the boundary between SlateDB's WAL buffering /
//! replay logic and the physical medium that durably stores WAL batches.
//! Each implementation defines how a flush batch is persisted, enumerated,
//! streamed back on recovery, and trimmed.
//!
//! Today there are two implementations:
//! - [`crate::wal::object_store_wal::ObjectStoreWalStore`] — the default, which
//!   writes each flush batch as a WAL-formatted SST into an `object_store`.
//! - [`crate::wal::corfu_wal::CorfuWalStore`] — an experimental backend that
//!   talks to a Corfu shared log via a sidecar gRPC proxy.
//!
//! The abstraction is per-batch (not per-entry): `WalBufferManager` hands the
//! store a whole `WalBatch` at flush time, and the store returns the WAL id
//! that was assigned to it. For backends like Corfu where the sequencer owns
//! id assignment, the returned id is whatever the shared log hands back.

use async_trait::async_trait;
use std::sync::Arc;

use crate::error::SlateDBError;
use crate::types::RowEntry;

/// A batch of WAL entries handed to [`WalStore::append_wal`] at flush time.
///
/// Entries are borrowed so the store can serialize in place without an extra
/// clone. `last_tick` and `last_seq` carry the metadata the buffer already
/// tracked, in case the backend wants to stamp it alongside the payload.
pub struct WalBatch<'a> {
    pub entries: &'a [RowEntry],
    pub last_tick: i64,
    pub last_seq: u64,
}

/// Streaming reader over the entries of a single persisted WAL.
///
/// The replay path calls [`WalEntryReader::next_entry`] in a loop until it
/// yields `None`, mirroring the shape of the previous `SstIterator`-based
/// replay loop. Implementations are not required to be cheap to clone and
/// must tolerate being dropped mid-iteration.
#[allow(private_interfaces)]
#[async_trait]
pub trait WalEntryReader: Send {
    async fn next_entry(&mut self) -> Result<Option<RowEntry>, SlateDBError>;
}

/// Backend-agnostic WAL storage.
///
/// Implementations must be cheap to clone via `Arc` (they are stored inside
/// `Arc<dyn WalStore>`) and safe to call concurrently from the flusher,
/// the recovery iterator, and any background GC task.
#[allow(private_interfaces)]
#[async_trait]
pub trait WalStore: Send + Sync + 'static {
    /// Durably append one flush batch and return the WAL id the backend
    /// assigned to it. Backends that own id assignment (e.g. Corfu's
    /// sequencer) return the address here; backends that accept client ids
    /// (e.g. object store) allocate one internally.
    ///
    /// The returned id MUST be strictly greater than every id returned by
    /// any previously completed `append_wal` call on this store, so that the
    /// id can be used as an ordering key by higher layers.
    async fn append_wal(&self, batch: WalBatch<'_>) -> Result<u64, SlateDBError>;

    /// List WAL ids strictly greater than `after`, in ascending order, up to
    /// the current tail.
    async fn list_wals(&self, after: u64) -> Result<Vec<u64>, SlateDBError>;

    /// Open a streaming reader over the entries of one WAL.
    async fn open_wal(
        &self,
        wal_id: u64,
    ) -> Result<Box<dyn WalEntryReader>, SlateDBError>;

    /// Return the highest WAL id currently visible to readers.
    async fn tail(&self) -> Result<u64, SlateDBError>;

    /// Prefix-trim everything up to and including `up_to`. Backends without
    /// native GC may treat this as a best-effort hint.
    async fn trim(&self, up_to: u64) -> Result<(), SlateDBError>;

    /// Estimate the on-the-wire size of a WAL containing `entry_count`
    /// entries totalling `size_bytes` of payload. Used by the buffer to
    /// decide when to trigger a flush; must be cheap (non-async, no I/O).
    fn estimate_encoded_size(&self, entry_count: usize, size_bytes: usize) -> usize;
}

/// Convenience alias for the shared, dynamically-dispatched handle that
/// SlateDB passes around once a store has been selected.
pub type DynWalStore = Arc<dyn WalStore>;
