use crate::config::BlobOptions;
use crate::db::DbInner;
use crate::db_state;
use crate::db_state::{PackFile, SsTableHandle};
use crate::error::SlateDBError;
use crate::format::sst::EncodedSsTable;
use crate::iter::RowEntryIterator;
use crate::mem_table::KVTable;
use crate::merge_operator::{MergeOperatorIterator, MergeOperatorRequiredIterator};
use crate::oracle::Oracle;
use crate::prefix_extractor::PrefixTarget;
use crate::reader::DbStateReader;
use crate::retention_iterator::RetentionIterator;
use crate::sst_builder::EncodedSsTableBuilder;
use crate::types::{BlobRef, RowEntry, ValueDeletable};
use crate::utils::IdGenerator;
use bytes::{Bytes, BytesMut};
use futures::stream::{self, TryStreamExt};
use std::sync::Arc;
use ulid::Ulid;

/// One encoded-but-not-yet-uploaded SST from a memtable flush, tagged with
/// the segment it belongs to (RFC-0024). Mirrors the shape of post-upload
/// [`crate::memtable_flusher::uploader::SegmentedSstHandle`].
pub(crate) struct EncodedSegmentSst {
    pub(crate) prefix: Bytes,
    pub(crate) encoded: EncodedSsTable,
}

/// Output of building one immutable memtable: the per-segment encoded SSTs
/// (with externalized values already rewritten to `BlobRef`s) and the pack
/// files that back them. Packs are uploaded inside `build_imm_ssts` so the
/// pack data is durable before any SST upload begins; the caller is
/// responsible for registering `pack_files` into the manifest alongside the
/// uploaded SSTs.
pub(crate) struct BuiltMemtable {
    pub(crate) ssts: Vec<EncodedSegmentSst>,
    pub(crate) pack_files: Vec<PackFile>,
}

impl DbInner {
    /// Build one or more L0 SSTs from a single immutable memtable, grouping
    /// entries by the segment prefix derived from the configured extractor,
    /// and externalize large values into packed blob files (RFC: packed blob
    /// files). Pack uploads complete before this function returns, so the
    /// returned `EncodedSegmentSst`s already carry `BlobRef`s whose backing
    /// packs are durable in object storage.
    ///
    /// Returns one `EncodedSegmentSst` per segment that received at least
    /// one post-retention entry, sorted ascending by `prefix`, plus the
    /// `PackFile` records that the caller must register into the manifest
    /// alongside the uploaded SSTs. The memtable iterator yields keys in
    /// sorted order and segments own disjoint key intervals, so all entries
    /// for a given prefix arrive consecutively — the implementation streams
    /// one open builder at a time, finalizing on prefix transitions.
    ///
    /// When no extractor is configured, every entry routes to the empty
    /// prefix and the SSTs Vec contains at most one entry. If retention
    /// prunes every entry the SSTs Vec is empty in both the extractor and
    /// no-extractor cases — per-memtable progress in the manifest
    /// (`last_l0_seq`, `replay_after_wal_id`) advances independently of
    /// whether any SST landed. `pack_files` is empty when no externalized
    /// value was encountered (either because `blob_options` is disabled or
    /// no value met `min_value_size`).
    pub(crate) async fn build_imm_ssts(
        &self,
        imm_table: Arc<KVTable>,
    ) -> Result<BuiltMemtable, SlateDBError> {
        let mut pack_assembler = PackAssembler::new(self);
        let ssts = self
            .build_imm_ssts_inner(imm_table, &mut pack_assembler)
            .await?;
        let pack_files = pack_assembler.upload_all().await?;
        Ok(BuiltMemtable { ssts, pack_files })
    }

    /// Inner loop for [`Self::build_imm_ssts`]. Streams post-retention
    /// entries through `pack_assembler` (which rewrites large `Value` rows
    /// to `BlobRef`s) and then into segment-aware SST builders. Splitting
    /// the upload-all step out of this function keeps the pack-assembly
    /// borrow on `self` from outliving the encoded SSTs.
    async fn build_imm_ssts_inner(
        &self,
        imm_table: Arc<KVTable>,
        pack_assembler: &mut PackAssembler<'_>,
    ) -> Result<Vec<EncodedSegmentSst>, SlateDBError> {
        let Some(extractor) = self.segment_extractor.as_ref() else {
            let mut sst_builder = self.table_store.table_builder();
            let mut iter = self.iter_imm_table(imm_table).await?;
            let mut any = false;
            while let Some(mut entry) = iter.next().await? {
                pack_assembler.maybe_externalize(&mut entry);
                sst_builder.add(entry).await?;
                any = true;
            }
            if !any {
                return Ok(Vec::new());
            }
            return Ok(vec![EncodedSegmentSst {
                prefix: Bytes::new(),
                encoded: sst_builder.build().await?,
            }]);
        };
        let mut iter = self.iter_imm_table(imm_table).await?;
        let mut out: Vec<EncodedSegmentSst> = Vec::new();
        let mut current: Option<(Bytes, EncodedSsTableBuilder<'_>)> = None;
        while let Some(mut entry) = iter.next().await? {
            let n = extractor
                .prefix_len(&PrefixTarget::Point(entry.key.clone()))
                .expect("extractor returned None for a key already in the memtable");
            let prefix = entry.key.slice(0..n);
            let same_segment = current.as_ref().is_some_and(|(p, _)| p == &prefix);
            if !same_segment {
                if let Some((cur_prefix, builder)) = current.take() {
                    out.push(EncodedSegmentSst {
                        prefix: cur_prefix,
                        encoded: builder.build().await?,
                    });
                }
                current = Some((prefix, self.table_store.table_builder()));
            }
            pack_assembler.maybe_externalize(&mut entry);
            let (_, builder) = current.as_mut().expect("set on first iteration");
            builder.add(entry).await?;
        }
        if let Some((cur_prefix, builder)) = current {
            out.push(EncodedSegmentSst {
                prefix: cur_prefix,
                encoded: builder.build().await?,
            });
        }
        Ok(out)
    }

    /// Write `encoded_sst` to object storage at `id` and advance the
    /// monotonic durable tick from `imm_table`.
    pub(crate) async fn upload_sst(
        &self,
        id: &db_state::SsTableId,
        imm_table: Arc<KVTable>,
        encoded_sst: &EncodedSsTable,
        write_cache: bool,
    ) -> Result<SsTableHandle, SlateDBError> {
        let handle = self
            .table_store
            .write_sst(id, encoded_sst, write_cache)
            .await?;

        self.mono_clock
            .fetch_max_last_durable_tick(imm_table.last_tick());

        Ok(handle)
    }

    /// Write an empty WAL SST at `wal_id` as a fencing barrier. The
    /// object-storage put-if-absent at this slot is what fences in-flight
    /// WAL writes from older-epoch writers — see [`Self::fence_writers`].
    ///
    /// Builds the empty SST blob directly from an empty builder; bypasses
    /// the L0 build pipeline since there's no memtable to flush. L0 data
    /// must go through the segment-aware upload pipeline
    /// ([`Self::build_imm_ssts`]).
    pub(crate) async fn flush_empty_wal(&self, wal_id: u64) -> Result<(), SlateDBError> {
        let encoded_sst = self.table_store.table_builder().build().await?;
        let empty = crate::mem_table::WritableKVTable::new();
        self.upload_sst(
            &db_state::SsTableId::Wal(wal_id),
            empty.table().clone(),
            &encoded_sst,
            false,
        )
        .await?;
        Ok(())
    }

    /// Test helper: build L0 SSTs from `imm_table` via the segment-aware
    /// path ([`Self::build_imm_ssts`]) and upload each one with a freshly
    /// allocated [`db_state::SsTableId::Compacted`]. Returns the resulting
    /// handles in the same order as the segments. Without an extractor the
    /// result is at most one handle; an empty Vec means retention pruned
    /// every entry.
    #[cfg(test)]
    pub(crate) async fn flush_l0_for_test(
        &self,
        imm_table: Arc<KVTable>,
        write_cache: bool,
    ) -> Result<Vec<SsTableHandle>, SlateDBError> {
        let built = self.build_imm_ssts(imm_table.clone()).await?;
        let mut handles = Vec::with_capacity(built.ssts.len());
        for sst in built.ssts {
            let id = db_state::SsTableId::Compacted(
                self.rand.rng().gen_ulid(self.system_clock.as_ref()),
            );
            let handle = self
                .upload_sst(&id, imm_table.clone(), &sst.encoded, write_cache)
                .await?;
            handles.push(handle);
        }
        Ok(handles)
    }

    async fn iter_imm_table(
        &self,
        imm_table: Arc<KVTable>,
    ) -> Result<RetentionIterator<Box<dyn RowEntryIterator>>, SlateDBError> {
        let state = self.state.read().view();

        // Compute retention boundary using the minimum active sequences from active snapshots AND
        // active transactions AND durable watermark. This does not need to be atomic as even if a
        // new snapshot is created/dropped or a new transaction is created/dropped between reading
        // both snapshot_manager and txn_manager we will always have the min so any race here is
        // acceptable.
        //
        // Remote readers (DurabilityLevel::Remote) cap visibility at last_remote_persisted_seq,
        // so we must retain at least one version at or below that boundary for each key.
        // Otherwise, if we only keep a newer non-durable version, remote readers would skip
        // it and incorrectly fall back to an even older value.
        let durable_seq = self.oracle.last_remote_persisted_seq();
        let min_retention_seq = [
            Some(durable_seq),
            self.snapshot_manager.min_active_seq(),
            self.txn_manager.min_active_seq(),
        ]
        .into_iter()
        .flatten()
        .min();

        let merge_iter = if let Some(merge_operator) = self.flush_merge_operator.clone() {
            Box::new(MergeOperatorIterator::new(
                merge_operator,
                imm_table.iter(),
                false,
                min_retention_seq,
                None,
                false,
            ))
        } else {
            Box::new(MergeOperatorRequiredIterator::new(imm_table.iter()))
                as Box<dyn RowEntryIterator>
        };
        let mut iter = RetentionIterator::new(
            merge_iter,
            None,
            min_retention_seq,
            false,
            imm_table.last_tick(),
            self.system_clock.clone(),
            Arc::new(state.core().sequence_tracker.clone()),
            None,
        )
        .await?;
        iter.init().await?;
        Ok(iter)
    }
}

/// Externalizes large `Value` entries by packing them into shared object
/// files. Walks the post-retention entry stream in order, accumulating each
/// large value into the trailing pack buffer; a new pack opens whenever
/// appending the next value would push the active buffer past
/// `target_pack_size_bytes`. A single value larger than the target still
/// lands in its own pack. Each externalized entry's `Value` is rewritten in
/// place to a `BlobRef(pack_id, offset, length)` that points into one of
/// the packs assembled here.
struct PackAssembler<'a> {
    db: &'a DbInner,
    /// `None` when `blob_options` is unset; `maybe_externalize` becomes a
    /// no-op and `upload_all` yields an empty Vec.
    blob_options: Option<&'a BlobOptions>,
    pack_buffers: Vec<BytesMut>,
    pack_ids: Vec<Ulid>,
}

impl<'a> PackAssembler<'a> {
    fn new(db: &'a DbInner) -> Self {
        Self {
            db,
            blob_options: db.settings.blob_options.as_ref(),
            pack_buffers: Vec::new(),
            pack_ids: Vec::new(),
        }
    }

    /// If `entry`'s value qualifies for externalization, append its bytes
    /// to the currently-open pack buffer (opening a new pack first if
    /// adding the value would exceed `target_pack_size_bytes`) and rewrite
    /// `entry.value` to a `BlobRef`. Tombstones, merges, existing
    /// `BlobRef`s, and small values are left untouched.
    fn maybe_externalize(&mut self, entry: &mut RowEntry) {
        let Some(opts) = self.blob_options else {
            return;
        };
        let value = match &entry.value {
            ValueDeletable::Value(v) if v.len() >= opts.min_value_size => v.clone(),
            _ => return,
        };
        let target_pack_size = opts.target_pack_size_bytes.max(1);
        let need_new_pack = self
            .pack_buffers
            .last()
            .map(|buf| buf.len() + value.len() > target_pack_size)
            .unwrap_or(true);
        if need_new_pack {
            self.pack_buffers.push(BytesMut::new());
            self.pack_ids
                .push(self.db.rand.rng().gen_ulid(self.db.system_clock.as_ref()));
        }
        let pack_idx = self.pack_buffers.len() - 1;
        let offset =
            u32::try_from(self.pack_buffers[pack_idx].len()).expect("pack offset overflows u32");
        let length = u32::try_from(value.len()).expect("blob value size overflows u32");
        self.pack_buffers[pack_idx].extend_from_slice(&value);
        let pack_id = self.pack_ids[pack_idx];
        entry.value = ValueDeletable::BlobRef(BlobRef::new(pack_id, offset, length));
    }

    /// Upload every assembled pack concurrently and return the matching
    /// `PackFile` records. `live_bytes` is initialized to `total_bytes`
    /// because every byte is backed by exactly one freshly-stamped
    /// `BlobRef` in the SSTs being built. Concurrency is bounded by
    /// `blob_options.flush_concurrency`.
    async fn upload_all(self) -> Result<Vec<PackFile>, SlateDBError> {
        if self.pack_buffers.is_empty() {
            return Ok(Vec::new());
        }
        let opts = self
            .blob_options
            .expect("non-empty pack_buffers implies blob_options is set");
        let concurrency = opts.flush_concurrency.max(1);

        let pack_files: Vec<PackFile> = self
            .pack_ids
            .iter()
            .zip(self.pack_buffers.iter())
            .map(|(id, buf)| {
                let total_bytes = buf.len() as u64;
                PackFile {
                    pack_id: *id,
                    total_bytes,
                    live_bytes: total_bytes,
                }
            })
            .collect();

        let table_store = Arc::clone(&self.db.table_store);
        let uploads = self
            .pack_ids
            .into_iter()
            .zip(self.pack_buffers.into_iter().map(BytesMut::freeze))
            .map(move |(id, data)| {
                let table_store = Arc::clone(&table_store);
                Ok::<_, SlateDBError>(async move { table_store.put_pack(id, data).await })
            });

        let mut upload_stream = std::pin::pin!(stream::iter(uploads).try_buffered(concurrency));
        while upload_stream.try_next().await?.is_some() {}

        Ok(pack_files)
    }
}

#[cfg(test)]
mod tests {
    use super::BuiltMemtable;
    use crate::block_iterator::BlockIteratorLatest;
    use crate::config::{BlobOptions, Settings};
    use crate::db::Db;
    use crate::db_state::{SsTableHandle, SsTableId};
    use crate::error::SlateDBError;
    use crate::error::SlateDBError::MergeOperatorMissing;
    use crate::iter::RowEntryIterator;
    use crate::mem_table::WritableKVTable;
    use crate::merge_operator::{MERGE_OPERATOR_FLUSH_PATH, MERGE_OPERATOR_READ_PATH};
    use crate::object_store::memory::InMemory;
    use crate::test_utils::{
        lookup_merge_operator_operands, FixedThreeBytePrefixExtractor, StringConcatMergeOperator,
    };
    use crate::types::{BlobRef, RowEntry, ValueDeletable};
    use bytes::Bytes;
    use rstest::rstest;
    use slatedb_common::metrics::test_recorder_helper;
    use std::sync::Arc;
    use ulid::Ulid;

    async fn setup_test_db_with_merge_operator() -> Db {
        setup_test_db(true).await
    }

    async fn setup_test_db_without_merge_operator() -> Db {
        setup_test_db(false).await
    }

    async fn setup_test_db(set_merge_operator: bool) -> Db {
        setup_test_db_with_settings(set_merge_operator, Settings::default()).await
    }

    async fn setup_test_db_with_settings(set_merge_operator: bool, settings: Settings) -> Db {
        let object_store: Arc<dyn object_store::ObjectStore> = Arc::new(InMemory::new());
        let builder =
            Db::builder("/tmp/test_flush_unsegmented_sst", object_store).with_settings(settings);
        let builder = if set_merge_operator {
            builder.with_merge_operator(Arc::new(StringConcatMergeOperator))
        } else {
            builder
        };
        builder.build().await.unwrap()
    }

    async fn read_sst_entries(
        db: &Db,
        sst_handle: &SsTableHandle,
    ) -> Vec<(Bytes, u64, ValueDeletable)> {
        let index = db
            .inner
            .table_store
            .read_index(sst_handle, true)
            .await
            .unwrap();
        let block_count = index.borrow().block_meta().len();
        let blocks = db
            .inner
            .table_store
            .read_blocks(sst_handle, 0..block_count)
            .await
            .unwrap();
        let mut found_entries = Vec::new();
        for block in blocks {
            let mut block_iter = BlockIteratorLatest::new_ascending(block);
            block_iter.init().await.unwrap();

            while let Some(entry) = block_iter.next().await.unwrap() {
                found_entries.push((entry.key.clone(), entry.seq, entry.value.clone()));
            }
        }
        found_entries
    }

    async fn verify_sst(
        db: &Db,
        sst_handle: &SsTableHandle,
        entries: &[(Bytes, u64, ValueDeletable)],
    ) {
        let found_entries = read_sst_entries(db, sst_handle).await;
        assert_eq!(entries.len(), found_entries.len());
        for i in 0..found_entries.len() {
            let (actual_key, actual_seq, actual_value) = &found_entries[i];
            let (expected_key, expected_seq, expected_value) = &entries[i];
            assert_eq!(expected_key, actual_key);
            assert_eq!(expected_seq, actual_seq);
            assert_eq!(expected_value, actual_value);
        }
    }

    struct FlushImmTableTestCase {
        min_active_seq: u64,
        row_entries: Vec<RowEntry>,
        expected_entries: Vec<(Bytes, u64, ValueDeletable)>,
    }

    #[rstest]
    #[case::flush_empty_table(FlushImmTableTestCase {
        min_active_seq: 0,
        row_entries: vec![],
        expected_entries: vec![],
    })]
    #[case::flush_single_entry(FlushImmTableTestCase {
        min_active_seq: 0,
        row_entries: vec![
            RowEntry::new_value(b"key1", b"value1", 1),
        ],
        expected_entries: vec![
            (Bytes::from("key1"), 1, ValueDeletable::Value(Bytes::from("value1"))),
        ],
    })]
    #[case::flush_multiple_unique_keys(FlushImmTableTestCase {
        min_active_seq: 0,
        row_entries: vec![
            RowEntry::new_value(b"key1", b"value1", 1),
            RowEntry::new_value(b"key2", b"value2", 2),
            RowEntry::new_value(b"key3", b"value3", 3),
        ],
        expected_entries: vec![
            (Bytes::from("key1"), 1, ValueDeletable::Value(Bytes::from("value1"))),
            (Bytes::from("key2"), 2, ValueDeletable::Value(Bytes::from("value2"))),
            (Bytes::from("key3"), 3, ValueDeletable::Value(Bytes::from("value3"))),
        ],
    })]
    #[case::flush_all_seqs(FlushImmTableTestCase {
        min_active_seq: 0,
        row_entries: vec![
            RowEntry::new_value(&Bytes::from("key"), b"value1", 1),
            RowEntry::new_value(&Bytes::from("key"), b"value3", 3),
            RowEntry::new_value(&Bytes::from("key"), b"value2", 2),
        ],
        expected_entries: vec![
            (Bytes::from("key"), 3, ValueDeletable::Value(Bytes::from("value3"))),
            (Bytes::from("key"), 2, ValueDeletable::Value(Bytes::from("value2"))),
            (Bytes::from("key"), 1, ValueDeletable::Value(Bytes::from("value1"))),
        ],
    })]
    #[case::flush_some_highest_seqs(FlushImmTableTestCase {
        min_active_seq: 2,
        row_entries: vec![
            RowEntry::new_value(&Bytes::from("key"), b"value1", 1),
            RowEntry::new_value(&Bytes::from("key"), b"value3", 3),
            RowEntry::new_value(&Bytes::from("key"), b"value2", 2),
        ],
        expected_entries: vec![
            (Bytes::from("key"), 3, ValueDeletable::Value(Bytes::from("value3"))),
            (Bytes::from("key"), 2, ValueDeletable::Value(Bytes::from("value2"))),
        ],
    })]
    #[case::flush_only_highest_seq(FlushImmTableTestCase {
        min_active_seq: 3,
        row_entries: vec![
            RowEntry::new_value(&Bytes::from("key"), b"value1", 1),
            RowEntry::new_value(&Bytes::from("key"), b"value3", 3),
            RowEntry::new_value(&Bytes::from("key"), b"value2", 2),
        ],
        expected_entries: vec![
            (Bytes::from("key"), 3, ValueDeletable::Value(Bytes::from("value3")))
        ],
    })]
    #[case::flush_highest_seqs_multiple_key(FlushImmTableTestCase {
        min_active_seq: 6,
        row_entries: vec![
            RowEntry::new_value(&Bytes::from("key1"), b"value1", 1),
            RowEntry::new_value(&Bytes::from("key1"), b"value2", 2),
            RowEntry::new_value(&Bytes::from("key2"), b"value3", 3),
            RowEntry::new_value(&Bytes::from("key3"), b"value4", 4),
            RowEntry::new_value(&Bytes::from("key1"), b"value5", 5),
            RowEntry::new_value(&Bytes::from("key2"), b"value6", 6),
        ],
        expected_entries: vec![
            // This is the expected results, because for each key slate needs to
            // a value at or before the min_active_seq
            // (see retention_iterator for more details)
            (Bytes::from("key1"), 5, ValueDeletable::Value(Bytes::from("value5"))),
            (Bytes::from("key2"), 6, ValueDeletable::Value(Bytes::from("value6"))),
            (Bytes::from("key3"), 4, ValueDeletable::Value(Bytes::from("value4"))),
        ],
    })]
    #[case::flush_tombstones(FlushImmTableTestCase {
        min_active_seq: 5,
        row_entries: vec![
            RowEntry::new_value(&Bytes::from("key1"), b"value1", 1),
            RowEntry::new_tombstone(&Bytes::from("key1"), 2),
            RowEntry::new_tombstone(&Bytes::from("key2"), 3),
            RowEntry::new_tombstone(&Bytes::from("key3"), 4),
            RowEntry::new_value(&Bytes::from("key3"), b"value3", 5),
            RowEntry::new_tombstone(&Bytes::from("key2"), 6),
        ],
        expected_entries: vec![
            (Bytes::from("key1"), 2, ValueDeletable::Tombstone),
            (Bytes::from("key2"), 6, ValueDeletable::Tombstone),
            (Bytes::from("key2"), 3, ValueDeletable::Tombstone),
            (Bytes::from("key3"), 5, ValueDeletable::Value(Bytes::from("value3"))),
        ],
    })]
    #[case::flush_merges_with_earlier_active_seqs(FlushImmTableTestCase {
        min_active_seq: 0,
        row_entries: vec![
            RowEntry::new_merge(&Bytes::from("key1"), b"value1", 1),
            RowEntry::new_value(&Bytes::from("key2"), b"value2", 2),
            RowEntry::new_merge(&Bytes::from("key1"), b"value3", 3),
            RowEntry::new_merge(&Bytes::from("key3"), b"value4", 4),
            RowEntry::new_merge(&Bytes::from("key2"), b"value5", 5),
            RowEntry::new_value(&Bytes::from("key3"), b"value6", 6),
        ],
        expected_entries: vec![
            (Bytes::from("key1"), 3, ValueDeletable::Merge(Bytes::from("value3"))),
            (Bytes::from("key1"), 1, ValueDeletable::Merge(Bytes::from("value1"))),
            (Bytes::from("key2"), 5, ValueDeletable::Merge(Bytes::from("value5"))),
            (Bytes::from("key2"), 2, ValueDeletable::Value(Bytes::from("value2"))),
            (Bytes::from("key3"), 6, ValueDeletable::Value(Bytes::from("value6"))),
            (Bytes::from("key3"), 4, ValueDeletable::Merge(Bytes::from("value4"))),
        ],
    })]
    #[case::flush_merges_and_tombstones(FlushImmTableTestCase {
        min_active_seq: 0,
        row_entries: vec![
            RowEntry::new_merge(&Bytes::from("key1"), b"value1", 1),
            RowEntry::new_value(&Bytes::from("key2"), b"value2", 2),
            RowEntry::new_merge(&Bytes::from("key1"), b"value3", 3),
            RowEntry::new_tombstone(&Bytes::from("key1"), 4),
            RowEntry::new_merge(&Bytes::from("key3"), b"value4", 5),
            RowEntry::new_merge(&Bytes::from("key2"), b"value5", 6),
            RowEntry::new_value(&Bytes::from("key3"), b"value6", 7),
            RowEntry::new_tombstone(&Bytes::from("key3"), 8),
        ],
        expected_entries: vec![
            (Bytes::from("key1"), 4, ValueDeletable::Tombstone),
            (Bytes::from("key1"), 3, ValueDeletable::Merge(Bytes::from("value3"))),
            (Bytes::from("key1"), 1, ValueDeletable::Merge(Bytes::from("value1"))),
            (Bytes::from("key2"), 6, ValueDeletable::Merge(Bytes::from("value5"))),
            (Bytes::from("key2"), 2, ValueDeletable::Value(Bytes::from("value2"))),
            (Bytes::from("key3"), 8, ValueDeletable::Tombstone),
            (Bytes::from("key3"), 7, ValueDeletable::Value(Bytes::from("value6"))),
            (Bytes::from("key3"), 5, ValueDeletable::Merge(Bytes::from("value4"))),
        ],
    })]
    #[case::flush_merges_with_recent_active_seqs(FlushImmTableTestCase {
        min_active_seq: 6,
        row_entries: vec![
            RowEntry::new_merge(&Bytes::from("key1"), b"value1", 1),
            RowEntry::new_value(&Bytes::from("key2"), b"value2", 2),
            RowEntry::new_merge(&Bytes::from("key1"), b"value3", 3),
            RowEntry::new_merge(&Bytes::from("key3"), b"value4", 4),
            RowEntry::new_merge(&Bytes::from("key2"), b"value5", 5),
            RowEntry::new_value(&Bytes::from("key3"), b"value6", 6),
        ],
        expected_entries: vec![
            (Bytes::from("key1"), 3, ValueDeletable::Merge(Bytes::from("value1value3"))),
            (Bytes::from("key2"), 5, ValueDeletable::Value(Bytes::from("value2value5"))),
            (Bytes::from("key3"), 6, ValueDeletable::Value(Bytes::from("value6"))),
        ],
    })]
    #[tokio::test]
    async fn test_flush(#[case] test_case: FlushImmTableTestCase) {
        // Given
        let db = setup_test_db_with_merge_operator().await;
        db.inner
            .snapshot_manager
            .new_snapshot(Some(test_case.min_active_seq));
        // Set durable watermark high so it doesn't interfere with transaction-based retention tests
        db.inner.oracle.advance_durable_seq(u64::MAX);
        let table = WritableKVTable::new();
        let row_entries_length = test_case.row_entries.len();
        for row_entry in test_case.row_entries {
            table.put(row_entry);
        }
        assert_eq!(table.table().metadata().entry_num, row_entries_length);

        // When
        let handles = db
            .inner
            .flush_l0_for_test(table.table().clone(), false)
            .await
            .unwrap();

        // Then
        if test_case.expected_entries.is_empty() {
            assert!(
                handles.is_empty(),
                "expected no SSTs for empty post-retention memtable"
            );
        } else {
            let sst_handle = handles.into_iter().next().expect("expected single SST");
            verify_sst(&db, &sst_handle, &test_case.expected_entries).await;
        }

        db.close().await.unwrap();
    }

    #[tokio::test]
    async fn should_record_merge_operator_operands_on_flush_path() {
        let (metrics_recorder, _) = test_recorder_helper();
        let object_store: Arc<dyn object_store::ObjectStore> = Arc::new(InMemory::new());
        let db = Db::builder("/tmp/test_merge_operands_flush", object_store)
            .with_metrics_recorder(metrics_recorder.clone())
            .with_merge_operator(Arc::new(StringConcatMergeOperator))
            .build()
            .await
            .unwrap();

        db.inner.oracle.advance_durable_seq(u64::MAX);

        let table = WritableKVTable::new();
        table.put(RowEntry::new_merge(&Bytes::from("key1"), b"a", 1));
        table.put(RowEntry::new_merge(&Bytes::from("key1"), b"b", 2));

        assert_eq!(
            lookup_merge_operator_operands(metrics_recorder.as_ref(), MERGE_OPERATOR_READ_PATH),
            Some(0)
        );
        assert_eq!(
            lookup_merge_operator_operands(metrics_recorder.as_ref(), MERGE_OPERATOR_FLUSH_PATH,),
            Some(0)
        );

        db.inner
            .flush_l0_for_test(table.table().clone(), false)
            .await
            .unwrap();

        assert_eq!(
            lookup_merge_operator_operands(metrics_recorder.as_ref(), MERGE_OPERATOR_READ_PATH),
            Some(0)
        );
        assert_eq!(
            lookup_merge_operator_operands(metrics_recorder.as_ref(), MERGE_OPERATOR_FLUSH_PATH,),
            // Two raw merge rows produce one intermediate batch result and one
            // final merge_batch call over that result.
            Some(3)
        );

        db.close().await.unwrap();
    }

    #[tokio::test]
    async fn test_err_when_merge_operator_not_set_and_merges_exist() {
        // Given
        let db = setup_test_db_without_merge_operator().await;
        db.inner.snapshot_manager.new_snapshot(Some(0));
        let table = WritableKVTable::new();
        table.put(RowEntry::new_value(&Bytes::from("key"), b"value1", 1));
        table.put(RowEntry::new_merge(&Bytes::from("key"), b"value2", 2));

        // When
        db.inner
            .flush_l0_for_test(table.table().clone(), false)
            .await
            .map_or_else(
                |err| match err {
                    MergeOperatorMissing => Ok::<(), SlateDBError>(()),
                    _ => panic!("Should return MergeOperatorMissing error"),
                },
                |_| panic!("Should return MergeOperatorMissing error"),
            )
            .unwrap();
    }

    #[tokio::test]
    async fn test_no_err_merge_operator_not_set_and_no_merges() {
        // Given
        let db = setup_test_db_without_merge_operator().await;
        db.inner.snapshot_manager.new_snapshot(Some(0));
        let table = WritableKVTable::new();
        table.put(RowEntry::new_value(&Bytes::from("key1"), b"value1", 1));
        table.put(RowEntry::new_tombstone(&Bytes::from("key2"), 2));

        // When
        db.inner
            .flush_l0_for_test(table.table().clone(), false)
            .await
            .unwrap();
    }

    struct RetentionBoundaryTestCase {
        durable_seq: u64,
        snapshot_seq: Option<u64>,
        txn_seq: Option<u64>,
        expected_entries: Vec<(Bytes, u64, ValueDeletable)>,
    }

    #[rstest]
    #[case::durable_is_min(RetentionBoundaryTestCase {
        durable_seq: 1,
        snapshot_seq: Some(3),
        txn_seq: Some(2),
        expected_entries: vec![
            (Bytes::from("key"), 4, ValueDeletable::Value(Bytes::from("value4"))),
            (Bytes::from("key"), 3, ValueDeletable::Value(Bytes::from("value3"))),
            (Bytes::from("key"), 2, ValueDeletable::Value(Bytes::from("value2"))),
            (Bytes::from("key"), 1, ValueDeletable::Value(Bytes::from("value1"))),
        ],
    })]
    #[case::snapshot_is_min(RetentionBoundaryTestCase {
        durable_seq: 4,
        snapshot_seq: Some(2),
        txn_seq: Some(3),
        expected_entries: vec![
            (Bytes::from("key"), 4, ValueDeletable::Value(Bytes::from("value4"))),
            (Bytes::from("key"), 3, ValueDeletable::Value(Bytes::from("value3"))),
            (Bytes::from("key"), 2, ValueDeletable::Value(Bytes::from("value2"))),
        ],
    })]
    #[case::txn_is_min(RetentionBoundaryTestCase {
        durable_seq: 4,
        snapshot_seq: Some(3),
        txn_seq: Some(2),
        expected_entries: vec![
            (Bytes::from("key"), 4, ValueDeletable::Value(Bytes::from("value4"))),
            (Bytes::from("key"), 3, ValueDeletable::Value(Bytes::from("value3"))),
            (Bytes::from("key"), 2, ValueDeletable::Value(Bytes::from("value2"))),
        ],
    })]
    #[case::snapshot_is_none(RetentionBoundaryTestCase {
        durable_seq: 4,
        snapshot_seq: None,
        txn_seq: Some(2),
        expected_entries: vec![
            (Bytes::from("key"), 4, ValueDeletable::Value(Bytes::from("value4"))),
            (Bytes::from("key"), 3, ValueDeletable::Value(Bytes::from("value3"))),
            (Bytes::from("key"), 2, ValueDeletable::Value(Bytes::from("value2"))),
        ],
    })]
    #[case::txn_is_none(RetentionBoundaryTestCase {
        durable_seq: 4,
        snapshot_seq: Some(3),
        txn_seq: None,
        expected_entries: vec![
            (Bytes::from("key"), 4, ValueDeletable::Value(Bytes::from("value4"))),
            (Bytes::from("key"), 3, ValueDeletable::Value(Bytes::from("value3"))),
        ],
    })]
    #[case::snapshot_and_txn_are_none(RetentionBoundaryTestCase {
        durable_seq: 4,
        snapshot_seq: None,
        txn_seq: None,
        expected_entries: vec![
            (Bytes::from("key"), 4, ValueDeletable::Value(Bytes::from("value4"))),
        ],
    })]
    #[tokio::test]
    async fn should_use_min_of_retention_sources(#[case] test_case: RetentionBoundaryTestCase) {
        let db = setup_test_db_with_merge_operator().await;
        db.inner.oracle.advance_durable_seq(test_case.durable_seq);

        if let Some(snapshot_seq) = test_case.snapshot_seq {
            let (_, started_seq) = db.inner.snapshot_manager.new_snapshot(Some(snapshot_seq));
            assert_eq!(started_seq, snapshot_seq)
        }

        if let Some(txn_seq) = test_case.txn_seq {
            db.inner.oracle.advance_committed_seq(txn_seq);
            let (_, started_seq) = db.inner.txn_manager.new_transaction();
            assert_eq!(started_seq, txn_seq);
        }

        let table = WritableKVTable::new();
        table.put(RowEntry::new_value(&Bytes::from("key"), b"value1", 1));
        table.put(RowEntry::new_value(&Bytes::from("key"), b"value2", 2));
        table.put(RowEntry::new_value(&Bytes::from("key"), b"value3", 3));
        table.put(RowEntry::new_value(&Bytes::from("key"), b"value4", 4));

        let handles = db
            .inner
            .flush_l0_for_test(table.table().clone(), false)
            .await
            .unwrap();
        let sst_handle = handles.into_iter().next().expect("expected single SST");

        verify_sst(&db, &sst_handle, &test_case.expected_entries).await;
        db.close().await.unwrap();
    }

    async fn setup_test_db_with_extractor(
        path: &str,
        extractor: Arc<dyn crate::prefix_extractor::PrefixExtractor>,
    ) -> Db {
        let object_store: Arc<dyn object_store::ObjectStore> = Arc::new(InMemory::new());
        Db::builder(path, object_store)
            .with_segment_extractor(extractor)
            .build()
            .await
            .unwrap()
    }

    #[tokio::test]
    async fn build_imm_ssts_without_extractor_emits_single_empty_prefix() {
        let db = setup_test_db_without_merge_operator().await;
        db.inner.oracle.advance_durable_seq(u64::MAX);
        let table = WritableKVTable::new();
        table.put(RowEntry::new_value(b"k1", b"v1", 1));
        table.put(RowEntry::new_value(b"k2", b"v2", 2));

        let built = db
            .inner
            .build_imm_ssts(table.table().clone())
            .await
            .unwrap();

        assert_eq!(built.ssts.len(), 1);
        assert!(built.ssts[0].prefix.is_empty());
        db.close().await.unwrap();
    }

    #[tokio::test]
    async fn build_imm_ssts_with_extractor_yields_empty_vec_when_no_entries() {
        // With an extractor configured, an empty memtable produces no
        // entries and therefore opens no builders — the result is an
        // empty Vec.
        let db = setup_test_db_with_extractor(
            "/tmp/test_build_imm_ssts_empty",
            Arc::new(FixedThreeBytePrefixExtractor),
        )
        .await;
        db.inner.oracle.advance_durable_seq(u64::MAX);
        let table = WritableKVTable::new();

        let built = db
            .inner
            .build_imm_ssts(table.table().clone())
            .await
            .unwrap();

        assert!(built.ssts.is_empty());
        db.close().await.unwrap();
    }

    #[tokio::test]
    async fn build_imm_ssts_without_extractor_yields_empty_vec_when_no_entries() {
        // Without an extractor configured, an empty memtable also yields
        // an empty Vec — symmetric with the extractor case. Manifest
        // progress (last_l0_seq, replay frontier) advances independently.
        let db = setup_test_db_without_merge_operator().await;
        db.inner.oracle.advance_durable_seq(u64::MAX);
        let table = WritableKVTable::new();

        let built = db
            .inner
            .build_imm_ssts(table.table().clone())
            .await
            .unwrap();

        assert!(built.ssts.is_empty());
        db.close().await.unwrap();
    }

    #[tokio::test]
    async fn build_imm_ssts_with_extractor_groups_by_prefix() {
        let db = setup_test_db_with_extractor(
            "/tmp/test_build_imm_ssts_groups",
            Arc::new(FixedThreeBytePrefixExtractor),
        )
        .await;
        db.inner.oracle.advance_durable_seq(u64::MAX);
        let table = WritableKVTable::new();
        // Sorted within and across prefixes.
        table.put(RowEntry::new_value(b"aaa-1", b"v1", 1));
        table.put(RowEntry::new_value(b"aaa-2", b"v2", 2));
        table.put(RowEntry::new_value(b"bbb-1", b"v3", 3));
        table.put(RowEntry::new_value(b"ccc-1", b"v4", 4));
        table.put(RowEntry::new_value(b"ccc-2", b"v5", 5));

        let built = db
            .inner
            .build_imm_ssts(table.table().clone())
            .await
            .unwrap();

        let prefixes: Vec<&[u8]> = built.ssts.iter().map(|s| s.prefix.as_ref()).collect();
        assert_eq!(prefixes, vec![&b"aaa"[..], &b"bbb"[..], &b"ccc"[..]]);

        // Upload each SST and verify it carries exactly its prefix's entries.
        let expected: Vec<Vec<(Bytes, u64, ValueDeletable)>> = vec![
            vec![
                (
                    Bytes::from("aaa-1"),
                    1,
                    ValueDeletable::Value(Bytes::from("v1")),
                ),
                (
                    Bytes::from("aaa-2"),
                    2,
                    ValueDeletable::Value(Bytes::from("v2")),
                ),
            ],
            vec![(
                Bytes::from("bbb-1"),
                3,
                ValueDeletable::Value(Bytes::from("v3")),
            )],
            vec![
                (
                    Bytes::from("ccc-1"),
                    4,
                    ValueDeletable::Value(Bytes::from("v4")),
                ),
                (
                    Bytes::from("ccc-2"),
                    5,
                    ValueDeletable::Value(Bytes::from("v5")),
                ),
            ],
        ];
        for (sst, entries) in built.ssts.into_iter().zip(expected.into_iter()) {
            let id = SsTableId::Compacted(Ulid::new());
            let handle = db
                .inner
                .upload_sst(&id, table.table().clone(), &sst.encoded, false)
                .await
                .unwrap();
            verify_sst(&db, &handle, &entries).await;
        }
        db.close().await.unwrap();
    }

    #[tokio::test]
    async fn build_imm_ssts_with_extractor_single_segment_yields_one() {
        let db = setup_test_db_with_extractor(
            "/tmp/test_build_imm_ssts_single",
            Arc::new(FixedThreeBytePrefixExtractor),
        )
        .await;
        db.inner.oracle.advance_durable_seq(u64::MAX);
        let table = WritableKVTable::new();
        table.put(RowEntry::new_value(b"aaa-1", b"v1", 1));
        table.put(RowEntry::new_value(b"aaa-2", b"v2", 2));

        let built = db
            .inner
            .build_imm_ssts(table.table().clone())
            .await
            .unwrap();

        assert_eq!(built.ssts.len(), 1);
        assert_eq!(built.ssts[0].prefix.as_ref(), b"aaa");
        db.close().await.unwrap();
    }

    async fn setup_test_db_with_blob_options(blob_options: BlobOptions) -> Db {
        let settings = Settings {
            blob_options: Some(blob_options),
            ..Settings::default()
        };
        // Snapshots/transactions otherwise block retention from passing the
        // memtable contents through unchanged in tests; advance durable seq
        // happens explicitly per-test.
        setup_test_db_with_settings(false, settings).await
    }

    /// Collect the entries in the first SST handle returned by uploading the
    /// first segment of `built`. Only used by the pack-assembly tests, which
    /// always produce at most one segment.
    async fn build_and_upload_single_sst(
        db: &Db,
        table: &WritableKVTable,
    ) -> (BuiltMemtable, Option<SsTableHandle>) {
        let built = db
            .inner
            .build_imm_ssts(table.table().clone())
            .await
            .unwrap();
        let handle = match built.ssts.first() {
            Some(sst) => {
                let id = SsTableId::Compacted(Ulid::new());
                Some(
                    db.inner
                        .upload_sst(&id, table.table().clone(), &sst.encoded, false)
                        .await
                        .unwrap(),
                )
            }
            None => None,
        };
        (built, handle)
    }

    /// Verify a pack file matches the expected bytes via a full-range GET
    /// against the table store. Catches both wrong content and wrong
    /// total_bytes accounting in `PackFile`.
    async fn assert_pack_contents(db: &Db, pack_id: Ulid, expected: &[u8]) {
        let fetched = db
            .inner
            .table_store
            .get_pack_range(pack_id, 0, expected.len() as u32)
            .await
            .unwrap();
        assert_eq!(fetched.as_ref(), expected);
    }

    #[tokio::test]
    async fn pack_assembler_externalizes_only_large_values() {
        // min_value_size=8 forces the boundary between inline (Value) and
        // externalized (BlobRef) values: anything <8 bytes must stay inline,
        // anything >=8 bytes must externalize.
        let db = setup_test_db_with_blob_options(BlobOptions {
            min_value_size: 8,
            ..BlobOptions::default()
        })
        .await;
        db.inner.oracle.advance_durable_seq(u64::MAX);

        let small_a: &[u8] = b"abc";
        let small_b: &[u8] = b"de";
        let large_a: &[u8] = b"0123456789ABCDEF";
        let large_b: &[u8] = b"GHIJKLMNOPQRSTUV";
        let table = WritableKVTable::new();
        table.put(RowEntry::new_value(b"k1", small_a, 1));
        table.put(RowEntry::new_value(b"k2", large_a, 2));
        table.put(RowEntry::new_value(b"k3", small_b, 3));
        table.put(RowEntry::new_value(b"k4", large_b, 4));

        let (built, handle) = build_and_upload_single_sst(&db, &table).await;
        let handle = handle.expect("expected one SST from non-empty flush");

        // Exactly one pack — total externalized bytes (32) fit in the
        // default target_pack_size_bytes (64 MiB).
        assert_eq!(built.pack_files.len(), 1);
        let pack = &built.pack_files[0];
        assert_eq!(pack.total_bytes, (large_a.len() + large_b.len()) as u64);
        assert_eq!(pack.live_bytes, pack.total_bytes);

        let entries = read_sst_entries(&db, &handle).await;
        // Sorted ascending by key — k1, k2, k3, k4.
        assert_eq!(entries[0].2, ValueDeletable::Value(Bytes::from(small_a)));
        assert_eq!(entries[2].2, ValueDeletable::Value(Bytes::from(small_b)));
        let ValueDeletable::BlobRef(ref br_a) = entries[1].2 else {
            panic!("k2 expected BlobRef, got {:?}", entries[1].2);
        };
        let ValueDeletable::BlobRef(ref br_b) = entries[3].2 else {
            panic!("k4 expected BlobRef, got {:?}", entries[3].2);
        };
        assert_eq!(br_a.pack_id, pack.pack_id);
        assert_eq!(br_b.pack_id, pack.pack_id);
        assert_eq!(br_a.length, large_a.len() as u32);
        assert_eq!(br_b.length, large_b.len() as u32);
        // The retention iterator yields keys in sorted order, so large_a is
        // packed first (offset=0) and large_b follows immediately after.
        assert_eq!(br_a.offset, 0);
        assert_eq!(br_b.offset, large_a.len() as u32);

        let mut expected_pack = Vec::new();
        expected_pack.extend_from_slice(large_a);
        expected_pack.extend_from_slice(large_b);
        assert_pack_contents(&db, pack.pack_id, &expected_pack).await;

        db.close().await.unwrap();
    }

    #[tokio::test]
    async fn pack_assembler_splits_pack_at_target_size() {
        // target_pack_size_bytes=20 with three 10-byte values:
        //   buf=10  → add 10 → 20 (== target, not > target) fits.
        //   buf=20  → add 10 → 30  > 20 → seal, open new pack.
        // Expected: 2 packs, the first holding two values (20 bytes), the
        // second holding the trailing value (10 bytes).
        let db = setup_test_db_with_blob_options(BlobOptions {
            min_value_size: 4,
            target_pack_size_bytes: 20,
            ..BlobOptions::default()
        })
        .await;
        db.inner.oracle.advance_durable_seq(u64::MAX);

        let v1 = b"v1_aaaaaa".to_vec();
        let v1 = {
            let mut v = v1;
            v.push(b'!');
            v
        }; // 10 bytes
        let v2 = b"v2_bbbbbbb!".as_slice()[..10].to_vec(); // 10 bytes
        let v3 = b"v3_ccccccc!".as_slice()[..10].to_vec(); // 10 bytes
        assert_eq!(v1.len(), 10);
        assert_eq!(v2.len(), 10);
        assert_eq!(v3.len(), 10);

        let table = WritableKVTable::new();
        table.put(RowEntry::new_value(b"k1", &v1, 1));
        table.put(RowEntry::new_value(b"k2", &v2, 2));
        table.put(RowEntry::new_value(b"k3", &v3, 3));

        let (built, handle) = build_and_upload_single_sst(&db, &table).await;
        let handle = handle.expect("expected one SST");

        assert_eq!(built.pack_files.len(), 2);
        let pack1 = &built.pack_files[0];
        let pack2 = &built.pack_files[1];
        assert_eq!(pack1.total_bytes, 20);
        assert_eq!(pack1.live_bytes, 20);
        assert_eq!(pack2.total_bytes, 10);
        assert_eq!(pack2.live_bytes, 10);

        let entries = read_sst_entries(&db, &handle).await;
        let refs: Vec<&BlobRef> = entries
            .iter()
            .map(|(_, _, v)| match v {
                ValueDeletable::BlobRef(br) => br,
                other => panic!("expected BlobRef, got {other:?}"),
            })
            .collect();
        assert_eq!(refs[0].pack_id, pack1.pack_id);
        assert_eq!(refs[0].offset, 0);
        assert_eq!(refs[1].pack_id, pack1.pack_id);
        assert_eq!(refs[1].offset, 10);
        assert_eq!(refs[2].pack_id, pack2.pack_id);
        assert_eq!(refs[2].offset, 0);

        let mut pack1_expected = Vec::new();
        pack1_expected.extend_from_slice(&v1);
        pack1_expected.extend_from_slice(&v2);
        assert_pack_contents(&db, pack1.pack_id, &pack1_expected).await;
        assert_pack_contents(&db, pack2.pack_id, &v3).await;

        db.close().await.unwrap();
    }

    #[tokio::test]
    async fn pack_assembler_oversized_value_gets_own_pack() {
        // A single value larger than `target_pack_size_bytes` still gets
        // externalized — the seal condition only opens a new pack, it never
        // refuses to store a value. The pack ends up larger than the
        // target, which is the expected behavior per the doc on
        // `BlobOptions::target_pack_size_bytes`.
        let db = setup_test_db_with_blob_options(BlobOptions {
            min_value_size: 4,
            target_pack_size_bytes: 10,
            ..BlobOptions::default()
        })
        .await;
        db.inner.oracle.advance_durable_seq(u64::MAX);

        let big = vec![0xABu8; 64];
        let table = WritableKVTable::new();
        table.put(RowEntry::new_value(b"k", &big, 1));

        let (built, handle) = build_and_upload_single_sst(&db, &table).await;
        let handle = handle.expect("expected one SST");

        assert_eq!(built.pack_files.len(), 1);
        let pack = &built.pack_files[0];
        assert_eq!(pack.total_bytes, big.len() as u64);
        assert_eq!(pack.live_bytes, big.len() as u64);

        let entries = read_sst_entries(&db, &handle).await;
        let ValueDeletable::BlobRef(ref br) = entries[0].2 else {
            panic!("expected BlobRef, got {:?}", entries[0].2);
        };
        assert_eq!(br.pack_id, pack.pack_id);
        assert_eq!(br.offset, 0);
        assert_eq!(br.length, big.len() as u32);

        assert_pack_contents(&db, pack.pack_id, &big).await;

        db.close().await.unwrap();
    }

    #[tokio::test]
    async fn pack_assembler_disabled_when_blob_options_unset() {
        // No `blob_options` in Settings → `PackAssembler::maybe_externalize`
        // is a no-op, values stay inline, and no packs are uploaded.
        let db = setup_test_db_without_merge_operator().await;
        db.inner.oracle.advance_durable_seq(u64::MAX);

        let big = vec![0xCDu8; 128];
        let table = WritableKVTable::new();
        table.put(RowEntry::new_value(b"k", &big, 1));

        let (built, handle) = build_and_upload_single_sst(&db, &table).await;
        let handle = handle.expect("expected one SST");

        assert!(built.pack_files.is_empty());
        let entries = read_sst_entries(&db, &handle).await;
        assert_eq!(entries[0].2, ValueDeletable::Value(Bytes::from(big)));

        db.close().await.unwrap();
    }

    #[tokio::test]
    async fn pack_assembler_does_not_externalize_merges_or_tombstones() {
        // Only `ValueDeletable::Value` payloads count toward externalization.
        // A merge operand or tombstone — even one larger than
        // `min_value_size` — must pass through untouched and contribute
        // nothing to packs.
        let settings = Settings {
            blob_options: Some(BlobOptions {
                min_value_size: 4,
                ..BlobOptions::default()
            }),
            ..Settings::default()
        };
        let db = setup_test_db_with_settings(true, settings).await;
        db.inner.oracle.advance_durable_seq(u64::MAX);

        let big_merge = vec![0x11u8; 32];
        let table = WritableKVTable::new();
        table.put(RowEntry::new_merge(b"k_merge", &big_merge, 1));
        table.put(RowEntry::new_tombstone(b"k_tomb", 2));

        let built = db
            .inner
            .build_imm_ssts(table.table().clone())
            .await
            .unwrap();
        assert!(built.pack_files.is_empty());

        let id = SsTableId::Compacted(Ulid::new());
        let handle = db
            .inner
            .upload_sst(&id, table.table().clone(), &built.ssts[0].encoded, false)
            .await
            .unwrap();
        let entries = read_sst_entries(&db, &handle).await;
        assert_eq!(entries[0].2, ValueDeletable::Merge(Bytes::from(big_merge)));
        assert_eq!(entries[1].2, ValueDeletable::Tombstone);

        db.close().await.unwrap();
    }
}
