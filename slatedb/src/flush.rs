use crate::db::DbInner;
use crate::db_state;
use crate::db_state::SsTableHandle;
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
use bytes::Bytes;
use std::sync::Arc;

/// One encoded-but-not-yet-uploaded SST from a memtable flush, tagged with
/// the segment it belongs to (RFC-0024). Mirrors the shape of post-upload
/// [`crate::memtable_flusher::uploader::SegmentedSstHandle`].
pub(crate) struct EncodedSegmentSst {
    pub(crate) prefix: Bytes,
    pub(crate) encoded: EncodedSsTable,
}

impl DbInner {
    /// Build a single SST from an immutable memtable, ignoring any segment
    /// extractor. Returns `None` when the post-retention iterator yields
    /// zero entries — callers that want a real blob (e.g. the WAL fence)
    /// should construct one explicitly rather than relying on this path.
    /// For L0 flushes use [`Self::build_imm_ssts`] instead, which routes
    /// entries through segment-aware builders.
    async fn build_imm_sst(
        &self,
        imm_table: Arc<KVTable>,
    ) -> Result<Option<EncodedSsTable>, SlateDBError> {
        let mut sst_builder = self.table_store.table_builder();
        let mut iter = self.iter_imm_table(imm_table).await?;
        let mut any = false;
        while let Some(entry) = iter.next().await? {
            sst_builder.add(entry).await?;
            any = true;
        }
        if !any {
            return Ok(None);
        }
        Ok(Some(sst_builder.build().await?))
    }

    /// Build one or more L0 SSTs from a single immutable memtable, grouping
    /// entries by the segment prefix derived from the configured extractor.
    ///
    /// Returns one `(prefix, EncodedSsTable)` per segment that received at
    /// least one post-retention entry, sorted ascending by `prefix`. The
    /// memtable iterator yields keys in sorted order and segments own
    /// disjoint key intervals, so all entries for a given prefix arrive
    /// consecutively — the implementation streams one open builder at a
    /// time, finalizing on prefix transitions.
    ///
    /// When no extractor is configured, every entry routes to the empty
    /// prefix and the result is at most one entry. If retention prunes
    /// every entry the result is an empty Vec in both the extractor and
    /// no-extractor cases — per-memtable progress in the manifest
    /// (`last_l0_seq`, `replay_after_wal_id`) advances independently of
    /// whether any SST landed.
    pub(crate) async fn build_imm_ssts(
        &self,
        imm_table: Arc<KVTable>,
    ) -> Result<Vec<EncodedSegmentSst>, SlateDBError> {
        let Some(extractor) = self.segment_extractor.as_ref() else {
            return Ok(self
                .build_imm_sst(imm_table)
                .await?
                .into_iter()
                .map(|encoded| EncodedSegmentSst {
                    prefix: Bytes::new(),
                    encoded,
                })
                .collect());
        };
        let mut iter = self.iter_imm_table(imm_table).await?;
        let mut out: Vec<EncodedSegmentSst> = Vec::new();
        let mut current: Option<(Bytes, EncodedSsTableBuilder<'_>)> = None;
        while let Some(entry) = iter.next().await? {
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
        use crate::utils::IdGenerator;
        let built = self.build_imm_ssts(imm_table.clone()).await?;
        let mut handles = Vec::with_capacity(built.len());
        for sst in built {
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

    /// Externalizes large `Value` entries by packing them into shared object
    /// files. Walks `entries` in order, segments large values into pack
    /// buffers (sealed when adding the next value would exceed
    /// `target_pack_size_bytes`), uploads packs in parallel, and stamps each
    /// externalized entry's value with a `BlobRef(pack_id, offset, length)`.
    /// A single value larger than `target_pack_size_bytes` lands in its own
    /// pack. Returns the `PackFile` records for each new pack so the caller
    /// can register them in the manifest.
    async fn pack_externalized_values(
        &self,
        entries: &mut [RowEntry],
    ) -> Result<Vec<PackFile>, SlateDBError> {
        let Some(blob_options) = self.settings.blob_options.as_ref() else {
            return Ok(Vec::new());
        };

        let min_value_size = blob_options.min_value_size;
        let target_pack_size = blob_options.target_pack_size_bytes.max(1);
        let concurrency = blob_options.flush_concurrency.max(1);

        let mut pack_buffers: Vec<BytesMut> = Vec::new();
        let mut pack_ids: Vec<Ulid> = Vec::new();
        let mut assignments: Vec<(usize, usize, u32, u32)> = Vec::new();

        for (entry_idx, entry) in entries.iter().enumerate() {
            let value = match &entry.value {
                ValueDeletable::Value(v) if v.len() >= min_value_size => v,
                _ => continue,
            };

            let need_new_pack = pack_buffers
                .last()
                .map(|buf| buf.len() + value.len() > target_pack_size)
                .unwrap_or(true);
            if need_new_pack {
                pack_buffers.push(BytesMut::new());
                pack_ids.push(self.rand.rng().gen_ulid(self.system_clock.as_ref()));
            }

            let pack_idx = pack_buffers.len() - 1;
            let offset =
                u32::try_from(pack_buffers[pack_idx].len()).expect("pack offset overflows u32");
            let length = u32::try_from(value.len()).expect("blob value size overflows u32");
            pack_buffers[pack_idx].extend_from_slice(value);
            assignments.push((entry_idx, pack_idx, offset, length));
        }

        if pack_buffers.is_empty() {
            return Ok(Vec::new());
        }

        let pack_files: Vec<PackFile> = pack_ids
            .iter()
            .zip(pack_buffers.iter())
            .map(|(id, buf)| {
                let total_bytes = buf.len() as u64;
                PackFile {
                    pack_id: *id,
                    total_bytes,
                    live_bytes: total_bytes,
                }
            })
            .collect();

        for &(entry_idx, pack_idx, offset, length) in &assignments {
            let pack_id = pack_ids[pack_idx];
            entries[entry_idx].value =
                ValueDeletable::BlobRef(BlobRef::new(pack_id, offset, length));
        }

        let uploads = pack_ids
            .iter()
            .copied()
            .zip(pack_buffers.into_iter().map(BytesMut::freeze))
            .map(Ok::<(Ulid, Bytes), SlateDBError>);
        let mut upload_stream = std::pin::pin!(stream::iter(uploads)
            .map_ok(|(id, data)| self.table_store.put_pack(id, data))
            .try_buffered(concurrency));
        while upload_stream.try_next().await?.is_some() {}

        Ok(pack_files)
    }
}

#[cfg(test)]
mod tests {
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
    use crate::types::{RowEntry, ValueDeletable};
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
        let builder = Db::builder("/tmp/test_flush_unsegmented_sst", object_store)
            .with_settings(settings);
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

        let ssts = db
            .inner
            .build_imm_ssts(table.table().clone())
            .await
            .unwrap();

        assert_eq!(ssts.len(), 1);
        assert!(ssts[0].prefix.is_empty());
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

        let ssts = db
            .inner
            .build_imm_ssts(table.table().clone())
            .await
            .unwrap();

        assert!(ssts.is_empty());
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

        let ssts = db
            .inner
            .build_imm_ssts(table.table().clone())
            .await
            .unwrap();

        assert!(ssts.is_empty());
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

        let ssts = db
            .inner
            .build_imm_ssts(table.table().clone())
            .await
            .unwrap();

        let prefixes: Vec<&[u8]> = ssts.iter().map(|s| s.prefix.as_ref()).collect();
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
        for (sst, entries) in ssts.into_iter().zip(expected.into_iter()) {
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

        let ssts = db
            .inner
            .build_imm_ssts(table.table().clone())
            .await
            .unwrap();

        assert_eq!(ssts.len(), 1);
        assert_eq!(ssts[0].prefix.as_ref(), b"aaa");
        db.close().await.unwrap();
    }

    #[tokio::test]
    async fn should_externalize_large_values_to_blobs_during_flush() {
        let db = setup_test_db_with_settings(
            false,
            Settings {
                blob_options: Some(BlobOptions::default()),
                ..Settings::default()
            },
        )
        .await;
        db.inner.txn_manager.new_snapshot(Some(0));
        db.inner.oracle.advance_durable_seq(u64::MAX);

        let small_value = Bytes::from("small-value");
        let large_value = Bytes::from(vec![b'x'; 4096]);

        let table = WritableKVTable::new();
        table.put(RowEntry::new_value(b"blob-key", large_value.as_ref(), 2));
        table.put(RowEntry::new_value(b"inline-key", small_value.as_ref(), 1));
        let id = SsTableId::Compacted(Ulid::new());

        let (sst_handle, pack_files) = db
            .inner
            .flush_imm_table(&id, table.table().clone(), false)
            .await
            .unwrap();

        assert_eq!(pack_files.len(), 1);
        assert_eq!(pack_files[0].total_bytes, large_value.len() as u64);
        assert_eq!(pack_files[0].live_bytes, pack_files[0].total_bytes);

        let found_entries = read_sst_entries(&db, &sst_handle).await;
        assert_eq!(found_entries.len(), 2);

        let blob_entry = found_entries
            .iter()
            .find(|(key, _, _)| key.as_ref() == b"blob-key")
            .expect("blob entry should be present");
        assert_eq!(blob_entry.1, 2);

        let blob_ref = match &blob_entry.2 {
            ValueDeletable::BlobRef(blob_ref) => *blob_ref,
            value => panic!("expected blob ref, found {value:?}"),
        };
        assert_eq!(blob_ref.pack_id, pack_files[0].pack_id);
        assert_eq!(blob_ref.offset, 0);
        assert_eq!(blob_ref.length, large_value.len() as u32);
        let stored_blob = db
            .inner
            .table_store
            .get_pack_range(blob_ref.pack_id, blob_ref.offset, blob_ref.length)
            .await
            .unwrap();
        assert_eq!(stored_blob, large_value);

        let inline_entry = found_entries
            .iter()
            .find(|(key, _, _)| key.as_ref() == b"inline-key")
            .expect("inline entry should be present");
        assert_eq!(inline_entry.2, ValueDeletable::Value(small_value.clone()),);

        db.close().await.unwrap();
    }

    #[tokio::test]
    async fn should_keep_merge_operands_inline_when_blob_options_are_enabled() {
        let db = setup_test_db_with_settings(
            true,
            Settings {
                blob_options: Some(BlobOptions::default()),
                ..Settings::default()
            },
        )
        .await;
        db.inner.txn_manager.new_snapshot(Some(0));
        db.inner.oracle.advance_durable_seq(u64::MAX);

        let large_merge = Bytes::from(vec![b'm'; 4096]);
        let table = WritableKVTable::new();
        table.put(RowEntry::new_merge(b"merge-key", large_merge.as_ref(), 1));
        let id = SsTableId::Compacted(Ulid::new());

        let (sst_handle, pack_files) = db
            .inner
            .flush_imm_table(&id, table.table().clone(), false)
            .await
            .unwrap();
        assert!(pack_files.is_empty());

        let found_entries = read_sst_entries(&db, &sst_handle).await;
        assert_eq!(found_entries.len(), 1);
        assert_eq!(
            found_entries[0],
            (
                Bytes::from("merge-key"),
                1,
                ValueDeletable::Merge(large_merge),
            )
        );

        db.close().await.unwrap();
    }

    fn blob_ref(value: &ValueDeletable) -> crate::types::BlobRef {
        match value {
            ValueDeletable::BlobRef(blob_ref) => *blob_ref,
            other => panic!("expected blob ref, found {other:?}"),
        }
    }

    #[tokio::test]
    async fn should_pack_multiple_large_values_into_single_pack_when_they_fit() {
        let db = setup_test_db_with_settings(
            false,
            Settings {
                blob_options: Some(BlobOptions {
                    min_value_size: 16,
                    target_pack_size_bytes: 1024,
                    ..BlobOptions::default()
                }),
                ..Settings::default()
            },
        )
        .await;
        db.inner.txn_manager.new_snapshot(Some(0));
        db.inner.oracle.advance_durable_seq(u64::MAX);

        let v1 = Bytes::from(vec![b'a'; 64]);
        let v2 = Bytes::from(vec![b'b'; 96]);
        let v3 = Bytes::from(vec![b'c'; 128]);

        let table = WritableKVTable::new();
        table.put(RowEntry::new_value(b"k1", v1.as_ref(), 1));
        table.put(RowEntry::new_value(b"k2", v2.as_ref(), 2));
        table.put(RowEntry::new_value(b"k3", v3.as_ref(), 3));
        let id = SsTableId::Compacted(Ulid::new());

        let (sst_handle, pack_files) = db
            .inner
            .flush_imm_table(&id, table.table().clone(), false)
            .await
            .unwrap();

        assert_eq!(pack_files.len(), 1);
        let pack = &pack_files[0];
        let total = (v1.len() + v2.len() + v3.len()) as u64;
        assert_eq!(pack.total_bytes, total);
        assert_eq!(pack.live_bytes, total);

        let entries = read_sst_entries(&db, &sst_handle).await;
        let by_key: std::collections::HashMap<_, _> = entries
            .iter()
            .map(|(k, _, v)| (k.clone(), v.clone()))
            .collect();
        let r1 = blob_ref(&by_key[&Bytes::from_static(b"k1")]);
        let r2 = blob_ref(&by_key[&Bytes::from_static(b"k2")]);
        let r3 = blob_ref(&by_key[&Bytes::from_static(b"k3")]);
        assert_eq!(r1.pack_id, pack.pack_id);
        assert_eq!(r2.pack_id, pack.pack_id);
        assert_eq!(r3.pack_id, pack.pack_id);
        assert_eq!(r1.offset, 0);
        assert_eq!(r2.offset, r1.length);
        assert_eq!(r3.offset, r1.length + r2.length);

        for (blob_ref, expected) in [(r1, &v1), (r2, &v2), (r3, &v3)] {
            let stored = db
                .inner
                .table_store
                .get_pack_range(blob_ref.pack_id, blob_ref.offset, blob_ref.length)
                .await
                .unwrap();
            assert_eq!(&stored, expected);
        }

        db.close().await.unwrap();
    }

    #[tokio::test]
    async fn should_emit_multiple_packs_when_large_values_exceed_target_pack_size() {
        let db = setup_test_db_with_settings(
            false,
            Settings {
                blob_options: Some(BlobOptions {
                    min_value_size: 16,
                    target_pack_size_bytes: 200,
                    ..BlobOptions::default()
                }),
                ..Settings::default()
            },
        )
        .await;
        db.inner.txn_manager.new_snapshot(Some(0));
        db.inner.oracle.advance_durable_seq(u64::MAX);

        // Three 128-byte values; target is 200, so each pack holds at most one
        // value before the next would push it past target. Expect 3 packs.
        let v1 = Bytes::from(vec![b'a'; 128]);
        let v2 = Bytes::from(vec![b'b'; 128]);
        let v3 = Bytes::from(vec![b'c'; 128]);

        let table = WritableKVTable::new();
        table.put(RowEntry::new_value(b"k1", v1.as_ref(), 1));
        table.put(RowEntry::new_value(b"k2", v2.as_ref(), 2));
        table.put(RowEntry::new_value(b"k3", v3.as_ref(), 3));
        let id = SsTableId::Compacted(Ulid::new());

        let (sst_handle, pack_files) = db
            .inner
            .flush_imm_table(&id, table.table().clone(), false)
            .await
            .unwrap();

        assert_eq!(pack_files.len(), 3);
        for pack in &pack_files {
            assert_eq!(pack.total_bytes, 128);
            assert_eq!(pack.live_bytes, pack.total_bytes);
        }

        let entries = read_sst_entries(&db, &sst_handle).await;
        let mut pack_ids: Vec<_> = entries
            .iter()
            .map(|(_, _, v)| blob_ref(v).pack_id)
            .collect();
        pack_ids.sort();
        pack_ids.dedup();
        assert_eq!(pack_ids.len(), 3);

        for (_, _, value) in &entries {
            let r = blob_ref(value);
            assert_eq!(r.offset, 0, "each value owns the start of its own pack");
            assert_eq!(r.length, 128);
        }

        db.close().await.unwrap();
    }

    #[tokio::test]
    async fn should_place_oversized_value_in_its_own_pack() {
        let db = setup_test_db_with_settings(
            false,
            Settings {
                blob_options: Some(BlobOptions {
                    min_value_size: 16,
                    target_pack_size_bytes: 64,
                    ..BlobOptions::default()
                }),
                ..Settings::default()
            },
        )
        .await;
        db.inner.txn_manager.new_snapshot(Some(0));
        db.inner.oracle.advance_durable_seq(u64::MAX);

        // 1 KiB value with a 64-byte target. The value must go in its own
        // pack. The next small-but-still-externalized value should land in a
        // fresh pack rather than tagging onto the oversized one.
        let huge = Bytes::from(vec![b'x'; 1024]);
        let small_externalized = Bytes::from(vec![b'y'; 32]);

        let table = WritableKVTable::new();
        table.put(RowEntry::new_value(b"huge", huge.as_ref(), 1));
        table.put(RowEntry::new_value(
            b"small",
            small_externalized.as_ref(),
            2,
        ));
        let id = SsTableId::Compacted(Ulid::new());

        let (sst_handle, pack_files) = db
            .inner
            .flush_imm_table(&id, table.table().clone(), false)
            .await
            .unwrap();

        assert_eq!(pack_files.len(), 2);
        let entries = read_sst_entries(&db, &sst_handle).await;
        let by_key: std::collections::HashMap<_, _> = entries
            .iter()
            .map(|(k, _, v)| (k.clone(), v.clone()))
            .collect();
        let r_huge = blob_ref(&by_key[&Bytes::from_static(b"huge")]);
        let r_small = blob_ref(&by_key[&Bytes::from_static(b"small")]);
        assert_ne!(r_huge.pack_id, r_small.pack_id);
        assert_eq!(r_huge.offset, 0);
        assert_eq!(r_huge.length, huge.len() as u32);
        assert_eq!(r_small.offset, 0);
        assert_eq!(r_small.length, small_externalized.len() as u32);

        db.close().await.unwrap();
    }
}
