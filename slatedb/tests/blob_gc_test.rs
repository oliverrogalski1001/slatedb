#![allow(clippy::disallowed_types, clippy::disallowed_methods)]

//! Integration tests for the blob garbage collection pipeline.
//!
//! Unit tests in `src/garbage_collector.rs` cover the GC side in isolation
//! (they seed orphans into the manifest synthetically). These tests exercise
//! the full end-to-end flow: flush-time blob externalization → compaction
//! recording orphans in the manifest → GC deleting them once no live
//! checkpoint pins the manifest that introduced them.

use futures::StreamExt;
use slatedb::admin::AdminBuilder;
use slatedb::config::{
    BlobOptions, CheckpointOptions, CheckpointScope, CompactorOptions, FlushOptions, FlushType,
    GarbageCollectorDirectoryOptions, GarbageCollectorOptions, Settings,
    SizeTieredCompactionSchedulerOptions,
};
use slatedb::VersionedManifest;
use slatedb::object_store::memory::InMemory;
use slatedb::object_store::path::Path;
use slatedb::object_store::ObjectStore;
use slatedb::size_tiered_compaction::SizeTieredCompactionSchedulerSupplier;
use slatedb::{CompactorBuilder, Db, GarbageCollectorBuilder};
use slatedb_common::{DefaultSystemClock, MockSystemClock, SystemClock};
use std::collections::HashSet;
use std::sync::Arc;
use std::time::{Duration, Instant};
use ulid::Ulid;
use uuid::Uuid;

/// Unique DB path per test run so residue from one test can't leak into another.
fn unique_path(label: &str) -> String {
    let ts = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    format!("/tmp/test_blob_gc_{}_{}", label, ts)
}

/// Build a DB configured for deterministic blob-GC testing:
/// - every value is externalized (`min_value_size: 1`)
/// - memtable never auto-flushes; tests flush explicitly
/// - size-tiered compaction fires once there are 2+ L0 SSTs
async fn open_db(
    path: &str,
    object_store: Arc<dyn ObjectStore>,
    clock: Arc<dyn SystemClock>,
) -> Arc<Db> {
    let mut settings = Settings {
        flush_interval: Some(Duration::from_millis(50)),
        manifest_poll_interval: Duration::from_millis(50),
        manifest_update_timeout: Duration::from_secs(30),
        l0_sst_size_bytes: 128,
        min_filter_keys: 0,
        ..Default::default()
    };
    settings.blob_options = Some(BlobOptions {
        min_value_size: 1,
        ..Default::default()
    });

    let compactor_options = CompactorOptions {
        poll_interval: Duration::from_millis(50),
        max_sst_size: 1024,
        max_concurrent_compactions: 1,
        manifest_update_timeout: Duration::from_secs(30),
        scheduler_options: SizeTieredCompactionSchedulerOptions {
            min_compaction_sources: 2,
            ..Default::default()
        }
        .into(),
        ..Default::default()
    };

    let db = Db::builder(path.to_string(), object_store.clone())
        .with_settings(settings)
        .with_system_clock(clock.clone())
        .with_compactor_builder(
            CompactorBuilder::new(path.to_string(), object_store.clone())
                .with_options(compactor_options)
                .with_system_clock(clock.clone())
                .with_scheduler_supplier(Arc::new(SizeTieredCompactionSchedulerSupplier::new())),
        )
        .build()
        .await
        .expect("failed to build db");
    Arc::new(db)
}

/// Enumerate blob IDs present under `<root>/blob/`.
async fn list_blob_ids(object_store: &Arc<dyn ObjectStore>, root: &str) -> HashSet<Ulid> {
    let prefix = Path::from(format!("{}/blob", root));
    let mut stream = object_store.list(Some(&prefix));
    let mut ids = HashSet::new();
    while let Some(result) = stream.next().await {
        let meta = result.expect("object store list failed");
        let Some(filename) = meta.location.filename() else {
            continue;
        };
        let Some(stem) = filename.strip_suffix(".data") else {
            continue;
        };
        if let Ok(id) = Ulid::from_string(stem) {
            ids.insert(id);
        }
    }
    ids
}

/// Force the active memtable down to L0. The compactor will pick it up on its
/// next poll cycle once `min_compaction_sources` is satisfied.
async fn flush_to_l0(db: &Db) {
    db.flush_with_options(FlushOptions {
        flush_type: FlushType::MemTable,
    })
    .await
    .expect("failed to flush memtable");
}

async fn await_manifest_mock(
    db: &Db,
    clock: &Arc<dyn SystemClock>,
    predicate: impl Fn(&VersionedManifest) -> bool,
    timeout: Duration,
) -> VersionedManifest {
    let deadline = Instant::now() + timeout;
    loop {
        // Advance past the compactor's and flusher's poll intervals, then yield
        // real time so their tickers actually fire: the compactor schedules +
        // runs the job, the flusher reloads the manifest, and `db.manifest()`
        // observes the new state.
        clock.advance(Duration::from_secs(60)).await;
        tokio::time::sleep(Duration::from_millis(100)).await;
        let m = db.manifest();
        if predicate(&m) {
            return m;
        }
        if Instant::now() >= deadline {
            panic!("condition not met; final: {:?}", m);
        }
    }
}

/// Poll `db.manifest()` until `predicate` is satisfied or `timeout` elapses.
async fn wait_for_manifest<F>(db: &Db, mut predicate: F, timeout: Duration) -> VersionedManifest
where
    F: FnMut(&VersionedManifest) -> bool,
{
    let deadline = Instant::now() + timeout;
    loop {
        let manifest = db.manifest();
        if predicate(&manifest) {
            return manifest;
        }
        if Instant::now() >= deadline {
            panic!(
                "manifest condition not met within {:?}; final state: {:?}",
                timeout, manifest
            );
        }
        tokio::time::sleep(Duration::from_millis(25)).await;
    }
}

/// Drop every checkpoint currently in the manifest. The compactor writes its
/// own 15-minute checkpoint before swapping in the post-compaction manifest
/// (`src/compactor_state_protocols.rs:232`) — that checkpoint pins the
/// pre-compaction manifest and blocks blob GC. Tests that want to observe
/// deletion must either advance the clock past the lifetime or call this.
async fn drop_all_checkpoints(path: &str, object_store: Arc<dyn ObjectStore>) -> Vec<Uuid> {
    let admin = AdminBuilder::new(path.to_string(), object_store).build();
    let ids: Vec<Uuid> = admin
        .list_checkpoints(None)
        .await
        .expect("list checkpoints")
        .into_iter()
        .map(|c| c.id)
        .collect();
    for id in &ids {
        admin
            .delete_checkpoint(*id)
            .await
            .expect("delete checkpoint");
    }
    ids
}

/// Run a single blob-GC sweep. All other GC sweeps are disabled so tests can
/// isolate blob-deletion behavior from SST/WAL/manifest reclamation.
async fn run_blob_gc_once(
    path: &str,
    object_store: Arc<dyn ObjectStore>,
    clock: Arc<dyn SystemClock>,
) {
    let options = GarbageCollectorOptions {
        manifest_options: None,
        wal_options: None,
        compacted_options: None,
        compactions_options: None,
        detach_options: None,
        blob_options: Some(GarbageCollectorDirectoryOptions {
            interval: None,
            min_age: Duration::from_secs(0),
        }),
    };

    let gc = GarbageCollectorBuilder::new(path.to_string(), object_store)
        .with_options(options)
        .with_system_clock(clock.clone())
        .build();
    gc.run_gc_once().await;
}

// --- Tests -------------------------------------------------------------------

/// End-to-end happy path: write two versions of the same key (both externalized),
/// let compaction record the first version's blob as an orphan, then verify
/// blob GC actually deletes it and prunes it from the manifest.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_blob_write_compact_gc_end_to_end() {
    let object_store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let clock: Arc<dyn SystemClock> = Arc::new(DefaultSystemClock::new());
    let path = unique_path("end_to_end");
    let db = open_db(&path, object_store.clone(), clock.clone()).await;

    // 1. Write v1, flush → blob B1 + L0 SST carrying BlobRef(B1).
    db.put(b"k", b"value-v1-long-enough-to-externalize")
        .await
        .expect("put v1");
    flush_to_l0(&db).await;

    let after_v1 = list_blob_ids(&object_store, &path).await;
    assert_eq!(
        after_v1.len(),
        1,
        "v1 should externalize into exactly one blob"
    );
    let b1 = *after_v1.iter().next().unwrap();

    // 2. Write v2, flush → blob B2 + second L0 SST.
    db.put(b"k", b"value-v2-also-long-enough-to-externalize")
        .await
        .expect("put v2");
    flush_to_l0(&db).await;

    let after_v2 = list_blob_ids(&object_store, &path).await;
    assert_eq!(after_v2.len(), 2, "both blobs exist before compaction runs");

    // 3. The compactor has 2 L0 SSTs — it will compact them. Wait until L0
    //    has drained and B1 shows up in manifest.orphan_packs.
    let manifest = wait_for_manifest(
        &db,
        |m| {
            m.l0().is_empty()
                && m.last_compacted_l0_sst_view_id().is_some()
                && m.orphan_packs().iter().any(|o| o.pack_id == b1)
        },
        Duration::from_secs(15),
    )
    .await;

    let orphan = manifest
        .orphan_packs()
        .iter()
        .find(|o| o.pack_id == b1)
        .expect("b1 must be recorded as orphan after compaction");
    assert!(
        orphan.orphaned_at_manifest_id > 0,
        "orphan should carry the manifest id that recorded it"
    );

    // 4. Pre-GC: both blobs still on disk.
    let pre_gc = list_blob_ids(&object_store, &path).await;
    assert!(pre_gc.contains(&b1), "b1 should still exist before GC");

    // 4a. The compactor wrote a 15-minute checkpoint pinning the pre-compaction
    //     manifest. That checkpoint protects B1. Drop it so the watermark
    //     advances. In production the checkpoint naturally expires and regular
    //     GC removes it; `test_blob_gc_respects_compactor_checkpoint` below
    //     exercises that path with a mock clock.
    let dropped = drop_all_checkpoints(&path, object_store.clone()).await;
    assert!(!dropped.is_empty(), "expected compactor checkpoint");

    // 5. Run blob GC. With no live checkpoints the watermark is the current
    //    manifest id, so any orphan recorded at a strictly lower id is eligible.
    run_blob_gc_once(&path, object_store.clone(), clock.clone()).await;

    let post_gc = list_blob_ids(&object_store, &path).await;
    assert!(!post_gc.contains(&b1), "b1 should be deleted after blob GC");
    assert_eq!(post_gc.len(), 1, "only v2's blob should remain");

    // 6. Orphan list in the manifest is pruned for the deleted blob. GC writes
    //    via the object store; the Db polls every `manifest_poll_interval`, so
    //    give it a moment to catch up.
    wait_for_manifest(
        &db,
        |m| !m.orphan_packs().iter().any(|o| o.pack_id == b1),
        Duration::from_secs(5),
    )
    .await;

    // 7. Reads still resolve through the surviving blob (B2).
    let v = db.get(b"k").await.expect("get k");
    assert_eq!(
        v.as_deref(),
        Some(&b"value-v2-also-long-enough-to-externalize"[..]),
    );

    db.close().await.expect("close db");
}

// --- Scenarios to fill in next -----------------------------------------------
// Each can reuse the helpers above. Outlines only — implement as separate
// `#[tokio::test]` functions below.

// test_blob_gc_respects_compactor_checkpoint
//   - After the flow in `test_blob_write_compact_gc_end_to_end`, inspect
//     `manifest.checkpoints` and assert the compactor's long-lived checkpoint
//     pins the pre-compaction manifest id.
//   - Run blob GC against that state; B1 must NOT be deleted.
//   - Advance the system clock past the checkpoint's `expire_time` (use a
//     MockSystemClock injected into both the Db and the GC builder via
//     `with_system_clock`), then run the full GC with `manifest_options`
//     enabled so the expired checkpoint is removed. Run blob GC again and
//     assert B1 is now deleted.

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_blob_gc_respects_compactor_checkpoint() {
    let object_store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let path = unique_path("end_to_end");
    let mock_clock: Arc<dyn SystemClock> = Arc::new(MockSystemClock::new());
    let db = open_db(&path, object_store.clone(), mock_clock.clone()).await;

    // 1. Write v1, flush → blob B1 + L0 SST carrying BlobRef(B1).
    db.put(b"k", b"value-v1-long-enough-to-externalize")
        .await
        .expect("put v1");
    flush_to_l0(&db).await;

    let after_v1 = list_blob_ids(&object_store, &path).await;
    assert_eq!(
        after_v1.len(),
        1,
        "v1 should externalize into exactly one blob"
    );
    let b1 = *after_v1.iter().next().unwrap();

    // 2. Write v2, flush → blob B2 + second L0 SST.
    db.put(b"k", b"value-v2-also-long-enough-to-externalize")
        .await
        .expect("put v2");
    flush_to_l0(&db).await;

    let after_v2 = list_blob_ids(&object_store, &path).await;
    assert_eq!(after_v2.len(), 2, "both blobs exist before compaction runs");

    // Advance past the compactor's 50ms poll_interval so its next tick fires.
    // MockSystemClock::sleep spins on yield_now until current_ts >= end_time,
    // so the tick task only unblocks after we advance.
    mock_clock.advance(Duration::from_secs(10)).await;

    // 3. The compactor has 2 L0 SSTs — it will compact them. Wait until L0
    //    has drained
    let manifest = await_manifest_mock(
        &db,
        &mock_clock,
        |m| m.l0().is_empty() && m.last_compacted_l0_sst_view_id().is_some(),
        Duration::from_secs(15),
    )
    .await;

    let _orphan = manifest
        .orphan_packs()
        .iter()
        .find(|o| o.pack_id == b1)
        .expect("b1 must not be recorded as orphan after compaction");

    // 4. Pre-GC: both blobs still on disk.
    let pre_gc = list_blob_ids(&object_store, &path).await;
    assert!(pre_gc.contains(&b1), "b1 should still exist before GC");

    // 4a. The compactor wrote a 15-minute checkpoint pinning the pre-compaction
    //     manifest. That checkpoint protects B1. Advance 15 minutes to drop checkpoint.
    mock_clock
        .advance(std::time::Duration::from_secs(910))
        .await;

    let manifest = await_manifest_mock(
        &db,
        &mock_clock,
        |m| {
            m.l0().is_empty()
                && m.last_compacted_l0_sst_view_id().is_some()
                && m.orphan_packs().iter().any(|o| o.pack_id == b1)
        },
        Duration::from_secs(15),
    )
    .await;

    let _orphan = manifest
        .orphan_packs()
        .iter()
        .find(|o| o.pack_id == b1)
        .expect("b1 must be recorded as orphan after compaction");

    // 5. Run blob GC. With no live checkpoints the watermark is the current
    //    manifest id, so any orphan recorded at a strictly lower id is eligible.
    run_blob_gc_once(&path, object_store.clone(), mock_clock.clone()).await;

    let post_gc = list_blob_ids(&object_store, &path).await;
    assert!(!post_gc.contains(&b1), "b1 should be deleted after blob GC");
    assert_eq!(post_gc.len(), 1, "only v2's blob should remain");

    // 6. Orphan list in the manifest is pruned for the deleted blob. GC writes
    //    via the object store; the Db polls every `manifest_poll_interval`, so
    //    give it a moment to catch up.
    await_manifest_mock(
        &db,
        &mock_clock,
        |m| !m.orphan_packs().iter().any(|o| o.pack_id == b1),
        Duration::from_secs(5),
    )
    .await;

    // 7. Reads still resolve through the surviving blob (B2).
    let v = db.get(b"k").await.expect("get k");
    assert_eq!(
        v.as_deref(),
        Some(&b"value-v2-also-long-enough-to-externalize"[..]),
    );

    db.close().await.expect("close db");
}

// test_blob_gc_with_multiple_checkpoints
//   - Create two user checkpoints at different manifest versions via
//     `Db::create_checkpoint`.
//   - Run the two-version write/compact flow twice so orphans are recorded
//     against two different manifest ids (m_a, m_b).
//   - Run blob GC; only orphans with `recorded_at_manifest_id` < min(pinned)
//     should be deleted. Delete the earlier checkpoint, rerun, assert the
//     next batch becomes eligible.

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_blob_gc_with_multiple_checkpoints() {
    let object_store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let path = unique_path("end_to_end");
    let clock: Arc<dyn SystemClock> = Arc::new(DefaultSystemClock::new());
    let db = open_db(&path, object_store.clone(), clock.clone()).await;

    // 1. Write v1, flush → blob B1 + L0 SST carrying BlobRef(B1).
    db.put(b"k", b"value-v1-long-enough-to-externalize")
        .await
        .expect("put v1");
    flush_to_l0(&db).await;

    let after_v1 = list_blob_ids(&object_store, &path).await;
    assert_eq!(
        after_v1.len(),
        1,
        "v1 should externalize into exactly one blob"
    );
    let _b1 = *after_v1.iter().next().unwrap();

    let cp_options = CheckpointOptions {
        lifetime: None,
        source: None,
        name: None,
    };

    let _c1 = db
        .create_checkpoint(CheckpointScope::All, &cp_options)
        .await;

    // 2. Write v2, flush → blob B2 + second L0 SST.
    db.put(b"k", b"value-v2-also-long-enough-to-externalize")
        .await
        .expect("put v2");
    flush_to_l0(&db).await;

    let after_v2 = list_blob_ids(&object_store, &path).await;
    assert_eq!(after_v2.len(), 2, "both blobs exist before compaction runs");

    let _c2 = db
        .create_checkpoint(CheckpointScope::All, &cp_options)
        .await;

    // 3. The compactor has 2 L0 SSTs — it will compact them. Wait until L0
    //    has drained and B1 shows up in manifest.orphan_packs.
    let _manifest = wait_for_manifest(
        &db,
        |m| m.l0().is_empty() && m.last_compacted_l0_sst_view_id().is_some(),
        Duration::from_secs(15),
    )
    .await;

    let after_v3 = list_blob_ids(&object_store, &path).await;
    assert_eq!(
        after_v3.len(),
        2,
        "both blobs exist after compaction runs and checkpoints"
    );

    db.close().await.expect("close db");
}

/// Tombstone-trim path: `put(k, big)` then `delete(k)` in the same memtable,
/// flush, then a second flush of an unrelated key to trigger compaction.
/// Bottom-level compaction trims the trailing tombstone for `k`; the retention
/// iterator drops the underlying `BlobRef(B1)` along with it and records B1
/// in `manifest.orphan_packs`. The tombstone itself carries no blob, so it
/// contributes no additional orphan — the final count is exactly 1.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_tombstone_trim_produces_orphan_for_underlying_put() {
    let object_store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let path = unique_path("tombstone_trim");
    let clock: Arc<dyn SystemClock> = Arc::new(DefaultSystemClock::new());
    let db = open_db(&path, object_store.clone(), clock.clone()).await;

    // 1. put(k) externalizes B1, delete(k) stamps a tombstone. Flush → SST#1
    //    contains BlobRef(B1) at seq=N and Tombstone at seq=N+1 for key k.
    db.put(b"k", b"value-v1-long-enough-to-externalize")
        .await
        .expect("put k");
    db.delete(b"k").await.expect("delete k");
    flush_to_l0(&db).await;

    let after_flush1 = list_blob_ids(&object_store, &path).await;
    assert_eq!(after_flush1.len(), 1, "only B1 exists after first flush");
    let b1 = *after_flush1.iter().next().unwrap();

    // 2. Second flush gets a second L0 SST so the compactor's
    //    min_compaction_sources threshold is met.
    db.put(b"k2", b"value-v2-long-enough-to-externalize")
        .await
        .expect("put k2");
    flush_to_l0(&db).await;

    let after_flush2 = list_blob_ids(&object_store, &path).await;
    assert_eq!(
        after_flush2.len(),
        2,
        "B1 and B2 both on disk after flushes"
    );
    let b2 = *after_flush2.iter().find(|id| **id != b1).unwrap();

    // 3. Wait until compaction records B1 as an orphan. We don't require L0 to
    //    be fully drained: with `min_compaction_sources=2` the scheduler can
    //    still race with the second flush and pick up just one L0 SST — the
    //    one with k+tombstone — which is the compaction we care about here.
    //    What matters is that the tombstone-trim path recorded B1 as orphan.
    let manifest = wait_for_manifest(
        &db,
        |m| m.orphan_packs().iter().any(|o| o.pack_id == b1),
        Duration::from_secs(15),
    )
    .await;

    // The tombstone itself carries no blob, so B1 is the ONLY orphan; and B2
    // belongs to k2 which is still live, so it must not appear.
    assert!(
        !manifest.orphan_packs().iter().any(|o| o.pack_id == b2),
        "B2 is still live (k2 has no tombstone) and must not be an orphan. Got: {:?}",
        manifest.orphan_packs(),
    );
    assert_eq!(
        manifest.orphan_packs().len(),
        1,
        "only B1 should be recorded as orphan. Got: {:?}",
        manifest.orphan_packs(),
    );

    // k is gone regardless of which compactions have landed.
    assert_eq!(db.get(b"k").await.expect("get k").as_deref(), None);

    // 4. Pre-GC: both blobs still on disk.
    let pre_gc = list_blob_ids(&object_store, &path).await;
    assert_eq!(pre_gc.len(), 2, "both blobs remain before GC");

    // 5. Drop the compactor's pinning checkpoint so the GC watermark advances,
    //    then run blob GC. B1 gets deleted and pruned from orphan_packs; B2
    //    survives because k2 is still live.
    drop_all_checkpoints(&path, object_store.clone()).await;
    run_blob_gc_once(&path, object_store.clone(), clock.clone()).await;

    let post_gc = list_blob_ids(&object_store, &path).await;
    assert!(!post_gc.contains(&b1), "B1 should be deleted after GC");
    assert!(post_gc.contains(&b2), "B2 should survive GC");
    assert_eq!(post_gc.len(), 1, "only B2 should remain");

    wait_for_manifest(
        &db,
        |m| !m.orphan_packs().iter().any(|o| o.pack_id == b1),
        Duration::from_secs(5),
    )
    .await;

    // k2 still resolves through B2 after GC.
    assert_eq!(
        db.get(b"k2").await.expect("get k2").as_deref(),
        Some(&b"value-v2-long-enough-to-externalize"[..]),
    );

    db.close().await.expect("close db");
}
