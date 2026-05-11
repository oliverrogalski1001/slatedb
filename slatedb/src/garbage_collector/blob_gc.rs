use super::{GcStats, GcTask};
use crate::config::GarbageCollectorDirectoryOptions;
use crate::error::SlateDBError;
use crate::garbage_collector::Manifest;
use crate::garbage_collector::ManifestStore;
use crate::garbage_collector::StoredManifest;
use crate::manifest::OrphanPack;
use crate::tablestore::TableStore;
use chrono::{DateTime, Utc};
use log::{debug, error};
use slatedb_common::clock::SystemClock;
use slatedb_txn_obj::{DirtyObject, SimpleTransactionalObject, TransactionalObject};
use std::collections::HashSet;
use std::sync::Arc;
use ulid::Ulid;

pub(crate) struct BlobGcTask {
    manifest_store: Arc<ManifestStore>,
    table_store: Arc<TableStore>,
    stats: Arc<GcStats>,
    system_clock: Arc<dyn SystemClock>,
    blob_options: GarbageCollectorDirectoryOptions,
}

impl std::fmt::Debug for BlobGcTask {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("BlobGcTask")
            .field("blob_options", &self.blob_options)
            .finish()
    }
}

impl BlobGcTask {
    pub(crate) fn new(
        manifest_store: Arc<ManifestStore>,
        table_store: Arc<TableStore>,
        stats: Arc<GcStats>,
        system_clock: Arc<dyn SystemClock>,
        blob_options: GarbageCollectorDirectoryOptions,
    ) -> Self {
        Self {
            manifest_store,
            table_store,
            stats,
            system_clock,
            blob_options,
        }
    }

    async fn prune_orphans_from_manifest(
        &self,
        stored_manifest: &mut StoredManifest,
        deleted_packs: HashSet<Ulid>,
    ) -> Result<(), SlateDBError> {
        stored_manifest
            .maybe_apply_update(|manifest| Self::filter_deleted_packs(manifest, &deleted_packs))
            .await?;
        Ok(())
    }

    fn filter_deleted_packs(
        manifest: &SimpleTransactionalObject<Manifest>,
        deleted_packs: &HashSet<Ulid>,
    ) -> Result<Option<DirtyObject<Manifest>>, SlateDBError> {
        let mut dirty = manifest.prepare_dirty()?;
        let retained_packs: Vec<OrphanPack> = dirty
            .value
            .core
            .orphan_packs
            .iter()
            .filter(|pack| !deleted_packs.contains(&pack.pack_id))
            .cloned()
            .collect();

        if dirty.value.core.orphan_packs.len() == retained_packs.len() {
            Ok(None)
        } else {
            dirty.value.core.orphan_packs = retained_packs;
            Ok(Some(dirty))
        }
    }
}

impl GcTask for BlobGcTask {
    fn resource(&self) -> &str {
        "Blobs"
    }

    async fn collect(&self, _now: DateTime<Utc>) -> Result<(), SlateDBError> {
        let mut stored_manifest =
            StoredManifest::load(Arc::clone(&self.manifest_store), self.system_clock.clone())
                .await?;
        let manifest = stored_manifest.manifest();
        let manifest_id = stored_manifest.id();

        let min_checkpoint_id = manifest
            .core
            .checkpoints
            .iter()
            .map(|c| c.manifest_id)
            .min()
            .unwrap_or(manifest_id);

        let to_delete: Vec<Ulid> = manifest
            .core
            .orphan_packs
            .iter()
            .filter(|pack| pack.orphaned_at_manifest_id < min_checkpoint_id)
            .map(|pack| pack.pack_id)
            .collect();

        debug!("garbage collecting {} packs", to_delete.len());

        if to_delete.is_empty() {
            return Ok(());
        }

        // TODO: make delete requests in parallel.
        let mut deleted_packs: HashSet<Ulid> = HashSet::with_capacity(to_delete.len());
        for pack_id in to_delete {
            match self.table_store.delete_pack(pack_id).await {
                Ok(()) => {
                    self.stats.gc_blob_count.increment(1);
                    deleted_packs.insert(pack_id);
                }
                // Pack already gone — still prune the orphan entry.
                Err(SlateDBError::ObjectStoreError(e))
                    if matches!(e.as_ref(), object_store::Error::NotFound { .. }) =>
                {
                    deleted_packs.insert(pack_id);
                }
                Err(e) => error!("error deleting pack [id={}, error={}]", pack_id, e),
            }
        }

        if deleted_packs.is_empty() {
            return Ok(());
        }

        self.prune_orphans_from_manifest(&mut stored_manifest, deleted_packs)
            .await
    }
}
