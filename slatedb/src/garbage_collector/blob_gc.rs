use super::{GcStats, GcTask};
use crate::config::GarbageCollectorDirectoryOptions;
use crate::error::SlateDBError;
use crate::garbage_collector::Manifest;
use crate::garbage_collector::ManifestStore;
use crate::garbage_collector::StoredManifest;
use crate::manifest::OrphanBlob;
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
        deleted_blobs: HashSet<Ulid>,
    ) -> Result<(), SlateDBError> {
        stored_manifest
            .maybe_apply_update(|manifest| Self::filter_deleted_blobs(manifest, &deleted_blobs))
            .await?;
        Ok(())
    }

    fn filter_deleted_blobs(
        manifest: &SimpleTransactionalObject<Manifest>,
        deleted_blobs: &HashSet<Ulid>,
    ) -> Result<Option<DirtyObject<Manifest>>, SlateDBError> {
        let mut dirty = manifest.prepare_dirty()?;
        let retained_blobs: Vec<OrphanBlob> = dirty
            .value
            .core
            .orphan_blobs
            .iter()
            .filter(|blob| !deleted_blobs.contains(&blob.blob_id))
            .cloned()
            .collect();

        if dirty.value.core.orphan_blobs.len() == retained_blobs.len() {
            Ok(None)
        } else {
            dirty.value.core.orphan_blobs = retained_blobs;
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
            .orphan_blobs
            .iter()
            .filter(|blob| blob.recorded_at_manifest_id < min_checkpoint_id)
            .map(|blob| blob.blob_id)
            .collect();

        debug!("garbage collecting {} blobs", to_delete.len());

        if to_delete.is_empty() {
            return Ok(());
        }

        // TODO: make delete requests in parallel; treat not-found as success.
        let mut deleted_ids: HashSet<Ulid> = HashSet::with_capacity(to_delete.len());
        for blob_id in to_delete {
            if let Err(e) = self.table_store.delete_blob(blob_id).await {
                error!("error deleting blob [id={}, error={}]", blob_id, e);
            } else {
                self.stats.gc_blob_count.increment(1);
                deleted_ids.insert(blob_id);
            }
        }

        if deleted_ids.is_empty() {
            return Ok(());
        }

        self.prune_orphans_from_manifest(&mut stored_manifest, deleted_ids)
            .await
    }
}
