use super::{GcStats, GcTask};
use crate::config::GarbageCollectorDirectoryOptions;
use crate::error::SlateDBError;
use crate::garbage_collector::ManifestStore;
use crate::tablestore::TableStore;
use chrono::{DateTime, Utc};
use std::sync::Arc;

pub(crate) struct BlobGcTask {
    manifest_store: Arc<ManifestStore>,
    table_store: Arc<TableStore>,
    stats: Arc<GcStats>,
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
        blob_options: GarbageCollectorDirectoryOptions,
    ) -> Self {
        Self {
            manifest_store,
            table_store,
            stats,
            blob_options,
        }
    }
}

impl GcTask for BlobGcTask {
    fn resource(&self) -> &str {
        "Blobs"
    }
    async fn collect(&self, _now: DateTime<Utc>) -> Result<(), SlateDBError> {
        // TODO: blob garbage collection
        Ok(())
    }
}
