use std::future::Future;
use std::sync::Arc;

use bytes::Bytes;
use slatedb_common::metrics::CounterFn;

use crate::db_metrics::DbMetrics;
use crate::error::SlateDBError;
use crate::types::BlobRef;

pub(crate) struct BlobCache {
    inner: moka::future::Cache<BlobRef, Bytes>,
    hit: Arc<dyn CounterFn>,
    miss: Arc<dyn CounterFn>,
}

impl BlobCache {
    pub(crate) fn new(max_capacity_bytes: u64, db_metrics: &DbMetrics) -> Self {
        let inner = moka::future::Cache::builder()
            .weigher(|_key: &BlobRef, value: &Bytes| {
                u32::try_from(value.len()).unwrap_or(u32::MAX)
            })
            .max_capacity(max_capacity_bytes)
            .build();
        Self {
            inner,
            hit: db_metrics.counter(stats::BLOB_CACHE_HIT).register(),
            miss: db_metrics.counter(stats::BLOB_CACHE_MISS).register(),
        }
    }

    pub(crate) async fn get_or_fetch<F, Fut>(
        &self,
        key: BlobRef,
        fetch: F,
    ) -> Result<Bytes, SlateDBError>
    where
        F: FnOnce() -> Fut,
        Fut: Future<Output = Result<Bytes, SlateDBError>>,
    {
        if let Some(cached) = self.inner.get(&key).await {
            self.hit.increment(1);
            return Ok(cached);
        }
        self.miss.increment(1);
        let value = fetch().await?;
        self.inner.insert(key, value.clone()).await;
        Ok(value)
    }

    #[cfg(test)]
    pub(crate) async fn entry_count(&self) -> u64 {
        self.inner.run_pending_tasks().await;
        self.inner.entry_count()
    }
}

pub mod stats {
    macro_rules! blob_cache_stat_name {
        ($suffix:expr) => {
            crate::stat_name!("blob_cache", $suffix)
        };
    }

    pub const BLOB_CACHE_HIT: &str = blob_cache_stat_name!("hit");
    pub const BLOB_CACHE_MISS: &str = blob_cache_stat_name!("miss");
}
