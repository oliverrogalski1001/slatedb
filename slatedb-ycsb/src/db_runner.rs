use std::sync::Arc;

use bytes::Bytes;
use rand::Rng;
use slatedb::config::{PutOptions, ReadOptions, ScanOptions, WriteOptions};
use slatedb::Db;

use crate::record::{encode_record, update_one_field};

/// Thin adapter layer mapping YCSB operations onto SlateDB's async API.
///
/// All methods return `Result<(), slatedb::Error>` so the caller can record
/// an error in the per-op histogram without aborting the workload.
pub(crate) struct DbRunner {
    pub(crate) db: Arc<Db>,
    pub(crate) write_options: WriteOptions,
    pub(crate) field_count: usize,
    pub(crate) field_length: usize,
    pub(crate) read_all_fields: bool,
}

impl DbRunner {
    pub(crate) async fn insert<R: Rng + ?Sized>(
        &self,
        rng: &mut R,
        key: Bytes,
    ) -> Result<(), slatedb::Error> {
        let value = encode_record(rng, self.field_count, self.field_length);
        self.db
            .put_with_options(&key, &value, &PutOptions::default(), &self.write_options)
            .await?;
        Ok(())
    }

    pub(crate) async fn read(&self, key: Bytes) -> Result<(), slatedb::Error> {
        let opts = ReadOptions::default();
        let value = self.db.get_with_options(&key, &opts).await?;
        if self.read_all_fields {
            // Force the caller to actually touch the bytes to match YCSB's
            // `readallfields` cost model (decoding happens in the critical section).
            if let Some(v) = value {
                let _ = v.len();
            }
        }
        Ok(())
    }

    pub(crate) async fn update<R: Rng + ?Sized>(
        &self,
        rng: &mut R,
        key: Bytes,
        write_all_fields: bool,
    ) -> Result<(), slatedb::Error> {
        let new_value = if write_all_fields {
            encode_record(rng, self.field_count, self.field_length)
        } else {
            // YCSB semantics: default updates replace a single random field
            // while preserving the rest, so we fetch, mutate, and put.
            let field_idx = rng.random_range(0..self.field_count);
            let existing = self.db.get(&key).await?;
            update_one_field(
                rng,
                existing.as_deref(),
                self.field_count,
                self.field_length,
                field_idx,
            )
        };
        self.db
            .put_with_options(
                &key,
                &new_value,
                &PutOptions::default(),
                &self.write_options,
            )
            .await?;
        Ok(())
    }

    pub(crate) async fn scan(&self, start_key: Bytes, limit: usize) -> Result<(), slatedb::Error> {
        let opts = ScanOptions::default();
        let mut iter = self
            .db
            .scan_with_options::<Bytes, _>(start_key.., &opts)
            .await?;
        let mut seen = 0usize;
        while seen < limit {
            match iter.next().await? {
                Some(_kv) => seen += 1,
                None => break,
            }
        }
        Ok(())
    }

    pub(crate) async fn read_modify_write<R: Rng + ?Sized>(
        &self,
        rng: &mut R,
        key: Bytes,
    ) -> Result<(), slatedb::Error> {
        let existing = self.db.get(&key).await?;
        let field_idx = rng.random_range(0..self.field_count);
        let new_value = update_one_field(
            rng,
            existing.as_deref(),
            self.field_count,
            self.field_length,
            field_idx,
        );
        self.db
            .put_with_options(
                &key,
                &new_value,
                &PutOptions::default(),
                &self.write_options,
            )
            .await?;
        Ok(())
    }
}
