//! Corfu shared-log [`WalStore`] backend.
//!
//! This module is feature-gated behind `corfu`. Enabling it adds two
//! dependencies — `tonic` and `prost` — and pulls in the generated
//! gRPC client for the `WalProxy` service defined in `proto/wal_proxy.proto`.
//!
//! ## Architecture
//!
//! SlateDB stays pure Rust. The actual Corfu client is a separate JVM
//! sidecar process (`corfu-wal-proxy`) that wraps the existing
//! `CorfuBridge.java` from the OzoneDB project. The Rust side talks to
//! that proxy over a local-loopback gRPC channel:
//!
//! ```text
//!   slatedb (Rust)  ──gRPC──▶  corfu-wal-proxy (JVM)  ──Netty──▶  Corfu cluster
//! ```
//!
//! Each `append_wal` call serializes the WAL batch into a single byte
//! payload, sends it to the proxy via the `Append` RPC, and uses the
//! returned global log address as the WAL id. Recovery enumerates WALs
//! via the `List` RPC and streams them back via `Read`.
//!
//! ## Payload format
//!
//! Each payload is a length-prefixed sequence of `RowEntry` records:
//!
//! ```text
//!   payload   := record*
//!   record    := u32 length || encoded_row
//!   encoded_row := u32 key_len || key
//!                || u8 value_kind  // 0=tombstone, 1=value, 2=merge
//!                || u32 value_len  // omitted for tombstone
//!                || value          // omitted for tombstone
//!                || u64 seq
//!                || u8 has_create_ts || i64 create_ts (if has_create_ts)
//!                || u8 has_expire_ts || i64 expire_ts (if has_expire_ts)
//! ```
//!
//! All integers are little-endian. The format is intentionally simple and
//! not bound to SlateDB's SST format — Corfu treats each WAL as an opaque
//! blob and the proxy never inspects the contents.

use async_trait::async_trait;
use bytes::{Buf, BufMut, Bytes, BytesMut};
use std::sync::Arc;
use tokio::sync::Mutex;
use tonic::transport::{Channel, Endpoint};
use tonic::Request;

use crate::error::SlateDBError;
use crate::types::{RowEntry, ValueDeletable};
use crate::wal::wal_proxy_proto::wal_proxy_client::WalProxyClient;
use crate::wal::wal_proxy_proto::{
    AppendRequest, ListRequest, ReadRequest, TailRequest, TrimRequest,
};
use crate::wal::wal_store::{WalBatch, WalEntryReader, WalStore};

const VALUE_KIND_TOMBSTONE: u8 = 0;
const VALUE_KIND_VALUE: u8 = 1;
const VALUE_KIND_MERGE: u8 = 2;

/// [`WalStore`] backed by a Corfu sidecar gRPC proxy.
pub struct CorfuWalStore {
    // The tonic-generated client is `Clone` and channels are cheap to
    // share, but we still wrap it in a Mutex so concurrent appenders
    // serialize on a single connection. The bottleneck for WAL writes is
    // the JVM bridge, not the channel.
    client: Arc<Mutex<WalProxyClient<Channel>>>,
}

impl CorfuWalStore {
    /// Connect to a running `corfu-wal-proxy` instance at `endpoint`
    /// (e.g. `"http://127.0.0.1:50111"`).
    pub async fn connect(endpoint: impl Into<String>) -> Result<Self, SlateDBError> {
        let endpoint = Endpoint::from_shared(endpoint.into())
            .map_err(|e| SlateDBError::from(std::io::Error::other(format!("invalid corfu endpoint: {e}"))))?;
        let channel = endpoint
            .connect()
            .await
            .map_err(|e| SlateDBError::from(std::io::Error::other(format!("failed to connect to corfu proxy: {e}"))))?;
        Ok(Self {
            client: Arc::new(Mutex::new(WalProxyClient::new(channel))),
        })
    }
}

#[async_trait]
impl WalStore for CorfuWalStore {
    async fn append_wal(&self, batch: WalBatch<'_>) -> Result<u64, SlateDBError> {
        let payload = encode_batch(batch.entries);
        let mut client = self.client.lock().await;
        let resp = client
            .append(Request::new(AppendRequest { payload: payload.into() }))
            .await
            .map_err(rpc_err)?;
        Ok(resp.into_inner().address)
    }

    async fn list_wals(&self, after: u64) -> Result<Vec<u64>, SlateDBError> {
        let mut client = self.client.lock().await;
        let resp = client
            .list(Request::new(ListRequest { after_addr: after }))
            .await
            .map_err(rpc_err)?;
        Ok(resp.into_inner().addresses)
    }

    async fn open_wal(
        &self,
        wal_id: u64,
    ) -> Result<Box<dyn WalEntryReader>, SlateDBError> {
        // Use a single-element Read stream to fetch one WAL by issuing a
        // read starting at `wal_id - 1` and consuming entries until we hit
        // one with the requested address. The proxy guarantees ascending
        // order so we can stop after the first match.
        let mut client = self.client.lock().await;
        let mut stream = client
            .read(Request::new(ReadRequest {
                after_addr: wal_id.saturating_sub(1),
            }))
            .await
            .map_err(rpc_err)?
            .into_inner();
        drop(client);

        loop {
            match stream.message().await.map_err(rpc_err)? {
                Some(entry) if entry.address == wal_id => {
                    let entries = decode_batch(&entry.payload)?;
                    return Ok(Box::new(VecWalReader::new(entries)));
                }
                Some(_) => continue,
                None => return Ok(Box::new(VecWalReader::new(Vec::new()))),
            }
        }
    }

    async fn tail(&self) -> Result<u64, SlateDBError> {
        let mut client = self.client.lock().await;
        let resp = client
            .tail(Request::new(TailRequest {}))
            .await
            .map_err(rpc_err)?;
        Ok(resp.into_inner().address)
    }

    async fn trim(&self, up_to: u64) -> Result<(), SlateDBError> {
        let mut client = self.client.lock().await;
        client
            .trim(Request::new(TrimRequest { up_to }))
            .await
            .map_err(rpc_err)?;
        Ok(())
    }

    fn estimate_encoded_size(&self, _entry_count: usize, size_bytes: usize) -> usize {
        // Corfu writes the raw payload — no SST framing overhead. Round up
        // by ~16 bytes per entry to account for the per-record header. The
        // estimator only needs to be in the right order of magnitude for
        // the buffer to decide when to flush.
        size_bytes + size_bytes / 16
    }
}

fn rpc_err(status: tonic::Status) -> SlateDBError {
    SlateDBError::from(std::io::Error::other(format!(
        "corfu wal proxy RPC failed: {status}"
    )))
}

fn encode_batch(entries: &[RowEntry]) -> Bytes {
    let mut buf = BytesMut::with_capacity(entries.iter().map(estimate_record_size).sum());
    for entry in entries {
        let mut record = BytesMut::new();
        encode_entry(&mut record, entry);
        buf.put_u32_le(record.len() as u32);
        buf.put_slice(&record);
    }
    buf.freeze()
}

fn estimate_record_size(entry: &RowEntry) -> usize {
    // length prefix + key header + key + value header + value + seq + ts flags
    4 + 4 + entry.key.len() + 1 + 4 + entry.value.len() + 8 + 1 + 8 + 1 + 8
}

fn encode_entry(buf: &mut BytesMut, entry: &RowEntry) {
    buf.put_u32_le(entry.key.len() as u32);
    buf.put_slice(&entry.key);
    match &entry.value {
        ValueDeletable::Tombstone => {
            buf.put_u8(VALUE_KIND_TOMBSTONE);
        }
        ValueDeletable::Value(v) => {
            buf.put_u8(VALUE_KIND_VALUE);
            buf.put_u32_le(v.len() as u32);
            buf.put_slice(v);
        }
        ValueDeletable::Merge(v) => {
            buf.put_u8(VALUE_KIND_MERGE);
            buf.put_u32_le(v.len() as u32);
            buf.put_slice(v);
        }
    }
    buf.put_u64_le(entry.seq);
    match entry.create_ts {
        Some(ts) => {
            buf.put_u8(1);
            buf.put_i64_le(ts);
        }
        None => buf.put_u8(0),
    }
    match entry.expire_ts {
        Some(ts) => {
            buf.put_u8(1);
            buf.put_i64_le(ts);
        }
        None => buf.put_u8(0),
    }
}

fn decode_batch(payload: &[u8]) -> Result<Vec<RowEntry>, SlateDBError> {
    let mut cursor = payload;
    let mut out = Vec::new();
    while !cursor.is_empty() {
        if cursor.remaining() < 4 {
            return Err(SlateDBError::from(std::io::Error::other(
                "truncated wal record length prefix",
            )));
        }
        let len = cursor.get_u32_le() as usize;
        if cursor.remaining() < len {
            return Err(SlateDBError::from(std::io::Error::other(
                "truncated wal record body",
            )));
        }
        let record = &cursor[..len];
        cursor.advance(len);
        out.push(decode_entry(record)?);
    }
    Ok(out)
}

fn decode_entry(mut buf: &[u8]) -> Result<RowEntry, SlateDBError> {
    let truncated =
        || SlateDBError::from(std::io::Error::other("truncated wal entry"));
    if buf.remaining() < 4 {
        return Err(truncated());
    }
    let key_len = buf.get_u32_le() as usize;
    if buf.remaining() < key_len + 1 {
        return Err(truncated());
    }
    let key = Bytes::copy_from_slice(&buf[..key_len]);
    buf.advance(key_len);
    let kind = buf.get_u8();
    let value = match kind {
        VALUE_KIND_TOMBSTONE => ValueDeletable::Tombstone,
        VALUE_KIND_VALUE | VALUE_KIND_MERGE => {
            if buf.remaining() < 4 {
                return Err(truncated());
            }
            let v_len = buf.get_u32_le() as usize;
            if buf.remaining() < v_len {
                return Err(truncated());
            }
            let v = Bytes::copy_from_slice(&buf[..v_len]);
            buf.advance(v_len);
            if kind == VALUE_KIND_VALUE {
                ValueDeletable::Value(v)
            } else {
                ValueDeletable::Merge(v)
            }
        }
        other => {
            return Err(SlateDBError::from(std::io::Error::other(format!(
                "unknown wal value kind: {other}"
            ))));
        }
    };
    if buf.remaining() < 8 + 1 {
        return Err(truncated());
    }
    let seq = buf.get_u64_le();
    let create_ts = if buf.get_u8() == 1 {
        if buf.remaining() < 8 {
            return Err(truncated());
        }
        Some(buf.get_i64_le())
    } else {
        None
    };
    if buf.remaining() < 1 {
        return Err(truncated());
    }
    let expire_ts = if buf.get_u8() == 1 {
        if buf.remaining() < 8 {
            return Err(truncated());
        }
        Some(buf.get_i64_le())
    } else {
        None
    };
    Ok(RowEntry::new(key, value, seq, create_ts, expire_ts))
}

/// Trivial [`WalEntryReader`] over an in-memory `Vec<RowEntry>`.
struct VecWalReader {
    iter: std::vec::IntoIter<RowEntry>,
}

impl VecWalReader {
    fn new(entries: Vec<RowEntry>) -> Self {
        Self {
            iter: entries.into_iter(),
        }
    }
}

#[async_trait]
impl WalEntryReader for VecWalReader {
    async fn next_entry(&mut self) -> Result<Option<RowEntry>, SlateDBError> {
        Ok(self.iter.next())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn round_trip_encodes_and_decodes_a_mixed_batch() {
        let entries = vec![
            RowEntry::new(
                Bytes::from_static(b"alpha"),
                ValueDeletable::Value(Bytes::from_static(b"one")),
                1,
                Some(100),
                None,
            ),
            RowEntry::new(
                Bytes::from_static(b"beta"),
                ValueDeletable::Tombstone,
                2,
                None,
                Some(200),
            ),
            RowEntry::new(
                Bytes::from_static(b"gamma"),
                ValueDeletable::Merge(Bytes::from_static(b"+1")),
                3,
                Some(300),
                Some(400),
            ),
        ];
        let bytes = encode_batch(&entries);
        let decoded = decode_batch(&bytes).unwrap();
        assert_eq!(decoded.len(), entries.len());
        for (orig, got) in entries.iter().zip(decoded.iter()) {
            assert_eq!(orig.key, got.key);
            assert_eq!(orig.value, got.value);
            assert_eq!(orig.seq, got.seq);
            assert_eq!(orig.create_ts, got.create_ts);
            assert_eq!(orig.expire_ts, got.expire_ts);
        }
    }

    #[test]
    fn empty_batch_round_trips_to_empty() {
        let bytes = encode_batch(&[]);
        assert!(bytes.is_empty());
        let decoded = decode_batch(&bytes).unwrap();
        assert!(decoded.is_empty());
    }
}
