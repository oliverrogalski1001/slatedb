#[cfg(feature = "corfu")]
pub mod corfu_wal;
pub(crate) mod object_store_wal;
pub(crate) mod wal_sst_builder;
pub mod wal_store;

#[cfg(feature = "corfu")]
pub(crate) mod wal_proxy_proto {
    tonic::include_proto!("slatedb.wal_proxy.v1");
}
