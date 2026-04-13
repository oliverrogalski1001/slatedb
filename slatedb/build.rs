use std::env;

fn main() {
    // Only generate the Corfu WAL proxy bindings when the `corfu` feature is
    // enabled. Default builds skip protoc/tonic-build entirely so they don't
    // pay any extra codegen cost.
    if env::var_os("CARGO_FEATURE_CORFU").is_none() {
        return;
    }

    println!("cargo:rerun-if-changed=proto/wal_proxy.proto");
    tonic_build::configure()
        .build_server(false)
        .build_client(true)
        .compile_protos(&["proto/wal_proxy.proto"], &["proto"])
        .expect("failed to compile wal_proxy.proto");
}
