# corfu-wal-proxy

gRPC sidecar that exposes a SlateDB-compatible WAL store on top of a Corfu
shared log. SlateDB (Rust) talks to this process over local loopback gRPC;
this process talks to Corfu via the existing `CorfuBridge` wrapper around
`CorfuRuntime` + `IStreamView`.

## Architecture

```
slatedb (Rust)  ──gRPC──▶  corfu-wal-proxy (JVM)  ──Netty──▶  Corfu cluster
```

## Build

The proto file is generated from `../slatedb/proto/wal_proxy.proto`, the
same source SlateDB's Rust client builds its client stubs from, so the two
sides stay in sync automatically.

```
mvn package
```

This produces `target/corfu-wal-proxy.jar` as a runnable jar-with-dependencies.

## Run

```
java -jar target/corfu-wal-proxy.jar \
    --corfu localhost:9000 \
    --stream slatedb-wal \
    --listen 127.0.0.1:50111
```

Then enable the `corfu` feature in SlateDB and point it at the proxy:

```rust
use slatedb::wal::corfu_wal::CorfuWalStore;

let wal_store = CorfuWalStore::connect("http://127.0.0.1:50111").await?;
let db = Db::builder("my_db", object_store)
    .with_wal_store(std::sync::Arc::new(wal_store))
    .build()
    .await?;
```

## Prerequisites

- JDK 11+
- Maven 3.6+
- A running Corfu cluster reachable at the `--corfu` address
- The `ozonedb:corfu-bridge:1.0` artifact installed in your local Maven
  repo (from `ozonedb/ozonedb-jni-maven/corfu-bridge`)
