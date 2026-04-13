# slatedb-ycsb

A native-Rust [YCSB](https://github.com/brianfrankcooper/YCSB) driver for SlateDB. Parses standard YCSB `.properties` workload files, runs the load and run phases against a live SlateDB instance backed by any supported object store, and reports per-operation latency histograms in YCSB's output format.

## Quick start

```sh
# In-memory smoke test against Workload C (read-only after load)
CLOUD_PROVIDER=memory cargo run -p slatedb-ycsb -- \
  --path /ycsb-smoke \
  load-run -P slatedb-ycsb/workloads/workloadc \
  --threadcount 4
```

Two-phase run against a local-FS object store:

```sh
# .env contents:
#   CLOUD_PROVIDER=local
#   LOCAL_PATH=/tmp/slatedb-ycsb

cargo run -p slatedb-ycsb -- --env-file .env --path /bench \
  load -P slatedb-ycsb/workloads/workloada --threadcount 8

cargo run -p slatedb-ycsb -- --env-file .env --path /bench \
  run -P slatedb-ycsb/workloads/workloada --threadcount 8 --duration 60
```

## Supported YCSB properties

| YCSB key | Notes |
|---|---|
| `recordcount` | Records inserted during the load phase. |
| `operationcount` | Operations issued during the run phase. |
| `readproportion`, `updateproportion`, `insertproportion`, `scanproportion`, `readmodifywriteproportion` | Proportions are normalized at runtime if they don't sum to 1.0. |
| `requestdistribution` | `uniform`, `zipfian`, `latest`, `sequential`, `hotspot`. |
| `maxscanlength`, `scanlengthdistribution` | Used by Workload E. |
| `fieldcount`, `fieldlength` | Multi-field records (default 10 × 100 bytes). |
| `insertorder` | `hashed` or `ordered`. Default `hashed` matches YCSB Java. |
| `zeropadding` | Key digit padding. |
| `readallfields`, `writeallfields` | Controls decode / full-field rewrite semantics. |
| `threadcount` | Overridden by `--threadcount` CLI flag if set. |
| `zipfian_constant`, `hotspotdatafraction`, `hotspotopnfraction` | Tunable distribution parameters. |

Properties can also be overridden individually: `-p recordcount=10000 -p operationcount=100000`.

## Output

The driver prints YCSB-style aggregate metrics after each phase:

```
[OVERALL], RunTime(ms), 61234
[OVERALL], Throughput(ops/sec), 16328.22
[READ], Operations, 950000
[READ], AverageLatency(us), 142.71
[READ], 95thPercentileLatency(us), 321
[READ], 99thPercentileLatency(us), 812
[UPDATE], Operations, 50000
[UPDATE], AverageLatency(us), 1803.44
...
```

This format is compatible with existing YCSB log parsers.
