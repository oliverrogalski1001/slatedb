#!/usr/bin/env bash

set -eu # stop on errors and undefined variables

DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
export RUST_LOG=${RUST_LOG:-info}
OUT="target/bencher/results"

if [ -f "$HOME/.env" ]; then
  set -a
  source "$HOME/.env"
  set +a
fi

mkdir -p $OUT/logs

run_bench() {
  local put_percentage="$1"
  local concurrency="$2"
  local num_keys="$3"
  local log_file="$4"

  local clean_flag=""
  if [ -n "${SLATEDB_BENCH_CLEAN:-}" ]; then
    clean_flag="--clean"
  fi

  local durable_flag=""
  if [ -n "${SLATEDB_BENCH_AWAIT_DURABLE:-}" ]; then
    durable_flag="--await-durable"
  fi

  local bench_cmd="cargo run -r --package slatedb-bencher -- \
    --path /slatedb-bencher_${put_percentage}_${concurrency} $clean_flag db \
    --db-options-path $DIR/SlateDb.toml \
    --duration 60 \
    --val-len 8192 \
    --block-cache-size 100663296 \
    --meta-cache-size 33554432 \
    --put-percentage $put_percentage \
    --concurrency $concurrency \
    --key-count $num_keys \
    $durable_flag \
  "

  $bench_cmd | tee "$log_file"
}

# Extract final stats from a log file and append a CSV row to the results file.
append_final_stats() {
  local put_percentage="$1"
  local concurrency="$2"
  local log_file="$3"
  local csv_file="$4"

  grep "db final" "$log_file" | sed -E 's/.*elapsed: ([0-9.]+)s.*put\/s: ([0-9.]+) \(([0-9.]+) MiB\/s\).*get\/s: ([0-9.]+) \(([0-9.]+) MiB\/s\).*get db hit ratio: ([0-9.]+)%.*puts=([0-9]+).*gets=([0-9]+).*/'"$put_percentage,$concurrency"',\1,\2,\3,\4,\5,\6,\7,\8/' >>"$csv_file"
}

# Set CLOUD_PROVIDER to local if not already set
export CLOUD_PROVIDER=${CLOUD_PROVIDER:-local}
echo "Using cloud provider: $CLOUD_PROVIDER"

# Set LOCAL_PATH if CLOUD_PROVIDER is local and path not already set
if [ "$CLOUD_PROVIDER" = "local" ]; then
  export LOCAL_PATH=${LOCAL_PATH:-/tmp/slatedb}
  mkdir -p $LOCAL_PATH
  echo "Using local path: $LOCAL_PATH"
fi

CSV_FILE="$OUT/results.csv"
echo "put_percentage,concurrency,elapsed_s,puts_per_s,puts_mib_per_s,gets_per_s,gets_mib_per_s,get_hit_ratio_pct,total_puts,total_gets" >"$CSV_FILE"

for put_percentage in 100; do
  for concurrency in 1 4 8 16 32; do
    log_file="$OUT/logs/${put_percentage}_${concurrency}.log"
    num_keys=$((put_percentage * 1000))

    run_bench "$put_percentage" "$concurrency" "$num_keys" "$log_file"
    append_final_stats "$put_percentage" "$concurrency" "$log_file" "$CSV_FILE"
  done
done
