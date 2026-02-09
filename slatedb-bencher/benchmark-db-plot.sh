#!/usr/bin/env bash

set -eu # stop on errors and undefined variables

DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
WARMUP=0 # ignore the first N samples, equal to 30 seconds with default settings
OUT="target/bencher/results"

mkdir -p $OUT/dats
mkdir -p $OUT/logs
mkdir -p $OUT/plots

run_bench() {
  local put_percentage="$1"
  local concurrency="$2"
  local num_keys="$3"
  local log_file="$4"

  local clean_flag=""
  if [ -n "${SLATEDB_BENCH_CLEAN:-}" ]; then
    clean_flag="--clean"
  fi

  local bench_cmd="cargo run -r --package slatedb-bencher -- \
    --path /slatedb-bencher_${put_percentage}_${concurrency} $clean_flag db \
    --db-options-path $DIR/Slatedb.toml \
    --duration 60 \
    --val-len 8192 \
    --block-cache-size 100663296 \
    --meta-cache-size 33554432 \
    --put-percentage $put_percentage \
    --concurrency $concurrency \
    --key-count $num_keys \
  "

  $bench_cmd | tee "$log_file"
}

generate_dat() {
    local input_file="$1"
    local output_file="$2"

    echo "Parsing stats for $input_file -> $output_file"

    # Extract elapsed time, puts/s, and gets/s using sed and awk for cross-platform compatibility
    grep "stats dump" "$input_file" | sed -E 's/.*elapsed ([0-9.]+).*put\/s: ([0-9.]+).*get\/s: ([0-9.]+).*/\1 \2 \3/' > "$output_file"
}

generate_plot() {
    local dat_file="$1"
    local plot_file="$2"
    local put_percentage="$3"
    local concurrency="$4"

    if [ ! -f "$dat_file" ] || [ ! -s "$dat_file" ]; then
        echo "Warning: dat file $dat_file does not exist or is empty"
        return 1
    fi

    mkdir -p "$(dirname "$plot_file")"

    gnuplot <<EOF
set terminal pngcairo size 1024,768 enhanced font 'Arial,12'
set output '${plot_file}'
set title "SlateDB [puts=${put_percentage}%, threads=${concurrency}]"
set xlabel "Elapsed Time (s)"
set ylabel "Requests/s"
set grid
set key top right
plot '${dat_file}' using 1:2 with lines linewidth 2 linecolor rgb '#1e81b0' title 'puts/s', \
     '${dat_file}' using 1:3 with lines linewidth 2 linecolor rgb '#e28743' title 'gets/s'
EOF

    echo "Generated plot: $plot_file"
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

for put_percentage in 20 40 60 80 100; do
  for concurrency in 1 32; do
    log_file="$OUT/logs/${put_percentage}_${concurrency}.log"
    dat_file="$OUT/dats/${put_percentage}_${concurrency}.dat"
    plot_file="$OUT/plots/${put_percentage}_${concurrency}.png"
    num_keys=$((put_percentage * 1000))

    run_bench "$put_percentage" "$concurrency" "$num_keys" "$log_file"
    generate_dat "$log_file" "$dat_file"
    generate_plot "$dat_file" "$plot_file" "$put_percentage" "$concurrency"
  done
done
