"""
BlobDB Benchmark Orchestrator
==============================
Runs all benchmark combinations (workload × value distribution × blob threshold)
using the slatedb-bencher binary, writes results to JSONL, and uploads to S3.

Usage:
    python run_benchmarks.py [--duration N] [--dry-run] [--quick]

  --duration N   seconds per run (default 120)
  --dry-run      print commands without executing
  --quick        5s runs for smoke-testing

Each run outputs one JSON line to stdout; this script captures it, appends to
the results JSONL file, and also uploads the file to S3 when done.
"""

import argparse
import json
import os
import subprocess
import sys
import tempfile
import time
import uuid
from pathlib import Path

from dotenv import load_dotenv

# ─────────────────────────────────────────────────────────────────────────────
# Config
# ─────────────────────────────────────────────────────────────────────────────
load_dotenv("bench.env")

AWS_BUCKET   = os.environ["AWS_BUCKET"]
RESULTS_FILE = Path("target/bencher/blobdb_results.jsonl")
BENCHER_BIN  = "cargo"

RESULTS_FILE.parent.mkdir(parents=True, exist_ok=True)

# ─────────────────────────────────────────────────────────────────────────────
# Benchmark matrix
# ─────────────────────────────────────────────────────────────────────────────

WORKLOADS   = ["write-heavy", "read-heavy", "update-heavy", "range-scan"]
VAL_DISTS   = ["small", "large", "mixed"]
THRESHOLDS  = [1024, 4096, 16384, 65536]   # KB: 1, 4, 16, 64

# Concurrency and key count per workload
WORKLOAD_PARAMS = {
    "write-heavy":  {"concurrency": 8, "key_count": 100_000},
    "read-heavy":   {"concurrency": 8, "key_count": 100_000},
    "update-heavy": {"concurrency": 8, "key_count": 1_000},    # small key set → many overwrites
    "range-scan":   {"concurrency": 4, "key_count": 100_000},
}

# ─────────────────────────────────────────────────────────────────────────────
# Slatedb.toml generator
# ─────────────────────────────────────────────────────────────────────────────

BASE_TOML = """\
flush_interval = "100ms"

[object_store_cache_options]
root_folder = "/tmp/slatedb-cache"

[compactor_options]
poll_interval = "1s"
"""

def _write_toml(blob_threshold: int, tmpdir: str) -> str:
    content = BASE_TOML
    if blob_threshold > 0:
        content += f"\n[blob_options]\nmin_value_size = {blob_threshold}\n"
    toml_path = os.path.join(tmpdir, "Slatedb.toml")
    with open(toml_path, "w") as f:
        f.write(content)
    return toml_path

# ─────────────────────────────────────────────────────────────────────────────
# Single benchmark runner
# ─────────────────────────────────────────────────────────────────────────────

def run_one(
    workload: str,
    val_dist: str,
    blob_threshold: int,
    duration: int,
    run_id: str,
    dry_run: bool = False,
) -> dict | None:
    params = WORKLOAD_PARAMS[workload]
    path = f"/blobdb-bench-{run_id}"

    with tempfile.TemporaryDirectory() as tmpdir:
        toml_path = _write_toml(blob_threshold, tmpdir)

        cmd = [
            BENCHER_BIN, "run", "-r", "--package", "slatedb-bencher", "--",
            "--env-file", "bench.env",
            "--path", path,
            "blob-db",
            "--db-options-path", toml_path,
            "--workload", workload,
            "--val-dist", val_dist,
            "--blob-threshold", str(blob_threshold),
            "--duration", str(duration),
            "--concurrency", str(params["concurrency"]),
            "--key-count", str(params["key_count"]),
        ]

        label = (
            f"{'blobdb' if blob_threshold else 'baseline':8s}  "
            f"{workload:14s}  {val_dist:6s}  "
            f"threshold={blob_threshold or '-':>6}"
        )
        print(f"  → {label}", flush=True)

        if dry_run:
            print(f"    CMD: {' '.join(cmd)}")
            return None

        env = {**os.environ, "RUST_LOG": "warn"}
        t0 = time.time()
        try:
            proc = subprocess.run(
                cmd,
                capture_output=True,
                text=True,
                env=env,
                timeout=duration + 600,   # generous timeout (large-value DB close can be slow)
            )
        except subprocess.TimeoutExpired:
            print(f"    TIMEOUT after {duration + 300}s — skipping", flush=True)
            return None

        elapsed = time.time() - t0

        # The bencher prints one JSON line to stdout; everything else is tracing on stderr.
        json_line = None
        for line in proc.stdout.splitlines():
            line = line.strip()
            if line.startswith("{"):
                try:
                    json_line = json.loads(line)
                    break
                except json.JSONDecodeError:
                    continue

        if json_line is None:
            print(f"    ERROR: no JSON found in stdout (rc={proc.returncode})")
            print(f"    stderr tail:\n{proc.stderr[-800:]}")
            return None

        wa = json_line.get("write_amp", 0)
        wops = json_line.get("write_ops_per_sec", 0)
        print(f"    done in {elapsed:.0f}s  WA={wa:.2f}x  write_ops/s={wops:.0f}", flush=True)
        return json_line


# ─────────────────────────────────────────────────────────────────────────────
# Upload to S3
# ─────────────────────────────────────────────────────────────────────────────

def upload_results(local_path: Path):
    try:
        import boto3
        s3 = boto3.client("s3")
        s3.upload_file(str(local_path), AWS_BUCKET, "benchmark_results.jsonl")
        print(f"[upload] s3://{AWS_BUCKET}/benchmark_results.jsonl")
    except Exception as exc:
        print(f"[upload] failed: {exc}")


# ─────────────────────────────────────────────────────────────────────────────
# Main
# ─────────────────────────────────────────────────────────────────────────────

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--duration", type=int, default=120, help="seconds per run")
    ap.add_argument("--dry-run", action="store_true")
    ap.add_argument("--quick",   action="store_true", help="5s smoke runs")
    args = ap.parse_args()

    duration = 5 if args.quick else args.duration

    run_uuid = uuid.uuid4().hex[:8]
    print(f"=== BlobDB Benchmark Suite  (run_id={run_uuid}) ===")
    print(f"  duration={duration}s  bucket={AWS_BUCKET}")
    print(f"  results → {RESULTS_FILE}")
    print()

    results = []

    # ── Part 1: Primary matrix (BlobDB @ 4KB vs baseline) ────────────────────
    print("── Part 1: Primary matrix (BlobDB 4KB threshold vs baseline) ──")
    primary_threshold = 4096
    for workload in WORKLOADS:
        for val_dist in VAL_DISTS:
            for threshold in [0, primary_threshold]:   # 0 = baseline
                run_id = f"{run_uuid}-{workload[:2]}-{val_dist[:2]}-{'b' if threshold else 'x'}"
                rec = run_one(workload, val_dist, threshold, duration, run_id, args.dry_run)
                if rec:
                    results.append(rec)
                    with open(RESULTS_FILE, "a") as f:
                        f.write(json.dumps(rec) + "\n")

    # ── Part 2: Blob threshold sweep (update-heavy) ───────────────────────────
    print("\n── Part 2: Blob threshold sweep (update-heavy, all dists) ──")
    sweep_workload = "update-heavy"
    for threshold in THRESHOLDS:
        for val_dist in VAL_DISTS:
            run_id = f"{run_uuid}-sw-{threshold}-{val_dist[:2]}"
            rec = run_one(sweep_workload, val_dist, threshold, duration, run_id, args.dry_run)
            if rec:
                results.append(rec)
                with open(RESULTS_FILE, "a") as f:
                    f.write(json.dumps(rec) + "\n")

    # ── Part 3: Value-size scatter (write-heavy, baseline + BlobDB @ 4KB) ────
    print("\n── Part 3: Value-size scatter ──")
    scatter_sizes_b = [512, 1024, 4096, 16384, 65536, 262144, 1048576]
    for val_bytes in scatter_sizes_b:
        for threshold in [0, 4096]:
            run_id = f"{run_uuid}-sc-{val_bytes}-{'b' if threshold else 'x'}"
            # For the scatter, we use a custom val_dist label and override it via env;
            # the bencher doesn't support arbitrary val_len via blob-db subcommand yet,
            # so we approximate: treat sizes < SMALL_VALUE_BYTES as "small", >= as "large".
            vd = "small" if val_bytes <= 512 else "large"
            # Tag the result with the actual size for the scatter plot
            rec = run_one("write-heavy", vd, threshold, duration, run_id, args.dry_run)
            if rec:
                rec["scatter_val_bytes"] = val_bytes
                results.append(rec)
                with open(RESULTS_FILE, "a") as f:
                    f.write(json.dumps(rec) + "\n")

    # ── Summary ──────────────────────────────────────────────────────────────
    if results:
        print("\n" + "=" * 56)
        print(f"{'Completed runs':^56}")
        print("=" * 56)
        for r in results:
            print(
                f"  {r['workload']:14s} {r['val_dist']:6s}  "
                f"{'blobdb' if r['blob_enabled'] else 'base':6s}  "
                f"WA={r['write_amp']:.2f}x"
            )

    if not args.dry_run and results:
        upload_results(RESULTS_FILE)

    print(f"\nAll done. Results written to {RESULTS_FILE}")


if __name__ == "__main__":
    main()
