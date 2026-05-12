# /// script
# requires-python = ">=3.10"
# dependencies = [
#     "python-dotenv",
# ]
# ///
"""
BlobDB Benchmark Orchestrator
==============================
Sweeps value sizes from 1KiB → 1MiB, comparing baseline SlateDB vs BlobDB
(key-value separation with threshold = val_size so every put goes to blob
storage). Writes one JSON line per bencher invocation to a local JSONL file.

Usage:
    python run_benchmarks.py [--duration N] [--db-size SIZE] [--dry-run] [--quick] [--wal]

  --duration N    seconds per run (default 120)
  --db-size SIZE  target dataset size per cell (default 1GiB, e.g. 10GiB, 500MB);
                  key_count is derived as db_size / val_size_bytes
  --dry-run       print commands without executing
  --quick         5s runs for smoke-testing
  --wal/--no-wal  enable/disable WAL (default: --no-wal)
"""

import argparse
import json
import os
import subprocess
import tempfile
import time
import uuid
from datetime import datetime
from pathlib import Path

from dotenv import load_dotenv

# ─────────────────────────────────────────────────────────────────────────────
# Config
# ─────────────────────────────────────────────────────────────────────────────
load_dotenv("bench.env")

AWS_BUCKET = os.environ["AWS_BUCKET"]
MC_ALIAS = os.environ.get("MC_ALIAS", "myminio")
RUN_TIMESTAMP = datetime.now().strftime("%Y%m%d-%H%M%S")
RESULTS_DIR = Path("target/bencher") / RUN_TIMESTAMP
RESULTS_FILE = RESULTS_DIR / "results.jsonl"
BENCHER_BIN = "cargo"

RESULTS_DIR.mkdir(parents=True, exist_ok=True)

# ─────────────────────────────────────────────────────────────────────────────
# Benchmark matrix
# ─────────────────────────────────────────────────────────────────────────────

# Measure workloads run against a DB that was pre-populated by `insert`.
# Order matters: each cell runs these sequentially against the loaded DB.
MEASURE_WORKLOADS = [
    "overwrite",  # random overwrites
    "update-heavy",  # point lookup/write mix (50/50)
    "range-mix",  # range scan/write mix (95/5)
    "point-lookup",  # pure point lookups
    "range-scan",  # pure range scans (10 Nexts/Seek via DEFAULT_SCAN_WIDTH)
]
VAL_SIZES = [1024, 4096, 16384, 65536, 262144, 1048576]  # 1KiB → 1MiB, ×4 log-spaced

# Concurrency is shared across the cell (insert + measures use the same value
# so the load partitions cleanly across the same task count).
CONCURRENCY = 16


def parse_size(s: str) -> int:
    """Parse a size like '10GiB', '500MB', '4096' into bytes."""
    units = {
        "": 1,
        "B": 1,
        "K": 1024,
        "KB": 1000,
        "KIB": 1024,
        "M": 1024**2,
        "MB": 1000**2,
        "MIB": 1024**2,
        "G": 1024**3,
        "GB": 1000**3,
        "GIB": 1024**3,
        "T": 1024**4,
        "TB": 1000**4,
        "TIB": 1024**4,
    }
    s = s.strip()
    i = 0
    while i < len(s) and (s[i].isdigit() or s[i] == "."):
        i += 1
    if i == 0:
        raise argparse.ArgumentTypeError(f"invalid size: {s!r}")
    unit = s[i:].upper().strip()
    if unit not in units:
        raise argparse.ArgumentTypeError(f"unknown size unit: {unit!r}")
    return int(float(s[:i]) * units[unit])


def fmt_size(b: int) -> str:
    """Format bytes as a compact human-readable label (e.g., 1024 → '1KiB')."""
    if b >= 1024 * 1024 and b % (1024 * 1024) == 0:
        return f"{b // (1024 * 1024)}MiB"
    if b >= 1024 and b % 1024 == 0:
        return f"{b // 1024}KiB"
    return f"{b}B"


# ─────────────────────────────────────────────────────────────────────────────
# Slatedb.toml generator
# ─────────────────────────────────────────────────────────────────────────────

BASE_TOML = """\
flush_interval = "100ms"
wal_enabled = {wal_enabled}

[compactor_options]
poll_interval = "1s"
"""


def _write_toml(kv_sep: bool, blob_threshold: int, wal_enabled: bool, tmpdir: str) -> str:
    content = BASE_TOML.format(wal_enabled="true" if wal_enabled else "false")
    if kv_sep:
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
    val_size_bytes: int,
    kv_sep: bool,
    blob_threshold: int,
    duration: int,
    path: str,
    key_count: int,
    wal_enabled: bool,
    dry_run: bool = False,
) -> dict | None:
    """Run a single bencher invocation. The DB at `path` is opened if it
    already exists (used by measure workloads after an `insert` phase) or
    created fresh otherwise. When `kv_sep` is False the blob threshold is
    ignored on both sides (TOML omits `[blob_options]`, bencher skips
    `--kv-sep`)."""
    with tempfile.TemporaryDirectory() as tmpdir:
        toml_path = _write_toml(kv_sep, blob_threshold, wal_enabled, tmpdir)

        cmd = [
            BENCHER_BIN,
            "run",
            "-r",
            "--package",
            "slatedb-bencher",
            "--",
            "--env-file",
            "bench.env",
            "--path",
            path,
            "blob-db",
            "--db-options-path",
            toml_path,
            "--workload",
            workload,
            "--val-size",
            str(val_size_bytes),
            "--blob-threshold",
            str(blob_threshold),
            "--duration",
            str(duration),
            "--concurrency",
            str(CONCURRENCY),
            "--key-count",
            str(key_count),
        ]
        if kv_sep:
            cmd.append("--kv-sep")

        label = (
            f"{'blobdb' if kv_sep else 'baseline':8s}  "
            f"{workload:14s}  val={fmt_size(val_size_bytes):>6}  "
            f"threshold={blob_threshold if kv_sep else '-':>6}  keys={key_count}"
        )
        print(f"  → {label}", flush=True)

        if dry_run:
            print(f"    CMD: {' '.join(cmd)}")
            return None

        env = {**os.environ, "RUST_LOG": "warn"}
        t0 = time.time()
        # Insert can take a long time for large datasets; give it room.
        load_budget = 3600 if workload == "insert" else duration + 600
        try:
            proc = subprocess.run(
                cmd,
                capture_output=True,
                text=True,
                env=env,
                timeout=load_budget,
            )
        except subprocess.TimeoutExpired:
            print(f"    TIMEOUT after {load_budget}s — skipping", flush=True)
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

        # Total write amplification: all bytes written to object storage on the
        # write path (WAL flushes + memtable→L0 flushes + blob pack uploads +
        # compaction outputs) ÷ user-supplied key+value bytes. Overrides the
        # bencher's `write_amp` field with the metric-based calculation.
        user_bytes = json_line.get("user_write_bytes", 0)
        wal_bytes = json_line.get("wal_flush_bytes", 0)
        l0_bytes = json_line.get("l0_flush_bytes", 0)
        blob_bytes = json_line.get("blob_flush_bytes", 0)
        compact_bytes = json_line.get("bytes_compacted", 0)
        physical_bytes = wal_bytes + l0_bytes + blob_bytes + compact_bytes
        wa = physical_bytes / user_bytes if user_bytes > 0 else 0.0
        json_line["write_amp"] = wa
        wops = json_line.get("write_ops_per_sec", 0)
        rops = json_line.get("read_ops_per_sec", 0)
        print(
            f"    done in {elapsed:.0f}s  WA={wa:.2f}x  "
            f"write_ops/s={wops:.0f}  read_ops/s={rops:.0f}",
            flush=True,
        )
        return json_line


def cleanup_path(path: str):
    """Best-effort: delete every object under `path` so the next cell starts fresh.
    Uses `mc rm --force --recursive`; assumes `mc alias set $MC_ALIAS …` has
    been run for whichever S3-compatible endpoint the bencher is hitting."""
    prefix = path.lstrip("/")
    target = f"{MC_ALIAS}/{AWS_BUCKET}/{prefix}"
    try:
        proc = subprocess.run(
            ["mc", "rm", "--force", "--recursive", target],
            capture_output=True,
            text=True,
        )
        if proc.returncode != 0:
            print(
                f"  [cleanup] mc rm failed for {target} (rc={proc.returncode}): "
                f"{proc.stderr.strip()}",
                flush=True,
            )
            return
        print(f"  [cleanup] removed {target}/*", flush=True)
    except FileNotFoundError:
        print("  [cleanup] mc not found on PATH — install MinIO client", flush=True)
    except Exception as exc:
        print(f"  [cleanup] failed for {target}: {exc}", flush=True)


# ─────────────────────────────────────────────────────────────────────────────
# Main
# ─────────────────────────────────────────────────────────────────────────────


def append_result(rec: dict):
    with open(RESULTS_FILE, "a") as f:
        f.write(json.dumps(rec) + "\n")


def run_cell(
    val_size_bytes: int,
    kv_sep: bool,
    threshold: int,
    measure_workloads: list[str],
    duration: int,
    db_size_bytes: int,
    run_uuid: str,
    cell_tag: str,
    keep_db: bool,
    wal_enabled: bool,
    dry_run: bool,
) -> list[dict]:
    """Run one (val_size, kv_sep, threshold) cell: insert once, then each
    measure workload against the same path. The path is left in place if
    `keep_db` so the next part can reuse it; otherwise it's deleted at the end.
    `db_size_bytes` ÷ `val_size_bytes` sets key_count.
    """
    key_count = max(1, db_size_bytes // val_size_bytes)
    path = f"/blobdb-bench-{run_uuid}-{cell_tag}"
    print(
        f"\n── cell: val_size={fmt_size(val_size_bytes)}  kv_sep={kv_sep}  "
        f"threshold={threshold if kv_sep else '-':>8}  "
        f"key_count={key_count}  path={path} ──"
    )

    out: list[dict] = []

    # Load phase. Ignored by the orchestrator's JSONL — we don't measure load
    # throughput here, we just need the data on disk.
    load = run_one(
        "insert",
        val_size_bytes,
        kv_sep,
        threshold,
        duration,
        path,
        key_count,
        wal_enabled,
        dry_run,
    )
    if load is None and not dry_run:
        print("    ABORT: insert failed for this cell")
        return out
    if load is not None:
        load["cell_tag"] = cell_tag
        load["phase"] = "load"
        append_result(load)
        out.append(load)

    for wl in measure_workloads:
        rec = run_one(
            wl,
            val_size_bytes,
            kv_sep,
            threshold,
            duration,
            path,
            key_count,
            wal_enabled,
            dry_run,
        )
        if rec is None:
            continue
        rec["cell_tag"] = cell_tag
        rec["phase"] = "measure"
        append_result(rec)
        out.append(rec)

    if not keep_db and not dry_run:
        cleanup_path(path)

    return out


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--duration", type=int, default=120, help="seconds per measure run")
    ap.add_argument("--dry-run", action="store_true")
    ap.add_argument("--quick", action="store_true", help="5s smoke runs")
    ap.add_argument(
        "--db-size",
        type=parse_size,
        default=parse_size("1GiB"),
        help="target dataset size per cell (e.g. 10GiB, 500MB); "
        "key_count = db_size / val_size_bytes",
    )
    ap.add_argument(
        "--keep-db",
        action="store_true",
        help="don't delete the populated DBs at the end of each cell",
    )
    ap.add_argument(
        "--wal",
        action=argparse.BooleanOptionalAction,
        default=False,
        help="enable WAL (default: disabled). Use --wal to enable, --no-wal to disable.",
    )
    args = ap.parse_args()

    duration = 5 if args.quick else args.duration
    db_size_bytes = args.db_size

    run_uuid = uuid.uuid4().hex[:8]
    print(f"=== BlobDB Benchmark Suite  (run_id={run_uuid}) ===")
    print(
        f"  duration={duration}s  bucket={AWS_BUCKET}  db_size={db_size_bytes:,} B  "
        f"wal={'on' if args.wal else 'off'}"
    )
    print(
        "  derived key counts: "
        + ", ".join(f"{fmt_size(v)}={max(1, db_size_bytes // v):,}" for v in VAL_SIZES)
    )
    print(f"  results → {RESULTS_FILE}")

    results: list[dict] = []

    # ── Value-size sweep: baseline vs blob (threshold = val_size, all-blob) ──
    print("\n── Value-size sweep: baseline vs blob (threshold = val_size) ──")
    for val_size in VAL_SIZES:
        for kv_sep, threshold in [(False, 0), (True, val_size)]:
            tag = f"vs{val_size}-{'b' if kv_sep else 'x'}"
            results.extend(
                run_cell(
                    val_size,
                    kv_sep,
                    threshold,
                    MEASURE_WORKLOADS,
                    duration,
                    db_size_bytes,
                    run_uuid,
                    tag,
                    keep_db=False,
                    wal_enabled=args.wal,
                    dry_run=args.dry_run,
                )
            )

    # ── Summary ──────────────────────────────────────────────────────────────
    if results:
        print("\n" + "=" * 64)
        print(f"{'Completed runs':^64}")
        print("=" * 64)
        for r in results:
            phase = r.get("phase", "?")
            print(
                f"  [{phase:7s}] {r['workload']:14s} "
                f"val={fmt_size(r['val_size_bytes']):>6}  "
                f"{'blobdb' if r['blob_enabled'] else 'base':6s}  "
                f"WA={r['write_amp']:.2f}x"
            )

    print(f"\nAll done. Results written to {RESULTS_FILE}")


if __name__ == "__main__":
    main()
