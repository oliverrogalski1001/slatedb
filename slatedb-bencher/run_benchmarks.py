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

  --duration N         seconds per run (default 120)
  --db-size SIZE       target dataset size per cell (default 1GiB, e.g. 10GiB, 500MB);
                       key_count is derived as db_size / val_size_bytes
  --space-cool-down N  seconds to wait after the last measure workload before
                       sampling object-store size for space amplification
                       (default 60; needs to be ≥ max GC min_age for an
                       accurate steady-state number — see Slatedb.toml)
  --dry-run            print commands without executing
  --quick              5s runs for smoke-testing
  --wal/--no-wal       enable/disable WAL (default: --no-wal)
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
CONCURRENCY = 26

# Cache budget shared across the comparison. Baseline gets the whole budget as
# block cache; BlobDB splits it between block cache (pointer SSTs) and blob
# cache (values). Meta cache is held constant on both sides — it's index+bloom,
# orthogonal to where values live, and shouldn't count toward the comparable
# budget.
TOTAL_DATA_CACHE_BYTES = 10 * 1024**3  # 10 GiB (≈10% of a 100 GiB dataset)
META_CACHE_BYTES = 64 * 1024 * 1024  # 64 MiB
# Pointer-side SST entry ≈ key_len(16) + pointer(~16) + block-header slack.
POINTER_ENTRY_BYTES = 48
MIN_BLOCK_CACHE_BYTES = 64 * 1024 * 1024


def split_caches(
    key_count: int, total: int, kv_sep: bool
) -> tuple[int, int | None]:
    """Split a fixed cache budget between block and blob caches.

    Baseline (kv_sep=False) gives the whole budget to the block cache. BlobDB
    sizes the block cache to cover the pointer-SST footprint with headroom
    (1.5×), then donates the rest to the blob cache. Block is capped at 90%
    of the total so the blob cache is always present."""
    if not kv_sep:
        return total, None
    pointer_footprint = key_count * POINTER_ENTRY_BYTES
    desired_block = max(MIN_BLOCK_CACHE_BYTES, int(1.5 * pointer_footprint))
    cap = max(MIN_BLOCK_CACHE_BYTES, int(total * 0.9))
    block = min(desired_block, cap)
    blob = max(0, total - block)
    return block, blob


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
    if b >= 1024**3 and b % (1024**3) == 0:
        return f"{b // (1024**3)}GiB"
    if b >= 1024**3:
        return f"{b / (1024**3):.1f}GiB"
    if b >= 1024 * 1024 and b % (1024 * 1024) == 0:
        return f"{b // (1024 * 1024)}MiB"
    if b >= 1024 * 1024:
        return f"{b / (1024 * 1024):.1f}MiB"
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

[garbage_collector_options.manifest_options]
interval = "60s"
min_age = "120s"

[garbage_collector_options.wal_options]
interval = "60s"
min_age = "60s"

[garbage_collector_options.compacted_options]
interval = "60s"
min_age = "120s"

[garbage_collector_options.compactions_options]
interval = "60s"
min_age = "120s"

[garbage_collector_options.blob_options]
interval = "60s"
min_age = "60s"
"""


def _write_toml(
    kv_sep: bool,
    blob_threshold: int,
    wal_enabled: bool,
    blob_cache_bytes: int | None,
    tmpdir: str,
) -> str:
    content = BASE_TOML.format(wal_enabled="true" if wal_enabled else "false")
    if kv_sep:
        content += f"\n[blob_options]\nmin_value_size = {blob_threshold}\n"
        if blob_cache_bytes is not None and blob_cache_bytes > 0:
            content += (
                f"[blob_options.cache]\n"
                f"max_capacity_bytes = {blob_cache_bytes}\n"
            )
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
    block_cache_bytes: int,
    meta_cache_bytes: int,
    blob_cache_bytes: int | None,
    dry_run: bool = False,
) -> dict | None:
    """Run a single bencher invocation. The DB at `path` is opened if it
    already exists (used by measure workloads after an `insert` phase) or
    created fresh otherwise. When `kv_sep` is False the blob threshold is
    ignored on both sides (TOML omits `[blob_options]`, bencher skips
    `--kv-sep`)."""
    with tempfile.TemporaryDirectory() as tmpdir:
        toml_path = _write_toml(
            kv_sep, blob_threshold, wal_enabled, blob_cache_bytes, tmpdir
        )

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
            "--block-cache-size",
            str(block_cache_bytes),
            "--meta-cache-size",
            str(meta_cache_bytes),
        ]
        if kv_sep:
            cmd.append("--kv-sep")

        blob_str = (
            fmt_size(blob_cache_bytes)
            if (kv_sep and blob_cache_bytes is not None)
            else "-"
        )
        label = (
            f"{'blobdb' if kv_sep else 'baseline':8s}  "
            f"{workload:14s}  val={fmt_size(val_size_bytes):>6}  "
            f"threshold={blob_threshold if kv_sep else '-':>6}  keys={key_count}  "
            f"block={fmt_size(block_cache_bytes):>6}  blob={blob_str:>6}"
        )
        print(f"  → {label}", flush=True)

        if dry_run:
            print(f"    CMD: {' '.join(cmd)}")
            return None

        env = {**os.environ, "RUST_LOG": "warn"}
        t0 = time.time()
        # Insert can take a long time for large datasets; give it room.
        # load_budget = 3600 if workload == "insert" else duration + 600
        proc = subprocess.run(
            cmd,
            capture_output=True,
            text=True,
            env=env,
        )

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


def measure_storage_bytes(path: str) -> int | None:
    """Return the total bytes stored under `path` in the object store via
    `mc du --json`. Returns None on failure. `mc du` emits one JSON object
    per directory plus a final summary; the last line is the recursive total."""
    prefix = path.lstrip("/")
    target = f"{MC_ALIAS}/{AWS_BUCKET}/{prefix}"
    try:
        proc = subprocess.run(
            ["mc", "du", "--json", target],
            capture_output=True,
            text=True,
        )
        if proc.returncode != 0:
            print(
                f"  [space] mc du failed for {target} (rc={proc.returncode}): "
                f"{proc.stderr.strip()}",
                flush=True,
            )
            return None
        lines = [ln for ln in proc.stdout.splitlines() if ln.strip()]
        if not lines:
            print(f"  [space] mc du returned no output for {target}", flush=True)
            return None
        last = json.loads(lines[-1])
        return int(last.get("size", 0))
    except FileNotFoundError:
        print("  [space] mc not found on PATH — install MinIO client", flush=True)
        return None
    except Exception as exc:
        print(f"  [space] failed for {target}: {exc}", flush=True)
        return None


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
    space_cool_down: int,
    total_data_cache: int,
    meta_cache_bytes: int,
    dry_run: bool,
) -> list[dict]:
    """Run one (val_size, kv_sep, threshold) cell: insert once, then each
    measure workload against the same path. The path is left in place if
    `keep_db` so the next part can reuse it; otherwise it's deleted at the end.
    `db_size_bytes` ÷ `val_size_bytes` sets key_count.
    """
    key_count = max(1, db_size_bytes // val_size_bytes)
    block_cache_bytes, blob_cache_bytes = split_caches(
        key_count, total_data_cache, kv_sep
    )
    path = f"/blobdb-bench-{run_uuid}-{cell_tag}"
    print(
        f"\n── cell: val_size={fmt_size(val_size_bytes)}  kv_sep={kv_sep}  "
        f"threshold={threshold if kv_sep else '-':>8}  "
        f"key_count={key_count}  "
        f"block={fmt_size(block_cache_bytes)}  "
        f"blob={fmt_size(blob_cache_bytes) if blob_cache_bytes is not None else '-'}  "
        f"path={path} ──"
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
        block_cache_bytes,
        meta_cache_bytes,
        blob_cache_bytes,
        dry_run,
    )
    if load is None and not dry_run:
        print("    ABORT: insert failed for this cell")
        return out
    if load is not None:
        load["cell_tag"] = cell_tag
        load["phase"] = "load"
        load["block_cache_bytes"] = block_cache_bytes
        load["meta_cache_bytes"] = meta_cache_bytes
        load["blob_cache_bytes"] = blob_cache_bytes
        load["total_data_cache_bytes"] = total_data_cache
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
            block_cache_bytes,
            meta_cache_bytes,
            blob_cache_bytes,
            dry_run,
        )
        if rec is None:
            continue
        rec["cell_tag"] = cell_tag
        rec["phase"] = "measure"
        rec["block_cache_bytes"] = block_cache_bytes
        rec["meta_cache_bytes"] = meta_cache_bytes
        rec["blob_cache_bytes"] = blob_cache_bytes
        rec["total_data_cache_bytes"] = total_data_cache
        append_result(rec)
        out.append(rec)

    # ── Space amplification ────────────────────────────────────────────────
    # Wait for compaction + GC to settle so the on-disk size reflects the
    # steady state, then ask the object store for the total bytes under
    # `path`. Denominator is the load phase's `user_write_bytes` — the
    # actual user-facing bytes the bencher managed to insert (insert mode
    # writes unique keys, so this is the DB's logical footprint at the end
    # of the load phase). Measure-phase writes are mostly overwrites of
    # those keys and don't grow logical state.
    load_user_bytes = int(load.get("user_write_bytes", 0)) if load else 0
    if not dry_run:
        if load_user_bytes <= 0:
            print(
                "  [space] skipping: load phase emitted no user_write_bytes",
                flush=True,
            )
        else:
            if space_cool_down > 0:
                print(
                    f"  [space] sleeping {space_cool_down}s for compaction/GC to settle...",
                    flush=True,
                )
                time.sleep(space_cool_down)
            physical_bytes = measure_storage_bytes(path)
            if physical_bytes is not None:
                space_amp = physical_bytes / load_user_bytes
                print(
                    f"  [space] physical={physical_bytes:,}B  "
                    f"load_user_bytes={load_user_bytes:,}B  SA={space_amp:.2f}x",
                    flush=True,
                )
                space_record = {
                    "phase": "space",
                    "workload": "space-amp",
                    "cell_tag": cell_tag,
                    "val_size_bytes": val_size_bytes,
                    "key_count": key_count,
                    "blob_enabled": kv_sep,
                    "blob_threshold": threshold if kv_sep else 0,
                    "wal_enabled": wal_enabled,
                    "logical_bytes": load_user_bytes,
                    "physical_storage_bytes": physical_bytes,
                    "space_amp": space_amp,
                    "space_cool_down_seconds": space_cool_down,
                    "block_cache_bytes": block_cache_bytes,
                    "meta_cache_bytes": meta_cache_bytes,
                    "blob_cache_bytes": blob_cache_bytes,
                    "total_data_cache_bytes": total_data_cache,
                    # Echo a write_amp so the summary loop's lookup is safe.
                    "write_amp": 0.0,
                }
                append_result(space_record)
                out.append(space_record)
                # In-memory backfill for the end-of-run summary print. The
                # on-disk JSONL keeps each row independent; downstream
                # analysis should join `phase=space` rows to workload rows
                # on `cell_tag`.
                for r in out:
                    if r.get("phase") in ("load", "measure"):
                        r["space_amp"] = space_amp

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
    ap.add_argument(
        "--space-cool-down",
        type=int,
        default=60,
        help="seconds to wait after the last measure workload before sampling "
        "object-store size for space amplification (0 disables). Should be "
        "≥ max GC min_age in Slatedb.toml for steady-state accuracy.",
    )
    ap.add_argument(
        "--total-data-cache",
        type=parse_size,
        default=TOTAL_DATA_CACHE_BYTES,
        help="total memory budget for value-side caches (block + blob). "
        "Baseline gives all to block cache; BlobDB splits between block "
        "(pointer SSTs) and blob (values). Default 10GiB.",
    )
    ap.add_argument(
        "--meta-cache",
        type=parse_size,
        default=META_CACHE_BYTES,
        help="meta cache size (index + bloom). Held constant on both arms "
        "and excluded from --total-data-cache. Default 64MiB.",
    )
    args = ap.parse_args()

    duration = 5 if args.quick else args.duration
    space_cool_down = 0 if args.quick else args.space_cool_down
    db_size_bytes = args.db_size
    total_data_cache = args.total_data_cache
    meta_cache_bytes = args.meta_cache

    run_uuid = uuid.uuid4().hex[:8]
    print(f"=== BlobDB Benchmark Suite  (run_id={run_uuid}) ===")
    print(
        f"  duration={duration}s  bucket={AWS_BUCKET}  db_size={db_size_bytes:,} B  "
        f"wal={'on' if args.wal else 'off'}"
    )
    print(
        f"  cache budget: total_data={fmt_size(total_data_cache)}  "
        f"meta={fmt_size(meta_cache_bytes)} (constant on both arms)"
    )
    print(
        "  derived key counts: "
        + ", ".join(f"{fmt_size(v)}={max(1, db_size_bytes // v):,}" for v in VAL_SIZES)
    )
    print("  BlobDB block/blob split per val_size (baseline gets total as block):")
    for v in VAL_SIZES:
        kc = max(1, db_size_bytes // v)
        b, bl = split_caches(kc, total_data_cache, kv_sep=True)
        print(
            f"    val={fmt_size(v):>6}  block={fmt_size(b):>8}  "
            f"blob={fmt_size(bl):>8}"
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
                    space_cool_down=space_cool_down,
                    total_data_cache=total_data_cache,
                    meta_cache_bytes=meta_cache_bytes,
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
            sa = r.get("space_amp")
            sa_str = f"  SA={sa:.2f}x" if sa is not None else ""
            print(
                f"  [{phase:7s}] {r['workload']:14s} "
                f"val={fmt_size(r['val_size_bytes']):>6}  "
                f"{'blobdb' if r['blob_enabled'] else 'base':6s}  "
                f"WA={r['write_amp']:.2f}x{sa_str}"
            )

    print(f"\nAll done. Results written to {RESULTS_FILE}")


if __name__ == "__main__":
    main()
