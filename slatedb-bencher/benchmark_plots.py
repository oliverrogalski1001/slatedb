"""
BlobDB vs SlateDB Benchmark Visualization
==========================================
Loads results from `target/bencher/<timestamp>/results.jsonl` (or an explicit
path / glob) and plots BlobDB (key-value separation) against the baseline
SlateDB across the value-size sweep produced by `run_benchmarks.py`.

Plots produced (under ./output):
  1. Write amplification        vs value size — faceted by workload
  2. Write throughput (MiB/s)   vs value size — faceted by workload
  3. Read  throughput (MiB/s)   vs value size — faceted by read workloads
  4. P99 latency (put / get)    vs value size — faceted by workload
  5. WA-improvement heatmap     (SlateDB ÷ BlobDB) over workload × value size
  6. Write amplification at representative value sizes — grouped bars
  7. Summary table printed to stdout
"""

import argparse
import json
import warnings
from pathlib import Path

import matplotlib
import matplotlib.pyplot as plt
import matplotlib.ticker as ticker
import numpy as np
import pandas as pd

matplotlib.rcParams["pdf.fonttype"] = 42   # editable text in Illustrator
matplotlib.rcParams["ps.fonttype"] = 42
warnings.filterwarnings("ignore")

# ─────────────────────────────────────────────────────────────────────────────
# Dimensions emitted by the bencher (workload names are underscore-form, see
# WorkloadPattern::as_str in slatedb-bencher/src/blob_db.rs).
# ─────────────────────────────────────────────────────────────────────────────
LOAD_WORKLOAD = "insert"
MEASURE_WORKLOADS = ["overwrite", "update_heavy", "range_mix", "point_lookup", "range_scan"]
ALL_WORKLOADS = [LOAD_WORKLOAD] + MEASURE_WORKLOADS

# Workloads that issue reads (used to filter read-only plots).
READ_WORKLOADS = ["update_heavy", "range_mix", "point_lookup", "range_scan"]
# Workloads that issue writes (used to filter put-latency plots).
WRITE_WORKLOADS = ["insert", "overwrite", "update_heavy", "range_mix"]

VAL_SIZES = [1024, 4096, 16384, 65536, 262144, 1048576]   # 1KiB → 1MiB

# Publication palette: BlobDB = teal, SlateDB = coral.
PALETTE = {"BlobDB": "#2196A6", "SlateDB": "#E05C3A"}
SYSTEMS = ["BlobDB", "SlateDB"]


def fmt_size(b: int) -> str:
    if b >= 1024 * 1024 and b % (1024 * 1024) == 0:
        return f"{b // (1024 * 1024)}MiB"
    if b >= 1024 and b % 1024 == 0:
        return f"{b // 1024}KiB"
    return f"{b}B"


def display_workload(w: str) -> str:
    return w.replace("_", "-")


# ─────────────────────────────────────────────────────────────────────────────
# Results loading
# ─────────────────────────────────────────────────────────────────────────────

def find_latest_results() -> Path | None:
    """Return the most recent `target/bencher/<timestamp>/results.jsonl`, or
    None if no run is on disk."""
    root = Path("target/bencher")
    if not root.exists():
        return None
    candidates = sorted(root.glob("*/results.jsonl"))
    return candidates[-1] if candidates else None


def load_records(path: Path) -> pd.DataFrame:
    records = []
    with path.open() as f:
        for line in f:
            line = line.strip()
            if not line.startswith("{"):
                continue
            try:
                records.append(json.loads(line))
            except json.JSONDecodeError:
                pass
    if not records:
        raise SystemExit(f"No JSON records in {path}")
    df = pd.DataFrame(records)

    # Normalise legacy hyphen-form workload names to underscore-form so older
    # runs still group with current ones.
    df["workload"] = df["workload"].str.replace("-", "_", regex=False)

    df["system"] = df["blob_enabled"].map({True: "BlobDB", False: "SlateDB"})
    df["val_size_label"] = df["val_size_bytes"].apply(fmt_size)

    # The orchestrator recomputes write_amp from byte counters; honour that if
    # the field is present, otherwise fall back to the bencher's own value.
    return df


def cell(df: pd.DataFrame, workload: str, system: str, metric: str) -> pd.Series:
    """Return metric values for (workload, system) indexed by val_size_bytes,
    reindexed to the full VAL_SIZES axis so missing points appear as NaN."""
    sub = df[(df["workload"] == workload) & (df["system"] == system)]
    if sub.empty:
        return pd.Series(index=VAL_SIZES, dtype=float)
    return (
        sub.groupby("val_size_bytes")[metric]
        .mean()
        .reindex(VAL_SIZES)
    )


# ─────────────────────────────────────────────────────────────────────────────
# Plotting helpers
# ─────────────────────────────────────────────────────────────────────────────

def _save(fig: plt.Figure, name: str, output_dir: Path, dpi: int = 200):
    path = output_dir / name
    fig.savefig(path, dpi=dpi, bbox_inches="tight")
    print(f"  saved → {path}")


def _set_size_axis(ax):
    ax.set_xscale("log", base=2)
    ax.set_xticks(VAL_SIZES)
    ax.xaxis.set_major_formatter(
        ticker.FuncFormatter(lambda v, _: fmt_size(int(v)))
    )
    ax.tick_params(axis="x", labelrotation=30, labelsize=8)


def _facet_grid(workloads: list[str], figsize=(15, 8)) -> tuple[plt.Figure, np.ndarray]:
    """2×3 grid covering up to 6 workloads; turn off any unused axes."""
    nrows, ncols = 2, 3
    fig, axes = plt.subplots(nrows, ncols, figsize=figsize, sharex=True)
    for ax in axes.flat[len(workloads):]:
        ax.axis("off")
    return fig, axes


def _line_plot(ax, df, workload, metric, ylabel, log_y=False):
    for sys, marker in [("BlobDB", "o"), ("SlateDB", "s")]:
        s = cell(df, workload, sys, metric)
        if s.notna().any():
            ax.plot(
                s.index, s.values,
                marker=marker, linestyle="-", linewidth=2, markersize=6,
                color=PALETTE[sys], label=sys, zorder=3,
            )
    ax.set_title(display_workload(workload), fontsize=11)
    ax.set_ylabel(ylabel, fontsize=9)
    if log_y:
        ax.set_yscale("log")
        ax.yaxis.set_major_formatter(ticker.FuncFormatter(lambda v, _: f"{v:g}"))
    _set_size_axis(ax)
    ax.grid(True, which="major", alpha=0.3)
    ax.legend(fontsize=8)


# ─────────────────────────────────────────────────────────────────────────────
# Plots
# ─────────────────────────────────────────────────────────────────────────────

def plot_write_amp(df, output_dir):
    print("[graph 1] Write amplification vs value size")
    fig, axes = _facet_grid(ALL_WORKLOADS)
    for ax, wl in zip(axes.flat, ALL_WORKLOADS):
        _line_plot(ax, df, wl, "write_amp", "Write amp (×)")
    for ax in axes[-1, :]:
        ax.set_xlabel("Value size", fontsize=9)
    fig.suptitle(
        "Write Amplification vs Value Size — BlobDB (KV-separated) vs SlateDB baseline",
        fontsize=13, y=1.00,
    )
    fig.tight_layout()
    _save(fig, "graph1_write_amp_vs_valsize.png", output_dir)
    plt.close(fig)


def plot_write_throughput(df, output_dir):
    print("[graph 2] Write throughput (MiB/s) vs value size")
    workloads = [w for w in ALL_WORKLOADS if w in WRITE_WORKLOADS]
    fig, axes = _facet_grid(workloads)
    for ax, wl in zip(axes.flat, workloads):
        _line_plot(ax, df, wl, "write_mibps", "Write MiB/s")
    for ax in axes[-1, :]:
        ax.set_xlabel("Value size", fontsize=9)
    fig.suptitle("Write Throughput vs Value Size", fontsize=13, y=1.00)
    fig.tight_layout()
    _save(fig, "graph2_write_throughput.png", output_dir)
    plt.close(fig)


def plot_read_throughput(df, output_dir):
    print("[graph 3] Read throughput (MiB/s) vs value size")
    workloads = [w for w in ALL_WORKLOADS if w in READ_WORKLOADS]
    fig, axes = _facet_grid(workloads)
    for ax, wl in zip(axes.flat, workloads):
        _line_plot(ax, df, wl, "read_mibps", "Read MiB/s")
    for ax in axes[-1, :]:
        ax.set_xlabel("Value size", fontsize=9)
    fig.suptitle("Read Throughput vs Value Size", fontsize=13, y=1.00)
    fig.tight_layout()
    _save(fig, "graph3_read_throughput.png", output_dir)
    plt.close(fig)


def plot_latency(df, output_dir):
    print("[graph 4] P99 latency vs value size")
    fig, axes = plt.subplots(1, 2, figsize=(14, 5))
    ax_put, ax_get = axes

    # P99 put latency, all write-bearing workloads on one axis (one line per
    # (workload, system) pair). Convert µs → ms for readability.
    for wl in [w for w in ALL_WORKLOADS if w in WRITE_WORKLOADS]:
        for sys, ls in [("BlobDB", "-"), ("SlateDB", "--")]:
            s = cell(df, wl, sys, "put_p99_us") / 1000.0
            if s.notna().any():
                ax_put.plot(
                    s.index, s.values,
                    linestyle=ls, marker="o", linewidth=1.6, markersize=4,
                    color=PALETTE[sys],
                    label=f"{display_workload(wl)} / {sys}",
                )
    ax_put.set_title("Put P99 latency", fontsize=12)
    ax_put.set_ylabel("Latency (ms, log)", fontsize=10)
    ax_put.set_yscale("log")
    ax_put.yaxis.set_major_formatter(ticker.FuncFormatter(lambda v, _: f"{v:g}"))
    _set_size_axis(ax_put)
    ax_put.set_xlabel("Value size", fontsize=10)
    ax_put.legend(fontsize=7, ncol=2)
    ax_put.grid(True, which="major", alpha=0.3)

    for wl in [w for w in ALL_WORKLOADS if w in READ_WORKLOADS]:
        for sys, ls in [("BlobDB", "-"), ("SlateDB", "--")]:
            s = cell(df, wl, sys, "get_p99_us") / 1000.0
            if s.notna().any():
                ax_get.plot(
                    s.index, s.values,
                    linestyle=ls, marker="o", linewidth=1.6, markersize=4,
                    color=PALETTE[sys],
                    label=f"{display_workload(wl)} / {sys}",
                )
    ax_get.set_title("Get / Scan P99 latency", fontsize=12)
    ax_get.set_ylabel("Latency (ms, log)", fontsize=10)
    ax_get.set_yscale("log")
    ax_get.yaxis.set_major_formatter(ticker.FuncFormatter(lambda v, _: f"{v:g}"))
    _set_size_axis(ax_get)
    ax_get.set_xlabel("Value size", fontsize=10)
    ax_get.legend(fontsize=7, ncol=2)
    ax_get.grid(True, which="major", alpha=0.3)

    fig.suptitle("P99 Latency vs Value Size — BlobDB vs SlateDB", fontsize=13, y=1.02)
    fig.tight_layout()
    _save(fig, "graph4_latency_p99.png", output_dir)
    plt.close(fig)


def plot_wa_heatmap(df, output_dir):
    print("[graph 5] Write-amp improvement heatmap")

    # improvement[w][vs] = SlateDB_WA / BlobDB_WA  (>1 means blob is better)
    mat = np.full((len(ALL_WORKLOADS), len(VAL_SIZES)), np.nan)
    for i, wl in enumerate(ALL_WORKLOADS):
        b = cell(df, wl, "BlobDB",  "write_amp")
        s = cell(df, wl, "SlateDB", "write_amp")
        ratio = s / b.replace(0, np.nan)
        mat[i, :] = ratio.values

    fig, ax = plt.subplots(figsize=(10, 4.5))

    # Diverging colormap centred at 1.0 (parity); cap range so a single outlier
    # cell doesn't squash the rest of the gradient.
    vmax = np.nanpercentile(mat, 95) if np.isfinite(mat).any() else 2.0
    vmin = 1.0 / vmax if vmax > 0 else 0.5
    im = ax.imshow(
        mat, aspect="auto", cmap="RdBu_r",
        vmin=vmin, vmax=vmax,
        norm=matplotlib.colors.LogNorm(vmin=max(vmin, 1e-2), vmax=max(vmax, 1.01)),
    )
    cbar = fig.colorbar(im, ax=ax, label="SlateDB WA  /  BlobDB WA   (>1 → blob wins)")
    cbar.ax.tick_params(labelsize=8)

    ax.set_xticks(range(len(VAL_SIZES)))
    ax.set_xticklabels([fmt_size(v) for v in VAL_SIZES], fontsize=9)
    ax.set_yticks(range(len(ALL_WORKLOADS)))
    ax.set_yticklabels([display_workload(w) for w in ALL_WORKLOADS], fontsize=9)
    ax.set_xlabel("Value size", fontsize=10)
    ax.set_title("Write-Amplification Improvement Factor (SlateDB ÷ BlobDB)", fontsize=12)

    for i in range(mat.shape[0]):
        for j in range(mat.shape[1]):
            v = mat[i, j]
            if np.isnan(v):
                continue
            colour = "white" if abs(np.log10(max(v, 1e-3))) > 0.4 else "black"
            ax.text(j, i, f"{v:.2f}", ha="center", va="center",
                    fontsize=8, color=colour)

    fig.tight_layout()
    _save(fig, "graph5_wa_improvement_heatmap.png", output_dir)
    plt.close(fig)


def plot_wa_bars(df, output_dir, focus_sizes=(4096, 65536, 1048576)):
    print(f"[graph 6] WA grouped bars at value sizes: "
          f"{', '.join(fmt_size(v) for v in focus_sizes)}")

    focus_sizes = [v for v in focus_sizes if v in VAL_SIZES]
    fig, axes = plt.subplots(1, len(focus_sizes), figsize=(5 * len(focus_sizes), 4.5),
                             sharey=True)
    if len(focus_sizes) == 1:
        axes = [axes]

    x = np.arange(len(ALL_WORKLOADS))
    bw = 0.4

    for ax, vs in zip(axes, focus_sizes):
        b_vals = [cell(df, wl, "BlobDB",  "write_amp").get(vs, np.nan) for wl in ALL_WORKLOADS]
        s_vals = [cell(df, wl, "SlateDB", "write_amp").get(vs, np.nan) for wl in ALL_WORKLOADS]

        bars_b = ax.bar(x - bw/2, b_vals, bw, color=PALETTE["BlobDB"],
                        label="BlobDB", zorder=3)
        bars_s = ax.bar(x + bw/2, s_vals, bw, color=PALETTE["SlateDB"],
                        label="SlateDB", zorder=3, hatch="//", edgecolor="white")

        for bars in (bars_b, bars_s):
            for bar in bars:
                h = bar.get_height()
                if np.isfinite(h) and h > 0:
                    ax.text(bar.get_x() + bar.get_width()/2,
                            h * 1.02, f"{h:.1f}",
                            ha="center", va="bottom", fontsize=7,
                            fontweight="bold")

        ax.set_title(f"Value size = {fmt_size(vs)}", fontsize=11)
        ax.set_xticks(x)
        ax.set_xticklabels([display_workload(w) for w in ALL_WORKLOADS],
                           rotation=30, ha="right", fontsize=9)
        ax.set_ylabel("Write amp (×)", fontsize=10)
        ax.legend(fontsize=9)
        ax.grid(True, which="major", axis="y", alpha=0.3)

    fig.suptitle("Write Amplification at Representative Value Sizes", fontsize=13, y=1.02)
    fig.tight_layout()
    _save(fig, "graph6_wa_bars_focus_sizes.png", output_dir)
    plt.close(fig)


# ─────────────────────────────────────────────────────────────────────────────
# Summary table
# ─────────────────────────────────────────────────────────────────────────────

def print_summary(df: pd.DataFrame):
    print("\n" + "=" * 78)
    print(f"{'Write-amp summary — BlobDB vs SlateDB (mean over value sizes)':^78}")
    print("=" * 78)
    print(f"{'Workload':<14} {'BlobDB WA':>11} {'SlateDB WA':>12} "
          f"{'Δ (×)':>10} {'Reduction':>12} {'Speedup':>10}")
    print("-" * 78)

    for wl in ALL_WORKLOADS:
        b = cell(df, wl, "BlobDB",  "write_amp")
        s = cell(df, wl, "SlateDB", "write_amp")
        if b.dropna().empty and s.dropna().empty:
            continue
        b_mean = float(b.mean(skipna=True)) if b.notna().any() else float("nan")
        s_mean = float(s.mean(skipna=True)) if s.notna().any() else float("nan")
        if np.isnan(b_mean) or np.isnan(s_mean) or b_mean == 0:
            print(f"{display_workload(wl):<14} {b_mean:>10.2f}× {s_mean:>11.2f}×"
                  f"{'  n/a':>10} {'n/a':>12} {'n/a':>10}")
            continue
        reduction = (s_mean - b_mean) / s_mean * 100
        speedup   = s_mean / b_mean
        print(f"{display_workload(wl):<14} {b_mean:>10.2f}× {s_mean:>11.2f}× "
              f"{s_mean - b_mean:>9.2f}× {reduction:>10.1f}% {speedup:>9.2f}×")

    print("-" * 78)
    b_all = df[df["system"] == "BlobDB" ]["write_amp"].mean()
    s_all = df[df["system"] == "SlateDB"]["write_amp"].mean()
    if b_all and s_all:
        print(f"{'Overall':.<14} {b_all:>10.2f}× {s_all:>11.2f}× "
              f"{s_all - b_all:>9.2f}× {(s_all - b_all) / s_all * 100:>10.1f}% "
              f"{s_all / b_all:>9.2f}×")
    print("=" * 78)


# ─────────────────────────────────────────────────────────────────────────────
# Entry point
# ─────────────────────────────────────────────────────────────────────────────

def parse_args() -> argparse.Namespace:
    ap = argparse.ArgumentParser(description=__doc__.splitlines()[1])
    ap.add_argument(
        "results", nargs="?", default=None,
        help="Path to results.jsonl (default: latest target/bencher/*/results.jsonl)",
    )
    ap.add_argument(
        "--output-dir", default="output",
        help="Directory to write plots into (default: ./output)",
    )
    return ap.parse_args()


def main():
    args = parse_args()

    results_path = Path(args.results) if args.results else find_latest_results()
    if results_path is None or not results_path.exists():
        raise SystemExit(
            "No results.jsonl found. Run run_benchmarks.py first or pass a path."
        )
    print(f"[data] Loading {results_path}")
    df = load_records(results_path)
    print(
        f"[data] {len(df)} records "
        f"({df['workload'].nunique()} workloads, "
        f"{df['val_size_bytes'].nunique()} value sizes, "
        f"systems: {sorted(df['system'].unique())})"
    )

    output_dir = Path(args.output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)

    try:
        plt.style.use("seaborn-v0_8-whitegrid")
    except OSError:
        plt.style.use("seaborn-whitegrid")

    plot_write_amp(df, output_dir)
    plot_write_throughput(df, output_dir)
    plot_read_throughput(df, output_dir)
    plot_latency(df, output_dir)
    plot_wa_heatmap(df, output_dir)
    plot_wa_bars(df, output_dir)

    print_summary(df)
    print(f"\nAll figures saved to: {output_dir.resolve()}/")


if __name__ == "__main__":
    main()
