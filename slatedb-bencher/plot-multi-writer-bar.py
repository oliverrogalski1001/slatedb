#!/usr/bin/env -S uv run --script
# /// script
# requires-python = ">=3.12"
# dependencies = ["matplotlib", "pandas", "numpy"]
# ///

import pandas as pd
import matplotlib.pyplot as plt
import numpy as np
import sys

csv_path = sys.argv[1]

df = pd.read_csv(csv_path)

writers = sorted(df["num_writers"].unique())
intervals = sorted(df["flush_interval_ms"].unique())

x = np.arange(len(writers))
n_bars = len(intervals)
width = 0.8 / n_bars

fig, ax = plt.subplots(figsize=(12, 6))

grouped = df.groupby(["num_writers", "flush_interval_ms"])["effective_puts_per_s"]
means = grouped.mean().unstack(fill_value=0)
sems = grouped.sem().unstack(fill_value=0)

for i, interval in enumerate(intervals):
    values = [means.loc[w, interval] if w in means.index else 0 for w in writers]
    errors = [sems.loc[w, interval] if w in sems.index else 0 for w in writers]
    offset = (i - n_bars / 2 + 0.5) * width
    ax.bar(x + offset, values, width, yerr=errors, capsize=3, label=f"{interval}ms")

ax.set_xlabel("Number of Writers")
ax.set_ylabel("Throughput (puts/s)")
ax.set_title("SlateDB Multi-Writer Throughput by Writer Count and Flush Interval")
ax.set_xticks(x)
ax.set_xticklabels(writers)
ax.legend(title="Flush Interval", bbox_to_anchor=(1.02, 1), loc="upper left")
ax.set_yscale("log")
ax.grid(True, alpha=0.3, axis="y")

plt.tight_layout()
plt.savefig(csv_path.replace(".csv", "-bar.png"), dpi=500)
plt.show()
