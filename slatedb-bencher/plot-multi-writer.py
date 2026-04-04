#!/usr/bin/env -S uv run --script
# /// script
# requires-python = ">=3.12"
# dependencies = ["matplotlib", "pandas"]
# ///

import pandas as pd
import matplotlib.pyplot as plt
import sys

csv_path = sys.argv[1]

df = pd.read_csv(csv_path)

fig, ax = plt.subplots(figsize=(8, 5))
ax.plot(df["num_writers"], df["effective_puts_per_s"], marker="o", linewidth=2)
ax.set_xlabel("Number of Writers")
ax.set_ylabel("Throughput (puts/s)")
ax.set_title("SlateDB Multi-Writer Throughput")
ax.set_xticks(df["num_writers"])
ax.grid(True, alpha=0.3)

plt.tight_layout()
plt.savefig(csv_path.replace(".csv", ".png"), dpi=500)
plt.show()
