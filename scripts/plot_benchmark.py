import sys

import matplotlib

matplotlib.use("Agg")
import matplotlib.pyplot as plt  # noqa: E402
import pandas as pd  # noqa: E402

src, dst = sys.argv[1], sys.argv[2]
df = pd.read_csv(src).sort_values("seconds")
fig, ax = plt.subplots(figsize=(8, 0.6 * len(df) + 1.2))
bars = ax.barh(df["engine"], df["seconds"], color="#c2410c")
ax.bar_label(bars, fmt="%.1f s", padding=4)
ax.set_xlabel("wall-clock seconds (lower is better)")
ax.set_title("ARGUS: same aggregation, different engines")
ax.spines[["top", "right"]].set_visible(False)
fig.tight_layout()
fig.savefig(dst, dpi=150)
print(f"saved {dst}")
