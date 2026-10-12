#!/usr/bin/env python3
"""Three-arm nexmark format matrix figures (v2 csv->csv, v2 native->csv, v3 native->native)."""
import matplotlib

matplotlib.use("Agg")
import matplotlib.pyplot as plt
import numpy as np

QUERIES = [
    "person_\nprojection", "person_\nstar", "bid_ts_\nprojection", "bid_\nfilter_eq",
    "q0_bid_\nstar", "q1_\nprice", "q2_filter_\nproj", "q5_window_\njoin", "q8_\njoin", "q8_\nvariant",
]
# GB/s of own input bytes (medians)
CSV_CSV = [10.912, 10.878, 9.057, 9.245, 9.326, 6.811, 9.316, 0.630, 0.340, 0.178]
NAT_CSV = [10.756, 10.286, 9.475, 10.263, 6.197, 6.148, 10.100, 0.641, 0.383, 0.140]
NAT_NAT = [8.002, 7.582, 10.186, 10.494, 9.597, 9.641, 10.553, 0.648, 0.386, 0.152]
# Mrows/s
MR_CSV_CSV = [49.5, 49.4, 258.7, 264.0, 266.3, 194.5, 266.0, 18.0, 3.4, 4.9]
MR_NAT_CSV = [29.2, 28.0, 394.8, 427.6, 258.2, 256.1, 420.8, 26.7, 3.2, 5.7]
MR_NAT_NAT = [21.7, 20.6, 424.4, 437.3, 399.9, 401.7, 439.7, 27.0, 3.2, 6.2]
# % of own file's dd ceiling (scan queries only; ceilings measured per run)
PC_CSV_CSV = [79.1, 78.8, 74.9, 76.4, 77.1, 56.3, 77.0]
PC_NAT_CSV = [92.0, 87.9, 77.0, 83.4, 50.4, 50.0, 82.1]
PC_NAT_NAT = [87.9, 83.3, 78.4, 80.7, 73.8, 74.2, 81.2]

COLORS = {"csv": "#b0b0b0", "natcsv": "#6baed6", "natnat": "#08519c"}
LABELS = ["csv → csv", "native → csv", "native → native"]
W = 0.27


def grouped(ax, idx, a, b, c, fmt="{:.1f}", fontsize=7):
    x = np.arange(len(idx))
    for off, vals, key in [(-W, a, "csv"), (0, b, "natcsv"), (W, c, "natnat")]:
        bars = ax.bar(x + off, vals, W, color=COLORS[key], edgecolor="white", linewidth=0.4)
        for r in bars:
            ax.annotate(fmt.format(r.get_height()), (r.get_x() + r.get_width() / 2, r.get_height()),
                        ha="center", va="bottom", fontsize=fontsize, rotation=90 if fmt == "{:.1f}" else 0)
    ax.set_xticks(x)
    ax.set_xticklabels(idx, fontsize=8)
    ax.spines[["top", "right"]].set_visible(False)


# ---------- Figure 1: throughput -------------------------------------------------
fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(12.5, 4.6), width_ratios=[7, 3])
grouped(ax1, QUERIES[:7], CSV_CSV[:7], NAT_CSV[:7], NAT_NAT[:7])
ax1.set_ylabel("throughput (GB/s of own input)")
ax1.set_ylim(0, 12.8)
ax1.set_title("scan queries", fontsize=10)
grouped(ax2, QUERIES[7:], CSV_CSV[7:], NAT_CSV[7:], NAT_NAT[7:], fmt="{:.2f}", fontsize=7)
ax2.set_ylim(0, 0.78)
ax2.set_title("stateful queries (own scale)", fontsize=10)
handles = [plt.Rectangle((0, 0), 1, 1, color=COLORS[k]) for k in ["csv", "natcsv", "natnat"]]
fig.legend(handles, LABELS, ncol=3, loc="upper center", bbox_to_anchor=(0.5, 1.01), frameon=False, fontsize=10)
fig.suptitle("Nexmark format matrix: input→output pipelines  (W16 pinned, CCX-sharded queue, 2026-07-09)",
             y=1.10, fontsize=11)
fig.text(0.01, -0.04,
         "GB/s = each arm's own input bytes/s. Caveat: person .nes rolled a slow page-cache class (9.1 GB/s dd) in the native→native run\n"
         "vs 11.7 in native→csv — person raw GB/s is not comparable across arms; see the %-of-ceiling figure. bid files ~12.1–13.0 GB/s all runs.",
         fontsize=7.5, va="top")
fig.tight_layout()
fig.savefig("figures/nexmark_matrix_throughput.png", dpi=180, bbox_inches="tight")

# ---------- Figure 2: % of ceiling + Mrows/s -------------------------------------
fig2, (axp, axm) = plt.subplots(2, 1, figsize=(12.5, 8.2))
grouped(axp, QUERIES[:7], PC_CSV_CSV, PC_NAT_CSV, PC_NAT_NAT, fmt="{:.0f}")
axp.axhline(100, color="black", linewidth=0.8, linestyle="--")
axp.text(6.55, 101.5, "own-file memcpy ceiling (dd)", fontsize=8, ha="right")
axp.set_ylabel("% of own file's dd ceiling")
axp.set_ylim(0, 112)
axp.set_title("per-byte engine efficiency, normalized per file (scan queries)", fontsize=10)

x = np.arange(len(QUERIES))
for off, vals, key in [(-W, MR_CSV_CSV, "csv"), (0, MR_NAT_CSV, "natcsv"), (W, MR_NAT_NAT, "natnat")]:
    axm.bar(x + off, vals, W, color=COLORS[key], edgecolor="white", linewidth=0.4)
for i, (c, n) in enumerate(zip(MR_CSV_CSV, MR_NAT_NAT)):
    axm.annotate(f"×{n / c:.2f}", (i + W, n * 1.15), ha="center", fontsize=8.5,
                 fontweight="bold", color="#08519c")
axm.set_yscale("log")
axm.set_ylim(1, 900)
axm.set_xticks(x)
axm.set_xticklabels(QUERIES, fontsize=8)
axm.set_ylabel("Mrows/s (log)")
axm.spines[["top", "right"]].set_visible(False)
axm.set_title("outcome: rows/s (log scale) — label = native→native vs csv→csv ratio", fontsize=10)

fig2.legend(handles, LABELS, ncol=3, loc="upper center", bbox_to_anchor=(0.5, 1.005), frameon=False, fontsize=10)
fig2.text(0.01, -0.02,
          "Top: GB/s normalized by each arm's own-file dd ceiling — removes the page-cache-class lottery; ~80% = supply-bound (io copy wall).\n"
          "Bottom: Mrows/s = format-invariant outcome; the ratio folds per-byte efficiency × byte footprint (bid .nes 0.69× csv, person .nes 1.67× csv).",
          fontsize=7.5, va="top")
fig2.tight_layout(rect=[0, 0, 1, 0.96])
fig2.savefig("figures/nexmark_matrix_efficiency.png", dpi=180, bbox_inches="tight")
print("written")
