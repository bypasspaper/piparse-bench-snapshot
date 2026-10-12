# SPDX-License-Identifier: Apache-2.0
"""Five-arm low-N (1..100) scaling figure on the user's 22-point log-spaced grid:
NES-Naive, NES-SIMD, NES-Native, Flink, Spark. Throughput in records/s (format-invariant),
p99 per-batch latency in ms. Both panels log-y (arms span ~0.5..280 Mrec/s and ~2 ms..80 s).

Naive/Flink/Spark come from nes_naive_flink_spark_lowN_fine (22 pts each). Native + SIMD are
subset (to the 22 grid N) from their steps-of-1 runs. All CSV arms use csv bytes/row; Native uses
native bytes/row.
"""
from pathlib import Path
import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
from piparse_bench.store import Store

GRID = [1, 2, 3, 4, 5, 6, 8, 10, 13, 16, 20, 25, 32, 40, 45, 50, 56, 63, 71, 79, 89, 100]
CSV_BPR = 3595281395 / 31300000
NAT_BPR = 3443000000 / 31300000

COLORS = {"NES-Naive": "#d62728", "NES-SIMD": "#2ca02c", "NES-Native": "#1f77b4",
          "Flink": "#7d5bbe", "Spark": "#e69f00"}
MARKERS = {"NES-Naive": "s", "NES-SIMD": "o", "NES-Native": "^", "Flink": "D", "Spark": "v"}
ORDER = ["NES-Native", "NES-SIMD", "NES-Naive", "Flink", "Spark"]

# (experiment, run_group, series, bytes_per_row)
SRC = {
    "NES-Native": ("nes_native_scaling_lowN_fine", None, "NES-Native", NAT_BPR),
    "NES-SIMD":   ("nes_simd_scaling_lowN_fine",   None, "NES-SIMD",   CSV_BPR),
    "NES-Naive":  ("nes_naive_flink_spark_lowN_fine", "20260901T175023-5d971803", "NES-Naive", CSV_BPR),
    "Flink":      ("nes_naive_flink_spark_lowN_fine", "20260901T175023-5d971803", "Flink", CSV_BPR),
    "Spark":      ("nes_naive_flink_spark_lowN_fine", "20260901T175023-5d971803", "Spark", CSV_BPR),
}


def latest_rg(con, exp):
    r = con.execute("SELECT run_group FROM runs WHERE experiment=? AND run_group IS NOT NULL "
                    "ORDER BY timestamp DESC LIMIT 1", [exp]).fetchone()
    return r[0] if r else None


def pull(con, exp, rg, series):
    if rg is None:
        rg = latest_rg(con, exp)
    q = ("SELECT cast(x_value as int) n, throughput_mbps, p99_latency_us FROM runs "
         "WHERE experiment=? AND run_group=? AND series=?")
    return {n: (tp, p99) for n, tp, p99 in con.execute(q, [exp, rg, series]).fetchall()}


def main():
    s = Store()
    con = s.conn if hasattr(s, "conn") else s._conn
    data = {arm: pull(con, e, rg, ser) for arm, (e, rg, ser, _) in SRC.items()}

    fig, (axL, axR) = plt.subplots(1, 2, figsize=(15, 6))
    fig.suptitle("Five-arm low-N scaling (N=1..100, 22-pt grid): Naive / SIMD / Native / Flink / Spark\n"
                 "10-query stateless mix, warm cache; throughput records/s (format-invariant)",
                 fontsize=13, fontweight="bold")
    for arm in ORDER:
        d = data[arm]
        bpr = SRC[arm][3]
        ns = [n for n in GRID if n in d and d[n][0] is not None]
        axL.plot(ns, [d[n][0] / bpr for n in ns], "-", marker=MARKERS[arm], color=COLORS[arm],
                 lw=2, ms=6, label=arm)
        nsp = [n for n in GRID if n in d and d[n][1] is not None]
        axR.plot(nsp, [d[n][1] / 1000 for n in nsp], "-", marker=MARKERS[arm], color=COLORS[arm],
                 lw=2, ms=6, label=arm)

    axL.set_yscale("log"); axL.set_xlabel("# concurrent sources / queries (N)")
    axL.set_ylabel("Aggregate throughput (Mrecords/s)")
    axL.set_title("Throughput vs N"); axL.legend(fontsize=9); axL.grid(True, which="both", ls=":", alpha=0.4)

    axR.set_yscale("log"); axR.set_xlabel("# concurrent sources / queries (N)")
    axR.set_ylabel("p99 per-batch latency (ms)")
    axR.set_title("p99 latency vs N (tail)"); axR.legend(fontsize=9); axR.grid(True, which="both", ls=":", alpha=0.4)

    fig.tight_layout(rect=(0, 0, 1, 0.92))
    out = Path(__file__).resolve().parent.parent / "results" / "figures"
    out.mkdir(parents=True, exist_ok=True)
    for ext in ("png", "pdf"):
        fig.savefig(out / f"five_arm_lowN_1to100.{ext}", dpi=140, bbox_inches="tight")
    print("wrote", out / "five_arm_lowN_1to100.png")
    # quick summary
    for arm in ORDER:
        d = data[arm]; bpr = SRC[arm][3]
        vals = {n: d[n][0] / bpr for n in GRID if n in d}
        pk = max(vals, key=vals.get)
        print(f"  {arm:>11}: N=1 {vals.get(1, float('nan')):7.1f}  peak {vals[pk]:7.1f} @N={pk}  N=100 {vals.get(100, float('nan')):7.1f} Mrec/s")


if __name__ == "__main__":
    main()
