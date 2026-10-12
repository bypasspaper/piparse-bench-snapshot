# SPDX-License-Identifier: Apache-2.0
"""Fine (steps-of-1, N=1..100) vs coarse (step-20) resolution for a single NES arm.

Usage:
    .venv/bin/python scripts/plot_nes_fine_vs_coarse.py <series> <fine_experiment> <csv|native>

  e.g.  ... NES-SIMD   nes_simd_scaling_lowN_fine   csv
        ... NES-Native nes_native_scaling_lowN_fine native

Coarse points come from the step-20 run_group 20260901T024303-4285bc04 (Naive+Native+... ) for the
same series (N<=100). Throughput -> records/s via the arm's input bytes-per-row.
Output: results/figures/<series>_fine_vs_coarse_1to100.png/.pdf
"""
import sys
from pathlib import Path
import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
from piparse_bench.store import Store

BPR = {"csv": 3595281395 / 31300000, "native": 3443000000 / 31300000}
COARSE_RG = "20260901T024303-4285bc04"
COARSE_EXP = "nes_naive_native_scaling_step20"


def latest_rg(con, exp):
    r = con.execute("SELECT run_group FROM runs WHERE experiment=? AND run_group IS NOT NULL "
                    "ORDER BY timestamp DESC LIMIT 1", [exp]).fetchone()
    return r[0] if r else None


def pull(con, exp, rg, series, nmax=100):
    q = ("SELECT cast(x_value as int) n, throughput_mbps, p99_latency_us FROM runs "
         "WHERE experiment=? AND run_group=? AND series=? AND cast(x_value as int)<=? ORDER BY n")
    return {n: (tp, p99) for n, tp, p99 in con.execute(q, [exp, rg, series, nmax]).fetchall()}


def main():
    series = sys.argv[1] if len(sys.argv) > 1 else "NES-SIMD"
    fine_exp = sys.argv[2] if len(sys.argv) > 2 else "nes_simd_scaling_lowN_fine"
    fmt = sys.argv[3] if len(sys.argv) > 3 else "csv"
    bpr = BPR[fmt]
    color = {"NES-SIMD": "#2ca02c", "NES-Native": "#1f77b4", "NES-Naive": "#d62728"}.get(series, "#1f77b4")

    s = Store()
    con = s.conn if hasattr(s, "conn") else s._conn
    fine_rg = latest_rg(con, fine_exp)
    fine = pull(con, fine_exp, fine_rg, series)
    coarse = pull(con, COARSE_EXP, COARSE_RG, series)
    # coarse Native/Naive live in COARSE_RG; SIMD's coarse lives in the 3-engine run as "NebulaStream"
    if not coarse and series == "NES-SIMD":
        coarse = pull(con, "nes_vs_flink_vs_spark_scaling_step20", "20260817T164347-3ec56b98", "NebulaStream")
    print(f"fine={fine_exp}@{fine_rg} ({len(fine)}) coarse ({len(coarse)})")

    fig, (axL, axR) = plt.subplots(1, 2, figsize=(15, 6))
    fig.suptitle(f"{series}: fine steps-of-1 vs coarse step-20, N=1..100", fontsize=13, fontweight="bold")

    fn = sorted(fine)
    cn = sorted(coarse)
    axL.plot(fn, [fine[n][0] / bpr for n in fn], "-o", color=color, lw=1.8, ms=3, label="fine (step 1)")
    axL.plot(cn, [coarse[n][0] / bpr for n in cn], "D", color="#d62728", ms=9, mfc="none", mew=2, label="coarse (step 20)")
    axL.set_xlabel("# concurrent sources / queries (N)"); axL.set_ylabel("Aggregate throughput (Mrecords/s)")
    axL.set_title("Throughput vs N"); axL.legend(); axL.grid(True, ls=":", alpha=0.4)

    axR.plot(fn, [fine[n][1] / 1000 for n in fn], "-o", color=color, lw=1.8, ms=3, label="fine (step 1)")
    axR.plot(cn, [coarse[n][1] / 1000 for n in cn], "D", color="#d62728", ms=9, mfc="none", mew=2, label="coarse (step 20)")
    axR.set_xlabel("# concurrent sources / queries (N)"); axR.set_ylabel("p99 per-batch latency (ms)")
    axR.set_title("p99 latency vs N"); axR.legend(); axR.grid(True, ls=":", alpha=0.4)

    fig.tight_layout(rect=(0, 0, 1, 0.94))
    out = Path(__file__).resolve().parent.parent / "results" / "figures"
    out.mkdir(parents=True, exist_ok=True)
    stem = f"{series.lower().replace('-', '_')}_fine_vs_coarse_1to100"
    for ext in ("png", "pdf"):
        fig.savefig(out / f"{stem}.{ext}", dpi=140, bbox_inches="tight")
    print("wrote", out / f"{stem}.png")


if __name__ == "__main__":
    main()
