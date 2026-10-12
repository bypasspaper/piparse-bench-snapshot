# SPDX-License-Identifier: Apache-2.0
"""Compare FINE (steps-of-1, N=1..100) vs COARSE (step-20) resolution for the NES-Native arm,
so we can decide whether the fine grid reveals low-N structure the coarse grid misses (before
committing to the expensive full 5-arm fine sweep).

Left  : aggregate throughput (Mrecords/s = throughput_mbps / 110.0 native bytes/row) vs N.
Right : p99 per-batch latency (ms) vs N.
Fine  = line (steps of 1); Coarse = big markers (step-20 subset N<=100).

Output: results/figures/native_fine_vs_coarse_1to100.png/.pdf
"""
from pathlib import Path
import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
from piparse_bench.store import Store

NAT_BPR = 3443000000 / 31300000  # 110.0

FINE_EXP = "nes_native_scaling_lowN_fine"
COARSE_EXP = "nes_naive_native_scaling_step20"
COARSE_RG = "20260901T024303-4285bc04"


def latest_rg(con, exp):
    r = con.execute("SELECT run_group FROM runs WHERE experiment=? AND run_group IS NOT NULL "
                    "ORDER BY timestamp DESC LIMIT 1", [exp]).fetchone()
    return r[0] if r else None


def pull(con, exp, rg, series, nmax=100):
    q = ("SELECT cast(x_value as int) n, throughput_mbps, p99_latency_us FROM runs "
         "WHERE experiment=? AND run_group=? AND series=? AND cast(x_value as int)<=? ORDER BY n")
    out = {}
    for n, tp, p99 in con.execute(q, [exp, rg, series, nmax]).fetchall():
        out[n] = (tp, p99)
    return out


def main():
    s = Store()
    con = s.conn if hasattr(s, "conn") else s._conn
    fine_rg = latest_rg(con, FINE_EXP)
    fine = pull(con, FINE_EXP, fine_rg, "NES-Native")
    coarse = pull(con, COARSE_EXP, COARSE_RG, "NES-Native")
    print(f"fine={FINE_EXP}@{fine_rg} ({len(fine)} pts)  coarse={COARSE_EXP}@{COARSE_RG} ({len(coarse)} pts)")

    fig, (axL, axR) = plt.subplots(1, 2, figsize=(15, 6))
    fig.suptitle("NES-Native (no parse/serde): fine steps-of-1 vs coarse step-20, N=1..100",
                 fontsize=13, fontweight="bold")

    fn = sorted(fine)
    axL.plot(fn, [fine[n][0] / NAT_BPR for n in fn], "-", color="#1f77b4", lw=1.8, ms=3,
             marker="o", label="fine (step 1)")
    cn = sorted(coarse)
    axL.plot(cn, [coarse[n][0] / NAT_BPR for n in cn], "D", color="#d62728", ms=9,
             mfc="none", mew=2, label="coarse (step 20)")
    axL.set_xlabel("# concurrent sources / queries (N)")
    axL.set_ylabel("Aggregate throughput (Mrecords/s)")
    axL.set_title("Throughput vs N")
    axL.legend(); axL.grid(True, ls=":", alpha=0.4)

    axR.plot(fn, [fine[n][1] / 1000 for n in fn], "-", color="#1f77b4", lw=1.8, ms=3,
             marker="o", label="fine (step 1)")
    axR.plot(cn, [coarse[n][1] / 1000 for n in cn], "D", color="#d62728", ms=9,
             mfc="none", mew=2, label="coarse (step 20)")
    axR.set_xlabel("# concurrent sources / queries (N)")
    axR.set_ylabel("p99 per-batch latency (ms)")
    axR.set_title("p99 latency vs N")
    axR.legend(); axR.grid(True, ls=":", alpha=0.4)

    fig.tight_layout(rect=(0, 0, 1, 0.94))
    out = Path(__file__).resolve().parent.parent / "results" / "figures"
    out.mkdir(parents=True, exist_ok=True)
    for ext in ("png", "pdf"):
        fig.savefig(out / f"native_fine_vs_coarse_1to100.{ext}", dpi=140, bbox_inches="tight")
    print("wrote", out / "native_fine_vs_coarse_1to100.png")


if __name__ == "__main__":
    main()
