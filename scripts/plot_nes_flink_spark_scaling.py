# SPDX-License-Identifier: Apache-2.0
"""Two-panel many-source SCALING figure: NebulaStream vs Apache Flink vs Apache Spark, running the
10-query stateless PAPER_WORKLOAD as a heterogeneous mix, csv -> json, over N concurrent
sources/queries.

Left  : aggregate throughput (GB/s) vs N (log x) — does the engine sustain throughput as N grows?
Right : p99 per-batch ingest->sink latency (ms) vs N (log-log) — the tail story.

Data is pulled live from the pbench DuckDB, scoped to the LATEST run_group of the chosen
experiment (so a re-run's headline is not contaminated by stale rows). Usage:

    .venv/bin/python scripts/plot_nes_flink_spark_scaling.py [experiment_name]

default experiment = nes_vs_flink_vs_spark_scaling (falls back to the _probe experiment if the
full sweep has no rows yet). Output: results/figures/<experiment>.png/.pdf
"""
import statistics
import sys
from pathlib import Path

import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt

from piparse_bench.store import Store

# NES green (matches the prior scaling figure), Flink purple, Spark Okabe-Ito orange.
COLORS = {"NebulaStream": "#2ca02c", "Flink": "#7d5bbe", "Spark": "#e69f00"}
MARKERS = {"NebulaStream": "o", "Flink": "s", "Spark": "^"}


def latest_run_group(con, exp):
    row = con.execute(
        "SELECT run_group FROM runs WHERE experiment=? AND run_group IS NOT NULL "
        "ORDER BY timestamp DESC LIMIT 1", [exp]).fetchone()
    return row[0] if row else None


def curves(con, exp, run_group):
    """{series: {N: {tp, p50, p99, p999}}} medianed over reps, for the given run_group."""
    q = ("SELECT series, x_value, throughput_mbps, p50_latency_us, p99_latency_us, p999_latency_us "
         "FROM runs WHERE experiment=? AND run_group=?")
    out: dict = {}
    for series, x, tp, p50, p99, p999 in con.execute(q, [exp, run_group]).fetchall():
        n = int(float(x))
        d = out.setdefault(series, {}).setdefault(n, {"tp": [], "p50": [], "p99": [], "p999": []})
        if tp is not None:
            d["tp"].append(tp)
        if p50 is not None:
            d["p50"].append(p50)
        if p99 is not None:
            d["p99"].append(p99)
        if p999 is not None:
            d["p999"].append(p999)
    med: dict = {}
    for series, byn in out.items():
        med[series] = {n: {k: (statistics.median(v) if v else None) for k, v in d.items()}
                       for n, d in byn.items()}
    return med


def merge(dst, src):
    """Merge src curves into dst (later N points win; Ns don't overlap across probe experiments)."""
    for series, byn in src.items():
        d = dst.setdefault(series, {})
        d.update(byn)
    return dst


def main():
    exps = sys.argv[1:] if len(sys.argv) > 1 else ["nes_vs_flink_vs_spark_scaling"]
    s = Store()
    con = s.conn if hasattr(s, "conn") else s._conn
    data: dict = {}
    used = []
    for exp in exps:
        rg = latest_run_group(con, exp)
        if rg is None:
            continue
        merge(data, curves(con, exp, rg))
        used.append(f"{exp}@{rg}")
    if not data:
        print("no rows found for the scaling experiment(s) yet")
        return
    exp = exps[0]  # output filename stem
    print(f"experiments={used} series={list(data)}")

    fig, (axL, axR) = plt.subplots(1, 2, figsize=(15, 6))
    fig.suptitle(
        "Many-source scaling: NebulaStream vs Apache Flink vs Apache Spark\n"
        "10-query stateless mix, csv -> json, warm page cache — per-batch ingest->sink latency",
        fontsize=13, fontweight="bold")

    def series_sorted(data):
        order = ["NebulaStream", "Flink", "Spark"]
        return sorted(data, key=lambda x: order.index(x) if x in order else 99)

    for series in series_sorted(data):
        c = data[series]
        ns = sorted(n for n in c if c[n]["tp"] is not None)
        if not ns:
            continue
        color = COLORS.get(series, None)
        mk = MARKERS.get(series, "o")
        axL.plot(ns, [c[n]["tp"] / 1000 for n in ns], "-", marker=mk, color=color, lw=2.5, ms=7,
                 label=series)
        nsp = [n for n in ns if c[n]["p99"] is not None]
        if nsp:
            axR.plot(nsp, [c[n]["p99"] / 1000 for n in nsp], "-", marker=mk, color=color, lw=2.5,
                     ms=7, label=series)

    axL.set_xscale("log", base=2)
    axL.set_xlabel("# concurrent sources / queries (N)", fontsize=11)
    axL.set_ylabel("Aggregate throughput (GB/s, csv input bytes)", fontsize=11)
    axL.set_title("Throughput vs N", fontsize=11)
    axL.legend(fontsize=10, loc="best")
    axL.grid(True, which="both", ls=":", alpha=0.4)

    axR.set_xscale("log", base=2)
    axR.set_yscale("log")
    axR.set_xlabel("# concurrent sources / queries (N)", fontsize=11)
    axR.set_ylabel("p99 per-batch latency (ms)", fontsize=11)
    axR.set_title("p99 latency vs N (tail)", fontsize=11)
    axR.legend(fontsize=10, loc="best")
    axR.grid(True, which="both", ls=":", alpha=0.4)

    fig.tight_layout(rect=(0, 0, 1, 0.93))
    out = Path(__file__).resolve().parent.parent / "results" / "figures"
    out.mkdir(parents=True, exist_ok=True)
    for ext in ("png", "pdf"):
        fig.savefig(out / f"{exp}.{ext}", dpi=140, bbox_inches="tight")
    print("wrote", out / f"{exp}.png")


if __name__ == "__main__":
    main()
