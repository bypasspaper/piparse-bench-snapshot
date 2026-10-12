# SPDX-License-Identifier: Apache-2.0
"""Two-panel NES-INTERNAL many-source scaling figure: NES-Naive vs NES-SIMD vs NES-Native,
running the 10-query stateless PAPER_WORKLOAD as a heterogeneous mix over N concurrent
sources/queries. This is the parse/serde cost axis as a function of source count:

    Naive  = interpreted scalar-find CSV -> JSON, original (stod/stringstream) codecs (W16, ccx off)
    SIMD   = compiled SIMDCSV -> JSON, fast codecs, CCX-cell production config (the existing green curve)
    Native = SystemNative in + NATIVE out (no parse, no serialize) -- the upper bound

Left  : aggregate throughput vs N in records/s (Mrows/s), FORMAT-INVARIANT (native .nes bytes !=
        csv bytes, so we normalize MB/s by each arm's input bytes/row).
Right : p99 per-batch ingest->sink latency (ms) vs N (log-y) -- the tail story.

Throughput is derived from the stored aggregate `throughput_mbps` (total bytes / cross-query
wall-span) divided by the arm's input bytes-per-row:
    Mrows/s = throughput_mbps / bytes_per_row       (MB=1e6 convention; the per-arm ratio is
                                                      what carries the format-invariance)
    csv    bytes/row = 3595281395 / 31300000 = 114.865
    native bytes/row = 3443000000 / 31300000 = 110.000

Usage:
    .venv/bin/python scripts/plot_nes_naive_simd_native_scaling.py

Naive+Native come from `nes_naive_native_scaling_step20` (latest run_group). SIMD comes from the
existing `nes_vs_flink_vs_spark_scaling_step20` run_group (series NebulaStream, relabeled NES-SIMD).
Output: results/figures/nes_naive_simd_native_scaling.png/.pdf
"""
import statistics
import sys
from pathlib import Path

import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt

from piparse_bench.store import Store

# NES-internal arm palette: Naive muted red, SIMD green (matches prior figures), Native blue.
COLORS = {"NES-Naive": "#d62728", "NES-SIMD": "#2ca02c", "NES-Native": "#1f77b4"}
MARKERS = {"NES-Naive": "s", "NES-SIMD": "o", "NES-Native": "^"}

CSV_BYTES_PER_ROW = 3595281395 / 31300000     # 114.865
NATIVE_BYTES_PER_ROW = 3443000000 / 31300000  # 110.000
BYTES_PER_ROW = {"NES-Naive": CSV_BYTES_PER_ROW, "NES-SIMD": CSV_BYTES_PER_ROW,
                 "NES-Native": NATIVE_BYTES_PER_ROW}

# The existing SIMD scaling curve (NES arm of the 3-engine step20 sweep).
SIMD_EXP = "nes_vs_flink_vs_spark_scaling_step20"
SIMD_RUN_GROUP = "20260817T164347-3ec56b98"
SIMD_SERIES_SRC = "NebulaStream"           # its name in that run
NEW_EXP = "nes_naive_native_scaling_step20"


def latest_run_group(con, exp):
    row = con.execute(
        "SELECT run_group FROM runs WHERE experiment=? AND run_group IS NOT NULL "
        "ORDER BY timestamp DESC LIMIT 1", [exp]).fetchone()
    return row[0] if row else None


def curves(con, exp, run_group, series_filter=None, rename=None):
    """{series: {N: {tp_mbps, p50, p99, p999}}} medianed over reps."""
    q = ("SELECT series, x_value, throughput_mbps, p50_latency_us, p99_latency_us, p999_latency_us "
         "FROM runs WHERE experiment=? AND run_group=?")
    out: dict = {}
    for series, x, tp, p50, p99, p999 in con.execute(q, [exp, run_group]).fetchall():
        if series_filter and series != series_filter:
            continue
        name = rename.get(series, series) if rename else series
        n = int(float(x))
        d = out.setdefault(name, {}).setdefault(n, {"tp": [], "p50": [], "p99": [], "p999": []})
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


def main():
    s = Store()
    con = s.conn if hasattr(s, "conn") else s._conn

    data: dict = {}
    used = []

    # Naive + Native (new run, latest run_group)
    rg_new = latest_run_group(con, NEW_EXP)
    if rg_new:
        data.update(curves(con, NEW_EXP, rg_new))
        used.append(f"{NEW_EXP}@{rg_new}")

    # SIMD (existing 3-engine sweep, fixed run_group, NES arm relabeled)
    simd = curves(con, SIMD_EXP, SIMD_RUN_GROUP, series_filter=SIMD_SERIES_SRC,
                  rename={SIMD_SERIES_SRC: "NES-SIMD"})
    if simd:
        data.update(simd)
        used.append(f"{SIMD_EXP}@{SIMD_RUN_GROUP} (SIMD)")

    if not data:
        print("no rows found yet")
        return
    print(f"sources={used} series={list(data)}")

    fig, (axL, axR) = plt.subplots(1, 2, figsize=(15, 6))
    fig.suptitle(
        "NES many-source scaling — Naive vs SIMD vs Native (no parse/serde)\n"
        "10-query stateless mix, warm page cache; throughput in records/s (format-invariant)",
        fontsize=13, fontweight="bold")

    order = ["NES-Naive", "NES-SIMD", "NES-Native"]

    def series_sorted(d):
        return sorted(d, key=lambda x: order.index(x) if x in order else 99)

    for series in series_sorted(data):
        c = data[series]
        bpr = BYTES_PER_ROW.get(series, CSV_BYTES_PER_ROW)
        ns = sorted(n for n in c if c[n]["tp"] is not None)
        if ns:
            axL.plot(ns, [c[n]["tp"] / bpr for n in ns], "-", marker=MARKERS.get(series, "o"),
                     color=COLORS.get(series), lw=2.5, ms=7, label=series)
        nsp = [n for n in sorted(c) if c[n]["p99"] is not None]
        if nsp:
            axR.plot(nsp, [c[n]["p99"] / 1000 for n in nsp], "-", marker=MARKERS.get(series, "o"),
                     color=COLORS.get(series), lw=2.5, ms=7, label=series)

    axL.set_xlabel("# concurrent sources / queries (N)", fontsize=11)
    axL.set_ylabel("Aggregate throughput (Mrecords/s)", fontsize=11)
    axL.set_title("Throughput vs N (records/s, format-invariant)", fontsize=11)
    axL.legend(fontsize=10, loc="best")
    axL.grid(True, which="both", ls=":", alpha=0.4)

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
        fig.savefig(out / f"nes_naive_simd_native_scaling.{ext}", dpi=140, bbox_inches="tight")
    print("wrote", out / "nes_naive_simd_native_scaling.png")


if __name__ == "__main__":
    main()
