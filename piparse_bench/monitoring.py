"""Parse MonitoringSink CSV output and aggregate across concurrent queries."""

from __future__ import annotations

import csv
from dataclasses import dataclass
from pathlib import Path
from statistics import median


@dataclass
class MonitoringResult:
    """Parsed result from a single MonitoringSink CSV file.

    Latencies are in MICROSECONDS (the sink stamps source read-start and sink arrival in us;
    the old `_ms` naming was a mislabel — the value was always us). first_ts_us / last_ts_us
    are ABSOLUTE epoch microseconds (read-start of the first buffer, arrival of the last),
    used to recover the true cross-query system wall-span.
    """
    processing_time_s: float
    throughput_mbps: float
    avg_latency_us: float
    min_latency_us: float
    max_latency_us: float
    p50_latency_us: float
    p99_latency_us: float
    p999_latency_us: float
    first_ts_us: float
    last_ts_us: float


@dataclass
class CodecMonitoringResult:
    """Parsed result from a single CodecMonitoringSink CSV file."""
    main_ratio_min: float
    main_ratio_max: float
    main_ratio_avg: float
    main_rate_min: float
    main_rate_max: float
    main_rate_avg: float
    child_ratio_min: float
    child_ratio_max: float
    child_ratio_avg: float
    child_rate_min: float
    child_rate_max: float
    child_rate_avg: float


def parse_monitoring_csv(path: Path) -> MonitoringResult:
    """Parse a MonitoringSink CSV file.

    The MonitoringSink writes a CSV with columns (headers normalized to snake_case here):
        processing time in s, throughput in MB/s,
        average latency us, min latency us, max latency us,
        p50 latency us, p99 latency us, p999 latency us,
        first read start us, last arrival us

    There is typically one row (the final summary), but if multiple rows exist
    we take the last one (the final measurement).
    """
    rows = []
    with open(path) as f:
        reader = csv.reader(f)
        header = next(reader, None)
        if header is None:
            raise ValueError(f"Empty MonitoringSink CSV: {path}")

        # Normalize header names
        header = [h.strip().lower().replace(" ", "_") for h in header]

        for row in reader:
            if not row or not row[0].strip():
                continue
            rows.append(row)

    if not rows:
        raise ValueError(f"No data rows in MonitoringSink CSV: {path}")

    # Take the last row
    last = rows[-1]
    values = [float(v.strip()) for v in last]

    # Map by header position
    def _get(name: str, alternatives: list[str] | None = None) -> float:
        names = [name] + (alternatives or [])
        for n in names:
            if n in header:
                return values[header.index(n)]
        # Fall back to positional
        return 0.0

    return MonitoringResult(
        processing_time_s=_get("processing_time_in_s", ["processing_time_s", "processing_time", "time"]),
        throughput_mbps=_get("throughput_in_mb/s", ["throughput_mb_s", "throughput_mbps", "throughput"]),
        # The CSV header is "average latency us" -> normalized "average_latency_us". The old code
        # looked for "average_latency" and silently fell back to 0.0 (the latency-drop bug).
        avg_latency_us=_get("average_latency_us", ["average_latency", "avg_latency_ms", "avg_latency"]),
        min_latency_us=_get("min_latency_us", ["min_latency"]),
        max_latency_us=_get("max_latency_us", ["max_latency"]),
        p50_latency_us=_get("p50_latency_us"),
        p99_latency_us=_get("p99_latency_us"),
        p999_latency_us=_get("p999_latency_us"),
        first_ts_us=_get("first_read_start_us"),
        last_ts_us=_get("last_arrival_us"),
    )


def _detect_csv_type(path: Path) -> str:
    """Detect whether a CSV is from MonitoringSink or CodecMonitoringSink."""
    with open(path) as f:
        header = f.readline().strip().lower()
    if "main ratio" in header or "main_ratio" in header:
        return "codec"
    return "monitoring"


def parse_codec_monitoring_csv(path: Path) -> CodecMonitoringResult:
    """Parse a CodecMonitoringSink CSV file.

    Columns: main ratio min, main ratio max, main ratio avg,
             main rate min, main rate max, main rate avg,
             child ratio min, child ratio max, child ratio avg,
             child rate min, child rate max, child rate avg
    """
    rows = []
    with open(path) as f:
        reader = csv.reader(f)
        header = next(reader, None)
        if header is None:
            raise ValueError(f"Empty CodecMonitoringSink CSV: {path}")

        header = [h.strip().lower().replace(" ", "_") for h in header]

        for row in reader:
            if not row or not row[0].strip():
                continue
            rows.append(row)

    if not rows:
        raise ValueError(f"No data rows in CodecMonitoringSink CSV: {path}")

    last = rows[-1]
    values = [float(v.strip()) for v in last]

    def _get(name: str) -> float:
        if name in header:
            return values[header.index(name)]
        return 0.0

    return CodecMonitoringResult(
        main_ratio_min=_get("main_ratio_min"),
        main_ratio_max=_get("main_ratio_max"),
        main_ratio_avg=_get("main_ratio_avg"),
        main_rate_min=_get("main_rate_min"),
        main_rate_max=_get("main_rate_max"),
        main_rate_avg=_get("main_rate_avg"),
        child_ratio_min=_get("child_ratio_min"),
        child_ratio_max=_get("child_ratio_max"),
        child_ratio_avg=_get("child_ratio_avg"),
        child_rate_min=_get("child_rate_min"),
        child_rate_max=_get("child_rate_max"),
        child_rate_avg=_get("child_rate_avg"),
    )


def aggregate_codec_monitoring_results(files: list[Path]) -> dict[str, float | int]:
    """Aggregate codec monitoring results across files.

    For single-file case (typical), returns values directly.
    For multiple files, takes medians of each metric.
    """
    per_query = [parse_codec_monitoring_csv(f) for f in files]
    if not per_query:
        return {}

    if len(per_query) == 1:
        r = per_query[0]
    else:
        # Median across queries for each field
        from dataclasses import fields
        medians = {}
        for field in fields(CodecMonitoringResult):
            vals = [getattr(q, field.name) for q in per_query]
            medians[field.name] = median(vals)
        r = CodecMonitoringResult(**medians)

    return {
        "codec_main_ratio_min": r.main_ratio_min,
        "codec_main_ratio_max": r.main_ratio_max,
        "codec_main_ratio_avg": r.main_ratio_avg,
        "codec_main_rate_min": r.main_rate_min,
        "codec_main_rate_max": r.main_rate_max,
        "codec_main_rate_avg": r.main_rate_avg,
        "codec_child_ratio_min": r.child_ratio_min,
        "codec_child_ratio_max": r.child_ratio_max,
        "codec_child_ratio_avg": r.child_ratio_avg,
        "codec_child_rate_min": r.child_rate_min,
        "codec_child_rate_max": r.child_rate_max,
        "codec_child_rate_avg": r.child_rate_avg,
        "num_queries": len(files),
    }


def aggregate_monitoring_results(files: list[Path]) -> dict[str, float | int]:
    """Aggregate across N concurrent query MonitoringSink outputs.

    Returns a metrics dict compatible with RunResult fields.
    """
    per_query = [parse_monitoring_csv(f) for f in files]

    if not per_query:
        return {}

    throughputs = [q.throughput_mbps for q in per_query]

    # DEFENSIBLE aggregate throughput = total bytes / true cross-query wall-span.
    # Each query reports MB/s and its own window (processing_time_s) -> per-query MB =
    # rate * window. The wall-span is max(last arrival) - min(first read start) ACROSS all
    # queries (absolute epoch us). Summing per-query rates over STAGGERED windows overcounts
    # (it produced 115 GB/s > the box's ~84 GB/s DRAM peak) -- so that sum is kept only as
    # `throughput_sum_mbps` for transparency, and is NOT the primary metric.
    total_mb = sum(q.throughput_mbps * q.processing_time_s for q in per_query)
    first = min(q.first_ts_us for q in per_query)
    last = max(q.last_ts_us for q in per_query)
    wall_span_s = (last - first) / 1_000_000.0
    if wall_span_s > 0 and total_mb > 0:
        agg_throughput_mbps = total_mb / wall_span_s
    else:
        # No absolute timestamps (e.g. pre-percentile sink) -> fall back to the old sum.
        agg_throughput_mbps = sum(throughputs)

    # Latency percentiles are per-query (the sink emits each query's own p50/p99/p999 over its
    # retained samples). We cannot pool raw samples across queries, so we report the MEDIAN
    # across queries of each percentile, plus the WORST (max) tail. All microseconds.
    def _med(attr: str) -> float:
        return median(getattr(q, attr) for q in per_query)

    return {
        # Primary: defensible system throughput (wall-span based).
        "throughput_mbps": agg_throughput_mbps,
        # Reference only: the old (overcounting) sum of per-query rates.
        "throughput_sum_mbps": sum(throughputs),
        # Per-query statistics
        "throughput_per_query_median": median(throughputs),
        "throughput_per_query_min": min(throughputs),
        "throughput_per_query_max": max(throughputs),
        # Latency (microseconds): median-across-queries of avg/p50/p99/p999, plus worst tail.
        "avg_latency_us": _med("avg_latency_us"),
        "min_latency_us": min(q.min_latency_us for q in per_query),
        "max_latency_us": max(q.max_latency_us for q in per_query),
        "p50_latency_us": _med("p50_latency_us"),
        "p99_latency_us": _med("p99_latency_us"),
        "p999_latency_us": _med("p999_latency_us"),
        "p99_latency_us_worst": max(q.p99_latency_us for q in per_query),
        # Processing time (longest query determines overall time)
        "processing_time_s": max(q.processing_time_s for q in per_query),
        "num_queries": len(files),
    }
