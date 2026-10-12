"""DuckDB storage: create schema, insert RunResults, aggregated view, CSV export."""

from __future__ import annotations

import json
import math
from pathlib import Path
from typing import Any

import duckdb

from .models import QueryAggregation, RunResult

RESULTS_DIR = Path(__file__).parent.parent / "results"
DB_PATH = RESULTS_DIR / "piparse_bench.duckdb"

_CREATE_TABLE = """
CREATE TABLE IF NOT EXISTS runs (
    run_id              VARCHAR PRIMARY KEY,
    timestamp           TIMESTAMP,
    experiment          VARCHAR NOT NULL,
    variant             VARCHAR NOT NULL,
    series              VARCHAR NOT NULL,
    repetition          INTEGER NOT NULL,
    query_ref           VARCHAR NOT NULL,

    system              VARCHAR NOT NULL,
    branch              VARCHAR,
    mode                VARCHAR,
    build               VARCHAR,
    git_commit          VARCHAR,
    hostname            VARCHAR,

    x_param             VARCHAR NOT NULL,
    x_value             VARCHAR NOT NULL,

    config              JSON NOT NULL,

    worker_threads      INTEGER,
    io_threads          INTEGER,
    operator_buffer_size INTEGER,
    execution_mode      VARCHAR,
    input_formatter     VARCHAR,
    output_formatter    VARCHAR,
    invoke_mode         VARCHAR,
    source_type         VARCHAR,
    sink_type           VARCHAR,
    num_queries         INTEGER,

    throughput_mbps             DOUBLE,
    throughput_sum_mbps         DOUBLE,
    throughput_per_query_median DOUBLE,
    throughput_per_query_min    DOUBLE,
    throughput_per_query_max    DOUBLE,
    processing_time_s           DOUBLE,
    input_rows                  BIGINT,
    throughput_mrows_s          DOUBLE,
    avg_latency_us              DOUBLE,
    min_latency_us              DOUBLE,
    max_latency_us              DOUBLE,
    p50_latency_us              DOUBLE,
    p99_latency_us              DOUBLE,
    p999_latency_us             DOUBLE,
    p99_latency_us_worst        DOUBLE,

    codec_main_ratio_min        DOUBLE,
    codec_main_ratio_max        DOUBLE,
    codec_main_ratio_avg        DOUBLE,
    codec_main_rate_min         DOUBLE,
    codec_main_rate_max         DOUBLE,
    codec_main_rate_avg         DOUBLE,
    codec_child_ratio_min       DOUBLE,
    codec_child_ratio_max       DOUBLE,
    codec_child_ratio_avg       DOUBLE,
    codec_child_rate_min        DOUBLE,
    codec_child_rate_max        DOUBLE,
    codec_child_rate_avg        DOUBLE,

    run_group           VARCHAR
);
"""

_CREATE_VIEW = """
-- The aggregated view scopes each experiment to its LATEST `pbench run` invocation
-- (the run_group of its most recent row), so a re-run's headline metric reflects ONLY
-- the fresh reps -- never a median contaminated by stale historical rows (which is what
-- silently produced the "engine_tcp_passthrough_smoke -> 2138 MB/s (n=9)" artifact: a
-- median over 6 old pre-optimization rows + 3 fresh ~5.2 GB/s rows). Full history stays
-- in `runs` for manual cross-run queries. NULL run_group (legacy pre-migration rows) is
-- COALESCEd to a single sentinel group, so old DBs keep their previous behavior until a
-- new stamped run lands for that experiment.
CREATE OR REPLACE VIEW aggregated AS
WITH latest_group AS (
    SELECT experiment, arg_max(COALESCE(run_group, '__legacy__'), timestamp) AS rg
    FROM runs GROUP BY experiment
)
SELECT
    r.experiment, r.variant, r.series, r.x_param, r.x_value, r.query_ref,
    COUNT(*) as num_repetitions,
    MEDIAN(throughput_mbps)    AS throughput_mbps,
    MEDIAN(processing_time_s)  AS processing_time_s,
    MEDIAN(throughput_mrows_s) AS throughput_mrows_s,
    MAX(input_rows)            AS input_rows,
    MEDIAN(throughput_sum_mbps) AS throughput_sum_mbps,
    MEDIAN(avg_latency_us)     AS avg_latency_us,
    MIN(min_latency_us)        AS min_latency_us,
    MAX(max_latency_us)        AS max_latency_us,
    MEDIAN(p50_latency_us)     AS p50_latency_us,
    MEDIAN(p99_latency_us)     AS p99_latency_us,
    MEDIAN(p999_latency_us)    AS p999_latency_us,
    MAX(p99_latency_us_worst)  AS p99_latency_us_worst,
    MIN(codec_main_ratio_min)  AS codec_main_ratio_min,
    MAX(codec_main_ratio_max)  AS codec_main_ratio_max,
    MEDIAN(codec_main_ratio_avg) AS codec_main_ratio_avg,
    MIN(codec_main_rate_min)   AS codec_main_rate_min,
    MAX(codec_main_rate_max)   AS codec_main_rate_max,
    MEDIAN(codec_main_rate_avg)  AS codec_main_rate_avg,
    MIN(codec_child_ratio_min) AS codec_child_ratio_min,
    MAX(codec_child_ratio_max) AS codec_child_ratio_max,
    MEDIAN(codec_child_ratio_avg) AS codec_child_ratio_avg,
    MIN(codec_child_rate_min)  AS codec_child_rate_min,
    MAX(codec_child_rate_max)  AS codec_child_rate_max,
    MEDIAN(codec_child_rate_avg) AS codec_child_rate_avg
FROM runs r
JOIN latest_group lg
    ON r.experiment = lg.experiment
   AND COALESCE(r.run_group, '__legacy__') = lg.rg
GROUP BY r.experiment, r.variant, r.series, r.x_param, r.x_value, r.query_ref;
"""

# Column-explicit INSERT: robust to physical column order (existing DBs get new columns
# appended at the end via ALTER, fresh DBs get them mid-table from CREATE TABLE -- naming the
# columns means either order inserts correctly). Omitted columns (e.g. legacy *_latency_ms on
# already-migrated DBs) are left NULL.
_INSERT = """
INSERT OR REPLACE INTO runs (
    run_id, timestamp, experiment, variant, series, repetition, query_ref,
    system, branch, mode, build, git_commit, hostname,
    x_param, x_value, config,
    worker_threads, io_threads, operator_buffer_size, execution_mode,
    input_formatter, output_formatter, invoke_mode, source_type, sink_type,
    num_queries,
    throughput_mbps, throughput_sum_mbps, throughput_per_query_median, throughput_per_query_min,
    throughput_per_query_max, processing_time_s, input_rows, throughput_mrows_s,
    avg_latency_us, min_latency_us, max_latency_us,
    p50_latency_us, p99_latency_us, p999_latency_us, p99_latency_us_worst,
    codec_main_ratio_min, codec_main_ratio_max, codec_main_ratio_avg,
    codec_main_rate_min, codec_main_rate_max, codec_main_rate_avg,
    codec_child_ratio_min, codec_child_ratio_max, codec_child_ratio_avg,
    codec_child_rate_min, codec_child_rate_max, codec_child_rate_avg,
    run_group
) VALUES (
    $run_id, $timestamp, $experiment, $variant, $series, $repetition, $query_ref,
    $system, $branch, $mode, $build, $git_commit, $hostname,
    $x_param, $x_value, $config,
    $worker_threads, $io_threads, $operator_buffer_size, $execution_mode,
    $input_formatter, $output_formatter, $invoke_mode, $source_type, $sink_type,
    $num_queries,
    $throughput_mbps, $throughput_sum_mbps, $throughput_per_query_median, $throughput_per_query_min,
    $throughput_per_query_max, $processing_time_s, $input_rows, $throughput_mrows_s,
    $avg_latency_us, $min_latency_us, $max_latency_us,
    $p50_latency_us, $p99_latency_us, $p999_latency_us, $p99_latency_us_worst,
    $codec_main_ratio_min, $codec_main_ratio_max, $codec_main_ratio_avg,
    $codec_main_rate_min, $codec_main_rate_max, $codec_main_rate_avg,
    $codec_child_ratio_min, $codec_child_ratio_max, $codec_child_ratio_avg,
    $codec_child_rate_min, $codec_child_rate_max, $codec_child_rate_avg,
    $run_group
);
"""


def geometric_mean(values: list[float]) -> float:
    """Compute geometric mean of positive values."""
    if not values or any(v <= 0 for v in values):
        return 0.0
    return math.exp(sum(math.log(v) for v in values) / len(values))


class Store:
    def __init__(self, db_path: str | Path | None = None):
        self.db_path = Path(db_path) if db_path else DB_PATH
        self.db_path.parent.mkdir(parents=True, exist_ok=True)
        self.conn = duckdb.connect(str(self.db_path))
        self._init_schema()

    def _init_schema(self) -> None:
        self.conn.execute(_CREATE_TABLE)
        # Migrate: add codec monitoring columns if missing (existing DBs)
        _CODEC_COLUMNS = [
            "codec_main_ratio_min", "codec_main_ratio_max", "codec_main_ratio_avg",
            "codec_main_rate_min", "codec_main_rate_max", "codec_main_rate_avg",
            "codec_child_ratio_min", "codec_child_ratio_max", "codec_child_ratio_avg",
            "codec_child_rate_min", "codec_child_rate_max", "codec_child_rate_avg",
        ]
        # Migrate: latency-in-us + percentiles + defensible-aggregate columns (existing DBs).
        # The legacy avg/min/max_latency_ms columns stay physically present but are no longer
        # written (the value was always us; these are superseded by the _us columns).
        _LATENCY_COLUMNS = [
            "throughput_sum_mbps",
            "avg_latency_us", "min_latency_us", "max_latency_us",
            "p50_latency_us", "p99_latency_us", "p999_latency_us", "p99_latency_us_worst",
        ]
        existing = {
            row[0] for row in
            self.conn.execute("SELECT column_name FROM information_schema.columns WHERE table_name = 'runs'").fetchall()
        }
        for col in _CODEC_COLUMNS + _LATENCY_COLUMNS:
            if col not in existing:
                self.conn.execute(f"ALTER TABLE runs ADD COLUMN {col} DOUBLE")
        # Migrate: run_group (per-invocation id; scopes the aggregated view to latest run)
        if "run_group" not in existing:
            self.conn.execute("ALTER TABLE runs ADD COLUMN run_group VARCHAR")
        # Migrate: format-invariant rows metric (catalog DataEntry.rows x repeat / window).
        # Old rows keep NULL — derivable offline from catalog rows + processing_time_s.
        if "input_rows" not in existing:
            self.conn.execute("ALTER TABLE runs ADD COLUMN input_rows BIGINT")
        if "throughput_mrows_s" not in existing:
            self.conn.execute("ALTER TABLE runs ADD COLUMN throughput_mrows_s DOUBLE")
        self.conn.execute(_CREATE_VIEW)

    def insert(self, result: RunResult) -> None:
        """Insert a single RunResult."""
        self.conn.execute(_INSERT, {
            "run_id": result.run_id,
            "timestamp": result.timestamp,
            "experiment": result.experiment,
            "variant": result.variant,
            "series": result.series,
            "repetition": result.repetition,
            "query_ref": result.query_ref,
            "system": result.system,
            "branch": result.branch,
            "mode": result.mode,
            "build": result.build,
            "git_commit": result.git_commit,
            "hostname": result.hostname,
            "x_param": result.x_param,
            "x_value": result.x_value,
            "config": json.dumps(result.config),
            "worker_threads": result.worker_threads,
            "io_threads": result.io_threads,
            "operator_buffer_size": result.operator_buffer_size,
            "execution_mode": result.execution_mode,
            "input_formatter": result.input_formatter,
            "output_formatter": result.output_formatter,
            "invoke_mode": result.invoke_mode,
            "source_type": result.source_type,
            "sink_type": result.sink_type,
            "num_queries": result.num_queries,
            "throughput_mbps": result.throughput_mbps,
            "throughput_sum_mbps": result.throughput_sum_mbps,
            "throughput_per_query_median": result.throughput_per_query_median,
            "throughput_per_query_min": result.throughput_per_query_min,
            "throughput_per_query_max": result.throughput_per_query_max,
            "processing_time_s": result.processing_time_s,
            "input_rows": result.input_rows,
            "throughput_mrows_s": result.throughput_mrows_s,
            "avg_latency_us": result.avg_latency_us,
            "min_latency_us": result.min_latency_us,
            "max_latency_us": result.max_latency_us,
            "p50_latency_us": result.p50_latency_us,
            "p99_latency_us": result.p99_latency_us,
            "p999_latency_us": result.p999_latency_us,
            "p99_latency_us_worst": result.p99_latency_us_worst,
            "codec_main_ratio_min": result.codec_main_ratio_min,
            "codec_main_ratio_max": result.codec_main_ratio_max,
            "codec_main_ratio_avg": result.codec_main_ratio_avg,
            "codec_main_rate_min": result.codec_main_rate_min,
            "codec_main_rate_max": result.codec_main_rate_max,
            "codec_main_rate_avg": result.codec_main_rate_avg,
            "codec_child_ratio_min": result.codec_child_ratio_min,
            "codec_child_ratio_max": result.codec_child_ratio_max,
            "codec_child_ratio_avg": result.codec_child_ratio_avg,
            "codec_child_rate_min": result.codec_child_rate_min,
            "codec_child_rate_max": result.codec_child_rate_max,
            "codec_child_rate_avg": result.codec_child_rate_avg,
            "run_group": result.run_group,
        })

    def export_csv(self, experiment: str, output_dir: str | Path | None = None) -> Path:
        """Export aggregated results for an experiment to CSV."""
        out_dir = Path(output_dir) if output_dir else RESULTS_DIR / "csv"
        out_dir.mkdir(parents=True, exist_ok=True)
        csv_path = out_dir / f"{experiment}.csv"

        self.conn.execute(
            f"COPY (SELECT * FROM aggregated WHERE experiment = $1 ORDER BY variant, series, x_value) "
            f"TO '{csv_path}' (HEADER, DELIMITER ',')",
            [experiment],
        )
        return csv_path

    def get_aggregated(self, experiment: str) -> list[dict]:
        """Get per-query aggregated results as list of dicts."""
        result = self.conn.execute(
            "SELECT * FROM aggregated WHERE experiment = $1 ORDER BY variant, series, x_value, query_ref",
            [experiment],
        )
        columns = [desc[0] for desc in result.description]
        return [dict(zip(columns, row)) for row in result.fetchall()]

    def get_aggregated_multi_query(
        self,
        experiment: str,
        metric: str = "throughput_mbps",
        aggregation: QueryAggregation = QueryAggregation.GEOMETRIC_MEAN,
    ) -> list[dict]:
        """Get results aggregated across queries (for multi-query variants).

        First aggregates repetitions per query (median), then aggregates across
        queries using the specified method (geometric or arithmetic mean).
        """
        per_query = self.get_aggregated(experiment)
        if not per_query:
            return []

        # Group by (variant, series, x_param, x_value)
        from collections import defaultdict
        groups: dict[tuple, list[dict]] = defaultdict(list)
        for row in per_query:
            key = (row["variant"], row["series"], row["x_param"], row["x_value"])
            groups[key].append(row)

        results = []
        for (variant, series, x_param, x_value), rows in groups.items():
            values = [r[metric] for r in rows if r.get(metric) is not None]
            if not values:
                continue

            if aggregation == QueryAggregation.GEOMETRIC_MEAN:
                agg_value = geometric_mean(values)
            else:
                agg_value = sum(values) / len(values)

            results.append({
                "variant": variant,
                "series": series,
                "x_param": x_param,
                "x_value": x_value,
                "num_queries": len(rows),
                "query_refs": [r["query_ref"] for r in rows],
                metric: agg_value,
            })

        return results

    def get_results_summary(self, experiment: str) -> str:
        """Get a human-readable summary of results."""
        rows = self.get_aggregated(experiment)
        if not rows:
            return f"No results found for experiment '{experiment}'"

        lines = [f"Results for '{experiment}' ({len(rows)} data points):"]
        lines.append("")

        _CODEC_COLS = [
            "codec_main_ratio_min", "codec_main_ratio_max", "codec_main_ratio_avg",
            "codec_main_rate_min", "codec_main_rate_max", "codec_main_rate_avg",
            "codec_child_ratio_min", "codec_child_ratio_max", "codec_child_ratio_avg",
            "codec_child_rate_min", "codec_child_rate_max", "codec_child_rate_avg",
        ]

        current_variant = None
        for row in rows:
            if row["variant"] != current_variant:
                current_variant = row["variant"]
                lines.append(f"  Variant: {current_variant}")

            qref = row.get("query_ref", "")
            qref_str = f" [{qref}]" if qref else ""
            is_codec = any(row.get(c) is not None for c in _CODEC_COLS)

            if is_codec:
                lines.append(
                    f"    {row['series']:20s} x={row['x_value']:>6s}  "
                    f"(n={row['num_repetitions']}){qref_str}"
                )
                for col in _CODEC_COLS:
                    val = row.get(col)
                    if val is not None:
                        lines.append(f"        {col:24s} {val}")
            else:
                tp = row.get("throughput_mbps")
                tp_str = f"{tp:.1f} MB/s" if tp is not None else "N/A"
                # Format-invariant companion metric: rows don't change with the encoding, so
                # Mrows/s comparisons across formats charge each format its byte footprint,
                # while MB/s (own input) shows per-byte engine efficiency. Report both.
                mr = row.get("throughput_mrows_s")
                mr_str = f"  {mr:.1f} Mrows/s" if mr is not None else ""
                lines.append(
                    f"    {row['series']:20s} x={row['x_value']:>6s}  "
                    f"throughput={tp_str}{mr_str}  (n={row['num_repetitions']}){qref_str}"
                )

        return "\n".join(lines)

    def delete_experiment(self, experiment: str) -> int:
        """Delete all runs for an experiment. Returns number of rows deleted."""
        count = self.conn.execute(
            "SELECT COUNT(*) FROM runs WHERE experiment = $1", [experiment]
        ).fetchone()[0]
        self.conn.execute("DELETE FROM runs WHERE experiment = $1", [experiment])
        return count

    def close(self) -> None:
        self.conn.close()
