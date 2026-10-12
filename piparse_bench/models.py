"""Pydantic data models for the PiParse benchmarking framework."""

from __future__ import annotations

import uuid
from datetime import datetime
from enum import Enum
from typing import Any, Literal

from pydantic import BaseModel, Field


# ── Enums ──────────────────────────────────────────────────────────────────────

class SourceType(str, Enum):
    FILE = "File"
    BLOCKING_FILE = "BlockingFile"
    IOURING_FILE = "IoUringFile"
    IOURING_FILE_ASYNC = "IoUringFileAsync"
    TCP = "TCP"
    BLOCKING_TCP = "BlockingTCP"
    IOURING_TCP = "IoUringTCP"


class SinkType(str, Enum):
    FILE = "File"
    CHECKSUM = "Checksum"
    VOID = "Void"
    MONITORING_SINK = "Monitoring"
    CODEC_MONITORING_SINK = "CodecMonitoring"


class InputFormatterType(str, Enum):
    CSV = "CSV"
    JSON = "JSON"
    UNCOMPILED_CSV = "UncompiledCSV"
    UNCOMPILED_JSON = "UncompiledJSON"
    SEQUENTIAL_CSV = "SequentialCSV"
    SEQUENTIAL_JSON = "SequentialJSON"
    SEQUENTIAL_UNCOMPILED_CSV = "SequentialUncompiledCSV"
    SEQUENTIAL_UNCOMPILED_JSON = "SequentialUncompiledJSON"


class OutputFormatterType(str, Enum):
    CSV = "CSV"
    JSON = "JSON"
    CSV_UNCOMPILED = "CSVUncompiled"
    JSON_UNCOMPILED = "JSONUncompiled"


class ExecutionMode(str, Enum):
    COMPILER = "COMPILER"
    INTERPRETER = "INTERPRETER"


class DumpCompilationResult(str, Enum):
    NONE = "NONE"
    IR = "IR"
    ASM = "ASM"


class ChartType(str, Enum):
    LINE_CHART = "line_chart"
    BAR_CHART = "bar_chart"
    GROUPED_BAR = "grouped_bar"
    HEATMAP = "heatmap"


class AxisScale(str, Enum):
    LINEAR = "linear"
    LOG = "log"


class Aggregation(str, Enum):
    MEDIAN = "median"
    MEAN = "mean"
    MIN = "min"
    MAX = "max"


# ── Query Catalog ──────────────────────────────────────────────────────────────

class SchemaField(BaseModel):
    name: str
    type: str
    not_null: bool = True


class DataEntry(BaseModel):
    file: str
    size_bytes: int
    # Logical row count of the file. Enables the format-invariant throughput_mrows_s metric:
    # the same logical stream is e.g. 6.44 GB as CSV but 4.42 GB as .nes, so MB/s(own input)
    # measures per-byte engine efficiency while Mrows/s (+ time/pass) measures the workload
    # outcome that charges a format its byte footprint. Report BOTH (user convention 2026-07-08).
    rows: int | None = None


class ValidationEntry(BaseModel):
    tuples: int
    checksum: int


class FlinkRowtime(BaseModel):
    column: str                    # source column holding ms-epoch event time (BIGINT-mapped)
    delay: str = "1 SECOND"        # watermark bound, SQL interval text e.g. "1 SECOND"


class SparkRowtime(BaseModel):
    """Spark Structured Streaming event-time attribute for windowed queries (mode: spark).

    Analog of FlinkRowtime. When set, render_spark's driver derives a computed rowtime column
    `__rt` = timestamp_millis(<column>) (the raw value is ms-epoch — TimeMeasure is ms in NES)
    and applies `withWatermark("__rt", delay)`. The query's spark_sql references `__rt` in
    window(...)/GROUP BY. `delay` is a Spark interval string (e.g. "1 millisecond"); the 1ms
    default matches the Flink ports' 0.001-SECOND watermark (0 delay drops a row whose ts equals
    its ending window's max after an equal-ts predecessor — see the stateful experiment notes)."""
    column: str
    delay: str = "1 millisecond"


class QuerySource(BaseModel):
    name: str
    schema_: list[SchemaField] = Field(alias="schema")
    data: dict[str, DataEntry] = {}
    instances: int = 1
    # 0-based index of the timestamp column in the CSV data file. When set,
    # the TCP generator shifts this column by the previous iteration's last
    # timestamp on each replay, so windowed queries see strictly increasing
    # timestamps across loops.
    timestamp_field: int | None = None
    # Flink event-time attribute for windowed queries (mode: flink). When set, the rendered
    # source DDL gains a computed rowtime column named `__rt` = TO_TIMESTAMP_LTZ(<column>, 3)
    # (NES window semantics treat the raw value as MILLISECONDS - TimeMeasure is ms) plus
    # `WATERMARK FOR __rt AS __rt - INTERVAL '<delay>'`. The query's flink_sql references
    # __rt in window DESCRIPTOR()s. delay covers the file's max out-of-order backjump
    # (late rows below the watermark are DROPPED by Flink -> count mismatches vs NES).
    flink_rowtime: FlinkRowtime | None = None
    # Spark Structured Streaming event-time attribute (mode: spark). See SparkRowtime.
    spark_rowtime: SparkRowtime | None = None

    model_config = {"populate_by_name": True}


class QueryCatalogEntry(BaseModel):
    name: str
    description: str = ""
    sql: str
    sources: list[QuerySource]
    sink_schema: list[SchemaField] = []
    validation: dict[str, ValidationEntry] = {}
    # Flink-dialect SELECT body for the `mode: flink` runner. The source CREATE TABLE and
    # blackhole sink DDL are auto-derived from `sources`/`schema`/`data`, so only the SELECT
    # (which differs from NES SQL — backticked reserved words, CAST instead of typed literals,
    # Flink windowing syntax) is hand-authored per query. None = query not yet ported to Flink.
    flink_sql: str | None = None
    # Spark-SQL SELECT body for the `mode: spark` runner (Structured Streaming). Like flink_sql,
    # source read + void sink are auto-derived by render_spark, so only the SELECT is authored per
    # query (backticked reserved words, CAST AS DOUBLE, single-quoted strings, `window(__rt, ...)`
    # in GROUP BY for windowed queries). None = query not yet ported to Spark.
    spark_sql: str | None = None


class QueryCatalogFile(BaseModel):
    """Top-level wrapper for query YAML files."""
    query: QueryCatalogEntry


# ── Experiment YAML ────────────────────────────────────────────────────────────

class WorkerConfig(BaseModel):
    """Worker parameters (nested under defaults.worker)."""
    model_config = {"extra": "allow"}

    query_engine: dict[str, Any] = {}
    default_query_execution: dict[str, Any] = {}
    number_of_buffers_in_global_buffer_manager: int | None = None
    dump_compilation_result: str | None = None


class QueryConfig(BaseModel):
    """Query-level parameters."""
    model_config = {"extra": "allow"}

    source_type: str | None = None
    sink_type: str | None = None
    input_formatter: dict[str, str] | None = None
    output_formatter: dict[str, str] | None = None
    input_parser: dict[str, str] | None = None
    output_parser: dict[str, str] | None = None


class GeneratorConfig(BaseModel):
    model_config = {"extra": "allow"}

    data_file: str | None = None
    repeat: int | None = None
    mode: str | None = None
    hosts: list[str] | None = None


class Defaults(BaseModel):
    model_config = {"extra": "allow"}

    system: str = "nebulastream"
    branch: str | None = None
    mode: str = "systest"
    build: str = "native"
    data_size: str = "large"
    worker: dict[str, Any] = {}
    topology: str = "single-node"
    query_config: dict[str, Any] = {}
    generator: dict[str, Any] | None = None
    duration_seconds: int | None = None


# ── Axes ───────────────────────────────────────────────────────────────────────

class SimpleAxis(BaseModel):
    label: str
    parameter: str
    values: list[Any]


class CartesianAxis(BaseModel):
    label: str
    cartesian: dict[str, list[Any]]


class ExplicitAxisEntry(BaseModel):
    name: str
    overrides: dict[str, Any] = {}


class ExplicitAxis(BaseModel):
    label: str
    explicit: list[ExplicitAxisEntry]


class ExplicitSeriesEntry(BaseModel):
    name: str
    baseline: bool = False
    overrides: dict[str, Any] = {}
    system: str | None = None
    config: dict[str, Any] | None = None


class ParameterSeries(BaseModel):
    label: str
    parameter: str
    values: list[Any]
    baseline: Any | None = None


class ExplicitSeries(BaseModel):
    label: str
    explicit: list[ExplicitSeriesEntry]


class Axes(BaseModel):
    x: SimpleAxis | CartesianAxis | ExplicitAxis
    series: ParameterSeries | ExplicitSeries | None = None

    model_config = {"extra": "allow"}


class YAxis(BaseModel):
    metric: str
    label: str


class Visualization(BaseModel):
    type: ChartType
    x_label: str | None = None
    y_label: str | None = None
    x_scale: AxisScale = AxisScale.LINEAR
    y_scale: AxisScale = AxisScale.LINEAR
    group_label: str | None = None
    value_label: str | None = None
    legend_position: str | None = None
    shared_y: bool = True


class QueryAggregation(str, Enum):
    GEOMETRIC_MEAN = "geometric_mean"
    ARITHMETIC_MEAN = "arithmetic_mean"


class Variant(BaseModel):
    model_config = {"extra": "allow"}

    name: str
    description: str = ""
    query: str | None = None
    queries: list[str] | None = None
    query_aggregation: QueryAggregation = QueryAggregation.GEOMETRIC_MEAN
    overrides: dict[str, Any] = {}
    axes: Axes
    y: YAxis
    visualization: Visualization | None = None

    def get_query_refs(self) -> list[str]:
        """Return all query references for this variant."""
        if self.queries:
            return self.queries
        if self.query:
            return [self.query]
        raise ValueError(f"Variant '{self.name}': exactly one of 'query' or 'queries' must be specified")


class ExperimentMeta(BaseModel):
    name: str
    description: str = ""
    primary_metric: str = "throughput_mbps"
    higher_is_better: bool = True
    repetitions: int = 10
    aggregation: Aggregation = Aggregation.MEDIAN


class ExperimentFile(BaseModel):
    """Top-level structure of an experiment YAML file."""
    experiment: ExperimentMeta
    defaults: Defaults = Defaults()
    variants: list[Variant]


# ── RunConfig (fully resolved, ready to execute) ──────────────────────────────

class RunConfig(BaseModel):
    """A single fully-resolved run configuration (one data point, one repetition)."""
    experiment: str
    variant: str
    series: str
    repetition: int
    x_param: str
    x_value: str

    system: str
    branch: str | None = None
    mode: str
    build: str
    data_size: str
    topology: str

    worker_flags: dict[str, Any] = {}
    query_config: dict[str, Any] = {}
    query_ref: str  # e.g. "nexmark/bid_star"

    generator: dict[str, Any] | None = None
    duration_seconds: int | None = None
    num_queries: int = 1
    # Heterogeneous-mix scaling: when set, the N concurrent instances (num_queries) round-robin
    # over this list of query refs instead of all running `query_ref`. Each instance still gets
    # its own source + sink; the cross-instance aggregation pools over all N regardless of shape.
    # None (default) = the classic single-query fan-out (all N run `query_ref`).
    query_mix: list[str] | None = None

    # Full config snapshot for storage
    full_config: dict[str, Any] = {}


# ── RunResult ─────────────────────────────────────────────────────────────────

class RunResult(BaseModel):
    """Result of a single benchmark run."""
    run_id: str = Field(default_factory=lambda: str(uuid.uuid4()))
    timestamp: datetime = Field(default_factory=datetime.now)
    # Identifies all reps of ONE `pbench run` invocation (a timestamped, sortable id
    # stamped by cli.run). The `aggregated` view scopes to the LATEST run_group per
    # experiment so a re-run's headline metric is NOT contaminated by stale history.
    run_group: str | None = None

    # Identity (copied from RunConfig)
    experiment: str
    variant: str
    series: str
    repetition: int
    query_ref: str = ""  # e.g. "nexmark/bid_star"
    x_param: str
    x_value: str

    # Environment
    system: str
    branch: str | None = None
    mode: str | None = None
    build: str | None = None
    git_commit: str | None = None
    hostname: str | None = None

    # Full config
    config: dict[str, Any] = {}

    # Flattened columns
    worker_threads: int | None = None
    io_threads: int | None = None
    operator_buffer_size: int | None = None
    execution_mode: str | None = None
    input_formatter: str | None = None
    output_formatter: str | None = None
    invoke_mode: str | None = None
    source_type: str | None = None
    sink_type: str | None = None
    num_queries: int | None = None

    # Metrics. throughput_mbps is the DEFENSIBLE wall-span aggregate (total bytes / cross-query
    # wall-span); throughput_sum_mbps is the old overcounting sum-of-per-query-rates (reference
    # only). Latencies are MICROSECONDS (the sink stamps in us; the old `_ms` naming was a
    # mislabel). Percentiles are median-across-queries; p99_latency_us_worst is the worst tail.
    throughput_mbps: float | None = None
    throughput_sum_mbps: float | None = None
    throughput_per_query_median: float | None = None
    throughput_per_query_min: float | None = None
    throughput_per_query_max: float | None = None
    processing_time_s: float | None = None
    # Format-invariant throughput: logical input rows (catalog DataEntry.rows summed over the
    # query's sources, x generator.repeat) / processing_time_s. NULL when the catalog lacks row
    # counts. Compare formats on THIS (or time/pass) + throughput_mbps together: equal Mrows/s
    # with unequal MB/s = pure byte-footprint difference; unequal MB/s at equal bytes = engine cost.
    input_rows: int | None = None
    throughput_mrows_s: float | None = None
    avg_latency_us: float | None = None
    min_latency_us: float | None = None
    max_latency_us: float | None = None
    p50_latency_us: float | None = None
    p99_latency_us: float | None = None
    p999_latency_us: float | None = None
    p99_latency_us_worst: float | None = None

    # Codec monitoring metrics
    codec_main_ratio_min: float | None = None
    codec_main_ratio_max: float | None = None
    codec_main_ratio_avg: float | None = None
    codec_main_rate_min: float | None = None
    codec_main_rate_max: float | None = None
    codec_main_rate_avg: float | None = None
    codec_child_ratio_min: float | None = None
    codec_child_ratio_max: float | None = None
    codec_child_ratio_avg: float | None = None
    codec_child_rate_min: float | None = None
    codec_child_rate_max: float | None = None
    codec_child_rate_avg: float | None = None
