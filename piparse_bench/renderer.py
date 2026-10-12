"""Render query catalog entries + RunConfig into .test files or query YAMLs."""

from __future__ import annotations

import re
from pathlib import Path
from typing import Any

import yaml

from .models import QueryCatalogEntry, QuerySource, RunConfig, SchemaField
from .resolver import load_query

_CODEC_EXTENSIONS: dict[str, str] = {
    "LZ4": ".lz4",
    "ZSTD": ".zst",
    "Snappy": ".snz",
}

# NES catalog types → Flink SQL types (for the `mode: flink` runner). NES has no unsigned
# types in Flink, so UINT64 maps to BIGINT (the ms-epoch timestamps fit); VARSIZED → STRING.
_NES_TO_FLINK_TYPE: dict[str, str] = {
    "UINT64": "BIGINT",
    "INT64": "BIGINT",
    # unsigned types widen (zero-extend) into the next signed Flink type — required by the
    # nes-native reader's storage-kind compatibility (a U32 cannot be served as INT), and
    # value-correct for csv too (UINT32 values > 2^31 would overflow INT).
    "UINT32": "BIGINT",
    "INT32": "INT",
    "UINT16": "INT",
    "INT16": "SMALLINT",
    "UINT8": "SMALLINT",
    "INT8": "TINYINT",
    "FLOAT64": "DOUBLE",
    "FLOAT32": "FLOAT",
    "VARSIZED": "STRING",
    "BOOLEAN": "BOOLEAN",
}

# NES catalog types → Spark SQL DDL types (for the `mode: spark` runner). Spark, like Flink, has
# no unsigned types: UINT64/UINT32 widen to BIGINT (ms-epoch fits), narrower unsigned widen one
# signed step so a value above the signed max never overflows; VARSIZED → STRING. Kept as its own
# map (not aliased to _NES_TO_FLINK_TYPE) so the two engines' dialects can drift independently.
_NES_TO_SPARK_TYPE: dict[str, str] = {
    "UINT64": "BIGINT",
    "INT64": "BIGINT",
    "UINT32": "BIGINT",
    "INT32": "INT",
    "UINT16": "INT",
    "INT16": "SMALLINT",
    "UINT8": "SMALLINT",
    "INT8": "TINYINT",
    "FLOAT64": "DOUBLE",
    "FLOAT32": "FLOAT",
    "VARSIZED": "STRING",
    "BOOLEAN": "BOOLEAN",
}


def _apply_codec_extension(file_path: str, source_codec: str | None) -> str:
    """Append codec-specific extension to a data file path if codec is set."""
    if source_codec and source_codec in _CODEC_EXTENSIONS:
        return file_path + _CODEC_EXTENSIONS[source_codec]
    return file_path


def _schema_to_sql(fields: list[SchemaField]) -> str:
    """Convert schema fields to SQL column definitions."""
    parts = []
    for f in fields:
        col = f"{f.name} {f.type}"
        if f.not_null:
            col += " NOT NULL"
        parts.append(col)
    return ", ".join(parts)


def _render_source_set_clause(
    qc: dict[str, Any], *, overload: bool = False, source_codec: str | None = None,
) -> str:
    """Build the SET(...) clause for a physical source."""
    sets: list[str] = []

    # Input formatter type
    fmt = qc.get("input_formatter", {})
    if isinstance(fmt, dict) and "type" in fmt:
        sets.append(f'"{fmt["type"]}" AS INPUT_FORMATTER.`TYPE`')
    elif isinstance(fmt, str):
        sets.append(f'"{fmt}" AS INPUT_FORMATTER.`TYPE`')

    # Threading mode — emit whenever specified. The engine does NOT infer it from the formatter name:
    # a Sequential* formatter (IsSequential=true) still needs the SOURCE to run its sequential routine
    # (emit Execute, prepend truncated bytes, aligned buffers). Without THREADING_MODE the blocking source
    # runs the parallel routine feeding a sequential formatter -> misalignment -> 0 throughput. Uppercased
    # to match the InputFormatterThreadingMode enum spelling used in the committed .test files.
    if isinstance(fmt, dict) and "threading_mode" in fmt:
        sets.append(f'"{str(fmt["threading_mode"]).upper()}" AS INPUT_FORMATTER.`THREADING_MODE`')

    # allow_commas_in_strings — emitted ONLY when the experiment sets it explicitly. The ENGINE
    # default is TRUE (quote-aware kernel, ~22% extra index CPU); the worker-cli renderer already
    # defaults it FALSE for CSV formatters, but the systest path historically did not emit the key
    # at all — so committed systest experiments (fullstack_*, formatter_*) ran quote-AWARE. We do
    # NOT retro-default it here (that would silently change what their recorded RESULTS mean);
    # fast-config experiments must set input_formatter.allow_commas_in_strings: false themselves.
    if isinstance(fmt, dict) and "allow_commas_in_strings" in fmt:
        val = "TRUE" if fmt["allow_commas_in_strings"] else "FALSE"
        sets.append(f"{val} AS INPUT_FORMATTER.`ALLOW_COMMAS_IN_STRINGS`")

    # assume_clean_strings — CSV-family only. ENGINE default is TRUE (strings asserted CLEAN -> the
    # output formatter raw-forwards them verbatim, skipping the per-row JSON escape scan = csv->json
    # varsized passthrough). Emitted only when the experiment sets it explicitly; set FALSE to force
    # the escaping path (the pre-gate baseline behaviour) for an in-binary A/B of the passthrough gate.
    if isinstance(fmt, dict) and "assume_clean_strings" in fmt:
        val = "TRUE" if fmt["assume_clean_strings"] else "FALSE"
        sets.append(f"{val} AS INPUT_FORMATTER.`ASSUME_CLEAN_STRINGS`")

    # eager_parse — naive-vs-lazy serde lever (InputFormatterDescriptor.EAGER_PARSE). Default FALSE
    # (lazy passthrough); TRUE materialises every leaf (numeric + varsized) at scan -> the output
    # re-serialises them = the "naive code generation" baseline. See render_worker_cli for the full note.
    if isinstance(fmt, dict) and "eager_parse" in fmt:
        val = "TRUE" if fmt["eager_parse"] else "FALSE"
        sets.append(f"{val} AS INPUT_FORMATTER.`EAGER_PARSE`")

    # string_length — REQUIRED by the SystemNative formatter (no engine default): comma-separated
    # max byte lengths consumed by VARSIZED fields in schema order. Extra entries are ignored and
    # schemas without VARSIZED still need the key set (any dummy value works), so a single global
    # value is safe for multi-source queries (e.g. nexmark person+auction).
    if isinstance(fmt, dict) and "string_length" in fmt:
        sets.append(f"'{fmt['string_length']}' AS INPUT_FORMATTER.`STRING_LENGTH`")

    # sequence_shredder_mode — selects the spanning-tuple resolution structure (LOCK_FREE default,
    # LOCKING, LOCK_FREE_FRAGMENTS = fragment-copy shredder that does not pin buffers per source).
    if isinstance(fmt, dict) and "sequence_shredder_mode" in fmt:
        sets.append(f'"{str(fmt["sequence_shredder_mode"]).upper()}" AS INPUT_FORMATTER.`SEQUENCE_SHREDDER_MODE`')

    # Formatter-specific input options pass through verbatim (e.g. HL7's delimiter keys, incl.
    # subcomponent_delimiter: '' to take '&' out of the structural class for '&'-bearing data
    # like the nexmark bid URLs). Values may carry escape sequences ('\\x1C\\r'); the engine
    # unescapes them at validation.
    _handled_input_keys = {
        "type", "threading_mode", "allow_commas_in_strings", "assume_clean_strings",
        "string_length", "sequence_shredder_mode", "eager_parse",
    }
    if isinstance(fmt, dict):
        for key, value in fmt.items():
            if key not in _handled_input_keys:
                sets.append(f"'{value}' AS INPUT_FORMATTER.`{key.upper()}`")

    # Input parser overrides
    for dtype, parser in qc.get("input_parser", {}).items():
        sets.append(f'"{parser}" AS INPUT_FORMATTER.{dtype}_PARSER')

    # LazyValue overloaded comparisons
    if overload:
        sets.append("TRUE AS INPUT_FORMATTER.LAZY_INT_OVERLOADS")
        sets.append("TRUE AS INPUT_FORMATTER.LAZY_FLOAT_OVERLOADS")
        sets.append("TRUE AS INPUT_FORMATTER.LAZY_UINT_OVERLOADS")

    # Source codec
    if source_codec:
        sets.append(f"'{source_codec}' AS `SOURCE`.CODEC")

    if sets:
        return " SET(" + ", ".join(sets) + ")"
    return ""


def _compute_input_size(config: RunConfig, query: QueryCatalogEntry) -> int:
    """Compute total input data size in bytes, accounting for generator repeats."""
    data_size = config.data_size
    total_size = 0
    for src in query.sources:
        if data_size in src.data:
            total_size += src.data[data_size].size_bytes
        elif src.data:
            total_size += next(iter(src.data.values())).size_bytes

    # Account for generator repeats (TCP sources replay data multiple times)
    gen = config.generator
    if gen:
        repeat = gen.get("repeat", 1)
        total_size *= repeat

    return total_size


def _render_sink_into_clause(
    qc: dict[str, Any], input_size: int | None = None, sink_codec: str | None = None,
) -> str:
    """Build additional config items for the INTO clause."""
    parts: list[str] = []

    # Output formatter type
    ofmt = qc.get("output_formatter", {})
    ofmt_type = ofmt.get("type") if isinstance(ofmt, dict) else ofmt
    if ofmt_type:
        # Uncompiled output formatters translate to NATIVE + legacy format
        _UNCOMPILED_OUTPUT = {"CSVUncompiled": "CSV", "JSONUncompiled": "JSON", "HL7Uncompiled": "HL7", "XMLUncompiled": "XML"}
        if ofmt_type in _UNCOMPILED_OUTPUT:
            parts.append("'NATIVE' AS `SINK`.OUTPUT_FORMAT")
            parts.append(f"'{_UNCOMPILED_OUTPUT[ofmt_type]}' AS `SINK`.LEGACY_OUTPUT_FORMAT")
        else:
            parts.append(f'"{ofmt_type}" AS `SINK`.OUTPUT_FORMAT')

    # Input data size for Monitoring sink
    if input_size is not None:
        parts.append(f'{input_size} AS `SINK`.SIZE_OF_INPUT_DATA_IN_BYTES')

    # Output parser overrides
    for dtype, parser in qc.get("output_parser", {}).items():
        parts.append(f'"{parser}" AS `OUTPUT_FORMATTER`.{dtype}_PARSER')

    # Output-formatter format options (e.g. HL7's segment_name / message_header): every key of the
    # output_formatter dict except the type becomes an OUTPUT_FORMATTER.<KEY> config entry.
    if isinstance(ofmt, dict):
        for key, value in ofmt.items():
            if key != "type":
                parts.append(f"'{value}' AS `OUTPUT_FORMATTER`.{key.upper()}")

    # Sink codec
    if sink_codec:
        parts.append(f"'{sink_codec}' AS `SINK`.CODEC")

    return ", ".join(parts)


# ── Systest rendering ─────────────────────────────────────────────────────────

def render_systest(config: RunConfig, query: QueryCatalogEntry) -> str:
    """Render a .test file string for the systest runner."""
    qc = config.query_config
    lines: list[str] = []

    source_type = qc.get("source_type", "File")
    sink_type = qc.get("sink_type", "Monitoring")
    data_size = config.data_size
    overload = config.full_config.get("overload", False)
    source_codec = config.full_config.get("source_codec")
    if source_codec == "None":
        source_codec = None
    sink_codec = config.full_config.get("sink_codec")

    # CREATE LOGICAL SOURCE for each source
    for src in query.sources:
        schema_sql = _schema_to_sql(src.schema_)
        lines.append(f"CREATE LOGICAL SOURCE {src.name}({schema_sql});")

        # CREATE PHYSICAL SOURCE
        set_clause = _render_source_set_clause(qc, overload=overload, source_codec=source_codec)
        lines.append(
            f"CREATE PHYSICAL SOURCE FOR {src.name} TYPE {source_type}{set_clause};"
        )

        # ATTACH FILE — use 'encoded' data entry when codec is set
        file_key = "encoded" if source_codec and "encoded" in src.data else data_size
        if file_key in src.data:
            file = _apply_codec_extension(src.data[file_key].file, source_codec)
            lines.append(f"ATTACH FILE {file}")
        elif src.data:
            first_key = next(iter(src.data))
            file = _apply_codec_extension(src.data[first_key].file, source_codec)
            lines.append(f"ATTACH FILE {file}")

    # Query + INTO sink
    input_size = _compute_input_size(config, query) if sink_type == "Monitoring" else None
    sink_config = _render_sink_into_clause(qc, input_size=input_size, sink_codec=sink_codec)
    sql = query.sql.strip()
    if config.mode == "bench-runner" and not sink_config:
        # nes-bench-runner's binder rejects an EMPTY inline sink ("INTO Void" / "INTO
        # Void()" both fail to bind), unlike systest which tolerates it. A non-empty
        # inline sink (e.g. INTO Checksum("CSV" AS SINK.OUTPUT_FORMAT)) binds fine.
        # So for the empty-config case (typically a Void throughput sink) emit a named
        # CREATE SINK, the form every committed benchmark .test uses.
        sink_schema = query.sink_schema or query.sources[0].schema_
        sink_name = f"{query.sources[0].name}_sink"
        lines.append(f"CREATE SINK {sink_name}({_schema_to_sql(sink_schema)}) TYPE {sink_type};")
        lines.append(f"{sql} INTO {sink_name};")
    else:
        lines.append(f"{sql} INTO {sink_type}({sink_config});")

    # Separator + validation
    lines.append("----")
    if data_size in query.validation:
        v = query.validation[data_size]
        lines.append(f"{v.tuples} {v.checksum}")
    else:
        lines.append("")

    return "\n".join(lines)


# ── Worker-CLI rendering ──────────────────────────────────────────────────────

def render_worker_cli(
    config: RunConfig,
    query: QueryCatalogEntry,
    query_index: int = 0,
    monitoring_dir: str = "/tmp/piparse_bench",
    tcp_host: str = "localhost",
    tcp_base_port: int = 1111,
    worker_port: int = 8080,
    data_dir: str | None = None,
) -> dict[str, Any]:
    """Render a query YAML dict for the nes-cli runner.

    Returns a dict that can be yaml.dump()'d and passed to nes-cli.
    """
    qc = config.query_config
    source_type = qc.get("source_type", "TCP")
    sink_type = qc.get("sink_type", "Monitoring")
    data_size = config.data_size
    overload = config.full_config.get("overload", False)
    source_codec = config.full_config.get("source_codec")
    if source_codec == "None":
        source_codec = None
    sink_codec = config.full_config.get("sink_codec")

    # Determine input formatter type
    fmt = qc.get("input_formatter", {})
    fmt_type = fmt.get("type", "CSV") if isinstance(fmt, dict) else (fmt or "CSV")

    # Determine output formatter type
    ofmt = qc.get("output_formatter", {})
    ofmt_type = ofmt.get("type", "CSV") if isinstance(ofmt, dict) else (ofmt or "CSV")

    # Build sink name
    sink_name = f"{query.sources[0].name}Sink"

    # SQL with INTO
    sql = query.sql.strip()
    query_sql = f"{sql} INTO {sink_name}"

    # Build logical sources
    logical = []
    for src in query.sources:
        schema_list = [{"name": f.name, "type": f.type} for f in src.schema_]
        logical.append({"name": src.name, "schema": schema_list})

    # Build physical sources
    physical = []
    for i, src in enumerate(query.sources):
        gen_port = tcp_base_port + i
        phys: dict[str, Any] = {
            "logical": src.name,
            "type": source_type,
            "host": f"{tcp_host}:{worker_port}",
        }
        phys["parser_config"] = {"type": fmt_type}

        # allow_commas_in_strings — the CSV indexer default is TRUE (quote-aware: ignore commas inside
        # quoted strings). Quote-awareness costs a per-64B-block prefixXor quote-parity pass PLUS an
        # always-computed quote SIMD compare in the kernel: measured ~22% of index CPU and ~+11% e2e on
        # projection-heavy queries (formatter-scaling A/B on f16, 2026-07-05; see the index-field-scaling
        # findings). EVERY pbench dataset is quote-free numeric CSV, so FALSE is both CORRECT and faster
        # -> default it FALSE for all CSV formatters here (this is "the fast config"). CAVEAT: a dataset
        # with quoted strings containing commas MUST override input_formatter.allow_commas_in_strings: true,
        # else the flat-band indexer misaligns fields silently (a Checksum sink catches it; Monitoring won't).
        if "CSV" in (fmt_type or ""):
            allow_commas = fmt.get("allow_commas_in_strings", False) if isinstance(fmt, dict) else False
            phys["parser_config"]["allow_commas_in_strings"] = bool(allow_commas)

        # Threading mode — the worker-cli path must emit this too (not just the .test renderer).
        # The engine does NOT infer SEQUENTIAL from a Sequential* formatter name: without it the
        # blocking source runs the PARALLEL routine and the formatter executes as a WORKER TASK
        # (index+parse on WorkerThread-N) instead of INLINE on the source thread -> "sequential"
        # silently becomes 2-thread parallel. Key/value match InputFormatterDescriptor.THREADING_MODE
        # ("threading_mode", enum SEQUENTIAL/PARALLEL uppercased).
        if isinstance(fmt, dict) and "threading_mode" in fmt:
            phys["parser_config"]["threading_mode"] = str(fmt["threading_mode"]).upper()

        # string_length — REQUIRED by SystemNative (comma-separated VARSIZED max byte lengths,
        # consumed in schema order; extra entries ignored, so one global value serves all sources).
        if isinstance(fmt, dict) and "string_length" in fmt:
            phys["parser_config"]["string_length"] = str(fmt["string_length"])

        # sequence_shredder_mode — spanning-tuple resolution structure (LOCK_FREE default | LOCKING |
        # LOCK_FREE_FRAGMENTS). FRAGMENTS copies the boundary bytes instead of pinning ~1 buffer/source
        # in the shredder frontier, decoupling cell-pool sizing from source fan-in (the N=512 wedge).
        if isinstance(fmt, dict) and "sequence_shredder_mode" in fmt:
            phys["parser_config"]["sequence_shredder_mode"] = str(fmt["sequence_shredder_mode"]).upper()

        # eager_parse — the naive-vs-lazy serde lever (InputFormatterDescriptor.EAGER_PARSE). ENGINE default
        # FALSE (lazy: numeric AND varsized leaves stay {ptr,size} views and are raw-forwarded to the output
        # = passthrough serde). TRUE materialises EVERY leaf at scan time -> the output sees non-lazy values
        # and re-serialises them (numeric via the output codec, varsized via the escape-scan writer) = the
        # "naive code generation" arm that splits the compile rung into naive(eager) vs lazy(passthrough).
        # Emit as a REAL bool (yaml true/false) so the engine's ConfigParameter<bool> parses it -- the generic
        # pass-through below would str() it to "True"; hence it is also in _handled_input_keys.
        if isinstance(fmt, dict) and "eager_parse" in fmt:
            phys["parser_config"]["eager_parse"] = bool(fmt["eager_parse"])

        # Formatter-specific input options pass through verbatim (e.g. HL7's delimiter keys, incl.
        # subcomponent_delimiter: '' to take '&' out of the structural class for '&'-bearing data
        # like the nexmark bid URLs). Mirrors the .test renderer's pass-through.
        _handled_input_keys = {
            "type", "threading_mode", "allow_commas_in_strings", "assume_clean_strings",
            "string_length", "sequence_shredder_mode", "eager_parse",
        }
        if isinstance(fmt, dict):
            for key, value in fmt.items():
                if key not in _handled_input_keys:
                    phys["parser_config"][key] = str(value)

        # LazyValue overloaded comparisons
        if overload:
            phys["parser_config"]["lazy_int_overloads"] = True
            phys["parser_config"]["lazy_float_overloads"] = True
            phys["parser_config"]["lazy_uint_overloads"] = True

        # Input parser overrides
        for dtype, parser in qc.get("input_parser", {}).items():
            phys["parser_config"][f"{dtype}_parser"] = parser

        # Source config (TCP connection, file path, codec, etc.)
        src_config: dict[str, Any] = {}
        if source_type in ("TCP", "IoUringTCP", "IoUringTCPBlocking"):
            src_config["socketHost"] = tcp_host
            src_config["socketPort"] = gen_port
        elif source_type == "BlockingTCP":
            src_config["socket_host"] = tcp_host
            src_config["socket_port"] = gen_port
            src_config["flush_interval_ms"] = 1000
        elif source_type in ("File", "BlockingFile", "MmapFile", "IoUringFile", "IoUringFileAsync", "InMemory"):
            # Resolve the data file under the runner-provided testdata dir. InMemory reads the file into
            # TupleBuffers in its ctor (NES_INMEM_PREFILL) and replays them zero-copy (NES_INMEM_REPEAT),
            # so the timed window does pure compute with no I/O -- the engine analogue of an in-RAM buffer.
            entry = src.data.get(data_size) or (next(iter(src.data.values())) if src.data else None)
            if entry is None:
                raise RuntimeError(f"File source '{src.name}' has no data entry")
            file_path = str(Path(data_dir) / entry.file) if data_dir else entry.file
            src_config["file_path"] = file_path
        if source_codec:
            src_config["codec"] = source_codec
        # Extra per-source config passthrough (query_config.source_config), merged LAST so
        # experiments can override defaults set above -- e.g. connect_timeout_seconds for
        # BlockingTCP: its SO_RCVTIMEO (default 10 s) also gates the FIRST byte, and a
        # high-N all-released-together start needs a much larger value (see
        # tcp_parse_scaling_ext.yaml RESULTS: 226/500 sources died at start with EAGAIN).
        src_config.update(qc.get("source_config") or {})
        if src_config:
            phys["source_config"] = src_config

        physical.append(phys)

    # Build sink — CLI uses '$' as the source/field name delimiter instead of '.'
    sink_schema = query.sink_schema or query.sources[0].schema_
    sink_schema_list = [{"name": f.name.replace(".", "$"), "type": f.type} for f in sink_schema]

    total_size = _compute_input_size(config, query)

    monitoring_path = f"{monitoring_dir}/query_{query_index:04d}_monitoring.csv"

    sink_config: dict[str, Any] = {}
    _UNCOMPILED_OUTPUT = {"CSVUncompiled": "CSV", "JSONUncompiled": "JSON", "HL7Uncompiled": "HL7", "XMLUncompiled": "XML"}
    if sink_type == "Monitoring":
        sink_config = {
            "file_path": monitoring_path,
            "size_of_input_data_in_bytes": total_size,
        }
        if ofmt_type in _UNCOMPILED_OUTPUT:
            sink_config["output_format"] = "NATIVE"
            sink_config["legacy_output_format"] = _UNCOMPILED_OUTPUT[ofmt_type]
        else:
            sink_config["output_format"] = ofmt_type
    elif sink_type == "CodecMonitoring":
        sink_config = {
            "file_path": monitoring_path,
        }
    elif sink_type == "Checksum":
        # Correctness sink: accumulates an order- AND content-sensitive checksum over the
        # materialized output and writes "count,checksum" at EoS. legacy_output_format=CSV
        # forces full field materialization so the checksum catches any missing / duplicated /
        # out-of-order tuple (incl. cross-buffer stitching corruption). Used to A/B two sources
        # for byte-equivalence (e.g. IoUringFile vs the async File baseline).
        sink_config = {
            "file_path": f"{monitoring_dir}/query_{query_index:04d}_checksum.csv",
            "legacy_output_format": "CSV",
        }

    # Sink codec
    if sink_codec:
        sink_config["codec"] = sink_codec

    # Output-formatter parser overrides (e.g. ZMIJF64 for float output) go in the
    # sink's `parser_config`, NOT `config`: CLIStarter maps sink.config -> the SINK
    # descriptor (OUTPUT_FORMAT etc.) and sink.parser_config -> the OUTPUT_FORMATTER
    # descriptor (getOutputFormatterConfig, where {dtype}_parser lives). Putting a
    # parser key in `config` makes nes-cli reject the sink (unknown SINK key -> exit 1).
    sink_parser_config: dict[str, Any] = {}
    for dtype, parser in qc.get("output_parser", {}).items():
        sink_parser_config[f"{dtype}_parser"] = parser

    # Output-formatter format options (e.g. HL7's segment_name / message_header) also live in the
    # sink's parser_config (the OUTPUT_FORMATTER descriptor).
    if isinstance(ofmt, dict):
        for key, value in ofmt.items():
            if key != "type":
                sink_parser_config[key] = str(value)

    sink = {
        "name": sink_name,
        "host": f"{tcp_host}:{worker_port}",
        "type": sink_type,
        "schema": sink_schema_list,
        "parser_config": sink_parser_config,
    }
    if sink_config:
        sink["config"] = sink_config

    # Build workers section
    workers = [{"host": f"{tcp_host}:{worker_port}", "max_operators": 10000}]

    result = {
        "query": query_sql,
        "logical": logical,
        "physical": physical,
        "sinks": [sink],
        "workers": workers,
    }

    return result


def render_worker_cli_yaml(
    config: RunConfig,
    query: QueryCatalogEntry,
    query_index: int = 0,
    monitoring_dir: str = "/tmp/piparse_bench",
    tcp_host: str = "localhost",
    tcp_base_port: int = 1111,
    worker_port: int = 8080,
    data_dir: str | None = None,
) -> str:
    """Render a query YAML string for the nes-cli runner."""
    d = render_worker_cli(
        config, query, query_index, monitoring_dir, tcp_host, tcp_base_port, worker_port,
        data_dir=data_dir,
    )
    return yaml.dump(d, default_flow_style=False, sort_keys=False)


# ── Flink rendering ───────────────────────────────────────────────────────────

def _flink_type(nes_type: str) -> str:
    return _NES_TO_FLINK_TYPE.get(nes_type.upper(), "STRING")


# NES schema type → csv_to_binary.py layout token (the nes-native format's 'schema' option).
_NES_TO_LAYOUT_TOKEN: dict[str, str] = {
    "UINT64": "uint64", "INT64": "int64", "UINT32": "uint32", "INT32": "int32",
    "UINT16": "uint16", "INT16": "int16", "UINT8": "uint8", "INT8": "int8",
    "FLOAT64": "float64", "FLOAT32": "float32", "BOOLEAN": "bool", "CHAR": "char",
}


def _nes_native_layout(schema: list, config: RunConfig) -> str:
    """Build the nes-native layout string for a source schema. VARSIZED fields become
    string[N] slots; N comes from query_config.input_formatter.string_length (the SAME
    comma-separated list SystemNative consumes, in schema order, one cursor per source —
    slot width N includes the NUL padding byte, exactly csv_to_binary.py's string[N])."""
    fmt = (config.full_config.get("query_config") or {}).get("input_formatter") or {}
    lengths = [s.strip() for s in str(fmt.get("string_length", "")).split(",") if s.strip()]
    cursor = 0
    tokens: list[str] = []
    for f in schema:
        t = f.type.upper()
        if t == "VARSIZED":
            if cursor >= len(lengths):
                raise ValueError(
                    "nes-native rendering needs query_config.input_formatter.string_length "
                    f"(one entry per VARSIZED field in schema order) for field '{f.name}'."
                )
            tokens.append(f"string[{lengths[cursor]}]")
            cursor += 1
        elif t in _NES_TO_LAYOUT_TOKEN:
            tokens.append(_NES_TO_LAYOUT_TOKEN[t])
        else:
            raise ValueError(f"No nes-native layout token for NES type '{f.type}'.")
    return ",".join(tokens)


def _ext_jar_path() -> str:
    """The flink-ext jar with the custom connectors (blackhole-format sink, nes-native format)."""
    jar = Path(__file__).resolve().parent.parent / "flink-ext" / "pbench-flink-ext.jar"
    if not jar.exists():
        raise ValueError(
            f"custom Flink connectors need {jar}; build it with flink-ext/build.sh "
            "(compiles offline against the venv's pyflink jars)."
        )
    return str(jar)


def _spark_ext_jar_path() -> str:
    """The spark-ext jar with the custom `file-microbatch-csv` DataSource V2 streaming source
    (reads one bounded file as byte-range micro-batches -> real per-batch latencies)."""
    jar = Path(__file__).resolve().parent.parent / "spark-ext" / "pbench-spark-ext.jar"
    if not jar.exists():
        raise ValueError(
            f"custom Spark source needs {jar}; build it with spark-ext/build.sh "
            "(compiles offline against the venv's pyspark jars)."
        )
    return str(jar)


def _resolve_data_entry(src: QuerySource, data_size: str):
    """Pick the data file for the requested size, falling back to the first entry (mirrors
    the systest renderer's ATTACH FILE resolution and resolver.compute_input_rows)."""
    return src.data.get(data_size) or (next(iter(src.data.values())) if src.data else None)


# Self-contained PyFlink Table API driver. Executed as a subprocess by FlinkRunner (fresh JVM
# per rep). No literal { } braces below except the .format() placeholders — keep it that way.
_FLINK_DRIVER_TEMPLATE = '''"""Auto-generated by piparse-bench render_flink — PyFlink Table API benchmark driver.

Runs a nexmark query against Apache Flink. Pure Table API/SQL → native JVM operators, so there
is NO per-record Python in the data path. Default: INSERT the query into a blackhole sink and
print BYTES/ELAPSED/WALL. With `--count`: print ROWS (output tuple count) and skip timing.
"""
import os
import shutil
import sys
import tempfile
import time

from pyflink.common import Configuration
from pyflink.table import EnvironmentSettings, TableEnvironment

DDL = {ddl_literal}
INSERTS = {inserts_literal}
COUNT_SQL = {count_sql}
SIZE_BYTES = {size_bytes}
PARALLELISM = {parallelism}
NUM_SLOTS = {num_slots}
EXEC_MODE = {exec_mode}
EXT_JARS = {ext_jars}
FETCH_IO_SIZE = {fetch_io_size}
EXTRA_CONF = {extra_conf}

# json_file output mode: the sink DDL carries a __JSON_OUT_DIR__ marker; materialize it as a
# fresh tmpfs dir per run (real JSON bytes flow through Flink's serializer, files are deleted
# after timing — the closest Flink gets to "serialize then void").
_JSON_MARKER = "__JSON_OUT_DIR__"
_json_dir = None
if any(_JSON_MARKER in _s for _s in DDL):
    _shm = "/dev/shm" if os.path.isdir("/dev/shm") else None
    _json_dir = tempfile.mkdtemp(prefix="pbench_flink_json_", dir=_shm)
    DDL = [_s.replace(_JSON_MARKER, _json_dir) for _s in DDL]

_count_mode = "--count" in sys.argv
# The correctness pass counts the query OUTPUT. A streaming global COUNT(*) emits a per-record
# retract/update changelog (collect() would stream millions of rows to Python and [0] would be
# an early intermediate), so force BATCH for --count — batch global aggregation yields ONE final
# row. Throughput mode keeps EXEC_MODE (a projection is append-only; nothing returns to Python).
_use_mode = "batch" if _count_mode else EXEC_MODE
# Max-efficiency config (applied at cluster creation — the reliable path). These are the
# "give Flink every advantage" knobs that are legitimate for this workload; verified to hold
# the single-stream ceiling at ~110 MB/s, confirming it is a genuine single-thread parse bound.
_conf = Configuration()
_conf.set_string("parallelism.default", str(PARALLELISM))   # = NES worker-thread count
if NUM_SLOTS:  # latency scaling: N single-threaded pipelines need N task slots to run at once
    _conf.set_string("taskmanager.numberOfTaskSlots", str(NUM_SLOTS))
if EXT_JARS:  # custom connectors (e.g. the blackhole-format void sink) into the job classpath
    _conf.set_string("pipeline.jars", ";".join("file://" + _j for _j in EXT_JARS))
_conf.set_string("pipeline.object-reuse", "true")           # no defensive record copies
_conf.set_string("pipeline.operator-chaining", "true")      # source+project+sink in one thread
_conf.set_string("metrics.latency.interval", "0")           # no latency-marker overhead
# Deterministic time rendering: DATE_FORMAT/TO_TIMESTAMP_LTZ use the session time zone;
# UTC matches NES's CASTFROMUNIXTS (ISO-8601 UTC) so time-formatting queries compare fairly.
_conf.set_string("table.local-time-zone", "UTC")
# Latency arm only: align Flink's per-fetch byte size (bytes read/parsed per readBatch, the unit
# that all rows in a batch share as their ingest stamp) to NES's operator_buffer_size, so the
# per-batch stamp granularity matches NES's per-TupleBuffer stamp. Latency scales linearly with
# this; throughput is invariant to it. Derived from the experiment's operator_buffer_size, never
# hardcoded. Untouched (Flink default 1mb) for the throughput arms.
if FETCH_IO_SIZE:
    _conf.set_string("source.file.stream.io-fetch-size", str(FETCH_IO_SIZE))
# Experiment-level Flink configuration overrides (`flink_config:` mapping in the experiment
# YAML), applied LAST so they win. Meant for cluster-robustness knobs at high N that do not
# change what is measured -- e.g. heartbeat.timeout / pekko.ask.timeout, which the N=1000
# point on the Pi needed (default 50 s heartbeat.timeout died in an 88 s JVM stall). Same
# Configuration object as taskmanager.numberOfTaskSlots, so it reaches the MiniCluster the
# same proven way.
for _k, _v in EXTRA_CONF.items():
    _conf.set_string(str(_k), str(_v))
# Checkpointing is left DISABLED (default) = max throughput. Managed/network memory is left at
# Flink defaults on purpose: a stateless projection is parse-bound and unaffected by it, but
# future STATEFUL queries (windows/joins) need that working memory, so we don't starve them here.
_builder = EnvironmentSettings.new_instance()
_builder = _builder.in_batch_mode() if _use_mode == "batch" else _builder.in_streaming_mode()
t_env = TableEnvironment.create(_builder.with_configuration(_conf).build())

for _stmt in DDL:
    t_env.execute_sql(_stmt)

if _count_mode:
    _rows = list(t_env.execute_sql(COUNT_SQL).collect())[0][0]
    print("ROWS=%d" % int(_rows))
    sys.exit(0)

# Warmup: force the local MiniCluster to spin up OUTSIDE the timed region so its
# ~0.3-0.5s startup does not inflate a short job.
list(t_env.execute_sql("SELECT 1").collect())

# One or more INSERT pipelines. A StatementSet runs them CONCURRENTLY as one job (the latency
# scaling arm submits N independent source→sink pipelines this way); for a single insert it is
# equivalent to execute_sql. Time the whole set.
_t0 = time.perf_counter()
_stmt_set = t_env.create_statement_set()
for _ins in INSERTS:
    _stmt_set.add_insert_sql(_ins)
_res = _stmt_set.execute()
_res.wait()  # bounded filesystem sources -> the job terminates on its own
_wall = time.perf_counter() - _t0
try:
    _net = _res.get_job_client().get_job_execution_result().result().get_net_runtime() / 1000.0
    _elapsed = _net if _net and _net > 0 else _wall
except Exception:
    _elapsed = _wall

print("BYTES=%d" % SIZE_BYTES)
print("ELAPSED=%.6f" % _elapsed)
print("WALL=%.6f" % _wall)

if _json_dir is not None:
    _out_bytes = 0
    for _root, _dirs, _files in os.walk(_json_dir):
        for _fn in _files:
            _out_bytes += os.path.getsize(os.path.join(_root, _fn))
    print("JSON_OUT_BYTES=%d" % _out_bytes)
    shutil.rmtree(_json_dir, ignore_errors=True)
'''


def _inject_ingest_column(select_sql: str) -> str:
    """Append `, \\`__ingest_ns\\`` to the SELECT list of a single-block projection so the source's
    carried ingest-timestamp column flows to the sink. Inserts before the first top-level (paren
    depth 0) FROM. Only valid for a flat `SELECT ... FROM <src>` (the stateless single-source
    latency arm); joins/aggregations that collapse rows would need a defined ingest semantics."""
    depth = 0
    lower = select_sql.lower()
    i = 0
    while i < len(select_sql):
        c = select_sql[i]
        if c == "(":
            depth += 1
        elif c == ")":
            depth -= 1
        elif depth == 0 and lower.startswith("from", i) and (i == 0 or not select_sql[i - 1].isalnum()) \
                and (i + 4 >= len(select_sql) or not select_sql[i + 4].isalnum()):
            # A bare `SELECT *` already forwards the source's trailing synthetic __ingest_ns column
            # unchanged (the csv-latency source appends it last), so injecting a second one would
            # duplicate the column and break the sink arity. Skip injection for pure-star projections.
            proj_body = re.sub(r"^\s*select\s+", "", select_sql[:i], flags=re.I).strip()
            if proj_body == "*":
                return select_sql
            return f"{select_sql[:i]}, `__ingest_ns` {select_sql[i:]}"
        i += 1
    raise ValueError(f"Could not find a top-level FROM to inject __ingest_ns into: {select_sql!r}")


def _uniquify_sink_fields(fields) -> list[tuple[str, str]]:
    """Return (name, type) pairs with duplicate names disambiguated (Flink DDL forbids duplicate
    column names; a NES sink schema may repeat a field, e.g. bid.timestamp AND auction.timestamp
    under one label)."""
    seen: dict[str, int] = {}
    out: list[tuple[str, str]] = []
    for f in fields:
        n = seen.get(f.name, 0)
        seen[f.name] = n + 1
        out.append((f"{f.name}_{n + 1}" if n else f.name, f.type))
    return out


def render_flink(config: RunConfig, query: QueryCatalogEntry, testdata_dir: str | None = None,
                 **kwargs) -> str:
    """Render a self-contained PyFlink driver script (str) for the `flink` mode.

    One filesystem CSV source table per query source (types mapped from the catalog schema),
    the hand-authored `query.flink_sql` as the SELECT, a blackhole sink (parse-focused: no
    output serialization). Parallelism = NES worker-thread count; streaming by default.
    """
    if query.flink_sql is None:
        raise ValueError(f"Query '{query.name}' has no flink_sql; cannot render for mode 'flink'.")
    if testdata_dir is None:
        raise ValueError("render_flink requires testdata_dir (the root holding the CSV data files).")

    out_fmt = str(config.full_config.get("flink_output_format", "none")).lower()
    # latency = per-batch latency over a passthrough (non-serializing) sink; latency_json = the
    # same but the sink ALSO runs Flink's real JSON encoder (fair csv→json throughput + latency in
    # one run — the scaling figure's arm).
    latency_mode = out_fmt in ("latency", "latency_json")
    serialize_json = out_fmt == "latency_json"
    # "Run the query N times" = N independent source→project→sink pipelines (latency scaling arm,
    # the analog of NES num_queries concurrent File-source queries on one shared pool). Each
    # pipeline gets its own source table `<src>__p{i}` reading the same (warm) file and its own
    # latency sink; all N run concurrently in one job via a StatementSet.
    n_pipelines = int(config.num_queries or 1) if latency_mode else 1
    # Heterogeneous mix: the N pipelines round-robin over query_mix (each pipeline a possibly-
    # different query with its OWN output schema); absent = all pipelines run `query`. Every mix
    # query shares the same single source (`bid`), so the per-pipeline source tables are uniform;
    # only the SELECT and the sink schema vary per pipeline.
    pipeline_queries = ([load_query(r) for r in config.query_mix] if config.query_mix else [query])
    latency_src_name: str | None = None

    ddl_statements: list[str] = []
    total_bytes = 0
    total_rows = 0
    needs_ext_jar = False
    for src in query.sources:
        entry = _resolve_data_entry(src, config.data_size)
        if entry is None:
            raise ValueError(
                f"Source '{src.name}' has no data entry for size '{config.data_size}'."
            )
        data_path = str(Path(testdata_dir) / entry.file)
        total_bytes += entry.size_bytes
        if entry.rows:
            total_rows += entry.rows
        # Backtick every column (cheap way to quote reserved words like `timestamp`); nullable
        # (Flink NOT NULL enforcement on a CSV source can reject otherwise-fine rows).
        cols = ", ".join(f"`{f.name}` {_flink_type(f.type)}" for f in src.schema_)
        # Event-time attribute for windowed queries: computed ms-epoch rowtime + watermark
        # (see models.FlinkRowtime). flink_sql references `__rt` in DESCRIPTOR()s. Applies to
        # BOTH csv and .nes sources (computed columns are schema-level, connector-agnostic).
        if src.flink_rowtime is not None:
            rt = src.flink_rowtime
            amount, unit = rt.delay.split(maxsplit=1)  # "1 SECOND" -> INTERVAL '1' SECOND
            cols += (
                f", `__rt` AS TO_TIMESTAMP_LTZ(`{rt.column}`, 3), "
                f"WATERMARK FOR `__rt` AS `__rt` - INTERVAL '{amount}' {unit}"
            )
        if entry.file.endswith(".nes"):
            # NES native file → the custom zero-parse nes-native format (flink-ext jar):
            # fixed-width packed-LE rows served as flyweight RowData views; field access =
            # direct little-endian loads, no parsing/casting; the stream is chunked into
            # buffers at row boundaries purely for framing.
            #
            # DEFAULT = filesystem + 'format'='nes-native': ONE sequential stream / one reader,
            # exactly like the text arm (Flink local files are one split) — the fair
            # format-isolating arm ("native behaves like text").
            #
            # OPT-IN flink_native_split_size (e.g. '64mb') → the nes-native-file connector,
            # which chops the file into fixed-size byte-range splits so subtasks ingest ONE
            # file in parallel (possible only because rows are fixed-width). That changes the
            # ingestion topology, not just the format — a capability probe, NOT a comparison
            # arm against single-split text.
            needs_ext_jar = True
            layout = _nes_native_layout(src.schema_, config)
            split_size = config.full_config.get("flink_native_split_size")
            if split_size:
                ddl_statements.append(
                    f"CREATE TABLE {src.name} ({cols}) WITH ("
                    f"'connector' = 'nes-native-file', 'path' = '{data_path}', "
                    f"'schema' = '{layout}', 'split-size' = '{split_size}')"
                )
            else:
                ddl_statements.append(
                    f"CREATE TABLE {src.name} ({cols}) WITH ("
                    f"'connector' = 'filesystem', 'path' = '{data_path}', "
                    f"'format' = 'nes-native', 'nes-native.schema' = '{layout}')"
                )
        elif latency_mode:
            # Latency arm: the custom `csv-latency` format delegates to Flink's native CSV parser
            # and appends a trailing __ingest_ns BIGINT (nanoTime stamped at read/parse start; see
            # CsvLatencyFileFormatFactory) that the file does not contain — carried unchanged
            # through Flink's own source→task queue to the latency-blackhole sink. Options keep the
            # same bare keys, prefixed with the format identifier. One source table per pipeline.
            needs_ext_jar = True
            latency_src_name = src.name
            cols += ", `__ingest_ns` BIGINT"
            for _i in range(n_pipelines):
                ddl_statements.append(
                    f"CREATE TABLE {src.name}__p{_i} ({cols}) WITH ("
                    f"'connector' = 'filesystem', 'path' = '{data_path}', 'format' = 'csv-latency', "
                    f"'csv-latency.field-delimiter' = ',', 'csv-latency.ignore-parse-errors' = 'false')"
                )
        else:
            ddl_statements.append(
                f"CREATE TABLE {src.name} ({cols}) WITH ("
                f"'connector' = 'filesystem', 'path' = '{data_path}', 'format' = 'csv', "
                f"'csv.field-delimiter' = ',', 'csv.ignore-parse-errors' = 'false')"
            )

    # Sink — schema must match the SELECT output arity/types. Use the catalog sink_schema
    # (the declared query output) with generic identifiers; fall back to the first source
    # schema (matches `SELECT *` queries).
    #
    # flink_output_format selects the csv→json workload shape (feasibility probed 2026-07-13):
    #   none (default) — blackhole sink, NO output serialization (blackhole rejects a 'format'
    #                    option; it discards RowData). Parse-focused arm.
    #   json_object    — JSON serialization IN THE QUERY: wrap the SELECT in JSON_OBJECT(), emit
    #                    one STRING column into blackhole. True void sink, but this is Calcite's
    #                    per-row JSON-node builder, NOT Flink's optimized encoder — and it emits
    #                    keys in ALPHABETICAL order, not schema order. ~2.4x slower than
    #                    json_file in the feasibility probe.
    #   json_file      — filesystem sink with format='json' (Flink's real
    #                    JsonRowDataSerializationSchema, schema-ordered keys) writing to a tmpfs
    #                    dir that the driver deletes after timing. Writes real bytes (memcpy to
    #                    page cache) — closest analog to NES's Monitoring sink over a JSON
    #                    output formatter. JSON keys = the catalog sink_schema field names.
    #   json_blackhole — THE fair csv→json arm (2026-07-14): custom `blackhole-format` connector
    #                    (flink-ext/pbench-flink-ext.jar) that discovers Flink's real json
    #                    SerializationFormatFactory, serializes every RowData (byte-identical to
    #                    json_file minus the per-row '\n') and DISCARDS the bytes — native JSON
    #                    serialization, zero sink overhead. Subtasks print
    #                    `PBENCH_VOIDSINK bytes=N rows=M` on close (volume accounting).
    select_sql = query.flink_sql.strip().rstrip(";")
    sink_fields = query.sink_schema or (query.sources[0].schema_ if query.sources else [])
    # Flink DDL forbids duplicate column names (NES sink schemas may repeat a field, e.g.
    # q8_variant emits bid.timestamp AND auction.timestamp under the same label) — uniquify.
    _seen: dict[str, int] = {}
    _uniq_fields = []
    for f in sink_fields:
        n = _seen.get(f.name, 0)
        _seen[f.name] = n + 1
        _uniq_fields.append((f"{f.name}_{n + 1}" if n else f.name, f.type))
    sink_fields = [type("F", (), {"name": n, "type": t})() for n, t in _uniq_fields]
    ext_jars: list[str] = []
    fetch_io_size: int | None = None
    inserts: list[str] | None = None  # set by the latency arm (N pipelines); else derived from insert_sql
    if needs_ext_jar or out_fmt in ("json_blackhole", "latency", "latency_json"):
        ext_jars.append(_ext_jar_path())
    if latency_mode:
        # End-to-end per-batch LATENCY arm — the fair Flink analog of NES's MonitoringSink. The
        # `csv-latency` source format (see the source loop above) stamps an ingestion time at
        # read/parse start and appends it as a trailing __ingest_ns column; here it is threaded
        # through each pipeline's projection to a latency sink, which records arrival_ns - ingest_ns
        # over the full source→sink path (read + native parse + Flink's own source→task queue +
        # projection + sink). sample-every ≈ one NES TupleBuffer's worth of input
        # (operator_buffer_size / avg_row_bytes) → NES per-buffer sampling granularity.
        #
        # HETEROGENEOUS MIX: pipeline i runs pipeline_queries[i % len]'s flink_sql into its OWN
        # sink (each query's output schema differs), a round-robin of the workload = the analog of
        # NES's N concurrent File-source queries of mixed shape. serialize_json (latency_json) picks
        # the json-latency-blackhole sink, which runs Flink's real JSON encoder per row (fair
        # csv→json throughput) AND stamps latency; otherwise the passthrough latency-blackhole.
        if len(query.sources) != 1:
            raise ValueError(
                "flink_output_format: latency currently supports single-source (stateless) "
                f"queries only; '{query.name}' has {len(query.sources)} sources (threading the "
                "ingest column through a join/aggregation is not yet defined)."
            )
        qe_exec = (config.full_config.get("worker") or {}).get("default_query_execution") or {}
        buf_bytes = int(qe_exec.get("operator_buffer_size") or 131072)
        avg_row_bytes = (total_bytes / total_rows) if total_rows else 1.0
        sample_every = max(1, round(buf_bytes / avg_row_bytes)) if avg_row_bytes else 1
        fetch_io_size = buf_bytes  # align Flink's per-fetch batch to NES's TupleBuffer size
        sink_conn = ("'connector' = 'json-latency-blackhole', 'format' = 'json'"
                     if serialize_json else "'connector' = 'latency-blackhole'")
        inserts = []
        for _i in range(n_pipelines):
            q_i = pipeline_queries[_i % len(pipeline_queries)]
            if q_i.flink_sql is None:
                raise ValueError(
                    f"Query '{q_i.name}' (mix pipeline {_i}) has no flink_sql; cannot render.")
            select_i = q_i.flink_sql.strip().rstrip(";")
            fields_i = _uniquify_sink_fields(
                q_i.sink_schema or (q_i.sources[0].schema_ if q_i.sources else []))
            # Real (uniquified) field names so the JSON keys match the json_blackhole arm's bytes.
            sink_cols_i = ", ".join(f"`{n}` {_flink_type(t)}" for n, t in fields_i)
            sink_cols_i += ", `__ingest_ns` BIGINT"
            ddl_statements.append(
                f"CREATE TABLE pbench_sink__p{_i} ({sink_cols_i}) WITH ("
                f"{sink_conn}, 'sample-every' = '{sample_every}')"
            )
            # Point pipeline i's SELECT at its own source table `<src>__p{i}`; carry __ingest_ns
            # (injected by _inject_ingest_column) into that pipeline's sink. Anchor the rename on
            # FROM/JOIN so a source name that also appears inside a string literal (e.g. q22's
            # CONCAT('alert: bid on channel ', ...)) is NOT rewritten.
            base_insert_i = _inject_ingest_column(select_i)
            pipe_sql = re.sub(rf"(\b(?:from|join)\s+){re.escape(latency_src_name)}\b",
                              rf"\1{latency_src_name}__p{_i}", base_insert_i, flags=re.I)
            inserts.append(f"INSERT INTO pbench_sink__p{_i} {pipe_sql}")
        insert_sql = None  # multi-pipeline uses `inserts`; see the driver StatementSet
    elif out_fmt == "json_blackhole":
        sink_cols = ", ".join(f"`{f.name}` {_flink_type(f.type)}" for f in sink_fields)
        ddl_statements.append(
            f"CREATE TABLE pbench_sink ({sink_cols}) WITH ("
            f"'connector' = 'blackhole-format', 'format' = 'json')"
        )
        insert_sql = f"INSERT INTO pbench_sink {select_sql}"
    elif out_fmt == "json_object":
        ddl_statements.append("CREATE TABLE pbench_sink (`j` STRING) WITH ('connector' = 'blackhole')")
        col_aliases = ", ".join(f"c{i}" for i in range(len(sink_fields)))
        key_values = ", ".join(f"KEY '{f.name}' VALUE c{i}" for i, f in enumerate(sink_fields))
        insert_sql = (
            f"INSERT INTO pbench_sink SELECT JSON_OBJECT({key_values}) "
            f"FROM ({select_sql}) AS pbench_j({col_aliases})"
        )
    elif out_fmt == "json_file":
        sink_cols = ", ".join(f"`{f.name}` {_flink_type(f.type)}" for f in sink_fields)
        ddl_statements.append(
            f"CREATE TABLE pbench_sink ({sink_cols}) WITH ("
            f"'connector' = 'filesystem', 'path' = '__JSON_OUT_DIR__', 'format' = 'json')"
        )
        insert_sql = f"INSERT INTO pbench_sink {select_sql}"
    elif out_fmt == "none":
        sink_cols = ", ".join(f"`c{i}` {_flink_type(f.type)}" for i, f in enumerate(sink_fields))
        ddl_statements.append(
            f"CREATE TABLE pbench_sink ({sink_cols}) WITH ('connector' = 'blackhole')"
        )
        insert_sql = f"INSERT INTO pbench_sink {select_sql}"
    else:
        raise ValueError(
            f"Unknown flink_output_format '{out_fmt}' "
            "(expected none | json_object | json_file | json_blackhole | latency)."
        )
    # Count the query OUTPUT (not just the source) so filters/joins are handled correctly; for a
    # filterless projection this equals the input line count. In latency mode the base source table
    # was replaced by per-pipeline copies, so count pipeline 0's source.
    count_sql = f"SELECT COUNT(*) AS c FROM ({select_sql}) AS pbench_q"
    if latency_mode and latency_src_name:
        count_sql = re.sub(rf"(\b(?:from|join)\s+){re.escape(latency_src_name)}\b",
                           rf"\1{latency_src_name}__p0", count_sql, flags=re.I)

    worker = config.full_config.get("worker") or {}
    qe = worker.get("query_engine") or {}
    exec_mode = str(config.full_config.get("flink_execution_mode", "streaming")).lower()
    if latency_mode:
        # N single-threaded pipelines run concurrently: each at parallelism 1 (a single
        # non-splittable CSV is one reader anyway), with N task slots so all N run at once. This
        # is the analog of NES's N concurrent File-source queries; total input = N × file bytes.
        inserts_final = inserts or []
        parallelism = 1
        num_slots: int | None = n_pipelines
        size_bytes = total_bytes * n_pipelines
    else:
        inserts_final = [insert_sql]
        parallelism = int(qe.get("number_of_worker_threads")
                          or config.full_config.get("flink_parallelism") or 1)
        num_slots = None
        size_bytes = total_bytes

    ddl_literal = "[\n" + "".join(f"    {s!r},\n" for s in ddl_statements) + "]"
    inserts_literal = "[\n" + "".join(f"    {s!r},\n" for s in inserts_final) + "]"
    return _FLINK_DRIVER_TEMPLATE.format(
        ddl_literal=ddl_literal,
        inserts_literal=inserts_literal,
        count_sql=repr(count_sql),
        size_bytes=size_bytes,
        parallelism=parallelism,
        num_slots=repr(num_slots),
        exec_mode=repr(exec_mode),
        ext_jars=repr(ext_jars),
        fetch_io_size=repr(fetch_io_size),
        extra_conf=repr(dict(config.full_config.get("flink_config") or {})),
    )


# ── Spark rendering ───────────────────────────────────────────────────────────

def _spark_type(nes_type: str) -> str:
    return _NES_TO_SPARK_TYPE.get(nes_type.upper(), "STRING")


# Self-contained PySpark Structured Streaming driver. Executed as a subprocess by SparkRunner
# (fresh JVM per rep). No literal curly braces below except the .format() placeholders — keep it
# that way (SOURCES is injected as a dict repr; the body uses [] and dict indexing, %-formatting
# instead of f-strings, and a 2-element list accumulator instead of a {} literal).
_SPARK_DRIVER_TEMPLATE = '''"""Auto-generated by piparse-bench render_spark — PySpark Structured Streaming driver.

Runs a query against Apache Spark Structured Streaming. Each source is a file-streaming CSV
reader (one temp view per source, explicit schema); the hand-authored spark_sql is the SELECT.
Default (json_void): serialize the query output to JSON through a FAIR void sink — Spark's real
row->JSON encoder (to_json == JacksonGenerator) runs per row in the executors, the UTF-8 byte
length is summed and the bytes are discarded (only two scalars per micro-batch return to the
driver; no disk, no full collect). Prints BYTES (input bytes) / ELAPSED / WALL and a
PBENCH_VOIDSINK line. With --count: batch-read the same input, run the same SQL, print ROWS
(output tuple count) for the correctness gate — mirrors the Flink driver's batch --count path.
"""
import os
import sys
import time

# Driver heap MUST be set before the JVM launches (local mode = one JVM for driver+executor);
# SparkSession.config("spark.driver.memory") is too late. Windowed joins/aggregations otherwise
# OOM the ~1 GB default heap. PYSPARK_SUBMIT_ARGS is read by the pyspark launcher at gateway start.
os.environ["PYSPARK_SUBMIT_ARGS"] = {submit_args!r}

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

SOURCES = {sources_literal}
SPARK_SQL = {spark_sql!r}
OUTPUT_FORMAT = {output_format!r}
OUTPUT_MODE = {output_mode!r}
PARALLELISM = {parallelism}
SIZE_BYTES = {size_bytes}
MAX_BYTES_PER_TRIGGER = {max_bytes_per_trigger!r}
MAX_PARTITION_BYTES = {max_partition_bytes!r}

_count_mode = "--count" in sys.argv

# local[N] = N worker threads (the same knob that drives NES worker threads / Flink parallelism).
# By default a single large CSV is split by spark.sql.files.maxPartitionBytes (128 MiB) into
# row-aligned tasks, so Spark PARALLEL-reads one file across local[N]. That offset-based file
# splitting is a batch/file capability (needs file size), not a streaming one, and Flink is held
# to a single split — so for a fair streaming comparison MAX_PARTITION_BYTES >= file size makes the
# file ONE split = a single sequential reader (Spark couples read+parse, so this is single-threaded
# ingest, matching Flink; NES stays 1 read -> N parse).
_builder = (SparkSession.builder
            .master("local[%d]" % PARALLELISM)
            .appName("pbench_spark")
            .config("spark.ui.enabled", "false")
            .config("spark.sql.shuffle.partitions", str(PARALLELISM))
            .config("spark.sql.streaming.numRecentProgressUpdates", "2000")
            .config("spark.scheduler.mode", "FAIR")
            .config("spark.sql.session.timeZone", "UTC"))
if MAX_PARTITION_BYTES is not None:
    # maxPartitionBytes alone can't drop below the file-scan floor (minPartitionNum = default
    # parallelism = local[N]); set minPartitionNum=1 too so a single file becomes exactly ONE
    # split = a genuine single sequential reader (parity with Flink's single local-file split).
    _builder = (_builder
                .config("spark.sql.files.maxPartitionBytes", str(MAX_PARTITION_BYTES))
                .config("spark.sql.files.minPartitionNum", "1"))
spark = _builder.getOrCreate()
spark.sparkContext.setLogLevel("ERROR")


def _register_sources(streaming):
    _reader = spark.readStream if streaming else spark.read
    for _name in SOURCES:
        _s = SOURCES[_name]
        _r = (_reader
              .schema(_s["schema_ddl"])
              .option("header", "false")
              .option("delimiter", ",")
              .option("mode", "FAILFAST")
              .option("pathGlobFilter", _s["glob"]))
        if streaming and MAX_BYTES_PER_TRIGGER is not None:
            _r = _r.option("maxBytesPerTrigger", str(MAX_BYTES_PER_TRIGGER))
        _df = _r.csv(_s["path_dir"])
        _rt = _s.get("rowtime")
        if _rt is not None:
            # raw value is ms-epoch -> TIMESTAMP with ms precision (NES TimeMeasure is ms).
            _df = _df.withColumn("__rt", F.expr("timestamp_millis(`%s`)" % _rt["column"]))
            if streaming:
                _df = _df.withWatermark("__rt", _rt["delay"])
        _df.createOrReplaceTempView(_name)


if _count_mode:
    # Batch mode: no watermark (all windows kept) = the ground-truth output count, same as the
    # Flink driver forcing BATCH for --count.
    _register_sources(False)
    _n = spark.sql("SELECT COUNT(*) AS c FROM (%s) AS pbench_q" % SPARK_SQL).first()["c"]
    print("ROWS=%d" % int(_n))
    spark.stop()
    sys.exit(0)


_register_sources(True)
_out = spark.sql(SPARK_SQL)

_acc = [0, 0]  # [bytes, rows] accumulated across micro-batches


def _void(_bdf, _bid):
    if OUTPUT_FORMAT == "json_void":
        _cols = _bdf.columns
        _agg = _bdf.select(
            F.sum(F.octet_length(F.to_json(F.struct(*[_bdf[_c] for _c in _cols])))).alias("b"),
            F.count(F.lit(1)).alias("r"),
        ).first()
        _acc[0] += int(_agg["b"] or 0)
        _acc[1] += int(_agg["r"] or 0)
    else:
        # Parse-only arm: run the projection/filter, discard via the noop sink (no serialization).
        _bdf.write.format("noop").mode("append").save()


# Warm the JVM / Catalyst OUTSIDE the timed region (mirrors the Flink driver's SELECT 1 warmup).
spark.range(1).count()

# CHECKPOINT FAIRNESS (see the scaling template): Flink runs with checkpointing disabled; Spark
# Structured Streaming can't fully disable its offset/commit log, but our queries are stateless, so
# put the checkpoint on tmpfs (RAM) to remove the disk-I/O confound (else Spark uses an implicit
# local-disk temp dir).
import tempfile
import shutil
_ckpt_root = tempfile.mkdtemp(prefix="pbench_spark_ckpt_",
                              dir=("/dev/shm" if os.path.isdir("/dev/shm") else None))

_t0 = time.perf_counter()
_q = (_out.writeStream
      .foreachBatch(_void)
      .option("checkpointLocation", _ckpt_root + "/q0")
      .outputMode(OUTPUT_MODE)
      .trigger(availableNow=True)
      .start())
_q.awaitTermination()
_elapsed = time.perf_counter() - _t0
shutil.rmtree(_ckpt_root, ignore_errors=True)

print("BYTES=%d" % SIZE_BYTES)
print("ELAPSED=%.6f" % _elapsed)
print("WALL=%.6f" % _elapsed)
print("PBENCH_VOIDSINK bytes=%d rows=%d" % (_acc[0], _acc[1]))
spark.stop()
'''


def render_spark(config: RunConfig, query: QueryCatalogEntry, testdata_dir: str | None = None,
                 **kwargs) -> str:
    """Render a self-contained PySpark Structured Streaming driver script (str) for `spark` mode.

    One file-streaming CSV source table per query source (schema from the catalog), the
    hand-authored `query.spark_sql` as the SELECT, and a fair JSON void sink (to_json per row,
    bytes discarded). Parallelism = NES worker-thread count. Output format via
    `spark_output_format`: json_void (default, the fair csv->json arm) | none (parse-only)."""
    # Scaling / heterogeneous-mix path: N concurrent Structured Streaming queries in one session
    # (num_queries), round-robining query_mix over per-stream views. Kept as a separate function so
    # the single-query (incl. multi-source stateful join) path below is untouched.
    if config.query_mix or (config.num_queries or 1) > 1:
        return _render_spark_scaling(config, query, testdata_dir)
    if query.spark_sql is None:
        raise ValueError(f"Query '{query.name}' has no spark_sql; cannot render for mode 'spark'.")
    if testdata_dir is None:
        raise ValueError("render_spark requires testdata_dir (the root holding the CSV data files).")

    out_fmt = str(config.full_config.get("spark_output_format", "json_void")).lower()
    if out_fmt not in ("json_void", "none"):
        raise ValueError(
            f"Unknown spark_output_format '{out_fmt}' (expected json_void | none)."
        )

    sources: dict[str, Any] = {}
    total_bytes = 0
    windowed = False
    for src in query.sources:
        entry = _resolve_data_entry(src, config.data_size)
        if entry is None:
            raise ValueError(
                f"Source '{src.name}' has no data entry for size '{config.data_size}'."
            )
        data_path = Path(testdata_dir) / entry.file
        total_bytes += entry.size_bytes
        # Backtick every column (quote reserved words like `timestamp`); STRING for VARSIZED.
        schema_ddl = ", ".join(f"`{f.name}` {_spark_type(f.type)}" for f in src.schema_)
        s: dict[str, Any] = {
            "path_dir": str(data_path.parent),
            "glob": data_path.name,
            "schema_ddl": schema_ddl,
        }
        if src.spark_rowtime is not None:
            windowed = True
            s["rowtime"] = {"column": src.spark_rowtime.column, "delay": src.spark_rowtime.delay}
        sources[src.name] = s

    qe = (config.full_config.get("worker") or {}).get("query_engine") or {}
    parallelism = int(qe.get("number_of_worker_threads")
                      or config.full_config.get("spark_parallelism") or 1)
    # Windowed queries (aggregations + stream-stream joins) run in MULTIPLE time-ordered
    # micro-batches via maxBytesPerTrigger, so the watermark advances between batches and evicts
    # closed-window / expired-join state — bounded memory (a single availableNow batch over the
    # whole file keeps ALL state at once and OOMs), and the faithful streaming analog of Flink/NES
    # incremental eviction. With multi-batch, "append" emits each closed window / join match
    # exactly ONCE (fair serialization; "update" would re-serialize a window every batch it changes
    # -> inflated cost). Stateless projections stay single-batch append (no state). All overridable
    # via spark_output_mode / spark_max_bytes_per_trigger.
    output_mode = str(config.full_config.get("spark_output_mode") or "append")
    max_bytes = config.full_config.get("spark_max_bytes_per_trigger")
    if max_bytes is None and windowed:
        max_bytes = 256 * 1024 * 1024  # 256 MiB/batch -> incremental watermark eviction
    # Single-reader (fair streaming, parity with Flink): spark_single_reader=true sets
    # spark.sql.files.maxPartitionBytes above the total input size so each file is ONE split (no
    # offset-based parallel read). Explicit spark_max_partition_bytes overrides. None = Spark
    # default (128 MiB splits = parallel read of one file).
    max_partition_bytes = config.full_config.get("spark_max_partition_bytes")
    if max_partition_bytes is None and config.full_config.get("spark_single_reader"):
        max_partition_bytes = total_bytes + (1 << 20)  # > whole input -> one split per file
    # Local mode = one JVM for driver+executor; size its heap generously (windowed state + the
    # per-batch output rows + JSON serialization). Overridable via spark_driver_memory.
    driver_mem = str(config.full_config.get("spark_driver_memory") or "32g")
    submit_args = f"--driver-memory {driver_mem} --conf spark.driver.maxResultSize=2g pyspark-shell"

    return _SPARK_DRIVER_TEMPLATE.format(
        submit_args=submit_args,
        sources_literal=repr(sources),
        spark_sql=query.spark_sql.strip().rstrip(";"),
        output_format=out_fmt,
        output_mode=output_mode,
        parallelism=parallelism,
        size_bytes=total_bytes,
        # pass raw values (None or int); the template's !r conversion does the repr. Pre-repr'ing
        # here would double-repr None into the string 'None' (fatal for the .config() byte-parser).
        max_partition_bytes=max_partition_bytes,
        max_bytes_per_trigger=max_bytes,
    )


# Self-contained PySpark Structured Streaming driver for the SCALING / heterogeneous-mix arm:
# NUM_STREAMS concurrent streaming queries in one SparkSession (num_queries), round-robining
# MIX_SQLS. Single-source only. No literal { } braces below except the .format() placeholders and
# the doubled {{ }} literals.
_SPARK_SCALING_TEMPLATE = '''"""Auto-generated by piparse-bench _render_spark_scaling — N concurrent Structured Streaming queries.

NUM_STREAMS concurrent streaming queries in ONE SparkSession (the analog of NES's N concurrent
File-source queries and Flink's N StatementSet pipelines): all share the local[N] executor and the
driver's micro-batch scheduler. Each stream is an independent file-streaming CSV reader over the
same warm input, registered as a per-stream view `<src>__p{{i}}`; MIX_SQLS (the heterogeneous
workload) is round-robined across the streams, each query's source reference rewritten to its view.

json_void: serialize each stream's output to JSON through a FAIR void sink (Spark's real to_json ==
JacksonGenerator runs per row in the executors; UTF-8 byte length summed, bytes discarded).
json_void_latency ADDS per-micro-batch latency via a StreamingQueryListener (triggerExecution ms
per batch = Spark's per-batch analog of NES's per-TupleBuffer / Flink's per-readBatch stamp; a
Spark micro-batch is necessarily coarser than 128 KiB — an architectural property set by
maxBytesPerTrigger, not a bug). Prints BYTES (aggregate input bytes) / ELAPSED / WALL,
PBENCH_VOIDSINK, and (latency) one PBENCH_LATENCY_SAMPLES line per query. With --count: batch-read
MIX_SQLS[0] and print ROWS for the correctness gate.
"""
import os
import re
import sys
import time
import json

os.environ["PYSPARK_SUBMIT_ARGS"] = {submit_args!r}

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

SOURCE = {source_literal}
SRC_NAME = {src_name!r}
MIX_SQLS = {mix_sqls_literal}
NUM_STREAMS = {num_streams}
OUTPUT_FORMAT = {output_format!r}
OUTPUT_MODE = {output_mode!r}
PARALLELISM = {parallelism}
SIZE_BYTES = {size_bytes}
MAX_BYTES_PER_TRIGGER = {max_bytes_per_trigger!r}
MAX_PARTITION_BYTES = {max_partition_bytes!r}

_count_mode = "--count" in sys.argv
_latency = OUTPUT_FORMAT == "json_void_latency"

_builder = (SparkSession.builder
            .master("local[%d]" % PARALLELISM)
            .appName("pbench_spark")
            .config("spark.ui.enabled", "false")
            .config("spark.sql.shuffle.partitions", str(PARALLELISM))
            .config("spark.sql.streaming.numRecentProgressUpdates", "2000")
            .config("spark.scheduler.mode", "FAIR")
            .config("spark.sql.session.timeZone", "UTC"))
if MAX_PARTITION_BYTES is not None:
    _builder = (_builder
                .config("spark.sql.files.maxPartitionBytes", str(MAX_PARTITION_BYTES))
                .config("spark.sql.files.minPartitionNum", "1"))
spark = _builder.getOrCreate()
spark.sparkContext.setLogLevel("ERROR")


def _register(view, streaming):
    # Custom source: the file-microbatch-csv DataSource V2 reads the ONE file as byte-range
    # micro-batches (real per-batch latencies). It only implements MICRO_BATCH_READ, so the
    # non-streaming --count path always falls back to the native batch CSV reader below.
    if streaming and SOURCE.get("source_kind") == "file-microbatch-csv":
        _df = (spark.readStream
               .format("file-microbatch-csv")
               .schema(SOURCE["schema_ddl"])
               .option("path", SOURCE["path_file"])
               .option("maxBytesPerTrigger", str(SOURCE["batch_bytes"]))
               .load())
    else:
        _reader = spark.readStream if streaming else spark.read
        _r = (_reader
              .schema(SOURCE["schema_ddl"])
              .option("header", "false")
              .option("delimiter", ",")
              .option("mode", "FAILFAST")
              .option("pathGlobFilter", SOURCE["glob"]))
        if streaming and MAX_BYTES_PER_TRIGGER is not None:
            _r = _r.option("maxBytesPerTrigger", str(MAX_BYTES_PER_TRIGGER))
        _df = _r.csv(SOURCE["path_dir"])
    _rt = SOURCE.get("rowtime")
    if _rt is not None:
        _df = _df.withColumn("__rt", F.expr("timestamp_millis(`%s`)" % _rt["column"]))
        if streaming:
            _df = _df.withWatermark("__rt", _rt["delay"])
    _df.createOrReplaceTempView(view)


def _point_at(sql, view):
    # Rewrite the source reference (after FROM/JOIN) to this stream's view WITHOUT touching a source
    # name that also appears inside a string literal (e.g. q22's CONCAT('alert: bid on channel ')).
    return re.sub(r"(\\b(?:from|join)\\s+)" + re.escape(SRC_NAME) + r"\\b",
                  lambda m: m.group(1) + view, sql, flags=re.I)


if _count_mode:
    _register(SRC_NAME, False)
    _n = spark.sql("SELECT COUNT(*) AS c FROM (%s) AS pbench_q" % MIX_SQLS[0]).first()["c"]
    print("ROWS=%d" % int(_n))
    spark.stop()
    sys.exit(0)

_accs = [[0, 0] for _ in range(NUM_STREAMS)]


def _make_void(_idx):
    def _void(_bdf, _bid):
        if OUTPUT_FORMAT in ("json_void", "json_void_latency"):
            _cols = _bdf.columns
            _agg = _bdf.select(
                F.sum(F.octet_length(F.to_json(F.struct(*[_bdf[_c] for _c in _cols])))).alias("b"),
                F.count(F.lit(1)).alias("r"),
            ).first()
            _accs[_idx][0] += int(_agg["b"] or 0)
            _accs[_idx][1] += int(_agg["r"] or 0)
        else:
            _bdf.write.format("noop").mode("append").save()
    return _void


# Warm the JVM / Catalyst OUTSIDE the timed region (mirrors the Flink driver's SELECT 1 warmup).
spark.range(1).count()

# CHECKPOINT FAIRNESS: Flink runs with checkpointing DISABLED (max throughput). Spark Structured
# Streaming CANNOT fully disable its offset/commit log -- it is how progress is tracked -- but our
# queries are STATELESS (no state store), so it is only ~2 tiny writes per single-batch stream.
# Point every query's checkpoint at a tmpfs dir so those writes are RAM-backed (zero disk-I/O
# confound) and ephemeral -- the closest fair analog to Flink's disabled checkpointing. (If no
# checkpointLocation is set, Spark silently uses an implicit temp dir on LOCAL DISK instead.)
import tempfile
import shutil
_ckpt_root = tempfile.mkdtemp(prefix="pbench_spark_ckpt_",
                              dir=("/dev/shm" if os.path.isdir("/dev/shm") else None))

_t0 = time.perf_counter()
_queries = []
for _i in range(NUM_STREAMS):
    _view = SRC_NAME + "__p%d" % _i
    _register(_view, True)
    _sql = _point_at(MIX_SQLS[_i % len(MIX_SQLS)], _view)
    _out = spark.sql(_sql)
    _q = (_out.writeStream
          .foreachBatch(_make_void(_i))
          .option("checkpointLocation", _ckpt_root + "/q%d" % _i)
          .outputMode(OUTPUT_MODE)
          .trigger(availableNow=True)
          .start())
    _queries.append(_q)
for _q in _queries:
    _q.awaitTermination()
_elapsed = time.perf_counter() - _t0
shutil.rmtree(_ckpt_root, ignore_errors=True)

_bytes = sum(_a[0] for _a in _accs)
_rows = sum(_a[1] for _a in _accs)
print("BYTES=%d" % SIZE_BYTES)
print("ELAPSED=%.6f" % _elapsed)
print("WALL=%.6f" % _elapsed)
print("PBENCH_VOIDSINK bytes=%d rows=%d" % (_bytes, _rows))
if _latency:
    # Per-micro-batch latency is read SYNCHRONOUSLY from each query's recentProgress after it
    # terminates (the async StreamingQueryListener can drop events before spark.stop()). Each
    # batch's triggerExecution (ms) is the Spark per-batch analog of NES's per-TupleBuffer /
    # Flink's per-readBatch stamp; under N concurrent streams contending for the shared local[N]
    # executor it stretches (the collapse signal). Pooled across all streams (ms -> ns).
    _pooled = []
    for _q in _queries:
        for _prog in _q.recentProgress:
            try:
                # recentProgress elements are dicts in PySpark 3.5 (older builds returned objects
                # with a .json property) — handle both.
                _p = _prog if isinstance(_prog, dict) else json.loads(_prog.json)
                _dur = (_p.get("durationMs") or {{}}).get("triggerExecution")
                if _dur is not None and (_p.get("numInputRows") or 0) > 0:
                    _pooled.append(int(float(_dur) * 1e6))
            except Exception:
                pass
    if _pooled:
        print("PBENCH_LATENCY_SAMPLES " + ",".join("%d" % _s for _s in _pooled))
spark.stop()
'''


def _render_spark_scaling(config: RunConfig, query: QueryCatalogEntry,
                          testdata_dir: str | None) -> str:
    """Render the N-concurrent-stream Spark driver (num_queries streams, query_mix round-robin).

    Single-source only (the stateless scaling workload). All mix queries must share the same one
    source (they all read `bid`); each stream reads it independently as a per-stream view."""
    if testdata_dir is None:
        raise ValueError("render_spark requires testdata_dir (the root holding the CSV data files).")
    out_fmt = str(config.full_config.get("spark_output_format", "json_void")).lower()
    if out_fmt not in ("json_void", "json_void_latency", "none"):
        raise ValueError(
            f"Unknown spark_output_format '{out_fmt}' (expected json_void | json_void_latency | none).")

    # Resolve the query mix (round-robin list) and require single-source with a shared source name.
    mix_queries = ([load_query(r) for r in config.query_mix] if config.query_mix else [query])
    src_names = {q.sources[0].name for q in mix_queries if q.sources}
    if any(len(q.sources) != 1 for q in mix_queries) or len(src_names) != 1:
        raise ValueError(
            "spark scaling (num_queries>1 / query_mix) supports single-source queries sharing one "
            f"source name; got sources {[[s.name for s in q.sources] for q in mix_queries]}.")
    for q in mix_queries:
        if q.spark_sql is None:
            raise ValueError(f"Query '{q.name}' has no spark_sql; cannot render for the spark mix.")

    primary_src = mix_queries[0].sources[0]
    entry = _resolve_data_entry(primary_src, config.data_size)
    if entry is None:
        raise ValueError(
            f"Source '{primary_src.name}' has no data entry for size '{config.data_size}'.")
    data_path = Path(testdata_dir) / entry.file
    schema_ddl = ", ".join(f"`{f.name}` {_spark_type(f.type)}" for f in primary_src.schema_)
    # Streaming source: native `csv` (built-in file source, whole-file micro-batch) or the custom
    # `file-microbatch-csv` DataSource V2 (byte-range micro-batches of the ONE file -> real
    # per-batch latencies). spark_batch_bytes sets the custom source's per-batch size (default 4 MiB).
    spark_source = str(config.full_config.get("spark_source") or "csv").lower()
    if spark_source not in ("csv", "file-microbatch-csv"):
        raise ValueError(
            f"Unknown spark_source '{spark_source}' (expected csv | file-microbatch-csv).")
    batch_bytes = int(config.full_config.get("spark_batch_bytes") or (4 * 1024 * 1024))
    source: dict[str, Any] = {
        "path_dir": str(data_path.parent),
        "glob": data_path.name,
        "schema_ddl": schema_ddl,
        "source_kind": spark_source,
        "path_file": str(data_path),
        "batch_bytes": batch_bytes,
    }
    if primary_src.spark_rowtime is not None:
        source["rowtime"] = {"column": primary_src.spark_rowtime.column,
                             "delay": primary_src.spark_rowtime.delay}

    num_streams = int(config.num_queries or 1)
    total_bytes = entry.size_bytes * num_streams  # aggregate input across all concurrent streams

    qe = (config.full_config.get("worker") or {}).get("query_engine") or {}
    # Prefer an explicit spark_parallelism (lets the scaling arm give Spark all 24 physical cores,
    # matching NES's 16 workers + 8 io and Flink's up-to-24 slots, instead of NES's worker count).
    parallelism = int(config.full_config.get("spark_parallelism")
                      or qe.get("number_of_worker_threads") or 1)
    output_mode = str(config.full_config.get("spark_output_mode") or "append")
    # Latency needs MULTIPLE time-ordered micro-batches to yield per-batch samples; maxBytesPerTrigger
    # sets the batch size. Spark cannot do 128 KiB micro-batches at scale (per-batch driver overhead),
    # so the latency batch is necessarily coarser than NES/Flink's 128 KiB — an honest micro-batch
    # property, documented, and tunable via spark_max_bytes_per_trigger. Default 16 MiB.
    max_bytes = config.full_config.get("spark_max_bytes_per_trigger")
    if max_bytes is None and out_fmt == "json_void_latency":
        max_bytes = 16 * 1024 * 1024
    # Single-reader per stream (fair: a sequential reader, no offset-splitting) when requested.
    max_partition_bytes = config.full_config.get("spark_max_partition_bytes")
    if max_partition_bytes is None and config.full_config.get("spark_single_reader"):
        max_partition_bytes = entry.size_bytes + (1 << 20)
    driver_mem = str(config.full_config.get("spark_driver_memory") or "32g")
    jars_arg = ""
    if spark_source == "file-microbatch-csv":
        jars_arg = f"--jars {_spark_ext_jar_path()} "
    submit_args = (
        f"{jars_arg}--driver-memory {driver_mem} --conf spark.driver.maxResultSize=2g pyspark-shell")

    return _SPARK_SCALING_TEMPLATE.format(
        submit_args=submit_args,
        source_literal=repr(source),
        src_name=primary_src.name,
        mix_sqls_literal=repr([q.spark_sql.strip().rstrip(";") for q in mix_queries]),
        num_streams=num_streams,
        output_format=out_fmt,
        output_mode=output_mode,
        parallelism=parallelism,
        size_bytes=total_bytes,
        max_bytes_per_trigger=max_bytes,
        max_partition_bytes=max_partition_bytes,
    )


# ── Dispatch ──────────────────────────────────────────────────────────────────

def render_for_config(config: RunConfig, **kwargs) -> str:
    """Load query catalog entry and render for the given config's mode."""
    query = load_query(config.query_ref)

    if config.mode in ("systest", "bench-runner"):
        # bench-runner consumes the identical .test format as systest.
        return render_systest(config, query)

    if config.mode == "worker-cli":
        return render_worker_cli_yaml(config, query, **kwargs)

    if config.mode == "flink":
        return render_flink(config, query, **kwargs)

    if config.mode == "spark":
        return render_spark(config, query, **kwargs)

    raise NotImplementedError(f"Rendering for mode '{config.mode}' not yet implemented")
