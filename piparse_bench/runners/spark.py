"""Spark runner: render a PySpark Structured Streaming driver, run it as a subprocess, parse metrics."""

from __future__ import annotations

import os
import re
import shutil
import subprocess
import sys
import tempfile
from pathlib import Path

from ..models import RunConfig, RunResult
from ..renderer import _resolve_data_entry, render_for_config
from ..resolver import compute_input_rows, load_query
from .base import Runner


def _spark_parallelism(config: RunConfig) -> int:
    """Spark job parallelism (recorded in the worker_threads slot). Prefer an explicit
    `spark_parallelism` (the scaling arm gives Spark all 24 cores), else the NES worker-thread
    count (head-to-head, one knob drives all engines), else 1."""
    qe = (config.full_config.get("worker") or {}).get("query_engine") or {}
    return int(config.full_config.get("spark_parallelism")
               or qe.get("number_of_worker_threads") or 1)


class SparkRunner(Runner):
    """Runner for Apache Spark Structured Streaming via PySpark.

    Renders a self-contained PySpark driver script (see renderer.render_spark) and runs it in a
    fresh subprocess per rep — one cold JVM each, mirroring NES's re-exec-per-rep and the Flink
    runner's fairness. Structured Streaming compiles the DataFrame/SQL plan to JVM operators, so
    there is NO per-record Python in the data path. Metrics (BYTES/ELAPSED/ROWS) are parsed from
    the driver's stdout; all NES-specific RunResult columns stay NULL.

    NOTE (architecture, state in the paper): Spark Structured Streaming is MICRO-BATCH, not
    record-at-a-time like Flink/NES. Expect it to favor throughput and hurt latency tails.
    """

    _MARKER_RE = re.compile(r"^(BYTES|ELAPSED|WALL|ROWS)=([0-9.]+)\s*$", re.MULTILINE)
    # One line per streaming query (spark_output_format: json_void_latency). Samples are per-micro-
    # batch triggerExecution times in NANOSECONDS (the driver converts its ms → ns), same protocol
    # as the Flink latency arm so the runner can POOL exact percentiles across all concurrent streams.
    _SAMPLES_RE = re.compile(r"^PBENCH_LATENCY_SAMPLES (.+)$", re.MULTILINE)

    @classmethod
    def _parse_latency(cls, stdout: str) -> dict[str, float] | None:
        """Pool every raw per-batch sample (ns) across ALL concurrent streams' PBENCH_LATENCY_SAMPLES
        lines and compute exact percentiles over the pooled set (median-of-per-stream-percentiles
        would be wrong) — mirrors FlinkRunner._parse_latency. Latencies returned in microseconds."""
        pooled: list[int] = []
        for m in cls._SAMPLES_RE.finditer(stdout):
            for tok in m.group(1).split(","):
                tok = tok.strip()
                if tok:
                    pooled.append(int(tok))
        if not pooled:
            return None
        pooled.sort()
        ns_to_us = 1.0 / 1000.0
        n = len(pooled)

        def _pct(p: float) -> float:
            idx = min(int(p * (n - 1) + 0.5), n - 1)
            return pooled[idx] * ns_to_us

        return {
            "min_latency_us": pooled[0] * ns_to_us,
            "max_latency_us": pooled[-1] * ns_to_us,
            "avg_latency_us": sum(pooled) / n * ns_to_us,
            "p50_latency_us": _pct(0.50),
            "p99_latency_us": _pct(0.99),
            "p999_latency_us": _pct(0.999),
            "p99_latency_us_worst": _pct(0.99),
        }

    def __init__(self, testdata_dir: str | Path | None = None,
                 python_bin: str | None = None, timeout: int = 900):
        self.testdata_dir = Path(testdata_dir) if testdata_dir else None
        # Interpreter that has pyspark installed. Defaults to the pbench interpreter itself
        # (pyspark co-installs into the same venv); override with --spark-python.
        self.python_bin = python_bin or sys.executable
        # High-N / windowed runs can take minutes; PBENCH_SPARK_TIMEOUT overrides the per-run
        # subprocess timeout (seconds), twin of the Flink runner's PBENCH_FLINK_TIMEOUT.
        self.timeout = int(os.environ.get("PBENCH_SPARK_TIMEOUT") or timeout)
        self._pyspark_ok: bool | None = None  # memoized (validate runs once per config)

    def _pyspark_importable(self) -> bool:
        if self._pyspark_ok is None:
            try:
                r = subprocess.run(
                    [self.python_bin, "-c", "import pyspark"],
                    capture_output=True, text=True, timeout=120,
                )
                self._pyspark_ok = r.returncode == 0
            except Exception:
                self._pyspark_ok = False
        return self._pyspark_ok

    def validate(self, config: RunConfig) -> list[str]:
        errors: list[str] = []
        if config.mode != "spark":
            errors.append(f"SparkRunner requires mode='spark', got '{config.mode}'")
            return errors
        if self.testdata_dir is None:
            errors.append(
                "SparkRunner needs a testdata dir: pass --testdata-dir, or --build-dir "
                "(then it defaults to <build-dir>/nes-systests/testdata)."
            )
        if not self._pyspark_importable():
            errors.append(
                f"pyspark not importable with '{self.python_bin}'. Install it "
                "(uv pip install 'pyspark>=3.5,<4') or pass --spark-python <interpreter>."
            )
        if shutil.which("java") is None and not os.environ.get("JAVA_HOME"):
            errors.append(
                "Java not found (no `java` on PATH and JAVA_HOME unset); PySpark needs a JVM "
                "(Spark 3.5 supports Java 8/11/17)."
            )
        try:
            query = load_query(config.query_ref)
        except Exception as e:
            errors.append(f"Could not load query '{config.query_ref}': {e}")
            return errors
        if query.spark_sql is None:
            errors.append(
                f"Query '{config.query_ref}' has no `spark_sql`; add a Spark-dialect SELECT to "
                "its catalog YAML to run it with mode: spark."
            )
        if self.testdata_dir is not None:
            for src in query.sources:
                entry = _resolve_data_entry(src, config.data_size)
                if entry is None:
                    errors.append(
                        f"Source '{src.name}' has no data entry for size '{config.data_size}'."
                    )
                    continue
                path = self.testdata_dir / entry.file
                if not path.exists():
                    errors.append(f"Data file not found: {path}")
        return errors

    def _warm_inputs(self, config: RunConfig) -> None:
        """Pull each source's data file into the page cache before the timed run, so Spark reads
        from warm cache — parity with NES's worker-cli warm_read (the Spark file source itself has
        no warm step). Gated on warm_read; prints a residency line to the pbench log."""
        if not config.full_config.get("warm_read") or self.testdata_dir is None:
            return
        from ..coldcache import warm_file
        try:
            query = load_query(config.query_ref)
        except Exception:
            return
        for src in query.sources:
            entry = _resolve_data_entry(src, config.data_size)
            if entry is None:
                continue
            path = self.testdata_dir / entry.file
            if not path.exists():
                continue
            try:
                res = warm_file(path)
                print(f"  [warm_read] {'WARM' if res.fully_warm else 'PARTIAL'} {path.name}: "
                      f"{res.resident_after_mb:.0f}/{res.total_mb:.0f} MB resident")
            except Exception as e:  # never let warming abort the run
                print(f"  [warm_read] WARN could not warm {path.name}: {e}")

    def execute(self, config: RunConfig) -> RunResult:
        self._warm_inputs(config)
        driver = render_for_config(config, testdata_dir=str(self.testdata_dir))

        with tempfile.NamedTemporaryFile(
            mode="w", suffix="_spark.py", delete=False, prefix="pbench_"
        ) as f:
            f.write(driver)
            driver_path = Path(f.name)

        keep_logs = bool(os.environ.get("PBENCH_KEEP_LOGS"))
        try:
            result = subprocess.run(
                [self.python_bin, str(driver_path)],
                capture_output=True, text=True, timeout=self.timeout, env={**os.environ},
            )
            if keep_logs:
                # Driver stdout carries the PBENCH_VOIDSINK bytes=/rows= line (serialized output
                # volume) — keep it next to the driver script.
                log_path = driver_path.with_suffix(".stdout")
                log_path.write_text(result.stdout)
            markers = {
                m.group(1): float(m.group(2))
                for m in self._MARKER_RE.finditer(result.stdout)
            }
            if result.returncode != 0 or "ELAPSED" not in markers:
                raise RuntimeError(
                    f"Spark driver failed (exit {result.returncode}, no ELAPSED marker):\n"
                    f"python: {self.python_bin}\n"
                    f"driver: {driver_path}\n"
                    f"stdout: {result.stdout[-2000:]}\n"
                    f"stderr: {result.stderr[-2000:]}"
                )
            return self._build_result(config, markers, self._parse_latency(result.stdout))
        finally:
            if not keep_logs:
                driver_path.unlink(missing_ok=True)

    def _build_result(self, config: RunConfig, markers: dict[str, float],
                      latency: dict[str, float] | None = None) -> RunResult:
        elapsed = markers.get("ELAPSED") or None
        nbytes = markers.get("BYTES") or 0
        input_rows = compute_input_rows(config)
        throughput_mbps = (nbytes / 1_000_000 / elapsed) if (nbytes and elapsed) else None
        mrows = (input_rows / 1e6 / elapsed) if (input_rows and elapsed) else None
        out_fmt = str(config.full_config.get("spark_output_format", "json_void")).lower()
        output_formatter = {"json_void": "JSON", "json_void_latency": "JSON", "none": None}.get(out_fmt)
        sink_type = {"json_void": "void-format", "json_void_latency": "void-format-latency",
                     "none": "blackhole"}.get(out_fmt, "blackhole")
        return RunResult(
            experiment=config.experiment,
            variant=config.variant,
            series=config.series,
            repetition=config.repetition,
            query_ref=config.query_ref,
            x_param=config.x_param,
            x_value=config.x_value,
            system=config.system,
            branch=config.branch,
            mode=config.mode,
            build=config.build,
            config=config.full_config,
            # Record Spark parallelism in the worker_threads slot so a thread-count sweep plots
            # all engines on one x-axis (NES worker threads == Spark local[N] by construction).
            worker_threads=_spark_parallelism(config),
            input_formatter="CSV",
            output_formatter=output_formatter,
            source_type="filesystem",
            sink_type=sink_type,
            num_queries=config.num_queries,
            throughput_mbps=throughput_mbps,
            processing_time_s=elapsed,
            input_rows=input_rows,
            throughput_mrows_s=mrows,
            **(latency or {}),
        )
