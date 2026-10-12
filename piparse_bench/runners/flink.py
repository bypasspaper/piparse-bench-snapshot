"""Flink runner: render a PyFlink Table API driver, run it as a subprocess, parse metrics."""

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


def _flink_parallelism(config: RunConfig) -> int:
    """Flink job parallelism = NES worker-thread count (so the same knob drives both engines),
    falling back to an explicit `flink_parallelism` then 1."""
    qe = (config.full_config.get("worker") or {}).get("query_engine") or {}
    return int(qe.get("number_of_worker_threads")
               or config.full_config.get("flink_parallelism") or 1)


class FlinkRunner(Runner):
    """Runner for Apache Flink via PyFlink Table API/SQL.

    Renders a self-contained PyFlink driver script (see renderer.render_flink) and runs it in a
    fresh subprocess per rep — one cold JVM each, mirroring NES's re-exec-per-rep fairness.
    PyFlink Table/SQL compiles to native JVM operators, so there is NO per-record Python in the
    data path ("just python" costs nothing at runtime). Metrics (BYTES/ELAPSED) are parsed from
    the driver's stdout; all NES-specific RunResult columns stay NULL.
    """

    _MARKER_RE = re.compile(r"^(BYTES|ELAPSED|WALL|ROWS)=([0-9.]+)\s*$", re.MULTILINE)
    # One line per sink subtask (flink_output_format: latency). Latencies are NANOSECONDS.
    _LATENCY_RE = re.compile(
        r"^PBENCH_LATENCY subtask=(\d+) rows=(\d+) samples=(\d+) min=(\d+) max=(\d+) "
        r"avg=([0-9.eE+-]+) p50=(\d+) p99=(\d+) p999=(\d+)\s*$",
        re.MULTILINE,
    )

    _SAMPLES_RE = re.compile(r"^PBENCH_LATENCY_SAMPLES (.+)$", re.MULTILINE)

    @classmethod
    def _parse_latency(cls, stdout: str) -> dict[str, float] | None:
        """Fold the sink latency output into us-scaled RunResult fields.

        Preferred path: POOL every raw sample (ns) across ALL pipelines' sinks
        (PBENCH_LATENCY_SAMPLES lines) and compute exact percentiles over the pooled set — the
        only correct aggregate across N concurrent pipelines (median-of-per-pipeline-percentiles
        would be wrong). Falls back to the per-subtask PBENCH_LATENCY summary lines (median across
        subtasks) if no samples lines are present."""
        pooled: list[int] = []
        for m in cls._SAMPLES_RE.finditer(stdout):
            for tok in m.group(1).split(","):
                tok = tok.strip()
                if tok:
                    pooled.append(int(tok))
        if pooled:
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
        subs = []
        for m in cls._LATENCY_RE.finditer(stdout):
            samples = int(m.group(3))
            if samples == 0:
                continue
            subs.append({
                "samples": samples,
                "min": int(m.group(4)), "max": int(m.group(5)), "avg": float(m.group(6)),
                "p50": int(m.group(7)), "p99": int(m.group(8)), "p999": int(m.group(9)),
            })
        if not subs:
            return None
        ns_to_us = 1.0 / 1000.0

        def _median(vals: list[float]) -> float:
            s = sorted(vals)
            return s[len(s) // 2]

        total = sum(s["samples"] for s in subs)
        return {
            "min_latency_us": min(s["min"] for s in subs) * ns_to_us,
            "max_latency_us": max(s["max"] for s in subs) * ns_to_us,
            "avg_latency_us": sum(s["avg"] * s["samples"] for s in subs) / total * ns_to_us,
            "p50_latency_us": _median([s["p50"] for s in subs]) * ns_to_us,
            "p99_latency_us": _median([s["p99"] for s in subs]) * ns_to_us,
            "p999_latency_us": _median([s["p999"] for s in subs]) * ns_to_us,
            "p99_latency_us_worst": max(s["p99"] for s in subs) * ns_to_us,
        }

    def __init__(self, testdata_dir: str | Path | None = None,
                 python_bin: str | None = None, timeout: int = 900):
        self.testdata_dir = Path(testdata_dir) if testdata_dir else None
        # Interpreter that has pyflink installed. Defaults to the pbench interpreter itself
        # (PyFlink 2.0/2.1 co-installs into the same 3.12 venv); override with --flink-python.
        self.python_bin = python_bin or sys.executable
        # High-N latency sweeps (N pipelines each reading the whole file) can run many minutes;
        # PBENCH_FLINK_TIMEOUT overrides the per-run subprocess timeout (seconds).
        self.timeout = int(os.environ.get("PBENCH_FLINK_TIMEOUT") or timeout)
        self._pyflink_ok: bool | None = None  # memoized (validate runs once per config)

    def _pyflink_importable(self) -> bool:
        if self._pyflink_ok is None:
            try:
                r = subprocess.run(
                    [self.python_bin, "-c", "import pyflink"],
                    capture_output=True, text=True, timeout=120,
                )
                self._pyflink_ok = r.returncode == 0
            except Exception:
                self._pyflink_ok = False
        return self._pyflink_ok

    def validate(self, config: RunConfig) -> list[str]:
        errors: list[str] = []
        if config.mode != "flink":
            errors.append(f"FlinkRunner requires mode='flink', got '{config.mode}'")
            return errors
        if self.testdata_dir is None:
            errors.append(
                "FlinkRunner needs a testdata dir: pass --testdata-dir, or --build-dir "
                "(then it defaults to <build-dir>/nes-systests/testdata)."
            )
        if str(config.full_config.get("flink_output_format", "none")).lower() in (
            "json_blackhole", "latency", "latency_json"
        ):
            ext_jar = Path(__file__).resolve().parent.parent.parent / "flink-ext" / "pbench-flink-ext.jar"
            if not ext_jar.exists():
                errors.append(
                    f"flink_output_format needs the custom connectors in {ext_jar}; "
                    "build it with flink-ext/build.sh."
                )
        if not self._pyflink_importable():
            errors.append(
                f"pyflink not importable with '{self.python_bin}'. Install it "
                "(uv pip install -e \".[flink]\" or pip install 'apache-flink>=2.0,<2.2') "
                "or pass --flink-python <interpreter>."
            )
        if shutil.which("java") is None and not os.environ.get("JAVA_HOME"):
            errors.append(
                "Java not found (no `java` on PATH and JAVA_HOME unset); PyFlink needs a JVM (11 or 17)."
            )
        try:
            query = load_query(config.query_ref)
        except Exception as e:
            errors.append(f"Could not load query '{config.query_ref}': {e}")
            return errors
        if query.flink_sql is None:
            errors.append(
                f"Query '{config.query_ref}' has no `flink_sql`; add a Flink-dialect SELECT to "
                "its catalog YAML to run it with mode: flink."
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
                if entry.file.endswith(".nes"):
                    ext_jar = (Path(__file__).resolve().parent.parent.parent
                               / "flink-ext" / "pbench-flink-ext.jar")
                    if not ext_jar.exists():
                        errors.append(
                            f".nes sources need the nes-native format from {ext_jar}; "
                            "build it with flink-ext/build.sh."
                        )
        return errors

    def _warm_inputs(self, config: RunConfig) -> None:
        """Pull each source's data file into the page cache before the timed run, so Flink reads
        from warm cache — parity with NES's worker-cli warm_read (the Flink file source itself has
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
            mode="w", suffix="_flink.py", delete=False, prefix="pbench_"
        ) as f:
            f.write(driver)
            driver_path = Path(f.name)

        keep_logs = bool(os.environ.get("PBENCH_KEEP_LOGS"))
        try:
            env = {**os.environ}
            # `flink_jvm_args:` (string or list) in the experiment YAML -> JVM_ARGS, which
            # pyflink_gateway_server.py prepends to the MiniCluster JVM command line (heap
            # size, GC logging). Appended to any JVM_ARGS already in the environment. Needed
            # because the gateway JVM otherwise runs on the default 25%-of-RAM heap.
            jvm_args = config.full_config.get("flink_jvm_args")
            if jvm_args:
                if isinstance(jvm_args, (list, tuple)):
                    jvm_args = " ".join(str(a) for a in jvm_args)
                env["JVM_ARGS"] = f"{env.get('JVM_ARGS', '')} {jvm_args}".strip()
            result = subprocess.run(
                [self.python_bin, str(driver_path)],
                capture_output=True, text=True, timeout=self.timeout, env=env,
            )
            if keep_logs:
                # Driver stdout carries per-subtask PBENCH_VOIDSINK bytes=/rows= lines (split
                # distribution + serialized output volume) — keep it next to the driver script.
                log_path = driver_path.with_suffix(".stdout")
                log_path.write_text(result.stdout)
            markers = {
                m.group(1): float(m.group(2))
                for m in self._MARKER_RE.finditer(result.stdout)
            }
            if result.returncode != 0 or "ELAPSED" not in markers:
                raise RuntimeError(
                    f"Flink driver failed (exit {result.returncode}, no ELAPSED marker):\n"
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
        # Native (.nes) sources go through the custom nes-native format instead of csv.
        try:
            query = load_query(config.query_ref)
            entries = [_resolve_data_entry(s, config.data_size) for s in query.sources]
            input_formatter = "Native" if any(
                e is not None and e.file.endswith(".nes") for e in entries) else "CSV"
        except Exception:
            input_formatter = "CSV"
        throughput_mbps = (nbytes / 1_000_000 / elapsed) if (nbytes and elapsed) else None
        mrows = (input_rows / 1e6 / elapsed) if (input_rows and elapsed) else None
        # csv→json arms (see render_flink): json_object serializes in the query into blackhole;
        # json_file runs Flink's real JSON encoder into a tmpfs dir deleted after timing;
        # json_blackhole runs the same real JSON encoder into the custom void sink (the fair arm).
        out_fmt = str(config.full_config.get("flink_output_format", "none")).lower()
        output_formatter = {
            "json_object": "JSON_OBJECT", "json_file": "JSON", "json_blackhole": "JSON",
            "latency_json": "JSON",
        }.get(out_fmt)
        sink_type = {
            "json_file": "filesystem-tmpfs", "json_blackhole": "blackhole-format",
            "latency": "latency-blackhole", "latency_json": "json-latency-blackhole",
        }.get(out_fmt, "blackhole")
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
            # Record Flink parallelism in the worker_threads slot so a thread-count sweep plots
            # both engines on one x-axis (NES worker threads == Flink parallelism by construction).
            worker_threads=_flink_parallelism(config),
            input_formatter=input_formatter,
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
