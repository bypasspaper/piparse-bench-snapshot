"""nes-bench-runner runner: invoke the dedicated benchmark binary, parse its JSON.

Why a separate runner from SystestRunner: the `systest -b` path computes throughput
from the source FILE SIZE via the embedded backend, and for an InMemory (prefill)
source it yields a null/zero benchmark result (the formatter never runs — observed
DIAG_SIMDINDEX calls=0). nes-bench-runner is the harness the prefill numbers were
established on: it actually drains the engine and reports a true `engine window`
(min-running -> max-stop) plus per-query bytes/seconds in BenchRunnerResults.json.

Renders the SAME .test file as systest (render_systest) — only the binary, the
arguments, and the result parsing differ. Env vars (NES_INMEM_PREFILL / _REPEAT /
_BUFSIZE) are forwarded; the NES_INMEM_REPEAT throughput correction is applied
(bench-runner's bytesPerSecond is input-file-size based, blind to REPEAT — see
SystestRunner._resolve_env).
"""

from __future__ import annotations

import json
import subprocess
import tempfile
from pathlib import Path
from typing import Any

import click

from ..models import RunConfig, RunResult
from ..registry import flatten_worker_config
from ..renderer import render_for_config
from .base import Runner
from .systest import SystestRunner


class BenchRunnerRunner(Runner):
    def __init__(self, build_dir: str | Path, invoke_modes_dir: str | Path | None = None):
        self.build_dir = Path(build_dir)
        self.bench_bin = self.build_dir / "nes-systests" / "bench-runner" / "nes-bench-runner"
        # Large testdata is baked under <build>/nes-systests/testdata (ENABLE_LARGE_TESTS).
        self.test_data_dir = self.build_dir / "nes-systests" / "testdata"
        self.invoke_modes_dir = (
            Path(invoke_modes_dir) if invoke_modes_dir
            else Path(__file__).parent.parent.parent / "configs" / "invoke_modes"
        )

    def validate(self, config: RunConfig) -> list[str]:
        errors = []
        if config.mode != "bench-runner":
            errors.append(f"BenchRunnerRunner requires mode='bench-runner', got '{config.mode}'")
        if not self.bench_bin.exists():
            errors.append(f"nes-bench-runner binary not found: {self.bench_bin}")
        return errors

    def _build_worker_flags(self, config: RunConfig) -> list[str]:
        """Convert nested worker config to --key=value flags (mirrors SystestRunner)."""
        flat = flatten_worker_config(config.worker_flags)
        flags = []
        for key, value in flat.items():
            if key == "worker.query_engine.invoke_mode_configuration":
                if value in ("default", "fnattr", "inline"):
                    mode_file = self.invoke_modes_dir / f"{value}.yaml"
                    if mode_file.exists():
                        value = str(mode_file)
            flags.append(f"--{key}={value}")
        return flags

    def execute(self, config: RunConfig) -> RunResult:
        test_content = render_for_config(config)

        with tempfile.NamedTemporaryFile(
            mode="w", suffix=".test", delete=False, prefix="pbench_"
        ) as f:
            f.write(test_content)
            test_file = Path(f.name)

        work_dir = Path(tempfile.mkdtemp(prefix="pbench_brwd_"))
        json_path = work_dir / "BenchRunnerResults.json"

        try:
            cmd = [
                str(self.bench_bin),
                "-t", f"{test_file}:1",
                "--data", str(self.test_data_dir),
                "--workingDir", str(work_dir),
                "--repeat", "1",
                "--json", str(json_path),
                "--",
                *self._build_worker_flags(config),
            ]
            env, repeat = SystestRunner._resolve_env(config)

            result = subprocess.run(
                cmd, capture_output=True, text=True, timeout=600, env=env,
            )

            if not json_path.exists():
                raise RuntimeError(
                    f"nes-bench-runner produced no results JSON (exit {result.returncode}):\n"
                    f"cmd: {' '.join(cmd)}\n"
                    f"stdout: {result.stdout[-2000:]}\n"
                    f"stderr: {result.stderr[-1500:]}"
                )

            metrics = self._parse_json(json_path)
            if repeat and repeat != 1.0 and metrics.get("throughput_mbps"):
                metrics["throughput_mbps"] *= repeat

            if result.returncode != 0:
                click.echo(
                    f" WARNING: nes-bench-runner exited {result.returncode} but produced "
                    f"results — storing anyway.\n  stderr: {result.stderr[-1000:]}",
                    err=True,
                )

            return self._build_result(config, metrics)
        finally:
            test_file.unlink(missing_ok=True)
            # Leave work_dir for post-mortem only on failure; otherwise clean it.
            try:
                for p in work_dir.glob("*"):
                    if p.is_file():
                        p.unlink()
                work_dir.rmdir()
            except OSError:
                pass

    def _parse_json(self, json_path: Path) -> dict[str, Any]:
        """Read BenchRunnerResults.json -> engine-window throughput + time.

        Schema: {batches: [{engineBytesPerSecond, engineWindowSeconds,
                            queries: [{bytesPerSecond, time, ...}]}]}.
        Uses the aggregate engine window (min running -> max stop across queries);
        for a single-query test that equals the one query's bytesPerSecond.
        """
        data = json.loads(json_path.read_text())
        batches = data.get("batches") or []
        if not batches:
            raise RuntimeError(f"No batches in {json_path}: {json_path.read_text()[:500]}")
        batch = batches[0]
        bps = batch.get("engineBytesPerSecond")
        secs = batch.get("engineWindowSeconds")
        if bps is None:
            queries = batch.get("queries") or []
            if not queries:
                raise RuntimeError(f"No queries in batch: {json.dumps(batch)[:500]}")
            bps = queries[0].get("bytesPerSecond", 0)
            secs = queries[0].get("time", 0)
        return {
            "throughput_mbps": (bps or 0) / 1_000_000,
            "processing_time_s": secs or 0,
        }

    def _build_result(self, config: RunConfig, metrics: dict[str, Any]) -> RunResult:
        qc = config.query_config
        wf = config.worker_flags
        qe = wf.get("query_engine", {})
        dqe = wf.get("default_query_execution", {})

        fmt = qc.get("input_formatter", {})
        input_fmt = fmt.get("type") if isinstance(fmt, dict) else fmt
        ofmt = qc.get("output_formatter", {})
        output_fmt = ofmt.get("type") if isinstance(ofmt, dict) else ofmt

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
            worker_threads=qe.get("number_of_worker_threads"),
            io_threads=qe.get("number_of_io_threads"),
            operator_buffer_size=dqe.get("operator_buffer_size"),
            execution_mode=dqe.get("execution_mode"),
            input_formatter=input_fmt,
            output_formatter=output_fmt,
            invoke_mode=qe.get("invoke_mode_configuration"),
            source_type=qc.get("source_type"),
            sink_type=qc.get("sink_type"),
            num_queries=config.num_queries,
            throughput_mbps=metrics.get("throughput_mbps"),
            processing_time_s=metrics.get("processing_time_s"),
        )
