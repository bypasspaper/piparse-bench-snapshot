"""Systest runner: invoke the systest binary, parse JSON output."""

from __future__ import annotations

import json
import os
import subprocess
import tempfile
from pathlib import Path
from typing import Any

import click

from ..models import RunConfig, RunResult
from ..registry import flatten_worker_config
from ..renderer import render_for_config
from ..resolver import compute_input_rows
from .base import Runner


class SystestRunner(Runner):
    def __init__(self, build_dir: str | Path, invoke_modes_dir: str | Path | None = None):
        self.build_dir = Path(build_dir)
        self.systest_bin = self.build_dir / "nes-systests" / "systest" / "systest"
        self.invoke_modes_dir = (
            Path(invoke_modes_dir) if invoke_modes_dir
            else Path(__file__).parent.parent.parent / "configs" / "invoke_modes"
        )

    def validate(self, config: RunConfig) -> list[str]:
        errors = []
        if config.mode != "systest":
            errors.append(f"SystestRunner requires mode='systest', got '{config.mode}'")
        if not self.systest_bin.exists():
            errors.append(f"systest binary not found: {self.systest_bin}")
        return errors

    def _build_worker_flags(self, config: RunConfig) -> list[str]:
        """Convert nested worker config to --key=value flags."""
        flat = flatten_worker_config(config.worker_flags)
        flags = []
        for key, value in flat.items():
            # Resolve invoke mode name to file path
            if key == "worker.query_engine.invoke_mode_configuration":
                if value in ("default", "fnattr", "inline"):
                    mode_file = self.invoke_modes_dir / f"{value}.yaml"
                    if mode_file.exists():
                        value = str(mode_file)
            flags.append(f"--{key}={value}")
        return flags

    @staticmethod
    def _resolve_env(config: RunConfig) -> tuple[dict[str, str], float]:
        """Merge the experiment's `env:` block into the process environment and
        extract the NES_INMEM_REPEAT throughput multiplier.

        The systest/bench-runner JSON computes bytesPerSecond from the INPUT FILE
        SIZE and is blind to NES_INMEM_REPEAT (which loops the prefilled buffers N
        passes inside the source). So a REPEAT=N run reports 1/N of the true rate;
        the caller multiplies throughput by N to recover it. Returns (env, repeat).
        """
        env = dict(os.environ)
        raw = config.full_config.get("env", {}) or {}
        for k, v in raw.items():
            env[k] = str(v)
        repeat = 1.0
        if env.get("NES_INMEM_REPEAT"):
            try:
                repeat = float(env["NES_INMEM_REPEAT"])
            except ValueError:
                repeat = 1.0
        return env, repeat

    def _build_command(
        self, config: RunConfig, test_file: Path, topology_yaml: Path | None
    ) -> list[str]:
        cmd = [
            str(self.systest_bin),
            "-t", f"{test_file}:1",
            "-e", config.data_size, "CompilationIntensive",
            "-b",
        ]

        if topology_yaml and topology_yaml.exists():
            cmd.extend(["-c", str(topology_yaml)])

        cmd.append("--")
        cmd.extend(self._build_worker_flags(config))

        return cmd

    def execute(self, config: RunConfig) -> RunResult:
        # Render the .test file
        test_content = render_for_config(config)

        with tempfile.NamedTemporaryFile(
            mode="w", suffix=".test", delete=False, prefix="pbench_"
        ) as f:
            f.write(test_content)
            test_file = Path(f.name)

        try:
            # Resolve topology
            topology_yaml = None
            topo_path = (
                Path(__file__).parent.parent.parent
                / "configs" / "topologies" / f"{config.topology}.yaml"
            )
            if topo_path.exists():
                topology_yaml = topo_path

            cmd = self._build_command(config, test_file, topology_yaml)
            env, repeat = self._resolve_env(config)

            result = subprocess.run(
                cmd,
                capture_output=True,
                text=True,
                timeout=600,
                env=env,
            )

            if result.returncode != 0:
                # Try to salvage benchmark results (e.g. checksum mismatch
                # still produces valid JSON output)
                try:
                    metrics = self._parse_output(result.stdout)
                except Exception:
                    raise RuntimeError(
                        f"systest failed (exit {result.returncode}):\n"
                        f"cmd: {' '.join(cmd)}\n"
                        f"stdout: {result.stdout[:2000]}\n"
                        f"stderr: {result.stderr[:2000]}"
                    )
                click.echo(
                    f" WARNING: systest exited with code {result.returncode} "
                    f"but produced results — storing anyway (likely checksum mismatch).\n"
                    f"  stderr: {result.stderr[:2000]}",
                    err=True,
                )
                self._apply_repeat(metrics, repeat)
                return self._build_result(config, metrics)

            metrics = self._parse_output(result.stdout)
            self._apply_repeat(metrics, repeat)
            return self._build_result(config, metrics)

        finally:
            test_file.unlink(missing_ok=True)

    @staticmethod
    def _apply_repeat(metrics: dict[str, Any], repeat: float) -> None:
        """Recover the true throughput for an NES_INMEM_REPEAT prefill run (see
        _resolve_env). The reported bytesPerSecond is file-size-based, so the engine
        actually processed `repeat` x that many bytes in the same window."""
        if repeat and repeat != 1.0 and metrics.get("throughput_mbps"):
            metrics["throughput_mbps"] *= repeat

    def _parse_output(self, stdout: str) -> dict[str, Any]:
        """Parse the JSON output from systest -b."""
        # systest -b outputs a JSON array embedded in other text.
        # Find the opening "[\n    {" and try progressively from each "]"
        # after it until we get valid JSON.
        import re
        match = re.search(r"\[\s*\{", stdout)
        if not match:
            raise RuntimeError(f"No JSON array found in systest output:\n{stdout[:2000]}")
        json_start = match.start()

        # Search for closing "]" from the JSON start forward
        data = None
        search_from = json_start
        while True:
            json_end = stdout.find("]", search_from)
            if json_end == -1:
                break
            try:
                data = json.loads(stdout[json_start:json_end + 1])
                break
            except json.JSONDecodeError:
                search_from = json_end + 1

        if data is None:
            raise RuntimeError(
                f"No valid JSON array found in systest output:\n{stdout[:2000]}"
            )
        if not data:
            raise RuntimeError("Empty JSON array from systest output")

        # Take the first (usually only) entry
        entry = data[0]
        return {
            "throughput_mbps": entry.get("bytesPerSecond", 0) / 1_000_000,
            "processing_time_s": entry.get("time", 0),
        }

    def _build_result(self, config: RunConfig, metrics: dict[str, Any]) -> RunResult:
        """Build a RunResult from config and parsed metrics."""
        qc = config.query_config
        wf = config.worker_flags

        # Format-invariant rows metric (catalog rows x repeat / window). NES_INMEM_REPEAT runs
        # multiply rows the same way _apply_repeat multiplied MB/s (the window covers N passes).
        input_rows = compute_input_rows(config)
        if input_rows is not None:
            env_repeat = (config.full_config.get("env", {}) or {}).get("NES_INMEM_REPEAT")
            if env_repeat:
                try:
                    input_rows *= int(env_repeat)
                except ValueError:
                    pass
        t = metrics.get("processing_time_s")
        mrows = (input_rows / 1e6 / t) if (input_rows and t) else None

        # Extract flattened columns
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
            input_rows=input_rows,
            throughput_mrows_s=mrows,
        )
