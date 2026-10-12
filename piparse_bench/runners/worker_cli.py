"""Worker/CLI runner: start worker, submit queries via nes-cli, collect results."""

from __future__ import annotations

import json
import os
import re
import shutil
import signal
import socket
import subprocess
import tempfile
import time
import uuid
import threading

import click
from pathlib import Path
from typing import Any

from ..coldcache import evict_file, warm_file
from ..models import RunConfig, RunResult
from ..monitoring import aggregate_monitoring_results, aggregate_codec_monitoring_results, _detect_csv_type
from ..registry import flatten_worker_config
from ..renderer import render_worker_cli_yaml, _apply_codec_extension
from ..resolver import compute_input_rows, load_query
from .base import Runner

# Max seconds to wait for the worker's gRPC port to accept connections
_WORKER_STARTUP_WAIT = 60
# Max seconds to wait for a TCP generator to bind its port (it reads the whole
# data file into memory *before* binding, so a multi-GB file takes a while).
_GENERATOR_STARTUP_WAIT = 120
# Grace period after SIGTERM before SIGKILL
_KILL_GRACE = 5


class WorkerCliRunner(Runner):
    def __init__(
        self,
        build_dir: str | Path,
        generator_bin: str | Path | None = None,
        invoke_modes_dir: str | Path | None = None,
        tcp_host: str = "localhost",
        tcp_base_port: int = 1111,
        worker_port: int = 8080,
    ):
        self.build_dir = Path(build_dir)
        self.worker_bin = self.build_dir / "nes-single-node-worker" / "nes-single-node-worker"
        self.cli_bin = self.build_dir / "nes-frontend" / "apps" / "nes-cli"
        self.generator_bin = Path(generator_bin) if generator_bin else None
        self.invoke_modes_dir = (
            Path(invoke_modes_dir) if invoke_modes_dir
            else Path(__file__).parent.parent.parent / "configs" / "invoke_modes"
        )
        self.tcp_host = tcp_host
        self.tcp_base_port = tcp_base_port
        self.worker_port = worker_port

    def validate(self, config: RunConfig) -> list[str]:
        errors = []
        if config.mode != "worker-cli":
            errors.append(f"WorkerCliRunner requires mode='worker-cli', got '{config.mode}'")
        if not self.worker_bin.exists():
            errors.append(f"Worker binary not found: {self.worker_bin}")
        if not self.cli_bin.exists():
            errors.append(f"nes-cli binary not found: {self.cli_bin}")

        source_type = config.query_config.get("source_type", "TCP")
        if source_type in ("TCP", "BlockingTCP", "IoUringTCP", "IoUringTCPBlocking"):
            if self.generator_bin is None:
                errors.append("TCP source requires --generator-bin")
            elif not self.generator_bin.exists():
                errors.append(f"Generator binary not found: {self.generator_bin}")

        # duration_seconds is optional — if omitted, we poll monitoring CSVs for completion

        return errors

    def _build_worker_flags(self, config: RunConfig) -> list[str]:
        """Convert nested worker config to --key=value flags."""
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

    def _open_log(self, name: str):
        """Open a per-process log file under the run's log dir and track it for cleanup.

        Worker/generator output must NOT go to an unread PIPE: a verbose multi-second
        run can fill the 64 KiB pipe buffer and deadlock the child. Redirect to files.
        """
        path = self._log_dir / name
        fh = open(path, "w")
        self._log_files.append(fh)
        return fh, path

    def _start_worker(self, config: RunConfig) -> subprocess.Popen:
        """Start the NES single-node worker."""
        cmd = [str(self.worker_bin)]
        # Only override the gRPC bind address for a non-default port (sidestep a port already held
        # by another session's worker). Left untouched at 8080 so default behavior is unchanged.
        # NB: the worker parses the value as YAML, and the default `[::]:PORT` form is invalid YAML
        # (leading `[` reads as a flow sequence → "bad conversion"), so use a plain host:port.
        if self.worker_port != 8080:
            cmd.append(f"--grpc=127.0.0.1:{self.worker_port}")
        cmd += self._build_worker_flags(config)
        # Core-budget method for TCP benches (TCP_COLLAPSE_MECHANISM.md): pin the ENGINE to a
        # cpulist disjoint from the generator's. Top-level experiment key `worker_cpus`.
        worker_cpus = config.full_config.get("worker_cpus")
        if worker_cpus:
            cmd = ["taskset", "-c", str(worker_cpus)] + cmd
        env = dict(os.environ)
        # File sources read the file once; for a steady multi-pass window, replay it N
        # times via NES_FILE_REPEAT (the engine lseeks back to 0 on EOF). Reuse the
        # generator.repeat knob so File and TCP runs ingest the same total bytes, and
        # _compute_input_size already multiplies the MonitoringSink size by repeat.
        source_type = config.query_config.get("source_type", "TCP")
        if source_type in ("File", "BlockingFile", "MmapFile"):
            repeat = (config.generator or {}).get("repeat", 1)
            env["NES_FILE_REPEAT"] = str(repeat)
        # Arbitrary worker env vars from the experiment (e.g. NES_IOURING_DIRECT, NES_IOURING_QD
        # for the io_uring source). Stringified so YAML ints/bools pass through cleanly.
        for key, value in (config.full_config.get("worker_env") or {}).items():
            env[str(key)] = str(value)
        log_fh, log_path = self._open_log("worker.log")
        proc = subprocess.Popen(cmd, stdout=log_fh, stderr=subprocess.STDOUT, env=env)
        self._wait_for_worker_ready(proc, log_path)
        return proc

    def _wait_for_worker_ready(self, proc: subprocess.Popen, log_path: Path) -> None:
        """Busy-connect to the worker's gRPC port until it accepts (deterministic;
        far better than a blind sleep)."""
        deadline = time.time() + _WORKER_STARTUP_WAIT
        while time.time() < deadline:
            if proc.poll() is not None:
                log = _tail(log_path)
                raise RuntimeError(
                    f"Worker exited during startup (exit {proc.returncode}):\n{log}"
                )
            try:
                with socket.create_connection((self.tcp_host, self.worker_port), timeout=2):
                    return
            except OSError:
                time.sleep(0.2)
        raise RuntimeError(
            f"Worker gRPC port {self.worker_port} not accepting within "
            f"{_WORKER_STARTUP_WAIT}s:\n{_tail(log_path)}"
        )

    def _start_generators(self, config: RunConfig) -> list[subprocess.Popen]:
        """Start TCP data generators for each source."""
        if self.generator_bin is None:
            return []

        gen_config = config.generator or {}
        query = load_query(config.query_ref)
        generators = []

        # Independent mode: one generator PER (query, source) on its own port, each
        # serving just that query's connections -> N generator processes for N
        # concurrent queries (uncaps the producer side). Shared mode (default): one
        # generator per source serving all num_queries clients on one port.
        independent = _independent_generators(config)
        n_sources = len(query.sources)
        num_queries = max(1, config.num_queries)
        query_indices = range(num_queries) if independent else [0]

        for q in query_indices:
            for i, src in enumerate(query.sources):
                data_file = gen_config.get("data_file")
                if data_file is None:
                    source_codec = config.full_config.get("source_codec")
                    if source_codec == "None":
                        source_codec = None
                    # Use 'encoded' data entry when codec is set
                    if source_codec and "encoded" in src.data:
                        data_entry = src.data["encoded"]
                    else:
                        data_entry = src.data.get(config.data_size) or next(iter(src.data.values()), None)
                    if data_entry:
                        data_file = str(self.build_dir / "nes-systests" / "testdata" / _apply_codec_extension(data_entry.file, source_codec))

                if data_file is None:
                    raise RuntimeError(f"No data file for source '{src.name}'")

                port = self.tcp_base_port + (q * n_sources if independent else 0) + i
                repeat = gen_config.get("repeat", 100)
                mode = gen_config.get("mode", "uniform")
                rate = gen_config.get("rate", 10000)
                target_mb = gen_config.get("target_mb")
                # Clients this generator must serve. Independent: just this query's
                # connections (instances). Shared: all queries' (instances x num_queries).
                num_clients = src.instances * (1 if independent else num_queries)

                cmd = [
                    str(self.generator_bin),
                    "--file", str(data_file),
                    "--port", str(port),
                    "--host", self.tcp_host,
                    "-n", str(num_clients),
                    "-m", str(repeat),
                    "-r", str(rate),
                ]
                # The async tokio generator (async-tcp-generator: FIXED --threads pool,
                # thread count independent of N) has a different CLI: --threads and
                # --chunk-size-kb, but NO --mode/--timestamp-field/--target-mb. Select it
                # with generator.kind: async (or by pointing --generator-bin at a binary
                # with 'async' in its name).
                is_async_gen = gen_config.get("kind") == "async" or "async" in Path(self.generator_bin).name
                if is_async_gen:
                    cmd += ["--threads", str(gen_config.get("threads", 4))]
                    if gen_config.get("chunk_size_kb") is not None:
                        cmd += ["--chunk-size-kb", str(gen_config["chunk_size_kb"])]
                else:
                    cmd += ["--mode", mode]
                    if src.timestamp_field is not None:
                        cmd.extend(["--timestamp-field", str(src.timestamp_field)])
                    if target_mb is not None:
                        cmd += ["--target-mb", str(target_mb)]
                # Pin the generator (generator.cpus cpulist) DISJOINT from worker_cpus --
                # unpinned generators confound every TCP number (cross-CCX skb transfer).
                gen_cpus = gen_config.get("cpus")
                if gen_cpus:
                    cmd = ["taskset", "-c", str(gen_cpus)] + cmd

                tag = f"q{q}_" if independent else ""
                log_fh, log_path = self._open_log(f"generator_{tag}{i}_{src.name}.log")
                proc = subprocess.Popen(cmd, stdout=log_fh, stderr=subprocess.STDOUT)
                # The generator reads the whole file into memory BEFORE binding its port,
                # so a multi-GB file takes seconds. Sync on its "Listening on" log line —
                # do NOT connect a readiness probe: that consumes the single client slot the
                # generator is waiting for, so it would start streaming to the dead probe.
                self._wait_for_generator_ready(proc, log_path, src.name)
                generators.append(proc)

        return generators

    def _wait_for_generator_ready(self, proc: subprocess.Popen, log_path: Path, name: str) -> None:
        """Wait until the generator has loaded its file and is listening (tracing logs
        'Listening on ...' just before bind)."""
        deadline = time.time() + _GENERATOR_STARTUP_WAIT
        while time.time() < deadline:
            if proc.poll() is not None:
                raise RuntimeError(
                    f"Generator for source '{name}' exited during startup "
                    f"(exit {proc.returncode}):\n{_tail(log_path)}"
                )
            if log_path.exists() and "Listening on" in log_path.read_text(errors="replace"):
                return
            time.sleep(0.2)
        raise RuntimeError(
            f"Generator for source '{name}' not listening within "
            f"{_GENERATOR_STARTUP_WAIT}s:\n{_tail(log_path)}"
        )

    def _resolve_file_source_paths(self, config: RunConfig) -> list[Path]:
        """Resolve the on-disk data file(s) for a File query's sources, mirroring the
        renderer's file_path resolution (data entry for data_size, codec extension)."""
        query = load_query(config.query_ref)
        source_codec = config.full_config.get("source_codec")
        if source_codec == "None":
            source_codec = None
        testdata = self.build_dir / "nes-systests" / "testdata"
        paths = []
        for src in query.sources:
            entry = src.data.get(config.data_size) or (next(iter(src.data.values())) if src.data else None)
            if entry is None:
                continue
            paths.append(testdata / _apply_codec_extension(entry.file, source_codec))
        return paths

    def _prepare_file_cache(self, config: RunConfig) -> None:
        """Put the File source's data file(s) into a known cache state *before* the
        query reads them, so warm-vs-cold is a controlled A/B:

        - `cold_read: true`  -> posix_fadvise(DONTNEED) evicts the file (rung-1 method,
          no root) -> the read hits the disk. The cold arm.
        - `warm_read: true`  -> sequentially reads the file into the page cache -> the
          read is a cache hit. The warm arm (proves eviction actually moves throughput,
          not just a mincore counter: same config, flip the knob, watch GB/s drop).

        Both mincore-verify the resulting residency and log it. Called after the worker
        is up but before query submit: the worker doesn't touch the data file until the
        source opens at query start, and for File there is no generator, so nothing
        perturbs the cache in between. No-op for non-File sources / when neither set."""
        cold = bool(config.full_config.get("cold_read"))
        warm = bool(config.full_config.get("warm_read"))
        if not (cold or warm):
            return
        if cold and warm:
            raise RuntimeError("cold_read and warm_read are mutually exclusive — set at most one.")
        source_type = config.query_config.get("source_type", "TCP")
        if source_type not in ("File", "BlockingFile", "MmapFile", "IoUringFile", "IoUringFileAsync"):
            click.echo(f"  [cache] ignored: source_type={source_type} is not a File source")
            return
        # Cold only makes sense single-pass. NES_FILE_REPEAT>1 re-reads the file, so
        # passes 2..N are served warm from the cache the first pass just populated and
        # would dominate the (whole-run) MonitoringSink window -> not a cold number.
        repeat = int((config.generator or {}).get("repeat", 1) or 1)
        if cold and repeat > 1:
            raise RuntimeError(
                f"cold_read requires single-pass reads, but generator.repeat={repeat}. "
                f"Multi-pass replay warms the page cache after pass 1, corrupting the cold "
                f"measurement. Set generator.repeat: 1 for cold runs (use a large data file "
                f"for a long single-pass window)."
            )
        for path in self._resolve_file_source_paths(config):
            if not path.exists():
                click.echo(f"  [cache] WARN: file not found, cannot prepare: {path}")
                continue
            if cold:
                res = evict_file(path)
                ok, tag = res.fully_cold, "COLD" if res.fully_cold else "STILL-WARM"
                label = "cold_read"
            else:
                res = warm_file(path)
                ok, tag = res.fully_warm, "WARM" if res.fully_warm else "PARTIAL"
                label = "warm_read"
            click.echo(
                f"  [{label}] {tag} {path.name}: resident "
                f"{res.resident_before_mb:.0f} -> {res.resident_after_mb:.0f} MB of {res.total_mb:.0f} MB"
            )
            if not ok:
                click.echo(
                    f"  [{label}] WARN: residency {res.resident_after_mb:.0f} MB is not the "
                    f"expected {'~0' if cold else 'full'} — the measurement may be mixed."
                )

    def _submit_query(self, config: RunConfig, query_yaml_path: Path) -> str:
        """Submit a query via nes-cli and return the query ID."""
        cmd = [
            str(self.cli_bin),
            "-d",
            "-t", str(query_yaml_path),
            "start",
        ]
        result = subprocess.run(cmd, capture_output=True, text=True, timeout=30)
        if result.returncode != 0:
            raise RuntimeError(
                f"nes-cli failed (exit {result.returncode}):\n{result.stderr[:1000]}"
            )

        # Parse query ID from output
        # Expected: something like "Query submitted with id: 42"
        click.echo(f"  nes-cli start stdout: {result.stdout.strip()}")
        if result.stderr.strip():
            click.echo(f"  nes-cli start stderr: {result.stderr.strip()[:500]}")
        for line in result.stdout.splitlines():
            line = line.strip()
            if "id" in line.lower():
                # Extract the number
                parts = line.split()
                for part in reversed(parts):
                    try:
                        return part.strip(":,.")
                    except ValueError:
                        continue
        # Fallback: return the full output
        return result.stdout.strip()

    def _stop_query(self, query_id: str, topology_yaml: Path) -> None:
        """Stop a running query."""
        try:
            result = subprocess.run(
                [str(self.cli_bin), "-d", "-t", str(topology_yaml), "stop", query_id],
                capture_output=True, text=True, timeout=10,
            )
            click.echo(f"  nes-cli stop (qid={query_id}): exit={result.returncode}")
            if result.stdout.strip():
                click.echo(f"  stop stdout: {result.stdout.strip()[:500]}")
            if result.stderr.strip():
                click.echo(f"  stop stderr: {result.stderr.strip()[:500]}")
        except subprocess.TimeoutExpired:
            click.echo(f"  nes-cli stop timed out for qid={query_id}")
        except Exception as e:
            click.echo(f"  nes-cli stop failed for qid={query_id}: {e}")

    def execute(self, config: RunConfig) -> RunResult:
        monitoring_dir = Path(tempfile.mkdtemp(prefix="pbench_mon_"))
        # Worker + generator stdout/stderr go to log files in here (see _open_log).
        self._log_dir = monitoring_dir
        self._log_files: list = []

        worker_proc = None
        generators: list[subprocess.Popen] = []
        query_ids: list[str] = []
        cpu_sampler: _ThreadCpuSampler | None = None
        run_id = str(uuid.uuid4())   # known up front so the artifacts dir can be named by it

        try:
            # 1. Start worker (+ per-thread CPU sampler for its whole lifetime, see _ThreadCpuSampler)
            worker_proc = self._start_worker(config)
            if not os.environ.get("PBENCH_NO_THREADCPU"):
                # aux/ subdir: the runner globs monitoring_dir/*.csv as SINK files (non-recursive), keep out of it
                (monitoring_dir / "aux").mkdir(exist_ok=True)
                # Sample whoever LISTENS on the worker port: if a stale worker holds it, worker_proc sits idle and
                # the query runs in the ghost. The port-holder is the process that actually does the work.
                sampled_pid = _pid_on_port(self.worker_port) or worker_proc.pid
                if sampled_pid != worker_proc.pid:
                    click.echo(f"  [threadcpu] WARNING: port {self.worker_port} held by pid {sampled_pid}, not the "
                               f"launched worker {worker_proc.pid} (stale worker?); sampling the port-holder")
                cpu_sampler = _ThreadCpuSampler(sampled_pid, monitoring_dir / "aux" / "threadcpu.csv", interval=0.25)
                cpu_sampler.start()

            # 2. Start generators if TCP source
            source_type = config.query_config.get("source_type", "TCP")
            if source_type in ("TCP", "BlockingTCP", "IoUringTCP", "IoUringTCPBlocking"):
                generators = self._start_generators(config)

            # 2b. For cold/warm File runs, put the data file(s) into the requested cache
            # state now — after the worker is up (it hasn't read the file yet), before submit.
            self._prepare_file_cache(config)

            # 3. Submit queries.
            # query_mix (heterogeneous scaling): the N concurrent instances round-robin over a
            # list of query refs; each catalog entry is loaded once and cached. Absent = the
            # classic single-query fan-out (all N instances run `query_ref`).
            if config.query_mix:
                _mix_cache: dict[str, Any] = {}
                for ref in config.query_mix:
                    if ref not in _mix_cache:
                        _mix_cache[ref] = load_query(ref)
                query_list = [_mix_cache[ref] for ref in config.query_mix]
            else:
                query_list = [load_query(config.query_ref)]
            independent = _independent_generators(config)
            yaml_paths: list[Path] = []
            for i in range(config.num_queries):
                query = query_list[i % len(query_list)]
                n_sources = len(query.sources)
                # Independent mode: each concurrent query gets its own port range
                # (base + i*n_sources) and thus its own generator process, so the
                # producer side is not capped by a single generator. Shared mode
                # (default): all queries connect to one generator per source.
                base_port = self.tcp_base_port + (i * n_sources if independent else 0)
                query_yaml = render_worker_cli_yaml(
                    config, query,
                    query_index=i,
                    monitoring_dir=str(monitoring_dir),
                    tcp_host=self.tcp_host,
                    tcp_base_port=base_port,
                    worker_port=self.worker_port,
                    data_dir=str(self.build_dir / "nes-systests" / "testdata"),
                )
                # Write query YAML to temp file
                fd, yaml_path = tempfile.mkstemp(suffix=".yaml", prefix="pbench_q_")
                with os.fdopen(fd, "w") as f:
                    f.write(query_yaml)
                yaml_paths.append(Path(yaml_path))

                qid = self._submit_query(config, Path(yaml_path))
                query_ids.append(qid)

            # 4. Wait for completion
            duration = config.duration_seconds
            monitoring_files = sorted(monitoring_dir.glob("*.csv"))
            if duration is not None:
                time.sleep(duration)
                # Stop queries after fixed duration
                for qid, ypath in zip(query_ids, yaml_paths):
                    self._stop_query(qid, ypath)
            else:
                # Poll monitoring CSVs until they have data (query finished at EoS).
                max_wait = config.full_config.get("completion_timeout_s", 600)
                waited = 0
                while waited < max_wait:
                    monitoring_files = sorted(monitoring_dir.glob("*.csv"))
                    if monitoring_files and all(f.stat().st_size > 0 for f in monitoring_files):
                        break
                    if worker_proc.poll() is not None:
                        raise RuntimeError(
                            f"Worker died (exit {worker_proc.returncode}) before MonitoringSink "
                            f"produced output:\n{_tail(self._log_dir / 'worker.log')}"
                        )
                    # Fast-fail on a logged query failure: the worker stays alive when a
                    # query fails (e.g. global-buffer-pool exhaustion on a stateful query),
                    # so without this we'd burn the full completion_timeout_s waiting on a
                    # MonitoringSink that will never finalize.
                    qfail = _scan_query_failure(self._log_dir / "worker.log")
                    if qfail:
                        raise RuntimeError(
                            f"Query failed (worker still alive):\n{qfail}\n"
                            f"worker.log tail:\n{_tail(self._log_dir / 'worker.log')}"
                        )
                    time.sleep(2)
                    waited += 2
                else:
                    raise RuntimeError(
                        f"MonitoringSink produced no output within {max_wait}s — the query "
                        f"likely stalled (e.g. total streamed data exceeded the worker buffer "
                        f"pool, deadlocking the source). worker.log tail:\n"
                        f"{_tail(self._log_dir / 'worker.log')}"
                    )

            # 5. Clean up query YAML files
            for p in yaml_paths:
                p.unlink(missing_ok=True)

        finally:
            # 6. Cleanup: final CPU sample, stop generators and worker, then close their log files.
            if cpu_sampler is not None:
                cpu_sampler.stop()
            for g in generators:
                _terminate(g)
            if worker_proc:
                _terminate(worker_proc)
            for fh in self._log_files:
                try:
                    fh.close()
                except Exception:
                    pass

        monitoring_files = sorted(monitoring_dir.glob("*.csv"))
        if not monitoring_files or any(f.stat().st_size == 0 for f in monitoring_files):
            raise RuntimeError(
                f"Empty MonitoringSink CSV in {monitoring_dir}"
            )

        sink_type = config.query_config.get("sink_type", "Monitoring")
        if sink_type == "Checksum":
            # No throughput metrics from a Checksum sink — it writes "count,checksum". Echo each
            # so a correctness A/B can be read straight from the run output, and stash for the result.
            checksums = []
            for f in monitoring_files:
                last = (f.read_text(errors="replace").strip().splitlines() or [""])[-1]
                click.echo(f"  [checksum] {f.name}: {last}")
                checksums.append(last)
            metrics = {"num_queries": len(monitoring_files), "checksum": ";".join(checksums)}
        else:
            csv_type = _detect_csv_type(monitoring_files[0])
            if csv_type == "codec":
                metrics = aggregate_codec_monitoring_results(monitoring_files)
            else:
                metrics = aggregate_monitoring_results(monitoring_files)

        # Persist per-run artifacts (sink CSVs, thread-CPU series, worker log) under results/artifacts/<exp>/<run_id>/
        artifacts = _persist_artifacts(monitoring_dir, config.experiment, run_id)

        # Cleanup monitoring dir (keep it — incl. worker/generator logs — when debugging).
        if os.environ.get("PBENCH_KEEP_LOGS"):
            click.echo(f"  [PBENCH_KEEP_LOGS] run logs kept in {monitoring_dir}")
        else:
            shutil.rmtree(monitoring_dir, ignore_errors=True)

        return self._build_result(config, metrics, run_id=run_id, artifacts=artifacts)

    def _build_result(self, config: RunConfig, metrics: dict[str, Any], run_id: str | None = None,
                      artifacts: str | None = None) -> RunResult:
        qc = config.query_config
        wf = config.worker_flags
        qe = wf.get("query_engine", {})
        dqe = wf.get("default_query_execution", {})

        fmt = qc.get("input_formatter", {})
        input_fmt = fmt.get("type") if isinstance(fmt, dict) else fmt
        ofmt = qc.get("output_formatter", {})
        output_fmt = ofmt.get("type") if isinstance(ofmt, dict) else ofmt

        # Format-invariant rows metric: catalog rows x generator.repeat / MonitoringSink window.
        # (generator.repeat maps to NES_FILE_REPEAT for File sources, so rows scale like the
        # size_of_input_data_in_bytes the sink's MB/s already uses.)
        input_rows = compute_input_rows(config)
        t = metrics.get("processing_time_s")
        mrows = (input_rows / 1e6 / t) if (input_rows and t) else None

        full_config = dict(config.full_config)
        if artifacts:
            full_config["_artifacts"] = artifacts   # results/artifacts/<exp>/<run_id>/ (joinable on run_id too)
        return RunResult(
            **({"run_id": run_id} if run_id else {}),
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
            config=full_config,
            worker_threads=qe.get("number_of_worker_threads"),
            io_threads=qe.get("number_of_io_threads"),
            operator_buffer_size=dqe.get("operator_buffer_size"),
            execution_mode=dqe.get("execution_mode"),
            input_formatter=input_fmt,
            output_formatter=output_fmt,
            invoke_mode=qe.get("invoke_mode_configuration"),
            source_type=qc.get("source_type"),
            sink_type=qc.get("sink_type"),
            num_queries=metrics.get("num_queries", config.num_queries),
            throughput_mbps=metrics.get("throughput_mbps"),
            throughput_sum_mbps=metrics.get("throughput_sum_mbps"),
            throughput_per_query_median=metrics.get("throughput_per_query_median"),
            throughput_per_query_min=metrics.get("throughput_per_query_min"),
            throughput_per_query_max=metrics.get("throughput_per_query_max"),
            processing_time_s=metrics.get("processing_time_s"),
            input_rows=input_rows,
            throughput_mrows_s=mrows,
            avg_latency_us=metrics.get("avg_latency_us"),
            min_latency_us=metrics.get("min_latency_us"),
            max_latency_us=metrics.get("max_latency_us"),
            p50_latency_us=metrics.get("p50_latency_us"),
            p99_latency_us=metrics.get("p99_latency_us"),
            p999_latency_us=metrics.get("p999_latency_us"),
            p99_latency_us_worst=metrics.get("p99_latency_us_worst"),
            codec_main_ratio_min=metrics.get("codec_main_ratio_min"),
            codec_main_ratio_max=metrics.get("codec_main_ratio_max"),
            codec_main_ratio_avg=metrics.get("codec_main_ratio_avg"),
            codec_main_rate_min=metrics.get("codec_main_rate_min"),
            codec_main_rate_max=metrics.get("codec_main_rate_max"),
            codec_main_rate_avg=metrics.get("codec_main_rate_avg"),
            codec_child_ratio_min=metrics.get("codec_child_ratio_min"),
            codec_child_ratio_max=metrics.get("codec_child_ratio_max"),
            codec_child_ratio_avg=metrics.get("codec_child_ratio_avg"),
            codec_child_rate_min=metrics.get("codec_child_rate_min"),
            codec_child_rate_max=metrics.get("codec_child_rate_max"),
            codec_child_rate_avg=metrics.get("codec_child_rate_avg"),
        )


def _independent_generators(config: RunConfig) -> bool:
    """True if each concurrent query should get its own generator process on its own
    port (set `generator.independent_per_query: true`). Default False = one shared
    generator per source. Use for source-count sweeps where a single generator
    process would cap the producer side."""
    return bool((config.generator or {}).get("independent_per_query", False))


def _scan_query_failure(path: Path) -> str | None:
    """Return the first 'Query Failed' line in the worker log, else None.

    The worker process survives a per-query failure, so polling worker_proc.poll()
    never catches it; we have to read the log.
    """
    try:
        for line in path.read_text(errors="replace").splitlines():
            if "Query Failed" in line or "buffer allocation failure" in line:
                return line.strip()
    except OSError:
        pass
    return None


def _tail(path: Path, n: int = 40) -> str:
    """Return the last n lines of a log file (for error messages)."""
    try:
        lines = path.read_text(errors="replace").splitlines()
        return "\n".join(lines[-n:])
    except OSError:
        return f"(could not read {path})"


def _pid_on_port(port: int) -> int | None:
    """Return the pid LISTENING on `port` (the process that actually serves submitted queries). A stale/ghost
    worker holding the port means a freshly-launched worker sits idle -- sampling the port-holder is always the
    process doing the work. Uses ss; None if nothing is listening."""
    try:
        out = subprocess.run(["ss", "-ltnHp", f"sport = :{port}"], capture_output=True, text=True, timeout=5).stdout
    except Exception:
        return None
    m = re.search(r"pid=(\d+)", out)
    return int(m.group(1)) if m else None


class _ThreadCpuSampler(threading.Thread):
    """Per-thread CPU time series for the worker (2026-09-25): every `interval` s read /proc/<pid>/task/*/stat and
    append (wall_s, mono_s, tid, comm, utime_ticks, stime_ticks) to a CSV. Late-spawned threads (DataSrc-N,
    WorkerThread-N appear only after query deploy) are picked up automatically; vanished threads just stop.
    Authoritative per-thread CPU (nes-profile gotcha #10: pidstat/ps %cpu lie; /proc task stat does not).
    CLK_TCK is written in the header so ticks -> seconds is self-contained. Lets CPU-per-row be derived AFTER
    the fact for any window (e.g. the MonitoringSink window) without re-running."""

    def __init__(self, pid: int, out_path: Path, interval: float = 0.5):
        super().__init__(daemon=True)
        self.pid, self.out_path, self.interval = pid, out_path, interval
        self._stop_evt = threading.Event()   # NOT _stop: threading.Thread has an internal _stop() method

    def _sample(self, fh) -> None:
        wall, mono = time.time(), time.monotonic()
        task_dir = f"/proc/{self.pid}/task"
        try:
            tids = os.listdir(task_dir)
        except OSError:
            return
        for tid in tids:
            try:
                raw = open(f"{task_dir}/{tid}/stat").read()
            except OSError:
                continue
            # comm may contain spaces; it is delimited by the LAST ')'
            rp = raw.rfind(")")
            comm = raw[raw.find("(") + 1 : rp]
            fields = raw[rp + 2 :].split()
            utime, stime = fields[11], fields[12]   # fields 14/15 of the full stat line
            fh.write(f"{wall:.3f},{mono:.3f},{tid},{comm},{utime},{stime}\n")
        fh.flush()

    def run(self) -> None:
        with open(self.out_path, "w") as fh:
            fh.write(f"# pid={self.pid} clk_tck={os.sysconf('SC_CLK_TCK')} interval_s={self.interval}\n")
            fh.write("wall_s,mono_s,tid,comm,utime_ticks,stime_ticks\n")
            while not self._stop_evt.is_set():
                self._sample(fh)
                self._stop_evt.wait(self.interval)
            self._sample(fh)   # final sample after stop is requested (before the worker is terminated)

    def stop(self) -> None:
        self._stop_evt.set()
        self.join(timeout=5)


def _persist_artifacts(monitoring_dir: Path, experiment: str, run_id: str) -> str | None:
    """Copy the run's MonitoringSink CSV(s), thread-CPU series and worker log to results/artifacts/<experiment>/<run_id>/
    (persistent, joinable on run_id) so any later visualisation can be re-derived without re-running.
    Disable with PBENCH_NO_ARTIFACTS=1."""
    if os.environ.get("PBENCH_NO_ARTIFACTS"):
        return None
    root = Path(__file__).resolve().parents[2] / "results" / "artifacts" / experiment / run_id
    root.mkdir(parents=True, exist_ok=True)
    for f in sorted(monitoring_dir.glob("*.csv")) + [monitoring_dir / "worker.log", monitoring_dir / "aux" / "threadcpu.csv"]:
        if f.exists():
            shutil.copy2(f, root / f.name)
    return str(root.relative_to(root.parents[3]))


def _terminate(proc: subprocess.Popen) -> None:
    """Gracefully terminate a process (SIGTERM then SIGKILL)."""
    if proc.poll() is not None:
        return
    proc.terminate()
    try:
        proc.wait(timeout=_KILL_GRACE)
    except subprocess.TimeoutExpired:
        proc.kill()
        proc.wait(timeout=2)
