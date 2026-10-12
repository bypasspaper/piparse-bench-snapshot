"""CLI entry point: pbench run, pbench plan, pbench validate, pbench queries."""

from __future__ import annotations

import socket
import subprocess
import sys
import uuid
from collections import defaultdict
from datetime import datetime
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

import click
import yaml

from .expander import expand_experiment
from .models import ExplicitSeries, ParameterSeries, RunConfig


def _baseline_series_name(variant) -> str | None:
    """Return the series name that corresponds to the baseline, or None."""
    s = getattr(variant.axes, "series", None)
    if s is None:
        return None
    if isinstance(s, ParameterSeries):
        return str(s.baseline) if s.baseline is not None else None
    if isinstance(s, ExplicitSeries):
        for entry in s.explicit:
            if entry.baseline:
                return entry.name
    return None
from .validation import validate_experiment
from .renderer import render_for_config
from .resolver import load_experiment, load_query, QUERIES_DIR
from .store import Store


@click.group()
def main():
    """PiParse Benchmarking Framework."""
    pass


# ── Branch switching ──────────────────────────────────────────────────────────

@dataclass
class BranchContext:
    """Tracks branch state and handles switching + rebuilding."""
    repo_root: Path
    cmake_configure: str | None = None
    cmake_build: str | None = None
    current_branch: str | None = field(default=None, init=False)

    def __post_init__(self) -> None:
        # Seed with the repo's actual current branch so switch() is a no-op when the
        # experiment already targets the checked-out branch. Without this, the first
        # switch() ALWAYS sees current_branch=None != target and demands
        # --cmake-configure/--cmake-build (sys.exit(1)) even when nothing needs rebuilding.
        try:
            self.current_branch = subprocess.run(
                ["git", "rev-parse", "--abbrev-ref", "HEAD"],
                cwd=self.repo_root, capture_output=True, text=True, check=True,
            ).stdout.strip() or None
        except Exception:
            self.current_branch = None

    def switch(self, branch: str | None) -> None:
        """Switch to the given branch and rebuild if needed."""
        if branch is None or branch == self.current_branch:
            return

        if self.cmake_configure is None and self.cmake_build is None:
            click.echo(
                f"  Branch switch to '{branch}' requested but no cmake commands provided.\n"
                f"  Use --cmake-configure and/or --cmake-build to enable automatic rebuilds.",
                err=True,
            )
            sys.exit(1)

        click.echo(f"\n  Switching to branch: {branch}")
        subprocess.run(
            ["git", "checkout", branch],
            cwd=self.repo_root, check=True,
        )

        if self.cmake_configure:
            click.echo(f"  Configuring: {self.cmake_configure}")
            subprocess.run(
                self.cmake_configure, shell=True,
                cwd=self.repo_root, check=True,
            )

        if self.cmake_build:
            click.echo(f"  Building: {self.cmake_build}")
            subprocess.run(
                self.cmake_build, shell=True,
                cwd=self.repo_root, check=True,
            )

        self.current_branch = branch
        click.echo(f"  Build complete for branch: {branch}\n")


# ── pbench plan ────────────────────────────────────────────────────────────────

@main.command()
@click.argument("experiment_paths", nargs=-1, required=True, type=click.Path(exists=True))
@click.option("--render", is_flag=True, help="Also render .test files and print them")
def plan(experiment_paths: tuple[str, ...], render: bool):
    """Dry run: expand experiment(s), show execution plan."""
    for exp_path in experiment_paths:
        exp_path = Path(exp_path)
        if exp_path.is_dir():
            yamls = sorted(exp_path.glob("*.yaml"))
        else:
            yamls = [exp_path]

        for yaml_path in yamls:
            _plan_one(yaml_path, render)


def _plan_one(yaml_path: Path, render: bool) -> None:
    click.echo(f"\n{'='*70}")
    click.echo(f"Experiment: {yaml_path}")
    click.echo(f"{'='*70}")

    experiment = load_experiment(yaml_path)
    meta = experiment.experiment
    click.echo(f"  Name:        {meta.name}")
    click.echo(f"  Description: {meta.description}")
    click.echo(f"  Repetitions: {meta.repetitions}")
    click.echo(f"  Metric:      {meta.primary_metric} ({'higher' if meta.higher_is_better else 'lower'} is better)")

    configs = expand_experiment(experiment)

    # Group by variant and series
    by_variant: dict[str, dict[str, list[RunConfig]]] = defaultdict(lambda: defaultdict(list))
    for rc in configs:
        by_variant[rc.variant][rc.series].append(rc)

    total_runs = len(configs)
    unique_points = total_runs // meta.repetitions
    click.echo(f"\n  Total runs:     {total_runs}")
    click.echo(f"  Unique points:  {unique_points}")
    click.echo(f"  Repetitions:    {meta.repetitions}")

    # Group by branch for build planning
    branches = set(rc.branch for rc in configs if rc.branch)
    if branches:
        click.echo(f"  Branches:       {', '.join(sorted(branches))}")

    for variant_name, series_dict in by_variant.items():
        variant = next(v for v in experiment.variants if v.name == variant_name)
        click.echo(f"\n  Variant: {variant_name}")
        query_refs = variant.get_query_refs()
        if len(query_refs) == 1:
            click.echo(f"    Query: {query_refs[0]}")
        else:
            click.echo(f"    Queries ({len(query_refs)}): {', '.join(query_refs)}")
            click.echo(f"    Query aggregation: {variant.query_aggregation.value}")
        click.echo(f"    Y:     {variant.y.metric} ({variant.y.label})")

        for series_name, rcs in series_dict.items():
            x_values = sorted(set(rc.x_value for rc in rcs))
            click.echo(f"    Series '{series_name}': {len(x_values)} x-values × {meta.repetitions} reps = {len(rcs)} runs")
            click.echo(f"      X ({rcs[0].x_param}): {x_values}")

        if render:
            sample = next(iter(next(iter(series_dict.values()))))
            click.echo(f"\n    --- Sample render, mode={sample.mode} (first config) ---")
            # Flink rendering needs a testdata root; use a placeholder for this preview (the
            # actual runner substitutes --testdata-dir). Other modes take no extra kwargs.
            render_kwargs = ({"testdata_dir": "<TESTDATA_DIR>"}
                             if sample.mode in ("flink", "spark") else {})
            try:
                rendered = render_for_config(sample, **render_kwargs)
            except Exception as e:  # never let a preview crash `plan`
                rendered = f"[render unavailable: {e}]"
            for line in rendered.split("\n"):
                click.echo(f"    {line}")
            click.echo(f"    --- end ---")


# ── pbench run ─────────────────────────────────────────────────────────────────

def _detach_and_exit(log_file: str | None) -> None:
    """Re-exec this exact `pbench run` in a NEW SESSION and exit the launcher.

    Why: long experiment runs launched from supervised shells (agent-harness background
    tasks, ssh sessions, terminal multiplexers) die when the supervisor reaps its child
    process group — this killed two ~20-minute head-to-head runs mid-flight (2026-07-14).
    start_new_session=True (setsid) detaches the child from the launcher's process group
    and controlling terminal, so no group-directed signal can reach it; stdio goes to a
    logfile that ends with the normal 'Done:' summary. A watchdog should poll the PRINTED
    PID (kill -0), never `pgrep -f` a pattern that may match itself.
    """
    import os

    log_path = Path(log_file) if log_file else (
        Path("results/logs") / f"pbench_run_{datetime.now().strftime('%Y%m%dT%H%M%S')}.log")
    log_path.parent.mkdir(parents=True, exist_ok=True)
    argv = [a for a in sys.argv if a != "--detach"]
    # strip a --log-file/--log-file=... pair too — the child must not re-detach or re-parse it
    cleaned: list[str] = []
    skip_next = False
    for a in argv:
        if skip_next:
            skip_next = False
            continue
        if a == "--log-file":
            skip_next = True
            continue
        if a.startswith("--log-file="):
            continue
        cleaned.append(a)
    env = {**os.environ, "PYTHONUNBUFFERED": "1"}  # line-granular progress for tail -f
    # argv[0] is bench.py or the pbench console script — both are python source, so
    # re-launching through sys.executable works for either invocation style.
    with open(log_path, "wb") as log:
        proc = subprocess.Popen(
            [sys.executable] + cleaned,
            stdout=log, stderr=subprocess.STDOUT, stdin=subprocess.DEVNULL,
            start_new_session=True, env=env,
        )
    click.echo(f"[detached] pid={proc.pid} log={log_path}")
    click.echo(f"[detached] follow with: tail -f {log_path}   (ends with the 'Done:' summary)")
    sys.exit(0)


@main.command()
@click.argument("experiment_paths", nargs=-1, required=True, type=click.Path(exists=True))
@click.option("--build-dir", default=None, type=click.Path(exists=True),
              help="NebulaStream build directory (required for NES modes: systest/bench-runner/worker-cli)")
@click.option("--generator-bin", default=None, type=click.Path(),
              help="Path to tcp-generator binary (required for worker-cli mode with TCP sources)")
@click.option("--testdata-dir", default=None, type=click.Path(),
              help="Data root for Flink sources (default: <build-dir>/nes-systests/testdata)")
@click.option("--flink-python", default=None, type=click.Path(),
              help="Python interpreter with pyflink for mode: flink (default: pbench's own interpreter)")
@click.option("--spark-python", default=None, type=click.Path(),
              help="Python interpreter with pyspark for mode: spark (default: pbench's own interpreter)")
@click.option("--db", default=None, type=click.Path(), help="DuckDB path (default: results/piparse_bench.duckdb)")
@click.option("--dry-run", is_flag=True, help="Validate and expand but don't execute")
@click.option("--remote", is_flag=True, help="Run on remote benchmark server (uses remote.yaml)")
@click.option("--remote-config", default=None, type=click.Path(), help="Path to remote.yaml")
@click.option("--fetch/--no-fetch", default=False, help="Fetch results after remote run")
@click.option("--cmake-configure", default=None, type=str,
              help="CMake configure command to run in repo root on branch switch")
@click.option("--cmake-build", default=None, type=str,
              help="CMake build command to run in repo root on branch switch")
@click.option("--detach", is_flag=True,
              help="Re-exec the run in its OWN session (setsid) with output to a logfile, "
                   "print pid + log path, and return immediately. The run then survives the "
                   "launcher dying or killing its process group (Ctrl-C'd terminals, dropped "
                   "ssh sessions, agent-harness background-task reaping). Follow progress "
                   "with tail -f; the log ends with the normal 'Done:' summary.")
@click.option("--log-file", default=None, type=click.Path(),
              help="Logfile for --detach (default: results/logs/pbench_run_<timestamp>.log)")
@click.option("--worker-port", default=8080, type=int,
              help="gRPC port the NES worker binds to (default 8080). Change it to sidestep a "
                   "port already held by another session's worker.")
def run(experiment_paths: tuple[str, ...], build_dir: str | None, generator_bin: str | None,
        testdata_dir: str | None, flink_python: str | None, spark_python: str | None,
        db: str | None, dry_run: bool, remote: bool, remote_config: str | None, fetch: bool,
        cmake_configure: str | None, cmake_build: str | None,
        detach: bool, log_file: str | None, worker_port: int):
    """Run one or more experiments."""
    if detach:
        _detach_and_exit(log_file)
    if remote:
        from .remote import RemoteConfig, deploy as do_deploy, run_remote, fetch as do_fetch
        cfg = RemoteConfig.load(remote_config)

        click.echo(f"Deploying to {cfg.ssh_target}...")
        do_deploy(cfg)

        click.echo(f"Running remotely...")
        extra = []
        if dry_run:
            extra.append("--dry-run")
        run_remote(cfg, list(experiment_paths), extra_args=extra, verbose=True)

        if fetch:
            click.echo(f"Fetching results...")
            do_fetch(cfg)

        return

    from .runners.flink import FlinkRunner
    from .runners.spark import SparkRunner

    # NES runners need a build dir; the Flink/Spark runners do not. --build-dir is therefore
    # required only when the experiment uses a NES mode (systest/bench-runner/worker-cli) —
    # a Flink-/Spark-only experiment can run without it. Missing NES runners surface later as a
    # clear "No runner for mode 'systest'" per-config error (see _run_one).
    runners: dict[str, Any] = {}
    branch_ctx: BranchContext | None = None
    if build_dir is not None:
        from .runners.systest import SystestRunner
        from .runners.bench_runner import BenchRunnerRunner
        from .runners.worker_cli import WorkerCliRunner

        repo_root = Path(build_dir).parent
        runners["systest"] = SystestRunner(build_dir=build_dir)
        runners["bench-runner"] = BenchRunnerRunner(build_dir=build_dir)
        runners["worker-cli"] = WorkerCliRunner(
            build_dir=build_dir, generator_bin=generator_bin, worker_port=worker_port)
        branch_ctx = BranchContext(
            repo_root=repo_root,
            cmake_configure=cmake_configure,
            cmake_build=cmake_build,
        )

    # Flink: testdata defaults to the NES testdata tree when --build-dir is given, so the
    # head-to-head needs no extra flag.
    if testdata_dir is None and build_dir is not None:
        testdata_dir = str(Path(build_dir) / "nes-systests" / "testdata")
    runners["flink"] = FlinkRunner(testdata_dir=testdata_dir, python_bin=flink_python)
    runners["spark"] = SparkRunner(testdata_dir=testdata_dir, python_bin=spark_python)

    store = Store(db_path=db) if db else Store()
    hostname = socket.gethostname()

    # One timestamped, sortable id for this whole `pbench run` invocation. Stamped on
    # every result so the `aggregated` view can scope each experiment to its latest run
    # (no stale-history contamination). Shared across the experiments in one invocation.
    run_group = f"{datetime.now():%Y%m%dT%H%M%S}-{uuid.uuid4().hex[:8]}"
    click.echo(f"run_group: {run_group}")

    for exp_path in experiment_paths:
        exp_path = Path(exp_path)
        if exp_path.is_dir():
            yamls = sorted(exp_path.rglob("*.yaml"))
        else:
            yamls = [exp_path]

        for yaml_path in yamls:
            _run_one(yaml_path, runners, store, hostname, dry_run, branch_ctx, run_group)

    store.close()


def _run_one(
    yaml_path: Path,
    runners: dict[str, Any],
    store: Store,
    hostname: str,
    dry_run: bool,
    branch_ctx: BranchContext | None = None,
    run_group: str | None = None,
) -> None:
    experiment = load_experiment(yaml_path)

    # Validate experiment structure (baseline, query refs, etc.)
    struct_errors = validate_experiment(experiment)
    if struct_errors:
        click.echo(f"Experiment validation errors in {yaml_path}:", err=True)
        for e in struct_errors:
            click.echo(f"  - {e}", err=True)
        sys.exit(1)

    configs = expand_experiment(experiment)
    meta = experiment.experiment

    click.echo(f"\nExperiment: {meta.name} ({len(configs)} runs)")

    # Validate all configs against the appropriate runner
    errors = []
    for rc in configs:
        runner = runners.get(rc.mode)
        if runner is None:
            errors.append(f"No runner for mode '{rc.mode}'. Available: {list(runners.keys())}")
        else:
            errors.extend(runner.validate(rc))
    if errors:
        # Deduplicate
        seen = set()
        for e in errors:
            if e not in seen:
                click.echo(f"  - {e}", err=True)
                seen.add(e)
        sys.exit(1)

    if dry_run:
        click.echo("Dry run — skipping execution.")
        return

    # Group by branch for efficient switching
    by_branch: dict[str | None, list[RunConfig]] = defaultdict(list)
    for rc in configs:
        by_branch[rc.branch].append(rc)

    completed = 0
    failed = 0

    for branch, branch_configs in by_branch.items():
        if branch_ctx and branch:
            branch_ctx.switch(branch)
        elif branch:
            click.echo(f"\n  Branch: {branch}")

        # Resolve the actual HEAD commit for this branch group (recorded per result for
        # provenance; was previously left as None in the store).
        git_commit = None
        if branch_ctx:
            try:
                git_commit = subprocess.run(
                    ["git", "rev-parse", "--short", "HEAD"],
                    cwd=branch_ctx.repo_root, capture_output=True, text=True, check=True,
                ).stdout.strip() or None
            except Exception:
                git_commit = None

        for rc in branch_configs:
            label = f"  [{completed+failed+1}/{len(configs)}] {rc.variant}/{rc.series} x={rc.x_value} rep={rc.repetition}"
            if rc.query_ref:
                label += f" [{rc.query_ref}]"
            click.echo(label, nl=False)

            try:
                runner = runners[rc.mode]
                result = runner.execute(rc)
                result.hostname = hostname
                result.run_group = run_group
                if git_commit:
                    result.git_commit = git_commit
                store.insert(result)
                completed += 1

                tp = result.throughput_mbps
                tp_str = f" → {tp:.1f} MB/s" if tp is not None else ""
                click.echo(tp_str)

            except Exception as e:
                failed += 1
                click.echo(f" FAILED: {e}", err=True)

    click.echo(f"\nDone: {completed} completed, {failed} failed")

    if completed > 0:
        click.echo(f"\n{store.get_results_summary(meta.name)}")


# ── pbench validate ────────────────────────────────────────────────────────────

@main.command()
@click.argument("experiment_paths", nargs=-1, required=True, type=click.Path(exists=True))
def validate(experiment_paths: tuple[str, ...]):
    """Validate experiment YAML(s) without running."""
    all_ok = True
    for exp_path in experiment_paths:
        exp_path = Path(exp_path)
        if exp_path.is_dir():
            yamls = sorted(exp_path.rglob("*.yaml"))
        else:
            yamls = [exp_path]

        for yaml_path in yamls:
            try:
                experiment = load_experiment(yaml_path)

                # Structural validation (baseline, query refs, etc.)
                struct_errors = validate_experiment(experiment)
                if struct_errors:
                    for e in struct_errors:
                        click.echo(f"  ERR {yaml_path}: {e}", err=True)
                    all_ok = False
                    continue

                configs = expand_experiment(experiment)

                # Validate query catalog references
                for variant in experiment.variants:
                    for qref in variant.get_query_refs():
                        load_query(qref)

                click.echo(f"  OK  {yaml_path} ({len(configs)} runs)")
            except Exception as e:
                click.echo(f"  ERR {yaml_path}: {e}", err=True)
                all_ok = False

    if not all_ok:
        sys.exit(1)


# ── pbench queries ─────────────────────────────────────────────────────────────

@main.command()
def queries():
    """List available queries from the catalog."""
    for yaml_path in sorted(QUERIES_DIR.rglob("*.yaml")):
        ref = str(yaml_path.relative_to(QUERIES_DIR).with_suffix(""))
        try:
            q = load_query(ref)
            click.echo(f"  {ref:40s} {q.description or q.sql[:60]}")
        except Exception as e:
            click.echo(f"  {ref:40s} (error: {e})")


# ── pbench results ─────────────────────────────────────────────────────────────

@main.command()
@click.argument("experiment_name")
@click.option("--db", default=None, type=click.Path(), help="DuckDB path")
@click.option("--reset", is_flag=True, help="Delete all stored results for this experiment")
def results(experiment_name: str, db: str | None, reset: bool):
    """Show results summary for an experiment."""
    store = Store(db_path=db) if db else Store()
    if reset:
        count = store.delete_experiment(experiment_name)
        click.echo(f"Deleted {count} runs for experiment '{experiment_name}'.")
    else:
        click.echo(store.get_results_summary(experiment_name))
    store.close()


# ── pbench export ──────────────────────────────────────────────────────────────

@main.command()
@click.argument("experiment_name")
@click.option("--db", default=None, type=click.Path(), help="DuckDB path")
@click.option("--output-dir", default=None, type=click.Path(), help="Output directory for CSV")
def export(experiment_name: str, db: str | None, output_dir: str | None):
    """Export aggregated results to CSV."""
    store = Store(db_path=db) if db else Store()
    csv_path = store.export_csv(experiment_name, output_dir)
    click.echo(f"Exported to {csv_path}")
    store.close()


# ── pbench viz ─────────────────────────────────────────────────────────────────

@main.command()
@click.argument("experiment_paths", nargs=-1, required=True, type=click.Path(exists=True))
@click.option("--db", default=None, type=click.Path(), help="DuckDB path")
@click.option("--output-dir", default=None, type=click.Path(), help="Output directory for figures")
@click.option("--format", "fmt", type=click.Choice(["pdf", "tikz", "both"]), default="both",
              help="Output format")
def viz(experiment_paths: tuple[str, ...], db: str | None, output_dir: str | None, fmt: str):
    """Generate figures from stored results."""
    from .viz.charts import render_chart
    from .viz.tikz import render_tikz

    store = Store(db_path=db) if db else Store()
    fig_dir = Path(output_dir) if output_dir else Path("results") / "figures"
    fig_dir.mkdir(parents=True, exist_ok=True)

    for exp_path in experiment_paths:
        exp_path = Path(exp_path)
        if exp_path.is_dir():
            yamls = sorted(exp_path.glob("*.yaml"))
        else:
            yamls = [exp_path]

        for yaml_path in yamls:
            experiment = load_experiment(yaml_path)
            _viz_experiment(experiment, store, fig_dir, fmt)

    store.close()


def _viz_experiment(experiment, store: Store, fig_dir: Path, fmt: str) -> None:
    from .viz.charts import compute_shared_y_limits, render_chart
    from .viz.tikz import render_tikz

    exp_name = experiment.experiment.name
    default_metric = experiment.experiment.primary_metric

    # First pass: collect data per variant and group by Y metric for shared scaling
    variant_data: dict[str, list[dict]] = {}
    y_metric_groups: dict[str, list[str]] = defaultdict(list)

    for variant in experiment.variants:
        if variant.visualization is None:
            continue

        variant_metric = variant.y.metric or default_metric
        is_multi = variant.queries is not None and len(variant.queries) > 1
        if is_multi:
            data = store.get_aggregated_multi_query(
                exp_name,
                metric=variant_metric,
                aggregation=variant.query_aggregation,
            )
            data = [r for r in data if r.get("variant") == variant.name]
        else:
            data = store.get_aggregated(exp_name)
            data = [r for r in data if r["variant"] == variant.name]

        variant_data[variant.name] = data
        # Variants opted out via `shared_y: false` still get their own chart
        # but don't contribute to — nor consume — the shared Y-limit for their metric.
        if data and variant.visualization.shared_y:
            y_metric_groups[variant.y.metric].append(variant.name)

    # Compute shared Y limits for groups with >1 variant sharing the same Y metric
    shared_y_limits: dict[str, tuple[float, float] | None] = {}
    for y_metric, variant_names in y_metric_groups.items():
        if len(variant_names) > 1:
            datasets = [variant_data[vn] for vn in variant_names]
            shared_y_limits[y_metric] = compute_shared_y_limits(datasets, y_metric)

    # Second pass: render charts with shared Y limits where applicable
    for variant in experiment.variants:
        viz_spec = variant.visualization
        if viz_spec is None:
            click.echo(f"  SKIP {exp_name}/{variant.name} (no visualization spec)")
            continue

        data = variant_data.get(variant.name, [])
        if not data:
            click.echo(f"  SKIP {exp_name}/{variant.name} (no data)")
            continue

        # Determine labels
        x_label = viz_spec.x_label or variant.axes.x.label
        y_label = viz_spec.y_label or variant.y.label
        series_label = variant.axes.series.label if variant.axes.series else None
        y_limits = shared_y_limits.get(variant.y.metric) if viz_spec.shared_y else None
        baseline_name = _baseline_series_name(variant)

        base_name = f"{exp_name}_{variant.name}"
        data_csv = fig_dir / f"{base_name}_data.csv"

        variant_metric = variant.y.metric or default_metric

        if fmt in ("pdf", "both"):
            pdf_path = fig_dir / f"{base_name}.pdf"
            render_chart(data, variant_metric, viz_spec, x_label, y_label, pdf_path, data_csv, series_label=series_label, y_limits=y_limits, baseline=baseline_name)
            click.echo(f"  PDF  {pdf_path}")

        if fmt in ("tikz", "both"):
            tikz_path = fig_dir / f"{base_name}.tikz"
            render_tikz(data, variant_metric, viz_spec, x_label, y_label, tikz_path, data_csv, series_label=series_label, y_limits=y_limits, baseline=baseline_name)
            click.echo(f"  TikZ {tikz_path}")


# ── pbench deploy ──────────────────────────────────────────────────────────────

@main.command()
@click.option("--config", "config_path", default=None, type=click.Path(),
              help="Path to remote.yaml")
@click.option("-v", "--verbose", is_flag=True)
def deploy(config_path: str | None, verbose: bool):
    """Deploy the framework to the remote benchmark server."""
    from .remote import RemoteConfig, deploy as do_deploy
    cfg = RemoteConfig.load(config_path)
    click.echo(f"Deploying to {cfg.ssh_target}:{cfg.deploy_path}")
    do_deploy(cfg, verbose=verbose)
    click.echo("Done.")


# ── pbench fetch ───────────────────────────────────────────────────────────────

@main.command()
@click.option("--config", "config_path", default=None, type=click.Path(),
              help="Path to remote.yaml")
@click.option("-v", "--verbose", is_flag=True)
def fetch(config_path: str | None, verbose: bool):
    """Fetch results from the remote benchmark server."""
    from .remote import RemoteConfig, fetch as do_fetch
    cfg = RemoteConfig.load(config_path)
    click.echo(f"Fetching results from {cfg.ssh_target}")
    do_fetch(cfg, verbose=verbose)
    click.echo("Done.")
