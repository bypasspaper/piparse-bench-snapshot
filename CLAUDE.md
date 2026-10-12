# piparse-bench

YAML-driven benchmarking framework for PiParse (integrated into NebulaStream).
Configures, executes, stores, and visualizes benchmark results.

## Architecture

```
Experiment YAML → Resolve → Expand → Render → Execute → Store → Visualize
```

- **One YAML per experiment**, each producing one or more figures
- **Query catalog** (`queries/`): central definitions, experiments reference by name
- **Two execution modes**: `systest` (file sources, JSON output) and `worker-cli` (TCP sources, MonitoringSink CSV)
- **DuckDB** for results storage, **matplotlib** + **pgfplots/TikZ** for visualization

## Key directories

| Path | Purpose |
|------|---------|
| `piparse_bench/` | Python package (all orchestration logic) |
| `piparse_bench/models.py` | Pydantic data models — start here to understand the data flow |
| `piparse_bench/cli.py` | Click CLI — all 9 commands |
| `piparse_bench/expander.py` | Variant → List[RunConfig] via cartesian product |
| `piparse_bench/renderer.py` | Query catalog + config → `.test` file or CLI YAML |
| `piparse_bench/runners/` | `systest.py` (invoke binary), `worker_cli.py` (start worker + generators) |
| `piparse_bench/store.py` | DuckDB schema, insert, aggregation, CSV export |
| `piparse_bench/viz/` | `charts.py` (matplotlib), `tikz.py` (pgfplots) |
| `piparse_bench/validation.py` | Baseline enforcement, dependency rules, query ref checks |
| `piparse_bench/monitoring.py` | MonitoringSink CSV parser + multi-query aggregation |
| `piparse_bench/remote.py` | SSH deploy/run/fetch via rsync |
| `piparse_bench/docker.py` | Docker build and container execution |
| `queries/` | Query catalog (one YAML per query, organized by benchmark) |
| `configs/` | Parameter registry, topologies, invoke modes |
| `experiments/` | Experiment definitions |
| `generator/` | Rust TCP data generator (tokio async, rate-limited) |

## Design document

The full design spec is in `DESIGN.md` in the parent directory (not in this repo).
It defines the YAML schemas, rendering rules, execution flow, and DuckDB schema.
When in doubt about how something should work, check `DESIGN.md` first.

## Development setup

```bash
python3 -m venv .venv
.venv/bin/pip install -e ".[dev,viz]"

# Build the TCP generator
cd generator && cargo build --release
```

## CLI commands

```bash
pbench run --build-dir <NES_BUILD> experiments/format_scalability.yaml
pbench run --remote --fetch experiments/format_scalability.yaml   # remote execution
pbench plan experiments/format_scalability.yaml                   # dry run
pbench plan --render experiments/format_scalability.yaml          # dry run + show .test
pbench validate experiments/                                      # check all YAMLs
pbench viz experiments/format_scalability.yaml                    # generate figures
pbench viz --format tikz experiments/format_scalability.yaml      # TikZ only
pbench export <experiment_name>                                   # CSV from DuckDB
pbench results <experiment_name>                                  # summary table
pbench queries                                                    # list catalog
pbench deploy                                                     # rsync to server
pbench fetch                                                      # rsync results back
```

## Conventions

- **Experiment YAML schema**: see `models.py:ExperimentFile`, `Variant`, `Axes`
- **Query catalog schema**: see `models.py:QueryCatalogEntry`
- **Axes types**: `SimpleAxis` (one param sweep), `CartesianAxis` (N×M product), `ExplicitAxis` (named entries with overrides)
- **Series types**: `ParameterSeries` (one param varies) or `ExplicitSeries` (named entries with overrides, branches, systems)
- **Baseline**: every variant with a series axis must designate exactly one baseline
- **Multi-query variants**: `queries: [...]` + `query_aggregation: geometric_mean` — runs all queries, aggregates with geomean
- **Dependency rules**: defined in `configs/parameters.yaml` under `dependencies`, validated automatically

## Buffer-pool sizing (CRITICAL — read before running stateful queries)

The worker's global buffer pool is `worker.number_of_buffers_in_global_buffer_manager` (a COUNT) ×
`worker.default_query_execution.operator_buffer_size` (bytes/buffer). The right setting depends on query class —
there is **no single default that works for both**:

- **Stateless / passthrough / filter / map** (no windows): wants **few BIG buffers**. Optimum ≈ 4096 × 128 KiB
  (512 MB); throughput is non-monotonic and the 8.59 GB production default is the *worst* point (−37%). Total
  in-flight data must stay < pool because MonitoringSink finalizes only at EoS (so for passthrough keep
  `generator.repeat`×data < pool bytes).

- **Stateful (windowed JOIN / aggregation)**: state (slices / hash tables) is consumed by buffer **COUNT, not
  bytes** — you **cannot trade buffer size for count**. Passthrough-tuned pools (e.g. 4096–65536 buffers) FAIL
  with `Global buffer pool could not allocate buffer before timeout`. Use **small buffers (4 KiB) + a high count**
  (the engine's own large-scale config = `2,000,000 × 4 KiB = 8 GB`, W=4, `slice_cache.enable_slice_cache: true`).
  Measured count floors (File, W=4, 4 KiB, bid/auction/person large): **q8 ≤64K, q5 ≤250K, q3 ~2M**.

- **Hard limits**: `count × operator_buffer_size` must fit RAM (benchmark box = 64 GB). And the pool count is the
  binding constraint for stateful state, so size by COUNT.

- **⚠️ KNOWN ENGINE BUG — windows only trigger when data ARRIVES.** A pre-join/pre-window filter that thins a
  windowed input means its windows stop triggering → slice state is never freed → unbounded accumulation (needs a
  huge pool or OOMs). This is why **q3** (`auction WHERE category==10` upstream of a tumbling join) needs ~2M
  buffers while **q5/q8** (no pre-join filter) run at ≤64K–250K. Not a pbench/query bug — a real engine windowing
  bug. When a stateful query's pool appetite looks pathological, check for a filter feeding a windowed operator.

- **Runner behavior**: `worker_cli.py` fast-fails on a logged `Query Failed` / `buffer allocation failure` (the
  worker survives a per-query failure, so without this the completion poll burns the full `completion_timeout_s`).

## Adding a new query

1. Create `queries/<benchmark>/<name>.yaml` following the `QueryCatalogEntry` schema
2. Include: `name`, `sql`, `sources` (with schema + data files), `sink_schema`, `validation` (tuples + checksum)
3. Reference it from experiments as `<benchmark>/<name>` (e.g. `nexmark/bid_star`)

## Adding a new experiment

1. Create `experiments/<name>.yaml` following the `ExperimentFile` schema
2. Define `defaults` (system, branch, mode, worker config, query_config)
3. Define `variants` with axes (x + optional series), y metric, visualization type
4. Run `pbench validate` and `pbench plan` before executing

## Flink backend (cross-engine comparison via `mode: flink`)

pbench can benchmark **Apache Flink** alongside NebulaStream so the two engines land in one figure.
The runner is `piparse_bench/runners/flink.py` (PyFlink Table API/SQL). `render_flink`
(`renderer.py`) emits a self-contained PyFlink driver that pbench runs as a **subprocess per rep**
(fresh JVM each — fair vs NES's re-exec). PyFlink Table/SQL compiles to native JVM operators, so
there is **no per-record Python** in the data path (only Python UDFs would be slow; we use none).

- **Install (opt-in extra):** `uv pip install -e ".[flink]"`. Pin is `apache-flink>=2.0,<2.2` —
  2.0/2.1 support **Java 11 AND Python 3.12** (this box), so PyFlink co-installs into the same
  `.venv`; 2.2+ needs Java 17, 1.20 needs Python ≤3.11 (both avoided). Java 11 already present.
- **Query porting:** add a `flink_sql:` (Flink-dialect SELECT) to the query catalog YAML — Flink's
  dialect differs (backtick reserved words like `` `timestamp` ``, `CAST(x AS DOUBLE)` not
  `FLOAT64(x)`, TUMBLE/HOP windowing TVFs). The source `CREATE TABLE` + blackhole sink DDL are
  auto-derived from the catalog `sources`/`schema` (NES→Flink type map in `renderer.py`). Only
  `q1_price` is ported so far.
- **How to run:** an ExplicitSeries with a Flink entry that overrides `mode`, e.g.
  `- {name: Flink, system: flink, overrides: {mode: flink, flink_execution_mode: streaming}}`
  (see `experiments/flink_vs_nes_q1.yaml`). `--build-dir` is optional for Flink-only runs;
  `--testdata-dir` defaults to `<build-dir>/nes-systests/testdata`; `--flink-python` defaults to
  pbench's own interpreter. Both engines must run in ONE `pbench run` (shared `run_group`) or the
  `aggregated` view drops the older one; they must differ by **series** (the view collapses `system`).
- **Fairness = parse-focused:** Flink `blackhole` sink vs NES `Void`/`Monitoring`. NES's `Void`
  **does not report throughput in systest `-b`** (use `Monitoring`/`Checksum`); `Monitoring` still
  runs the CSV output-formatter, so NES does slightly MORE work than Flink-blackhole → the
  comparison is conservative for NES. Headline = `throughput_mrows_s`; MB/s alongside.
- **Correctness = tuple-count parity**, not NES's checksum (unreproducible in Flink). The driver's
  `--count` mode prints `ROWS=` (forces BATCH — a streaming global `COUNT(*)` emits a per-record
  changelog and hangs/misreports). For a filterless projection, ROWS == input lines == `wc -l`.
- **Single-stream parse is the point, not a gap:** Flink couples read+parse in one non-splittable
  split → ONE thread reads AND parses a single CSV file → FLAT ~110 MB/s across parallelism 1→8,
  streaming↔batch, and every efficiency knob (verified). NES parallel-parses one stream (sequential
  read → shared queue → M parser threads) → ~6 GB/s at 8 threads. This ~54x gap is architectural,
  not under-tuning. **Do NOT "fix" it by sharding the file or opening N connections** — that turns
  one stream into many and measures a different, easier workload. `render_flink` already applies
  Flink's max-efficiency config (object-reuse, chaining, checkpointing off, latency-metrics off);
  memory is left at defaults so future stateful queries aren't starved. Measure steady state (the
  6.4 GB `large` input warms the JIT, ~+15% over `medium`).
- **Gotchas:** the q1 `small` file (`bid_653M.csv`) is **5-column** — smoke on `medium`/`large`
  (4-column).

## Running locally (recipe + gotchas that cost real time)

```bash
cd ~/work/piparse-bench
pkill -x main 2>/dev/null            # PRE-FLIGHT: sweep stray workers (see below)
PBENCH_KEEP_LOGS=1 uv run python bench.py run \
  --build-dir   ~/work/nebulastream-public/cmake-build-release \
  --generator-bin ~/work/piparse-bench/generator/target/release/tcp-generator \
  experiments/<name>.yaml
```

- **Long runs: add `--detach`.** It re-execs the run in its OWN session (setsid) with
  output to `results/logs/pbench_run_<ts>.log` (override: `--log-file`), prints
  `pid=... log=...` and returns immediately — the run then survives the launcher dying
  or killing its process group (Ctrl-C'd terminals, dropped ssh, agent-harness
  background-task reaping, which killed two ~20-min runs mid-flight on 2026-07-14).
  Follow with `tail -f`; the log ends with the normal `Done:` summary. Watchdogs should
  poll the printed PID (`kill -0 <pid>`), NEVER `pgrep -f` a pattern that can match the
  watchdog's own cmdline.

- **`uv run`, not bare `python`** — deps (pydantic, duckdb, click) live in `.venv`; bare
  `python bench.py` dies with `ModuleNotFoundError: pydantic`.
- **TCP experiments need `--generator-bin`** pointing at pbench's own
  `generator/target/release/tcp-generator`. The old `testing/rustE2EBenchmarking/tcp-server`
  was REMOVED (nebulastream commit `bf6c11922c`) — it burst-deadlocked vs a pinned client and
  faked a pinning stall. Don't resurrect it.
- **`pkill -x main` pre-flight.** The worker's process comm is `main` (the binary is
  `nes-single-node-worker` but argv[0] truncates). The runner only kills processes IT spawned,
  NOT strays left by a Ctrl-C'd prior run — those keep streaming and contaminate the next
  measurement (the "6 workers running while measuring" failure). `pkill -x nes-single-node-worker`
  silently fails (name >15 ch); use `pkill -x main`. NEVER `pkill -f` with a pattern in your own
  cmdline — it self-kills the shell.
- **`--cmake-build` is NO LONGER required when already on the experiment's branch.** `BranchContext`
  now seeds `current_branch` from the actual checked-out branch, so `switch()` is a no-op and won't
  `sys.exit(1)` demanding cmake commands. Pass `--cmake-build '<cmd>'` only when you genuinely want a
  rebuild on branch switch.

### Trust the right number (results provenance)

- Each `pbench run` invocation gets one timestamped **`run_group`** (e.g.
  `20260629T114540-cef72647`), stamped on every result row, plus the resolved **`git_commit`**.
- The **`aggregated` view is scoped to the LATEST `run_group` per experiment** — a re-run's headline
  metric reflects only its fresh reps, never a median polluted by stale historical `runs` rows.
  (Before this fix, `engine_tcp_passthrough_smoke` reported `2138 MB/s (n=9)` = a median over 6 old
  pre-optimization rows + 3 fresh ~5.2 GB/s rows. The real number is ~5.2 GB/s.)
- Full history stays in `runs`; query it directly for cross-run / cross-commit comparisons.
- The per-rep `→ X MB/s` prints and the kept `query_*_monitoring.csv` (under `/tmp/pbench_mon_*`
  when `PBENCH_KEEP_LOGS=1`) are always per-run-correct — cross-check against them.

## CRLF datasets — LF conversions live IN PLACE of the ExternalData originals

Ten shipped benchmark CSVs are CRLF in the ExternalData store (nexmark person/auction/
auction_modified/bid_653M, ysb 1k+10k, smartgrid 974K, lrb 560K, cluster_monitoring
lightsaber_1M, debs WIND_TURBINES). The trailing `\r` breaks fast parsers on numeric last
columns (FastINT32/UINT16/INT16 throw or mis-parse), forces the JSON escape path on varsized
last columns, and made csv-arm output differ from the native arm by exactly 13 x rows
(byte-sum checksum). Since 2026-07-14 the catalogs assume **the file at the original name is
already LF**: each is a real `tr -d '\r'` copy that replaced the ExternalData symlink
(no `_lf` suffixed files anymore).

**Gotcha (the reason this section exists):** a build-dir wipe or a forced re-run of the
ExternalData fetch step restores the CRLF symlinks SILENTLY — catalog `size_bytes` will then
mismatch and fast-stack runs fail in confusing ways (eisel "malformed", FastINT32 throws).
After any build-dir regeneration run:

```
scripts/check_lf_datasets.sh <build-dir>/nes-systests/testdata   # exits 1 + lists offenders
```

and re-convert offenders with `tmp=$(mktemp); tr -d '\r' < file > $tmp && rm file && mv $tmp file`
(rm-then-mv, never write through the symlink — that would corrupt the shared MD5 object).
Incremental builds are safe: the LF file's mtime is newer than the `-hash-stamp`, so ninja
leaves it alone.

## Running on the benchmark server

1. Create `remote.yaml` (see `DESIGN.md` §11.2 for schema)
2. `pbench run --remote --fetch experiments/<name>.yaml`

Or manually: `pbench deploy`, `ssh server 'cd piparse-bench && python bench.py run ...'`, `pbench fetch`
