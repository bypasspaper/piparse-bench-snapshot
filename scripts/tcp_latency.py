#!/usr/bin/env python3
"""Network async-vs-blocking: TCP (asio, io-pool multiplexed) vs BlockingTCP (1 thread/conn),
scaling connections. The network analog of the File experiment (perccx_latency.py) — tests
whether blocking degrades more gracefully when threads PARK in recv() (vs warm-File where they
were always runnable).

Producer = the async tokio generator (FIXED --threads pool serving N connections as tasks), so
the localhost producer does NOT inject its own thread-explosion (the confound with the 1-thread-
per-client blocking generator). Everything FLOATS on all 48 logical cores, exactly like the File
runs, so the resource budget is identical and TCP-vs-File is comparable. GEN_CPUS taskset's the
generator if you want to isolate it; default float. Verify containment with profile_blocking.py.

Latency = engine-internal (source read-start -> sink), MonitoringSink percentiles (us). Throughput
= wall-span (robust to submit stagger) + sum (overcounts) for reference.

Usage:  python tcp_latency.py [conn_counts ...]         (default: 50 100 250)
Env:    RATE (MB/s TOTAL split across conns; default 6000 = controlled sub-saturation; set high
        e.g. 100000 for blast/saturation), WINDOW_S (target steady window, default 30 -> ITERS),
        GEN_THREADS (default 8), REPS (default 2), ARMS (default 'TCP,BlockingTCP'),
        GEN_CPUS (taskset cpulist for the generator; default float).
"""
import os, sys, subprocess, socket, time, csv, statistics, math, glob
sys.path.insert(0, os.path.expanduser('~/work/piparse-bench'))
from piparse_bench.models import RunConfig
from piparse_bench.resolver import load_query
from piparse_bench.renderer import render_worker_cli_yaml

BUILD = os.path.expanduser('~/work/nebulastream-public/cmake-build-release')
WORKER = f"{BUILD}/nes-single-node-worker/nes-single-node-worker"
CLI = f"{BUILD}/nes-frontend/apps/nes-cli"
DATA = f"{BUILD}/nes-systests/testdata"
FNATTR = os.path.expanduser('~/work/piparse-bench/configs/invoke_modes/fnattr.yaml')
GEN = os.path.expanduser('~/work/piparse-bench/generator/target/release/async-tcp-generator')
DATA_FILE = f"{DATA}/large/nexmark/bid_1GB.csv"
FILE_BYTES = 1089194961
ROOT = "/tmp/tcp_lat"
RATE = int(os.environ.get('RATE', 100000))        # MB/s total; default BLAST. Set e.g. 6000 for controlled load.
WINDOW_S = int(os.environ.get('WINDOW_S', 25))
GEN_THREADS = int(os.environ.get('GEN_THREADS', 4))       # measured ~3-4 GB/s/thread -> 4 feeds saturation
CHUNK_KB = int(os.environ.get('CHUNK_KB', 512))           # bigger = smoother pacing + less gen CPU (fewer syscalls)
REPS = int(os.environ.get('REPS', 2))
WORKER_THREADS = int(os.environ.get('WORKER_THREADS', 16))
# ISOLATED PLACEMENT (fixes the floating-generator confound): generator on CCX3 (+SMT), engine on
# CCX0-2 (+SMT). NOTE this SEPARATES producer<->consumer onto different CCX -> a symmetric cross-CCX
# skb cost (handover: co-location is ~2x; here both arms pay it equally, so the async-vs-blocking
# COMPARISON is unbiased even if absolute throughput is below the co-located optimum). Set both to
# '' to float. CCX3 = phys 18-23 + SMT 42-47; CCX0-2 = phys 0-17 + SMT 24-41.
GEN_CPUS = os.environ.get('GEN_CPUS', '18-23,42-47')
WORKER_CPUS = os.environ.get('WORKER_CPUS', '0-17,24-41')
# arms: (label, source_type, io_threads). io moot for BlockingTCP. async io-sweep tests whether more
# io threads rescue the async recv-copy bottleneck at high connection count.
_ALL_ARMS = {'async_io6': ('TCP', 6), 'async_io8': ('TCP', 8), 'async_io16': ('TCP', 16),
             'async_io24': ('TCP', 24), 'blocking': ('BlockingTCP', 8)}
ARMS = os.environ.get('ARMS', 'async_io8,async_io16,blocking').split(',')
# PIN=true applies the file-case pin1worker win: pin_threads pins io->[0,io), workers->[io,io+w) to
# FIXED cores (no migration jitter). Requires io+WORKER_THREADS <= 18 so the pinned cores stay in
# CCX0-2 (0-17) and don't collide with the generator's CCX3 (18-23).
PIN = os.environ.get('PIN', 'false').lower() in ('1', 'true', 'yes')
COUNTS = [int(x) for x in sys.argv[1:]] or [50, 100, 250]
PORT = 17700
QUERY = 'nexmark/bid_passthrough_1gb'
query = load_query(QUERY)


def make_cfg(source_type, iters):
    qc = {'source_type': source_type, 'sink_type': 'Monitoring', 'input_formatter': {'type': 'SIMDCSV'},
          'input_parser': {'uint64': 'TracedUINT64', 'int32': 'TracedINT32', 'int64': 'TracedINT64'},
          'output_formatter': {'type': 'CSV'}}
    # generator.repeat=iters so MonitoringSink size_of_input_data = fileSize*iters (throughput honest).
    return RunConfig(experiment='x', variant='x', series='x', repetition=1, x_param='x', x_value='x',
                     system='nebulastream', mode='worker-cli', build='native', data_size='large',
                     topology='single-node', query_ref=QUERY, query_config=qc, worker_flags={},
                     full_config={'query_config': qc}, generator={'repeat': iters}, num_queries=1)


def wait_port(port, timeout=180):
    for _ in range(timeout * 2):
        try:
            socket.create_connection(('localhost', port), timeout=1).close(); return True
        except OSError:
            time.sleep(0.5)
    return False


def wait_log(path, needle, proc, timeout=120):
    deadline = time.time() + timeout
    while time.time() < deadline:
        if proc.poll() is not None:
            return False
        if os.path.exists(path) and needle in open(path, errors='replace').read():
            return True
        time.sleep(0.2)
    return False


def parse_csv(path):
    try:
        row = list(csv.DictReader(open(path)))[0]
        return dict(tput=float(row['throughput in MB/s']),
                    first=float(row['first read start us']), last=float(row['last arrival us']),
                    p50=float(row['p50 latency us']), p99=float(row['p99 latency us']),
                    p999=float(row['p999 latency us']))
    except Exception as e:
        print(f"    ERR {path}: {e}"); return None


def run(label, source_type, io, n):
    # ITERS to hit ~WINDOW_S. Base on the EXPECTED actual aggregate throughput (min of the offered
    # rate and an assumed ~12 GB/s TCP ceiling), NOT the nominal blast rate -- otherwise blast
    # (RATE=100000) would compute a huge iters. wall ~= N*iters*1GB / eff_gbps = WINDOW.
    eff_gbps = min(RATE / 1000.0, 12.0)
    iters = max(1, math.ceil(WINDOW_S * eff_gbps / n))
    os.system(f"rm -rf {ROOT}; mkdir -p {ROOT}/mon")
    # 1. worker, tasksetted to WORKER_CPUS (isolated from the generator)
    wflags = [f'--grpc=0.0.0.0:8080', f'--worker.query_engine.number_of_io_threads={io}',
              f'--worker.query_engine.number_of_worker_threads={WORKER_THREADS}',
              f'--worker.query_engine.pin_threads={"true" if PIN else "false"}',
              '--worker.query_engine.admission_queue_size=8192',
              '--worker.number_of_buffers_in_global_buffer_manager=8192',
              '--worker.default_query_execution.operator_buffer_size=131072',
              '--worker.default_query_execution.execution_mode=COMPILER',
              f'--worker.query_engine.invoke_mode_configuration={FNATTR}']
    wcmd = (['taskset', '-c', WORKER_CPUS] if WORKER_CPUS else []) + [WORKER] + wflags
    worker = subprocess.Popen(wcmd, stdout=open(f"{ROOT}/worker.log", 'w'), stderr=subprocess.STDOUT)
    if not wait_port(8080):
        print(f"{label} n={n}: worker down"); worker.kill(); return None
    # 2. async generator (fixed thread pool), tasksetted to CCX3
    gcmd = (['taskset', '-c', GEN_CPUS] if GEN_CPUS else []) + \
           [GEN, '--file', DATA_FILE, '-n', str(n), '-m', str(iters), '-r', str(RATE),
            '--port', str(PORT), '--threads', str(GEN_THREADS), '--chunk-size-kb', str(CHUNK_KB)]
    gen = subprocess.Popen(gcmd, stdout=open(f"{ROOT}/gen.log", 'w'), stderr=subprocess.STDOUT)
    if not wait_log(f"{ROOT}/gen.log", 'Listening on', gen):
        print(f"{label} n={n}: generator never listened"); worker.kill(); gen.kill(); return None
    # 3. submit n TCP-source queries (each opens one connection to the generator)
    cfg = make_cfg(source_type, iters)
    for idx in range(n):
        y = render_worker_cli_yaml(cfg, query, query_index=idx, monitoring_dir=f"{ROOT}/mon",
                                   tcp_host='127.0.0.1', tcp_base_port=PORT, worker_port=8080, data_dir=DATA)
        yp = f"{ROOT}/q{idx}.yaml"; open(yp, 'w').write(y)
        subprocess.run([CLI, '-d', '-t', yp, 'start'], capture_output=True, text=True, timeout=60)
    # 4. wait for all monitoring CSVs to finalize (generator streams then closes -> EOS -> sink writes)
    csvs = [f"{ROOT}/mon/query_{i:04d}_monitoring.csv" for i in range(n)]
    deadline = time.time() + 600
    while time.time() < deadline:
        if all(os.path.exists(c) and os.path.getsize(c) > 0 for c in csvs):
            break
        if worker.poll() is not None:
            print(f"{label} n={n}: worker DIED"); break
        time.sleep(2)
    time.sleep(1)
    rows = [r for c in csvs if (r := parse_csv(c))]
    gen.kill(); worker.kill()
    if not rows:
        return None
    agg_sum = sum(r['tput'] for r in rows) / 1000.0
    wall_s = (max(r['last'] for r in rows) - min(r['first'] for r in rows)) / 1e6
    agg_wall = (len(rows) * FILE_BYTES * iters) / wall_s / 1e9 if wall_s > 0 else 0
    return dict(n_ok=len(rows), iters=iters, agg_wall=agg_wall, agg_sum=agg_sum,
                p50=statistics.median(r['p50'] for r in rows), p99=statistics.median(r['p99'] for r in rows),
                p999=statistics.median(r['p999'] for r in rows))


print(f"RATE={RATE} MB/s total ({'BLAST' if RATE >= 50000 else 'controlled'}), WINDOW_S={WINDOW_S}, "
      f"GEN_THREADS={GEN_THREADS}, CHUNK={CHUNK_KB}KB, REPS={REPS}\n"
      f"GEN_CPUS={GEN_CPUS or 'float'}  WORKER_CPUS={WORKER_CPUS or 'float'}  arms={ARMS} counts={COUNTS}\n", flush=True)
print(f"{'arm':12}{'conns':>6}{'iters':>6}{'wall_GBps':>10}{'sum_GBps':>9}{'p50_us':>9}{'p99_us':>9}{'p999_us':>9}", flush=True)
for n in COUNTS:
    for label in ARMS:
        source_type, io = _ALL_ARMS[label]
        acc = []
        for _ in range(REPS):
            r = run(label, source_type, io, n)
            if r:
                acc.append(r)
            time.sleep(1)
        if not acc:
            print(f"{label:12}{n:>6}   FAILED", flush=True); continue
        med = lambda k: statistics.median(x[k] for x in acc)
        print(f"{label:12}{n:>6}{acc[-1]['iters']:>6}{med('agg_wall'):>10.1f}{med('agg_sum'):>9.1f}"
              f"{med('p50'):>9.0f}{med('p99'):>9.0f}{med('p999'):>9.0f}  (n_ok={acc[-1]['n_ok']})", flush=True)
