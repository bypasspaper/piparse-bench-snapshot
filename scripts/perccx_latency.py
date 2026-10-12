#!/usr/bin/env python3
"""Shared-nothing per-CCX vs unpinned single-worker -- LATENCY + throughput, many sources.

Tests the hypothesis (user + §21r/§23): pinning io+workers per-CCX (shared-nothing, one task
queue per worker process, taskset-confined) keeps produce->consume->recycle L3-local (amp
3.7->1.54, DRAM 33% of peak) -> should cut the ~11ms HOL/queueing TAIL from io_worker_split_sweep,
AND raise aggregate throughput toward the §21r 43.5 GB/s (vs ~16 single-worker).

Two arms at EQUAL total thread budget (24 = 8io + 16w) and EQUAL total sources, same steady
window (NES_FILE_REPEAT), same warm 1GB passthrough:
  UNPINNED : 1 worker,  io=8  w=16,             no taskset (global queue, threads float 4 CCX)
  PINNED   : 4 workers, io=2  w=4 each (=24),   taskset per CCX (shared-nothing, L3-local handoff)

Sources are split evenly across the pinned workers (total/4 per CCX). Latency comes from each
query's MonitoringSink CSV (p50/p99/p999 us -- sink emits them now); aggregate throughput is
computed here from bytes/wall-span using the absolute first/last timestamps (the sink's own MB/s
reads 1/REPEAT under NES_FILE_REPEAT, so we do NOT use it).

Usage:  python perccx_latency.py [total_sources ...]      (default: 50 100)
Env:    REPEAT (default 6), REPS (default 3).
GOTCHAS: each worker needs its OWN monitoring dir; --grpc=0.0.0.0:<port>; pre-warm the file;
pin_threads on the pinned workers is OFF (taskset confines + NES_PIN_CPU_OFFSET not needed since
each process starts pinning from core 0 within its taskset mask... actually the engine CPU_SETs
absolute cores 0..N, so we rely on taskset to remap -- pin_threads=false lets taskset confine)."""
import os, sys, subprocess, socket, time, csv, statistics, math
sys.path.insert(0, os.path.expanduser('~/work/piparse-bench'))
from piparse_bench.models import RunConfig
from piparse_bench.resolver import load_query
from piparse_bench.renderer import render_worker_cli_yaml

BUILD = os.path.expanduser('~/work/nebulastream-public/cmake-build-release')
WORKER = f"{BUILD}/nes-single-node-worker/nes-single-node-worker"
CLI = f"{BUILD}/nes-frontend/apps/nes-cli"
DATA = f"{BUILD}/nes-systests/testdata"
FNATTR = os.path.expanduser('~/work/piparse-bench/configs/invoke_modes/fnattr.yaml')
DATA_FILE = f"{DATA}/large/nexmark/bid_1GB.csv"
FILE_BYTES = 1089194961
CCX = ['0-5', '6-11', '12-17', '18-23']
ROOT = "/tmp/perccx_lat"
REPEAT = int(os.environ.get('REPEAT', 20))
REPS = int(os.environ.get('REPS', 3))
POOL = 256                                 # rebound per-run from POOLS below (wflags reads this global)
POOLS = [int(x) for x in os.environ.get('POOLS', '256,4096,32768').split(',')]   # global buffer pool sweep
ARMS = os.environ.get('ARMS', 'unpinned,pin1worker,perCCX').split(',')           # which placements to run
TOTALS = [int(x) for x in sys.argv[1:]] or [50, 100]

QUERY = 'nexmark/bid_passthrough_1gb'
query = load_query(QUERY)


def make_cfg(source_type):
    """Build a RunConfig for a given source_type ('File' async, 'BlockingFile' 1-thread/source).
    Both get NES_FILE_REPEAT looping (async natively; BlockingFile via the added seek-back path)."""
    qc = {'source_type': source_type, 'sink_type': 'Monitoring', 'input_formatter': {'type': 'SIMDCSV'},
          'input_parser': {'uint64': 'TracedUINT64', 'int32': 'TracedINT32', 'int64': 'TracedINT64'},
          'output_formatter': {'type': 'CSV'}}
    return RunConfig(experiment='x', variant='x', series='x', repetition=1, x_param='x', x_value='x',
                     system='nebulastream', mode='worker-cli', build='native', data_size='large',
                     topology='single-node', query_ref=QUERY, query_config=qc,
                     worker_flags={}, full_config={'query_config': qc}, generator={'repeat': REPEAT}, num_queries=1)


def wflags(port, io, w, pin):
    return [f'--grpc=0.0.0.0:{port}', f'--worker.query_engine.number_of_io_threads={io}',
            f'--worker.query_engine.number_of_worker_threads={w}',
            f'--worker.query_engine.pin_threads={"true" if pin else "false"}',
            '--worker.query_engine.admission_queue_size=8192',   # default 1000 saturates at >=1000 sources
            f'--worker.number_of_buffers_in_global_buffer_manager={POOL}',
            '--worker.default_query_execution.operator_buffer_size=131072',
            '--worker.default_query_execution.execution_mode=COMPILER',
            f'--worker.query_engine.invoke_mode_configuration={FNATTR}']


def prewarm():
    with open(DATA_FILE, 'rb') as f:
        while f.read(1 << 24):
            pass


def wait_port(port, timeout=180):
    for _ in range(timeout * 2):
        try:
            socket.create_connection(('localhost', port), timeout=1).close(); return True
        except OSError:
            time.sleep(0.5)
    return False


def parse_csv(path):
    """Return dict(tput_mbps, p50, p99, p999) from a monitoring CSV, or None. Throughput is the
    sink's own per-query MB/s, which is correct because generator.repeat == NES_FILE_REPEAT (the
    sink's size_of_input_data is fileSize*REPEAT, matching the REPEAT passes the source streams).
    Aggregate = SUM of per-query throughput (the validated two_worker_ccx method: all queries run
    long overlapping steady windows under REPEAT, so startup stagger is amortized)."""
    try:
        row = list(csv.DictReader(open(path)))[0]
        return dict(tput=float(row['throughput in MB/s']),
                    first=float(row['first read start us']), last=float(row['last arrival us']),
                    p50=float(row['p50 latency us']), p99=float(row['p99 latency us']),
                    p999=float(row['p999 latency us']))
    except Exception as e:
        print(f"    ERR {path}: {e}"); return None


def run_arm(name, source_type, total, specs):
    """specs = list of (taskset_cores_or_None, io, w, n_sources, pin) per worker process."""
    cfg = make_cfg(source_type)
    os.system(f"rm -rf {ROOT}; mkdir -p " + " ".join(f"{ROOT}/w{i}" for i in range(len(specs))))
    env = dict(os.environ); env['NES_FILE_REPEAT'] = str(REPEAT)
    ports = [8080 + i for i in range(len(specs))]
    procs = []
    for i, (cores, io, w, _, pin) in enumerate(specs):
        cmd = (['taskset', '-c', cores] if cores else []) + [WORKER] + wflags(ports[i], io, w, pin)
        procs.append(subprocess.Popen(cmd, stdout=open(f"{ROOT}/w{i}.log", 'w'), stderr=subprocess.STDOUT, env=env))
    for p in ports:
        if not wait_port(p):
            print(f"  [{name}] worker on {p} never came up"); [x.kill() for x in procs]; return None
    csvs = []
    for i, (_, _, _, nsrc, _) in enumerate(specs):
        for idx in range(nsrc):
            y = render_worker_cli_yaml(cfg, query, query_index=idx, monitoring_dir=f"{ROOT}/w{i}",
                                       tcp_host='localhost', worker_port=ports[i], data_dir=DATA)
            yp = f"{ROOT}/w{i}_q{idx}.yaml"; open(yp, 'w').write(y)
            subprocess.run([CLI, '-d', '-t', yp, 'start'], capture_output=True, text=True, timeout=60)
            csvs.append(f"{ROOT}/w{i}/query_{idx:04d}_monitoring.csv")
    deadline = time.time() + 600
    while time.time() < deadline:
        if all(os.path.exists(c) and os.path.getsize(c) > 0 for c in csvs):
            break
        if any(p.poll() is not None for p in procs):
            print(f"  [{name}] a worker DIED"); break
        time.sleep(2)
    time.sleep(1)
    rows = [r for c in csvs if (r := parse_csv(c))]
    for p in procs:
        p.kill()
    if not rows:
        return None
    # Two throughput views. SUM of per-query rates is valid only when windows fully overlap
    # (few sources, long REPEAT); at many sources the sequential submit staggers the windows and
    # the SUM OVERCOUNTS (seen: 84.7 GB/s > DRAM peak at 250 src / REPEAT=1). WALL-SPAN =
    # total_bytes / (max last_arrival - min first_read_start) is robust to stagger but INCLUDES the
    # startup ramp (submit + JIT of N pipelines), so it is a LOWER bound at high N. Truth is between.
    agg_sum = sum(r['tput'] for r in rows) / 1000.0
    wall_s = (max(r['last'] for r in rows) - min(r['first'] for r in rows)) / 1e6
    agg_wall = (len(rows) * FILE_BYTES * REPEAT) / wall_s / 1e9 if wall_s > 0 else 0
    p50s = [r['p50'] for r in rows]; p99s = [r['p99'] for r in rows]; p999s = [r['p999'] for r in rows]
    return dict(n_ok=len(rows), agg_sum=agg_sum, agg_wall=agg_wall,
                p50=statistics.median(p50s), p99=statistics.median(p99s),
                p999=statistics.median(p999s), p99_worst=max(p99s))


def split_even(total, k):
    base, rem = divmod(total, k)
    return [base + (1 if i < rem else 0) for i in range(k)]


print(f"pre-warming {DATA_FILE} ..."); prewarm()
print(f"REPEAT={REPEAT} REPS={REPS} totals={TOTALS} pools={POOLS}\n", flush=True)
print(f"{'arm':11}{'total':>6}{'pool':>7}{'wall_GBps':>10}{'sum_GBps':>9}{'p50_us':>9}{'p99_us':>9}{'p999_us':>9}", flush=True)
for total in TOTALS:
    for pool in POOLS:
        POOL = pool   # rebind module global -> wflags picks it up
        for arm, stype, specs in [
            ('unpinned', 'File', [(None, 8, 16, total, False)]),
            ('pin1worker', 'File', [(None, 8, 16, total, True)]),
            ('perCCX', 'File', [(CCX[i], 2, 4, n, False) for i, n in enumerate(split_even(total, 4))]),
            # BlockingFile: 1 OS thread per source (io setting is moot -- no async sources -> no io pool
            # created). Same 16 workers as the async single-worker arms => only the source model differs.
            ('blockUnpinned', 'BlockingFile', [(None, 8, 16, total, False)]),
            ('blockPinned', 'BlockingFile', [(None, 8, 16, total, True)]),
        ]:
            if arm not in ARMS:
                continue
            acc = []
            for _ in range(REPS):
                r = run_arm(arm, stype, total, specs)
                if r:
                    acc.append(r)
                time.sleep(1)
            if not acc:
                print(f"{arm:11}{total:>6}{pool:>7}   FAILED", flush=True); continue
            med = lambda k: statistics.median(x[k] for x in acc)
            print(f"{arm:11}{total:>6}{pool:>7}{med('agg_wall'):>10.1f}{med('agg_sum'):>9.1f}{med('p50'):>9.0f}"
                  f"{med('p99'):>9.0f}{med('p999'):>9.0f}  (n_ok={acc[-1]['n_ok']})", flush=True)
