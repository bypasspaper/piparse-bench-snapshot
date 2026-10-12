#!/usr/bin/env python3
"""Engine-side mechanism evidence for the TCP async-vs-blocking comparison (the profiling I should
have done the first time). ISOLATED placement (generator -> CCX3, worker -> CCX0-2). Profiles the
WORKER pid over a steady 10s window: OS threads, CPUs busy, runqueue-wait fraction (from
/proc/pid/task/*/schedstat run_delay), IPC, context-switches, migrations.

Answers: at 250 connections, does the async io pool (8 or 16 threads) show a DIFFERENT engine-side
signature (runqueue-wait, IPC, thread count) than blocking (N threads)? Is the async tail a real
HOL/starvation effect or a placement artifact?

Usage:  python engine_profile.py [conns]         (default 250)
Env:    ARM (async_io8|async_io16|blocking, default async_io8), GEN_THREADS(4), CHUNK_KB(512).
"""
import os, sys, subprocess, socket, time, glob, re
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
ROOT = "/tmp/eng_prof"; PORT = 17702; WINDOW = 10
GEN_CPUS = os.environ.get('GEN_CPUS', '18-23,42-47'); WORKER_CPUS = os.environ.get('WORKER_CPUS', '0-17,24-41')
GEN_THREADS = int(os.environ.get('GEN_THREADS', 4)); CHUNK_KB = int(os.environ.get('CHUNK_KB', 512))
WORKER_THREADS = int(os.environ.get('WORKER_THREADS', 16))
LO_RX = '/sys/class/net/lo/statistics/rx_bytes'
N = int(sys.argv[1]) if len(sys.argv) > 1 else 250
ARM = os.environ.get('ARM', 'async_io8')
_ARMS = {'async_io8': ('TCP', 8), 'async_io16': ('TCP', 16), 'blocking': ('BlockingTCP', 8)}
SRC, IO = _ARMS[ARM]
query = load_query('nexmark/bid_passthrough_1gb')


def wait_port(p, t=180):
    for _ in range(t * 2):
        try:
            socket.create_connection(('localhost', p), timeout=1).close(); return True
        except OSError:
            time.sleep(0.5)
    return False


def wait_log(path, needle, proc, t=120):
    d = time.time() + t
    while time.time() < d:
        if proc.poll() is not None:
            return False
        if os.path.exists(path) and needle in open(path, errors='replace').read():
            return True
        time.sleep(0.2)
    return False


def schedstat(pid):
    cpu = delay = 0
    for f in glob.glob(f"/proc/{pid}/task/*/schedstat"):
        try:
            a, b, _ = open(f).read().split(); cpu += int(a); delay += int(b)
        except Exception:
            pass
    return cpu, delay


def ctxt_split(pid):
    """Sum voluntary (blocking/park wakeups, e.g. recv-wait) vs nonvoluntary (preemption =
    runqueue contention) context switches across all worker threads. The discriminator: file
    blocking collapse was nonvoluntary-dominated (always-runnable threads preempted); TCP blocking
    should be voluntary-dominated IF recv-parking keeps threads off the runqueue."""
    vol = nonvol = 0
    for f in glob.glob(f"/proc/{pid}/task/*/status"):
        try:
            for line in open(f):
                if line.startswith('voluntary_ctxt_switches:'):
                    vol += int(line.split()[1])
                elif line.startswith('nonvoluntary_ctxt_switches:'):
                    nonvol += int(line.split()[1])
        except Exception:
            pass
    return vol, nonvol


def perf_stat(pid, secs):
    p = subprocess.run(['perf', 'stat', '-p', str(pid), '-e',
                        'task-clock,context-switches,cpu-migrations,cycles,instructions', '--', 'sleep', str(secs)],
                       capture_output=True, text=True)
    out = {}
    for line in p.stderr.splitlines():
        m = re.match(r'\s*([\d,]+)\s+(\S+)', line)
        if m:
            out[m.group(2)] = int(m.group(1).replace(',', ''))
    return out


qc = {'source_type': SRC, 'sink_type': 'Monitoring', 'input_formatter': {'type': 'SIMDCSV'},
      'input_parser': {'uint64': 'TracedUINT64', 'int32': 'TracedINT32', 'int64': 'TracedINT64'},
      'output_formatter': {'type': 'CSV'}}
cfg = RunConfig(experiment='x', variant='x', series='x', repetition=1, x_param='x', x_value='x',
                system='nebulastream', mode='worker-cli', build='native', data_size='large', topology='single-node',
                query_ref='nexmark/bid_passthrough_1gb', query_config=qc, worker_flags={},
                full_config={'query_config': qc}, generator={'repeat': 20}, num_queries=1)

os.system(f"rm -rf {ROOT}; mkdir -p {ROOT}/mon")
wflags = ['--grpc=0.0.0.0:8080', f'--worker.query_engine.number_of_io_threads={IO}',
          f'--worker.query_engine.number_of_worker_threads={WORKER_THREADS}', '--worker.query_engine.pin_threads=false',
          '--worker.query_engine.admission_queue_size=8192', '--worker.number_of_buffers_in_global_buffer_manager=8192',
          '--worker.default_query_execution.operator_buffer_size=131072',
          '--worker.default_query_execution.execution_mode=COMPILER',
          f'--worker.query_engine.invoke_mode_configuration={FNATTR}']
worker = subprocess.Popen(['taskset', '-c', WORKER_CPUS, WORKER] + wflags,
                          stdout=open(f"{ROOT}/worker.log", 'w'), stderr=subprocess.STDOUT)
assert wait_port(8080), "worker down"
gen = subprocess.Popen(['taskset', '-c', GEN_CPUS, GEN, '--file', DATA_FILE, '-n', str(N), '-m', '20',
                        '-r', '100000', '--port', str(PORT), '--threads', str(GEN_THREADS), '--chunk-size-kb', str(CHUNK_KB)],
                       stdout=open(f"{ROOT}/gen.log", 'w'), stderr=subprocess.STDOUT)
assert wait_log(f"{ROOT}/gen.log", 'Listening on', gen), "gen down"
for idx in range(N):
    y = render_worker_cli_yaml(cfg, query, query_index=idx, monitoring_dir=f"{ROOT}/mon",
                               tcp_host='127.0.0.1', tcp_base_port=PORT, worker_port=8080, data_dir=DATA)
    yp = f"{ROOT}/q{idx}.yaml"; open(yp, 'w').write(y)
    subprocess.run([CLI, '-d', '-t', yp, 'start'], capture_output=True, text=True, timeout=60)

wpid = worker.pid
time.sleep(8)  # settle into steady streaming
nthreads = len(glob.glob(f"/proc/{wpid}/task/*"))
c0, d0 = schedstat(wpid)
v0, nv0 = ctxt_split(wpid)
gc0, _ = schedstat(gen.pid)
rx0 = int(open(LO_RX).read())
ps = perf_stat(wpid, WINDOW)
c1, d1 = schedstat(wpid)
v1, nv1 = ctxt_split(wpid)
gc1, _ = schedstat(gen.pid)
rx1 = int(open(LO_RX).read())
gen.kill(); worker.kill()
cpu_s = (c1 - c0) / 1e9; delay_s = (d1 - d0) / 1e9
wait_frac = delay_s / (cpu_s + delay_s) if (cpu_s + delay_s) > 0 else 0
cyc = ps.get('cycles', 0); ins = ps.get('instructions', 0)
vol_s = (v1 - v0) / WINDOW; nonvol_s = (nv1 - nv0) / WINDOW
tot_cs = vol_s + nonvol_s
nonvol_pct = (nonvol_s / tot_cs * 100) if tot_cs > 0 else 0
tput_gbps = (rx1 - rx0) / WINDOW / 1e9        # loopback rx delta over the window (box otherwise idle)
gen_cpu = (gc1 - gc0) / 1e9 / WINDOW          # generator CPUs busy (producer saturation tell)
print(f"\nENGINE profile: arm={ARM} src={SRC} io={IO} conns={N}  gen_threads={GEN_THREADS} worker_threads={WORKER_THREADS}")
print(f"  throughput_lo={tput_gbps:.1f} GB/s   gen_CPUs_busy={gen_cpu:.1f}/{GEN_THREADS}  "
      f"(gen_cpus={GEN_CPUS} worker_cpus={WORKER_CPUS})")
print(f"  OS_threads={nthreads}  CPUs_busy={cpu_s/WINDOW:.1f}  runq_wait={wait_frac*100:.0f}%  "
      f"IPC={(ins/cyc if cyc else 0):.2f}  cs/s={ps.get('context-switches',0)//WINDOW:,}  "
      f"mig/s={ps.get('cpu-migrations',0)//WINDOW:,}")
print(f"  ctx-split: vol/s={vol_s:,.0f}  nonvol/s={nonvol_s:,.0f}  nonvol={nonvol_pct:.0f}%  "
      f"(vol=recv-park wakeups, nonvol=preemption/runqueue-contention)")
print(f"  (cpu={cpu_s:.1f}s wait={delay_s:.1f}s over {WINDOW}s across {nthreads} threads)")
