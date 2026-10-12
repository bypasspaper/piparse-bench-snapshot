#!/usr/bin/env python3
"""Prove the async tokio generator stays CONTAINED at 250 connections (the localhost confound the
whole network experiment worried about). Launch worker + async generator + N BlockingTCP queries,
let it reach steady state, then profile the GENERATOR pid over a 10s window: thread count, CPU
(schedstat cpu_time), runqueue-wait fraction, context-switches, IPC, CPUs busy. If the generator
runs on ~GEN_THREADS threads at bounded CPU with ~0% runqueue-wait, it is NOT the bottleneck and
the async-vs-blocking-TCP result is trustworthy.
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
ROOT = "/tmp/gen_prof"; PORT = 17701
N = int(sys.argv[1]) if len(sys.argv) > 1 else 250
GEN_THREADS = int(os.environ.get('GEN_THREADS', 8))
ARM = os.environ.get('ARM', 'BlockingTCP')
WINDOW = 10
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


qc = {'source_type': ARM, 'sink_type': 'Monitoring', 'input_formatter': {'type': 'SIMDCSV'},
      'input_parser': {'uint64': 'TracedUINT64', 'int32': 'TracedINT32', 'int64': 'TracedINT64'},
      'output_formatter': {'type': 'CSV'}}
cfg = RunConfig(experiment='x', variant='x', series='x', repetition=1, x_param='x', x_value='x',
                system='nebulastream', mode='worker-cli', build='native', data_size='large', topology='single-node',
                query_ref='nexmark/bid_passthrough_1gb', query_config=qc, worker_flags={},
                full_config={'query_config': qc}, generator={'repeat': 8}, num_queries=1)

os.system(f"rm -rf {ROOT}; mkdir -p {ROOT}/mon")
wflags = ['--grpc=0.0.0.0:8080', '--worker.query_engine.number_of_io_threads=8',
          '--worker.query_engine.number_of_worker_threads=16', '--worker.query_engine.pin_threads=false',
          '--worker.query_engine.admission_queue_size=8192', '--worker.number_of_buffers_in_global_buffer_manager=8192',
          '--worker.default_query_execution.operator_buffer_size=131072',
          '--worker.default_query_execution.execution_mode=COMPILER',
          f'--worker.query_engine.invoke_mode_configuration={FNATTR}']
worker = subprocess.Popen([WORKER] + wflags, stdout=open(f"{ROOT}/worker.log", 'w'), stderr=subprocess.STDOUT)
assert wait_port(8080), "worker down"
gen = subprocess.Popen([GEN, '--file', DATA_FILE, '-n', str(N), '-m', '8', '-r', '100000',
                        '--port', str(PORT), '--threads', str(GEN_THREADS), '--chunk-size-kb', '64'],
                       stdout=open(f"{ROOT}/gen.log", 'w'), stderr=subprocess.STDOUT)
assert wait_log(f"{ROOT}/gen.log", 'Listening on', gen), "gen down"
for idx in range(N):
    y = render_worker_cli_yaml(cfg, query, query_index=idx, monitoring_dir=f"{ROOT}/mon",
                               tcp_host='127.0.0.1', tcp_base_port=PORT, worker_port=8080, data_dir=DATA)
    yp = f"{ROOT}/q{idx}.yaml"; open(yp, 'w').write(y)
    subprocess.run([CLI, '-d', '-t', yp, 'start'], capture_output=True, text=True, timeout=60)

gpid = gen.pid
time.sleep(6)  # settle into steady streaming
nthreads = len(glob.glob(f"/proc/{gpid}/task/*"))
c0, d0 = schedstat(gpid)
ps = perf_stat(gpid, WINDOW)
c1, d1 = schedstat(gpid)
gen.kill(); worker.kill()
cpu_s = (c1 - c0) / 1e9; delay_s = (d1 - d0) / 1e9
wait_frac = delay_s / (cpu_s + delay_s) if (cpu_s + delay_s) > 0 else 0
cyc = ps.get('cycles', 0); ins = ps.get('instructions', 0)
print(f"\nGENERATOR profile: arm={ARM} conns={N} gen_threads={GEN_THREADS}")
print(f"  OS threads={nthreads}  CPUs_busy={cpu_s/WINDOW:.1f}  runq_wait={wait_frac*100:.0f}%  "
      f"IPC={(ins/cyc if cyc else 0):.2f}  cs/s={ps.get('context-switches',0)//WINDOW:,}  "
      f"mig/s={ps.get('cpu-migrations',0)//WINDOW:,}")
print(f"  (cpu={cpu_s:.1f}s wait={delay_s:.1f}s over {WINDOW}s across {nthreads} threads)")
