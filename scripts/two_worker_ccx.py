#!/usr/bin/env python3
"""Per-CCX shared-nothing scaling: launch N nes-single-node-worker processes, each
`taskset`-confined to one CCX, each running 2 File bid_passthrough sources (IO=2/W=4,
pool 256, pin_threads off -- taskset confines), on separate gRPC ports. Sums the
2*N MonitoringSink throughputs. Verifies per-CCX scaling with NO engine change.

Usage:
  python two_worker_ccx.py [num_ccx]            # N workers on the first N CCX (default 2; max 4)
  python two_worker_ccx.py 0-5 6-11 12-17       # explicit taskset core-spec per worker
Controls (prove scaling is per-CCX, not per-process):
  python two_worker_ccx.py 0-5 0-5              # 2 workers SAME CCX -> ~11.8 GB/s (not 24)
  python two_worker_ccx.py 0-5 0-5 6-11 12-17 18-23   # CCX0 doubled -> ~44 (= 4-CCX, 5th wasted)
Measured (Threadripper PRO 5965WX, warm bid_6GB passthrough, repeat 20):
  1 CCX 12.6 / 2 CCX 24.0 / 3 CCX 34.0 / 4 CCX 43.5 GB/s  (~2.9x the unpinned 14-15 ceiling)
Wrap in perf to see DRAM (single flat plateau => parallel; ~28/50/62/74 GB/s for 1-4 CCX):
  perf stat -a -e amd_df/dram_channel_data_controller_{2,3,4,5}/ -I 1000 -- python two_worker_ccx.py 4
GOTCHAS: --grpc=0.0.0.0:<port> (NOT [::] -- yaml-cpp parses [...] as a sequence); each worker
needs its OWN monitoring dir (missing dir => sink "could not open output file" => query fails)."""
import os, sys, subprocess, socket, time, csv
sys.path.insert(0, os.path.expanduser('~/work/piparse-bench'))
from piparse_bench.models import RunConfig
from piparse_bench.resolver import load_query
from piparse_bench.renderer import render_worker_cli_yaml

DEFAULT_CCX = ['0-5', '6-11', '12-17', '18-23']           # 5965WX: 4 CCX x 6 cores
_args = sys.argv[1:]
if len(_args) == 1 and _args[0].isdigit():                # num_ccx -> first N CCX
    CCX_CORES = DEFAULT_CCX[:int(_args[0])]
elif _args:                                               # explicit taskset core-spec per worker
    CCX_CORES = _args
else:
    CCX_CORES = DEFAULT_CCX[:2]
N = len(CCX_CORES)
BUILD = os.path.expanduser('~/work/nebulastream-public/cmake-build-release')
WORKER = f"{BUILD}/nes-single-node-worker/nes-single-node-worker"
CLI = f"{BUILD}/nes-frontend/apps/nes-cli"
DATA = f"{BUILD}/nes-systests/testdata"
FNATTR = os.path.expanduser('~/work/piparse-bench/configs/invoke_modes/fnattr.yaml')
ROOT = "/tmp/nccx_mon"; REPEAT = 20; BASE_PORT = 8080
os.system(f"rm -rf {ROOT}; mkdir -p " + " ".join(f"{ROOT}/w{i}" for i in range(N)))

qc = {'source_type': 'File', 'sink_type': 'Monitoring', 'input_formatter': {'type': 'SIMDCSV'},
      'input_parser': {'uint64': 'TracedUINT64', 'int32': 'TracedINT32', 'int64': 'TracedINT64'},
      'output_formatter': {'type': 'CSV'}}
cfg = RunConfig(experiment='x', variant='x', series='x', repetition=1, x_param='x', x_value='x',
                system='nebulastream', mode='worker-cli', build='native', data_size='large',
                topology='single-node', query_ref='nexmark/bid_passthrough', query_config=qc,
                worker_flags={}, full_config={'query_config': qc}, generator={'repeat': REPEAT}, num_queries=2)
query = load_query('nexmark/bid_passthrough')


def wflags(port):
    return [f'--grpc=0.0.0.0:{port}', '--worker.query_engine.number_of_io_threads=2',
            '--worker.query_engine.number_of_worker_threads=4', '--worker.query_engine.pin_threads=false',
            '--worker.number_of_buffers_in_global_buffer_manager=256',
            '--worker.default_query_execution.operator_buffer_size=131072',
            '--worker.default_query_execution.execution_mode=COMPILER',
            f'--worker.query_engine.invoke_mode_configuration={FNATTR}']


env = dict(os.environ); env['NES_FILE_REPEAT'] = str(REPEAT)
workers, ports = [], [BASE_PORT + i for i in range(N)]
for i, port in enumerate(ports):
    workers.append(subprocess.Popen(['taskset', '-c', CCX_CORES[i], WORKER] + wflags(port),
                                     stdout=open(f"{ROOT}/w{i}.log", 'w'), stderr=subprocess.STDOUT, env=env))
for port in ports:
    for _ in range(180):
        try: socket.create_connection(('localhost', port), timeout=1).close(); break
        except OSError: time.sleep(0.5)
print(f"{N} workers up; submitting 2 queries each")
for i, port in enumerate(ports):
    for idx in range(2):
        y = render_worker_cli_yaml(cfg, query, query_index=idx, monitoring_dir=f"{ROOT}/w{i}",
                                   tcp_host='localhost', worker_port=port, data_dir=DATA)
        yp = f"{ROOT}/w{i}_q{idx}.yaml"; open(yp, 'w').write(y)
        r = subprocess.run([CLI, '-d', '-t', yp, 'start'], capture_output=True, text=True, timeout=30)
        print(f"  w{i}:{port} q{idx} -> rc={r.returncode}")
csvs = [(i, f"{ROOT}/w{i}/query_{idx:04d}_monitoring.csv") for i in range(N) for idx in range(2)]
for _ in range(300):
    if all(os.path.exists(c) and os.path.getsize(c) > 0 for _, c in csvs): break
    if any(w.poll() is not None for w in workers): print("WORKER DIED"); break
    time.sleep(2)
per = {}
for i, c in csvs:
    try: per.setdefault(i, 0); per[i] += float(list(csv.DictReader(open(c)))[0]['throughput in MB/s'])
    except Exception as e: print(f"  ERR {c}: {e}")
print("\n=== per-CCX worker throughput ===")
for i in range(N): print(f"  worker{i} (CCX{i}, cores {CCX_CORES[i]}): {round(per.get(i, 0), 1)} MB/s")
print(f"AGGREGATE ({N} workers, {2*N} sources): {round(sum(per.values()), 1)} MB/s")
for w in workers: w.kill()
