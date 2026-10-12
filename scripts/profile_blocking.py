#!/usr/bin/env python3
"""Profile WHERE the blocking-source latency/throughput drop comes from at 250/500 sources.

Hypothesis: 250-500 blocking OS source threads on 24c/48t = CPU oversubscription -> the source
threads spend most of their wall time WAITING ON THE RUNQUEUE (not running), and the scheduler
time-slices them -> context-switch storm + involuntary preemptions. The per-buffer latency is
then dominated by runqueue-wait, not by any I/O or compute cost.

For each config we launch ONE worker, submit N queries (NES_FILE_REPEAT loops for a long steady
window), let it settle, then over a fixed 10s STEADY window measure (all sudo-free, paranoid=-1):
  - perf stat -p <pid>: context-switches, cpu-migrations, instructions, cycles (-> IPC), task-clock
  - /proc/<pid>/task/*/schedstat delta: SUM cpu_time (running) vs SUM run_delay (waiting on runqueue)
    -> run_delay / (cpu_time+run_delay) = fraction of thread-time spent WAITING FOR A CPU = the
       oversubscription tax, and the direct latency source.
  - /proc/<pid>/task/*/status delta: nonvoluntary_ctxt_switches (involuntary preemptions).

Compare: async pin1worker (8 io threads) vs blocking (N source threads) at 250 and 500.

Usage: python profile_blocking.py            # runs the 3 configs below
Env:   WINDOW (default 10s steady window), SETTLE (default 8s post-submit ramp).
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
DATA_FILE = f"{DATA}/large/nexmark/bid_1GB.csv"
ROOT = "/tmp/prof_blk"
REPEAT = 8                # long steady window so we can profile 10s of pure steady state
WINDOW = int(os.environ.get('WINDOW', 10))
SETTLE = int(os.environ.get('SETTLE', 8))
QUERY = 'nexmark/bid_passthrough_1gb'
query = load_query(QUERY)


def make_cfg(stype):
    qc = {'source_type': stype, 'sink_type': 'Monitoring', 'input_formatter': {'type': 'SIMDCSV'},
          'input_parser': {'uint64': 'TracedUINT64', 'int32': 'TracedINT32', 'int64': 'TracedINT64'},
          'output_formatter': {'type': 'CSV'}}
    return RunConfig(experiment='x', variant='x', series='x', repetition=1, x_param='x', x_value='x',
                     system='nebulastream', mode='worker-cli', build='native', data_size='large',
                     topology='single-node', query_ref=QUERY, query_config=qc, worker_flags={},
                     full_config={'query_config': qc}, generator={'repeat': REPEAT}, num_queries=1)


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


def schedstat(pid):
    """Sum (cpu_time_ns, run_delay_ns) over all threads of pid."""
    cpu = delay = 0
    for f in glob.glob(f"/proc/{pid}/task/*/schedstat"):
        try:
            a, b, _ = open(f).read().split()
            cpu += int(a); delay += int(b)
        except Exception:
            pass
    return cpu, delay


def nonvol_ctxt(pid):
    total = n = 0
    for f in glob.glob(f"/proc/{pid}/task/*/status"):
        try:
            for line in open(f):
                if line.startswith('nonvoluntary_ctxt_switches:'):
                    total += int(line.split()[1]); n += 1; break
        except Exception:
            pass
    return total, n


def perf_stat(pid, secs):
    ev = "task-clock,context-switches,cpu-migrations,cycles,instructions"
    p = subprocess.run(['perf', 'stat', '-p', str(pid), '-e', ev, '--', 'sleep', str(secs)],
                       capture_output=True, text=True)
    out = {}
    for line in p.stderr.splitlines():
        m = re.match(r'\s*([\d,]+)\s+(\S+)', line)
        if m:
            out[m.group(2)] = int(m.group(1).replace(',', ''))
    return out


def run(name, stype, io, w, pin, n):
    os.system(f"rm -rf {ROOT}; mkdir -p {ROOT}/w0")
    env = dict(os.environ); env['NES_FILE_REPEAT'] = str(REPEAT)
    port = 8080
    flags = [f'--grpc=0.0.0.0:{port}', f'--worker.query_engine.number_of_io_threads={io}',
             f'--worker.query_engine.number_of_worker_threads={w}',
             f'--worker.query_engine.pin_threads={"true" if pin else "false"}',
             '--worker.query_engine.admission_queue_size=8192',
             '--worker.number_of_buffers_in_global_buffer_manager=8192',
             '--worker.default_query_execution.operator_buffer_size=131072',
             '--worker.default_query_execution.execution_mode=COMPILER',
             f'--worker.query_engine.invoke_mode_configuration={FNATTR}']
    proc = subprocess.Popen([WORKER] + flags, stdout=open(f"{ROOT}/w0.log", 'w'), stderr=subprocess.STDOUT, env=env)
    if not wait_port(port):
        print(f"{name}: worker down"); proc.kill(); return
    cfg = make_cfg(stype)
    for idx in range(n):
        y = render_worker_cli_yaml(cfg, query, query_index=idx, monitoring_dir=f"{ROOT}/w0",
                                   tcp_host='localhost', worker_port=port, data_dir=DATA)
        yp = f"{ROOT}/q{idx}.yaml"; open(yp, 'w').write(y)
        subprocess.run([CLI, '-d', '-t', yp, 'start'], capture_output=True, text=True, timeout=60)
    pid = proc.pid
    nthreads = len(glob.glob(f"/proc/{pid}/task/*"))
    time.sleep(SETTLE)                       # let the ramp settle
    c0, d0 = schedstat(pid); nv0, _ = nonvol_ctxt(pid)
    ps = perf_stat(pid, WINDOW)              # blocks WINDOW seconds
    c1, d1 = schedstat(pid); nv1, ntask = nonvol_ctxt(pid)
    proc.kill()
    cpu_s = (c1 - c0) / 1e9; delay_s = (d1 - d0) / 1e9
    wait_frac = delay_s / (cpu_s + delay_s) if (cpu_s + delay_s) > 0 else 0
    cyc = ps.get('cycles', 0); ins = ps.get('instructions', 0)
    ipc = ins / cyc if cyc else 0
    cs = ps.get('context-switches', 0); mig = ps.get('cpu-migrations', 0)
    taskclock_ms = ps.get('task-clock', 0)
    cpus_busy = taskclock_ms / 1000.0 / WINDOW      # avg CPUs utilized
    print(f"{name:14} thr={nthreads:4}  CPUsBusy={cpus_busy:5.1f}  IPC={ipc:4.2f}  "
          f"cs/s={cs//WINDOW:>9,}  mig/s={mig//WINDOW:>7,}  "
          f"runq_wait={wait_frac*100:4.0f}%  (cpu={cpu_s:5.1f}s wait={delay_s:6.1f}s over {WINDOW}s, "
          f"nonvol_cs={nv1-nv0:,})", flush=True)


print(f"pre-warming ..."); prewarm()
print(f"WINDOW={WINDOW}s SETTLE={SETTLE}s REPEAT={REPEAT}\n", flush=True)
run('async250',  'File',         8, 16, True, 250)
run('blocking250','BlockingFile', 8, 16, False, 250)
run('blocking500','BlockingFile', 8, 16, False, 500)
