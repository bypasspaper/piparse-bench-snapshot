#!/usr/bin/env python3
"""perf-stat the nes worker for one run: page faults + dTLB misses.

Tests the §40 hypothesis that a large buffer pool is slow on cold reads because
the buffers are barely reused within one pass, so most payload writes hit cold,
never-faulted pages (first-touch fault + zero). Expect page-faults to scale with
pool size (distinct pages touched) and dTLB-load-misses with the working set.

Run ALONGSIDE a single-config pbench run. Usage: perf_stat_worker.py [label]
"""
import glob
import subprocess
import sys
import time

NEEDLE = "cmake-build-release/nes-single-node-worker/nes-single-node-worker"
LABEL = sys.argv[1] if len(sys.argv) > 1 else "run"
EVENTS = "page-faults,minor-faults,major-faults,dTLB-load-misses,dTLB-loads,cache-misses"


def find_worker():
    for d in glob.glob("/proc/[0-9]*"):
        try:
            if NEEDLE in open(d + "/cmdline", "rb").read().replace(b"\0", b" ").decode():
                return int(d.split("/")[2])
        except Exception:
            pass
    return None


print(f"[perf_stat:{LABEL}] waiting for worker...", flush=True)
pid = None
t0 = time.time()
while time.time() - t0 < 120:
    pid = find_worker()
    if pid:
        break
    time.sleep(0.02)
if not pid:
    print(f"[perf_stat:{LABEL}] no worker found")
    sys.exit(1)
print(f"[perf_stat:{LABEL}] worker pid={pid}; counting until it exits", flush=True)

# perf stat -p pid -- sleep BIG ; perf stops when the worker exits or sleep ends.
proc = subprocess.run(
    ["perf", "stat", "-p", str(pid), "-e", EVENTS, "--", "sleep", "60"],
    capture_output=True, text=True,
)
print(f"\n[perf_stat:{LABEL}] ===== perf stat =====")
print(proc.stderr)
