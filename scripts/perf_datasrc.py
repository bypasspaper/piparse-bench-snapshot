#!/usr/bin/env python3
"""Flat perf profile of the IoUringFileSource producer thread (DataSrc-*).

Localizes WHAT the §31 single-producer 82%-on-CPU actually is (user emit/pool work
vs kernel O_DIRECT submit path: get_user_pages / iov_iter / blk). Release omits frame
pointers, so we take a FLAT symbol profile (-F 4000, no call graph) which is valid
without unwinding -- perf_event_paranoid=-1 + kptr_restrict=0 give us kernel symbols.

Run ALONGSIDE a multi-pass cold O_DIRECT pbench run (NES_IOURING_PASSES=N) so the
DataSrc thread lives long enough (single cold pass ~1.1s is too short).

Usage: perf_datasrc.py [record_seconds] [out_prefix]
"""
import glob
import os
import subprocess
import sys
import time

NEEDLE = "cmake-build-release/nes-single-node-worker/nes-single-node-worker"
SECS = int(sys.argv[1]) if len(sys.argv) > 1 else 8
OUT = sys.argv[2] if len(sys.argv) > 2 else "/tmp/perf_datasrc"


def find_worker():
    for d in glob.glob("/proc/[0-9]*"):
        try:
            cl = open(d + "/cmdline", "rb").read().replace(b"\0", b" ").decode()
            if NEEDLE in cl:
                return int(d.split("/")[2])
        except Exception:
            pass
    return None


def find_datasrc_tid(pid):
    for t in glob.glob(f"/proc/{pid}/task/*"):
        try:
            comm = open(t + "/comm").read().strip()
            if comm.startswith("DataSrc"):
                return t.split("/")[-1], comm
        except Exception:
            pass
    return None, None


print(f"[perf_datasrc] waiting for worker ({NEEDLE})...", flush=True)
pid = None
t0 = time.time()
while time.time() - t0 < 120:
    pid = find_worker()
    if pid:
        break
    time.sleep(0.03)
if not pid:
    print("[perf_datasrc] no worker found")
    sys.exit(1)
print(f"[perf_datasrc] worker pid={pid}, waiting for DataSrc thread...", flush=True)

tid = comm = None
t0 = time.time()
while time.time() - t0 < 120:
    tid, comm = find_datasrc_tid(pid)
    if tid:
        break
    time.sleep(0.01)
if not tid:
    print("[perf_datasrc] no DataSrc thread appeared")
    sys.exit(1)

print(f"[perf_datasrc] recording tid={tid} ({comm}) for {SECS}s flat @4000Hz", flush=True)
data = f"{OUT}.data"
rc = subprocess.call(
    ["perf", "record", "-F", "4000", "-t", tid, "-o", data, "--", "sleep", str(SECS)]
)
if rc != 0:
    print(f"[perf_datasrc] perf record exited {rc}")
    sys.exit(rc)

print(f"\n[perf_datasrc] ===== FLAT PROFILE (tid {tid}, {comm}) =====", flush=True)
rep = subprocess.run(
    ["perf", "report", "-i", data, "--stdio", "--no-children",
     "--sort=symbol", "--percent-limit", "0.5"],
    capture_output=True, text=True,
)
out = rep.stdout
with open(f"{OUT}.report.txt", "w") as f:
    f.write(out)
# Print the symbol lines (skip perf's comment header)
for line in out.splitlines():
    if line and not line.startswith("#"):
        print(line)
print(f"\n[perf_datasrc] full report: {OUT}.report.txt   raw: {data}", flush=True)
