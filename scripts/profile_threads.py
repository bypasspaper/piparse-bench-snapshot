#!/usr/bin/env python3
"""Sample per-thread CPU% of the nes-single-node-worker at 100ms via /proc.
Waits for the worker to appear, samples until it exits (or 12s), then reports
peak %CPU per thread name and the busiest 100ms sample's per-thread breakdown.
"""
import os, time, glob, sys, collections

HZ = os.sysconf('SC_CLK_TCK')
NEEDLE = 'cmake-build-release/nes-single-node-worker/nes-single-node-worker'

def find_worker():
    for d in glob.glob('/proc/[0-9]*'):
        try:
            cl = open(d + '/cmdline', 'rb').read().replace(b'\0', b' ').decode()
            if NEEDLE in cl:
                return int(d.split('/')[2])
        except Exception:
            pass
    return None

# Wait for the worker
pid = None
t0 = time.time()
while time.time() - t0 < 90:
    pid = find_worker()
    if pid:
        break
    time.sleep(0.03)
if not pid:
    print("PROFILE: no worker found")
    sys.exit(1)
print(f"PROFILE: worker pid={pid} HZ={HZ}")

def snap():
    res = {}
    for t in glob.glob(f'/proc/{pid}/task/*'):
        tid = t.split('/')[-1]
        try:
            comm = open(t + '/comm').read().strip()
            st = open(t + '/stat').read()
            post = st[st.rindex(')') + 2:].split()
            ticks = int(post[11]) + int(post[12])  # utime + stime
            res[tid] = (comm, ticks)
        except Exception:
            pass
    return res

samples = []
prev = snap(); prevt = time.time()
for _ in range(120):  # up to 12s at 100ms
    time.sleep(0.1)
    if not os.path.exists(f'/proc/{pid}'):
        break
    cur = snap(); curt = time.time(); dt = curt - prevt
    row = {}
    for tid, (comm, ticks) in cur.items():
        if tid in prev:
            row[tid] = (comm, (ticks - prev[tid][1]) / (HZ * dt) * 100.0)
    samples.append(row)
    prev = cur; prevt = curt

peak = collections.defaultdict(float)
for row in samples:
    for tid, (comm, c) in row.items():
        peak[comm] = max(peak[comm], c)

print(f"PROFILE: {len(samples)} samples (100ms). Peak %CPU per thread name:")
for comm, c in sorted(peak.items(), key=lambda x: -x[1]):
    if c > 2:
        print(f"  {comm:20s} {c:6.1f}%")

def total(row):
    return sum(c for _, c in row.values())

# Busiest sample = the steady read phase
if samples:
    busy = max(samples, key=total)
    print(f"PROFILE: busiest 100ms sample (total {total(busy):.0f}%), per-thread:")
    for tid, (comm, c) in sorted(busy.items(), key=lambda x: -x[1][1]):
        if c > 2:
            print(f"  {comm:20s} (tid {tid}) {c:6.1f}%")
    # Average %CPU over samples where the source thread is active (>20%) = the read window
    src_active = [r for r in samples if any(comm.startswith('DataSrc') and c > 20 for _, (comm, c) in [(t, r[t]) for t in r])]
    if src_active:
        agg = collections.defaultdict(list)
        for r in src_active:
            for tid, (comm, c) in r.items():
                agg[comm].append(c)
        print(f"PROFILE: avg %CPU over {len(src_active)} read-active samples:")
        for comm, cs in sorted(agg.items(), key=lambda x: -sum(x[1]) / len(x[1])):
            avg = sum(cs) / len(cs)
            if avg > 2:
                print(f"  {comm:20s} {avg:6.1f}%")
