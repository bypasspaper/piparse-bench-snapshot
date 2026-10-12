#!/usr/bin/env python3
"""Sample the DataSrc (io_uring source) thread's kernel blocking state (wchan) over its
lifetime (= the read window). Tells us where its off-CPU time goes: running (wchan 0),
io_uring completion wait (io_cqring_wait/io_uring), or pool/backpressure (futex)."""
import os, time, glob, sys, collections

NEEDLE = 'cmake-build-release/nes-single-node-worker/nes-single-node-worker'

def find_worker():
    for d in glob.glob('/proc/[0-9]*'):
        try:
            if NEEDLE in open(d + '/cmdline', 'rb').read().replace(b'\0', b' ').decode():
                return int(d.split('/')[2])
        except Exception:
            pass
    return None

pid = None; t0 = time.time()
while time.time() - t0 < 90:
    pid = find_worker()
    if pid:
        break
    time.sleep(0.03)
if not pid:
    print("WCHAN: no worker"); sys.exit(1)

def datasrc_tid():
    for t in glob.glob(f'/proc/{pid}/task/*'):
        try:
            if open(t + '/comm').read().strip().startswith('DataSrc'):
                return t.split('/')[-1]
        except Exception:
            pass
    return None

tid = None; t1 = time.time()
while time.time() - t1 < 100:
    tid = datasrc_tid()
    if tid:
        break
    time.sleep(0.003)
if not tid:
    print("WCHAN: no DataSrc thread appeared"); sys.exit(1)
print(f"WCHAN: worker={pid} DataSrc tid={tid}")

wchan = collections.Counter()
stacktop = collections.Counter()
n = 0
base = f'/proc/{pid}/task/{tid}'
while os.path.exists(base):
    try:
        w = open(base + '/wchan').read().strip()
    except Exception:
        break
    wchan['(running)' if w in ('0', '') else w] += 1
    try:
        s = open(base + '/stack').read().strip().splitlines()
        # first non-trivial frame
        top = '?'
        for ln in s:
            fn = ln.split(']')[-1].strip().split('+')[0]
            if fn and fn not in ('0xffffffffffffffff',):
                top = fn; break
        stacktop[top] += 1
    except Exception:
        stacktop['(stack-unreadable)'] += 1
    n += 1
    time.sleep(0.0015)

if n == 0:
    print("WCHAN: no samples"); sys.exit(0)
print(f"WCHAN: {n} samples over DataSrc lifetime. wchan distribution:")
for w, c in wchan.most_common(14):
    print(f"  {c * 100 / n:5.1f}%  {w}")
print("WCHAN: kernel-stack top-frame distribution (best effort):")
for s, c in stacktop.most_common(14):
    print(f"  {c * 100 / n:5.1f}%  {s}")
