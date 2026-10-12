#!/bin/bash
# Threadripper optimised-arm overload A/B: stateless then stateful, foreground. Held until the Pi run finishes.
# LAUNCH: setsid nohup ~/work/piparse-bench/results/run_tr_ovl.sh > ~/work/piparse-bench/results/logs/tr_ovl_run.log 2>&1 < /dev/null &
set -u
cd ~/work/piparse-bench
if pgrep -x main >/dev/null; then echo "ABORT: a NES worker (main) is already running on this host"; exit 2; fi
LOG=~/work/piparse-bench/results/logs/tr_ovl_continuity.log
snap() { echo "$(date +%FT%T) $1 gov=$(cat /sys/devices/system/cpu/cpu0/cpufreq/scaling_governor) load=$(cut -d' ' -f1-3 /proc/loadavg) tctl_mC=$(cat /sys/class/hwmon/hwmon*/temp1_input 2>/dev/null | sort -n | tail -1)" >> "$LOG"; }
snap PREFLIGHT
for f in nes_simd_overload_stateless nes_simd_overload_stateful; do
  snap "START $f"
  PBENCH_KEEP_LOGS=1 uv run python bench.py run "experiments/$f.yaml" --build-dir ~/work/nebulastream-public/cmake-build-release --worker-port 9099
  snap "END $f exit=$?"
done
snap DONE
