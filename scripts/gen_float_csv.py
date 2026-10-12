#!/usr/bin/env python3
"""Generate a CSV of floats with a target selectivity on `col1 > 42`."""

from __future__ import annotations

import argparse
import random
import sys
from pathlib import Path


_SIZE_SUFFIXES = {
    "": 1, "B": 1,
    "K": 1024, "KB": 1024, "KIB": 1024,
    "M": 1024**2, "MB": 1024**2, "MIB": 1024**2,
    "G": 1024**3, "GB": 1024**3, "GIB": 1024**3,
}


def parse_size(s: str) -> int:
    s = s.strip().upper()
    for i, c in enumerate(s):
        if not (c.isdigit() or c == "."):
            num, suffix = s[:i], s[i:].strip()
            break
    else:
        num, suffix = s, ""
    if suffix not in _SIZE_SUFFIXES:
        raise argparse.ArgumentTypeError(f"Unknown size suffix: {suffix!r}")
    return int(float(num) * _SIZE_SUFFIXES[suffix])


def _estimate_row_bytes(cols: int, low: float, high: float, seed: int | None) -> float:
    rng = random.Random(seed if seed is None else seed + 1)
    n = 200
    total = 0
    for _ in range(n):
        row = ",".join(f"{rng.uniform(low, high):.6f}" for _ in range(cols)) + "\n"
        total += len(row.encode("utf-8"))
    return total / n


def generate(
    size_bytes: int,
    cols: int,
    selectivity: float,
    output: Path,
    seed: int | None,
    low: float,
    high: float,
) -> tuple[int, int]:
    if cols < 1:
        raise ValueError("cols must be >= 1")
    if not 0.0 <= selectivity <= 100.0:
        raise ValueError("selectivity must be in [0, 100]")
    if not (low < 42 < high):
        raise ValueError(f"range [{low}, {high}) must straddle 42")

    avg_row = _estimate_row_bytes(cols, low, high, seed)
    rows = max(1, round(size_bytes / avg_row))

    rng = random.Random(seed)
    n_pass = round(rows * selectivity / 100.0)
    pass_mask = [True] * n_pass + [False] * (rows - n_pass)
    rng.shuffle(pass_mask)

    output.parent.mkdir(parents=True, exist_ok=True)
    written = 0
    with output.open("w") as f:
        for passes in pass_mask:
            if passes:
                first = rng.uniform(42.0 + 1e-9, high)
            else:
                first = rng.uniform(low, 42.0)
            rest = (rng.uniform(low, high) for _ in range(cols - 1))
            line = ",".join(f"{v:.6f}" for v in (first, *rest)) + "\n"
            f.write(line)
            written += len(line.encode("utf-8"))
    return rows, written


def _human(n: int) -> str:
    for unit in ("B", "KiB", "MiB", "GiB"):
        if n < 1024 or unit == "GiB":
            return f"{n:.2f} {unit}" if unit != "B" else f"{n} B"
        n /= 1024


def main() -> int:
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("-o", "--output", type=Path, required=True, help="Output CSV path")
    p.add_argument(
        "--size", type=parse_size, required=True,
        help="Target file size (e.g. 10MB, 1.5G, 4096)",
    )
    p.add_argument("-c", "--cols", type=int, required=True, help="Number of columns")
    p.add_argument(
        "-s", "--selectivity", type=float, required=True,
        help="Percent of rows where col1 > 42 (0-100)",
    )
    p.add_argument("--seed", type=int, default=None)
    p.add_argument("--low", type=float, default=0.0, help="Lower bound of float range")
    p.add_argument("--high", type=float, default=100.0, help="Upper bound of float range")
    args = p.parse_args()

    rows, written = generate(args.size, args.cols, args.selectivity, args.output, args.seed, args.low, args.high)
    print(f"Wrote {rows} rows x {args.cols} cols ({_human(written)}, target {_human(args.size)}) to {args.output}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
