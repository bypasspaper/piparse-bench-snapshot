#!/usr/bin/env bash
# SPDX-License-Identifier: Apache-2.0
#
# Build pbench-spark-ext.jar — the custom `file-microbatch-csv` DataSource V2 streaming source
# (reads ONE bounded file as byte-range, line-aligned micro-batches so Spark yields real per-batch
# latencies instead of a single whole-file number; see
# src/.../FileMicroBatchProvider.java + FileMicroBatchStream.java).
#
# Compiles OFFLINE against the jars bundled inside the pbench venv's pyspark installation (no Maven,
# no network) so the source is always built against the exact Spark version the benchmarks run.
# Requires a JDK 11+ (javac) on PATH. Pure Java (no scalac) — reuses Spark's UnivocityParser via
# Scala interop for byte-identical parsing.
#
# Usage: ./build.sh            # uses ../.venv/bin/python to locate pyspark
#        PYTHON=... ./build.sh # override the interpreter that has pyspark installed
set -euo pipefail
cd "$(dirname "$0")"

PYTHON="${PYTHON:-../.venv/bin/python}"
PYSPARK_JARS="$("$PYTHON" -c 'import os, pyspark; print(os.path.join(os.path.dirname(pyspark.__file__), "jars"))')"
[ -d "$PYSPARK_JARS" ] || { echo "pyspark jars dir not found: $PYSPARK_JARS" >&2; exit 1; }

CP="$(ls "$PYSPARK_JARS"/*.jar | paste -sd:)"
OUT="$(mktemp -d)"
trap 'rm -rf "$OUT"' EXIT

# NB: no -Werror here (unlike flink-ext) — Scala-interop calls (immutable.Map, UnivocityParser's
# Function1) unavoidably raise unchecked/rawtype lint that is safe and suppressed at the call site.
javac --release 11 -cp "$CP" -d "$OUT" \
    src/main/java/dev/pbench/spark/*.java
jar cf pbench-spark-ext.jar -C "$OUT" . -C src/main/resources .
echo "built $(pwd)/pbench-spark-ext.jar against $PYSPARK_JARS"
