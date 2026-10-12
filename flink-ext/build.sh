#!/usr/bin/env bash
# SPDX-License-Identifier: Apache-2.0
#
# Build pbench-flink-ext.jar — the custom `blackhole-format` table sink (Flink-native output
# serialization into a void sink; see src/.../FormatBlackHoleTableSinkFactory.java).
#
# Compiles OFFLINE against the jars bundled inside the pbench venv's pyflink installation
# (no Maven, no network) so the connector is always built against the exact Flink version
# the benchmarks run. Requires a JDK 11+ (javac) on PATH.
#
# Usage: ./build.sh            # uses ../.venv/bin/python to locate pyflink
#        PYTHON=... ./build.sh # override the interpreter that has pyflink installed
set -euo pipefail
cd "$(dirname "$0")"

PYTHON="${PYTHON:-../.venv/bin/python}"
PYFLINK_LIB="$("$PYTHON" -c 'import os, pyflink; print(os.path.join(os.path.dirname(pyflink.__file__), "lib"))')"
[ -d "$PYFLINK_LIB" ] || { echo "pyflink lib dir not found: $PYFLINK_LIB" >&2; exit 1; }

CP="$(ls "$PYFLINK_LIB"/flink-*.jar | paste -sd:)"
OUT="$(mktemp -d)"
trap 'rm -rf "$OUT"' EXIT

javac --release 11 -Xlint:all -Werror -cp "$CP" -d "$OUT" \
    src/main/java/dev/pbench/flink/*.java
jar cf pbench-flink-ext.jar -C "$OUT" . -C src/main/resources .
echo "built $(pwd)/pbench-flink-ext.jar against $PYFLINK_LIB"
