#!/usr/bin/env bash
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#    https://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#
# csv -> hl7 dataset conversion via the NES engine itself (the HL7 output formatter IS the
# generator -- no separate datagen script, same logical rows as the CSV/JSON variants).
#
# Usage: convert_csv_to_hl7.sh <build_dir> <conversion.yaml> <output.hl7> <expected_rows> [row_slice]
#   build_dir      NES cmake build dir (release!) with nes-single-node-worker + nes-cli
#   conversion.yaml  one of the *_to_hl7.yaml files next to this script
#   output.hl7     final output path (the raw sink file + header stripping are handled here)
#   expected_rows  row count of the source CSV; validated against the FS-CR message count
#   row_slice      optional: convert only the first N rows (writes a temp CSV slice) -- the
#                  cheap end-to-end proof before a full run
#
# GOTCHAS (learned the hard way, keep in sync with the YAMLs):
# - nes-cli 'start' is DETACHED: failures are swallowed -- always check worker.log on stall.
# - FileSink writes one 'name:TYPE:NULLABLE,...' header line; we strip it (the HL7 payload
#   contains no \n bytes, so 'tail -n +2' removes exactly that line).
# - Completion is detected by polling the sink file size until it is stable AND the FS-CR
#   count reaches expected_rows; the query is then stopped and the worker shut down.
set -euo pipefail

BUILD_DIR=$1
YAML_TEMPLATE=$2
OUTPUT=$3
EXPECTED_ROWS=$4
ROW_SLICE=${5:-}

WORKER_BIN="$BUILD_DIR/nes-single-node-worker/nes-single-node-worker"
CLI_BIN="$BUILD_DIR/nes-frontend/apps/nes-cli"
TESTDATA="$BUILD_DIR/nes-systests/testdata"
WORKDIR=$(mktemp -d /tmp/hl7_convert.XXXXXX)
RAW_OUT="$WORKDIR/raw_sink.hl7"

[[ -x $WORKER_BIN ]] || { echo "worker binary missing: $WORKER_BIN" >&2; exit 1; }
[[ -x $CLI_BIN ]] || { echo "nes-cli binary missing: $CLI_BIN" >&2; exit 1; }

# Materialize the YAML: substitute testdata dir + output path + worker port (default 9091 --
# 8080 is often taken; override with HL7_CONVERT_PORT); optionally slice the source.
PORT=${HL7_CONVERT_PORT:-9091}
YAML="$WORKDIR/conversion.yaml"
sed -e "s#__TESTDATA__#$TESTDATA#g" -e "s#__OUTPUT__#$RAW_OUT#g" -e "s#localhost:8080#localhost:$PORT#g" "$YAML_TEMPLATE" > "$YAML"
if [[ -n $ROW_SLICE ]]; then
    SRC=$(grep -oP 'file_path: \K.*\.csv' "$YAML" | head -1)
    SLICE="$WORKDIR/slice.csv"
    head -n "$ROW_SLICE" "$SRC" > "$SLICE"
    sed -i "s#$SRC#$SLICE#" "$YAML"
    EXPECTED_ROWS=$ROW_SLICE
fi

WORKER_PID=""
cleanup() {
    if [[ -n $WORKER_PID ]] && kill -0 "$WORKER_PID" 2>/dev/null; then
        kill "$WORKER_PID" 2>/dev/null || true
        wait "$WORKER_PID" 2>/dev/null || true
    fi
}
trap cleanup EXIT

echo "== starting worker on port $PORT (log: $WORKDIR/worker.log)"
"$WORKER_BIN" "--grpc=127.0.0.1:$PORT" > "$WORKDIR/worker.log" 2>&1 &
WORKER_PID=$!
sleep 2
kill -0 "$WORKER_PID" || { echo "worker died at startup:"; tail -20 "$WORKDIR/worker.log"; exit 1; }

echo "== submitting conversion query"
"$CLI_BIN" -d -t "$YAML" start | tee "$WORKDIR/cli.log"

echo "== waiting for completion (expect $EXPECTED_ROWS messages)"
count_msgs() { python3 -c "import sys;print(open(sys.argv[1],'rb').read().count(b'\x1c\r'))" "$1" 2>/dev/null || echo 0; }
LAST_SIZE=-1
STABLE=0
for _ in $(seq 1 720); do
    sleep 5
    SIZE=$(stat -c %s "$RAW_OUT" 2>/dev/null || echo 0)
    if [[ $SIZE -eq $LAST_SIZE && $SIZE -gt 0 ]]; then
        STABLE=$((STABLE + 1))
    else
        STABLE=0
    fi
    LAST_SIZE=$SIZE
    if [[ $STABLE -ge 2 ]]; then
        MSGS=$(count_msgs "$RAW_OUT")
        echo "   size stable at $SIZE bytes, $MSGS messages"
        [[ $MSGS -eq $EXPECTED_ROWS ]] && break
        [[ $STABLE -ge 6 ]] && { echo "STALLED at $MSGS/$EXPECTED_ROWS messages -- check $WORKDIR/worker.log" >&2; exit 1; }
    fi
done

MSGS=$(count_msgs "$RAW_OUT")
[[ $MSGS -eq $EXPECTED_ROWS ]] || { echo "message count $MSGS != expected $EXPECTED_ROWS" >&2; exit 1; }

echo "== stripping FileSink schema-header line"
NEWLINES=$(python3 -c "import sys;print(open(sys.argv[1],'rb').read(1000000).count(b'\n'))" "$RAW_OUT")
[[ $NEWLINES -eq 1 ]] || { echo "expected exactly 1 newline (the schema header) in the first 1MB, found $NEWLINES" >&2; exit 1; }
tail -n +2 "$RAW_OUT" > "$OUTPUT"

FINAL_MSGS=$(count_msgs "$OUTPUT")
FINAL_SIZE=$(stat -c %s "$OUTPUT")
[[ $FINAL_MSGS -eq $EXPECTED_ROWS ]] || { echo "post-strip message count $FINAL_MSGS != $EXPECTED_ROWS" >&2; exit 1; }
python3 - "$OUTPUT" <<'EOF'
import sys
with open(sys.argv[1], 'rb') as f:
    head = f.read(4)
    f.seek(-2, 2)
    tail = f.read(2)
assert head[:1] == b'\x0b', f"file must start with <VT>, got {head!r}"
assert tail == b'\x1c\r', f"file must end with <FS><CR>, got {tail!r}"
print("   framing OK: starts with <VT>, ends with <FS><CR>")
EOF

echo "== DONE: $OUTPUT ($FINAL_SIZE bytes, $FINAL_MSGS messages)"
echo "   worker log kept at $WORKDIR/worker.log"
