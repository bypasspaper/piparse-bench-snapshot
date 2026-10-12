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
# Run one checksum-gate YAML (worker + nes-cli) and print the resulting "count,checksum" line.
# Usage: run_checksum_gate.sh <build_dir> <gate.yaml> [timeout_s]
set -euo pipefail

BUILD_DIR=$1
YAML_TEMPLATE=$2
TIMEOUT=${3:-600}
PORT=${HL7_CONVERT_PORT:-9091}

WORKDIR=$(mktemp -d /tmp/hl7_gate.XXXXXX)
OUT="$WORKDIR/checksum.csv"
YAML="$WORKDIR/gate.yaml"
sed -e "s#__TESTDATA__#$BUILD_DIR/nes-systests/testdata#g" -e "s#__OUTPUT__#$OUT#g" \
    -e "s#localhost:8080#localhost:$PORT#g" "$YAML_TEMPLATE" > "$YAML"

WORKER_PID=""
cleanup() {
    if [[ -n $WORKER_PID ]] && kill -0 "$WORKER_PID" 2>/dev/null; then
        kill "$WORKER_PID" 2>/dev/null || true
        wait "$WORKER_PID" 2>/dev/null || true
    fi
}
trap cleanup EXIT

echo "== starting worker on port $PORT (log: $WORKDIR/worker.log)"
"$BUILD_DIR/nes-single-node-worker/nes-single-node-worker" "--grpc=127.0.0.1:$PORT" > "$WORKDIR/worker.log" 2>&1 &
WORKER_PID=$!
sleep 2
kill -0 "$WORKER_PID" || { echo "worker died at startup:"; tail -20 "$WORKDIR/worker.log"; exit 1; }

echo "== submitting gate query"
"$BUILD_DIR/nes-frontend/apps/nes-cli" -d -t "$YAML" start | tail -1

echo "== waiting for the checksum file (timeout ${TIMEOUT}s)"
for _ in $(seq 1 $((TIMEOUT / 5))); do
    sleep 5
    ## the Checksum sink writes header+result at EoS; two lines = done
    if [[ -s $OUT ]] && [[ $(wc -l < "$OUT") -ge 2 ]]; then
        break
    fi
done
[[ -s $OUT ]] || { echo "TIMEOUT: no checksum written -- check $WORKDIR/worker.log" >&2; exit 1; }

echo "== RESULT:"
cat "$OUT"
echo "   (worker log: $WORKDIR/worker.log)"
