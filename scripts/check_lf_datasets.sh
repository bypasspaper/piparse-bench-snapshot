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

# Guard against ExternalData silently restoring CRLF originals after a build-dir wipe.
# The query catalogs assume every benchmark CSV is LF *at its original name* (see
# CLAUDE.md "CRLF datasets"). Scans the first 8 KiB of every .csv under the given
# testdata root; exits 1 listing any file containing \r.
#
# Usage: scripts/check_lf_datasets.sh <build-dir>/nes-systests/testdata

set -euo pipefail

root="${1:?usage: check_lf_datasets.sh <testdata-root>}"
bad=0
while IFS= read -r -d '' f; do
    if head -c 8192 "$f" | grep -q $'\r'; then
        echo "CRLF: $f"
        bad=1
    fi
done < <(find "$root" -name '*.csv' -print0)

if [ "$bad" -eq 1 ]; then
    echo "^ re-convert with: tmp=\$(mktemp); tr -d '\\r' < FILE > \$tmp && rm FILE && mv \$tmp FILE" >&2
    exit 1
fi
echo "all csv datasets under $root are LF"
