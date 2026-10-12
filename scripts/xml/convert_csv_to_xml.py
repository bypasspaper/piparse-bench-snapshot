#!/usr/bin/env python3
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
# csv -> packed record-per-line XML twin generator (the XML analog of convert_csv_to_hl7.sh,
# but a plain python stream transform -- there is no XML output formatter and none is needed
# for the input story).
#
# Emits, per csv row:  <rec><ts>v0</ts><aid>v1</aid>...</rec>\n
# packed (no whitespace between tags), NO <?xml?> prolog (it would index as a wrong-arity
# record), NO escaping / entity processing (assume-clean contract, same as HL7/CSV
# assume_clean_strings): values must be free of '<', '>', '\n' -- asserted per line ('&' is
# FINE: '&' is not structural in this XML mode and entities are not processed).
#
# The point (see XML_INDEXER_PLAN.md): splitting each line at EVERY '<' and '>' yields, for F
# data fields, arity A = 4F+5 leaves in a fixed pattern with the values as clean standalone
# leaves at slot 4f+4 -- so the UNMODIFIED HL7 indexer parses this by configuration:
#   type: HL7, message_delimiter: '\n', segment_delimiter: '<', field_delimiter: '>',
#   component_delimiter: '', subcomponent_delimiter: ''
#
# Usage: convert_csv_to_xml.py INPUT.csv OUTPUT.xml --tags ts,aid,bid,pr,ch,url [--root rec]
#                              [--skip-header] [--rows N]
#   --tags        comma-separated element names, one per csv column (arity is validated per row)
#   --root        record element name (default: rec)
#   --skip-header drop the first input line (bid_strings_3GB.csv has NO header; ysb/person vary)
#   --rows N      convert only the first N data rows (the cheap end-to-end proof slice)
import argparse
import os
import sys


def main() -> None:
    ap = argparse.ArgumentParser(description="csv -> packed record-per-line XML twin")
    ap.add_argument("input")
    ap.add_argument("output")
    ap.add_argument("--tags", required=True, help="comma-separated element names, one per csv column")
    ap.add_argument("--root", default="rec")
    ap.add_argument("--skip-header", action="store_true")
    ap.add_argument("--rows", type=int, default=None)
    args = ap.parse_args()

    tags = args.tags.split(",")
    num_fields = len(tags)
    for t in tags + [args.root]:
        assert t and all(c.isalnum() or c == "_" for c in t), f"bad element name: {t!r}"

    # <rec><ts>{}</ts>...</rec>\n -- one .format(*fields) per row (fast path, C-level).
    template = (
        f"<{args.root}>" + "".join(f"<{t}>{{}}</{t}>" for t in tags) + f"</{args.root}>\n"
    )
    glue = len(template.format(*[""] * num_fields))
    arity = 4 * num_fields + 5

    rows = 0
    chunk: list[str] = []
    CHUNK_ROWS = 1 << 16
    with open(args.input, encoding="ascii", newline="\n") as fin, open(
        args.output, "w", encoding="ascii", newline="\n"
    ) as fout:
        if args.skip_header:
            fin.readline()
        for line in fin:
            if args.rows is not None and rows >= args.rows:
                break
            # One C-level scan covers every field at once: a '<' or '>' anywhere in the line
            # would corrupt the structural split; '\n' inside a value is impossible line-wise.
            if "<" in line or ">" in line:
                sys.exit(f"row {rows + 1}: value contains structural byte '<' or '>': {line!r}")
            fields = line.rstrip("\n").split(",")
            if len(fields) != num_fields:
                sys.exit(f"row {rows + 1}: {len(fields)} csv fields, expected {num_fields}: {line!r}")
            chunk.append(template.format(*fields))
            rows += 1
            if len(chunk) >= CHUNK_ROWS:
                fout.write("".join(chunk))
                chunk.clear()
        fout.write("".join(chunk))

    size = os.path.getsize(args.output)
    print(f"rows={rows} bytes={size} glue_bytes_per_row={glue} (incl. trailing \\n) arity={arity}")
    print(f"schema: {arity} leaves, values at 0-indexed slots " + ", ".join(str(4 * f + 4) for f in range(num_fields)))


if __name__ == "__main__":
    main()
