# flink-ext — custom Flink connectors for pbench

Java extensions loaded into the PyFlink driver via `pipeline.jars`
(see `renderer.py: render_flink`). Build:

```
./build.sh        # offline: javac against the venv's own pyflink jars, no Maven/network
```

Rebuild after any pyflink version bump (the jar is version-matched by construction and
gitignored — `pbench run` fails with a pointer here if it is missing).

## `blackhole-format` (FormatBlackHoleTableSinkFactory)

A blackhole table sink that ACCEPTS a `format` option — the missing arm for fair
csv→json benchmarks. Flink's built-in `blackhole` rejects `'format'` (it discards
RowData with zero output serialization), and the two workarounds are wrong:
`JSON_OBJECT()` in the SELECT measures Calcite's per-row JSON-node builder, and a
`filesystem` json sink measures the real serializer plus file-writer overhead.

This connector discovers the real `SerializationFormatFactory` for the declared
format (for `'format'='json'`: `JsonRowDataSerializationSchema`, schema-ordered keys,
verified byte-identical to the filesystem sink's output minus the per-row `\n`),
serializes every row, and discards the bytes. Each subtask prints
`PBENCH_VOIDSINK bytes=N rows=M` on close for volume accounting.

```sql
CREATE TABLE snk (`a` BIGINT, `b` DOUBLE) WITH (
  'connector' = 'blackhole-format', 'format' = 'json')
```

Measured (q1 medium bid_1GB, parallelism 8, 2026-07-14): parse-only blackhole
105.2 MB/s, blackhole-format json **56.9**, JSON_OBJECT→blackhole 33.0,
filesystem-json→tmpfs 12.8.

## `nes-native` (NesNativeFormatFactory + NesNativeBulkFormat)

A zero-parse decoding format for NES "native" files (fixed-width packed
little-endian rows, the layout written by `nes-systests/testdata/small/`
`csv_to_binary.py`) — the native-input arm of the csv|native → native|json
matrix. Batches are read as byte chunks; rows are served through a *reused
flyweight* `RowData` whose accessors are single little-endian loads straight
from the chunk (`ByteBuffer.getLong` → one mov on x86). No parsing, no casting:
unsigned/narrow ints zero/sign-extend into the declared column type (one AND),
`uint64` is reinterpreted as BIGINT (values ≥ 2^63 would wrap — fine for
ms-epoch timestamps), `string[N]` slots become zero-copy `BinaryStringData`
views (bounded strlen for the NUL terminator is the only per-access scan).

Splittable at row boundaries: a row belongs to the split containing its first
byte, so parallelism can engage on a single file — unlike Flink's csv format,
which binds one file to one split.

```sql
CREATE TABLE bid (`timestamp` BIGINT, auctionId INT, bidder INT, price DOUBLE)
WITH ('connector' = 'filesystem', 'path' = '/data/bid_6GB.nes',
      'format' = 'nes-native', 'nes-native.schema' = 'uint64,int32,int32,float64')
```

Options: `nes-native.schema` (required, csv_to_binary syntax incl. `string[N]`,
`bool`, `char`), `nes-native.fetch-size` (default 1 MiB), `nes-native.object-reuse`
(default true; set false if a buffering operator ever needs to retain row refs).

Correctness (2026-07-14): COUNT + SUM(timestamp/auctionId/bidder) identical to
the csv arm over all 183,991,721 bid_6GB rows; exact value round-trip incl.
extreme ints, 1e300, empty/full-width NUL-padded strings, bools.

## `nes-native-file` (NesNativeFileSourceFactory) — capability probe, not the fair arm

**Fairness model (user call 2026-07-14): native input must behave like text —
one sequential stream, one reader.** That is exactly `'format'='nes-native'`
on the filesystem connector (Flink local files are one split), and it is what
pbench renders by default for `.nes` sources. Use this connector only for the
explicitly-labeled parallel-ingestion probe.

It wraps the same `NesNativeBulkFormat` behind a fixed-size byte-range split
enumerator (`split-size`, default 64 MiB; the reader row-aligns each range),
so a single .nes file parallel-reads across all subtasks — possible only
because rows are fixed-width (text cannot be split without scanning for
delimiters, and Flink's csv doesn't split at all; Flink's LocalFileSystem
reports every file as ONE block, so the filesystem connector never splits
local files either — verified: 7 of 8 subtasks idle). Parallel ingestion
changes the topology, not just the format: do not quote it against
single-split text arms.

```sql
CREATE TABLE bid (`timestamp` BIGINT, auctionId INT, bidder INT, price DOUBLE)
WITH ('connector' = 'nes-native-file', 'path' = '/data/bid_6GB.nes',
      'schema' = 'uint64,int32,int32,float64')
```

Options: `path`, `schema` (required); `split-size`, `fetch-size`,
`object-reuse` (optional).

Measured (bid_6GB.nes = 4.4 GB / 184M rows, 2026-07-14):
- **fair sequential arm** (filesystem + format): SELECT * into blackhole
  1215 MB/s (50.6 Mrows/s), into json 49 MB/s (2.1 Mrows/s). Single-thread
  reader cost 19.8 ns/row total = 2.1 ns raw page-cache read + ~17.7 ns Flink
  per-record emission floor; the csv parse costs 313 ns/row (15.8×).
- split probe (this connector, p=8): blackhole 6058 MB/s (252 Mrows/s), json
  331 MB/s (13.8 Mrows/s — the JSON encoder is the wall at ~500 ns/row/thread).
