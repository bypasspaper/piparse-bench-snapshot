// SPDX-License-Identifier: Apache-2.0
package dev.pbench.spark;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.RandomAccessFile;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;

import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.catalyst.csv.CSVOptions;
import org.apache.spark.sql.catalyst.csv.UnivocityParser;
import org.apache.spark.sql.connector.read.InputPartition;
import org.apache.spark.sql.connector.read.PartitionReader;
import org.apache.spark.sql.connector.read.PartitionReaderFactory;
import org.apache.spark.sql.connector.read.streaming.MicroBatchStream;
import org.apache.spark.sql.connector.read.streaming.Offset;
import org.apache.spark.sql.connector.read.streaming.ReadLimit;
import org.apache.spark.sql.connector.read.streaming.SupportsAdmissionControl;
import org.apache.spark.sql.connector.read.streaming.SupportsTriggerAvailableNow;
import org.apache.spark.sql.types.StructType;

/**
 * Reads ONE bounded file as byte-range, line-aligned micro-batches. Offsets ARE byte positions;
 * each {@link #latestOffset(Offset, ReadLimit)} advances the end offset by {@code maxBytesPerTrigger}
 * bytes (extended to the next newline so batches split on whole lines), so {@code AvailableNow}
 * walks the file in chunks to EOF — one micro-batch per chunk. Parsing reuses Spark's own
 * {@link UnivocityParser} (identical to the native CSV reader) so only the batching differs.
 */
public class FileMicroBatchStream
        implements MicroBatchStream, SupportsAdmissionControl, SupportsTriggerAvailableNow {

    private final StructType schema;
    private final String path;
    private final long maxBytesPerTrigger;
    private final String delimiter;
    private final long fileSize;

    private RandomAccessFile alignRaf; // lazily opened for line-boundary scans on the driver
    private long availableNowEnd = -1; // snapshotted EOF for the AvailableNow trigger

    FileMicroBatchStream(StructType schema, String path, long maxBytesPerTrigger, String delimiter) {
        this.schema = schema;
        this.path = path;
        this.maxBytesPerTrigger = maxBytesPerTrigger > 0 ? maxBytesPerTrigger
                : FileMicroBatchProvider.DEFAULT_MAX_BYTES_PER_TRIGGER;
        this.delimiter = delimiter;
        this.fileSize = new java.io.File(path).length();
    }

    // ── SparkDataStream ──────────────────────────────────────────────────────

    @Override
    public Offset initialOffset() {
        return new LongByteOffset(0L);
    }

    @Override
    public Offset deserializeOffset(String json) {
        return new LongByteOffset(Long.parseLong(json.trim()));
    }

    @Override
    public void commit(Offset end) {
        // bounded local file — no external offset store to advance
    }

    @Override
    public void stop() {
        synchronized (this) {
            if (alignRaf != null) {
                try {
                    alignRaf.close();
                } catch (IOException ignored) {
                    // best-effort close
                }
                alignRaf = null;
            }
        }
    }

    // ── MicroBatchStream ─────────────────────────────────────────────────────

    /** No-arg latestOffset = all data available (whole file). */
    @Override
    public Offset latestOffset() {
        return new LongByteOffset(fileSize);
    }

    @Override
    public InputPartition[] planInputPartitions(Offset start, Offset end) {
        long s = ((LongByteOffset) start).pos;
        long e = ((LongByteOffset) end).pos;
        if (e <= s) {
            return new InputPartition[0];
        }
        // One partition per batch = the fair single-sequential-reader model (parallelism across
        // concurrent STREAMS, not within one stream's batch).
        return new InputPartition[] {new FilePartition(path, s, e)};
    }

    @Override
    public PartitionReaderFactory createReaderFactory() {
        return new ReaderFactory(schema, delimiter);
    }

    // ── SupportsAdmissionControl / SupportsTriggerAvailableNow ────────────────

    @Override
    public ReadLimit getDefaultReadLimit() {
        // Anything other than ReadAllAvailable makes the AvailableNow driver ask us for an offset
        // per trigger; latestOffset(start, limit) below ignores the limit and chunks by bytes.
        return ReadLimit.maxRows(Long.MAX_VALUE);
    }

    @Override
    public void prepareForTriggerAvailableNow() {
        availableNowEnd = fileSize; // snapshot EOF once; the file is bounded and static
    }

    @Override
    public Offset latestOffset(Offset startOffset, ReadLimit limit) {
        long s = ((LongByteOffset) startOffset).pos;
        long cap = availableNowEnd >= 0 ? availableNowEnd : fileSize;
        if (s >= cap) {
            return new LongByteOffset(s); // no progress -> AvailableNow terminates
        }
        long tentative = Math.min(s + maxBytesPerTrigger, cap);
        long end = tentative >= cap ? cap : alignForward(tentative, cap);
        if (end <= s) {
            end = cap; // a single line longer than the chunk: take it whole (guaranteed progress)
        }
        return new LongByteOffset(end);
    }

    /** Advance {@code from} to just after the next {@code '\n'} (or {@code cap} if none before it). */
    private long alignForward(long from, long cap) {
        try {
            RandomAccessFile raf = alignRaf();
            raf.seek(from);
            byte[] buf = new byte[65536];
            long p = from;
            while (p < cap) {
                int n = raf.read(buf);
                if (n <= 0) {
                    break;
                }
                int scan = (int) Math.min(n, cap - p);
                for (int i = 0; i < scan; i++) {
                    if (buf[i] == '\n') {
                        return p + i + 1;
                    }
                }
                p += n;
            }
            return cap;
        } catch (IOException ex) {
            throw new RuntimeException("line-alignment read failed at " + from + " in " + path, ex);
        }
    }

    private synchronized RandomAccessFile alignRaf() throws IOException {
        if (alignRaf == null) {
            alignRaf = new RandomAccessFile(path, "r");
        }
        return alignRaf;
    }

    // ── offset / partition / reader ──────────────────────────────────────────

    /** Offset = absolute byte position in the file; json() is the plain decimal for the commit log. */
    static final class LongByteOffset extends Offset {
        final long pos;

        LongByteOffset(long pos) {
            this.pos = pos;
        }

        @Override
        public String json() {
            return Long.toString(pos);
        }
    }

    /** A byte range [start, end) of the file — guaranteed to cover whole lines. */
    static final class FilePartition implements InputPartition {
        private static final long serialVersionUID = 1L;
        final String path;
        final long start;
        final long end;

        FilePartition(String path, long start, long end) {
            this.path = path;
            this.start = start;
            this.end = end;
        }
    }

    /** Serializable factory; builds a fresh UnivocityParser per partition on the executor. */
    static final class ReaderFactory implements PartitionReaderFactory {
        private static final long serialVersionUID = 1L;
        private final StructType schema;
        private final String delimiter;

        ReaderFactory(StructType schema, String delimiter) {
            this.schema = schema;
            this.delimiter = delimiter;
        }

        @Override
        public PartitionReader<InternalRow> createReader(InputPartition partition) {
            FilePartition fp = (FilePartition) partition;
            return new CsvChunkReader(schema, delimiter, fp.path, fp.start, fp.end);
        }
    }

    /** Reads whole lines in [start, end) and parses each with Spark's own UnivocityParser. */
    static final class CsvChunkReader implements PartitionReader<InternalRow> {
        private final scala.Function1<String, scala.Option<InternalRow>> parse;
        private final BufferedReader reader;
        private InternalRow current;

        CsvChunkReader(StructType schema, String delimiter, String path, long start, long end) {
            Map<String, String> opts = new HashMap<>();
            opts.put("sep", delimiter);
            opts.put("header", "false");
            opts.put("mode", "FAILFAST"); // mirror the native reader's mode in the driver
            CSVOptions csvOptions = new CSVOptions(scalaMap(opts), false, "UTC");
            UnivocityParser parser = new UnivocityParser(schema, schema, csvOptions);
            this.parse = parser.parse();
            try {
                FileInputStream fis = new FileInputStream(path);
                fis.getChannel().position(start);
                InputStream bounded = new BoundedInputStream(fis, end - start);
                this.reader = new BufferedReader(
                        new InputStreamReader(bounded, StandardCharsets.UTF_8), 1 << 20);
            } catch (IOException ex) {
                throw new RuntimeException("failed to open chunk [" + start + "," + end + ") of " + path, ex);
            }
        }

        @Override
        public boolean next() throws IOException {
            String line;
            while ((line = reader.readLine()) != null) {
                if (line.isEmpty()) {
                    continue;
                }
                scala.Option<InternalRow> row = parse.apply(line);
                if (row.isDefined()) {
                    current = row.get();
                    return true;
                }
            }
            return false;
        }

        @Override
        public InternalRow get() {
            return current;
        }

        @Override
        public void close() throws IOException {
            reader.close();
        }
    }

    /** An InputStream that returns EOF after {@code limit} bytes (bounds a chunk to its byte range). */
    static final class BoundedInputStream extends InputStream {
        private final InputStream in;
        private long remaining;

        BoundedInputStream(InputStream in, long limit) {
            this.in = in;
            this.remaining = limit;
        }

        @Override
        public int read() throws IOException {
            if (remaining <= 0) {
                return -1;
            }
            int b = in.read();
            if (b >= 0) {
                remaining--;
            }
            return b;
        }

        @Override
        public int read(byte[] buf, int off, int len) throws IOException {
            if (remaining <= 0) {
                return -1;
            }
            int toRead = (int) Math.min(len, remaining);
            int n = in.read(buf, off, toRead);
            if (n > 0) {
                remaining -= n;
            }
            return n;
        }

        @Override
        public void close() throws IOException {
            in.close();
        }
    }

    /** Build an immutable Scala Map from a Java Map (for the CSVOptions constructor). */
    @SuppressWarnings("unchecked")
    private static scala.collection.immutable.Map<String, String> scalaMap(Map<String, String> java) {
        scala.collection.immutable.Map<String, String> m = scala.collection.immutable.Map$.MODULE$.empty();
        for (Map.Entry<String, String> e : java.entrySet()) {
            m = m.updated(e.getKey(), e.getValue());
        }
        return m;
    }
}
