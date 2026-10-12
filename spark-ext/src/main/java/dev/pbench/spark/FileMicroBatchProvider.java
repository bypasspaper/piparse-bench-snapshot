// SPDX-License-Identifier: Apache-2.0
package dev.pbench.spark;

import java.util.EnumSet;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import org.apache.spark.sql.connector.catalog.SupportsRead;
import org.apache.spark.sql.connector.catalog.Table;
import org.apache.spark.sql.connector.catalog.TableCapability;
import org.apache.spark.sql.connector.catalog.TableProvider;
import org.apache.spark.sql.connector.expressions.Transform;
import org.apache.spark.sql.connector.read.Scan;
import org.apache.spark.sql.connector.read.ScanBuilder;
import org.apache.spark.sql.connector.read.streaming.MicroBatchStream;
import org.apache.spark.sql.sources.DataSourceRegister;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.sql.util.CaseInsensitiveStringMap;

/**
 * A Spark DataSource V2 streaming source that reads ONE bounded local file as many byte-range,
 * line-aligned MICRO-BATCHES (settable via {@code maxBytesPerTrigger}) instead of the single
 * whole-file micro-batch the built-in file streaming source produces.
 *
 * <p>Why this exists: Spark's built-in {@code readStream.csv(dir)} admits WHOLE FILES per trigger
 * (its {@code maxBytesPerTrigger} is a no-op on a single file), so a bounded file with
 * {@code Trigger.AvailableNow} yields exactly ONE micro-batch whose "per-batch latency" is really
 * whole-file processing time — useless for a per-batch ingest-&gt;sink latency comparison against
 * NES (per-TupleBuffer) and Flink (per-readBatch). This source sub-divides the SAME single file
 * into byte-range chunks (no physical file split), producing one micro-batch per chunk and thus a
 * real per-batch latency distribution.
 *
 * <p>Parsing reuses Spark's own {@link org.apache.spark.sql.catalyst.csv.UnivocityParser} so the
 * parse cost is byte-identical to the native CSV reader — the source changes only HOW the file is
 * batched, not how a line becomes a row, keeping throughput comparable to the built-in reader.
 *
 * <p>Options: {@code path} (the single CSV file), {@code maxBytesPerTrigger} (chunk size in bytes,
 * default 4 MiB), {@code delimiter} (default {@code ,}). The schema MUST be supplied via
 * {@code readStream.schema(...)}.
 */
public class FileMicroBatchProvider implements TableProvider, DataSourceRegister {

    static final long DEFAULT_MAX_BYTES_PER_TRIGGER = 4L * 1024 * 1024;

    @Override
    public String shortName() {
        return "file-microbatch-csv";
    }

    @Override
    public boolean supportsExternalMetadata() {
        return true; // the caller always supplies a schema via readStream.schema(...)
    }

    @Override
    public StructType inferSchema(CaseInsensitiveStringMap options) {
        throw new UnsupportedOperationException(
                "file-microbatch-csv requires an explicit schema via readStream.schema(...)");
    }

    @Override
    public Table getTable(StructType schema, Transform[] partitioning, Map<String, String> properties) {
        return new FileMicroBatchTable(schema, properties);
    }
}

/** The (streaming-readable) table: carries the schema + resolves per-scan options. */
class FileMicroBatchTable implements SupportsRead {
    private final StructType schema;
    private final Map<String, String> tableProps;

    FileMicroBatchTable(StructType schema, Map<String, String> tableProps) {
        this.schema = schema;
        this.tableProps = tableProps;
    }

    @Override
    public String name() {
        return "file-microbatch-csv";
    }

    @Override
    public StructType schema() {
        return schema;
    }

    @Override
    public Set<TableCapability> capabilities() {
        return EnumSet.of(TableCapability.MICRO_BATCH_READ);
    }

    @Override
    public ScanBuilder newScanBuilder(CaseInsensitiveStringMap options) {
        // Resolve options case-insensitively from the scan options, falling back to the table
        // properties captured at getTable() time. `path` is required; the rest have defaults.
        String path = firstNonNull(options.get("path"), tableProps.get("path"));
        if (path == null) {
            throw new IllegalArgumentException("file-microbatch-csv requires a 'path' option");
        }
        long maxBytes = options.getLong("maxBytesPerTrigger",
                parseLong(tableProps.get("maxBytesPerTrigger"),
                        FileMicroBatchProvider.DEFAULT_MAX_BYTES_PER_TRIGGER));
        String delimiter = firstNonNull(options.get("delimiter"),
                firstNonNull(tableProps.get("delimiter"), ","));
        return new FileMicroBatchScanBuilder(schema, path, maxBytes, delimiter);
    }

    private static String firstNonNull(String a, String b) {
        return a != null ? a : b;
    }

    private static long parseLong(String v, long dflt) {
        if (v == null || v.isEmpty()) {
            return dflt;
        }
        return Long.parseLong(v.trim());
    }
}

/** Trivial scan builder — the source has no pushdown; it just carries the resolved config. */
class FileMicroBatchScanBuilder implements ScanBuilder {
    private final StructType schema;
    private final String path;
    private final long maxBytesPerTrigger;
    private final String delimiter;

    FileMicroBatchScanBuilder(StructType schema, String path, long maxBytesPerTrigger, String delimiter) {
        this.schema = schema;
        this.path = path;
        this.maxBytesPerTrigger = maxBytesPerTrigger;
        this.delimiter = delimiter;
    }

    @Override
    public Scan build() {
        return new FileMicroBatchScan(schema, path, maxBytesPerTrigger, delimiter);
    }
}

/** The scan — hands back the {@link FileMicroBatchStream} for micro-batch execution. */
class FileMicroBatchScan implements Scan {
    private final StructType schema;
    private final String path;
    private final long maxBytesPerTrigger;
    private final String delimiter;

    FileMicroBatchScan(StructType schema, String path, long maxBytesPerTrigger, String delimiter) {
        this.schema = schema;
        this.path = path;
        this.maxBytesPerTrigger = maxBytesPerTrigger;
        this.delimiter = delimiter;
    }

    @Override
    public StructType readSchema() {
        return schema;
    }

    @Override
    public String description() {
        return "file-microbatch-csv[" + path + ", maxBytesPerTrigger=" + maxBytesPerTrigger + "]";
    }

    @Override
    public MicroBatchStream toMicroBatchStream(String checkpointLocation) {
        return new FileMicroBatchStream(schema, path, maxBytesPerTrigger, delimiter);
    }
}
