// SPDX-License-Identifier: Apache-2.0
package dev.pbench.flink;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.api.connector.sink2.Sink;
import org.apache.flink.api.connector.sink2.SinkWriter;
import org.apache.flink.api.connector.sink2.WriterInitContext;
import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ConfigOptions;
import org.apache.flink.table.connector.ChangelogMode;
import org.apache.flink.table.connector.Projection;
import org.apache.flink.table.connector.format.EncodingFormat;
import org.apache.flink.table.connector.sink.DynamicTableSink;
import org.apache.flink.table.connector.sink.SinkV2Provider;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.utils.ProjectedRowData;
import org.apache.flink.table.factories.DynamicTableFactory;
import org.apache.flink.table.factories.DynamicTableSinkFactory;
import org.apache.flink.table.factories.FactoryUtil;
import org.apache.flink.table.factories.SerializationFormatFactory;
import org.apache.flink.table.types.DataType;

/**
 * A void sink that measures BOTH csv-&gt;json output volume AND end-to-end per-batch latency — the
 * fair Flink analog of NebulaStream's MonitoringSink over a JSON output formatter.
 *
 * <p>This is the union of {@link FormatBlackHoleTableSinkFactory} (run Flink's real
 * JsonRowDataSerializationSchema on every row, count the bytes, discard them) and
 * {@link LatencyBlackHoleTableSinkFactory} (stamp {@code System.nanoTime() - __ingest_ns} per
 * sampled row). It exists because the csv-&gt;json scaling arm needs the JSON serializer to run
 * (otherwise Flink's measured cost excludes serialization and is unfair) WHILE the same run
 * reports per-batch latency (the latency panel of the 1-&gt;1024 scaling figure).
 *
 * <p>The sink's physical row is {@code [ <query output columns...>, __ingest_ns BIGINT ]}: the
 * trailing {@code __ingest_ns} is the ingestion stamp carried from the {@code csv-latency} source
 * and must NOT be serialized into the JSON output. So the JSON encoder is built over the PROJECTED
 * row type (all columns except the last) and each row is serialized through a {@link
 * ProjectedRowData} view over indices {@code [0 .. arity-2]}; latency reads {@code
 * element.getLong(arity-1)}. Serialization runs on EVERY row (fair throughput); the arrival clock
 * is only read on sampled rows (one every {@code sample-every}, the NES per-TupleBuffer analog).
 *
 * <p>Each subtask prints, on close, {@code PBENCH_VOIDSINK bytes=N rows=M} (serialized volume) and
 * {@code PBENCH_LATENCY subtask=i ... p50/p99/p999} + {@code PBENCH_LATENCY_SAMPLES <raw ns csv>}
 * (the driver pools the raw samples across all concurrent pipelines for exact percentiles).
 */
public class JsonLatencyBlackHoleTableSinkFactory implements DynamicTableSinkFactory {

    public static final String IDENTIFIER = "json-latency-blackhole";

    public static final ConfigOption<Integer> SAMPLE_EVERY =
            ConfigOptions.key("sample-every")
                    .intType()
                    .defaultValue(1)
                    .withDescription("Retain one latency sample every N rows (NES per-buffer analog).");

    @Override
    public String factoryIdentifier() {
        return IDENTIFIER;
    }

    @Override
    public Set<ConfigOption<?>> requiredOptions() {
        Set<ConfigOption<?>> options = new HashSet<>();
        options.add(FactoryUtil.FORMAT); // 'format' = 'json'
        return options;
    }

    @Override
    public Set<ConfigOption<?>> optionalOptions() {
        Set<ConfigOption<?>> options = new HashSet<>();
        options.add(SAMPLE_EVERY);
        return options;
    }

    @Override
    public DynamicTableSink createDynamicTableSink(DynamicTableFactory.Context context) {
        FactoryUtil.TableFactoryHelper helper = FactoryUtil.createTableFactoryHelper(this, context);
        EncodingFormat<SerializationSchema<RowData>> format =
                helper.discoverEncodingFormat(SerializationFormatFactory.class, FactoryUtil.FORMAT);
        int sampleEvery = helper.getOptions().get(SAMPLE_EVERY);
        helper.validate();
        DataType physical = context.getPhysicalRowDataType();
        int arity = DataType.getFieldCount(physical);
        // Serialize every column EXCEPT the trailing __ingest_ns (indices 0 .. arity-2).
        int[] keep = new int[Math.max(0, arity - 1)];
        for (int i = 0; i < keep.length; i++) {
            keep[i] = i;
        }
        DataType projected = Projection.of(keep).project(physical);
        return new JsonLatencySink(format, projected, keep, arity, Math.max(1, sampleEvery));
    }

    private static class JsonLatencySink implements DynamicTableSink {
        private final EncodingFormat<SerializationSchema<RowData>> format;
        private final DataType projectedDataType;
        private final int[] keep;
        private final int arity;
        private final int sampleEvery;

        JsonLatencySink(
                EncodingFormat<SerializationSchema<RowData>> format,
                DataType projectedDataType,
                int[] keep,
                int arity,
                int sampleEvery) {
            this.format = format;
            this.projectedDataType = projectedDataType;
            this.keep = keep;
            this.arity = arity;
            this.sampleEvery = sampleEvery;
        }

        @Override
        public ChangelogMode getChangelogMode(ChangelogMode requestedMode) {
            return ChangelogMode.insertOnly();
        }

        @Override
        public SinkRuntimeProvider getSinkRuntimeProvider(Context context) {
            SerializationSchema<RowData> serializer =
                    format.createRuntimeEncoder(context, projectedDataType);
            return SinkV2Provider.of(new JsonLatencyVoidSink(serializer, keep, arity, sampleEvery));
        }

        @Override
        public DynamicTableSink copy() {
            return new JsonLatencySink(format, projectedDataType, keep, arity, sampleEvery);
        }

        @Override
        public String asSummaryString() {
            return "BlackHole(json+latency)";
        }
    }

    private static class JsonLatencyVoidSink implements Sink<RowData> {
        private static final long serialVersionUID = 1L;
        private final SerializationSchema<RowData> serializer;
        private final int[] keep;
        private final int arity;
        private final int sampleEvery;

        JsonLatencyVoidSink(
                SerializationSchema<RowData> serializer, int[] keep, int arity, int sampleEvery) {
            this.serializer = serializer;
            this.keep = keep;
            this.arity = arity;
            this.sampleEvery = sampleEvery;
        }

        @Override
        public SinkWriter<RowData> createWriter(WriterInitContext context) throws IOException {
            try {
                serializer.open(context.asSerializationSchemaInitializationContext());
            } catch (Exception e) {
                throw new IOException("Failed to open serialization schema", e);
            }
            int subtask = context.getTaskInfo().getIndexOfThisSubtask();
            return new JsonLatencyVoidWriter(serializer, keep, arity, sampleEvery, subtask);
        }
    }

    private static class JsonLatencyVoidWriter implements SinkWriter<RowData> {
        private final SerializationSchema<RowData> serializer;
        private final ProjectedRowData projectedRow;
        private final int ingestIdx;
        private final int sampleEvery;
        private final int subtask;
        private final List<Long> samples = new ArrayList<>();
        private long bytes;
        private long rows;

        JsonLatencyVoidWriter(
                SerializationSchema<RowData> serializer,
                int[] keep,
                int arity,
                int sampleEvery,
                int subtask) {
            this.serializer = serializer;
            this.projectedRow = ProjectedRowData.from(keep);
            this.ingestIdx = arity - 1; // __ingest_ns appended as the last projected column
            this.sampleEvery = sampleEvery;
            this.subtask = subtask;
        }

        @Override
        public void write(RowData element, Context context) {
            // Serialize the JSON on EVERY row (fair csv->json throughput) over a projection that
            // drops the trailing __ingest_ns; count the produced bytes and discard them.
            byte[] serialized = serializer.serialize(projectedRow.replaceRow(element));
            if (serialized != null) {
                bytes += serialized.length;
            }
            // Sample the arrival clock only every sample-every rows (a per-row nanoTime over 31 M
            // rows would add measurable overhead); sample BEFORE incrementing so row 0 is retained.
            if (rows % sampleEvery == 0) {
                long ingest = element.getLong(ingestIdx);
                samples.add(System.nanoTime() - ingest);
            }
            rows++;
        }

        @Override
        public void flush(boolean endOfInput) {}

        @Override
        public void close() {
            System.out.println("PBENCH_VOIDSINK bytes=" + bytes + " rows=" + rows);
            long n = samples.size();
            long min = 0;
            long max = 0;
            long p50 = 0;
            long p99 = 0;
            long p999 = 0;
            double avg = 0.0;
            if (n > 0) {
                Collections.sort(samples);
                long sum = 0;
                for (long v : samples) {
                    sum += v;
                }
                min = samples.get(0);
                max = samples.get(samples.size() - 1);
                avg = (double) sum / (double) n;
                p50 = percentile(0.50);
                p99 = percentile(0.99);
                p999 = percentile(0.999);
            }
            System.out.println(
                    "PBENCH_LATENCY subtask=" + subtask
                            + " rows=" + rows
                            + " samples=" + n
                            + " min=" + min
                            + " max=" + max
                            + " avg=" + avg
                            + " p50=" + p50
                            + " p99=" + p99
                            + " p999=" + p999);
            if (n > 0) {
                StringBuilder sb = new StringBuilder("PBENCH_LATENCY_SAMPLES ");
                for (int i = 0; i < samples.size(); i++) {
                    if (i > 0) {
                        sb.append(',');
                    }
                    sb.append(samples.get(i).longValue());
                }
                System.out.println(sb.toString());
            }
        }

        // nearest-rank over the sorted samples (matches MonitoringSink::stop's pct()).
        private long percentile(double p) {
            int size = samples.size();
            int idx = (int) (p * (size - 1) + 0.5);
            if (idx < 0) {
                idx = 0;
            }
            if (idx >= size) {
                idx = size - 1;
            }
            return samples.get(idx);
        }
    }
}
