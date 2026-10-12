// SPDX-License-Identifier: Apache-2.0
package dev.pbench.flink;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.apache.flink.api.connector.sink2.Sink;
import org.apache.flink.api.connector.sink2.SinkWriter;
import org.apache.flink.api.connector.sink2.WriterInitContext;
import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ConfigOptions;
import org.apache.flink.table.connector.ChangelogMode;
import org.apache.flink.table.connector.sink.DynamicTableSink;
import org.apache.flink.table.connector.sink.SinkV2Provider;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.factories.DynamicTableFactory;
import org.apache.flink.table.factories.DynamicTableSinkFactory;
import org.apache.flink.table.factories.FactoryUtil;
import org.apache.flink.table.types.DataType;

/**
 * A void sink that measures END-TO-END PER-ROW LATENCY, the fair Flink analog of NebulaStream's
 * MonitoringSink.
 *
 * <p>NES stamps {@code sourceCreationTimestamp} (us) on a TupleBuffer at read-start and, at the
 * sink, records {@code arrival - creation} per buffer, then reports p50/p99/p999 over all retained
 * per-buffer samples. Here the {@code csv-latency} source format (see
 * {@link CsvLatencyFileFormatFactory}) stamps an ingestion time at read/parse start and appends it
 * as the LAST column ({@code __ingest_ns}); this sink reads it, computes {@code System.nanoTime() -
 * ingest_ns} on every row (a valid subtraction because both stamps are JVM-global nanoTime in the
 * one MiniCluster process, even across the fetcher/task threads), and — to mirror NES's PER-BUFFER
 * granularity — retains one sample every {@code sample-every} rows (set by the driver to
 * operator_buffer_size / row_bytes, so one sample ≈ one NES TupleBuffer's worth of input). On close
 * each subtask prints
 * {@code PBENCH_LATENCY subtask=i rows=N samples=M p50=.. p99=.. p999=.. min=.. max=.. avg=..}
 * with all latencies in NANOSECONDS.
 *
 * <p>Because the stamp is taken on the source's read/parse path and carried through Flink's own
 * source→task queue, this measures the WHOLE end-to-end path (read + native parse + queue +
 * projection + sink) — the fair analog of NES's ingest→sink latency, not just the operator handoff.
 */
public class LatencyBlackHoleTableSinkFactory implements DynamicTableSinkFactory {

    public static final String IDENTIFIER = "latency-blackhole";

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
        return Collections.emptySet();
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
        int sampleEvery = helper.getOptions().get(SAMPLE_EVERY);
        helper.validate();
        int arity = DataType.getFieldCount(context.getPhysicalRowDataType());
        return new LatencySink(Math.max(1, sampleEvery), arity);
    }

    private static class LatencySink implements DynamicTableSink {
        private final int sampleEvery;
        private final int arity;

        LatencySink(int sampleEvery, int arity) {
            this.sampleEvery = sampleEvery;
            this.arity = arity;
        }

        @Override
        public ChangelogMode getChangelogMode(ChangelogMode requestedMode) {
            return ChangelogMode.insertOnly();
        }

        @Override
        public SinkRuntimeProvider getSinkRuntimeProvider(Context context) {
            return SinkV2Provider.of(new LatencyVoidSink(sampleEvery, arity));
        }

        @Override
        public DynamicTableSink copy() {
            return new LatencySink(sampleEvery, arity);
        }

        @Override
        public String asSummaryString() {
            return "BlackHole(latency)";
        }
    }

    private static class LatencyVoidSink implements Sink<RowData> {
        private static final long serialVersionUID = 1L;
        private final int sampleEvery;
        private final int arity;

        LatencyVoidSink(int sampleEvery, int arity) {
            this.sampleEvery = sampleEvery;
            this.arity = arity;
        }

        @Override
        public SinkWriter<RowData> createWriter(WriterInitContext context) throws IOException {
            int subtask = context.getTaskInfo().getIndexOfThisSubtask();
            return new LatencyVoidWriter(sampleEvery, arity, subtask);
        }
    }

    private static class LatencyVoidWriter implements SinkWriter<RowData> {
        private final int sampleEvery;
        private final int ingestIdx;
        private final int subtask;
        private final List<Long> samples = new ArrayList<>();
        private long rows;

        LatencyVoidWriter(int sampleEvery, int arity, int subtask) {
            this.sampleEvery = sampleEvery;
            this.ingestIdx = arity - 1; // now_ns() is appended as the last projected column
            this.subtask = subtask;
        }

        @Override
        public void write(RowData element, Context context) {
            // Only take the arrival stamp for SAMPLED rows — a per-row System.nanoTime() (~20 ns)
            // over every row would add ~0.6 s / 31 M rows of pure measurement overhead. Sampling
            // the clock too keeps instrumentation negligible while the retained sample's latency is
            // still exact. Sample BEFORE incrementing so the first row (rows==0) is always retained.
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
            // Raw samples (ns) so the driver can POOL exact percentiles across all concurrent
            // pipelines' sinks (median-of-per-pipeline-percentiles would not equal the pooled
            // percentile). One line per sink subtask; sampled count keeps this small (~thousands).
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
