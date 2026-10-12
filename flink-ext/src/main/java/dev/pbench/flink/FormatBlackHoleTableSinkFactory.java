// SPDX-License-Identifier: Apache-2.0
package dev.pbench.flink;

import java.io.IOException;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;

import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.api.connector.sink2.Sink;
import org.apache.flink.api.connector.sink2.SinkWriter;
import org.apache.flink.api.connector.sink2.WriterInitContext;
import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.table.connector.ChangelogMode;
import org.apache.flink.table.connector.format.EncodingFormat;
import org.apache.flink.table.connector.sink.DynamicTableSink;
import org.apache.flink.table.connector.sink.SinkV2Provider;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.factories.DynamicTableFactory;
import org.apache.flink.table.factories.DynamicTableSinkFactory;
import org.apache.flink.table.factories.FactoryUtil;
import org.apache.flink.table.factories.SerializationFormatFactory;
import org.apache.flink.table.types.DataType;

/**
 * A blackhole sink that ACCEPTS a {@code format} option.
 *
 * <p>Flink's built-in {@code blackhole} connector rejects {@code 'format'} (it discards RowData
 * with ZERO output serialization), so a fair csv-&gt;json benchmark arm was impossible: the
 * JSON_OBJECT()-in-the-query workaround measures Calcite's per-row JSON-node builder, and the
 * filesystem sink measures the real serializer PLUS file-writer overhead. This connector runs
 * every RowData through the discovered {@link EncodingFormat} (for {@code 'format'='json'} that
 * is Flink's own JsonRowDataSerializationSchema, schema-ordered keys — byte-identical to what a
 * Kafka/filesystem sink would emit) and then drops the bytes.
 *
 * <p>Each sink subtask prints {@code PBENCH_VOIDSINK bytes=N rows=M} on close; besides giving the
 * driver a serialized-volume line, the counter keeps {@code serialize()} observably live.
 */
public class FormatBlackHoleTableSinkFactory implements DynamicTableSinkFactory {

    public static final String IDENTIFIER = "blackhole-format";

    @Override
    public String factoryIdentifier() {
        return IDENTIFIER;
    }

    @Override
    public Set<ConfigOption<?>> requiredOptions() {
        Set<ConfigOption<?>> options = new HashSet<>();
        options.add(FactoryUtil.FORMAT);
        return options;
    }

    @Override
    public Set<ConfigOption<?>> optionalOptions() {
        return Collections.emptySet();
    }

    @Override
    public DynamicTableSink createDynamicTableSink(DynamicTableFactory.Context context) {
        FactoryUtil.TableFactoryHelper helper = FactoryUtil.createTableFactoryHelper(this, context);
        EncodingFormat<SerializationSchema<RowData>> format =
                helper.discoverEncodingFormat(SerializationFormatFactory.class, FactoryUtil.FORMAT);
        helper.validate();
        return new FormatBlackHoleSink(format, context.getPhysicalRowDataType());
    }

    private static class FormatBlackHoleSink implements DynamicTableSink {
        private final EncodingFormat<SerializationSchema<RowData>> format;
        private final DataType physicalDataType;

        FormatBlackHoleSink(
                EncodingFormat<SerializationSchema<RowData>> format, DataType physicalDataType) {
            this.format = format;
            this.physicalDataType = physicalDataType;
        }

        @Override
        public ChangelogMode getChangelogMode(ChangelogMode requestedMode) {
            return requestedMode;
        }

        @Override
        public SinkRuntimeProvider getSinkRuntimeProvider(Context context) {
            SerializationSchema<RowData> serializer =
                    format.createRuntimeEncoder(context, physicalDataType);
            return SinkV2Provider.of(new SerializingVoidSink(serializer));
        }

        @Override
        public DynamicTableSink copy() {
            return new FormatBlackHoleSink(format, physicalDataType);
        }

        @Override
        public String asSummaryString() {
            return "BlackHole(format)";
        }
    }

    private static class SerializingVoidSink implements Sink<RowData> {
        private static final long serialVersionUID = 1L;
        private final SerializationSchema<RowData> serializer;

        SerializingVoidSink(SerializationSchema<RowData> serializer) {
            this.serializer = serializer;
        }

        @Override
        public SinkWriter<RowData> createWriter(WriterInitContext context) throws IOException {
            try {
                serializer.open(context.asSerializationSchemaInitializationContext());
            } catch (Exception e) {
                throw new IOException("Failed to open serialization schema", e);
            }
            return new SerializingVoidWriter(serializer);
        }
    }

    private static class SerializingVoidWriter implements SinkWriter<RowData> {
        private final SerializationSchema<RowData> serializer;
        private long bytes;
        private long rows;

        SerializingVoidWriter(SerializationSchema<RowData> serializer) {
            this.serializer = serializer;
        }

        @Override
        public void write(RowData element, Context context) {
            byte[] serialized = serializer.serialize(element);
            if (serialized != null) {
                bytes += serialized.length;
            }
            rows++;
        }

        @Override
        public void flush(boolean endOfInput) {}

        @Override
        public void close() {
            System.out.println("PBENCH_VOIDSINK bytes=" + bytes + " rows=" + rows);
        }
    }
}
