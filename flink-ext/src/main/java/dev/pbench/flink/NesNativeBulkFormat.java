// SPDX-License-Identifier: Apache-2.0
package dev.pbench.flink;

import java.io.EOFException;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.file.src.FileSourceSplit;
import org.apache.flink.connector.file.src.reader.BulkFormat;
import org.apache.flink.connector.file.src.util.MutableRecordAndPosition;
import org.apache.flink.connector.file.src.util.RecordAndPosition;
import org.apache.flink.core.fs.FSDataInputStream;
import org.apache.flink.core.memory.MemorySegment;
import org.apache.flink.core.memory.MemorySegmentFactory;
import org.apache.flink.table.data.ArrayData;
import org.apache.flink.table.data.DecimalData;
import org.apache.flink.table.data.MapData;
import org.apache.flink.table.data.RawValueData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.StringData;
import org.apache.flink.table.data.TimestampData;
import org.apache.flink.table.data.binary.BinaryStringData;
import org.apache.flink.table.runtime.typeutils.InternalTypeInfo;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.types.RowKind;
import org.apache.flink.types.variant.Variant;

/**
 * BulkFormat for NES "native" files: fixed-width packed little-endian rows, no header, no null
 * bitmap (the layout written by nes-systests/testdata/small/csv_to_binary.py).
 *
 * <p>Design goal: ZERO per-field parsing or conversion. Each batch reads a chunk of bytes off
 * the stream; rows are served through a reused flyweight {@link RowData} whose accessors are
 * single little-endian loads straight from the chunk ({@code ByteBuffer.getLong} — intrinsified
 * to one mov on x86). Strings are NUL-padded {@code string[N]} slots, surfaced as zero-copy
 * {@link BinaryStringData} views over the chunk (the only per-access work is the bounded strlen
 * scan for the NUL terminator). The only unavoidable widenings are zero/sign extension for
 * unsigned/narrow ints mapped to wider Flink types (a single AND/shift, not a parse); uint64 is
 * reinterpreted as BIGINT (values >= 2^63 would wrap — fine for ms-epoch timestamps).
 *
 * <p>Splittable at row boundaries: a row belongs to the split containing its first byte; the
 * reader aligns the split start up to the next row boundary and may read past the split end to
 * finish its last row.
 */
public class NesNativeBulkFormat implements BulkFormat<RowData, FileSourceSplit> {

    private static final long serialVersionUID = 1L;

    // ── layout spec (csv_to_binary.py schema syntax) ────────────────────────────

    /** Per-field storage kind; ordinal is stored in a byte[] so the row flyweight stays lean. */
    enum Kind {
        I8(1), I16(2), I32(4), I64(8),
        U8(1), U16(2), U32(4), U64(8),
        F32(4), F64(8),
        BOOL(1), CHAR(1), STRING(-1);

        final int width; // -1 = width comes from string[N]

        Kind(int width) {
            this.width = width;
        }
    }

    private static final Pattern STRING_RE = Pattern.compile("^string\\[(\\d+)]$");
    private static final Kind[] KINDS = Kind.values(); // values() clones — cache for the hot path

    static final class Layout implements java.io.Serializable {
        private static final long serialVersionUID = 1L;
        final byte[] kinds;      // Kind ordinals per field
        final int[] offsets;     // byte offset of each field within the row
        final int[] widths;      // byte width of each field (strings: N incl padding)
        final int rowWidth;

        Layout(List<Kind> kindList, List<Integer> widthList) {
            int n = kindList.size();
            kinds = new byte[n];
            offsets = new int[n];
            widths = new int[n];
            int off = 0;
            for (int i = 0; i < n; i++) {
                kinds[i] = (byte) kindList.get(i).ordinal();
                offsets[i] = off;
                widths[i] = widthList.get(i);
                off += widths[i];
            }
            rowWidth = off;
        }
    }

    /** Parse the csv_to_binary schema string, e.g. "uint64,int32,float64,string[37],bool". */
    static Layout parseLayout(String schema) {
        List<Kind> kinds = new ArrayList<>();
        List<Integer> widths = new ArrayList<>();
        for (String raw : schema.split(",")) {
            String t = raw.strip().toLowerCase(Locale.ROOT);
            Matcher m = STRING_RE.matcher(t);
            Kind kind;
            int width;
            if (m.matches()) {
                kind = Kind.STRING;
                width = Integer.parseInt(m.group(1));
                if (width < 1) {
                    throw new IllegalArgumentException("string[N] needs N >= 1: " + raw);
                }
            } else {
                switch (t) {
                    case "int8": kind = Kind.I8; break;
                    case "int16": kind = Kind.I16; break;
                    case "int32": kind = Kind.I32; break;
                    case "int64": kind = Kind.I64; break;
                    case "uint8": kind = Kind.U8; break;
                    case "uint16": kind = Kind.U16; break;
                    case "uint32": kind = Kind.U32; break;
                    case "uint64": kind = Kind.U64; break;
                    case "float32": kind = Kind.F32; break;
                    case "float64": kind = Kind.F64; break;
                    case "bool": kind = Kind.BOOL; break;
                    case "char": kind = Kind.CHAR; break;
                    default:
                        throw new IllegalArgumentException(
                                "unknown nes-native type '" + raw + "' in schema '" + schema + "'");
                }
                width = kind.width;
            }
            kinds.add(kind);
            widths.add(width);
        }
        if (kinds.isEmpty()) {
            throw new IllegalArgumentException("empty nes-native schema");
        }
        return new Layout(kinds, widths);
    }

    // ── format ──────────────────────────────────────────────────────────────────

    private final Layout layout;
    private final RowType producedRowType;
    private final int fetchSize;
    private final boolean reuseRow;

    NesNativeBulkFormat(Layout layout, RowType producedRowType, int fetchSize, boolean reuseRow) {
        if (layout.kinds.length != producedRowType.getFieldCount()) {
            throw new IllegalArgumentException(
                    "nes-native schema has " + layout.kinds.length + " fields but the table has "
                            + producedRowType.getFieldCount());
        }
        this.layout = layout;
        this.producedRowType = producedRowType;
        this.fetchSize = Math.max(fetchSize, layout.rowWidth);
        this.reuseRow = reuseRow;
    }

    @Override
    public Reader<RowData> createReader(Configuration config, FileSourceSplit split)
            throws IOException {
        return doCreateReader(split, -1L);
    }

    @Override
    public Reader<RowData> restoreReader(Configuration config, FileSourceSplit split)
            throws IOException {
        long restoreOffset =
                split.getReaderPosition().map(p -> p.getOffset()).orElse(-1L);
        return doCreateReader(split, restoreOffset);
    }

    private Reader<RowData> doCreateReader(FileSourceSplit split, long restoreOffset)
            throws IOException {
        final long rowWidth = layout.rowWidth;
        if (split.fileSize() % rowWidth != 0) {
            throw new IOException(
                    "nes-native file " + split.path() + " size " + split.fileSize()
                            + " is not a multiple of the row width " + rowWidth
                            + " — schema/file mismatch?");
        }
        long firstRow = (split.offset() + rowWidth - 1) / rowWidth;
        long endRow = (split.offset() + split.length() + rowWidth - 1) / rowWidth;
        long startByte = firstRow * rowWidth;
        if (restoreOffset >= 0) {
            // record offsets we emit are exact row boundaries with skipCount 0
            startByte = Math.max(startByte, restoreOffset);
        }
        FSDataInputStream stream = split.path().getFileSystem().open(split.path());
        stream.seek(startByte);
        long remainingRows = Math.max(0, endRow - firstRow - (startByte - firstRow * rowWidth) / rowWidth);
        return new NesReader(stream, startByte, remainingRows);
    }

    @Override
    public boolean isSplittable() {
        return true;
    }

    @Override
    public TypeInformation<RowData> getProducedType() {
        return InternalTypeInfo.of(producedRowType);
    }

    // ── reader ──────────────────────────────────────────────────────────────────

    private final class NesReader implements Reader<RowData> {
        private final FSDataInputStream stream;
        private long nextRowByteOffset; // absolute file offset of the next unread row
        private long remainingRows;

        NesReader(FSDataInputStream stream, long startByte, long remainingRows) {
            this.stream = stream;
            this.nextRowByteOffset = startByte;
            this.remainingRows = remainingRows;
        }

        @Override
        public RecordIterator<RowData> readBatch() throws IOException {
            if (remainingRows <= 0) {
                return null;
            }
            int rowsThisBatch = (int) Math.min(remainingRows, fetchSize / layout.rowWidth);
            int bytes = rowsThisBatch * layout.rowWidth;
            byte[] chunk = new byte[bytes];
            int pos = 0;
            while (pos < bytes) {
                int n = stream.read(chunk, pos, bytes - pos);
                if (n < 0) {
                    throw new EOFException(
                            "unexpected EOF: wanted " + bytes + " bytes, got " + pos);
                }
                pos += n;
            }
            long batchStart = nextRowByteOffset;
            nextRowByteOffset += bytes;
            remainingRows -= rowsThisBatch;
            return new ChunkIterator(chunk, batchStart, rowsThisBatch);
        }

        @Override
        public void close() throws IOException {
            stream.close();
        }
    }

    private final class ChunkIterator implements RecordIterator<RowData> {
        private final byte[] chunk;
        private final long chunkFileOffset;
        private final int rowCount;
        private final PackedRowData reused; // null when reuseRow == false
        private final MemorySegment[] segments;
        private final ByteBuffer le;
        private final MutableRecordAndPosition<RowData> position = new MutableRecordAndPosition<>();
        private int nextRow;

        ChunkIterator(byte[] chunk, long chunkFileOffset, int rowCount) {
            this.chunk = chunk;
            this.chunkFileOffset = chunkFileOffset;
            this.rowCount = rowCount;
            this.segments = new MemorySegment[] {MemorySegmentFactory.wrap(chunk)};
            this.le = ByteBuffer.wrap(chunk).order(ByteOrder.LITTLE_ENDIAN);
            this.reused = reuseRow ? new PackedRowData(layout, chunk, le, segments) : null;
        }

        @Override
        public RecordAndPosition<RowData> next() {
            if (nextRow >= rowCount) {
                return null;
            }
            int rowOffset = nextRow * layout.rowWidth;
            nextRow++;
            PackedRowData row = reused;
            if (row == null) {
                row = new PackedRowData(layout, chunk, le, segments);
            }
            row.rowOffset = rowOffset;
            // offset of the NEXT record (a row boundary), 0 records to skip after it on restore
            position.set(row, chunkFileOffset + (long) nextRow * layout.rowWidth, 0);
            return position;
        }

        @Override
        public void releaseBatch() {}
    }

    // ── the flyweight row ───────────────────────────────────────────────────────

    /**
     * RowData view over one fixed-width row inside a chunk. Accessors are direct LE loads; the
     * accessor called is chosen by the planner from the DDL type, and each accepts every storage
     * kind that widens losslessly into it (zero/sign extension only — never a parse).
     */
    static final class PackedRowData implements RowData {
        private final Layout layout;
        private final byte[] chunk;
        private final ByteBuffer le;       // little-endian view over chunk
        private final MemorySegment[] segments; // same bytes, for zero-copy BinaryStringData
        int rowOffset;
        private RowKind rowKind = RowKind.INSERT;

        PackedRowData(Layout layout, byte[] chunk, ByteBuffer le, MemorySegment[] segments) {
            this.layout = layout;
            this.chunk = chunk;
            this.le = le;
            this.segments = segments;
        }

        private Kind kind(int pos) {
            return KINDS[layout.kinds[pos]];
        }

        private int fieldOffset(int pos) {
            return rowOffset + layout.offsets[pos];
        }

        private IllegalStateException badAccess(int pos, String accessor) {
            return new IllegalStateException(
                    "nes-native field " + pos + " has storage kind " + kind(pos)
                            + "; accessor " + accessor + " is not valid for it");
        }

        @Override
        public int getArity() {
            return layout.kinds.length;
        }

        @Override
        public RowKind getRowKind() {
            return rowKind;
        }

        @Override
        public void setRowKind(RowKind kind) {
            this.rowKind = kind;
        }

        @Override
        public boolean isNullAt(int pos) {
            return false; // packed layout has no null bitmap; catalog fields are not_null
        }

        @Override
        public boolean getBoolean(int pos) {
            if (kind(pos) != Kind.BOOL) {
                throw badAccess(pos, "getBoolean");
            }
            return chunk[fieldOffset(pos)] != 0;
        }

        @Override
        public byte getByte(int pos) {
            if (kind(pos) != Kind.I8) {
                throw badAccess(pos, "getByte");
            }
            return chunk[fieldOffset(pos)];
        }

        @Override
        public short getShort(int pos) {
            switch (kind(pos)) {
                case I16: return le.getShort(fieldOffset(pos));
                case I8: return chunk[fieldOffset(pos)];
                case U8: return (short) (chunk[fieldOffset(pos)] & 0xFF);
                default: throw badAccess(pos, "getShort");
            }
        }

        @Override
        public int getInt(int pos) {
            switch (kind(pos)) {
                case I32: return le.getInt(fieldOffset(pos));
                case U16: return le.getShort(fieldOffset(pos)) & 0xFFFF;
                case I16: return le.getShort(fieldOffset(pos));
                case U8: return chunk[fieldOffset(pos)] & 0xFF;
                case I8: return chunk[fieldOffset(pos)];
                default: throw badAccess(pos, "getInt");
            }
        }

        @Override
        public long getLong(int pos) {
            switch (kind(pos)) {
                case I64:
                case U64: // reinterpret; values >= 2^63 would wrap (documented)
                    return le.getLong(fieldOffset(pos));
                case U32: return le.getInt(fieldOffset(pos)) & 0xFFFFFFFFL;
                case I32: return le.getInt(fieldOffset(pos));
                case U16: return le.getShort(fieldOffset(pos)) & 0xFFFFL;
                case I16: return le.getShort(fieldOffset(pos));
                case U8: return chunk[fieldOffset(pos)] & 0xFFL;
                case I8: return chunk[fieldOffset(pos)];
                default: throw badAccess(pos, "getLong");
            }
        }

        @Override
        public float getFloat(int pos) {
            if (kind(pos) != Kind.F32) {
                throw badAccess(pos, "getFloat");
            }
            return le.getFloat(fieldOffset(pos));
        }

        @Override
        public double getDouble(int pos) {
            if (kind(pos) != Kind.F64) {
                throw badAccess(pos, "getDouble");
            }
            return le.getDouble(fieldOffset(pos));
        }

        @Override
        public StringData getString(int pos) {
            int off = fieldOffset(pos);
            switch (kind(pos)) {
                case STRING: {
                    // NUL-padded slot: content length = index of first 0 byte (always present)
                    int max = layout.widths[pos];
                    int len = 0;
                    while (len < max && chunk[off + len] != 0) {
                        len++;
                    }
                    return new BinaryStringData(segments, off, len);
                }
                case CHAR:
                    return new BinaryStringData(segments, off, 1);
                default:
                    throw badAccess(pos, "getString");
            }
        }

        @Override
        public byte[] getBinary(int pos) {
            throw new UnsupportedOperationException("nes-native: getBinary");
        }

        @Override
        public DecimalData getDecimal(int pos, int precision, int scale) {
            throw new UnsupportedOperationException("nes-native: getDecimal");
        }

        @Override
        public TimestampData getTimestamp(int pos, int precision) {
            throw new UnsupportedOperationException("nes-native: getTimestamp");
        }

        @Override
        public <T> RawValueData<T> getRawValue(int pos) {
            throw new UnsupportedOperationException("nes-native: getRawValue");
        }

        @Override
        public ArrayData getArray(int pos) {
            throw new UnsupportedOperationException("nes-native: getArray");
        }

        @Override
        public MapData getMap(int pos) {
            throw new UnsupportedOperationException("nes-native: getMap");
        }

        @Override
        public RowData getRow(int pos, int numFields) {
            throw new UnsupportedOperationException("nes-native: getRow");
        }

        @Override
        public Variant getVariant(int pos) {
            throw new UnsupportedOperationException("nes-native: getVariant");
        }
    }

    /** Fail fast when a DDL logical type cannot be served by the declared storage kind. */
    static void validateCompatibility(Layout layout, RowType rowType) {
        for (int i = 0; i < rowType.getFieldCount(); i++) {
            Kind kind = KINDS[layout.kinds[i]];
            LogicalType lt = rowType.getTypeAt(i);
            boolean ok;
            switch (lt.getTypeRoot()) {
                case BOOLEAN: ok = kind == Kind.BOOL; break;
                case TINYINT: ok = kind == Kind.I8; break;
                case SMALLINT: ok = kind == Kind.I16 || kind == Kind.I8 || kind == Kind.U8; break;
                case INTEGER:
                    ok = kind == Kind.I32 || kind == Kind.U16 || kind == Kind.I16
                            || kind == Kind.U8 || kind == Kind.I8;
                    break;
                case BIGINT:
                    ok = kind == Kind.I64 || kind == Kind.U64 || kind == Kind.U32
                            || kind == Kind.I32 || kind == Kind.U16 || kind == Kind.I16
                            || kind == Kind.U8 || kind == Kind.I8;
                    break;
                case FLOAT: ok = kind == Kind.F32; break;
                case DOUBLE: ok = kind == Kind.F64; break;
                case CHAR:
                case VARCHAR: ok = kind == Kind.STRING || kind == Kind.CHAR; break;
                default: ok = false;
            }
            if (!ok) {
                throw new IllegalArgumentException(
                        "nes-native: table field " + i + " (" + rowType.getFieldNames().get(i)
                                + " " + lt + ") cannot be served from storage kind " + kind);
            }
        }
    }
}
