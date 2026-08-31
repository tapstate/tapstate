package io.tapstate.runtime.srs;

import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.StreamSerializer;
import io.tapstate.core.event.Op;
import io.tapstate.spi.capture.SourcePosition;

import java.io.IOException;
import java.util.LinkedHashMap;
import java.util.Map;

/**
 * The Hazelcast serializer for a change-ring item. It writes the opaque wire forms only — the source
 * position as its token string, the op as its wire symbol — never the source-position or op object, so
 * nothing but stable strings and primitives crosses the ring boundary. The before/after row images are
 * written entry by entry (a leading count, {@code -1} for an absent image) so the serializer does not
 * lean on Hazelcast's support for any particular map implementation; the row values are the standard
 * scalar types Hazelcast serializes natively.
 */
public final class SrsItemSerializer implements StreamSerializer<SrsItem> {

    /** The type id for this serializer; must be unique across the platform's Hazelcast serialization config. */
    public static final int TYPE_ID = 10001;

    private static final int ABSENT = -1;

    @Override
    public int getTypeId() {
        return TYPE_ID;
    }

    @Override
    public void write(ObjectDataOutput out, SrsItem item) throws IOException {
        out.writeString(item.srcPos() == null ? null : item.srcPos().token());
        out.writeString(item.op().symbol());
        out.writeLong(item.ts());
        writeRow(out, item.before());
        writeRow(out, item.after());
        out.writeLong(item.schemaVer());
    }

    @Override
    public SrsItem read(ObjectDataInput in) throws IOException {
        String token = in.readString();
        SourcePosition srcPos = token == null ? null : new SourcePosition(token);
        Op op = Op.fromSymbol(in.readString());
        long ts = in.readLong();
        Map<String, Object> before = readRow(in);
        Map<String, Object> after = readRow(in);
        long schemaVer = in.readLong();
        return new SrsItem(srcPos, op, ts, before, after, schemaVer);
    }

    private static void writeRow(ObjectDataOutput out, Map<String, Object> row) throws IOException {
        if (row == null) {
            out.writeInt(ABSENT);
            return;
        }
        out.writeInt(row.size());
        for (Map.Entry<String, Object> entry : row.entrySet()) {
            out.writeString(entry.getKey());
            out.writeObject(entry.getValue());
        }
    }

    private static Map<String, Object> readRow(ObjectDataInput in) throws IOException {
        int size = in.readInt();
        if (size == ABSENT) {
            return null;
        }
        Map<String, Object> row = new LinkedHashMap<>();
        for (int i = 0; i < size; i++) {
            String key = in.readString();
            Object value = in.readObject();
            row.put(key, value);
        }
        return row;
    }
}
