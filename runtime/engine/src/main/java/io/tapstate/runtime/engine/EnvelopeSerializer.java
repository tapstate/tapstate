package io.tapstate.runtime.engine;

import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.StreamSerializer;
import io.tapstate.core.event.ChainPosition;
import io.tapstate.core.event.Envelope;
import io.tapstate.core.event.Op;
import io.tapstate.core.event.SourceOrder;
import java.io.IOException;
import java.util.LinkedHashMap;
import java.util.Map;

/**
 * How a change crosses a member boundary.
 *
 * <p><b>Nothing else can carry it.</b> A change is three row images and a map of chain positions, and a
 * row image is a map of names to whatever the source had there - so the zero-configuration mechanism
 * refuses it outright, for a field of type {@code Object} it has no way to plan for. Until this existed,
 * a graph spread over more than one member failed on its first event, at the edge carrying the root
 * stream, with a serialization error naming a type nobody had thought about. None of that is visible on
 * one member, where every partition is local and nothing is ever written for transport.
 *
 * <p><b>What travels is the wire form, never the object.</b> The op goes as its symbol and a position's
 * order as two numbers, so what crosses is strings and primitives whose meaning does not move when the
 * classes behind them do. Row images are written entry by entry with a leading count - {@code -1} for an
 * absent one - rather than as a map, so nothing here depends on which map implementation the row arrived
 * in; the values inside are the ordinary scalars the platform already carries.
 *
 * <p><b>This is a transport form and not a stored one.</b> No structure holds a change: they exist on the
 * edges between vertices and nowhere else, so what is written here reaches nothing that was written
 * before it. That is why it could be introduced without a migration, and why introducing one later would
 * be a different kind of change than it looks.
 */
public final class EnvelopeSerializer implements StreamSerializer<Envelope> {

    /** The type id for this serializer; must be unique across the platform's Hazelcast serialization config. */
    public static final int TYPE_ID = 10002;

    private static final int ABSENT = -1;

    @Override
    public int getTypeId() {
        return TYPE_ID;
    }

    @Override
    public void write(ObjectDataOutput out, Envelope envelope) throws IOException {
        out.writeString(envelope.op().symbol());
        out.writeLong(envelope.ts());
        out.writeString(envelope.src());
        writeRow(out, envelope.before());
        writeRow(out, envelope.after());
        writeRow(out, envelope.schema());
        writePositions(out, envelope.positions());
    }

    @Override
    public Envelope read(ObjectDataInput in) throws IOException {
        Op op = Op.fromSymbol(in.readString());
        long ts = in.readLong();
        String src = in.readString();
        Map<String, Object> before = readRow(in);
        Map<String, Object> after = readRow(in);
        Map<String, Object> schema = readRow(in);
        Map<String, ChainPosition> positions = readPositions(in);
        return new Envelope(op, ts, src, before, after, schema, positions);
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
            row.put(key, in.readObject());
        }
        return row;
    }

    /**
     * Where the change sits on each chain it belongs to, written as its own numbers and token rather than
     * as an object. What a restart resumes from must not depend on the shape of a class.
     */
    private static void writePositions(ObjectDataOutput out, Map<String, ChainPosition> positions)
            throws IOException {
        if (positions == null) {
            out.writeInt(ABSENT);
            return;
        }
        out.writeInt(positions.size());
        for (Map.Entry<String, ChainPosition> entry : positions.entrySet()) {
            out.writeString(entry.getKey());
            ChainPosition position = entry.getValue();
            out.writeLong(position.order().epoch());
            out.writeLong(position.order().seq());
            // A token is absent far more often than not, and the platform's string writer takes null.
            out.writeString(position.token());
        }
    }

    private static Map<String, ChainPosition> readPositions(ObjectDataInput in) throws IOException {
        int size = in.readInt();
        if (size == ABSENT) {
            return null;
        }
        Map<String, ChainPosition> positions = new LinkedHashMap<>();
        for (int i = 0; i < size; i++) {
            String chain = in.readString();
            long epoch = in.readLong();
            long seq = in.readLong();
            positions.put(chain, new ChainPosition(new SourceOrder(epoch, seq), in.readString()));
        }
        return positions;
    }
}
