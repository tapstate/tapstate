package io.tapstate.runtime.engine;

import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.DataSerializable;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/** The results of one pure transform invocation, released one item at a time on its original edge. */
public final class PreparedTransformRows implements DataSerializable {
    private List<Object> rows;

    public PreparedTransformRows() {}

    PreparedTransformRows(List<Object> rows) {
        this.rows = List.copyOf(rows);
    }

    List<Object> rows() {
        return rows;
    }

    @Override
    public void writeData(ObjectDataOutput out) throws IOException {
        out.writeInt(rows.size());
        for (Object row : rows) out.writeObject(row);
    }

    @Override
    public void readData(ObjectDataInput in) throws IOException {
        int size = in.readInt();
        var read = new ArrayList<Object>(size);
        for (int i = 0; i < size; i++) read.add(in.readObject());
        rows = List.copyOf(read);
    }
}
