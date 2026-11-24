package io.tapstate.runtime.srs;

import com.hazelcast.nio.ObjectDataInput;
import io.tapstate.core.event.Op;
import io.tapstate.core.event.SourceOrder;
import java.io.EOFException;
import java.io.IOException;
import java.util.Map;
import org.junit.jupiter.api.Test;
import static org.assertj.core.api.Assertions.*;

class SrsDdlTransportTest {
    @Test
    void projectsPortableDdlWithoutPersistingTheLocalReceipt() {
        var schema = SrsItem.portableSchema(Map.of(
                io.tapstate.spi.store.SourceSchemaChange.ENVELOPE_KEY, new Object(),
                "tapstateDdl", Map.of("format", 1, "action", "drop")));
        var item = new SrsItem(null, Op.DDL, 1, null, null, 0, null, 2, schema);
        assertThat(SrsProjection.toEnvelope(item, "T", new SourceOrder(2, 1)).schema())
                .containsOnlyKeys("tapstateDdl");
    }

    @Test
    void readsLegacyRecordsEndingAtEpoch() throws Exception {
        var in = input(-1, -1);
        assertThat(new SrsItemSerializer().read(in).schema()).isNull();
    }

    @Test
    void refusesTruncatedAndUnknownSchemaExtensions() throws Exception {
        var in = input(-1, -1, 1);
        assertThatThrownBy(() -> new SrsItemSerializer().read(in)).isInstanceOf(EOFException.class);
        var unknown = input(-1, -1, 99);
        assertThatThrownBy(() -> new SrsItemSerializer().read(unknown)).isInstanceOf(IOException.class);
    }

    private static ObjectDataInput input(int... integers) {
        var ints = new java.util.ArrayDeque<Integer>();
        for (int value : integers) ints.add(value);
        int[] strings = {0};
        return (ObjectDataInput) java.lang.reflect.Proxy.newProxyInstance(
                ObjectDataInput.class.getClassLoader(), new Class<?>[]{ObjectDataInput.class},
                (proxy, method, args) -> switch (method.getName()) {
                    case "readString" -> strings[0]++ == 0 ? null : "ddl";
                    case "readLong" -> 0L;
                    case "readBoolean" -> false;
                    case "readInt" -> {
                        if (ints.isEmpty()) throw new EOFException();
                        yield ints.remove();
                    }
                    default -> throw new AssertionError("unexpected read: " + method.getName());
                });
    }
}
