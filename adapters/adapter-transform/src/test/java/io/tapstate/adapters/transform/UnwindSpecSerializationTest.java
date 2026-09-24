package io.tapstate.adapters.transform;

import static org.assertj.core.api.Assertions.assertThat;

import io.tapstate.core.event.Envelope;
import io.tapstate.core.event.Op;
import io.tapstate.core.model.TransformBody;
import io.tapstate.spi.transform.TransformPort;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

/**
 * The expansion's spec is what the app captures in the Jet supplier, so it has to survive
 * serialization and still rebuild a working port on the far side.
 *
 * <p>Worth its own case beyond the projection's because of what this spec carries: the key of the
 * rows arriving at the node, which is a list built somewhere else and handed in. A field that does
 * not cross is not a compile error and not a local test failure - it is an empty key at runtime,
 * and an empty key does not throw. It quietly makes every expanded row of a parent identical, which
 * is the exact failure the whole write-key rule exists to prevent, arriving by the one route that
 * rule cannot see.
 */
class UnwindSpecSerializationTest {

    @Test
    @DisplayName("an unwind spec survives java serialization and still keys its rows on the far side")
    void unwindSpecRoundTripsAndStillPairsByKey() throws IOException, ClassNotFoundException {
        UnwindSpec spec = UnwindSpec.from(
                new TransformBody.Unwind("items", "item_no", true, "sku", "json"), List.of("o_id"));

        TransformPort port = StatelessTransforms.unwind(roundTrip(spec));

        // Pairing across an update is what reads every field of the spec at once: the path to find
        // the list, the element key to tell the rows apart, the parent key to keep two orders'
        // elements distinct, and the ordinal column to carry along.
        List<Envelope> out = port.transform(new Envelope(Op.UPDATE, 1L, "orders",
                row(List.of(element("a"), element("b"))), row(List.of(element("a"))), null));

        assertThat(out).hasSize(2);
        assertThat(out).filteredOn(e -> e.op() == Op.DELETE).singleElement()
                .satisfies(e -> assertThat(e.before()).containsEntry("items", element("b")));
        assertThat(out).filteredOn(e -> e.op() == Op.UPDATE).singleElement()
                .satisfies(e -> assertThat(e.after()).containsEntry("item_no", 0L));
    }

    private static Map<String, Object> element(String sku) {
        Map<String, Object> e = new LinkedHashMap<>();
        e.put("sku", sku);
        return e;
    }

    private static Map<String, Object> row(List<Map<String, Object>> items) {
        Map<String, Object> r = new LinkedHashMap<>();
        r.put("o_id", 7L);
        r.put("items", items);
        r.put("region", "eu");
        return r;
    }

    private static UnwindSpec roundTrip(UnwindSpec spec) throws IOException, ClassNotFoundException {
        ByteArrayOutputStream bytes = new ByteArrayOutputStream();
        try (ObjectOutputStream out = new ObjectOutputStream(bytes)) {
            out.writeObject(spec);
        }
        try (ObjectInputStream in = new ObjectInputStream(new ByteArrayInputStream(bytes.toByteArray()))) {
            return (UnwindSpec) in.readObject();
        }
    }
}
