package io.tapstate.adapters.pdk;

import io.tapstate.core.event.ConvertedValue;
import io.tapstate.core.event.Envelope;
import io.tapdata.entity.codec.TapCodecsRegistry;
import io.tapdata.entity.event.ddl.table.TapNewFieldEvent;
import io.tapdata.entity.event.dml.TapDeleteRecordEvent;
import io.tapdata.entity.event.dml.TapInsertRecordEvent;
import io.tapdata.entity.event.dml.TapUpdateRecordEvent;
import io.tapdata.entity.schema.value.TapStringValue;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.io.Serializable;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Locks the codec's field-by-field projection to a checked-in golden. One canonical PDK event per op
 * is decoded and rendered; a diff here is a real change to the envelope contract every transform
 * downstream binds to, so it must be a deliberate, reviewed change: regenerate with
 * {@code -Dtapstate.codec.golden.update=true}, then review the diff. Map keys are rendered sorted so
 * the lock is on which fields project where, not on a connector's incidental field order.
 */
class TapEventCodecGoldenTest {

    /** No connector registered a conversion here: this pins the projection, not the value lanes. */
    private static final TapCodecsRegistry CODECS = new TapCodecsRegistry();

    /** A driver's own type, standing in for the ones a real client hands back. */
    private record DriverKey(String hex) implements Serializable {
    }

    /**
     * The other lane, so the golden holds a row that met a connector conversion as well as rows that
     * met none. Without a sample decoded through a registry that has something in it, every value here
     * takes the bare lane and no change to the carried form could ever show up as a diff.
     */
    private static final TapCodecsRegistry CONNECTOR_CODECS = new TapCodecsRegistry()
            .registerToTapValue(DriverKey.class, (value, tapType) ->
                    new TapStringValue(((DriverKey) value).hex()));

    private static final Path GOLDEN =
            Path.of("src", "test", "resources", "golden", "tapevent-codec.golden.json");
    private static final boolean UPDATE = Boolean.getBoolean("tapstate.codec.golden.update");

    /** One decoded envelope per op, in a fixed order, from canonical PDK events. */
    private static List<Envelope> samples() {
        TapInsertRecordEvent insert = TapInsertRecordEvent.create()
                .table("orders").referenceTime(1000L).after(ordered("id", 1, "region", "eu"));
        TapUpdateRecordEvent update = TapUpdateRecordEvent.create()
                .table("orders").referenceTime(1000L)
                .before(ordered("id", 1, "region", "eu")).after(ordered("id", 1, "region", "us"));
        TapDeleteRecordEvent delete = TapDeleteRecordEvent.create()
                .table("orders").referenceTime(1000L).before(ordered("id", 1));
        TapInsertRecordEvent row = TapInsertRecordEvent.create()
                .table("orders").referenceTime(1000L).after(ordered("id", 7));
        TapNewFieldEvent ddl = new TapNewFieldEvent();
        ddl.setTableId("orders");
        ddl.setReferenceTime(1000L);
        ddl.setOriginDDL("ALTER TABLE orders ADD note VARCHAR(64)");
        TapInsertRecordEvent carried = TapInsertRecordEvent.create()
                .table("orders").referenceTime(1000L)
                .after(ordered("_id", new DriverKey("64f0c0de"), "region", "eu"));

        return List.of(
                TapEventCodec.decodeChange(insert, CODECS, Map.of()),
                TapEventCodec.decodeChange(update, CODECS, Map.of()),
                TapEventCodec.decodeChange(delete, CODECS, Map.of()),
                TapEventCodec.decodeSnapshotRow(row, CODECS, Map.of()),
                TapEventCodec.decodeChange(ddl, CODECS, Map.of()),
                // Decoded with a schema that names the column, so the golden holds the whole carrier -
                // both what travels for readers and what the write side rebuilds the driver type from.
                // Left unnamed, the second half would be locked as absent and no change to it could
                // ever show up as a diff.
                TapEventCodec.decodeChange(carried, CONNECTOR_CODECS, Map.of("_id", "OBJECT_ID")));
    }

    @Test
    void projectionMatchesTheCheckedInGolden() throws IOException {
        String rendered = render(samples());
        if (UPDATE) {
            Files.createDirectories(GOLDEN.getParent());
            Files.writeString(GOLDEN, rendered);
            return;
        }
        assertThat(Files.exists(GOLDEN))
                .as("codec golden missing — regenerate with -Dtapstate.codec.golden.update=true")
                .isTrue();
        assertThat(Files.readString(GOLDEN)).isEqualTo(rendered);
    }

    @Test
    void goldenUpdateToggleIsOffDuringNormalRuns() {
        // With the toggle set, the assertion path is skipped and the golden is rewritten — a real
        // projection regression would be silently rebaselined. This guard makes any toggled run RED.
        assertThat(UPDATE)
                .as("tapstate.codec.golden.update must not be set during a normal run — it rewrites the golden")
                .isFalse();
    }

    // ---- fixtures + a small deterministic JSON renderer (test-owned, stable) ----

    private static Map<String, Object> ordered(Object... kv) {
        Map<String, Object> m = new LinkedHashMap<>();
        for (int i = 0; i < kv.length; i += 2) {
            m.put((String) kv[i], kv[i + 1]);
        }
        return m;
    }

    private static String render(List<Envelope> envelopes) {
        StringBuilder sb = new StringBuilder("[\n");
        for (int i = 0; i < envelopes.size(); i++) {
            Envelope e = envelopes.get(i);
            sb.append("  {\n");
            sb.append("    \"op\": ").append(quote(e.op().symbol())).append(",\n");
            sb.append("    \"ts\": ").append(e.ts()).append(",\n");
            sb.append("    \"src\": ").append(quote(e.src())).append(",\n");
            sb.append("    \"before\": ").append(map(e.before())).append(",\n");
            sb.append("    \"after\": ").append(map(e.after())).append(",\n");
            sb.append("    \"schema\": ").append(map(e.schema())).append("\n");
            sb.append(i + 1 < envelopes.size() ? "  },\n" : "  }\n");
        }
        return sb.append("]\n").toString();
    }

    private static String map(Map<String, Object> m) {
        if (m == null) {
            return "null";
        }
        if (m.isEmpty()) {
            return "{}";
        }
        StringBuilder sb = new StringBuilder("{");
        boolean first = true;
        for (Map.Entry<String, Object> entry : new TreeMap<>(m).entrySet()) {
            if (!first) {
                sb.append(", ");
            }
            first = false;
            sb.append(quote(entry.getKey())).append(": ").append(value(entry.getValue()));
        }
        return sb.append("}").toString();
    }

    private static String value(Object v) {
        if (v == null) {
            return "null";
        }
        if (v instanceof ConvertedValue carrier) {
            // Both halves are contract: the portable value every reader downstream binds to, and the
            // name the source's schema gave the column, which is what the write side rebuilds the
            // driver's own type from. A column the schema did not describe carries no name, and that
            // absence is locked here too - it is what decides whether a target can restore the type.
            return "{\"value\": " + value(carrier.value())
                    + ", \"originType\": " + value(carrier.originType()) + "}";
        }
        if (v instanceof Number || v instanceof Boolean) {
            return v.toString();
        }
        return quote(v.toString());
    }

    private static String quote(String s) {
        return "\"" + s.replace("\\", "\\\\").replace("\"", "\\\"") + "\"";
    }
}
