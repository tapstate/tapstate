package io.tapstate.adapters.pdk;

import static org.assertj.core.api.Assertions.assertThat;

import io.tapstate.spi.store.KeyedStateStore;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;

/**
 * The state map a connector is handed when it is opened for a named pipeline node. The property under
 * test is the one a per-open map cannot have: what one open wrote, the <em>next</em> open of the same
 * node reads back. A connector mints an identity on its first run and expects to find it again; when it
 * does not, it mints another and the position it was given no longer matches the identity it is running
 * under.
 */
class AConnectorsNotesOutliveTheConnectorThatWroteThemTest {

    private final KeyedStateStore store = new HeapKeyedState();

    @Test
    void aLaterOpenOfTheSameNodeReadsWhatAnEarlierOneWrote() {
        map("pdk.state.p1.src_a").put("SERVER_NAME", "ff38f23a");

        // A second map over the same namespace stands for the next open: a different object entirely,
        // which is exactly what the connector gets.
        assertThat(map("pdk.state.p1.src_a").get("SERVER_NAME")).isEqualTo("ff38f23a");
    }

    @Test
    void anotherNodeSeesNothingOfIt() {
        map("pdk.state.p1.src_a").put("SERVER_NAME", "ff38f23a");

        assertThat(map("pdk.state.p1.src_b").get("SERVER_NAME")).isNull();
        assertThat(map("pdk.state.p2.src_a").get("SERVER_NAME")).isNull();
    }

    @Test
    void whatComesBackIsTheTypeThatWentIn() {
        byte[] history = {0x1f, (byte) 0x8b, 0x08};
        map("pdk.state.p1.src_a").put("MYSQL_SCHEMA_HISTORY", history);

        Object back = map("pdk.state.p1.src_a").get("MYSQL_SCHEMA_HISTORY");
        assertThat(back).isInstanceOf(byte[].class);
        assertThat((byte[]) back).containsExactly(history);
    }

    @Test
    @SuppressWarnings("unchecked")
    void aListCanBeEditedAndExplicitlyWrittenBack() {
        DurableStateMap notes = map("pdk.state.p1.src_a");
        List<String> original = new ArrayList<>(List.of("orders"));
        notes.put("tap_topic", original);
        original.add("not-stored");
        List<String> edited = (List<String>) notes.get("tap_topic");
        assertThat(edited).containsExactly("orders");

        edited.add("invoices");
        // Reads are detached snapshots: neither an alias kept by put nor an edited read writes itself.
        assertThat(notes.get("tap_topic")).isEqualTo(List.of("orders"));
        notes.put("tap_topic", edited);

        assertThat(map("pdk.state.p1.src_a").get("tap_topic"))
                .isEqualTo(List.of("orders", "invoices"));
    }

    @Test
    @SuppressWarnings("unchecked")
    void aMapCanBeEditedAndExplicitlyWrittenBack() {
        DurableStateMap notes = map("pdk.state.p1.src_a");
        Map<String, Object> original = new LinkedHashMap<>(Map.of("slot", "one"));
        notes.put("checkpoint", original);
        original.put("not-stored", true);
        Map<String, Object> edited = (Map<String, Object>) notes.get("checkpoint");
        assertThat(edited).containsOnlyKeys("slot");

        edited.put("cp", 7L);
        assertThat(notes.get("checkpoint")).isEqualTo(Map.of("slot", "one"));
        notes.put("checkpoint", edited);

        assertThat(map("pdk.state.p1.src_a").get("checkpoint"))
                .isEqualTo(Map.of("slot", "one", "cp", 7L));
    }

    @Test
    @SuppressWarnings("unchecked")
    void aLosingClaimReturnsTheSameEditableSnapshotShapeAsGet() {
        DurableStateMap notes = map("pdk.state.p1.src_a");
        notes.put("checkpoint", Map.of("topics", List.of("orders")));

        Map<String, Object> existing = (Map<String, Object>) notes.putIfAbsent(
                "checkpoint", Map.of("topics", List.of("loser")));
        ((List<String>) existing.get("topics")).add("invoices");
        existing.put("cp", 7L);
        assertThat(notes.get("checkpoint")).isEqualTo(Map.of("topics", List.of("orders")));
        notes.put("checkpoint", existing);

        assertThat(map("pdk.state.p1.src_a").get("checkpoint"))
                .isEqualTo(Map.of("topics", List.of("orders", "invoices"), "cp", 7L));
    }

    @Test
    void storingNothingUnderAKeyIsHowAKeyIsExpired() {
        // How a connector expires its own checkpoints: it puts null rather than calling remove.
        map("pdk.state.p1.src_a").put("cp", 12L);
        map("pdk.state.p1.src_a").put("cp", null);

        assertThat(map("pdk.state.p1.src_a").get("cp")).isNull();
    }

    @Test
    void theFirstToClaimAKeyKeepsItAndTheNextIsToldWhatIsThere() {
        assertThat(map("pdk.state.p1.src_a").putIfAbsent("firstConnectorId", "one")).isNull();

        // The value that stands, not the candidate: a connector that keeps what it was handed back keeps
        // the identity that actually won.
        assertThat(map("pdk.state.p1.src_a").putIfAbsent("firstConnectorId", "two")).isEqualTo("one");
        assertThat(map("pdk.state.p1.src_a").get("firstConnectorId")).isEqualTo("one");
    }

    @Test
    void removeGivesBackWhatItTookAway() {
        map("pdk.state.p1.src_a").put("k", "v");

        assertThat(map("pdk.state.p1.src_a").remove("k")).isEqualTo("v");
        assertThat(map("pdk.state.p1.src_a").get("k")).isNull();
    }

    @Test
    void clearingTakesThisNodesNotesAndNobodyElses() {
        map("pdk.state.p1.src_a").put("k", "v");
        map("pdk.state.p1.src_b").put("k", "other");

        map("pdk.state.p1.src_a").clear();

        assertThat(map("pdk.state.p1.src_a").get("k")).isNull();
        assertThat(map("pdk.state.p1.src_b").get("k")).isEqualTo("other");
    }

    @Test
    void resettingDoesTheSameAsClearing() {
        map("pdk.state.p1.src_a").put("k", "v");

        map("pdk.state.p1.src_a").reset();

        assertThat(map("pdk.state.p1.src_a").get("k")).isNull();
    }

    @Test
    void aKeyNobodyWroteReadsAsNothingRatherThanThrowing() {
        assertThat(map("pdk.state.p1.src_a").get("never-written")).isNull();
    }

    private DurableStateMap map(String namespace) {
        return new DurableStateMap(store, namespace);
    }
}
