package io.tapstate.adapters.pdk;

import static org.assertj.core.api.Assertions.assertThat;

import io.tapstate.spi.store.KeyedStateStore;
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
