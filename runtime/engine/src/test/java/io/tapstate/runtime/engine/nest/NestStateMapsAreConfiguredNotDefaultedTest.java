package io.tapstate.runtime.engine.nest;

import static io.tapstate.runtime.engine.nest.NestTreeFixtures.embed;
import static io.tapstate.runtime.engine.nest.NestTreeFixtures.nest;
import static io.tapstate.runtime.engine.nest.NestTreeFixtures.tables;
import static org.assertj.core.api.Assertions.assertThat;

import com.hazelcast.config.Config;
import com.hazelcast.config.EvictionConfig;
import com.hazelcast.config.EvictionPolicy;
import com.hazelcast.config.InMemoryFormat;
import com.hazelcast.config.MapConfig;
import com.hazelcast.config.MaxSizePolicy;
import io.tapstate.core.model.EmbedAs;
import io.tapstate.core.model.TransformBody;
import java.util.List;
import org.junit.jupiter.api.Test;

/**
 * Pins what a nest state map is configured to be, against what it would be if nobody configured it.
 *
 * <p>The distinction matters because every unconfigured default here is plausible and wrong for state that
 * a vertex must be able to read back. A map nobody configures keeps a backup replica of every entry, which
 * costs a copy on every single write and buys nothing: this state is rebuildable from the source, so a
 * replica is redundancy against a failure the state layer already survives. Expiry and eviction are worse
 * than costly - they are silent. A resolver mapping that expires stops answering for children that arrive
 * later, and an assembly that is evicted comes back as an empty document that is then emitted as if whole.
 * Nothing throws, no counter moves, and the output is simply wrong, which is why these are pinned rather
 * than left to whatever the substrate happens to default to.
 *
 * <p>The eviction case earns its place twice over: the class default of a bare {@link EvictionConfig} is
 * least-recently-used at ten thousand entries, so an implementation that reached for one to set a size
 * would silently start dropping state at the ten-thousandth key - with nothing behind the map to load it
 * back from. Eviction becomes an option once something does stand behind it, and not before.
 *
 * <p>The first case is the one that couples the two halves: it takes the names a real tree compiles to and
 * asks the config for those exact names. A pattern that stopped matching what the topology computes would
 * leave every map on the substrate defaults while every other assertion here still passed.
 */
class NestStateMapsAreConfiguredNotDefaultedTest {

    private static final String PIPELINE = "orders_to_docs";
    private static final String NODE = "assemble";

    private static Config configured() {
        Config config = new Config();
        config.addMapConfig(NestMaps.stateMaps());
        return config;
    }

    private static NestTopology compiled() {
        TransformBody.Nest tree = nest("customer", List.of("customer_id"),
                embed("policy", "customer_id", "customer_id", EmbedAs.ARRAY, "policies", List.of("policy_no"),
                        embed("claim", "policy_id", "policy_id", EmbedAs.ARRAY, "claims", List.of("claim_id"))),
                embed("order", "customer_id", "customer_id", EmbedAs.ARRAY, "orders", List.of("order_id"),
                        embed("item", "order_id", "order_id", EmbedAs.ARRAY, "items", List.of("item_id"))));
        return NestTopology.compile(PIPELINE, NODE, tree, tables());
    }

    @Test
    void everyMapNameATreeCompilesToResolvesToTheConfiguredState() {
        Config config = configured();

        assertThat(compiled().vertices())
                .describedAs("there are names to check, so a pattern that matched none of them cannot "
                        + "pass this by having nothing to walk")
                .hasSize(3);
        for (NestVertex vertex : compiled().vertices()) {
            assertThat(config.getMapConfigOrNull(vertex.mapName()))
                    .describedAs("%s matches a configured map, not the substrate default", vertex.mapName())
                    .isNotNull()
                    .extracting(MapConfig::getBackupCount)
                    .isEqualTo(0);
        }
    }

    @Test
    void aStateMapKeepsNoBackupCopyOfWhatItHolds() {
        MapConfig state = NestMaps.stateMaps();

        assertThat(state.getBackupCount()).isZero();
        assertThat(state.getAsyncBackupCount()).isZero();
        assertThat(MapConfig.DEFAULT_BACKUP_COUNT)
                .describedAs("an unconfigured map would keep one, so this is a decision and not the default")
                .isEqualTo(1);
    }

    @Test
    void nothingInAStateMapEverExpires() {
        MapConfig state = NestMaps.stateMaps();

        assertThat(state.getTimeToLiveSeconds()).isZero();
        assertThat(state.getMaxIdleSeconds()).isZero();
    }

    @Test
    void aStateMapEvictsNothingWhileNothingStandsBehindIt() {
        EvictionConfig eviction = NestMaps.stateMaps().getEvictionConfig();

        assertThat(eviction.getEvictionPolicy()).isEqualTo(EvictionPolicy.NONE);
        assertThat(eviction.getMaxSizePolicy()).isEqualTo(MaxSizePolicy.PER_NODE);
        assertThat(eviction.getSize()).isEqualTo(Integer.MAX_VALUE);
        assertThat(new EvictionConfig().getEvictionPolicy())
                .describedAs("a bare eviction config would drop entries, which is what this one must not do")
                .isEqualTo(EvictionPolicy.LRU);
    }

    /**
     * The format follows the access rather than the intuition, and the access carries the state to its
     * key instead of putting it across the map. Measured on both: carried onto a map holding objects, a
     * write costs no serialization of the document at all, where putting it costs one every time.
     *
     * <p>It only holds while no index is defined on these maps - an index turns what a carried write is
     * handed into a clone of the state rather than the state - which is why that is banned rather than
     * left to be noticed.
     */
    @Test
    void aStateMapKeepsItsValuesAsTheObjectsTheyAre() {
        assertThat(NestMaps.stateMaps().getInMemoryFormat()).isEqualTo(InMemoryFormat.OBJECT);
    }

    @Test
    void whatACapacityAlarmWouldReadStaysOn() {
        assertThat(NestMaps.stateMaps().isStatisticsEnabled()).isTrue();
    }

    /**
     * The mechanism a single namespace is set apart through, pinned where it is relied on rather than
     * where it is first used. A name that is configured exactly wins over the pattern that also matches it,
     * and the namespaces beside it are untouched - which is what lets one namespace take a capacity limit
     * later without every other one taking it too.
     */
    @Test
    void aNamespaceCanBeSetApartWithoutDraggingItsSiblingsAlong() {
        Config config = configured();
        NestTopology topology = compiled();
        String apart = topology.vertexAt(List.of("orders")).mapName();
        String sibling = topology.vertexAt(List.of("policies")).mapName();
        config.addMapConfig(new MapConfig(apart).setBackupCount(2));

        assertThat(config.getMapConfigOrNull(apart).getBackupCount()).isEqualTo(2);
        assertThat(config.getMapConfigOrNull(sibling).getBackupCount()).isZero();
    }
}
