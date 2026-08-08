package io.tapstate.runtime.engine.nest;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.hazelcast.config.Config;
import com.hazelcast.config.EvictionPolicy;
import com.hazelcast.config.JoinConfig;
import com.hazelcast.config.MaxSizePolicy;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import io.tapstate.core.common.TapstateException;
import io.tapstate.core.event.SourceOrder;
import io.tapstate.spi.store.KeyedStateStore;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

/**
 * What a deployment sets about how much a nest may hold is one number: how much of it stays in memory.
 * Everything past it is on the layer behind the map, and how much may be there is a question for whatever
 * that layer is rather than for this.
 *
 * <p>One number for every namespace rather than one each. What it bounds is the memory they all share, and
 * a deployment naming each level to bound the whole would be bounding it by however many it remembered to
 * name.
 *
 * <p><b>The budget is spent per partition, not per map.</b> A number below the partition count has been
 * rounded up to one entry each before it is enforced, so what is held is the partition count whatever was
 * asked for - a configured 10 measured 82 resident - and nothing in the configuration read back afterwards
 * says so. It is refused instead.
 */
class ANamespaceHoldsOnlyItsBudgetInMemoryTest {

    private static final String NAMESPACE = "nest.p1.n1.orders";

    /** The smallest budget that survives being spread across the partitions, so eviction is exact here. */
    private static final long BUDGET = 271L;

    private HazelcastInstance member;

    @AfterEach
    void stopMember() {
        if (member != null) {
            member.shutdown();
        }
    }

    @Test
    void aStateMapWithAStoreBehindItHoldsOnlyItsBudget() {
        var eviction = NestSettings.defaults().withEntriesHeldInMemory(50_000L)
                .backedStateMaps().getEvictionConfig();

        assertThat(eviction.getEvictionPolicy()).isEqualTo(EvictionPolicy.LRU);
        assertThat(eviction.getMaxSizePolicy()).isEqualTo(MaxSizePolicy.PER_NODE);
        assertThat(eviction.getSize()).isEqualTo(50_000);
    }

    /**
     * The one that decides whether this is a bound on memory or a way to lose state. With nothing behind
     * the map an evicted entry is in no other place, and what the map answers afterwards is its absence
     * rather than what was there - a resolver that stops answering for the children still pointing at it,
     * an assembly rebuilt from whatever arrives next and emitted as though it were whole. Neither says
     * anything while it happens, so a budget carried onto this shape would be silent loss wearing the name
     * of a capacity setting.
     */
    @Test
    void aStateMapWithNothingBehindItEvictsNothingHoweverSmallTheBudget() {
        var eviction = NestSettings.defaults().withEntriesHeldInMemory(BUDGET).stateMaps()
                .getEvictionConfig();

        assertThat(eviction.getEvictionPolicy())
                .describedAs("evicting with nothing to load the entry back from is losing it")
                .isEqualTo(EvictionPolicy.NONE);
    }

    @Test
    void aBudgetSmallerThanThePartitionsItIsSpentAcrossIsRefused() {
        assertThatThrownBy(() -> NestSettings.defaults().withEntriesHeldInMemory(BUDGET - 1))
                .isInstanceOf(TapstateException.class)
                .extracting(thrown -> ((TapstateException) thrown).code().code())
                .isEqualTo("nest.memory-budget-below-partition-count");
    }

    @Test
    void theRefusalNamesWhatWasAskedForAndWhatItIsSpentAcross() {
        assertThatThrownBy(() -> NestSettings.defaults().withEntriesHeldInMemory(10L))
                .isInstanceOf(TapstateException.class)
                .extracting(thrown -> ((TapstateException) thrown).args())
                .isEqualTo(Map.of("entries", 10L, "partitions", BUDGET));
    }

    @Test
    void aBudgetExactlyTheSizeOfThePartitionsItIsSpentAcrossIsTaken() {
        assertThat(NestSettings.defaults().withEntriesHeldInMemory(BUDGET).entriesHeldInMemory())
                .isEqualTo(BUDGET);
    }

    /**
     * The budget doing what it is for, on a running map: more is written than may stay, what stays is
     * bounded by the budget, and everything written still answers. The last of the three is what tells a
     * bound on memory apart from a loss - both hold less than was written, and only one of them can still
     * be asked.
     */
    @Test
    void whatIsWrittenPastTheBudgetLeavesMemoryAndStillAnswers() {
        HeapKeyedStateStore store = new HeapKeyedStateStore();
        member = startMember(store, BUDGET);
        NestStore<ResolverState> state = NestBinding.onMap().bind(member).forResolver(vertex());
        int written = 3_000;

        for (int i = 0; i < written; i++) {
            state.save(List.of("C" + i), declaring("parent-" + i));
        }

        assertThat(member.getMap(NAMESPACE).getLocalMapStats().getOwnedEntryCount())
                .describedAs("what stays in memory is held to the budget, not to what was written")
                .isLessThan(written);
        for (int i = 0; i < written; i++) {
            ResolverState reread = state.load(List.of("C" + i));
            assertThat(reread)
                    .describedAs("key %d left memory and the layer behind the map still has it", i)
                    .isNotNull();
            assertThat(reread.parentKey()).isEqualTo("parent-" + i);
        }
    }

    private static HazelcastInstance startMember(KeyedStateStore store, long budget) {
        Config config = new Config();
        config.setProperty("hazelcast.phone.home.enabled", "false");
        config.setProperty("hazelcast.shutdownhook.enabled", "false");
        JoinConfig join = config.getNetworkConfig().getJoin();
        join.getMulticastConfig().setEnabled(false);
        join.getTcpIpConfig().setEnabled(false);
        join.getAutoDetectionConfig().setEnabled(false);
        config.getNetworkConfig().getInterfaces().setEnabled(true).addInterface("127.0.0.1");
        config.addMapConfig(NestSettings.defaults().withEntriesHeldInMemory(budget).backedStateMaps());
        HazelcastInstance started = Hazelcast.newHazelcastInstance(config);
        // The configuration names the store; the member is what holds it. Bound after the member exists
        // and before any map on it is used, which is the window the resolution happens in.
        NestStateMapStoreFactory.bindTo(started, store);
        return started;
    }

    private static NestVertex vertex() {
        return new NestVertex(List.of("orders"), "nest:n1:orders", NAMESPACE,
                List.of("id"), List.of("customer_id"), List.of());
    }

    private static ResolverState declaring(Object parentKey) {
        ResolverState state = new ResolverState();
        state.declare(parentKey, new SourceOrder(1L, 1L));
        return state;
    }
}
