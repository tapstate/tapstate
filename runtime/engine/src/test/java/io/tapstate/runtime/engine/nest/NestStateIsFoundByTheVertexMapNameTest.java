package io.tapstate.runtime.engine.nest;

import static org.assertj.core.api.Assertions.assertThat;

import com.hazelcast.config.Config;
import com.hazelcast.config.JoinConfig;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import io.tapstate.core.event.SourceOrder;
import java.util.List;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * Pins that a vertex's state is found by the map name the topology computed for it, not by whichever store
 * object happened to hand it out.
 *
 * <p>The distinction is the whole point of moving the state off the heap. A vertex is handed a store when a
 * processor for it is created, and a processor is created again on every restart and again for every
 * parallel instance. If the store object is the thing that owns the entries, each of those gets an empty
 * one: a restart begins with nothing, which is exactly what a heap store does and exactly what the state
 * layer exists to stop. If instead the map name owns the entries, a second store over the same vertex reads
 * back what the first one wrote, and a restart resumes.
 *
 * <p>The name is computed at build time and has so far been inert - the topology asserts it was derived, and
 * nothing at run time ever consulted it. These cases are what make it load-bearing, so that a change to the
 * naming can no longer pass unnoticed.
 *
 * <p>The last case states the contrast rather than leaving it implied: the heap store deliberately fails
 * this, and must keep failing it. Were it ever "fixed" to share entries between store objects, the on-heap
 * binding would start to look restart-safe while still losing everything when the process dies.
 */
class NestStateIsFoundByTheVertexMapNameTest {

    private HazelcastInstance member;

    @BeforeEach
    void startMember() {
        Config config = new Config();
        config.setProperty("hazelcast.phone.home.enabled", "false");
        config.setProperty("hazelcast.shutdownhook.enabled", "false");
        JoinConfig join = config.getNetworkConfig().getJoin();
        join.getMulticastConfig().setEnabled(false);
        join.getTcpIpConfig().setEnabled(false);
        join.getAutoDetectionConfig().setEnabled(false);
        config.getNetworkConfig().getInterfaces().setEnabled(true).addInterface("127.0.0.1");
        member = Hazelcast.newHazelcastInstance(config);
    }

    @AfterEach
    void stopMember() {
        if (member != null) {
            member.shutdown();
        }
    }

    @Test
    void aSecondStoreOverTheSameVertexReadsBackWhatTheFirstOneWrote() {
        NestBinding.NestStores stores = NestBinding.onMap().bind(member);
        NestVertex vertex = resolver("nest.p1.n1.orders");

        stores.forResolver(vertex).save(7L, declaring("customer-1"));

        ResolverState reread = stores.forResolver(vertex).load(7L);
        assertThat(reread).isNotNull();
        assertThat(reread.parentKey()).isEqualTo("customer-1");
    }

    @Test
    void twoVerticesWithDifferentMapNamesDoNotAnswerEachOthersKeys() {
        NestBinding.NestStores stores = NestBinding.onMap().bind(member);
        NestVertex orders = resolver("nest.p1.n1.orders");
        NestVertex payouts = resolver("nest.p1.n1.payouts");

        stores.forResolver(orders).save(7L, declaring("customer-1"));

        assertThat(stores.forResolver(payouts).load(7L)).isNull();
        assertThat(stores.forResolver(orders).load(7L)).isNotNull();
    }

    @Test
    void anAssemblerAndAResolverAtTheSameNameShareNothingAcrossKinds() {
        NestBinding.NestStores stores = NestBinding.onMap().bind(member);
        NestVertex resolver = resolver("nest.p1.n1.orders");
        NestVertex assembler = assembler("nest.p1.n1.__root");

        stores.forResolver(resolver).save(7L, declaring("customer-1"));

        assertThat(stores.forAssembler(assembler).load(7L)).isNull();
    }

    @Test
    void removingAKeyThroughOneStoreIsSeenThroughAnother() {
        NestBinding.NestStores stores = NestBinding.onMap().bind(member);
        NestVertex vertex = resolver("nest.p1.n1.orders");
        stores.forResolver(vertex).save(7L, declaring("customer-1"));

        stores.forResolver(vertex).remove(7L);

        assertThat(stores.forResolver(vertex).load(7L)).isNull();
    }

    /**
     * The contrast, stated rather than implied: the heap store keeps its entries in the store object, so a
     * second store over the same vertex begins empty. That is what makes it wrong for anything that has to
     * outlive a processor, and it must stay wrong - a heap store that shared entries would read as
     * restart-safe without being it.
     */
    @Test
    void theHeapBindingDeliberatelyFailsThisAndMustKeepFailingIt() {
        NestBinding.NestStores stores = NestBinding.onHeap();
        NestVertex vertex = resolver("nest.p1.n1.orders");

        stores.forResolver(vertex).save(7L, declaring("customer-1"));

        assertThat(stores.forResolver(vertex).load(7L)).isNull();
    }

    private static ResolverState declaring(Object parentKey) {
        ResolverState state = new ResolverState();
        state.declare(parentKey, new SourceOrder(1L, 1L));
        return state;
    }

    private static NestVertex resolver(String mapName) {
        return new NestVertex(List.of("orders"), "nest:n1:orders", mapName,
                List.of("id"), List.of("customer_id"), List.of());
    }

    private static NestVertex assembler(String mapName) {
        return new NestVertex(List.of(), "nest:n1", mapName, List.of("id"), List.of(), List.of());
    }
}
