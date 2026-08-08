package io.tapstate.app;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.hazelcast.core.HazelcastInstance;
import io.tapstate.core.event.ChainPosition;
import io.tapstate.core.event.SourceOrder;
import io.tapstate.runtime.engine.ReplayFloor;
import io.tapstate.runtime.engine.SinkAck;
import io.tapstate.runtime.srs.CaptureRunUnit;
import io.tapstate.spi.store.SrsMetaStore;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import org.junit.jupiter.api.Test;

/**
 * The read side of the durable frontier, for an operator that runs upstream of the sink that writes it.
 * What matters most here is that the two halves agree: the position a sink writes under a chain is the
 * position this reads back for it. A mismatch in any one coordinate - the table, the mining chain, the
 * consumer pipeline - does not fail, it answers "not known" forever, and everything downstream of that
 * quietly stops reclaiming anything at all.
 */
class StoreBackedReplayFloorFactoryTest {

    private static final Map<String, String> CHAINS = Map.of("orders", "mc-orders", "items", "mc-items");

    @Test
    void readsBackTheVeryPositionTheSinkSideWroteForThatChain() {
        InMemorySrsMetaStore store = new InMemorySrsMetaStore();
        store.create("mc-orders", null);
        store.create("mc-items", null);
        HazelcastInstance member = memberWith(store);

        SinkAck ack = new StoreBackedSinkAckFactory(CHAINS, "pipe-1").resolve(member);
        ack.advance("orders", at(7, "w7"));
        ack.advance("items", at(3, "w3"));

        ReplayFloor floor = new StoreBackedReplayFloorFactory(CHAINS, "pipe-1").resolve(member);

        assertThat(floor.of("orders")).contains(order(7));
        assertThat(floor.of("items")).contains(order(3));
    }

    @Test
    void doesNotTakeAnotherPipelinesConfirmedWritesForItsOwn() {
        InMemorySrsMetaStore store = new InMemorySrsMetaStore();
        store.create("mc-orders", null);
        HazelcastInstance member = memberWith(store);

        new StoreBackedSinkAckFactory(CHAINS, "pipe-2").resolve(member).advance("orders", at(9, "w9"));

        ReplayFloor floor = new StoreBackedReplayFloorFactory(CHAINS, "pipe-1").resolve(member);

        // Where another consumer has got to says nothing about where this one would resume; reading it as
        // this pipeline's own would let it forget records a replay of its own can still deliver.
        assertThat(floor.of("orders")).isEmpty();
    }

    @Test
    void knowsNothingOfAChainWithNoConfirmedWriteYet() {
        InMemorySrsMetaStore store = new InMemorySrsMetaStore();
        store.create("mc-orders", null);

        ReplayFloor floor =
                new StoreBackedReplayFloorFactory(CHAINS, "pipe-1").resolve(memberWith(store));

        // Nothing acked reads as "not known", never as "acked nothing" - a floor at the bottom would be a
        // licence to forget everything below it, which is every record there is.
        assertThat(floor.of("orders")).isEmpty();
    }

    @Test
    void knowsNothingOfAStreamThePipelineNeverSourced() {
        InMemorySrsMetaStore store = new InMemorySrsMetaStore();
        HazelcastInstance member = memberWith(store);

        ReplayFloor floor = new StoreBackedReplayFloorFactory(CHAINS, "pipe-1").resolve(member);

        // The write side treats this as a wiring defect and crashes, because a sink acking a chain the
        // pipeline never read means the graph is wrong. Reading is not the same: an operator asking about a
        // chain it holds nothing on is ordinary, and the answer it needs is simply "not known".
        assertThat(floor.of("unknown_table")).isEmpty();
    }

    @Test
    void knowsNothingWhileNoStoreIsBoundOnTheMember() {
        HazelcastInstance member = mock(HazelcastInstance.class);
        when(member.getUserContext()).thenReturn(new ConcurrentHashMap<>());

        ReplayFloor floor = new StoreBackedReplayFloorFactory(CHAINS, "pipe-1").resolve(member);

        assertThat(floor.of("orders")).isEmpty();
    }

    private static ChainPosition at(long seq, String token) {
        return new ChainPosition(order(seq), token);
    }

    private static SourceOrder order(long seq) {
        return new SourceOrder(1, seq);
    }

    private static HazelcastInstance memberWith(SrsMetaStore store) {
        ConcurrentMap<String, Object> context = new ConcurrentHashMap<>();
        context.put(CaptureRunUnit.SRS_META_USER_CONTEXT_KEY, store);
        HazelcastInstance member = mock(HazelcastInstance.class);
        when(member.getUserContext()).thenReturn(context);
        return member;
    }
}
