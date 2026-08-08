package io.tapstate.app;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.assertj.core.api.Assertions.catchThrowable;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.hazelcast.core.HazelcastInstance;
import io.tapstate.core.event.ChainPosition;
import io.tapstate.core.event.SourceOrder;
import io.tapstate.runtime.engine.SinkAck;
import io.tapstate.runtime.srs.CaptureRunUnit;
import io.tapstate.spi.store.ConsumerOffset;
import io.tapstate.spi.store.SrsMetaStore;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import org.junit.jupiter.api.Test;

/**
 * The production sink-ack factory maps a sink's chain (the {@code src} stream name, a table at L1) to its
 * mining chain and the consumer pipeline, resolves the durable store from the member it runs on, and
 * advances that consumer's durable sink-acked position. It ships only serializable coordinates and binds
 * the store member-side, so nothing store-bound crosses the wire.
 */
class StoreBackedSinkAckFactoryTest {

    @Test
    void advancesTheDurableSinkAckedPositionForTheChainThatMapsToTheTable() {
        InMemorySrsMetaStore store = new InMemorySrsMetaStore();
        store.create("mc-orders", null);
        store.create("mc-items", null);
        HazelcastInstance member = memberWith(store);

        SinkAck ack = new StoreBackedSinkAckFactory(
                Map.of("orders", "mc-orders", "items", "mc-items"), "pipe-1").resolve(member);

        ack.advance("orders", at(7, "w7"));
        ack.advance("items", at(3, "w3"));

        assertThat(ackedPosition(store, "mc-orders", "pipe-1")).isEqualTo("w7");
        assertThat(ackedPosition(store, "mc-items", "pipe-1")).isEqualTo("w3");
    }

    @Test
    void persistsTheChainsCdcStartForAPositionThatCarriesNoTokenOfItsOwn() {
        InMemorySrsMetaStore store = new InMemorySrsMetaStore();
        store.create("mc-orders", null);
        store.setCdcStart("mc-orders", "w0", 1L);
        HazelcastInstance member = memberWith(store);

        SinkAck ack = new StoreBackedSinkAckFactory(Map.of("orders", "mc-orders"), "pipe-1").resolve(member);

        // A snapshot row is ordered but is not a spot in a change stream, so it has no token. The frontier
        // has confirmed rows of the snapshot and no change at all, which is exactly where cdc begins.
        ack.advance("orders", new ChainPosition(SourceOrder.snapshotRow(1), null));

        assertThat(ackedPosition(store, "mc-orders", "pipe-1")).isEqualTo("w0");
    }

    @Test
    void aTokenlessPositionOnAChainWithNoRecordIsAnInvariantViolation() {
        InMemorySrsMetaStore store = new InMemorySrsMetaStore();
        HazelcastInstance member = memberWith(store);

        SinkAck ack = new StoreBackedSinkAckFactory(Map.of("orders", "mc-orders"), "pipe-1").resolve(member);

        // The capture writes where cdc begins before it drains a snapshot, so a snapshot row reaching a sink
        // without one means the chain was never seeded. Writing an absent position over a real one would be
        // a frontier that silently went backwards.
        assertThatThrownBy(() -> ack.advance("orders", new ChainPosition(SourceOrder.snapshotRow(1), null)))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("mc-orders");
    }

    @Test
    void resolvesToANoOpWhenNoStoreIsBoundOnTheMember() {
        HazelcastInstance member = mock(HazelcastInstance.class);
        when(member.getUserContext()).thenReturn(new ConcurrentHashMap<>());

        SinkAck ack = new StoreBackedSinkAckFactory(Map.of("orders", "mc-orders"), "pipe-1").resolve(member);

        // A member the assembly layer has not made SRS-capable resolves to a no-op ack rather than failing,
        // mirroring the read-cursor publisher; a sink still runs before the store is bound.
        assertThat(catchThrowable(() -> ack.advance("orders", at(1, "w1")))).isNull();
    }

    @Test
    void aChainWithNoMappedTableIsAnInvariantViolation() {
        InMemorySrsMetaStore store = new InMemorySrsMetaStore();
        HazelcastInstance member = memberWith(store);

        SinkAck ack = new StoreBackedSinkAckFactory(Map.of("orders", "mc-orders"), "pipe-1").resolve(member);

        // The sink advances a chain the pipeline never sourced: a builder-side wiring defect, surfaced bare.
        assertThatThrownBy(() -> ack.advance("unknown_table", at(1, "w1")))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("unknown_table");
    }

    /** One change's position: the order the engine assigned it, and the token the connector gave. */
    private static ChainPosition at(long seq, String token) {
        return new ChainPosition(new SourceOrder(1, seq), token);
    }

    private static HazelcastInstance memberWith(SrsMetaStore store) {
        ConcurrentMap<String, Object> context = new ConcurrentHashMap<>();
        context.put(CaptureRunUnit.SRS_META_USER_CONTEXT_KEY, store);
        HazelcastInstance member = mock(HazelcastInstance.class);
        when(member.getUserContext()).thenReturn(context);
        return member;
    }

    private static String ackedPosition(SrsMetaStore store, String chainId, String pipelineId) {
        return store.read(chainId).orElseThrow().consumerOffsets().stream()
                .filter(offset -> offset.pipelineId().equals(pipelineId))
                .map(ConsumerOffset::sinkAckedSrcpos)
                .findFirst()
                .orElse(null);
    }
}
