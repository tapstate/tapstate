package io.tapstate.app;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.assertj.core.api.Assertions.catchThrowable;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.hazelcast.core.HazelcastInstance;
import io.tapstate.core.event.ChainPosition;
import io.tapstate.core.event.SourceOrder;
import io.tapstate.runtime.engine.SinkAck;
import io.tapstate.runtime.srs.CaptureRunUnit;
import io.tapstate.spi.store.ConsumerOffset;
import io.tapstate.spi.store.SrsMetaStore;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import org.junit.jupiter.api.Test;
import org.mockito.AdditionalAnswers;

/**
 * The production sink-ack factory maps a sink's chain (the {@code src} stream name, a table at L1) to its
 * mining chain and the consumer pipeline, resolves the durable store from the member it runs on, and records
 * what that consumer's sink confirmed, table by table. It ships only serializable coordinates and binds the
 * store member-side, so nothing store-bound crosses the wire.
 */
class StoreBackedSinkAckFactoryTest {

    @Test
    void recordsEachTablesConfirmationUnderTheChainThatMapsToIt() {
        InMemorySrsMetaStore store = new InMemorySrsMetaStore();
        store.create("mc-orders", null);
        store.create("mc-items", null);
        HazelcastInstance member = memberWith(store);

        SinkAck ack = new StoreBackedSinkAckFactory(
                Map.of("orders", "mc-orders", "items", "mc-items"), "pipe-1").resolve(member);

        ack.advance("orders", at(7, "w7"));
        ack.advance("items", at(3, "w3"));

        assertThat(tableAck(store, "mc-orders", "pipe-1", "orders")).isEqualTo(at(7, "w7"));
        assertThat(tableAck(store, "mc-items", "pipe-1", "items")).isEqualTo(at(3, "w3"));
    }

    @Test
    void marksTheTableSnapshotCompleteWhenTheFrontierConfirmsItsSnapshotRows() {
        InMemorySrsMetaStore store = new InMemorySrsMetaStore();
        store.create("mc-orders", null);
        store.setCdcStart("mc-orders", "pipe-1", "w0", 1L);
        HazelcastInstance member = memberWith(store);

        SinkAck ack = new StoreBackedSinkAckFactory(
                Map.of("orders", "mc-orders", "items", "mc-orders"), "pipe-1").resolve(member);

        ack.advance("orders", new ChainPosition(SourceOrder.snapshotRow(1), null));

        // This is the only moment anyone learns a table's rows are in the target. The read side knows when
        // it finished reading, which is a different question: a table read and never written looks done to
        // it, and the next run skips it. What the tail then replays is only what changed after the snapshot
        // began -- a row that never changed again is simply absent from the target, for good.
        assertThat(store.read("mc-orders").orElseThrow().snapshotCompletedTables("pipe-1"))
                .containsExactly("orders");
        // The mark is this pipeline's, and only this pipeline's. A chain is keyed by the source connection
        // and excludes the table subset, so another pipeline reading the same database shares this record
        // -- and would otherwise be told its own initial load was done and skip it, leaving its target
        // short of every row of the table with the run healthy and nothing logged.
        assertThat(store.read("mc-orders").orElseThrow().snapshotCompletedTables("another-pipeline"))
                .isEmpty();
    }

    @Test
    void theSnapshotMarkSurvivesTheAdvancesThatFollowIt() {
        InMemorySrsMetaStore store = new InMemorySrsMetaStore();
        store.create("mc-orders", null);
        store.setCdcStart("mc-orders", "pipe-1", "w0", 1L);
        HazelcastInstance member = memberWith(store);

        SinkAck ack = new StoreBackedSinkAckFactory(Map.of("orders", "mc-orders"), "pipe-1").resolve(member);

        ack.advance("orders", new ChainPosition(SourceOrder.snapshotRow(1), null));
        ack.advance("orders", at(7, "w7"));
        store.advanceConsumerReadSeq("mc-orders", "pipe-1", "orders", 5L);

        // The mark, the table's confirmation and the read cursor are three facets of one consumer's record,
        // and the real store advances each with an update scoped to its own field. A store that rebuilds the
        // consumer from whichever facet is being written erases the other two -- and erases this one in the
        // direction nothing notices: the load reads as unfinished, so the next run reads the whole table
        // again and reaches the right target by the wrong route, with nothing thrown and nothing logged.
        assertThat(store.read("mc-orders").orElseThrow().snapshotCompletedTables("pipe-1"))
                .as("a change acked above the snapshot does not un-record the load")
                .containsExactly("orders");
        assertThat(tableAck(store, "mc-orders", "pipe-1", "orders")).isEqualTo(at(7, "w7"));
    }

    @Test
    void aChangeAckSaysNothingAboutWhetherASnapshotFinished() {
        InMemorySrsMetaStore store = new InMemorySrsMetaStore();
        store.create("mc-orders", null);
        HazelcastInstance member = memberWith(store);

        SinkAck ack = new StoreBackedSinkAckFactory(Map.of("orders", "mc-orders"), "pipe-1").resolve(member);

        ack.advance("orders", at(7, "w7"));

        // A change acked at a spot in the stream proves the sink wrote that change, not that it wrote a
        // snapshot. A cdc-only read has no snapshot at all, and marking one done here would let a later run
        // skip a full load that never ran.
        assertThat(store.read("mc-orders").orElseThrow().snapshotCompletedTables("pipe-1")).isEmpty();
    }

    @Test
    void aChangeAckRecordsWhereInItsOwnTablesRingTheChangeSat() {
        InMemorySrsMetaStore store = new InMemorySrsMetaStore();
        store.create("mc-shop", null);
        HazelcastInstance member = memberWith(store);
        SinkAck ack = new StoreBackedSinkAckFactory(
                Map.of("orders", "mc-shop", "items", "mc-shop"), "pipe-1").resolve(member);

        ack.advance("orders", at(7, "w7"));
        ack.advance("items", at(3, "w3"));

        // One chain, two tables, two rings. The chain's acked position is one pair for both and cannot say
        // where in either ring a run replacing this one carries on, so each table's own sequence is kept
        // apart and neither ring is positioned by the other's.
        assertThat(store.ringDoneThrough("mc-shop", "pipe-1"))
                .containsExactlyInAnyOrderEntriesOf(Map.of("orders", 7L, "items", 3L));
    }

    @Test
    void aQuietTableAckCannotAdvanceTheSharedSourcePastAnotherTablesPendingChange() {
        InMemorySrsMetaStore store = new InMemorySrsMetaStore();
        store.create("mc-shop", null);
        store.advanceSourceReadOffset("mc-shop", at(-1, "t0"));
        store.upsertConsumerOffset("mc-shop", new ConsumerOffset("pipe-1", Map.of(), null, List.of(), null, 0L));
        SinkAck ack = new StoreBackedSinkAckFactory(
                Map.of("orders", "mc-shop", "customers", "mc-shop"), "pipe-1")
                .resolve(memberWith(store));

        // One source log feeds both tables. The orders change at t1 is read but has not landed; the later
        // customers change at t2 lands first. Its acknowledgement says nothing about t1, so a restart has to
        // be able to read t1 from the source again -- and it can only do that from a position before it.
        ack.advance("customers", at(0, "t2"));

        assertThat(store.read("mc-shop").orElseThrow().sourceReadOffset())
                .as("a physical source read offset cannot cross an unacknowledged table")
                .isEqualTo("t0");
    }

    @Test
    void aSharedChainKeepsEachTablesAcknowledgementApartFromTheChainsOwn() {
        InMemorySrsMetaStore store = new InMemorySrsMetaStore();
        store.create("mc-shop", null);
        SinkAck ack = new StoreBackedSinkAckFactory(
                Map.of("orders", "mc-shop", "customers", "mc-shop"), "pipe-1")
                .resolve(memberWith(store));

        ack.advance("customers", at(0, "t2"));
        ack.advance("orders", at(5, "t5"));

        ConsumerOffset consumer = store.read("mc-shop").orElseThrow().consumerOffset("pipe-1").orElseThrow();
        // What each table landed is the input the chain's prefix is released from; the chain's own
        // acknowledgement waits for that release rather than taking whichever table spoke last.
        assertThat(consumer.sinkAckedByTable()).containsExactlyInAnyOrderEntriesOf(Map.of(
                "customers", at(0, "t2"), "orders", at(5, "t5")));
        assertThat(consumer.sinkAcked()).isNull();
    }

    @Test
    void aSnapshotRowSaysNothingAboutHowFarARingOrTheChainWasReached() {
        InMemorySrsMetaStore store = new InMemorySrsMetaStore();
        store.create("mc-orders", null);
        store.setCdcStart("mc-orders", "pipe-1", "w0", 1L);
        HazelcastInstance member = memberWith(store);
        SinkAck ack = new StoreBackedSinkAckFactory(Map.of("orders", "mc-orders"), "pipe-1").resolve(member);

        ack.advance("orders", new ChainPosition(SourceOrder.snapshotRow(1), null));

        assertThat(store.ringDoneThrough("mc-orders", "pipe-1"))
                .as("a snapshot row is ordered beneath every change and sits in no ring at all")
                .isEmpty();
        assertThat(ackedChainPosition(store, "mc-orders", "pipe-1"))
                .as("and it is no position of the source's log either: it moves nothing but the mark")
                .isNull();
    }

    @Test
    void aConfirmationForAChainThatWasNeverSeededIsAnInvariantViolation() {
        InMemorySrsMetaStore store = new InMemorySrsMetaStore();
        HazelcastInstance member = memberWith(store);

        SinkAck ack = new StoreBackedSinkAckFactory(Map.of("orders", "mc-orders"), "pipe-1").resolve(member);

        // The capture seeds the chain before anything of it can reach a sink, so a confirmation for a chain
        // with no record means the wiring ran out of order. It is surfaced bare rather than written anywhere.
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

    @Test
    void aChangeThatNamesNoPositionIsAckedByItsOrderAloneRatherThanRefused() {
        InMemorySrsMetaStore store = new InMemorySrsMetaStore();
        // A chain with no cdc start, which is every chain a cdc_only read ever has: only the snapshot
        // phase writes where changes begin, and that mode does not run one.
        store.create("mc-orders", null);
        HazelcastInstance member = memberWith(store);

        SinkAck ack = new StoreBackedSinkAckFactory(Map.of("orders", "mc-orders"), "pipe-1").resolve(member);

        // A source names a position for a run of changes when it has one and names none when it has not,
        // and that absence is load-bearing: the contract has a recipient carry on rather than invent one,
        // because an invented position claims changes were read that were not. Reaching for the chain's
        // cdc start here instead crashed the whole job, with nothing ever delivered to the target.
        ack.advance("orders", at(7, null));

        ChainPosition acked = tableAck(store, "mc-orders", "pipe-1", "orders");
        // Both halves, because each fails on its own: an ack quietly dropped would leave the release with
        // no input and still not throw, and a token conjured from somewhere would resume a later run past
        // changes it never delivered.
        assertThat(acked.order()).isEqualTo(new SourceOrder(1, 7));
        assertThat(acked.token()).isNull();
    }

    /**
     * A confirmation never moves the chain by itself -- not the source read offset, and not this pipeline's
     * chain-level position. Whoever reads the chain knows in what order the source handed its changes over
     * and moves both once everything up to a point has landed for everyone; a sink knows only its own table.
     */
    @Test
    void aConfirmationNeverMovesTheChainByItself() {
        InMemorySrsMetaStore store = new InMemorySrsMetaStore();
        store.create("mc-orders", null);
        HazelcastInstance member = memberWith(store);

        SinkAck ack = new StoreBackedSinkAckFactory(Map.of("orders", "mc-orders"), "pipe-1").resolve(member);

        ack.advance("orders", at(7, "w7"));

        assertThat(store.read("mc-orders").orElseThrow().sourceReadOffset()).isNull();
        assertThat(ackedChainPosition(store, "mc-orders", "pipe-1")).isNull();
        assertThat(tableAck(store, "mc-orders", "pipe-1", "orders")).isEqualTo(at(7, "w7"));
    }

    /**
     * The acknowledgement path is one scoped write per confirmation, and reads nothing.
     *
     * <p>It runs for every batch a sink lands, on every pipeline. The record it writes to also holds a schema
     * history that grows for the life of the chain, one entry per DDL, so reaching for the whole record here
     * would make every acknowledged batch pay for that history; and deciding here how far the chain may go
     * would need every consumer's position on every batch, which is the chain reader's work, done once for
     * all of them.
     *
     * <p>Counted rather than timed: a machine's speed moves a duration and leaves a call count alone.
     */
    @Test
    void theAckPathWritesOneConfirmationAndReadsNothing() {
        InMemorySrsMetaStore backing = new InMemorySrsMetaStore();
        backing.create("mc-orders", null);
        SrsMetaStore store = mock(SrsMetaStore.class, AdditionalAnswers.delegatesTo(backing));
        HazelcastInstance member = memberWith(store);

        SinkAck ack = new StoreBackedSinkAckFactory(Map.of("orders", "mc-orders"), "pipe-1").resolve(member);
        for (int seq = 3; seq <= 7; seq++) {
            ack.advance("orders", at(seq, "w" + seq));
        }

        verify(store, times(5)).advanceTableSinkAcked(eq("mc-orders"), eq("pipe-1"), eq("orders"), any());
        verify(store, never()).read(anyString());
        verify(store, never()).consumerOffsets(anyString());
        verify(store, never()).advanceSourceReadOffset(anyString(), any());
        verify(store, never()).advanceSinkAcked(anyString(), anyString(), any(ChainPosition.class));
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

    private static ChainPosition ackedChainPosition(SrsMetaStore store, String chainId, String pipelineId) {
        return store.read(chainId).orElseThrow().consumerOffset(pipelineId)
                .map(ConsumerOffset::sinkAcked)
                .orElse(null);
    }

    private static ChainPosition tableAck(SrsMetaStore store, String chainId, String pipelineId, String table) {
        return store.read(chainId).orElseThrow().consumerOffset(pipelineId).orElseThrow()
                .sinkAckedByTable().get(table);
    }
}
