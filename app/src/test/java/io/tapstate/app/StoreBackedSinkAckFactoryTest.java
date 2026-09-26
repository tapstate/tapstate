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
import io.tapstate.core.lifecycle.AwaitedLoad;
import io.tapstate.runtime.engine.SinkAck;
import io.tapstate.runtime.srs.CaptureRunUnit;
import io.tapstate.spi.store.ConsumerOffset;
import io.tapstate.spi.store.SrsMetaStore;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import org.junit.jupiter.api.Test;
import org.mockito.AdditionalAnswers;

/**
 * The production sink-ack factory maps a sink's chain (the {@code src} stream name, a table at L1) to its
 * mining chain and the consumer pipeline, resolves the durable store from the member it runs on, and
 * advances that consumer's durable sink-acked position. It ships only serializable coordinates and binds
 * the store member-side, so nothing store-bound crosses the wire.
 *
 * <p>Every sink processor reports as a writer of the run, and the pipeline's record moves only as far as the
 * slowest writer the run expects for a table. Most cases here have one writer, which is a pipeline with one
 * sink: its record then says exactly what that writer landed. The cases with two are what the writers are
 * kept apart for.
 */
class StoreBackedSinkAckFactoryTest {

    private static final String WRITER = "serve.s#0";
    private static final String OTHER_WRITER = "serve.t#0";

    @Test
    void advancesTheDurableSinkAckedPositionForTheChainThatMapsToTheTable() {
        InMemorySrsMetaStore store = new InMemorySrsMetaStore();
        store.create("mc-orders", null);
        store.create("mc-items", null);
        HazelcastInstance member = memberWith(store);

        SinkAck ack = soleWriter(member, Map.of("orders", "mc-orders", "items", "mc-items"), "pipe-1");

        ack.advance("orders", at(7, "w7"));
        ack.advance("items", at(3, "w3"));

        assertThat(ackedPosition(store, "mc-orders", "pipe-1")).isEqualTo("w7");
        assertThat(ackedPosition(store, "mc-items", "pipe-1")).isEqualTo("w3");
    }

    @Test
    void persistsTheChainsCdcStartForAPositionThatCarriesNoTokenOfItsOwn() {
        InMemorySrsMetaStore store = new InMemorySrsMetaStore();
        store.create("mc-orders", null);
        store.setCdcStart("mc-orders", "pipe-1", "w0", 1L);
        HazelcastInstance member = memberWith(store);

        SinkAck ack = soleWriter(member, Map.of("orders", "mc-orders"), "pipe-1");

        // A snapshot row is ordered but is not a spot in a change stream, so it has no token. The frontier
        // has confirmed rows of the snapshot and no change at all, which is exactly where cdc begins.
        ack.advance("orders", new ChainPosition(SourceOrder.snapshotRow(1), null));

        assertThat(ackedPosition(store, "mc-orders", "pipe-1")).isEqualTo("w0");
    }

    @Test
    void marksTheTableSnapshotCompleteWhenTheFrontierConfirmsItsSnapshotRows() {
        InMemorySrsMetaStore store = new InMemorySrsMetaStore();
        store.create("mc-orders", null);
        store.setCdcStart("mc-orders", "pipe-1", "w0", 1L);
        HazelcastInstance member = memberWith(store);

        SinkAck ack = soleWriter(member, Map.of("orders", "mc-orders", "items", "mc-orders"), "pipe-1");

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

        SinkAck ack = soleWriter(member, Map.of("orders", "mc-orders"), "pipe-1");

        ack.advance("orders", new ChainPosition(SourceOrder.snapshotRow(1), null));
        ack.advance("orders", at(7, "w7"));
        store.advanceConsumerReadSeq("mc-orders", "pipe-1", "orders", 5L);

        // The mark, the acked position and the read cursor are three facets of one consumer's record, and
        // the real store advances each with an update scoped to its own field. A store that rebuilds the
        // consumer from whichever facet is being written erases the other two -- and erases this one in the
        // direction nothing notices: the load reads as unfinished, so the next run reads the whole table
        // again and reaches the right target by the wrong route, with nothing thrown and nothing logged.
        assertThat(store.read("mc-orders").orElseThrow().snapshotCompletedTables("pipe-1"))
                .as("a change acked above the snapshot does not un-record the load")
                .containsExactly("orders");
        assertThat(ackedPosition(store, "mc-orders", "pipe-1")).isEqualTo("w7");
    }

    @Test
    void aChangeAckSaysNothingAboutWhetherASnapshotFinished() {
        InMemorySrsMetaStore store = new InMemorySrsMetaStore();
        store.create("mc-orders", null);
        HazelcastInstance member = memberWith(store);

        SinkAck ack = soleWriter(member, Map.of("orders", "mc-orders"), "pipe-1");

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
        SinkAck ack = soleWriter(member, Map.of("orders", "mc-shop", "items", "mc-shop"), "pipe-1");

        ack.advance("orders", at(7, "w7"));
        ack.advance("items", at(3, "w3"));

        // One chain, two tables, two rings. The chain's acked position is one pair for both and cannot say
        // where in either ring a run replacing this one carries on, so each table's own sequence is kept
        // apart and neither ring is positioned by the other's.
        assertThat(store.ringDoneThrough("mc-shop", "pipe-1"))
                .containsExactlyInAnyOrderEntriesOf(Map.of("orders", 7L, "items", 3L));
    }

    @Test
    void aSnapshotRowSaysNothingAboutHowFarARingWasReached() {
        InMemorySrsMetaStore store = new InMemorySrsMetaStore();
        store.create("mc-orders", null);
        store.setCdcStart("mc-orders", "pipe-1", "w0", 1L);
        HazelcastInstance member = memberWith(store);
        SinkAck ack = soleWriter(member, Map.of("orders", "mc-orders"), "pipe-1");

        ack.advance("orders", new ChainPosition(SourceOrder.snapshotRow(1), null));

        assertThat(store.ringDoneThrough("mc-orders", "pipe-1"))
                .as("a snapshot row is ordered beneath every change and sits in no ring at all")
                .isEmpty();
        assertThat(ackedPosition(store, "mc-orders", "pipe-1"))
                .as("while the chain's own acked position still moves, as it always has")
                .isEqualTo("w0");
    }

    @Test
    void aSnapshotRowOfAPipelineThatRecordedNoCdcStartIsAnInvariantViolation() {
        InMemorySrsMetaStore store = new InMemorySrsMetaStore();
        store.create("mc-orders", null);
        HazelcastInstance member = memberWith(store);

        SinkAck ack = soleWriter(member, Map.of("orders", "mc-orders"), "pipe-1");

        // The capture writes where cdc begins before it drains a snapshot, so a snapshot row reaching a sink
        // without one means this pipeline was never seeded. Writing an absent position over a real one would
        // be a frontier that silently went backwards.
        assertThatThrownBy(() -> ack.advance("orders", new ChainPosition(SourceOrder.snapshotRow(1), null)))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("mc-orders");
    }

    @Test
    void aSnapshotAckDoesNotBorrowAnotherPipelinesCdcStart() {
        InMemorySrsMetaStore store = new InMemorySrsMetaStore();
        store.create("mc-orders", null);
        store.setCdcStart("mc-orders", "pipe-1", "w0", 1L);
        HazelcastInstance member = memberWith(store);

        SinkAck ack = soleWriter(member, Map.of("orders", "mc-orders"), "pipe-2");

        assertThatThrownBy(() -> ack.advance("orders", new ChainPosition(SourceOrder.snapshotRow(1), null)))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("pipe-2");
    }

    @Test
    void resolvesToANoOpWhenNoStoreIsBoundOnTheMember() {
        HazelcastInstance member = mock(HazelcastInstance.class);
        when(member.getUserContext()).thenReturn(new ConcurrentHashMap<>());
        StoreBackedSinkAckFactory factory =
                new StoreBackedSinkAckFactory(Map.of("orders", "mc-orders"), "pipe-1", "run-1");

        // A member the assembly layer has not made SRS-capable resolves to a no-op ack rather than failing,
        // mirroring the read-cursor publisher; a sink still runs before the store is bound. Starting the run
        // there starts nothing, for the same reason.
        factory.beginRun(member, Map.of("orders", List.of(WRITER)));
        SinkAck ack = factory.resolve(member).forWriter(WRITER);

        assertThat(catchThrowable(() -> ack.advance("orders", at(1, "w1")))).isNull();
        assertThat(catchThrowable(() -> ack.bounded("orders", new SourceOrder(1, 2)))).isNull();
    }

    @Test
    void aChainWithNoMappedTableIsAnInvariantViolation() {
        InMemorySrsMetaStore store = new InMemorySrsMetaStore();
        store.create("mc-orders", null);
        HazelcastInstance member = memberWith(store);

        SinkAck ack = soleWriter(member, Map.of("orders", "mc-orders"), "pipe-1");

        // The sink advances a chain the pipeline never sourced: a builder-side wiring defect, surfaced bare.
        assertThatThrownBy(() -> ack.advance("unknown_table", at(1, "w1")))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("unknown_table");
    }

    @Test
    void aChangeThatNamesNoPositionLandsByItsOrderWithoutInventingAToken() {
        InMemorySrsMetaStore store = new InMemorySrsMetaStore();
        // A chain with no cdc start, which is every chain a cdc_only read ever has: only the snapshot
        // phase writes where changes begin, and that mode does not run one.
        store.create("mc-orders", null);
        HazelcastInstance member = memberWith(store);

        SinkAck ack = soleWriter(member, Map.of("orders", "mc-orders"), "pipe-1");

        // A source names a position for a run of changes when it has one and names none when it has not,
        // and that absence is load-bearing: the contract has a recipient carry on rather than invent one,
        // because an invented position claims changes were read that were not. Reaching for the chain's
        // cdc start here instead crashed the whole job, with nothing ever delivered to the target.
        ack.advance("orders", at(7, null));

        // Both halves, because each fails on its own: an ack quietly dropped would leave a replacing run
        // starting the ring over from where it last knew, and a token conjured from somewhere would resume
        // a later run past changes it never delivered.
        assertThat(store.ringDoneThrough("mc-orders", "pipe-1"))
                .as("the change landed, so a replacing run carries on in the ring past it")
                .containsEntry("orders", 7L);
        assertThat(ackedChainPosition(store, "mc-orders", "pipe-1"))
                .as("and nothing a read could resume from was recorded, because the change named nothing")
                .isNull();
    }

    @Test
    void anAcknowledgedChangeIsWhereTheChainSaysItsSourceHasBeenRead() {
        InMemorySrsMetaStore store = new InMemorySrsMetaStore();
        store.create("mc-orders", null);
        HazelcastInstance member = memberWith(store);

        SinkAck ack = soleWriter(member, Map.of("orders", "mc-orders"), "pipe-1");

        ack.advance("orders", at(7, "w7"));

        // How far a chain may say its source has been read is the lowest of what was read and what every
        // consumer has landed, and it used to be worked out only while a run of changes was being
        // forwarded -- against the acknowledgements that existed at that instant, which on a first forward
        // is none. This acknowledgement arrives afterwards and nothing carried it back, so the record kept
        // whatever an earlier forward had resolved: one delivery behind while changes kept coming, and
        // nothing at all once the source went quiet. A cdc-only read has no snapshot start to fall back on
        // either, so a run restarted from that state re-attached at the present moment and everything
        // written while it was down was gone, with nothing thrown and nothing logged.
        assertThat(store.read("mc-orders").orElseThrow().sourceReadOffset()).isEqualTo("w7");
    }

    @Test
    void theRecordedReadDoesNotPassAConsumerThatHasLandedLess() {
        InMemorySrsMetaStore store = new InMemorySrsMetaStore();
        store.create("mc-orders", null);
        HazelcastInstance member = memberWith(store);

        // A second pipeline on the same chain, three changes behind the first.
        soleWriter(member, Map.of("orders", "mc-orders"), "pipe-2").advance("orders", at(4, "w4"));
        soleWriter(member, Map.of("orders", "mc-orders"), "pipe-1").advance("orders", at(7, "w7"));

        // The faster one's acknowledgement must not carry the chain past what the slower one holds: a
        // change that sink has not written is one this chain still has to be able to hand out again, and
        // an offset that stepped over it would mean nothing ever fetches it.
        assertThat(store.read("mc-orders").orElseThrow().sourceReadOffset()).isEqualTo("w4");
    }

    @Test
    void aConsumerThatHasLandedNothingLeavesTheReadUnrecorded() {
        InMemorySrsMetaStore store = new InMemorySrsMetaStore();
        store.create("mc-orders", null);
        // A consumer on the chain that has acked nothing at all: the state of every pipeline before its
        // first write lands, and of one whose sink is failing.
        store.upsertConsumerOffset("mc-orders", new ConsumerOffset("pipe-2", Map.of(), null));
        HazelcastInstance member = memberWith(store);

        soleWriter(member, Map.of("orders", "mc-orders"), "pipe-1").advance("orders", at(7, "w7"));

        // Nothing is known about how far that consumer has got, so nothing may be written -- a chain that
        // read past it would drop changes it was never handed.
        assertThat(store.read("mc-orders").orElseThrow().sourceReadOffset()).isNull();
    }

    /**
     * The acknowledgement path asks for the consumers on their own; it never reads the whole record.
     *
     * <p>Working out how far the source may be said to have been read needs every consumer's position,
     * and the record holding them also holds a schema history that grows for the life of the chain, one
     * entry per DDL and unbounded. Reaching for the whole record here would make every acknowledged batch
     * pay for that history, so the cost would grow with the chain rather than with the work.
     *
     * <p>Counted rather than timed, for the reason the read side is: a machine's speed moves a duration
     * and leaves a call count alone. The read side's own count lives a layer down, against the change
     * stream; that layer cannot see this path at all, so this is the same figure for the side it misses.
     */
    @Test
    void theAckPathAsksForTheConsumersOnTheirOwnRatherThanReadingTheWholeRecord() {
        InMemorySrsMetaStore backing = new InMemorySrsMetaStore();
        backing.create("mc-orders", null);
        SrsMetaStore store = mock(SrsMetaStore.class, AdditionalAnswers.delegatesTo(backing));
        HazelcastInstance member = memberWith(store);

        SinkAck ack = soleWriter(member, Map.of("orders", "mc-orders"), "pipe-1");
        for (int seq = 3; seq <= 7; seq++) {
            ack.advance("orders", at(seq, "w" + seq));
        }

        verify(store, times(5)).consumerOffsets("mc-orders");
        verify(store, never()).read(anyString());
    }

    /**
     * A read offset that resolves to what was recorded last time is not written again.
     *
     * <p>What may be recorded is the lowest of every consumer's position, so while one consumer sits
     * still the answer is the same on every acknowledgement the others make. Writing it again tells the
     * record what it already holds -- a round trip per acknowledged batch, on the path every pipeline
     * uses, bought for nothing. The forwarding side skips that write for the same reason.
     *
     * <p>The slow consumer is what makes this discriminate: with one consumer alone every acknowledgement
     * raises the answer, so writing every time and writing only on a change look identical.
     */
    @Test
    void aResolvedReadOffsetThatHasNotMovedIsNotWrittenAgain() {
        InMemorySrsMetaStore backing = new InMemorySrsMetaStore();
        backing.create("mc-orders", null);
        // A second consumer that has landed w2 and stays there, so it pins the lowest for all five below.
        backing.upsertConsumerOffset("mc-orders", new ConsumerOffset("pipe-2", Map.of(), at(2, "w2")));
        SrsMetaStore store = mock(SrsMetaStore.class, AdditionalAnswers.delegatesTo(backing));
        HazelcastInstance member = memberWith(store);

        SinkAck ack = soleWriter(member, Map.of("orders", "mc-orders"), "pipe-1");
        for (int seq = 3; seq <= 7; seq++) {
            ack.advance("orders", at(seq, "w" + seq));
        }

        verify(store, times(1)).advanceSourceReadOffset(eq("mc-orders"), any());
        // Both halves: a count alone would be satisfied by writing the wrong position once, and the
        // position alone would be satisfied by writing the right one five times.
        assertThat(backing.read("mc-orders").orElseThrow().sourceReadOffset())
                .as("the one written is the position the slow consumer holds, not the fast one's")
                .isEqualTo("w2");
    }

    /**
     * Two writers land one table, and the pipeline has landed it as far as the slower one has - not as far as
     * whichever reported last, and not as far as the faster one. A resume from the faster one's position
     * would skip every change the slower one still holds, and nothing would ever write them again.
     */
    @Test
    void aTableHasLandedOnlyAsFarAsItsSlowestWriter() {
        InMemorySrsMetaStore store = new InMemorySrsMetaStore();
        store.create("mc-orders", null);
        HazelcastInstance member = memberWith(store);
        StoreBackedSinkAckFactory factory = startedRun(member, Map.of("orders", "mc-orders"), "pipe-1",
                Map.of("orders", List.of(WRITER, OTHER_WRITER)));
        SinkAck fast = factory.resolve(member).forWriter(WRITER);
        SinkAck slow = factory.resolve(member).forWriter(OTHER_WRITER);

        slow.advance("orders", at(50, "w50"));
        fast.advance("orders", at(100, "w100"));

        assertThat(ackedPosition(store, "mc-orders", "pipe-1"))
                .as("where the pipeline resumes: the slower writer's position, reported first or not")
                .isEqualTo("w50");
        assertThat(store.ringDoneThrough("mc-orders", "pipe-1")).containsEntry("orders", 50L);

        slow.advance("orders", at(100, "w100"));

        assertThat(ackedPosition(store, "mc-orders", "pipe-1"))
                .as("and once the slower one has caught up, both of theirs")
                .isEqualTo("w100");
        assertThat(store.ringDoneThrough("mc-orders", "pipe-1")).containsEntry("orders", 100L);
    }

    /**
     * A writer handed none of a table's rows holds the table back until a bound says none are coming, and no
     * longer. Until it has said anything it may be holding every change there is; once a bound covers them,
     * it is holding none, and the other writer's progress stands.
     */
    @Test
    void aWriterGivenNoneOfATablesRowsHoldsItBackOnlyUntilABoundSaysNoneAreComing() {
        InMemorySrsMetaStore store = new InMemorySrsMetaStore();
        store.create("mc-orders", null);
        HazelcastInstance member = memberWith(store);
        StoreBackedSinkAckFactory factory = startedRun(member, Map.of("orders", "mc-orders"), "pipe-1",
                Map.of("orders", List.of(WRITER, OTHER_WRITER)));
        SinkAck busy = factory.resolve(member).forWriter(WRITER);
        SinkAck idle = factory.resolve(member).forWriter(OTHER_WRITER);

        busy.advance("orders", at(7, "w7"));

        assertThat(ackedPosition(store, "mc-orders", "pipe-1"))
                .as("a writer that has said nothing may be holding every change there is")
                .isNull();

        idle.bounded("orders", new SourceOrder(1, 9));

        assertThat(ackedPosition(store, "mc-orders", "pipe-1")).isEqualTo("w7");
        assertThat(store.ringDoneThrough("mc-orders", "pipe-1"))
                .as("and the ring no further than the busy writer got: the bound runs past it, the rows do not")
                .containsEntry("orders", 7L);
    }

    /**
     * The table's load is recorded as landed only once every writer the run expects has passed it: each
     * writer is given its own share of the load's rows, and the one that has not reported may still hold its
     * share.
     */
    @Test
    void theLoadIsRecordedOnlyOnceEveryWriterHasPassedIt() {
        InMemorySrsMetaStore store = new InMemorySrsMetaStore();
        store.create("mc-orders", null);
        store.setCdcStart("mc-orders", "pipe-1", "w0", 1L);
        HazelcastInstance member = memberWith(store);
        StoreBackedSinkAckFactory factory = startedRun(member, Map.of("orders", "mc-orders"), "pipe-1",
                Map.of("orders", List.of(WRITER, OTHER_WRITER)));

        factory.resolve(member).forWriter(WRITER).advance("orders",
                new ChainPosition(SourceOrder.snapshotRow(1), null));

        assertThat(store.read("mc-orders").orElseThrow().snapshotCompletedTables("pipe-1")).isEmpty();

        factory.resolve(member).forWriter(OTHER_WRITER).advance("orders",
                new ChainPosition(SourceOrder.snapshotRow(1), null));

        assertThat(store.read("mc-orders").orElseThrow().snapshotCompletedTables("pipe-1"))
                .containsExactly("orders");
    }

    /**
     * A writer of a run that a later one has replaced lands nothing, however far it got: it belongs to a run
     * being stopped, and what it says could stand for a writer of the current run that has landed less.
     */
    @Test
    void aWriterOfAReplacedRunLandsNothing() {
        InMemorySrsMetaStore store = new InMemorySrsMetaStore();
        store.create("mc-orders", null);
        HazelcastInstance member = memberWith(store);
        Map<String, String> chains = Map.of("orders", "mc-orders");
        SinkAck stale = startedRun(member, chains, "pipe-1", Map.of("orders", List.of(WRITER)), "run-1")
                .resolve(member).forWriter(WRITER);
        StoreBackedSinkAckFactory current =
                startedRun(member, chains, "pipe-1", Map.of("orders", List.of(WRITER)), "run-2");

        stale.advance("orders", at(9, "w9"));

        assertThat(ackedPosition(store, "mc-orders", "pipe-1")).isNull();
        assertThat(store.writerRun("mc-orders", "pipe-1").orElseThrow().progressFor("orders")).isEmpty();

        current.resolve(member).forWriter(WRITER).advance("orders", at(5, "w5"));

        assertThat(ackedPosition(store, "mc-orders", "pipe-1")).isEqualTo("w5");
    }

    /**
     * A writer reporting into a run nothing started crashes bare: an execution starts its run before any of
     * its writers exists, so this is a writer wired to the wrong run, and nothing would ever wait for what it
     * landed.
     */
    @Test
    void aWriterReportingIntoARunNothingStartedIsAnInvariantViolation() {
        InMemorySrsMetaStore store = new InMemorySrsMetaStore();
        store.create("mc-orders", null);
        HazelcastInstance member = memberWith(store);
        SinkAck ack = new StoreBackedSinkAckFactory(Map.of("orders", "mc-orders"), "pipe-1", "run-1")
                .resolve(member).forWriter(WRITER);

        assertThatThrownBy(() -> ack.advance("orders", at(7, "w7")))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("run-1");
    }

    /**
     * A writer the run does not route a table to, reporting that table, crashes bare: left out of the writers
     * the run waits for, it would hold nothing back, and a resume could pass the changes it still holds.
     */
    @Test
    void aWriterTheRunDoesNotRouteATableToIsAnInvariantViolation() {
        InMemorySrsMetaStore store = new InMemorySrsMetaStore();
        store.create("mc-orders", null);
        HazelcastInstance member = memberWith(store);
        SinkAck ack = startedRun(member, Map.of("orders", "mc-orders"), "pipe-1",
                Map.of("orders", List.of(OTHER_WRITER))).resolve(member).forWriter(WRITER);

        assertThatThrownBy(() -> ack.advance("orders", at(7, "w7")))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining(WRITER);
    }

    /**
     * The member's ack, before a writer is named, lands nothing at all: progress with no writer behind it has
     * no place among the writers the run waits for, and a record it moved would stand for all of them.
     */
    @Test
    void anAckNoWriterIsNamedForLandsNothing() {
        InMemorySrsMetaStore store = new InMemorySrsMetaStore();
        store.create("mc-orders", null);
        HazelcastInstance member = memberWith(store);
        SinkAck unbound = startedRun(member, Map.of("orders", "mc-orders"), "pipe-1",
                Map.of("orders", List.of(WRITER))).resolve(member);

        assertThatThrownBy(() -> unbound.advance("orders", at(7, "w7")))
                .isInstanceOf(IllegalStateException.class);
        assertThat(ackedPosition(store, "mc-orders", "pipe-1")).isNull();
    }

    /**
     * A load a change waits for has landed once every writer of the sink it is awaited at has passed it - the
     * writers of another sink the table reaches do not count, and neither does the table being recorded as
     * loaded, which waits for them too. A writer given none of the load's rows passes it by a bound.
     */
    @Test
    void aLoadHasLandedOnceEveryWriterOfTheSinkItIsAwaitedAtHasPassedIt() {
        InMemorySrsMetaStore store = new InMemorySrsMetaStore();
        store.create("mc-orders", null);
        store.setCdcStart("mc-orders", "pipe-1", "w0", 1L);
        HazelcastInstance member = memberWith(store);
        StoreBackedSinkAckFactory factory = startedRun(member, Map.of("orders", "mc-orders"), "pipe-1",
                Map.of("orders", List.of("serve.s#0", "serve.s#1", "serve.t#0")));
        AwaitedLoad load = new AwaitedLoad("serve.s", "orders", List.of("serve.s#0", "serve.s#1"));

        assertThat(factory.loadLandings(member).stillLanding(List.of(load))).containsExactly(load);

        factory.resolve(member).forWriter("serve.s#0").advance("orders",
                new ChainPosition(SourceOrder.snapshotRow(1), null));

        assertThat(factory.loadLandings(member).stillLanding(List.of(load))).containsExactly(load);

        factory.resolve(member).forWriter("serve.s#1").bounded("orders", SourceOrder.snapshotRow(1));

        assertThat(factory.loadLandings(member).stillLanding(List.of(load))).isEmpty();
        assertThat(store.read("mc-orders").orElseThrow().snapshotCompletedTables("pipe-1"))
                .as("while serve.t still holds the table back from being recorded as loaded")
                .isEmpty();
    }

    /** A load recorded as landed has landed, in a later run whose writers have said nothing yet. */
    @Test
    void aLoadRecordedAsLandedHasLandedWhicheverRunLandedIt() {
        InMemorySrsMetaStore store = new InMemorySrsMetaStore();
        store.create("mc-orders", null);
        store.setCdcStart("mc-orders", "pipe-1", "w0", 1L);
        store.markSnapshotComplete("mc-orders", "pipe-1", "orders");
        HazelcastInstance member = memberWith(store);
        StoreBackedSinkAckFactory factory = startedRun(member, Map.of("orders", "mc-orders"), "pipe-1",
                Map.of("orders", List.of(WRITER)), "run-2");

        assertThat(factory.loadLandings(member).stillLanding(
                List.of(new AwaitedLoad("serve.s", "orders", List.of(WRITER))))).isEmpty();
    }

    /** A table whose load the pipeline does not read at all has none in flight for a change to overtake. */
    @Test
    void aTableThePipelineReadsNoLoadOfHasNoneInFlight() {
        InMemorySrsMetaStore store = new InMemorySrsMetaStore();
        store.create("mc-orders", null);
        HazelcastInstance member = memberWith(store);
        StoreBackedSinkAckFactory factory = startedRun(member, Map.of("orders", "mc-orders"), "pipe-1",
                Map.of("orders", List.of(WRITER)));

        assertThat(factory.loadLandings(member).stillLanding(
                List.of(new AwaitedLoad("serve.s", "orders", List.of(WRITER))))).isEmpty();
    }

    /**
     * Progress of a run that is not this one says nothing about this run's writers: a run started after it
     * has replaced it, and a run not started yet has no writers to speak of.
     */
    @Test
    void noRunButThisOneSaysALoadHasLanded() {
        InMemorySrsMetaStore store = new InMemorySrsMetaStore();
        store.create("mc-orders", null);
        store.setCdcStart("mc-orders", "pipe-1", "w0", 1L);
        HazelcastInstance member = memberWith(store);
        Map<String, String> chains = Map.of("orders", "mc-orders");
        AwaitedLoad load = new AwaitedLoad("serve.s", "orders", List.of(WRITER));
        StoreBackedSinkAckFactory notStarted = new StoreBackedSinkAckFactory(chains, "pipe-1", "run-1");

        assertThat(notStarted.loadLandings(member).stillLanding(List.of(load))).containsExactly(load);

        // Another sink's writer still holds the table back from being recorded as loaded, which would answer
        // for every run.
        StoreBackedSinkAckFactory current = startedRun(member, chains, "pipe-1",
                Map.of("orders", List.of(WRITER, OTHER_WRITER)), "run-2");
        current.resolve(member).forWriter(WRITER).bounded("orders", SourceOrder.snapshotRow(1));

        assertThat(current.loadLandings(member).stillLanding(List.of(load))).isEmpty();
        assertThat(notStarted.loadLandings(member).stillLanding(List.of(load)))
                .as("run-1's writers never said anything, whatever run-2's did")
                .containsExactly(load);
    }

    /** A member with no store bound has no record a load could be seen landing in, and holds nothing back. */
    @Test
    void aMemberWithNoStoreBoundHoldsNothingBack() {
        HazelcastInstance member = mock(HazelcastInstance.class);
        when(member.getUserContext()).thenReturn(new ConcurrentHashMap<>());
        StoreBackedSinkAckFactory factory =
                new StoreBackedSinkAckFactory(Map.of("orders", "mc-orders"), "pipe-1", "run-1");

        assertThat(factory.loadLandings(member).stillLanding(
                List.of(new AwaitedLoad("serve.s", "orders", List.of(WRITER))))).isEmpty();
    }

    /** One change's position: the order the engine assigned it, and the token the connector gave. */
    private static ChainPosition at(long seq, String token) {
        return new ChainPosition(new SourceOrder(1, seq), token);
    }

    /**
     * The ack of a pipeline whose one sink is its only writer: the run started with that writer expected on
     * every table the pipeline reads, and the member's ack bound to it - the way every sink reports.
     */
    private static SinkAck soleWriter(HazelcastInstance member, Map<String, String> chainIdByTable, String pipelineId) {
        Map<String, List<String>> writers = new LinkedHashMap<>();
        chainIdByTable.keySet().forEach(table -> writers.put(table, List.of(WRITER)));
        return startedRun(member, chainIdByTable, pipelineId, writers).resolve(member).forWriter(WRITER);
    }

    private static StoreBackedSinkAckFactory startedRun(HazelcastInstance member,
            Map<String, String> chainIdByTable, String pipelineId, Map<String, List<String>> writersByTable) {
        return startedRun(member, chainIdByTable, pipelineId, writersByTable, "run-1");
    }

    /** A factory for {@code runId}, with the run started as an execution starts it. */
    private static StoreBackedSinkAckFactory startedRun(HazelcastInstance member,
            Map<String, String> chainIdByTable, String pipelineId, Map<String, List<String>> writersByTable,
            String runId) {
        StoreBackedSinkAckFactory factory = new StoreBackedSinkAckFactory(chainIdByTable, pipelineId, runId);
        factory.beginRun(member, writersByTable);
        return factory;
    }

    private static HazelcastInstance memberWith(SrsMetaStore store) {
        ConcurrentMap<String, Object> context = new ConcurrentHashMap<>();
        context.put(CaptureRunUnit.SRS_META_USER_CONTEXT_KEY, store);
        HazelcastInstance member = mock(HazelcastInstance.class);
        when(member.getUserContext()).thenReturn(context);
        return member;
    }

    private static ChainPosition ackedChainPosition(SrsMetaStore store, String chainId, String pipelineId) {
        return store.read(chainId).orElseThrow().consumerOffsets().stream()
                .filter(offset -> offset.pipelineId().equals(pipelineId))
                .map(ConsumerOffset::sinkAcked)
                .filter(java.util.Objects::nonNull)
                .findFirst()
                .orElse(null);
    }

    private static String ackedPosition(SrsMetaStore store, String chainId, String pipelineId) {
        return store.read(chainId).orElseThrow().consumerOffsets().stream()
                .filter(offset -> offset.pipelineId().equals(pipelineId))
                .map(ConsumerOffset::sinkAckedSrcpos)
                .filter(java.util.Objects::nonNull)
                .findFirst()
                .orElse(null);
    }
}
