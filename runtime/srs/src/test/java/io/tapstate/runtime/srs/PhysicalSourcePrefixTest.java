package io.tapstate.runtime.srs;

import io.tapstate.core.common.TapstateException;
import io.tapstate.core.event.ChainPosition;
import io.tapstate.core.event.SourceOrder;
import io.tapstate.spi.capture.SourcePosition;
import io.tapstate.spi.store.ConsumerOffset;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * The reader's account of the runs it handed on: what it releases, when, and what it refuses to open over.
 * Each case drives the account directly -- a run recorded, confirmations landing -- and reads the chain's
 * record back, which is all a restart would ever see.
 */
class PhysicalSourcePrefixTest {

    private static final String CHAIN = "chain-prefix";

    private CaptureRunUnitTest.InMemoryMeta meta;
    private long epoch;
    private final CaptureHealth health = new CaptureHealth();
    private final List<String> cuts = new ArrayList<>();
    private final List<PhysicalSourcePrefix> opened = new ArrayList<>();

    @BeforeEach
    void seedTheChain() {
        meta = new CaptureRunUnitTest.InMemoryMeta();
        meta.create(CHAIN, null);
        epoch = meta.openEpoch(CHAIN);
    }

    @AfterEach
    void closeEveryAccount() {
        opened.forEach(PhysicalSourcePrefix::close);
    }

    /**
     * The case the account exists for. One pipeline reads orders and customers; the source hands over an
     * orders change and then a customers change, and customers lands first. A position written down on that
     * word would sit past the orders change, and a restart would never read it again.
     */
    @Test
    void aQuietTablesConfirmationDoesNotReleaseARunAnotherTableStillOwes() {
        select("pipe", "orders", "customers");
        PhysicalSourcePrefix prefix = shared("customers", "orders");
        prefix.start(at("t0"));
        prefix.admitted(Map.of("orders", 0L), "t1");
        prefix.admitted(Map.of("customers", 0L), "t2");

        ack("pipe", "customers", 0);
        prefix.tick();
        assertThat(sourceRead()).as("the orders change before it has not landed").isEqualTo("t0");

        ack("pipe", "orders", 0);
        prefix.tick();
        assertThat(sourceRead()).as("both runs released, in the order the source handed them over")
                .isEqualTo("t2");
        assertThat(consumer("pipe").sinkAcked().token()).isEqualTo("t2");
        assertThat(meta.releasedSourceReads).extracting(ChainPosition::token).containsExactly("t1", "t2");
    }

    /**
     * A run that carried no change but named a position -- a heartbeat -- moves the chain, and only behind
     * every run before it. It may be the only position a quiet source ever names.
     */
    @Test
    void aRunCarryingNoChangeIsReleasedOnlyBehindEveryRunBeforeIt() {
        select("pipe", "orders");
        PhysicalSourcePrefix prefix = shared("orders");
        prefix.start(at("t0"));
        prefix.admitted(Map.of("orders", 4L), "t1");
        prefix.admitted(Map.of(), "h2");
        prefix.tick();
        assertThat(sourceRead()).as("held behind the change still in flight").isEqualTo("t0");

        ack("pipe", "orders", 4);
        prefix.tick();
        assertThat(sourceRead()).isEqualTo("h2");

        prefix.admitted(Map.of(), "h3");
        assertThat(sourceRead()).as("with nothing ahead of it, released as it is recorded").isEqualTo("h3");
    }

    /** A run that carried nothing and named nothing tells nobody anything, and moves nothing. */
    @Test
    void aRunNamingNoPositionIsReleasedWithoutWritingAnything() {
        select("pipe", "orders");
        PhysicalSourcePrefix prefix = shared("orders");
        prefix.start(at("t0"));
        prefix.admitted(Map.of("orders", 0L), null);
        ack("pipe", "orders", 0);
        prefix.tick();

        assertThat(sourceRead()).isEqualTo("t0");
        assertThat(prefix.pendingBatches()).isZero();
    }

    /**
     * A full account holds the source back instead of letting it run ahead: a run that cannot be recorded
     * cannot be released later either. The wait ends as soon as a run is released.
     */
    @Test
    void aFullAccountHoldsItsSourceBackUntilARunIsReleased() throws Exception {
        select("pipe", "orders");
        PhysicalSourcePrefix prefix = shared("orders");
        prefix.start(at("t0"));
        for (int seq = 0; seq < PhysicalSourcePrefix.MAX_PENDING_BATCHES; seq++) {
            prefix.admitted(Map.of("orders", (long) seq), "t" + seq);
        }
        CompletableFuture<Void> room = CompletableFuture.runAsync(prefix::awaitRoom);
        assertThat(room).as("held back while every recorded run is still owed").isNotDone();
        Thread.sleep(3 * PhysicalSourcePrefix.TICK_MILLIS);
        assertThat(room).isNotDone();

        ack("pipe", "orders", 0);
        room.get(5, TimeUnit.SECONDS);
        assertThat(prefix.pendingBatches()).isEqualTo(PhysicalSourcePrefix.MAX_PENDING_BATCHES - 1);
    }

    /** Confirmations that land while the source is quiet are still acted on, by the shared re-check. */
    @Test
    void aConfirmationLandingWhileTheSourceIsQuietIsStillActedOn() throws Exception {
        select("pipe", "orders");
        PhysicalSourcePrefix prefix = shared("orders");
        prefix.start(at("t0"));
        prefix.admitted(Map.of("orders", 0L), "t1");

        ack("pipe", "orders", 0);

        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(5);
        while (!"t1".equals(sourceRead()) && System.nanoTime() < deadline) {
            Thread.sleep(PhysicalSourcePrefix.TICK_MILLIS / 2);
        }
        assertThat(sourceRead()).isEqualTo("t1");
    }

    /** A reader whose generation another reader has taken stops with a code, and writes nothing down. */
    @Test
    void aReaderThatLostItsGenerationStopsWithACode() {
        select("pipe", "orders");
        PhysicalSourcePrefix prefix = shared("orders");
        prefix.start(at("t0"));
        prefix.admitted(Map.of("orders", 0L), "t1");
        meta.openEpoch(CHAIN);
        ack("pipe", "orders", 0);

        // Whichever re-check gets there first: this thread's, which throws to it, or the shared one, which
        // records the failure on the run's health. Either way the reader has stopped.
        Throwable thrown = org.assertj.core.api.Assertions.catchThrowable(prefix::tick);
        Throwable failure = thrown != null ? thrown : health.failure().orElseThrow();
        assertThat(failure).isInstanceOfSatisfying(TapstateException.class,
                coded -> assertThat(coded.code()).isEqualTo(CaptureError.CHAIN_TAKEN_OVER));
        assertThatThrownBy(() -> prefix.admitted(Map.of(), "h9")).as("nothing more is recorded after it");
        assertThat(sourceRead()).isEqualTo("t0");
    }

    /**
     * The first stream of a chain is resumable from its first moment: where it began is written down before
     * anything is handed over. One that names no start where there is nothing to resume from is refused.
     */
    @Test
    void aFirstStreamIsAnchoredWhereItBeganOrRefusedWhenItNamesNoStart() {
        select("pipe", "orders");
        PhysicalSourcePrefix anchored = shared("orders");
        anchored.start(at("t0"));
        assertThat(meta.read(CHAIN).orElseThrow().sourceRead())
                .isEqualTo(new ChainPosition(new SourceOrder(epoch, -1L), "t0"));
        assertThat(meta.physicalPrefixTrusted(CHAIN)).isTrue();

        seedTheChain();
        select("pipe", "orders");
        PhysicalSourcePrefix nameless = shared("orders");
        assertThatThrownBy(() -> nameless.start(Optional.empty()))
                .isInstanceOfSatisfying(TapstateException.class,
                        failure -> assertThat(failure.code()).isEqualTo(CaptureError.RESUME_ANCHOR_UNAVAILABLE));
        assertThat(sourceRead()).isNull();
    }

    /**
     * An offset written before acknowledgements were kept per table cannot be trusted across several tables:
     * nothing says whether one table's word moved it past another's change.
     */
    @Test
    void anOffsetFromBeforeTableAcknowledgementsIsRefusedForSeveralTables() {
        meta.advanceSourceReadOffset(CHAIN, new ChainPosition(new SourceOrder(epoch, 7), "legacy"));
        select("pipe", "orders", "customers");

        assertThatThrownBy(() -> shared("customers", "orders"))
                .isInstanceOfSatisfying(TapstateException.class,
                        failure -> assertThat(failure.code()).isEqualTo(CaptureError.SHARED_POSITION_UNVERIFIED));
        assertThat(meta.physicalPrefixTrusted(CHAIN)).isFalse();
    }

    /**
     * A chain that only ever carried one table keeps its old offset: an acknowledgement on it could only
     * speak for that table. A consumer whose record shows it read another table rules that out.
     */
    @Test
    void aSingleTableChainsOldOffsetIsAdoptedOnlyWhenNothingElseWasEverReadOnIt() {
        meta.advanceSourceReadOffset(CHAIN, new ChainPosition(new SourceOrder(epoch, 7), "legacy"));
        meta.advanceConsumerReadSeq(CHAIN, "old", "orders", 12);
        select("pipe", "orders");
        shared("orders");
        assertThat(meta.physicalPrefixTrusted(CHAIN)).isTrue();

        seedTheChain();
        meta.advanceSourceReadOffset(CHAIN, new ChainPosition(new SourceOrder(epoch, 7), "legacy"));
        meta.advanceConsumerReadSeq(CHAIN, "old", "customers", 12);
        select("pipe", "orders");
        assertThatThrownBy(() -> shared("orders"))
                .isInstanceOfSatisfying(TapstateException.class,
                        failure -> assertThat(failure.code()).isEqualTo(CaptureError.SHARED_POSITION_UNVERIFIED));
    }

    /** A pipeline that stopped reading a table owes that table nothing, even for a run recorded before. */
    @Test
    void aConsumerThatStoppedReadingATableOwesItNothing() {
        select("pipe", "orders", "customers");
        PhysicalSourcePrefix prefix = shared("customers", "orders");
        prefix.start(at("t0"));
        prefix.admitted(Map.of("customers", 0L), "t1");

        select("pipe", "orders");
        prefix.tick();

        assertThat(sourceRead()).isEqualTo("t1");
    }

    /** A pipeline the chain no longer records owes nothing, and is not brought back by the release. */
    @Test
    void aConsumerThatLeftTheChainOwesNothing() {
        select("pipe", "orders");
        select("gone", "orders");
        PhysicalSourcePrefix prefix = shared("orders");
        prefix.start(at("t0"));
        prefix.admitted(Map.of("orders", 0L), "t1");
        ack("pipe", "orders", 0);
        prefix.tick();
        assertThat(sourceRead()).as("held for the pipeline still recorded").isEqualTo("t0");

        meta.detachConsumer(CHAIN, "gone");
        prefix.tick();

        assertThat(sourceRead()).isEqualTo("t1");
        assertThat(meta.read(CHAIN).orElseThrow().consumerOffset("gone")).isEmpty();
    }

    /**
     * A pipeline arriving after a run was written never sees it -- it starts past it in the ring -- so the run
     * is not held for it. One recorded as the run was, whose place in the ring is already past it, confirms it
     * by that place.
     */
    @Test
    void aPipelineArrivingPastARunIsNotHeldToIt() {
        select("pipe", "orders");
        PhysicalSourcePrefix prefix = shared("orders");
        prefix.start(at("t0"));
        prefix.admitted(Map.of("orders", 3L), "t1");
        select("late", "orders");
        ack("pipe", "orders", 3);
        prefix.tick();
        assertThat(sourceRead()).as("a pipeline that arrived afterwards owes it nothing").isEqualTo("t1");

        select("arriving", "orders");
        prefix.admitted(Map.of("orders", 5L), "t2");
        ack("pipe", "orders", 5);
        ack("late", "orders", 5);
        prefix.tick();
        assertThat(sourceRead()).as("recorded as the run was, with no place in the ring yet").isEqualTo("t1");

        meta.startRingAfter(CHAIN, "arriving", "orders", 5);
        prefix.tick();
        assertThat(sourceRead()).as("its place in the ring is past the run").isEqualTo("t2");
    }

    /**
     * A reader that took the chain over writes under a generation of its own, while the pipelines already on
     * it carry on reading under the selection they made before. Their confirmations still release its runs.
     */
    @Test
    void aTakeoverIsReleasedByThePipelinesAlreadyReading() {
        select("pipe", "orders");
        long takenOver = meta.openEpoch(CHAIN);
        PhysicalSourcePrefix prefix = PhysicalSourcePrefix.shared(
                meta, CHAIN, takenOver, List.of("orders"), health, (table, seq) -> { });
        opened.add(prefix);
        prefix.start(at("t0"));
        prefix.admitted(Map.of("orders", 9L), "t1");

        meta.advanceTableSinkAcked(CHAIN, "pipe", "orders", new ChainPosition(new SourceOrder(takenOver, 9), null));
        prefix.tick();

        assertThat(sourceRead()).isEqualTo("t1");
    }

    /** A pipeline reading the chain through a direct tail of its own never sees the shared ring. */
    @Test
    void aPipelineReadingDirectlyIsNotOwedTheSharedRing() {
        select("pipe", "orders");
        select("direct");
        PhysicalSourcePrefix prefix = shared("orders");
        prefix.start(at("t0"));
        prefix.admitted(Map.of("orders", 0L), "t1");

        ack("pipe", "orders", 0);
        prefix.tick();

        assertThat(sourceRead()).isEqualTo("t1");
        assertThat(consumer("direct").sinkAcked()).as("its own position is its own tail's to move").isNull();
    }

    /** Each table's durable log is cut, as a run is released, through the last sequence the run reached there. */
    @Test
    void aReleaseCutsEachTablesLogThroughWhatTheRunReachedThere() {
        select("pipe", "orders", "customers");
        PhysicalSourcePrefix prefix = shared("customers", "orders");
        prefix.start(at("t0"));
        prefix.admitted(Map.of("orders", 4L, "customers", 2L), "t1");
        prefix.admitted(Map.of(), "h2");
        assertThat(cuts).isEmpty();

        ack("pipe", "orders", 4);
        ack("pipe", "customers", 2);
        prefix.tick();

        assertThat(cuts).containsExactlyInAnyOrder("orders@4", "customers@2");
    }

    /**
     * A direct tail's account is owed to the one pipeline it streams to, and moves both that pipeline's
     * position and the chain's -- where that pipeline is the chain's only consumer.
     */
    @Test
    void aDirectTailIsReleasedByItsOwnPipelineAlone() {
        meta.selectConsumerTables(CHAIN, "direct", List.of(), epoch);
        PhysicalSourcePrefix prefix = PhysicalSourcePrefix.direct(meta, CHAIN, epoch, "direct", health);
        opened.add(prefix);
        prefix.start(at("t0"));
        prefix.admitted(Map.of("orders", 0L), null);
        prefix.admitted(Map.of("customers", 1L, "orders", 2L), "t2");

        ack("direct", "orders", 2);
        prefix.tick();
        assertThat(sourceRead()).as("customers has not landed").isEqualTo("t0");

        ack("direct", "customers", 1);
        prefix.tick();
        assertThat(sourceRead()).isEqualTo("t2");
        assertThat(consumer("direct").sinkAcked().token()).isEqualTo("t2");
    }

    private PhysicalSourcePrefix shared(String... tables) {
        PhysicalSourcePrefix prefix = PhysicalSourcePrefix.shared(meta, CHAIN, epoch, List.of(tables), health,
                (table, seq) -> cuts.add(table + "@" + seq));
        opened.add(prefix);
        return prefix;
    }

    private void select(String pipeline, String... tables) {
        meta.selectConsumerTables(CHAIN, pipeline, List.of(tables), epoch);
    }

    private void ack(String pipeline, String table, long seq) {
        meta.advanceTableSinkAcked(CHAIN, pipeline, table, new ChainPosition(new SourceOrder(epoch, seq), null));
    }

    private String sourceRead() {
        return meta.read(CHAIN).orElseThrow().sourceReadOffset();
    }

    private ConsumerOffset consumer(String pipeline) {
        return meta.read(CHAIN).orElseThrow().consumerOffset(pipeline).orElseThrow();
    }

    private static Optional<SourcePosition> at(String token) {
        return Optional.of(new SourcePosition(token));
    }
}
