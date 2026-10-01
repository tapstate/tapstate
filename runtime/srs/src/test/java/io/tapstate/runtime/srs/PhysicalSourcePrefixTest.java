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

    /**
     * A run that carried nothing a pipeline reads says nothing about that pipeline's target. Its acknowledged
     * position stays at the last change the target confirmed while the chain moves on past the quiet run, so
     * a source that keeps naming positions while nothing changes does not keep moving it.
     */
    @Test
    void aRunCarryingNothingForAPipelineLeavesWhereItsTargetStands() {
        select("pipe", "orders");
        select("other", "customers");
        PhysicalSourcePrefix prefix = shared("customers", "orders");
        prefix.start(at("t0"));
        prefix.admitted(Map.of("orders", 0L), "t1");
        ack("pipe", "orders", 0);
        prefix.tick();
        prefix.admitted(Map.of(), "h2");

        assertThat(sourceRead()).as("the quiet run still moves the chain").isEqualTo("h2");
        assertThat(consumer("pipe").sinkAcked().token()).as("its target confirmed t1 and nothing since")
                .isEqualTo("t1");
        assertThat(consumer("other").sinkAcked()).as("nothing it reads has come through yet").isNull();
    }

    /** A direct tail's quiet run moves the chain the same way, and leaves its pipeline's position alone. */
    @Test
    void aDirectTailsQuietRunMovesTheChainButNotItsPipelinesPosition() {
        meta.selectConsumerTables(CHAIN, "direct", List.of(), epoch);
        PhysicalSourcePrefix prefix = PhysicalSourcePrefix.direct(meta, CHAIN, epoch, "direct", health);
        opened.add(prefix);
        prefix.start(at("t0"));
        prefix.admitted(Map.of("orders", 0L), "t1");
        ack("direct", "orders", 0);
        prefix.tick();
        prefix.admitted(Map.of(), "h2");

        assertThat(sourceRead()).isEqualTo("h2");
        assertThat(consumer("direct").sinkAcked().token()).isEqualTo("t1");
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
     * A run nobody confirms -- one owed to a paused pipeline, or the last change of a table a transform
     * dropped -- does not hold the source back for everyone else on it. Runs go on being recorded behind it,
     * the newest folded into the last once the account is full, and nothing is released past it until it
     * lands. What is then released says what the runs said one by one: how far the source was read, where
     * the last change was, and for each pipeline where its own last change was.
     */
    @Test
    void aRunNobodyConfirmsDoesNotHoldTheSourceBack() {
        select("paused", "orders");
        select("busy", "customers");
        PhysicalSourcePrefix prefix = shared("customers", "orders");
        prefix.start(at("t0"));
        prefix.admitted(Map.of("orders", 0L), "t1");
        int runs = 2 * PhysicalSourcePrefix.MAX_PENDING_BATCHES;
        for (int run = 0; run < runs; run++) {
            prefix.admitted(Map.of("customers", (long) run), "c" + run);
            prefix.admitted(Map.of(), "h" + run);
        }
        assertThat(prefix.pendingBatches()).isLessThanOrEqualTo(PhysicalSourcePrefix.MAX_PENDING_BATCHES);

        // The busy pipeline confirms as far as the first run folded into the account's last entry.
        int firstFolded = (PhysicalSourcePrefix.MAX_PENDING_BATCHES - 2) / 2;
        ack("busy", "customers", firstFolded);
        prefix.tick();
        assertThat(sourceRead()).as("nothing passes the run the paused pipeline still owes").isEqualTo("t0");

        ack("paused", "orders", 0);
        prefix.tick();
        assertThat(sourceRead()).as("the last entry waits for every run folded into it")
                .isEqualTo("h" + (firstFolded - 1));

        ack("busy", "customers", runs - 1);
        prefix.tick();
        assertThat(sourceRead()).isEqualTo("h" + (runs - 1));
        assertThat(meta.resumeOffset(CHAIN)).as("a restart begins at the last change, not the quiet run after it")
                .contains("c" + (runs - 1));
        assertThat(consumer("busy").sinkAcked().token()).isEqualTo("c" + (runs - 1));
        assertThat(consumer("paused").sinkAcked().token()).as("its own last change, not anybody else's")
                .isEqualTo("t1");
        assertThat(prefix.pendingBatches()).isZero();
    }

    /** Quiet runs recorded behind a run still owed are folded into one: there is nothing in them to owe. */
    @Test
    void quietRunsBehindARunStillOwedAreFoldedIntoOne() {
        select("pipe", "orders");
        PhysicalSourcePrefix prefix = shared("orders");
        prefix.start(at("t0"));
        prefix.admitted(Map.of("orders", 0L), "t1");
        for (int beat = 0; beat < 10; beat++) {
            prefix.admitted(Map.of(), "h" + beat);
        }
        assertThat(prefix.pendingBatches()).isEqualTo(2);

        ack("pipe", "orders", 0);
        prefix.tick();
        assertThat(sourceRead()).isEqualTo("h9");
        assertThat(meta.resumeOffset(CHAIN)).contains("t1");
    }

    /**
     * A direct tail folds the same way, and a fold that ends in a quiet run still resumes from the change
     * before it.
     */
    @Test
    void aDirectTailFoldsWhatItCannotKeepApartAndResumesFromItsLastChange() {
        meta.selectConsumerTables(CHAIN, "direct", List.of(), epoch);
        PhysicalSourcePrefix prefix = PhysicalSourcePrefix.direct(meta, CHAIN, epoch, "direct", health);
        opened.add(prefix);
        prefix.start(at("t0"));
        int runs = 2 * PhysicalSourcePrefix.MAX_PENDING_BATCHES;
        for (int run = 0; run < runs; run++) {
            prefix.admitted(Map.of("orders", (long) run), "c" + run);
            prefix.admitted(Map.of(), "h" + run);
        }
        assertThat(prefix.pendingBatches()).isLessThanOrEqualTo(PhysicalSourcePrefix.MAX_PENDING_BATCHES);

        ack("direct", "orders", runs - 1);
        prefix.tick();
        assertThat(sourceRead()).isEqualTo("h" + (runs - 1));
        assertThat(meta.resumeOffset(CHAIN)).contains("c" + (runs - 1));
        assertThat(consumer("direct").sinkAcked().token()).isEqualTo("c" + (runs - 1));
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

    /**
     * A store that cannot be read for a moment does not stop the reader: the re-check skips that turn and a
     * later one releases what the confirmations allow. Nothing can be released on confirmations nobody could
     * read, so skipping is the safe direction.
     */
    @Test
    void aStoreThatCannotBeReadForAMomentDoesNotStopTheReader() {
        java.util.concurrent.atomic.AtomicBoolean unreadable = new java.util.concurrent.atomic.AtomicBoolean();
        meta = new CaptureRunUnitTest.InMemoryMeta() {
            @Override
            public synchronized List<ConsumerOffset> consumerOffsets(String miningChainId) {
                if (unreadable.get()) {
                    throw new TapstateException(io.tapstate.spi.store.IoError.STORE_UNAVAILABLE,
                            Map.of("detail", "the primary is being elected"), null);
                }
                return super.consumerOffsets(miningChainId);
            }
        };
        meta.create(CHAIN, null);
        epoch = meta.openEpoch(CHAIN);
        select("pipe", "orders");
        PhysicalSourcePrefix prefix = shared("orders");
        prefix.start(at("t0"));
        prefix.admitted(Map.of("orders", 0L), "t1");

        unreadable.set(true);
        ack("pipe", "orders", 0);
        prefix.tick();
        assertThat(health.failure()).as("the reader goes on").isEmpty();
        assertThat(sourceRead()).isEqualTo("t0");

        unreadable.set(false);
        prefix.tick();
        assertThat(sourceRead()).isEqualTo("t1");
        assertThat(health.failure()).isEmpty();
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
        assertThatThrownBy(() -> prefix.admitted(Map.of(), "h9"))
                .as("nothing more is recorded after it: every later call throws that same failure")
                .isSameAs(failure);
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
