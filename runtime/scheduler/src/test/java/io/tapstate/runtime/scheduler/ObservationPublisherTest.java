package io.tapstate.runtime.scheduler;

import io.tapstate.core.lifecycle.CasOutcome;
import io.tapstate.core.lifecycle.CheckpointDoc;
import io.tapstate.core.lifecycle.FrontierStallPressure;
import io.tapstate.core.lifecycle.NestColdLayerPressure;
import io.tapstate.core.lifecycle.NestStateWindow;
import io.tapstate.core.lifecycle.Observation;
import io.tapstate.core.lifecycle.ObservationFailure;
import io.tapstate.core.lifecycle.NestStateReading;
import io.tapstate.core.lifecycle.PipelineState;
import io.tapstate.core.lifecycle.TableSnapshot;
import io.tapstate.core.lifecycle.StateJson;
import io.tapstate.spi.store.ObservationStore;
import io.tapstate.spi.store.StateStore;
import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.function.Function;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.assertj.core.api.Assertions.entry;

/**
 * The runtime observation publisher reads a pipeline's converged actual state and writes it out as the
 * pipeline's latest observation, so the control read faces have a store-backed projection to read. L1
 * wires the errorCount metric from the actual state (0 healthy, 1 when FAILED); the remaining metrics are
 * absent and the snapshot dataset is published empty (no source yet). A pipeline with no checkpoint yet is
 * left unobserved rather than published as an empty doc.
 */
class ObservationPublisherTest {

    private static final Instant T0 = Instant.parse("2026-07-01T00:00:00Z");

    private final MutableStateStore state = new MutableStateStore();
    private final RecordingObservationStore observations = new RecordingObservationStore();
    private final ObservationPublisher publisher = new ObservationPublisher(state, observations);

    /** A publisher whose only wired source is the nest readings, watching them through {@code alert}. */
    private ObservationPublisher withWatch(
            NestColdLayerAlert alert, Function<String, Map<String, NestStateReading>> readings) {
        return new ObservationPublisher(state, observations,
                id -> OptionalLong.empty(), id -> Map.of(), id -> Map.of(), id -> Map.of(), readings,
                new NestColdLayerWatch(new NestColdLayerPressure(0.5, 100), alert));
    }

    /** Collects the namespaces reported, which is what a caller of the publisher can observe of the watch. */
    private static final class RecordingAlert implements NestColdLayerAlert {

        private final List<String> crossed = new ArrayList<>();
        private final List<String> cleared = new ArrayList<>();

        @Override
        public void crossed(String pipelineId, String namespace, NestStateWindow window) {
            crossed.add(namespace);
        }

        @Override
        public void cleared(String pipelineId, String namespace, NestStateWindow window) {
            cleared.add(namespace);
        }
    }

    @Test
    void publishesTheActualStateAsAnObservation() {
        state.seed("orders", PipelineState.RUNNING);

        publisher.publish("orders");

        Observation published = observations.read("orders").orElseThrow();
        assertThat(published.pipelineId()).isEqualTo("orders");
        assertThat(published.state()).isEqualTo(PipelineState.RUNNING);
        // errorCount is wired from the actual state: a healthy pipeline reports zero errors. The snapshot
        // source is not wired yet, so it is published empty (unavailable), not faked. This publisher was
        // built with no metric or position source, so recordCount is absent and positions are empty.
        assertThat(published.metrics()).containsOnly(entry("errorCount", 0L));
        assertThat(published.snapshot()).isEmpty();
        assertThat(published.positions()).isEmpty();
    }

    @Test
    void publishWiresTheRecordCountFromItsSourceIntoTheMetrics() {
        state.seed("orders", PipelineState.RUNNING);
        ObservationPublisher wired = new ObservationPublisher(
                state, observations, id -> OptionalLong.of(128L), id -> Map.of());

        wired.publish("orders");

        // recordCount rides the numeric metrics map alongside the always-present errorCount gauge.
        assertThat(observations.read("orders").orElseThrow().metrics())
                .containsOnly(entry("errorCount", 0L), entry("recordCount", 128L));
    }

    @Test
    void recordCountIsAbsentFromTheMetricsWhenItsSourceHasNoLiveJob() {
        state.seed("orders", PipelineState.RUNNING);
        ObservationPublisher wired = new ObservationPublisher(
                state, observations, id -> OptionalLong.empty(), id -> Map.of());

        wired.publish("orders");

        // A missing metric means the source is not wired (here: no live job), expressed by its absence
        // rather than a zero sentinel, so only the errorCount gauge is carried.
        assertThat(observations.read("orders").orElseThrow().metrics()).containsOnly(entry("errorCount", 0L));
    }

    @Test
    void publishWiresHowFarEachChainsFrontierTrailsIntoTheMetrics() {
        state.seed("orders", PipelineState.RUNNING);
        ObservationPublisher wired = new ObservationPublisher(state, observations,
                id -> OptionalLong.empty(), id -> Map.of(), id -> Map.of(),
                id -> Map.of("orders", 0L, "order_items", 480L));

        wired.publish("orders");

        // One entry per chain, named by it. A frontier standing still is one symptom of two causes, and the
        // distance is what tells them apart: order_items is running ahead of positions it was ever given,
        // while orders is exactly where its bound lets it be. A zero and a large number are both readings.
        assertThat(observations.read("orders").orElseThrow().metrics())
                .containsOnly(entry("errorCount", 0L),
                        entry("frontierGap.orders", 0L), entry("frontierGap.order_items", 480L));
    }

    @Test
    void publishWiresHowLongEachChainHasBeenPinnedIntoTheMetrics() {
        state.seed("orders", PipelineState.RUNNING);
        ObservationPublisher wired = new ObservationPublisher(state, observations,
                id -> OptionalLong.empty(), id -> Map.of(), id -> Map.of(),
                id -> Map.of("orders", 0L, "order_items", 480L),
                id -> Map.of(),
                new NestColdLayerWatch(NestColdLayerPressure.DEFAULT, NestColdLayerAlert.NONE),
                id -> Map.of("orders", 96_000L));

        wired.publish("orders");

        // The two readings are carried side by side and are not interchangeable, which is why the values
        // here differ and both are asserted by name: orders is pinned for a minute and a half at a
        // distance of zero, and reading either number under the other's name would describe a pipeline
        // that is not this one. order_items has a distance and is not pinned, so it has no entry here -
        // a zero would say it is pinned and has just advanced, which is the healthy end of this scale.
        assertThat(observations.read("orders").orElseThrow().metrics())
                .containsOnly(entry("errorCount", 0L),
                        entry("frontierGap.orders", 0L), entry("frontierGap.order_items", 480L),
                        entry("frontierStalledMillis.orders", 96_000L));
    }

    @Test
    void theTimePinnedIsAbsentFromTheMetricsWhenNoChainIsPinned() {
        state.seed("orders", PipelineState.RUNNING);
        ObservationPublisher wired = new ObservationPublisher(state, observations,
                id -> OptionalLong.empty(), id -> Map.of(), id -> Map.of(),
                id -> Map.of("orders", 0L));

        wired.publish("orders");

        // A pipeline whose chains all keep up publishes no duration at all. A zero here would be a chain
        // reporting that it is pinned and just advanced, which is a reading rather than the absence of one.
        assertThat(observations.read("orders").orElseThrow().metrics())
                .containsOnly(entry("errorCount", 0L), entry("frontierGap.orders", 0L));
    }

    @Test
    void theFrontierGapIsAbsentFromTheMetricsWhenNoSinkReportsOne() {
        state.seed("orders", PipelineState.RUNNING);
        ObservationPublisher wired = new ObservationPublisher(
                state, observations, id -> OptionalLong.empty(), id -> Map.of(), id -> Map.of());

        wired.publish("orders");

        // Absent means unmeasured, and a zero would read as a frontier keeping up with its bound - the
        // opposite reading, and the one an alarm over this number would stay quiet on.
        assertThat(observations.read("orders").orElseThrow().metrics()).containsOnly(entry("errorCount", 0L));
    }

    @Test
    void publishWiresWhatEachNestNamespaceHoldsAndWhatItCostsIntoTheMetrics() {
        state.seed("orders", PipelineState.RUNNING);
        ObservationPublisher wired = new ObservationPublisher(state, observations,
                id -> OptionalLong.empty(), id -> Map.of(), id -> Map.of(), id -> Map.of(),
                id -> Map.of("nest.orders.doc.$root", new NestStateReading(4_000L, 900L, 30L, 210L)));

        wired.publish("orders");

        // Counts per namespace rather than the one ratio they imply: a ratio published here would be an
        // average over the whole run, and a state layer that fell off its cliff a minute ago still reads
        // as healthy in it. Two scrapes of counts give any window a reader wants.
        assertThat(observations.read("orders").orElseThrow().metrics())
                .containsOnly(entry("errorCount", 0L),
                        entry("nestStateEntries.nest.orders.doc.$root", 4_000L),
                        entry("nestStateAccesses.nest.orders.doc.$root", 900L),
                        entry("nestStateBackfills.nest.orders.doc.$root", 30L),
                        entry("nestStateBackfillMillis.nest.orders.doc.$root", 210L),
                        entry("nestStatePendingHighWater.nest.orders.doc.$root", 0L));
    }

    /**
     * The reading no other one can stand in for. What waits under a key lives inside that key's entry, so a
     * queue growing towards the limit that bounds it moves none of the numbers above - the namespace holds
     * the same entries, the layer behind it holds the same, and the key is hot enough to be served from
     * memory throughout. Every published number stays flat and then the run stops.
     *
     * <p>Zero and absent are the same thing here on purpose, unlike the stored reading beside it: a
     * namespace where nothing has waited has genuinely had nothing waiting, which is a fact about the run
     * rather than a question nobody could ask.
     */
    @Test
    void publishWiresHowDeepOneKeysWaitHasEverGotIntoTheMetrics() {
        state.seed("orders", PipelineState.RUNNING);
        ObservationPublisher wired = new ObservationPublisher(state, observations,
                id -> OptionalLong.empty(), id -> Map.of(), id -> Map.of(), id -> Map.of(),
                id -> Map.of("nest.orders.doc.$root",
                        new NestStateReading(4_000L, 900L, 30L, 210L, 9_512L, OptionalLong.empty())));

        wired.publish("orders");

        assertThat(observations.read("orders").orElseThrow().metrics())
                .describedAs("a key that got within five hundred of the limit says so before it is reached")
                .contains(entry("nestStatePendingHighWater.nest.orders.doc.$root", 9_512L));
    }

    /**
     * What is in memory and how much there is are two numbers, and publishing only the first would say a
     * namespace holds four thousand when it holds a hundred times that with the rest on the layer behind
     * it. Once what stays in memory is a budget, the entries reading is what the budget costs rather than
     * what the pipeline has.
     */
    @Test
    void publishWiresHowMuchANamespaceHoldsAltogetherBesideWhatIsInMemory() {
        state.seed("orders", PipelineState.RUNNING);
        ObservationPublisher wired = new ObservationPublisher(state, observations,
                id -> OptionalLong.empty(), id -> Map.of(), id -> Map.of(), id -> Map.of(),
                id -> Map.of("nest.orders.doc.$root",
                        new NestStateReading(4_000L, 900L, 30L, 210L, OptionalLong.of(400_000L))));

        wired.publish("orders");

        assertThat(observations.read("orders").orElseThrow().metrics())
                .contains(entry("nestStateEntries.nest.orders.doc.$root", 4_000L),
                        entry("nestStateStored.nest.orders.doc.$root", 400_000L));
    }

    /**
     * A run keeping its state in memory alone has no second number, and one published anyway would be the
     * first wearing the name of the second - a namespace reading as though its cold layer held exactly what
     * memory did, which is the one shape that says nothing is being evicted when nothing can be.
     */
    @Test
    void howMuchANamespaceHoldsAltogetherIsAbsentWhereThereIsNoColdLayerToAsk() {
        state.seed("orders", PipelineState.RUNNING);
        ObservationPublisher wired = new ObservationPublisher(state, observations,
                id -> OptionalLong.empty(), id -> Map.of(), id -> Map.of(), id -> Map.of(),
                id -> Map.of("nest.orders.doc.$root", new NestStateReading(4_000L, 900L, 30L, 210L)));

        wired.publish("orders");

        assertThat(observations.read("orders").orElseThrow().metrics())
                .doesNotContainKey("nestStateStored.nest.orders.doc.$root");
    }

    /**
     * The one number about a nest that nothing else on this face can stand in for. A pipeline discarding
     * every row it reads and one discarding none produce the same documents, the same record count and the
     * same state readings, because the rows counted here were never going to appear in a document at all.
     */
    @Test
    void publishWiresHowManyChangesANamespaceCouldNeverPlaceInADocument() {
        state.seed("orders", PipelineState.RUNNING);
        ObservationPublisher wired = new ObservationPublisher(state, observations,
                id -> OptionalLong.empty(), id -> Map.of(), id -> Map.of(), id -> Map.of(), id -> Map.of(),
                new NestColdLayerWatch(NestColdLayerPressure.DEFAULT, NestColdLayerAlert.NONE),
                id -> Map.of(),
                new FrontierStallWatch(FrontierStallPressure.DEFAULT, FrontierStallAlert.NONE),
                id -> Map.of("nest.orders.doc.items", 412L));

        wired.publish("orders");

        assertThat(observations.read("orders").orElseThrow().metrics())
                .contains(entry("nestDeadLettered.nest.orders.doc.items", 412L));
    }

    /**
     * And absent, not zero, for a namespace that discarded nothing. The distinction is load-bearing here in
     * a way it is not for the other readings: rows that never reach a document leave no other trace, so a
     * zero published every pass is exactly what this reading would look like if it stopped being wired.
     */
    @Test
    void aNamespaceThatDiscardedNothingReportsNothingRatherThanZero() {
        state.seed("orders", PipelineState.RUNNING);
        ObservationPublisher wired = new ObservationPublisher(state, observations,
                id -> OptionalLong.empty(), id -> Map.of(), id -> Map.of(), id -> Map.of(),
                id -> Map.of("nest.orders.doc.items", new NestStateReading(4_000L, 900L, 30L, 210L)));

        wired.publish("orders");

        assertThat(observations.read("orders").orElseThrow().metrics())
                .doesNotContainKey("nestDeadLettered.nest.orders.doc.items");
    }

    /**
     * What a large rebuild is doing, while it does it. One dimension row edited can owe a million rows of
     * writing, and for as long as that takes every other reading on this face says healthy - the state is
     * RUNNING, the error count is zero, the queues drain - while the target holds half the old value and
     * half the new one. Both numbers are asserted because either alone answers nothing: the rows sent have
     * nothing to be read against, and the rows there are say only that something big is happening.
     */
    @Test
    void publishWiresHowFarALargeRebuildHasGotAndHowFarItHasToGo() {
        state.seed("orders", PipelineState.RUNNING);
        String subject = "join.orders.widen.index.customers/17";
        ObservationPublisher wired = new ObservationPublisher(state, observations,
                id -> OptionalLong.empty(), id -> Map.of(), id -> Map.of(), id -> Map.of(), id -> Map.of(),
                new NestColdLayerWatch(NestColdLayerPressure.DEFAULT, NestColdLayerAlert.NONE),
                id -> Map.of(),
                new FrontierStallWatch(FrontierStallPressure.DEFAULT, FrontierStallAlert.NONE),
                id -> Map.of(),
                id -> Map.of(subject, 40_000L),
                id -> Map.of(subject, 1_000_000L));

        wired.publish("orders");

        assertThat(observations.read("orders").orElseThrow().metrics())
                .contains(entry("joinRecomputeRowsDone." + subject, 40_000L),
                        entry("joinRecomputeRowsExpected." + subject, 1_000_000L));
    }

    /**
     * And nothing at all where no rebuild worth reporting is running. This is the control the reading above
     * means nothing without: every dimension edit rebuilds something, so a publisher that put a pair here
     * on every pass would satisfy the case above and bury the one rebuild that mattered in a constant
     * stream - which is how a number meant for a rare, minutes-long wait comes to be scrolled past.
     */
    @Test
    void aPipelineWithNoLargeRebuildRunningReportsNeitherNumber() {
        state.seed("orders", PipelineState.RUNNING);
        ObservationPublisher wired = new ObservationPublisher(state, observations,
                id -> OptionalLong.empty(), id -> Map.of(), id -> Map.of(), id -> Map.of(), id -> Map.of(),
                new NestColdLayerWatch(NestColdLayerPressure.DEFAULT, NestColdLayerAlert.NONE),
                id -> Map.of(),
                new FrontierStallWatch(FrontierStallPressure.DEFAULT, FrontierStallAlert.NONE),
                id -> Map.of(), id -> Map.of(), id -> Map.of());

        wired.publish("orders");

        assertThat(observations.read("orders").orElseThrow().metrics().keySet())
                .noneMatch(name -> name.startsWith("joinRecompute"));
    }

    /**
     * The readings are fetched once per pass and the pass is the only place they exist together, so this is
     * where the watch that turns them into a window has to be fed from. Fetching them a second time for it
     * would pay for the cold layer's count twice a tick.
     */
    @Test
    void publishFeedsTheReadingsItPublishesToTheColdLayerWatch() {
        state.seed("orders", PipelineState.RUNNING);
        RecordingAlert alert = new RecordingAlert();
        ObservationPublisher wired = withWatch(alert,
                id -> Map.of("nest.orders.doc.$root",
                        new NestStateReading(100L, 4_000L, 3_800L, 19_000L, OptionalLong.of(400_000L))));

        wired.publish("orders");

        assertThat(alert.crossed).containsExactly("nest.orders.doc.$root");
    }

    /**
     * Each pass is one end of a window, which is the whole reason the watch is fed from here rather than
     * handed a reading out of context: the second pass has to be differenced against the first.
     */
    @Test
    void theWindowTheWatchJudgesIsBetweenTwoPassesAndNotTheRunningTotals() {
        state.seed("orders", PipelineState.RUNNING);
        RecordingAlert alert = new RecordingAlert();
        Map<String, NestStateReading> readings = new HashMap<>();
        readings.put("nest.orders.doc.$root",
                new NestStateReading(100L, 4_000L, 40L, 200L, OptionalLong.of(400_000L)));
        ObservationPublisher wired = withWatch(alert, id -> Map.copyOf(readings));

        wired.publish("orders");
        readings.put("nest.orders.doc.$root",
                new NestStateReading(100L, 5_000L, 990L, 4_750L, OptionalLong.of(400_000L)));
        wired.publish("orders");

        // The totals read 20% served from storage and the interval between the passes read 95%. Judging the
        // totals would have found nothing: a run healthy for most of its life drowns the hour it was not.
        assertThat(alert.crossed).containsExactly("nest.orders.doc.$root");
    }

    /**
     * The observation is the contract; the alert is a courtesy on top of it. Written the other way round a
     * fault in the alerting path would take the read face down with it, and the read face is what says the
     * pipeline is alive at all.
     */
    @Test
    void anAlertThatThrowsDoesNotCostThePipelineItsObservation() {
        state.seed("orders", PipelineState.RUNNING);
        NestColdLayerAlert throwing = new NestColdLayerAlert() {

            @Override
            public void crossed(String pipelineId, String namespace, NestStateWindow window) {
                throw new IllegalStateException("the alerting path is broken");
            }

            @Override
            public void cleared(String pipelineId, String namespace, NestStateWindow window) {
            }
        };
        ObservationPublisher wired = withWatch(throwing,
                id -> Map.of("nest.orders.doc.$root",
                        new NestStateReading(100L, 4_000L, 3_800L, 19_000L, OptionalLong.of(400_000L))));

        assertThatThrownBy(() -> wired.publish("orders")).isInstanceOf(IllegalStateException.class);

        assertThat(observations.read("orders")).isPresent();
    }

    @Test
    void theNestStateReadingsAreAbsentFromTheMetricsWhenNoNamespaceReportsAny() {
        state.seed("orders", PipelineState.RUNNING);
        ObservationPublisher wired = new ObservationPublisher(state, observations,
                id -> OptionalLong.empty(), id -> Map.of(), id -> Map.of(), id -> Map.of(), id -> Map.of());

        wired.publish("orders");

        // Absent means unmeasured. Zeroes would read as a state layer holding nothing and serving every
        // read from memory - the healthy end of both scales, and the reading an alarm stays quiet on.
        assertThat(observations.read("orders").orElseThrow().metrics()).containsOnly(entry("errorCount", 0L));
    }

    @Test
    void publishWiresThePerTableSinkAckedPositionsFromItsSource() {
        state.seed("orders", PipelineState.RUNNING);
        ObservationPublisher wired = new ObservationPublisher(
                state, observations, id -> OptionalLong.empty(), id -> Map.of("orders", "gtid:aaa-1:100"));

        wired.publish("orders");

        // The durable sink-acked source position rides the positions map, keyed by table, as a String.
        assertThat(observations.read("orders").orElseThrow().positions())
                .containsOnly(entry("orders", "gtid:aaa-1:100"));
    }

    @Test
    void publishesAnErrorCountOfOneWhenThePipelineHasFailed() {
        state.seed("orders", PipelineState.FAILED);

        publisher.publish("orders");

        Observation published = observations.read("orders").orElseThrow();
        assertThat(published.state()).isEqualTo(PipelineState.FAILED);
        // A dead data-plane job is one observable error; every other state reports zero.
        assertThat(published.metrics()).containsOnly(entry("errorCount", 1L));
    }

    @Test
    void errorCountDropsBackToZeroWhenAFailedPipelineRecovers() {
        state.seed("orders", PipelineState.FAILED);
        publisher.publish("orders");
        assertThat(observations.read("orders").orElseThrow().metrics()).containsOnly(entry("errorCount", 1L));

        // Recovery goes through STOPPED (stop -> start); the gauge tracks the current state, not a running
        // total, so a non-FAILED state reports zero rather than accumulating the earlier failure.
        state.seed("orders", PipelineState.STOPPED);
        publisher.publish("orders");

        assertThat(observations.read("orders").orElseThrow().metrics()).containsOnly(entry("errorCount", 0L));
    }

    @Test
    void publishWiresThePerTableSnapshotProgressFromItsSource() {
        state.seed("orders", PipelineState.RUNNING);
        ObservationPublisher wired = new ObservationPublisher(state, observations,
                id -> OptionalLong.empty(), id -> Map.of(),
                id -> Map.of("orders", new TableSnapshot(500L, null, null)));

        wired.publish("orders");

        // The snapshot dataset was published empty from the start -- the read face, its verb and its endpoint
        // were all reachable and always answered nothing. It now carries what its source reports.
        assertThat(observations.read("orders").orElseThrow().snapshot())
                .containsOnly(entry("orders", new TableSnapshot(500L, null, null)));
    }

    @Test
    void snapshotIsEmptyWhenItsSourceReportsNoTable() {
        state.seed("orders", PipelineState.RUNNING);

        publisher.publish("orders");

        // A publisher with no snapshot source publishes empty (unavailable), never a faked zero-row table.
        assertThat(observations.read("orders").orElseThrow().snapshot()).isEmpty();
    }

    @Test
    void publishCarriesTheCodedFailureOfAJobThatDied() {
        state.seed("orders", PipelineState.FAILED);

        publisher.publish("orders", new ObservationFailure(
                "engine.job-failed", Map.of("pipeline", "orders", "cause", "the sink rejected the batch")));

        Observation published = observations.read("orders").orElseThrow();
        // A dead job is observable as a state and a count, but neither says why. The failure carries the
        // canonical code and its named arguments, so the read face can answer that from the store alone.
        assertThat(published.failure()).isNotNull();
        assertThat(published.failure().code()).isEqualTo("engine.job-failed");
        assertThat(published.failure().params())
                .containsOnly(entry("pipeline", "orders"), entry("cause", "the sink rejected the batch"));
    }

    @Test
    void publishWithoutAFailureLeavesTheFailureUnset() {
        state.seed("orders", PipelineState.RUNNING);

        publisher.publish("orders");

        // A healthy pipeline has no failure to carry: absence, not an empty-string code.
        assertThat(observations.read("orders").orElseThrow().failure()).isNull();
    }

    @Test
    void republishWithoutACauseKeepsTheStoredFailureWhileThePipelineStaysFailed() {
        // The converge side reports the cause only on the pass that drives the transition; every later
        // pass publishes without one. The store is the only carrier that survives a process restart, so a
        // still-FAILED pipeline keeps the reason it already published rather than going reasonless the
        // moment its publisher loses whatever in-memory copy it held.
        state.seed("orders", PipelineState.FAILED);
        publisher.publish("orders", new ObservationFailure("engine.job-failed", Map.of("cause", "boom")));

        publisher.publish("orders", null);

        ObservationFailure kept = observations.read("orders").orElseThrow().failure();
        assertThat(kept).isNotNull();
        assertThat(kept.code()).isEqualTo("engine.job-failed");
        assertThat(kept.params()).containsOnly(entry("cause", "boom"));
    }

    @Test
    void republishClearsTheFailureWhenThePipelineRecovers() {
        state.seed("orders", PipelineState.FAILED);
        publisher.publish("orders", new ObservationFailure("engine.job-failed", Map.of("pipeline", "orders")));
        assertThat(observations.read("orders").orElseThrow().failure()).isNotNull();

        state.seed("orders", PipelineState.RUNNING);
        publisher.publish("orders");

        // The observation is current-state, not a history: a recovered pipeline must not keep answering with
        // the failure that killed its previous run.
        assertThat(observations.read("orders").orElseThrow().failure()).isNull();
    }

    @Test
    void publishOfAPipelineWithNoCheckpointWritesNothing() {
        publisher.publish("never-run");

        assertThat(observations.read("never-run")).isEmpty();
    }

    @Test
    void republishOverwritesTheLatestProjection() {
        state.seed("orders", PipelineState.RUNNING);
        publisher.publish("orders");

        state.seed("orders", PipelineState.PAUSED);
        publisher.publish("orders");

        assertThat(observations.read("orders").orElseThrow().state()).isEqualTo(PipelineState.PAUSED);
    }

    @Test
    void publishReconcileFailureRecordsTheCountAgainstNewWhenNothingHasBeenObservedYet() {
        publisher.publishReconcileFailure("orders", 3L);

        Observation published = observations.read("orders").orElseThrow();
        // A pipeline that never converged witnessed no lifecycle state, so the projection is NEW rather than a
        // fabricated FAILED; the consecutive-failure count is the observable error signal.
        assertThat(published.state()).isEqualTo(PipelineState.NEW);
        assertThat(published.metrics()).containsOnly(entry("errorCount", 3L));
        assertThat(published.snapshot()).isEmpty();
    }

    @Test
    void publishReconcileFailurePreservesTheLastObservedStateAndCarriesTheCount() {
        state.seed("orders", PipelineState.RUNNING);
        publisher.publish("orders"); // the last state actually observed is RUNNING

        publisher.publishReconcileFailure("orders", 2L);

        Observation published = observations.read("orders").orElseThrow();
        // The last observed state is kept, not overwritten with FAILED — only the error count moves.
        assertThat(published.state()).isEqualTo(PipelineState.RUNNING);
        assertThat(published.metrics()).containsOnly(entry("errorCount", 2L));
    }

    @Test
    void publishReconcileFailurePreservesThePreviouslyPublishedFailureAndPositions() {
        // A pass that could not run witnessed no transition in the failure reason or the source positions,
        // any more than it witnessed one in the state: a dead pipeline whose reconcile then starts throwing
        // must keep saying why it died and where each table's read had gotten to, not go blank on both.
        ObservationFailure priorFailure = new ObservationFailure("engine.job-failed", Map.of("cause", "boom"));
        Map<String, String> priorPositions = Map.of("orders", "binlog.000123:456");
        observations.save(new Observation(
                "orders", PipelineState.FAILED, Map.of("errorCount", 1L), Map.of(), priorPositions, priorFailure));

        publisher.publishReconcileFailure("orders", 5L);

        Observation published = observations.read("orders").orElseThrow();
        assertThat(published.state()).isEqualTo(PipelineState.FAILED);
        assertThat(published.metrics()).containsOnly(entry("errorCount", 5L));
        assertThat(published.failure()).isEqualTo(priorFailure);
        assertThat(published.positions()).isEqualTo(priorPositions);
    }

    /** In-memory state store double: seedable checkpoints, read-only for what the publisher needs. */
    private static final class MutableStateStore implements StateStore {
        @Override
        public void delete(String pipelineId) {
            throw new UnsupportedOperationException("removal is not exercised by this double");
        }


        private final Map<String, CheckpointDoc> docs = new HashMap<>();

        void seed(String pipelineId, PipelineState state) {
            docs.put(pipelineId, CheckpointDoc.initial(pipelineId, StateJson.of(state), T0));
        }

        @Override
        public Optional<CheckpointDoc> read(String pipelineId) {
            return Optional.ofNullable(docs.get(pipelineId));
        }

        @Override
        public void create(String pipelineId, String stateJson, Instant touchTime) {
            throw new UnsupportedOperationException("not exercised by the publisher");
        }

        @Override
        public CasOutcome compareAndSwap(String pipelineId, long expectedEpoch, String nextStateJson, Instant touchTime) {
            throw new UnsupportedOperationException("not exercised by the publisher");
        }
    }

    /** In-memory observation store double. */
    private static final class RecordingObservationStore implements ObservationStore {
        @Override
        public void delete(String pipelineId) {
            throw new UnsupportedOperationException("removal is not exercised by this double");
        }


        private final Map<String, Observation> docs = new HashMap<>();

        @Override
        public void save(Observation observation) {
            docs.put(observation.pipelineId(), observation);
        }

        @Override
        public Optional<Observation> read(String pipelineId) {
            return Optional.ofNullable(docs.get(pipelineId));
        }
    }
}
