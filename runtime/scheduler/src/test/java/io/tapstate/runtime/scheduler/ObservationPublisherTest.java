package io.tapstate.runtime.scheduler;

import io.tapstate.core.lifecycle.CasOutcome;
import io.tapstate.core.lifecycle.CheckpointDoc;
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
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalLong;

import static org.assertj.core.api.Assertions.assertThat;
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

        // Four numbers per namespace rather than the one ratio they imply: a ratio published here would be
        // an average over the whole run, and a state layer that fell off its cliff a minute ago still reads
        // as healthy in it. Two scrapes of counts give any window a reader wants.
        assertThat(observations.read("orders").orElseThrow().metrics())
                .containsOnly(entry("errorCount", 0L),
                        entry("nestStateEntries.nest.orders.doc.$root", 4_000L),
                        entry("nestStateAccesses.nest.orders.doc.$root", 900L),
                        entry("nestStateBackfills.nest.orders.doc.$root", 30L),
                        entry("nestStateBackfillMillis.nest.orders.doc.$root", 210L));
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
