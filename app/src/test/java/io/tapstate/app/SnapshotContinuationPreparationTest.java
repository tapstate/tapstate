package io.tapstate.app;

import io.tapstate.core.event.Envelope;
import io.tapstate.core.lifecycle.CaptureReading;
import io.tapstate.core.lifecycle.CheckpointDoc;
import io.tapstate.core.lifecycle.DeliveryReading;
import io.tapstate.core.lifecycle.DesiredState;
import io.tapstate.core.lifecycle.FrontierStallPressure;
import io.tapstate.core.lifecycle.MetricAttributes;
import io.tapstate.core.lifecycle.MetricPoint;
import io.tapstate.core.lifecycle.NestColdLayerPressure;
import io.tapstate.core.lifecycle.Observation;
import io.tapstate.core.lifecycle.PipelineState;
import io.tapstate.core.lifecycle.SinkBatchReading;
import io.tapstate.core.lifecycle.StageReading;
import io.tapstate.core.lifecycle.StageRuntimeReading;
import io.tapstate.core.lifecycle.StateJson;
import io.tapstate.core.model.FromRef;
import io.tapstate.core.model.PipelineResource;
import io.tapstate.core.model.ReadMode;
import io.tapstate.core.model.ServeBlock;
import io.tapstate.core.model.Settings;
import io.tapstate.core.model.SourceMode;
import io.tapstate.core.model.SourceRef;
import io.tapstate.core.model.SourceResource;
import io.tapstate.core.model.SyncElement;
import io.tapstate.core.model.TableRef;
import io.tapstate.runtime.scheduler.FrontierStallAlert;
import io.tapstate.runtime.scheduler.FrontierStallWatch;
import io.tapstate.runtime.scheduler.NestColdLayerAlert;
import io.tapstate.runtime.scheduler.NestColdLayerWatch;
import io.tapstate.runtime.scheduler.ObservationPublisher;
import io.tapstate.runtime.srs.CaptureHealth;
import io.tapstate.runtime.srs.CaptureRun;
import io.tapstate.runtime.srs.MiningChainId;
import io.tapstate.runtime.srs.SnapshotBuffer;
import io.tapstate.runtime.srs.SnapshotPhase;
import io.tapstate.runtime.srs.SrsCoordinator;
import io.tapstate.spi.store.ObservationStore;
import io.tapstate.spi.store.StopAuthority;
import io.tapstate.spi.store.StopReservation;
import org.junit.jupiter.api.Test;

import java.time.Clock;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalLong;

import static org.assertj.core.api.Assertions.assertThat;

/** Controlled capture reads feed the real coordinator, publisher and current-scope continuation registry. */
class SnapshotContinuationPreparationTest {
    private static final String PIPE = "partial_load";
    private static final String SOURCE = "source_rows";
    private static final ObservationStore.Scope OLD = new ObservationStore.Scope("inc-a", 17);
    private static final ObservationStore.Scope NEXT = new ObservationStore.Scope("inc-a", 18);
    private static final StopReservation.JobIdentity NEXT_JOB = new StopReservation.JobIdentity("single", 88, "next-boot");

    @Test
    void aCompletedTablesHistoricalProgressIsNotAddedAgainWhenAGenuineResumeContinuesCounters() {
        try (Fixture fixture = new Fixture()) {
            var resume = new DesiredState(PIPE, PipelineState.RUNNING, "rev-1");
            var policy = StopReservation.CounterPolicy.freeze(fixture.paused, resume);
            assertThat(policy).isEqualTo(StopReservation.CounterPolicy.CONTINUE);
            var sourceKey = new ObservationScopeRegistry.ContinuationKey("resume-token", OLD, policy,
                    StopAuthority.standalone("single", OLD.executionGeneration()));
            var sourceTicket = fixture.scopes.beginSourceContinuation(PIPE, sourceKey, () -> true).orElseThrow();
            var frozen = fixture.scopes.prepareSourceContinuation(sourceTicket, Optional.empty(), Optional.empty(),
                    ObservationScopeRegistry.SourceReadStatus.UNAVAILABLE).snapshot().orElseThrow();
            fixture.replaceCapture();
            fixture.scopes.begin(PIPE, NEXT.pipelineIncarnationId(), NEXT.executionGeneration());
            var targetKey = new ObservationScopeRegistry.ContinuationKey("resume-token", OLD, policy,
                    StopAuthority.standalone("single", NEXT.executionGeneration()));
            var target = new ObservationScopeRegistry.ActualTarget(NEXT, NEXT_JOB);
            var targetTicket = fixture.scopes.beginTargetContinuation(PIPE, targetKey, target, () -> true).orElseThrow();
            assertThat(fixture.scopes.adoptTargetContinuation(targetTicket, Optional.empty(), Optional.of(frozen)).status())
                    .isEqualTo(ObservationScopeRegistry.PreparationStatus.KNOWN);
            var raw = fixture.prepareReplacement();
            assertThat(raw.observation().snapshot().get("alpha").rowsDone()).isEqualTo(2);
            assertThat(snapshot(raw.observation(), "alpha").value()).isZero();
            assertThat(currentRun(raw.observation(), "alpha").value()).isZero();
            var continued = fixture.scopes.prepareContinuationPublication(raw, target, () -> true).orElseThrow().projected();
            MetricPoint alpha = snapshot(continued.observation(), "alpha");
            assertThat(alpha.value()).as("alpha was already read and confirmed; the replacement read none of it").isEqualTo(2);
            assertThat(alpha.startTime()).isEqualTo(fixture.originalAlpha.startTime());
            assertThat(fixture.scopes.current(PIPE)).contains(NEXT);
        }
    }

    @Test
    void aStampedStartUsesFreshSnapshotProgressWithoutAddingThePreviousExecution() {
        try (Fixture fixture = new Fixture()) {
            var start = new DesiredState(PIPE, PipelineState.RUNNING, "rev-1", false, null, true, fixture.paused.epoch());
            assertThat(StopReservation.CounterPolicy.freeze(fixture.paused, start)).isEqualTo(StopReservation.CounterPolicy.RESET);
            fixture.replaceCapture();
            fixture.scopes.beginResetExecution(PIPE, NEXT);
            var raw = fixture.prepareReplacement();
            var projected = fixture.scopes.prepareContinuationPublication(raw,
                    new ObservationScopeRegistry.ActualTarget(NEXT, NEXT_JOB), () -> true).orElseThrow().projected();
            MetricPoint alpha = snapshot(projected.observation(), "alpha");
            assertThat(alpha.value()).isEqualTo(snapshot(raw.observation(), "alpha").value());
            assertThat(projected.observation().snapshot().get("alpha").rowsDone()).isEqualTo(2);
            assertThat(alpha.startTime()).isEqualTo(snapshot(raw.observation(), "alpha").startTime())
                    .isAfter(fixture.originalAlpha.startTime());
            assertThat(currentRun(projected.observation(), "alpha").value()).isZero();
            assertThat(fixture.scopes.activeContinuationKey(PIPE)).isEmpty();
        }
    }

    private static MetricPoint snapshot(Observation observation, String table) {
        return point(observation, "tapstate.pipeline.snapshot.rows", table);
    }
    private static MetricPoint currentRun(Observation observation, String table) {
        return point(observation, "tapstate.pipeline.snapshot.rows.current_run", table);
    }
    private static MetricPoint point(Observation observation, String name, String table) {
        return observation.facts().stream().filter(fact -> fact.name().equals(name)).flatMap(fact -> fact.points().stream())
                .filter(point -> table.equals(point.attributes().get(MetricAttributes.TABLE_ID))).findFirst().orElseThrow();
    }

    private static final class Fixture implements AutoCloseable {
        private final InMemoryArtifactStore artifacts = new InMemoryArtifactStore();
        private final InMemoryStorePort store;
        private final ObservationScopeRegistry scopes = new ObservationScopeRegistry();
        private final Instant observedAt = Instant.now().plusSeconds(60);
        private final SrsCoordinator srs;
        private MiningChainId chain;
        private StoreBackedPipelineCaptureCoordinator capture;
        private final MetricPoint originalAlpha;
        private final CheckpointDoc paused;

        private Fixture() {
            artifacts.save(new SourceResource(SOURCE, null, "mysql", Map.of("host", "controlled"), SourceMode.CDC,
                    List.of(TableRef.literal("alpha"), TableRef.literal("beta")), null, null));
            artifacts.save(new PipelineResource(PIPE, null, List.of(SourceRef.spec(SOURCE, true)), null, null,
                    new ServeBlock.Inline(null, FromRef.literal(SOURCE), List.of(new SyncElement("sink", SOURCE, null, null, null)), null, null),
                    new Settings(null, null, null, null, ReadMode.SNAPSHOT_AND_CDC, "earliest"), null));
            store = new InMemoryStorePort(artifacts); srs = new SrsCoordinator(store.meta());
            store.state().create(PIPE, StateJson.of(PipelineState.RUNNING), observedAt);
            capture = coordinator(); capture.startCapture(PIPE);
            scopes.begin(PIPE, OLD.pipelineIncarnationId(), OLD.executionGeneration());
            var first = publisher(capture, observedAt).prepareScoped(PIPE, null, OLD).orElseThrow();
            originalAlpha = snapshot(scopes.continueFrame(first, OLD).observation(), "alpha");
            assertThat(originalAlpha.value()).isEqualTo(2);
            store.meta().markSnapshotComplete(chain.value(), PIPE, "alpha");
            assertThat(capture.loadDelivered(PIPE)).as("beta has not been confirmed by its target").isFalse();
            var running = store.state().read(PIPE).orElseThrow();
            store.state().compareAndSwap(PIPE, running.epoch(), StateJson.of(PipelineState.PAUSED), observedAt.plusSeconds(1));
            paused = store.state().read(PIPE).orElseThrow();
        }

        private StoreBackedPipelineCaptureCoordinator coordinator() {
            CaptureStarter starter = (spec, handoff) -> {
                chain = MiningChainId.resolve(spec.config(), spec.srsKey());
                srs.provisionSource(spec.sourceId(), chain, spec.config().streams(), spec.retention());
                srs.attachConsumer(chain, spec.pipelineId());
                List<String> owed = SnapshotPhase.stillOwed(store.meta().read(chain.value()), PIPE, spec.config().streams());
                Map<String, Long> actualReads = new LinkedHashMap<>();
                for (String table : spec.config().streams()) {
                    long rows = owed.contains(table) ? table.equals("alpha") ? 2 : 3 : 0;
                    for (long row = 1; row <= rows; row++) {
                        handoff.accept(Envelope.read(row, table, Map.of("id", row), Map.of()));
                    }
                    actualReads.put(table, rows); handoff.loaded(table);
                }
                return new CaptureRun(Optional.of(chain), false, actualReads.values().stream().mapToLong(Long::longValue).sum(),
                        actualReads, Optional.empty(), Optional.empty(), new CaptureHealth());
            };
            return new StoreBackedPipelineCaptureCoordinator(store, starter, srs, new SnapshotBuffer());
        }

        private void replaceCapture() {
            capture.stopCapture(PIPE, false);
            capture = coordinator(); capture.startCapture(PIPE);
            assertThat(capture.snapshotProgress(PIPE).byTable().get("alpha").rowsDone()).isEqualTo(2);
            assertThat(capture.runSnapshotProgress(PIPE).byTable().get("alpha").rowsDone()).isZero();
            assertThat(capture.loadDelivered(PIPE)).isFalse();
            store.state().compareAndSwap(PIPE, paused.epoch(), StateJson.of(PipelineState.RUNNING), observedAt.plusSeconds(2));
        }

        private ObservationPublisher.Prepared prepareReplacement() {
            return publisher(capture, observedAt.plusSeconds(3)).prepareScoped(PIPE, null, NEXT).orElseThrow();
        }

        private ObservationPublisher publisher(StoreBackedPipelineCaptureCoordinator coordinator, Instant at) {
            return new ObservationPublisher(store.state(), store.observations(), id -> OptionalLong.empty(), id -> Map.of(),
                    coordinator::snapshotProgress, id -> Map.of(), id -> Map.of(),
                    new NestColdLayerWatch(NestColdLayerPressure.DEFAULT, NestColdLayerAlert.NONE),
                    id -> Map.of(), new FrontierStallWatch(FrontierStallPressure.DEFAULT, FrontierStallAlert.NONE),
                    id -> Map.of(), id -> Map.of(), id -> Map.of(), id -> CaptureReading.NONE, id -> DeliveryReading.NONE,
                    id -> StageReading.NONE, id -> SinkBatchReading.NONE, id -> Optional.empty(), id -> Map.of(),
                    (ObservationPublisher.StageRuntimeFacts) id -> StageRuntimeReading.NONE,
                    coordinator::runSnapshotProgress, Clock.fixed(at, ZoneOffset.UTC));
        }

        @Override public void close() { capture.stopCapture(PIPE, false); }
    }
}
