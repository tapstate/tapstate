package io.tapstate.control.core;

import io.tapstate.core.common.TapstateException;
import io.tapstate.core.dsl.DslParser;
import io.tapstate.core.lifecycle.LifecycleError;
import io.tapstate.core.lifecycle.MetricFact;
import io.tapstate.core.lifecycle.MetricPoint;
import io.tapstate.core.lifecycle.MetricType;
import io.tapstate.core.lifecycle.Observation;
import io.tapstate.core.lifecycle.ObservationFailure;
import io.tapstate.core.lifecycle.PipelineState;
import io.tapstate.core.lifecycle.TableSnapshot;
import io.tapstate.core.model.Resource;
import io.tapstate.spi.store.ArtifactStore;
import io.tapstate.spi.store.ObservationStore;
import io.tapstate.spi.store.ExecutionGenerationStore;
import io.tapstate.spi.store.WorkloadClaim;
import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.assertj.core.api.Assertions.catchThrowableOfType;

/**
 * The pipeline observation read side: the three store-backed read faces (status / metrics / snapshot)
 * each project the latest published observation for a pipeline, and a read of a pipeline that has
 * published no observation is a coded {@code monitor.no-observation} diagnostic — never a bare crash,
 * so the same read serves a frontend with no stderr/exit channel.
 */
class PipelineObservationQueryServiceTest {

    @Test
    void oldExecutionAndOldIncarnationRemainPendingUntilTheCurrentObservationArrives() {
        Resource pipeline = new DslParser().parse("""
                version: tapstate/v1
                kind: pipeline
                id: orders_sync
                source: src_x
                serve:
                  from: /.*/
                  sync:
                    - id: sink1
                      source: tgt_x
                      write_mode: upsert
                      ddl: apply
                """);
        AtomicBoolean exists = new AtomicBoolean(true);
        AtomicReference<Optional<String>> incarnation = new AtomicReference<>(Optional.of("inc-current"));
        ArtifactStore artifacts = new ArtifactStore() {
            @Override public void saveAll(List<Resource> resources) { throw new UnsupportedOperationException(); }
            @Override public Optional<Resource> get(String id) {
                return exists.get() && id.equals("orders_sync") ? Optional.of(pipeline) : Optional.empty();
            }
            @Override public List<Resource> list() { return exists.get() ? List.of(pipeline) : List.of(); }
            @Override public Optional<String> pipelineIncarnationId(String id) { return incarnation.get(); }
        };
        ExecutionGenerationStore generations = new ExecutionGenerationStore() {
            @Override public Optional<WorkloadClaim> advanceUnderClaim(WorkloadClaim expected, long revision) {
                throw new UnsupportedOperationException();
            }
            @Override public OptionalLong advanceStandalone(String clusterId, String id) {
                throw new UnsupportedOperationException();
            }
            @Override public OptionalLong currentGeneration(String clusterId, String id) {
                assertThat(clusterId).isEqualTo("cluster-a");
                return OptionalLong.of(42);
            }
        };
        Observation observed = running();
        AtomicReference<ObservationStore.Stored> stored = new AtomicReference<>(
                new ObservationStore.Stored(observed, Optional.of(new ObservationStore.Scope("inc-current", 41))));
        ObservationStore latest = new ObservationStore() {
            @Override public void save(Observation observation) { throw new UnsupportedOperationException(); }
            @Override public Optional<Observation> read(String id) { return Optional.of(stored.get().observation()); }
            @Override public Optional<Stored> readStored(String id) { return Optional.of(stored.get()); }
            @Override public void delete(String id) { throw new UnsupportedOperationException(); }
        };
        CurrentObservationReader current = new CurrentObservationReader(artifacts, generations, latest, "cluster-a");
        PipelineObservationQueryService service = new PipelineObservationQueryService(
                new ArtifactQueryService(artifacts), current);

        assertThat(service.findStatus("orders_sync")).isEmpty();
        assertThatThrownBy(() -> service.status("orders_sync"))
                .isInstanceOfSatisfying(TapstateException.class,
                        failure -> assertThat(failure.code()).isEqualTo(MonitorError.NO_OBSERVATION));
        stored.set(new ObservationStore.Stored(observed,
                Optional.of(new ObservationStore.Scope("inc-old", 42))));
        assertThat(service.findStatus("orders_sync")).isEmpty();
        stored.set(new ObservationStore.Stored(observed,
                Optional.of(new ObservationStore.Scope("inc-current", 42))));
        assertThat(service.status("orders_sync").state()).isEqualTo(PipelineState.RUNNING);
        stored.set(new ObservationStore.Stored(observed, Optional.empty()));
        assertThat(service.findStatus("orders_sync")).isEmpty();
        incarnation.set(Optional.empty());
        assertThat(service.status("orders_sync").state()).isEqualTo(PipelineState.RUNNING);
        exists.set(false);
        assertThat(service.findStatus("orders_sync")).isEmpty();
    }

    private static ObservationStore storeWith(Observation... published) {
        Map<String, Observation> map = new HashMap<>();
        for (Observation o : published) {
            map.put(o.pipelineId(), o);
        }
        return new ObservationStore() {
            @Override
            public void save(Observation observation) {
                map.put(observation.pipelineId(), observation);
            }

            @Override
            public Optional<Observation> read(String pipelineId) {
                return Optional.ofNullable(map.get(pipelineId));
            }

            @Override
            public void delete(String pipelineId) {
                map.remove(pipelineId);
            }
        };
    }

    /** An artifact query over an in-memory store holding a pipeline resource for each given id. */
    private static ArtifactQueryService artifactsWith(String... pipelineIds) {
        Map<String, Resource> byId = new HashMap<>();
        for (String id : pipelineIds) {
            byId.put(id, new DslParser().parse("""
                    version: tapstate/v1
                    kind: pipeline
                    id: %s
                    source: src_x
                    serve:
                      from: /.*/
                      sync:
                        - id: sink1
                          source: tgt_x
                          write_mode: upsert
                          ddl: apply
                    """.formatted(id)));
        }
        return new ArtifactQueryService(new ArtifactStore() {
            @Override
            public void saveAll(List<Resource> artifacts) {
                artifacts.forEach(r -> byId.put(r.id(), r));
            }

            @Override
            public Optional<Resource> get(String id) {
                return Optional.ofNullable(byId.get(id));
            }

            @Override
            public List<Resource> list() {
                return List.copyOf(byId.values());
            }
        });
    }

    /** An artifact query over an in-memory store holding one non-pipeline (source) resource. */
    private static ArtifactQueryService artifactsWithASource(String sourceId) {
        Map<String, Resource> byId = new HashMap<>();
        byId.put(sourceId, new DslParser().parse("""
                version: tapstate/v1
                kind: source
                id: %s
                connector: mysql
                mode: cdc
                tables: [ orders ]
                """.formatted(sourceId)));
        return new ArtifactQueryService(new ArtifactStore() {
            @Override
            public void saveAll(List<Resource> artifacts) {
                artifacts.forEach(r -> byId.put(r.id(), r));
            }

            @Override
            public Optional<Resource> get(String id) {
                return Optional.ofNullable(byId.get(id));
            }

            @Override
            public List<Resource> list() {
                return List.copyOf(byId.values());
            }
        });
    }

    private static Observation running() {
        return new Observation("orders_sync", PipelineState.RUNNING,
                Map.of("recordCount", 5L), Map.of("orders", new TableSnapshot(10L, 20L, 50)));
    }

    private static final Instant TAKEN = Instant.parse("2026-09-17T10:00:00Z");

    private static final MetricFact RECORDS_MEASURED = new MetricFact("tapstate.pipeline.records",
            MetricType.COUNTER, "{record}", List.of(MetricPoint.accumulated(
                    Map.of("tapstate.pipeline.id", "orders_sync", "direction", "in"), TAKEN, TAKEN, 100L)));

    private static Observation runningWithFacts() {
        return new Observation("orders_sync", PipelineState.RUNNING,
                Map.of("records.in", 100L), Map.of(), Map.of(), null, TAKEN, List.of(RECORDS_MEASURED));
    }

    private static Observation runningWithPositions() {
        return new Observation("orders_sync", PipelineState.RUNNING,
                Map.of("recordCount", 5L), Map.of("orders", new TableSnapshot(10L, 20L, 50)),
                Map.of("orders", "w7"));
    }

    @Test
    void statusProjectsThePublishedState() {
        var service = new PipelineObservationQueryService(artifactsWith("orders_sync"), storeWith(running()));

        PipelineStatus status = service.status("orders_sync");

        assertThat(status.pipelineId()).isEqualTo("orders_sync");
        assertThat(status.state()).isEqualTo(PipelineState.RUNNING);
    }

    @Test
    void statusOfAFailedPipelineCarriesTheCodedReasonItDied() {
        // FAILED on its own tells the user something broke but not what: the reason is published with the
        // state, so the status face answers it from the store rather than sending the user to the logs.
        Observation dead = new Observation("orders_sync", PipelineState.FAILED, Map.of("errorCount", 1L),
                Map.of(), Map.of(), new ObservationFailure("engine.job-failed",
                        Map.of("pipeline", "orders_sync", "cause", "sink refused the batch")));
        var service = new PipelineObservationQueryService(artifactsWith("orders_sync"), storeWith(dead));

        PipelineStatus status = service.status("orders_sync");

        assertThat(status.state()).isEqualTo(PipelineState.FAILED);
        assertThat(status.failure()).isNotNull();
        assertThat(status.failure().code()).isEqualTo("engine.job-failed");
        assertThat(status.failure().params()).containsEntry("cause", "sink refused the batch");
    }

    @Test
    void statusOfAHealthyPipelineCarriesNoFailure() {
        var service = new PipelineObservationQueryService(artifactsWith("orders_sync"), storeWith(running()));

        assertThat(service.status("orders_sync").failure()).isNull();
    }

    @Test
    void metricsProjectsThePublishedMetricMap() {
        var service = new PipelineObservationQueryService(artifactsWith("orders_sync"), storeWith(running()));

        assertThat(service.metrics("orders_sync").metrics()).containsEntry("recordCount", 5L);
    }

    @Test
    void metricsProjectsThePublishedFactsBesideTheFlatMap() {
        var service = new PipelineObservationQueryService(artifactsWith("orders_sync"), storeWith(runningWithFacts()));

        // The facts are carried through as the store holds them; this face adds nothing and drops nothing,
        // so what the wire renders is what the runtime measured.
        assertThat(service.metrics("orders_sync").facts()).containsExactly(RECORDS_MEASURED);
        assertThat(service.metrics("orders_sync").metrics()).containsEntry("records.in", 100L);
    }

    @Test
    void metricsFactsAreEmptyWhenTheObservationCarriesNone() {
        var service = new PipelineObservationQueryService(artifactsWith("orders_sync"), storeWith(running()));

        assertThat(service.metrics("orders_sync").facts()).isEmpty();
    }

    @Test
    void metricsProjectsThePublishedPositionsAsTheTargetAckedOnes() {
        var service = new PipelineObservationQueryService(artifactsWith("orders_sync"), storeWith(runningWithPositions()));

        // The stored projection calls them positions; this face calls them what they are, because it is
        // the face somebody reads to decide whether a run is stuck.
        assertThat(service.metrics("orders_sync").targetAckedPosition()).containsEntry("orders", "w7");
    }

    @Test
    void metricsPositionsAreEmptyWhenTheObservationHasNone() {
        var service = new PipelineObservationQueryService(artifactsWith("orders_sync"), storeWith(running()));

        assertThat(service.metrics("orders_sync").targetAckedPosition()).isEmpty();
    }

    @Test
    void theMetricsFaceNamesThePositionsItDoesNotRecord() {
        // Empty here would say this face records every position there is and simply has none of them,
        // which is the reading that turns a stalled target into an idle source.
        assertThat(PipelineMetrics.POSITIONS_NOT_COLLECTED)
                .containsExactly("sourceHeadPosition", "processedPosition");
    }

    @Test
    void snapshotProjectsThePublishedSnapshotMap() {
        var service = new PipelineObservationQueryService(artifactsWith("orders_sync"), storeWith(running()));

        assertThat(service.snapshot("orders_sync").snapshot().get("orders").rowsDone()).isEqualTo(10L);
    }

    @Test
    void statusOfAnAppliedPipelineThatHasNotConvergedYetIsNoObservationCoded() {
        // Applied but unobserved: the window between recording an intent and the first convergence pass.
        // This is transient and a caller is entitled to wait it out, so it keeps the no-observation code.
        var service = new PipelineObservationQueryService(artifactsWith("pl1"), storeWith());

        TapstateException thrown = catchThrowableOfType(
                () -> service.status("pl1"), TapstateException.class);

        assertThat(thrown.code()).isEqualTo(MonitorError.NO_OBSERVATION);
        assertThat(thrown.args()).containsEntry("pipeline", "pl1");
    }

    @Test
    void statusOfAPipelineThatWasNeverAppliedIsUnknownPipelineCoded() {
        // Never applied: permanent, and it must not read as the transient window above. Answering the same
        // code for both left a caller waiting out its whole bound on a mistyped id, then blaming the data.
        var service = new PipelineObservationQueryService(artifactsWith(), storeWith());

        TapstateException thrown = catchThrowableOfType(
                () -> service.status("ghost"), TapstateException.class);

        assertThat(thrown.code()).isEqualTo(LifecycleError.UNKNOWN_PIPELINE);
        assertThat(thrown.args()).containsEntry("pipeline", "ghost");
    }

    @Test
    void statusOfANonPipelineArtifactIsUnknownPipelineCoded() {
        // The id resolves to something, but not to a pipeline: a lifecycle verb has nothing to converge
        // and never will, exactly like an id that resolves to nothing at all. Answering the transient
        // no-observation code here would send a caller waiting out its bound on a source id.
        var service = new PipelineObservationQueryService(artifactsWithASource("src_orders"), storeWith());

        TapstateException thrown = catchThrowableOfType(
                () -> service.status("src_orders"), TapstateException.class);

        assertThat(thrown.code()).isEqualTo(LifecycleError.UNKNOWN_PIPELINE);
        assertThat(thrown.args()).containsEntry("pipeline", "src_orders");
    }

    @Test
    void metricsAndSnapshotTellTheSameTwoApartAsStatusDoes() {
        var service = new PipelineObservationQueryService(artifactsWith("pl1"), storeWith());

        assertThatThrownBy(() -> service.metrics("ghost"))
                .isInstanceOfSatisfying(TapstateException.class,
                        e -> assertThat(e.code()).isEqualTo(LifecycleError.UNKNOWN_PIPELINE));
        assertThatThrownBy(() -> service.snapshot("pl1"))
                .isInstanceOfSatisfying(TapstateException.class,
                        e -> assertThat(e.code()).isEqualTo(MonitorError.NO_OBSERVATION));
    }

    @Test
    void metricsOfAnAppliedPipelineWithNoObservationIsNoObservationCoded() {
        var service = new PipelineObservationQueryService(artifactsWith("orders_sync"), storeWith());

        assertThatThrownBy(() -> service.metrics("orders_sync"))
                .isInstanceOfSatisfying(TapstateException.class,
                        e -> assertThat(e.code()).isEqualTo(MonitorError.NO_OBSERVATION));
    }

    @Test
    void snapshotOfAnAppliedPipelineWithNoObservationIsNoObservationCoded() {
        var service = new PipelineObservationQueryService(artifactsWith("orders_sync"), storeWith());

        assertThatThrownBy(() -> service.snapshot("orders_sync"))
                .isInstanceOfSatisfying(TapstateException.class,
                        e -> assertThat(e.code()).isEqualTo(MonitorError.NO_OBSERVATION));
    }
}
