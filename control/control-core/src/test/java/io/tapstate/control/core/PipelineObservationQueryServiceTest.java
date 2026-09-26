package io.tapstate.control.core;

import io.tapstate.core.common.TapstateException;
import io.tapstate.core.dsl.DslParser;
import io.tapstate.core.lifecycle.ExecutionPlan;
import io.tapstate.core.lifecycle.ExecutionPlans;
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
import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

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
    void statusCarriesThePlanThePipelinesCurrentRunWasSubmittedOn() {
        ExecutionPlan plan = new ExecutionPlan("orders_sync", 3L, 7L, 11L, List.of("m1", "m2"),
                List.of(new ExecutionPlan.Node("orders_sink", 4, "node-default", "native", 2, 2, 4, List.of(),
                        1024, 0L, List.of("orders_sink"))),
                Instant.parse("2026-09-26T10:00:00Z"));
        ReadCountingPlans plans = new ReadCountingPlans(Map.of("orders_sync", plan));
        var service = new PipelineObservationQueryService(artifactsWith("orders_sync"), storeWith(running()), plans);

        assertThat(service.status("orders_sync").plan()).isEqualTo(plan);
        assertThat(plans.reads).containsExactly(List.of("orders_sync"));
    }

    @Test
    void statusOfAPipelineWhoseRunHasNoPlanRecordedCarriesNone() {
        var service = new PipelineObservationQueryService(artifactsWith("orders_sync"), storeWith(running()),
                new ReadCountingPlans(Map.of()));

        assertThat(service.status("orders_sync").plan()).isNull();
    }

    @Test
    void aReaderFollowingTheStateAsItChangesIsAnsweredWithoutThePlanBeingRead() {
        ReadCountingPlans plans = new ReadCountingPlans(Map.of("orders_sync", new ExecutionPlan("orders_sync",
                null, null, null, List.of("local"), List.of(), Instant.parse("2026-09-26T10:00:00Z"))));
        var service = new PipelineObservationQueryService(artifactsWith("orders_sync"), storeWith(running()), plans);

        PipelineStatus polled = service.lifecycleStatus("orders_sync");
        Optional<PipelineStatus> listed = service.findStatus("orders_sync");

        // Both are read once per pipeline per poll or per page: a plan read there would be read and thrown away,
        // since it changes only when a new run is submitted.
        assertThat(polled.state()).isEqualTo(PipelineState.RUNNING);
        assertThat(polled.plan()).isNull();
        assertThat(listed).get().extracting(PipelineStatus::plan).isNull();
        assertThat(plans.reads).isEmpty();
    }

    /** Answers from a fixed set of plans, remembering every set of pipelines it was asked about. */
    private static final class ReadCountingPlans implements ExecutionPlans {

        private final Map<String, ExecutionPlan> plans;
        private final List<List<String>> reads = new ArrayList<>();

        ReadCountingPlans(Map<String, ExecutionPlan> plans) {
            this.plans = plans;
        }

        @Override
        public Map<String, ExecutionPlan> current(Collection<String> pipelineIds) {
            reads.add(List.copyOf(pipelineIds));
            Map<String, ExecutionPlan> found = new HashMap<>();
            pipelineIds.forEach(id -> {
                if (plans.containsKey(id)) {
                    found.put(id, plans.get(id));
                }
            });
            return found;
        }
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
