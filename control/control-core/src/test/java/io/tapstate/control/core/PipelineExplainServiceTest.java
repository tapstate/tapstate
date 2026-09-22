package io.tapstate.control.core;

import io.tapstate.control.core.PipelineExplanation.Evidence;
import io.tapstate.control.core.PipelineExplanation.Failure;
import io.tapstate.control.core.PipelineExplanation.Freshness;
import io.tapstate.control.core.PipelineExplanation.Kind;
import io.tapstate.control.core.PipelineExplanation.NextAction;
import io.tapstate.control.core.PipelineExplanation.Source;
import io.tapstate.core.common.TapstateException;
import io.tapstate.core.dsl.DslParser;
import io.tapstate.core.lifecycle.LifecycleError;
import io.tapstate.core.lifecycle.Observation;
import io.tapstate.core.lifecycle.ObservationFailure;
import io.tapstate.core.lifecycle.PipelineState;
import io.tapstate.core.lifecycle.TableSnapshot;
import io.tapstate.core.model.Resource;
import io.tapstate.spi.store.ArtifactStore;
import io.tapstate.spi.store.ObservationStore;
import org.junit.jupiter.api.Test;

import java.time.Clock;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.catchThrowableOfType;

class PipelineExplainServiceTest {

    private static final String ID = "orders";
    private static final Instant NOW = Instant.parse("2026-09-21T12:00:00Z");

    @Test
    void staleObservationWinsWhenEveryRuleMatchesAndDoesNotInventAState() {
        Observation observation = new Observation(ID, PipelineState.RUNNING,
                Map.of("reconcileFailuresInARow", 7L, "recordCount", 0L,
                        "frontierStalledMillis.orders", 96_000L),
                Map.of("orders", new TableSnapshot(0, null, null)), Map.of(),
                new ObservationFailure("engine.job-failed", Map.of()), NOW.minusSeconds(45));

        PipelineExplanation answer = service(observation).explain(ID);

        assertThat(answer.kind()).isEqualTo(Kind.OBSERVATION_STALE);
        assertThat(answer.state()).isEqualTo(PipelineState.RUNNING);
        assertThat(answer.freshness()).isEqualTo(Freshness.STALE);
        assertThat(answer.observedAgeMillis()).isEqualTo(45_000L);
        assertThat(answer.evidence()).containsExactly(
                new Evidence(Source.STATUS, "observedAgeMillis", 45_000L));
        assertThat(answer.cannotSay()).containsExactly("explain.cannot-current-now");
        assertThat(answer.next().action()).isEqualTo(NextAction.CHECK_SERVER);
    }

    @Test
    void codedFailurePreservesItsCodeParamsAndSharedRenderedMessage() {
        Observation observation = new Observation(ID, PipelineState.FAILED, Map.of(), Map.of(), Map.of(),
                new ObservationFailure("engine.job-failed", Map.of("pipeline", ID)), NOW.minusSeconds(2));

        PipelineExplanation answer = service(observation).explain(ID);

        assertThat(answer.kind()).isEqualTo(Kind.CODED_FAILURE);
        assertThat(answer.evidence()).singleElement().satisfies(evidence -> {
            assertThat(evidence.source()).isEqualTo(Source.STATUS);
            assertThat(evidence.field()).isEqualTo("failure");
            assertThat(evidence.value()).isEqualTo(
                    new Failure("engine.job-failed", Map.of("pipeline", ID), "rendered failure"));
        });
        assertThat(answer.next().action()).isEqualTo(NextAction.OPEN_PIPELINE_LOGS);
    }

    @Test
    void reconcileFailuresCarryTheStreakAndOriginalStateAsTypedEvidence() {
        Observation observation = observation(PipelineState.NEW,
                Map.of("reconcileFailuresInARow", 3L, "recordCount", 128L), 1, NOW.minusSeconds(2));

        PipelineExplanation answer = service(observation).explain(ID);

        assertThat(answer.kind()).isEqualTo(Kind.RECONCILE_FAILURES);
        assertThat(answer.evidence()).containsExactly(
                new Evidence(Source.METRICS, "reconcileFailuresInARow", 3L),
                new Evidence(Source.STATUS, "state", PipelineState.NEW));
        assertThat(answer.cannotSay()).containsExactly("explain.cannot-job-alive");
    }

    @Test
    void noMovementKeepsAnAbsentRecordCountDistinctFromZeroRows() {
        Observation observation = observation(PipelineState.RUNNING, Map.of(), 0, NOW.minusSeconds(2));

        PipelineExplanation answer = service(observation).explain(ID);

        assertThat(answer.kind()).isEqualTo(Kind.NO_MOVEMENT);
        assertThat(answer.evidence()).containsExactly(
                new Evidence(Source.METRICS, "recordCount", null),
                new Evidence(Source.SNAPSHOT, "rowsDone", 0L));
        assertThat(answer.cannotSay()).containsExactly("explain.cannot-source-head");
    }

    @Test
    void stalledChainsAreEvidenceInDictionaryOrder() {
        Map<String, Long> metrics = new LinkedHashMap<>();
        metrics.put("recordCount", 128L);
        metrics.put("frontierStalledMillis.zeta", 96_000L);
        metrics.put("frontierStalledMillis.alpha", 60_000L);
        Observation observation = observation(PipelineState.RUNNING, metrics, 1, NOW.minusSeconds(2));

        PipelineExplanation answer = service(observation).explain(ID);

        assertThat(answer.kind()).isEqualTo(Kind.FRONTIER_STALLED);
        assertThat(answer.evidence()).extracting(Evidence::field)
                .containsExactly("frontierStalledMillis.alpha", "frontierStalledMillis.zeta");
        assertThat(answer.next().action()).isEqualTo(NextAction.CHECK_TARGET);
    }

    @Test
    void subThresholdPausesRemainTypedEvidenceWithoutAStoppedChainDiagnosis() {
        Observation observation = observation(PipelineState.RUNNING, Map.of(
                "reconcileFailuresInARow", 0L,
                "recordCount", 11L,
                "frontierStalledMillis.shipments", 9L,
                "frontierStalledMillis.orders", 196L), 11, NOW.minusSeconds(2));

        PipelineExplanation answer = service(observation).explain(ID);

        assertThat(answer.kind()).isEqualTo(Kind.NO_MATCH);
        assertThat(answer.evidence())
                .filteredOn(evidence -> evidence.field().equals("frontierStalledMillis"))
                .containsExactly(new Evidence(Source.METRICS, "frontierStalledMillis", Map.of(
                        "orders", 196L,
                        "shipments", 9L)));
        assertThat(answer.next()).isNull();
    }

    @Test
    void noMatchIsUnknownRatherThanHealthyAndNamesEveryStructuralLimit() {
        Observation observation = observation(PipelineState.PAUSED, Map.of("recordCount", 128L), 1, null);

        PipelineExplanation answer = service(observation).explain(ID);

        assertThat(answer.kind()).isEqualTo(Kind.NO_MATCH);
        assertThat(answer.message()).isEqualTo("explain.no-match");
        assertThat(answer.freshness()).isEqualTo(Freshness.UNKNOWN);
        assertThat(answer.observedAt()).isNull();
        assertThat(answer.observedAgeMillis()).isNull();
        assertThat(answer.cannotSay()).containsExactly(
                "explain.cannot-age",
                "explain.cannot-source-head",
                "explain.cannot-paused-job",
                "explain.cannot-snapshot-phase");
        assertThat(answer.next()).isNull();
        assertThat(answer.evidence()).extracting(Evidence::field).containsExactly(
                "observedAgeMillis", "failure", "reconcileFailuresInARow", "recordCount",
                "frontierStalledMillis", "rowsDone");
    }

    @Test
    void aFutureObservationAgeIsClampedToZero() {
        Observation observation = observation(PipelineState.RUNNING, Map.of("recordCount", 128L),
                1, NOW.plusSeconds(5));

        PipelineExplanation answer = service(observation).explain(ID);

        assertThat(answer.observedAgeMillis()).isZero();
        assertThat(answer.freshness()).isEqualTo(Freshness.FRESH);
    }

    @Test
    void missingObservationDistinguishesPendingPipelineFromUnknownId() {
        PipelineExplainService pending = serviceWithArtifacts(observations(), ID);
        TapstateException noObservation = catchThrowableOfType(() -> pending.explain(ID), TapstateException.class);
        TapstateException unknown = catchThrowableOfType(
                () -> serviceWithArtifacts(observations()).explain(ID), TapstateException.class);

        assertThat(noObservation.code()).isEqualTo(MonitorError.NO_OBSERVATION);
        assertThat(unknown.code()).isEqualTo(LifecycleError.UNKNOWN_PIPELINE);
    }

    private static PipelineExplainService service(Observation observation) {
        return serviceWithArtifacts(observations(observation), ID);
    }

    private static PipelineExplainService serviceWithArtifacts(ObservationStore observations, String... ids) {
        ExplanationMessages messages = (key, args) ->
                "engine.job-failed".equals(key) ? "rendered failure" : key;
        return new PipelineExplainService(artifacts(ids), observations,
                Clock.fixed(NOW, ZoneOffset.UTC), messages);
    }

    private static Observation observation(PipelineState state, Map<String, Long> metrics,
            long rowsDone, Instant observedAt) {
        return new Observation(ID, state, metrics,
                Map.of("orders", new TableSnapshot(rowsDone, null, null)), Map.of(), null, observedAt);
    }

    private static ObservationStore observations(Observation... values) {
        Map<String, Observation> byId = new LinkedHashMap<>();
        for (Observation value : values) {
            byId.put(value.pipelineId(), value);
        }
        return new ObservationStore() {
            @Override
            public void save(Observation observation) {
                byId.put(observation.pipelineId(), observation);
            }

            @Override
            public Optional<Observation> read(String pipelineId) {
                return Optional.ofNullable(byId.get(pipelineId));
            }

            @Override
            public void delete(String pipelineId) {
                byId.remove(pipelineId);
            }
        };
    }

    private static ArtifactQueryService artifacts(String... ids) {
        Map<String, Resource> resources = new LinkedHashMap<>();
        for (String id : ids) {
            resources.put(id, new DslParser().parse("""
                    version: tapstate/v1
                    kind: pipeline
                    id: %s
                    source: source
                    serve:
                      from: /.*/
                      sync:
                        - id: sink
                          source: target
                          write_mode: upsert
                          ddl: apply
                    """.formatted(id)));
        }
        return new ArtifactQueryService(new ArtifactStore() {
            @Override
            public void saveAll(List<Resource> artifacts) {
                artifacts.forEach(artifact -> resources.put(artifact.id(), artifact));
            }

            @Override
            public Optional<Resource> get(String id) {
                return Optional.ofNullable(resources.get(id));
            }

            @Override
            public List<Resource> list() {
                return List.copyOf(resources.values());
            }
        });
    }
}
