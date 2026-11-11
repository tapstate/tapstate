package io.tapstate.control.core;

import io.tapstate.control.core.PipelineExplanation.Evidence;
import io.tapstate.control.core.PipelineExplanation.Failure;
import io.tapstate.control.core.PipelineExplanation.Freshness;
import io.tapstate.control.core.PipelineExplanation.Kind;
import io.tapstate.control.core.PipelineExplanation.NextAction;
import io.tapstate.control.core.PipelineExplanation.Source;
import io.tapstate.core.common.TapstateException;
import io.tapstate.core.dsl.DslParser;
import io.tapstate.core.lifecycle.ExecutionPlan;
import io.tapstate.core.lifecycle.InitialLoad;
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

    /**
     * An absent record count and one standing at nought are not two spellings of one state. The count
     * is absent when there is no live job to read it from, and present at nought when a job is live
     * and has driven nothing through it. Both have moved nothing and both belong here, but the second
     * is the alarming one -- something is running and carrying nothing -- and it is the one a real
     * cluster reports. The case above covers the absent reading; this covers the live one, which
     * otherwise falls to "no rule matched": nothing driven, nothing loaded, and no conclusion drawn.
     *
     * <p>The nought has to survive into the evidence as a nought rather than a null, because the
     * evidence is what the reader is shown, and those two values are the difference between a job
     * that is running and one that is not there.
     */
    @Test
    void noMovementAlsoAnswersARecordCountPublishedAsNought() {
        Observation observation = observation(PipelineState.RUNNING, Map.of("recordCount", 0L), 0,
                NOW.minusSeconds(2));

        PipelineExplanation answer = service(observation).explain(ID);

        assertThat(answer.kind()).isEqualTo(Kind.NO_MOVEMENT);
        assertThat(answer.evidence()).containsExactly(
                new Evidence(Source.METRICS, "recordCount", 0L),
                new Evidence(Source.SNAPSHOT, "rowsDone", 0L));
        assertThat(answer.next().action()).isEqualTo(NextAction.OPEN_PIPELINE_LOGS);
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

    /**
     * A chain whose table is still in its initial load has no durable position to advance yet: its pinned time
     * grows for as long as the load runs. Reported as stopped, a long load reads as a broken target.
     */
    @Test
    void aChainWhoseTableIsStillLoadingIsNotDiagnosedAsStalled() {
        Observation observation = loading(Map.of(
                "recordCount", 100L,
                "frontierStalledMillis.order_line", 1_127_598L,
                "frontierStalledMillis.customer", 0L), Map.of(
                "order_line", new InitialLoad.Table(InitialLoad.Stage.READING, 50_344L, 100_280L),
                "customer", new InitialLoad.Table(InitialLoad.Stage.LANDED, 10_000L, 10_000L)));

        PipelineExplanation answer = service(observation).explain(ID);

        assertThat(answer.kind()).isEqualTo(Kind.NO_MATCH);
        assertThat(answer.evidence()).contains(new Evidence(Source.SNAPSHOT, "initialLoad",
                Map.of(
                        "order_line", new InitialLoad.Table(InitialLoad.Stage.READING, 50_344L, 100_280L),
                        "customer", new InitialLoad.Table(InitialLoad.Stage.LANDED, 10_000L, 10_000L))));
        assertThat(answer.cannotSay()).doesNotContain("explain.cannot-snapshot-phase");
    }

    /**
     * A copy publishes per-table landed flags without an initial-load reading. A table read through and not yet
     * landed is still loading, so its pinned time is no more a stall than a table the reading names as loading.
     */
    @Test
    void withoutAnInitialLoadReadingATableNotYetLandedIsNotDiagnosedAsStalled() {
        Observation observation = new Observation(ID, PipelineState.RUNNING, Map.of(
                "recordCount", 100L,
                "frontierStalledMillis.order_line", 248_780L,
                "frontierStalledMillis.customer", 96_000L), Map.of(
                "order_line", new TableSnapshot(100_280L, null, null, false),
                "customer", new TableSnapshot(10_000L, 10_000L, 100, true)), Map.of(), null, NOW.minusSeconds(2));

        PipelineExplanation answer = service(observation).explain(ID);

        assertThat(answer.kind()).isEqualTo(Kind.FRONTIER_STALLED);
        assertThat(answer.evidence()).extracting(Evidence::field)
                .containsExactly("frontierStalledMillis.customer");
    }

    @Test
    void aChainWhoseTableHasLandedIsStillDiagnosedAsStalled() {
        Observation observation = loading(Map.of(
                "recordCount", 100L,
                "frontierStalledMillis.order_line", 1_127_598L), Map.of(
                "order_line", new InitialLoad.Table(InitialLoad.Stage.LANDED, 100_280L, 100_280L)));

        PipelineExplanation answer = service(observation).explain(ID);

        assertThat(answer.kind()).isEqualTo(Kind.FRONTIER_STALLED);
        assertThat(answer.evidence()).extracting(Evidence::field)
                .containsExactly("frontierStalledMillis.order_line");
    }

    @Test
    void onlyTheChainsWhoseTablesHaveLandedAreNamedAsStalled() {
        Observation observation = loading(Map.of(
                "recordCount", 100L,
                "frontierStalledMillis.order_line", 1_127_598L,
                "frontierStalledMillis.customer", 96_000L), Map.of(
                "order_line", new InitialLoad.Table(InitialLoad.Stage.READING, 50_344L, 100_280L),
                "customer", new InitialLoad.Table(InitialLoad.Stage.LANDED, 10_000L, 10_000L)));

        PipelineExplanation answer = service(observation).explain(ID);

        assertThat(answer.kind()).isEqualTo(Kind.FRONTIER_STALLED);
        assertThat(answer.evidence()).extracting(Evidence::field)
                .containsExactly("frontierStalledMillis.customer");
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
    void anExplanationCarriesThePlanItsPipelinesRunWasSubmittedOnBesideAnUnchangedDiagnosis() {
        Observation observation = observation(PipelineState.RUNNING, Map.of("recordCount", 0L), 0L,
                NOW.minusSeconds(2));
        ExecutionPlan plan = new ExecutionPlan(ID, 3L, 7L, 11L, List.of("m1", "m2", "m3"),
                List.of(new ExecutionPlan.Node("orders_sink", 8, "explicit", "native", 3, 3, 9,
                        List.of("rounded-up"), 512, 50L, List.of("orders_sink"))),
                NOW.minusSeconds(60));
        PipelineExplainService planned = new PipelineExplainService(artifacts(ID), observations(observation),
                Clock.fixed(NOW, ZoneOffset.UTC), (key, args) -> key,
                pipelineIds -> pipelineIds.contains(ID) ? Map.of(ID, plan) : Map.of(),
                () -> List.of("m1", "m2", "m3", "m4"));

        PipelineExplanation answer = planned.explain(ID);

        assertThat(answer.plan()).isEqualTo(plan);
        assertThat(answer.awaitingRebalance()).containsExactly("m4");
        // No rule reads the plan: the diagnosis is the one the same observation gets with no plan recorded.
        assertThat(answer.withPlan(null)).isEqualTo(service(observation).explain(ID));
    }

    @Test
    void anExplanationSaysWhatEachAggregateStepKeepsAndWhatTheRunningStepCounted() {
        Resource pipeline = new DslParser().parse("""
                version: tapstate/v1
                kind: pipeline
                id: orders
                source: source
                transforms:
                  - id: paid
                    from: [ orders ]
                    type: filter
                    expr: "after.status == 'paid'"
                  - id: by_region
                    from: [ paid ]
                    type: aggregate
                    groupBy: { region: $region }
                    fields:
                      orders: { count: true }
                      most: { max: $amount }
                serve:
                  from: by_region
                  sync:
                    - { id: sink, source: target, write_mode: upsert, on_full_load: clear }
                """);
        ArtifactQueryService artifacts = artifactsOf(new LinkedHashMap<>(Map.of(ID, pipeline)));

        // The load said how many rows the input holds, and the running step has finished a count.
        Observation sized = new Observation(ID, PipelineState.RUNNING,
                Map.of("recordCount", 5L, "aggregateStateEntries.by_region", 2_400L),
                Map.of("orders", new TableSnapshot(1_000L, 1_000L, 100)), Map.of(), null, NOW.minusSeconds(2));
        assertThat(new PipelineExplainService(artifacts, observations(sized), Clock.fixed(NOW, ZoneOffset.UTC),
                (key, args) -> key).explain(ID).aggregates()).containsExactly(
                        new PipelineExplanation.Aggregate("by_region", 3, List.of("most"), 1_000L, 3_000L, 2_400L));

        // Neither is known: both are absent, and the cost per row still stands.
        Observation unsized = observation(PipelineState.RUNNING, Map.of("recordCount", 5L), 5L, NOW.minusSeconds(2));
        assertThat(new PipelineExplainService(artifacts, observations(unsized), Clock.fixed(NOW, ZoneOffset.UTC),
                (key, args) -> key).explain(ID).aggregates()).containsExactly(
                        new PipelineExplanation.Aggregate("by_region", 3, List.of("most"), null, null, null));
        assertThat(service(unsized).explain(ID).aggregates()).isEmpty();
    }

    @Test
    void anExplanationListsEachRollupFieldWithItsLevelFunctionFilterAndTheReferencesItReads() {
        Observation observation = observation(PipelineState.RUNNING, Map.of("recordCount", 5L), 5L,
                NOW.minusSeconds(2));
        Resource pipeline = new DslParser().parse("""
                version: tapstate/v1
                kind: pipeline
                id: orders
                source: source
                transforms:
                  - id: doc
                    type: nest
                    from: { t: transactions, i: items, pd: prizes, r: ratings }
                    root:
                      from: t
                      key: [id]
                      embed:
                        - from: i
                          on: { transaction_id: id }
                          as: array
                          path: item_movement
                          arrayKey: [id]
                          embed:
                            - { from: pd, on: { id: prize_definition_id }, as: object, path: prize_definition }
                          rollup:
                            qty: { sum: $amount, default: 0 }
                            reward_qty: { sum: $amount, filter: "=after.prize_definition.prize_code == 'reward'" }
                        - from: r
                          on: { trans_id: id }
                          as: rollup
                          arrayKey: [trans_id, rating_type]
                          rollup:
                            latest_score: { last: { value: $score, by: [ $model_date, $rating_type ] } }
                serve:
                  from: doc
                  sync:
                    - id: sink
                      source: target
                      write_mode: upsert
                """);
        ArtifactQueryService artifacts = artifactsOf(new LinkedHashMap<>(Map.of(ID, pipeline)));
        PipelineExplainService explaining = new PipelineExplainService(artifacts, observations(observation),
                Clock.fixed(NOW, ZoneOffset.UTC), (key, args) -> key);

        assertThat(explaining.explain(ID).rollups()).containsExactly(
                new PipelineExplanation.Rollup("doc", "$root", "item_movement", "qty", "sum", "$amount", null,
                        List.of()),
                new PipelineExplanation.Rollup("doc", "$root", "item_movement", "reward_qty", "sum", "$amount",
                        "=after.prize_definition.prize_code == 'reward'", List.of("prize_definition")),
                new PipelineExplanation.Rollup("doc", "$root", "$rollup[r]", "latest_score", "last",
                        "$score by $model_date, $rating_type", null, List.of()));
        assertThat(service(observation).explain(ID).rollups()).isEmpty();
    }

    @Test
    void anExplanationOfARunWithNoPlanRecordedCarriesNone() {
        Observation observation = observation(PipelineState.RUNNING, Map.of("recordCount", 5L), 5L,
                NOW.minusSeconds(2));

        assertThat(service(observation).explain(ID).plan()).isNull();
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

    private static Observation loading(Map<String, Long> metrics, Map<String, InitialLoad.Table> tables) {
        Instant observedAt = NOW.minusSeconds(2);
        return new Observation(ID, PipelineState.RUNNING, metrics,
                Map.of("orders", new TableSnapshot(60_344L, null, null)), Map.of(), null, observedAt, List.of(),
                new InitialLoad("execution:" + ID + ":1:" + NOW.minusSeconds(1_200), observedAt,
                        "snapshot_and_cdc", tables));
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
        return artifactsOf(resources);
    }

    private static ArtifactQueryService artifactsOf(Map<String, Resource> resources) {
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
