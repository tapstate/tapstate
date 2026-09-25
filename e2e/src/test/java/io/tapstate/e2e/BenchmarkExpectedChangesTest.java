package io.tapstate.e2e;

import org.bson.Document;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** The observer's physical target expectations follow each frozen measured source batch. */
class BenchmarkExpectedChangesTest {

    @Test
    void everyMeasuredBatchHasACompletePerTargetPlan() {
        for (BenchmarkWorkloadDefinitions.Workload workload : BenchmarkWorkloadDefinitions.all()) {
            for (BenchmarkWorkloadDefinitions.Phase phase : workload.phases()) {
                if (!phase.measured()) {
                    continue;
                }
                List<BenchmarkExpectedChanges.TargetPlan> plans =
                        BenchmarkExpectedChanges.forPhase(workload, phase);
                assertThat(plans).extracting(BenchmarkExpectedChanges.TargetPlan::target)
                        .containsExactlyElementsOf(phase.targets());
                assertThat(plans).allSatisfy(plan -> {
                    assertThat(plan.batches()).hasSize(phase.batches().size());
                    assertThat(plan.batches()).flatExtracting(batch -> batch)
                            .allSatisfy(change -> assertThat(change.kind())
                                    .isEqualTo(BenchmarkMongoDeliveryObserver.Kind.UPDATE));
                });
                assertThat(plans.stream().mapToLong(BenchmarkExpectedChanges.TargetPlan::totalChanges).sum())
                        .isEqualTo(phase.expectedLogicalOutputChanges());
            }
        }
    }

    @Test
    void copyAndStatelessExpandEachHundredIdSourceBatchToTheirOwnTargetKeys() {
        BenchmarkWorkloadDefinitions.Workload copy = BenchmarkWorkloadDefinitions.byId("copy");
        BenchmarkExpectedChanges.TargetPlan copied = BenchmarkExpectedChanges.forPhase(
                copy, copy.phase("cdc-update")).getFirst();
        assertThat(copied.forBatch(0)).hasSize(100);
        assertThat(copied.forBatch(0).getFirst().key()).isEqualTo("1");
        assertThat(copied.forBatch(119).getLast().key()).isEqualTo("12000");
        assertThat(copied.keyOf().apply(new Document("id", 42L))).isEqualTo("42");

        BenchmarkWorkloadDefinitions.Workload stateless = BenchmarkWorkloadDefinitions.byId("stateless");
        BenchmarkExpectedChanges.TargetPlan expanded = BenchmarkExpectedChanges.forPhase(
                stateless, stateless.phase("cdc-update")).getFirst();
        assertThat(expanded.forBatch(0)).hasSize(100);
        assertThat(expanded.forBatch(0).getFirst().key()).isEqualTo("2:0");
        assertThat(expanded.forBatch(0).get(1).key()).isEqualTo("2:1");
        assertThat(expanded.forBatch(119).getLast().key()).isEqualTo("12000:1");
        assertThat(expanded.keyOf().apply(new Document("id", 42L).append("item_index", 1L)))
                .isEqualTo("42:1");
    }

    @Test
    void coldReadTouchesOnlyNestAndCdcTouchesJoinAndNestInEachBatch() {
        BenchmarkWorkloadDefinitions.Workload stateful = BenchmarkWorkloadDefinitions.byId("stateful");
        BenchmarkWorkloadDefinitions.Phase coldPhase = stateful.phase("cold-read");
        List<BenchmarkExpectedChanges.TargetPlan> cold = BenchmarkExpectedChanges.forPhase(stateful, coldPhase);
        BenchmarkExpectedChanges.TargetPlan coldJoin = plan(cold, BenchmarkWorkloadDefinitions.Projection.JOIN);
        BenchmarkExpectedChanges.TargetPlan coldNest = plan(cold, BenchmarkWorkloadDefinitions.Projection.NEST);
        assertThat(coldJoin.totalChanges()).isZero();
        assertThat(coldJoin.forBatch(0)).isEmpty();
        assertThat(coldNest.forBatch(0)).hasSize(100);
        assertThat(coldNest.forBatch(0).getFirst().key()).isEqualTo("1");
        assertThat(coldNest.forBatch(119).getLast().key()).isEqualTo("12000");

        List<BenchmarkExpectedChanges.TargetPlan> cdc = BenchmarkExpectedChanges.forPhase(
                stateful, stateful.phase("cdc-update"));
        BenchmarkExpectedChanges.TargetPlan join = plan(cdc, BenchmarkWorkloadDefinitions.Projection.JOIN);
        BenchmarkExpectedChanges.TargetPlan nest = plan(cdc, BenchmarkWorkloadDefinitions.Projection.NEST);
        assertThat(join.forBatch(0)).hasSize(100);
        assertThat(nest.forBatch(0)).hasSize(100);
        assertThat(join.forBatch(0).getFirst().key()).isEqualTo("1");
        assertThat(nest.forBatch(0).getFirst().key()).isEqualTo("1");
        assertThat(join.keyOf().apply(new Document("order_id", 17L))).isEqualTo("17");
        assertThat(nest.keyOf().apply(new Document("id", 17L))).isEqualTo("17");
    }

    @Test
    void hotRootIsTouchedRepeatedlyButIsNeverRegisteredAsPhysicalMeasuredDelivery() {
        BenchmarkWorkloadDefinitions.Workload stateful = BenchmarkWorkloadDefinitions.byId("stateful");
        assertThat(BenchmarkExpectedChanges.hotStateLogicalRootTouches(stateful))
                .isEqualTo(Map.of("1", 64L));
        assertThatThrownBy(() -> BenchmarkExpectedChanges.forPhase(stateful, stateful.phase("hot-state")))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("non-measured phase");
        for (String phase : List.of("snapshot", "warm-up", "terminal")) {
            assertThatThrownBy(() -> BenchmarkExpectedChanges.forPhase(stateful, stateful.phase(phase)))
                    .isInstanceOf(IllegalArgumentException.class)
                    .hasMessageContaining("non-measured phase");
        }
    }

    @Test
    void aFixtureCountDriftOrMissingTargetKeyFailsRatherThanProducingP99Samples() {
        BenchmarkWorkloadDefinitions.Workload original = BenchmarkWorkloadDefinitions.byId("copy");
        BenchmarkWorkloadDefinitions.Phase cdc = original.phase("cdc-update");
        BenchmarkWorkloadDefinitions.Phase wrongCount = new BenchmarkWorkloadDefinitions.Phase(
                cdc.id(), cdc.stage(), cdc.measured(), cdc.expectedLogicalOutputChanges() - 1,
                cdc.sql(), cdc.statementsPerBatch(), cdc.batchInterval(),
                cdc.expectedLogicalCoverage(), cdc.targets());
        List<BenchmarkWorkloadDefinitions.Phase> changed = new ArrayList<>(original.phases());
        changed.set(changed.indexOf(cdc), wrongCount);
        BenchmarkWorkloadDefinitions.Workload drifted = new BenchmarkWorkloadDefinitions.Workload(
                original.id(), original.seed(), original.database(), original.pipelineIds(),
                original.sourceChains(), original.setupSql(), changed);
        assertThatThrownBy(() -> BenchmarkExpectedChanges.forPhase(drifted, wrongCount))
                .isInstanceOf(AssertionError.class)
                .hasMessageContaining("are 12000, expected 11999");

        BenchmarkExpectedChanges.TargetPlan good = BenchmarkExpectedChanges.forPhase(original, cdc).getFirst();
        assertThatThrownBy(() -> good.keyOf().apply(new Document("not_id", 1)))
                .isInstanceOf(AssertionError.class)
                .hasMessageContaining("target key field id is not numeric");
    }

    private static BenchmarkExpectedChanges.TargetPlan plan(
            List<BenchmarkExpectedChanges.TargetPlan> plans,
            BenchmarkWorkloadDefinitions.Projection projection) {
        return plans.stream().filter(plan -> plan.target().projection() == projection)
                .findFirst().orElseThrow();
    }
}
