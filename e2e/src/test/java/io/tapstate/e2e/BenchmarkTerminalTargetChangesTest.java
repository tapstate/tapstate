package io.tapstate.e2e;

import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** The unmeasured terminal checkpoint requires each final target and bounds optional Nest refinement. */
class BenchmarkTerminalTargetChangesTest {

    @Test
    void copyAndStatelessTerminalRowsProduceTheirFrozenInsertKeys() {
        BenchmarkWorkloadDefinitions.Workload copy = BenchmarkWorkloadDefinitions.byId("copy");
        assertThat(BenchmarkTerminalTargetChanges.forTarget(copy, copy.phase("terminal").targets().getFirst()))
                .containsExactly(insert("900001"));

        BenchmarkWorkloadDefinitions.Workload stateless = BenchmarkWorkloadDefinitions.byId("stateless");
        assertThat(BenchmarkTerminalTargetChanges.forTarget(
                stateless, stateless.phase("terminal").targets().getFirst()))
                .containsExactly(insert("900002:0"), insert("900002:1"));
    }

    @Test
    void statefulTerminalRequiresBothFinalTargetsAndAllowsOneNestedRefinement() {
        BenchmarkWorkloadDefinitions.Workload stateful = BenchmarkWorkloadDefinitions.byId("stateful");
        List<BenchmarkWorkloadDefinitions.TargetExpectation> targets = stateful.phase("terminal").targets();
        assertThat(BenchmarkTerminalTargetChanges.forTarget(stateful, targets.getFirst()))
                .containsExactly(insert("900011"));
        assertThat(BenchmarkTerminalTargetChanges.forTarget(stateful, targets.get(1)))
                .containsExactly(insert("900013"));
        assertThat(BenchmarkTerminalTargetChanges.optionalForTarget(stateful, targets.get(1)))
                .containsExactly(update("900013"));
        assertThat(stateful.phase("terminal").expectedLogicalOutputChanges()).isEqualTo(2);
    }

    @Test
    void nonterminalTargetOrChangedTerminalSqlFailsBeforeRegisteringAnObserverExpectation() {
        BenchmarkWorkloadDefinitions.Workload copy = BenchmarkWorkloadDefinitions.byId("copy");
        assertThatThrownBy(() -> BenchmarkTerminalTargetChanges.forTarget(
                copy, copy.phase("snapshot").targets().getFirst()))
                .isInstanceOf(IllegalArgumentException.class).hasMessageContaining("not a terminal checkpoint");

        BenchmarkWorkloadDefinitions.Phase terminal = copy.phase("terminal");
        BenchmarkWorkloadDefinitions.Phase driftedTerminal = new BenchmarkWorkloadDefinitions.Phase(
                terminal.id(), terminal.stage(), terminal.measured(), terminal.expectedLogicalOutputChanges(),
                List.of(terminal.sql().getFirst().replace("900001", "900099")), terminal.statementsPerBatch(),
                terminal.batchInterval(), terminal.expectedLogicalCoverage(), terminal.targets());
        List<BenchmarkWorkloadDefinitions.Phase> phases = new ArrayList<>(copy.phases());
        phases.set(phases.indexOf(terminal), driftedTerminal);
        BenchmarkWorkloadDefinitions.Workload drifted = new BenchmarkWorkloadDefinitions.Workload(
                copy.id(), copy.seed(), copy.database(), copy.pipelineIds(), copy.sourceChains(),
                copy.setupSql(), phases);
        assertThatThrownBy(() -> BenchmarkTerminalTargetChanges.forTarget(
                drifted, driftedTerminal.targets().getFirst()))
                .isInstanceOf(AssertionError.class)
                .hasMessageContaining("terminal SQL changed without a new physical target witness");
    }

    private static BenchmarkMongoDeliveryObserver.ExpectedChange insert(String key) {
        return new BenchmarkMongoDeliveryObserver.ExpectedChange(key,
                BenchmarkMongoDeliveryObserver.Kind.INSERT);
    }

    private static BenchmarkMongoDeliveryObserver.ExpectedChange update(String key) {
        return new BenchmarkMongoDeliveryObserver.ExpectedChange(key,
                BenchmarkMongoDeliveryObserver.Kind.UPDATE);
    }
}
