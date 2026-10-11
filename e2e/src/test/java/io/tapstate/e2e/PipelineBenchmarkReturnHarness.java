package io.tapstate.e2e;

import java.nio.file.Path;
import java.util.ArrayList;
import java.util.EnumMap;
import java.util.List;
import java.util.Map;

/** The original schedule and correctness oracle over typed return-interval forks. */
final class PipelineBenchmarkReturnHarness {
    @FunctionalInterface interface Driver {
        BenchmarkReturnCompletedMethod.CompletedFork run(BenchmarkWorkloadDefinitions.Workload workload,
                PipelineBenchmarkComparison.Arm arm, int ordinal, Path app) throws Exception;
    }
    static final class Report {
        private final Map<PipelineBenchmarkComparison.Workload, List<BenchmarkReturnCompletedMethod.CompletedFork>> forks;
        private final BenchmarkReturnIntervalComparison.Evaluation intervalEvaluation;
        private final Map<String, Object> externalParentWork;
        private Report(Map<PipelineBenchmarkComparison.Workload, List<BenchmarkReturnCompletedMethod.CompletedFork>> forks,
                BenchmarkReturnIntervalComparison.Evaluation intervalEvaluation, Map<String, Object> externalParentWork) {
            this.forks = Map.copyOf(forks); this.intervalEvaluation = intervalEvaluation;
            this.externalParentWork = Map.copyOf(externalParentWork);
        }
        Map<PipelineBenchmarkComparison.Workload, List<BenchmarkReturnCompletedMethod.CompletedFork>> forks() { return forks; }
        BenchmarkReturnIntervalComparison.Evaluation intervalEvaluation() { return intervalEvaluation; }
        Map<String, Object> externalParentWork() { return externalParentWork; }
        void requireCompletedMethodCost() {
            require(forks.size() == 3, "all original completed workloads are required");
            for (var group : forks.values()) {
                require(group.size() == 10, "all original alternating completed forks are required");
                for (var fork : group) require(fork != null && "ORIGINAL_METHOD_COMPLETED_AND_INDEPENDENT_BYTES_MATCHED"
                        .equals(fork.completionProof().get("state")), "independent completed-method proof is missing");
            }
        }
    }
    private PipelineBenchmarkReturnHarness() { }

    static Report run(PipelineBenchmarkHarness.Gate gate, Path a, Path b,
            List<BenchmarkWorkloadDefinitions.Workload> originals, Driver driver) throws Exception {
        return run(gate, a, b, originals, driver, null, null);
    }
    static Report run(PipelineBenchmarkHarness.Gate gate, Path a, Path b,
            List<BenchmarkWorkloadDefinitions.Workload> originals, Driver driver,
            BenchmarkReturnWorkBudgetBindings.Blob expectedA, BenchmarkReturnWorkBudgetBindings.Blob expectedB) throws Exception {
        require(BenchmarkReturnWorkBudget.current() == null && BenchmarkReturnWorkBudgetRun.current() == null,
                "aggregate controller must remain outside each original completed method");
        require(gate == PipelineBenchmarkHarness.Gate.OBSERVABILITY_COST,
                "typed business optimization remains unavailable before observability acceptance");
        require(a != null && b != null && driver != null && originals != null && originals.size() == 3,
                "all three registered original workloads and artifacts are required");
        require(originals.stream().map(BenchmarkWorkloadDefinitions.Workload::id).toList()
                .equals(List.of("copy", "stateless", "stateful")), "original workload order differs");
        require(expectedA != null && expectedB != null, "frozen original application byte identities are required");
        var forks = new EnumMap<PipelineBenchmarkComparison.Workload, List<BenchmarkReturnCompletedMethod.CompletedFork>>(PipelineBenchmarkComparison.Workload.class);
        var comparison = new EnumMap<PipelineBenchmarkComparison.Workload, List<BenchmarkReturnIntervalComparison.Fork>>(PipelineBenchmarkComparison.Workload.class);
        long driverInvocations = 0, individualOracleInvocations = 0, groupOracleInvocations = 0, chainVisits = 0;
        for (var workload : originals) {
            var kind = switch (workload.id()) {
                case "copy" -> PipelineBenchmarkComparison.Workload.COPY;
                case "stateless" -> PipelineBenchmarkComparison.Workload.STATELESS;
                case "stateful" -> PipelineBenchmarkComparison.Workload.STATEFUL;
                default -> throw new AssertionError("unknown original workload");
            };
            var completed = new ArrayList<BenchmarkReturnCompletedMethod.CompletedFork>(10);
            var points = new ArrayList<BenchmarkReturnIntervalComparison.Fork>(10);
            int ai = 0, bi = 0;
            for (var arm : PipelineBenchmarkComparison.schedule()) {
                int ordinal = arm == PipelineBenchmarkComparison.Arm.A ? ++ai : ++bi;
                driverInvocations++;
                var result = driver.run(workload, arm, ordinal, arm == PipelineBenchmarkComparison.Arm.A ? a : b);
                require(result != null && result.workload().equals(workload.id()) && result.arm() == arm && result.ordinal() == ordinal,
                        "completed interval fork differs from the original schedule");
                require((arm == PipelineBenchmarkComparison.Arm.A ? a : b).toString().equals(result.application().path())
                        && (arm == PipelineBenchmarkComparison.Arm.A ? expectedA : expectedB).equals(result.application().blob()),
                        "completed fork application differs from frozen original arm bytes");
                individualOracleInvocations++; chainVisits += result.correctness().chains().size();
                BenchmarkAckOracle.verify(List.of(result.correctness()), PipelineBenchmarkHarness.expectedTerminals(workload));
                completed.add(result); points.add(result.comparison());
            }
            groupOracleInvocations++;
            for (var fork : completed) chainVisits += fork.correctness().chains().size();
            BenchmarkAckOracle.verify(completed.stream().map(BenchmarkReturnCompletedMethod.CompletedFork::correctness).toList(),
                    PipelineBenchmarkHarness.expectedTerminals(workload));
            forks.put(kind, List.copyOf(completed)); comparison.put(kind, List.copyOf(points));
        }
        var childUpper = BenchmarkReturnBridgeSourceWork.upper(BenchmarkReturnBridgeSourceWork.Kind.ACK_VERIFY)
                .times(individualOracleInvocations + groupOracleInvocations)
                .plus(BenchmarkReturnBridgeSourceWork.upper(BenchmarkReturnBridgeSourceWork.Kind.TABLE_CONFIRMATION).times(chainVisits))
                .plus(BenchmarkReturnBridgeSourceWork.upper(BenchmarkReturnBridgeSourceWork.Kind.TABLE_COVER).times(chainVisits))
                // Each known cover calls its one fence accessor at most once; each nonnull copy
                // and each confirmation parse is already bounded by the same cursor source recipe.
                .plus(BenchmarkReturnBridgeSourceWork.upper(BenchmarkReturnBridgeSourceWork.Kind.FENCE_COPY).times(chainVisits))
                .plus(BenchmarkReturnBridgeSourceWork.upper(BenchmarkReturnBridgeSourceWork.Kind.JSON_PARSE).times(2 * chainVisits));
        var parentWork = Map.<String, Object>of("scope", "EXTERNAL_ORIGINAL_SCHEDULE_PARENT_SEPARATE_FROM_EACH_ORIGINAL_RUN",
                "enteredParentBodies", 1L, "completedParentBodies", 1L, "driverInvocations", driverInvocations,
                "individualOracleInvocations", individualOracleInvocations, "groupOracleInvocations", groupOracleInvocations,
                "knownTableChainVisits", chainVisits, "parentSourceUpper", BenchmarkReturnBridgeSourceWork.harnessUpper().evidence(),
                "knownOracleSourceUpperWithCallsiteChildUppers", childUpper.evidence(),
                "driverBodyScope", "OPAQUE_REQUEST_ORIGINAL_FORK_AND_EXTERNAL_ACQUISITION_SEPARATELY_ACCOUNTED");
        var report = new Report(forks, BenchmarkReturnIntervalComparison.evaluateObservabilityCost(comparison), parentWork);
        report.requireCompletedMethodCost();
        return report;
    }
    private static void require(boolean condition, String message) { if (!condition) throw new AssertionError("return harness " + message); }
}
