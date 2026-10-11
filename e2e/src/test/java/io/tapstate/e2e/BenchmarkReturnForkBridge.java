package io.tapstate.e2e;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/** Typed original-fork time facts; completed collection-cost proof remains an independent component. */
final class BenchmarkReturnForkBridge {
    static final String PROPERTY = "tapstate.e2e.benchmark.return-method";
    private final BenchmarkReturnWorkBudgetRun owner;
    private final BenchmarkReturnMethodAdmission.AdmittedMethod admitted;
    private final BenchmarkCausalClock.Identity root;
    private final String workloadId;
    private BenchmarkWorkloadDefinitions.Workload workload;
    private PipelineBenchmarkComparison.Arm arm;
    private int fork;
    private List<String> phases = List.of();
    private final List<BenchmarkReturnMethodAdmission.BoundPhase> bound = new ArrayList<>();
    private boolean finished;

    private BenchmarkReturnForkBridge(String workloadId, BenchmarkReturnWorkBudgetRun owner,
            BenchmarkReturnMethodAdmission.AdmittedMethod admitted, BenchmarkCausalClock.Identity root) {
        this.workloadId = workloadId; this.owner = owner; this.admitted = admitted; this.root = root;
    }

    static boolean selected(String value) {
        return BenchmarkReturnBridgeSourceWork.entered(BenchmarkReturnBridgeSourceWork.Kind.SELECT, () -> {
            if (value == null) return false;
            require(BenchmarkReturnMethodAdmission.METHOD.equals(value), "unsupported explicit return method");
            return true;
        });
    }

    /** Called after actual root-origin acquisition and before services/configuration. */
    static BenchmarkReturnForkBridge open(String workloadId, BenchmarkReturnWorkBudgetRun owner) {
        if (!selected(System.getProperty(PROPERTY))) return null;
        return BenchmarkReturnBridgeSourceWork.entered(BenchmarkReturnBridgeSourceWork.Kind.OPEN, () -> {
        require(owner != null && owner == BenchmarkReturnWorkBudgetRun.current(), "active acquired Run is missing");
        require(List.of("copy", "stateless", "stateful").contains(workloadId), "original workload is unsupported");
        require("true".equals(System.getProperty(RealBenchmarkForkDriver.WORK_BUDGET_DIAGNOSTICS_PROPERTY))
                && "true".equals(System.getProperty(RealBenchmarkForkDriver.NOMINAL_RETURN_DIAGNOSTICS_PROPERTY))
                && "true".equals(System.getProperty(RealBenchmarkForkDriver.WRITE_RETURN_DIAGNOSTICS_PROPERTY))
                && "true".equals(System.getProperty(RealBenchmarkForkDriver.NATIVE_COUNTER_DOMAIN_PROPERTY)),
                "original accounted nominal producer route is missing");
        require(System.getProperty(RealBenchmarkForkDriver.RETURN_COLLECTOR_CALIBRATION_PROPERTY) == null,
                "formal return route cannot use collector calibration");
        var conflicts = conflictProperties();
        require(conflicts.size() <= BenchmarkReturnBridgeSourceWork.CONFLICT_SLOTS, "conflict list exceeds its declared source envelope");
        for (String property : conflicts) require(System.getProperty(property) == null,
                "formal return route conflicts with " + property);
        require("PLAIN".equals(System.getProperty("tapstate.e2e.benchmark-smoke.capture-mode", "PLAIN"))
                && "true".equals(System.getProperty("tapstate.e2e.benchmark-smoke.steady-pilot")),
                "original plain steady profile is required");
        var acquired = BenchmarkReturnWorkBudgetRun.acquireTrustedPreflight();
        var actual = acquired.requireActiveFacts();
        require(workloadId.equals(actual.costRecipe().profile().prepared().inputs().workload().id()),
                "acquired workload differs from the selected fork");
        // These exact new source roles must be appended and authenticated before this route can run.
        for (String binary : List.of(BenchmarkReturnForkBridge.class.getName(), PipelineBenchmarkReturnHarness.class.getName())) {
            var declared = actual.helper().sources().stream().filter(source -> binary.equals(source.role().binaryName())).findFirst().orElseThrow();
            require(actual.authenticatedBytes().sources().contains(declared), "new bridge source was not acquired");
        }
        var admitted = BenchmarkReturnMethodAdmission.admitTrusted(acquired);
        admitted.requireProductionAuthority();
        return new BenchmarkReturnForkBridge(workloadId, owner, admitted, actual.root());

        });
    }

    static List<String> conflictProperties() {
        return List.of(RealBenchmarkForkDriver.ROOT_CPU_DIAGNOSTICS_PROPERTY,
                RealBenchmarkForkDriver.WRITE_RETURN_CLOCK_CONTROL_PROPERTY,
                RealBenchmarkForkDriver.WRITE_RETURN_METHOD_CONTROL_PROPERTY,
                RealBenchmarkForkDriver.WRITE_RETURN_COST_STAGES_PROPERTY,
                "tapstate.e2e.benchmark-smoke.jvm-gap-diagnostics", "tapstate.e2e.benchmark.compilation-diagnostics",
                "tapstate.e2e.benchmark.thread-point-diagnostics", "tapstate.e2e.benchmark.load-diagnostics",
                BenchmarkDualGcDiagnostics.ENABLED_PROPERTY, BenchmarkWitnessReadGate.PROPERTY,
                BenchmarkMongoDeliveryObserver.CLOCK_REJECTION_EVIDENCE_PROPERTY,
                BenchmarkMongoDeliveryObserver.NATIVE_OPERATION_WALL_EVIDENCE_PROPERTY,
                "tapstate.e2e.benchmark-smoke.paced-calibration", "tapstate.e2e.benchmark-smoke.cdc-settling-calibration",
                "tapstate.e2e.benchmark-smoke.full-cdc-settling-calibration");
    }

    void bindDriver(BenchmarkWorkloadDefinitions.Workload actual, PipelineBenchmarkComparison.Arm arm, int fork, Path app) {
        BenchmarkReturnBridgeSourceWork.entered(BenchmarkReturnBridgeSourceWork.Kind.BIND, () -> {
        BenchmarkReturnBridgeSourceWork.driverMaterial(actual);
        requireActive(); require(workload == null && actual != null && actual.id().equals(workloadId), "driver binding is repeated or foreign");
        owner.requireDriver(actual, arm, fork, app);
        require(actual.phases().size() <= 6, "original phase roster exceeds its source cap");
        var measured = new ArrayList<String>();
        for (var phase : actual.phases()) if (phase.measured()) measured.add(phase.id());
        require(measured.size() == (workloadId.equals("stateful") ? 2 : 1), "original measured phase roster differs");
        this.workload = actual; this.arm = arm; this.fork = fork; phases = List.copyOf(measured);

        return null; });
    }

    BenchmarkReturnMethodAdmission.BoundPhase phase(BenchmarkWorkloadDefinitions.Phase phase,
            List<BenchmarkForkEnvironment.BatchResult> source, BenchmarkWriteReturnCapture.Result capture,
            Map<String, Object> before, Map<String, Object> after, List<BenchmarkTargetClock.Reading> hello) {
        return BenchmarkReturnBridgeSourceWork.entered(BenchmarkReturnBridgeSourceWork.Kind.PHASE, () -> {
        requireActive(); require(workload != null && bound.size() < phases.size()
                && phases.get(bound.size()).equals(phase.id()), "original measured phase is missing, repeated or out of order");
        require(capture != null && !capture.samples().isEmpty(), "actual owned capture is missing");
        var facts = new BenchmarkReturnMethodAdmission.PhaseFacts(workload, phase, source, capture, root,
                capture.samples().getFirst().identity(), before, after, hello);
        var checked = BenchmarkReturnMethodAdmission.verifyPhase(admitted, facts);
        bound.add(checked); return checked;

        });
    }

    /** Window acceptance is deferred until the original terminal/ACK/checksum/error oracle has run. */
    Fork finish(BenchmarkAckOracle.Fork correctness, long peakHeap, long peakRss) {
        return BenchmarkReturnBridgeSourceWork.entered(BenchmarkReturnBridgeSourceWork.Kind.FINISH, () -> {
        requireActive(); require(workload != null && bound.size() == phases.size(), "complete original measured phases are missing");
        String id = workloadId + "-" + arm + "-" + fork;
        require(correctness != null && id.equals(correctness.id()), "original fork correctness identity differs");
        BenchmarkReturnBridgeSourceWork.requireCoverage(correctness, BenchmarkReturnBridgeSourceWork.coverageUpper(workload));
        BenchmarkAckOracle.verify(List.of(correctness), PipelineBenchmarkHarness.expectedTerminals(workload));
        var metrics = new ArrayList<BenchmarkReturnIntervalMetrics.Phase>();
        for (var phase : bound) metrics.add(phase.intervalMetrics());
        var pooled = BenchmarkReturnIntervalMetrics.aggregate(metrics);
        require(pooled.phases().equals(phases), "pooled original measured phase roster differs");
        require(peakHeap > 0 && peakRss > 0, "actual retained memory observations are missing");
        var result = new Fork(id, workloadId, arm, fork, pooled, correctness, peakHeap, peakRss, admitted.evidence());
        finished = true; return result;

        });
    }

    private void requireActive() {
        require(!finished && owner == BenchmarkReturnWorkBudgetRun.current() && BenchmarkReturnWorkBudget.current() != null,
                "original fork bridge is closed, borrowed or inactive");
        admitted.requireProductionAuthority();
    }

    /** This carrier cannot be constructed from a caller metric DTO or a control token. */
    static final class Fork {
        private final String id, workload;
        private final PipelineBenchmarkComparison.Arm arm;
        private final int ordinal;
        private final BenchmarkReturnIntervalMetrics.Fork metrics;
        private final BenchmarkAckOracle.Fork correctness;
        private final long peakHeap, peakRss;
        private final Map<String, Object> methodEvidence;
        private Fork(String id, String workload, PipelineBenchmarkComparison.Arm arm, int ordinal,
                BenchmarkReturnIntervalMetrics.Fork metrics, BenchmarkAckOracle.Fork correctness,
                long peakHeap, long peakRss, Map<String, Object> methodEvidence) {
            this.id = id; this.workload = workload; this.arm = arm; this.ordinal = ordinal;
            this.metrics = metrics; this.correctness = correctness; this.peakHeap = peakHeap; this.peakRss = peakRss;
            this.methodEvidence = BenchmarkReturnBridgeSourceWork.entered(BenchmarkReturnBridgeSourceWork.Kind.CARRIER, () -> {
                BenchmarkReturnBridgeSourceWork.methodMap(methodEvidence); return Map.copyOf(methodEvidence);
            });
        }
        String id() { return id; }
        String workload() { return workload; }
        PipelineBenchmarkComparison.Arm arm() { return arm; }
        int ordinal() { return ordinal; }
        BenchmarkAckOracle.Fork correctness() { return correctness; }
        BenchmarkReturnIntervalMetrics.Fork metrics() { return metrics; }
        BenchmarkReturnIntervalComparison.Fork comparison() {
        return BenchmarkReturnBridgeSourceWork.entered(BenchmarkReturnBridgeSourceWork.Kind.COMPARISON, () -> {
            // Negative/uncertain p99 lower endpoints remain in metrics and evidence. No clamping or scalar conversion.
            require(metrics.p99().lowerNanos() > 0, "p99 lower enclosure is not positive for relative comparison");
            return new BenchmarkReturnIntervalComparison.Fork(arm,
                    rateBounds(metrics.throughput()),
                    new BenchmarkReturnIntervalComparison.Bounds(BigDecimal.valueOf(metrics.p99().lowerNanos()),
                            BigDecimal.valueOf(metrics.p99().upperNanos())), metrics.fullCohortRows(), peakHeap, peakRss);

        });
    }
        Map<String, Object> evidence() {
        return BenchmarkReturnBridgeSourceWork.entered(BenchmarkReturnBridgeSourceWork.Kind.EVIDENCE, () -> {
            var out = new LinkedHashMap<String, Object>(); out.put("id", id); out.put("method", BenchmarkReturnMethodAdmission.METHOD);
            out.put("endpoint", BenchmarkReturnMethodAdmission.ENDPOINT);
            out.put("workload", workload); out.put("arm", arm.name()); out.put("ordinal", ordinal);
            out.put("peakHeapBytes", peakHeap); out.put("peakRssBytes", peakRss);
            out.put("methodAdmission", methodEvidence); out.put("phases", metrics.phases()); out.put("fullCohortRows", metrics.fullCohortRows());
            var rate = rateBounds(metrics.throughput());
            out.put("throughput", Map.of("lower", decimal(rate.lower()), "upper", decimal(rate.upper()),
                    "representation", "DIRECTED_DECIMAL_STRINGS_FROM_BINARY_RATE_ENCLOSURES"));
            out.put("p99Nanos", Map.of("lower", metrics.p99().lowerNanos(), "upper", metrics.p99().upperNanos()));
            out.put("completedRows", Map.of("lower", metrics.completedLower(), "upper", metrics.completedUpper()));
            out.put("durationNanos", Map.of("lower", metrics.durationLower(), "upper", metrics.durationUpper()));
            out.put("completedCostProof", "AWAITING_INDEPENDENT_AFTER_RECEIPT_COMPONENT");
            BenchmarkReturnProducerOrigins.FLAGS.forEach(flag -> out.put(flag, false)); return Map.copyOf(out);

        });
    }
    }
    static BenchmarkReturnIntervalComparison.Bounds rateBounds(BenchmarkReturnTimeBounds.Rate rate) {
        require(rate != null, "rate enclosure is missing");
        return new BenchmarkReturnIntervalComparison.Bounds(rateDecimal(rate.lowerRecordsPerSecond(), false),
                rateDecimal(rate.upperRecordsPerSecond(), true));
    }
    private static BigDecimal rateDecimal(double value, boolean upper) {
        // Complete cohort<=262144, duration>=1 and duration<=Long.MAX_VALUE independently bound this domain.
        require(Double.isFinite(value) && value >= 0.000001 && value < 300000000000000.0,
                "rate is outside the registered count/duration material domain");
        return new BigDecimal(value).setScale(48, upper ? RoundingMode.CEILING : RoundingMode.FLOOR).stripTrailingZeros();
    }
    static String decimal(BigDecimal value) {
        require(value != null && value.precision() <= 64 && Math.abs((long) value.scale()) <= 64,
                "decimal projection exceeds prior material");
        String text = value.toPlainString(); require(text.length() <= 160, "decimal projection exceeds its source string cap");
        return text;
    }
    private static void require(boolean condition, String message) { if (!condition) throw new AssertionError("return fork bridge " + message); }
}
