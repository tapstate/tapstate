package io.tapstate.e2e;

import io.tapstate.core.common.JsonWriter;
import io.tapstate.testsupport.DockerGate;
import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.time.Instant;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/** A real-process fork proves that external delivery timing and terminal ACK agree. */
class RealBenchmarkForkDriverIT {

    private static final String BOOT_JAR_PROPERTY = "tapstate.e2e.benchmark-smoke.jar";
    private static final String ARM_PROPERTY = "tapstate.e2e.benchmark-smoke.arm";
    private static final String FORK_PROPERTY = "tapstate.e2e.benchmark-smoke.fork";
    private static final String MODE_PROPERTY = "tapstate.e2e.benchmark-smoke.capture-mode";
    private static final String OUTPUT_PROPERTY = "tapstate.e2e.benchmark-smoke.fork-output";

    record WriteReturnJfrScope(String workload, PipelineBenchmarkComparison.Arm arm,
            BenchmarkCaptureCalibrationLiveRunIT.Mode captureMode, Path output, boolean pilot,
            BenchmarkReturnClockSampler.Mode clockMode, boolean conflictingDiagnostics) { }

    static void requireWriteReturnJfrScope(WriteReturnJfrScope scope) {
        if (scope == null || !"stateless".equals(scope.workload()) || scope.arm() != PipelineBenchmarkComparison.Arm.B
                || scope.captureMode() != BenchmarkCaptureCalibrationLiveRunIT.Mode.PLAIN || !scope.pilot()
                || scope.output() == null || !scope.output().isAbsolute() || scope.output().getFileName() == null
                || scope.clockMode() != BenchmarkReturnClockSampler.Mode.PERIODIC || scope.conflictingDiagnostics()) {
            throw new AssertionError("return JFR diagnostics require one original plain stateless B pilot with periodic clocks and an explicit fork receipt");
        }
    }

    @BeforeAll
    static void requireServices() {
        String cpu = System.getProperty(RealBenchmarkForkDriver.ROOT_CPU_DIAGNOSTICS_PROPERTY);
        String nominal = System.getProperty(RealBenchmarkForkDriver.NOMINAL_RETURN_DIAGNOSTICS_PROPERTY);
        if (cpu != null || nominal != null) {
            String mode = System.getProperty(RealBenchmarkForkDriver.RETURN_COLLECTOR_CALIBRATION_PROPERTY);
            var calibration = "ON".equals(mode) ? RealBenchmarkForkDriver.CollectorCalibration.ON
                    : "OFF".equals(mode) ? RealBenchmarkForkDriver.CollectorCalibration.OFF : null;
            if (cpu != null) { RealBenchmarkForkDriver.rootCpuDiagnostics(cpu, calibration); }
            if (nominal != null) {
                RealBenchmarkForkDriver.nominalReturnDiagnostics(nominal,
                        "true".equals(System.getProperty(RealBenchmarkForkDriver.NATIVE_COUNTER_DOMAIN_PROPERTY)), calibration);
            }
            return;
        }
        requireBenchmarkServices();
    }

    private static void requireBenchmarkServices() {
        DockerGate.require();
        RealConnectorGate.require("mysql", "postgres", "mongodb");
        String configured = System.getProperty(BOOT_JAR_PROPERTY);
        Assumptions.assumeTrue(configured != null && !configured.isBlank(),
                "no -D" + BOOT_JAR_PROPERTY + ": skipping an explicit-JAR benchmark fork");
        assertThat(Files.isRegularFile(Path.of(configured))).isTrue();
    }

    @Test
    void copyForkUsesRealTargetChangesAndItsOwnTerminalPosition() throws Exception {
        run("copy");
    }

    @Test
    void statelessForkUsesPgoutputAndItsOwnTerminalPosition() throws Exception {
        run("stateless");
    }

    @Test
    void statefulForkObservesColdReadAndCdcAcrossBothTargets() throws Exception {
        run("stateful");
    }

    private static void run(String workloadId) throws Exception {
        boolean rootCpuDiagnostics = requireRootCpuRunServices(workloadId, RealBenchmarkForkDriverIT::requireBenchmarkServices);
        boolean nominalReturnDiagnostics = Boolean.getBoolean(RealBenchmarkForkDriver.NOMINAL_RETURN_DIAGNOSTICS_PROPERTY);
        Path forkOutput = forkOutput();
        PipelineBenchmarkComparison.Arm arm = PipelineBenchmarkComparison.Arm.valueOf(
                System.getProperty(ARM_PROPERTY, "A"));
        var mode = BenchmarkCaptureCalibrationLiveRunIT.Mode.valueOf(System.getProperty(MODE_PROPERTY, "PLAIN"));
        Path applicationJar = Path.of(System.getProperty(BOOT_JAR_PROPERTY));
        boolean writeReturnDiagnostics = Boolean.getBoolean(RealBenchmarkForkDriver.WRITE_RETURN_DIAGNOSTICS_PROPERTY);
        boolean jvmDiagnostics = Boolean.getBoolean("tapstate.e2e.benchmark-smoke.jvm-gap-diagnostics");
        var returnClockMode = RealBenchmarkForkDriver.writeReturnClockMode(
                System.getProperty(RealBenchmarkForkDriver.WRITE_RETURN_CLOCK_CONTROL_PROPERTY),
                writeReturnDiagnostics, Boolean.getBoolean("tapstate.e2e.benchmark-smoke.steady-pilot"));
        boolean conflictingReturnDiagnostics = List.of("tapstate.e2e.benchmark.load-diagnostics", "tapstate.e2e.benchmark.compilation-diagnostics",
                "tapstate.e2e.benchmark.thread-point-diagnostics", BenchmarkDualGcDiagnostics.ENABLED_PROPERTY, BenchmarkWitnessReadGate.PROPERTY,
                BenchmarkMongoDeliveryObserver.CLOCK_REJECTION_EVIDENCE_PROPERTY, BenchmarkMongoDeliveryObserver.NATIVE_OPERATION_WALL_EVIDENCE_PROPERTY,
                "tapstate.e2e.benchmark-smoke.paced-calibration", "tapstate.e2e.benchmark-smoke.cdc-settling-calibration",
                "tapstate.e2e.benchmark-smoke.full-cdc-settling-calibration").stream().anyMatch(Boolean::getBoolean);
        var collectorCalibration = System.getProperty(RealBenchmarkForkDriver.RETURN_COLLECTOR_CALIBRATION_PROPERTY) == null
                ? null : RealBenchmarkForkDriver.collectorCalibration(
                        System.getProperty(RealBenchmarkForkDriver.RETURN_COLLECTOR_CALIBRATION_PROPERTY), workloadId, arm,
                        writeReturnDiagnostics, Boolean.getBoolean("tapstate.e2e.benchmark-smoke.steady-pilot"), returnClockMode,
                        System.getProperty(RealBenchmarkForkDriver.NATIVE_CLOCK_LIBRARY_PROPERTY),
                        mode == BenchmarkCaptureCalibrationLiveRunIT.Mode.PLAIN, forkOutput != null,
                        conflictingReturnDiagnostics || jvmDiagnostics
                                || System.getProperty(RealBenchmarkForkDriver.WRITE_RETURN_METHOD_CONTROL_PROPERTY) != null
                                || System.getProperty(RealBenchmarkForkDriver.WRITE_RETURN_COST_STAGES_PROPERTY) != null
                                || System.getProperty(RealBenchmarkForkDriver.NATIVE_COUNTER_DOMAIN_PROPERTY) != null);
        boolean methodControl = RealBenchmarkForkDriver.admitWriteReturnMethodProtocol(
                System.getProperty(RealBenchmarkForkDriver.WRITE_RETURN_METHOD_CONTROL_PROPERTY), writeReturnDiagnostics,
                Boolean.getBoolean("tapstate.e2e.benchmark-smoke.steady-pilot"), returnClockMode,
                conflictingReturnDiagnostics || jvmDiagnostics || !"stateless".equals(workloadId)
                        || arm != PipelineBenchmarkComparison.Arm.B || mode != BenchmarkCaptureCalibrationLiveRunIT.Mode.PLAIN
                        || forkOutput == null);
        boolean costStages = RealBenchmarkForkDriver.writeReturnCostStages(
                System.getProperty(RealBenchmarkForkDriver.WRITE_RETURN_COST_STAGES_PROPERTY), writeReturnDiagnostics,
                Boolean.getBoolean("tapstate.e2e.benchmark-smoke.steady-pilot"), returnClockMode,
                conflictingReturnDiagnostics || jvmDiagnostics || !"stateless".equals(workloadId)
                        || arm != PipelineBenchmarkComparison.Arm.B || mode != BenchmarkCaptureCalibrationLiveRunIT.Mode.PLAIN
                        || forkOutput == null || System.getProperty(RealBenchmarkForkDriver.WRITE_RETURN_METHOD_CONTROL_PROPERTY) != null);
        String nativeLibrary = RealBenchmarkForkDriver.nativeClockLibrary(
                System.getProperty(RealBenchmarkForkDriver.NATIVE_CLOCK_LIBRARY_PROPERTY), writeReturnDiagnostics,
                Boolean.getBoolean("tapstate.e2e.benchmark-smoke.steady-pilot"), returnClockMode,
                conflictingReturnDiagnostics || jvmDiagnostics || !"stateless".equals(workloadId)
                        || !RealBenchmarkForkDriver.nativeClockArmAllowed(arm,
                                Boolean.getBoolean(RealBenchmarkForkDriver.NATIVE_COUNTER_DOMAIN_PROPERTY))
                        || mode != BenchmarkCaptureCalibrationLiveRunIT.Mode.PLAIN
                        || forkOutput == null || costStages
                        || System.getProperty(RealBenchmarkForkDriver.WRITE_RETURN_METHOD_CONTROL_PROPERTY) != null);
        boolean nativeCounterDomain = RealBenchmarkForkDriver.nativeCounterDomain(
                System.getProperty(RealBenchmarkForkDriver.NATIVE_COUNTER_DOMAIN_PROPERTY), nativeLibrary, writeReturnDiagnostics,
                Boolean.getBoolean("tapstate.e2e.benchmark-smoke.steady-pilot"), returnClockMode,
                conflictingReturnDiagnostics || jvmDiagnostics || costStages
                        || System.getProperty(RealBenchmarkForkDriver.WRITE_RETURN_METHOD_CONTROL_PROPERTY) != null);
        if (writeReturnDiagnostics && (mode != BenchmarkCaptureCalibrationLiveRunIT.Mode.PLAIN || forkOutput == null
                || !Boolean.getBoolean("tapstate.e2e.benchmark-smoke.steady-pilot")
                || conflictingReturnDiagnostics)) {
            throw new AssertionError("return diagnostics require an original plain pilot with an explicit fork receipt");
        }
        if (writeReturnDiagnostics && jvmDiagnostics) {
            requireWriteReturnJfrScope(new WriteReturnJfrScope(workloadId, arm, mode, forkOutput,
                    Boolean.getBoolean("tapstate.e2e.benchmark-smoke.steady-pilot"), returnClockMode, conflictingReturnDiagnostics));
        }
        if (Boolean.getBoolean(BenchmarkMongoDeliveryObserver.NATIVE_OPERATION_WALL_EVIDENCE_PROPERTY)
                && !Boolean.getBoolean(BenchmarkMongoDeliveryObserver.CLOCK_REJECTION_EVIDENCE_PROPERTY)) {
            throw new AssertionError("native operation wall evidence requires decoded clock refusal evidence");
        }
        if (Boolean.getBoolean(BenchmarkMongoDeliveryObserver.CLOCK_REJECTION_EVIDENCE_PROPERTY)
                && (!"stateless".equals(workloadId) || arm != PipelineBenchmarkComparison.Arm.B
                || mode != BenchmarkCaptureCalibrationLiveRunIT.Mode.PLAIN || forkOutput == null
                || !Boolean.getBoolean("tapstate.e2e.benchmark-smoke.steady-pilot")
                || List.of("tapstate.e2e.benchmark.load-diagnostics", "tapstate.e2e.benchmark.compilation-diagnostics",
                        "tapstate.e2e.benchmark.thread-point-diagnostics", "tapstate.e2e.benchmark-smoke.jvm-gap-diagnostics",
                        BenchmarkDualGcDiagnostics.ENABLED_PROPERTY, BenchmarkWitnessReadGate.PROPERTY,
                        "tapstate.e2e.benchmark-smoke.paced-calibration", "tapstate.e2e.benchmark-smoke.cdc-settling-calibration",
                        "tapstate.e2e.benchmark-smoke.full-cdc-settling-calibration").stream().anyMatch(Boolean::getBoolean))) {
            throw new AssertionError("clock refusal evidence requires one original plain stateless B diagnostic");
        }
        if (Boolean.getBoolean(BenchmarkWitnessReadGate.PROPERTY) && (!"copy".equals(workloadId)
                || arm != PipelineBenchmarkComparison.Arm.B
                || mode != BenchmarkCaptureCalibrationLiveRunIT.Mode.PLAIN || forkOutput == null
                || !Boolean.getBoolean("tapstate.e2e.benchmark-smoke.steady-pilot")
                || Boolean.getBoolean("tapstate.e2e.benchmark.load-diagnostics")
                || Boolean.getBoolean("tapstate.e2e.benchmark.compilation-diagnostics")
                || Boolean.getBoolean("tapstate.e2e.benchmark.thread-point-diagnostics")
                || Boolean.getBoolean("tapstate.e2e.benchmark-smoke.jvm-gap-diagnostics")
                || Boolean.getBoolean(BenchmarkDualGcDiagnostics.ENABLED_PROPERTY)
                || Boolean.getBoolean("tapstate.e2e.benchmark-smoke.paced-calibration")
                || Boolean.getBoolean("tapstate.e2e.benchmark-smoke.cdc-settling-calibration")
                || Boolean.getBoolean("tapstate.e2e.benchmark-smoke.full-cdc-settling-calibration"))) {
            throw new AssertionError("deferred target witness requires one unchanged plain original copy B fork");
        }
        boolean threadPointDiagnostics = Boolean.getBoolean("tapstate.e2e.benchmark.thread-point-diagnostics");
        if (threadPointDiagnostics && (!"copy".equals(workloadId)
                || arm != PipelineBenchmarkComparison.Arm.B
                || mode != BenchmarkCaptureCalibrationLiveRunIT.Mode.PLAIN || forkOutput == null
                || !Boolean.getBoolean("tapstate.e2e.benchmark-smoke.steady-pilot")
                || !Boolean.getBoolean("tapstate.e2e.benchmark.load-diagnostics")
                || Boolean.getBoolean("tapstate.e2e.benchmark.compilation-diagnostics")
                || Boolean.getBoolean("tapstate.e2e.benchmark-smoke.jvm-gap-diagnostics")
                || Boolean.getBoolean(BenchmarkDualGcDiagnostics.ENABLED_PROPERTY)
                || Boolean.getBoolean("tapstate.e2e.benchmark-smoke.paced-calibration")
                || Boolean.getBoolean("tapstate.e2e.benchmark-smoke.cdc-settling-calibration")
                || Boolean.getBoolean("tapstate.e2e.benchmark-smoke.full-cdc-settling-calibration"))) {
            throw new AssertionError("thread point diagnostics require one unchanged plain original copy B fork with load points");
        }
        BenchmarkJdiCostObserver.Artifact artifact = mode == BenchmarkCaptureCalibrationLiveRunIT.Mode.PLAIN
                ? null : BenchmarkJdiCostObserver.Artifact.open(
                applicationJar, arm == PipelineBenchmarkComparison.Arm.A ? BenchmarkJdiCostObserver.Arm.REFERENCE
                        : BenchmarkJdiCostObserver.Arm.OBSERVABILITY, BenchmarkJdiCostObserver.selectedArtifactSet());
        try (artifact) {
            if (jvmDiagnostics && mode != BenchmarkCaptureCalibrationLiveRunIT.Mode.PLAIN) {
                throw new AssertionError("JVM gap diagnostics require an independent plain artifact run");
            }
            boolean dualGcDiagnostics = Boolean.getBoolean(BenchmarkDualGcDiagnostics.ENABLED_PROPERTY);
            boolean compilationDiagnostics = Boolean.getBoolean("tapstate.e2e.benchmark.compilation-diagnostics");
            if (compilationDiagnostics && (!"copy".equals(workloadId)
                    || arm != PipelineBenchmarkComparison.Arm.B
                    || mode != BenchmarkCaptureCalibrationLiveRunIT.Mode.PLAIN || jvmDiagnostics || dualGcDiagnostics
                    || !Boolean.getBoolean("tapstate.e2e.benchmark-smoke.steady-pilot") || forkOutput == null
                    || Boolean.getBoolean("tapstate.e2e.benchmark-smoke.paced-calibration")
                    || Boolean.getBoolean("tapstate.e2e.benchmark-smoke.cdc-settling-calibration")
                    || Boolean.getBoolean("tapstate.e2e.benchmark-smoke.full-cdc-settling-calibration")
                    || Boolean.getBoolean("tapstate.e2e.benchmark.load-diagnostics"))) {
                throw new AssertionError("compilation diagnostics require one unchanged plain original copy B fork");
            }
            if (dualGcDiagnostics && (!"copy".equals(workloadId)
                    || arm != PipelineBenchmarkComparison.Arm.B
                    || mode != BenchmarkCaptureCalibrationLiveRunIT.Mode.PLAIN || jvmDiagnostics)) {
                throw new AssertionError("owned dual GC diagnostics require one plain copy B fork without JFR");
            }
            BenchmarkDualGcDiagnostics.Session dualGc = dualGcDiagnostics
                    ? BenchmarkDualGcDiagnostics.open() : null;
            RealBenchmarkForkDriver driver = collectorCalibration != null
                    ? new RealBenchmarkForkDriver((store, operator, jar) -> new BenchmarkForkEnvironment.OwnedBoot(
                            RealProcessServer.start(store, operator, jar, "127.0.0.1", port -> List.of(),
                                    collectorCalibrationArguments(collectorCalibration, nativeLibrary)), null))
                    : writeReturnDiagnostics && jvmDiagnostics
                    ? new RealBenchmarkForkDriver(BenchmarkJvmDiagnostics::startWithWriteReturns)
                    : writeReturnDiagnostics
                    ? new RealBenchmarkForkDriver((store, operator, jar) -> new BenchmarkForkEnvironment.OwnedBoot(
                            RealProcessServer.start(store, operator, jar, "127.0.0.1",
                                    port -> List.of(), returnArguments(methodControl, costStages, nativeLibrary)), null))
                    : dualGc != null
                    ? new RealBenchmarkForkDriver(dualGc::start)
                    : jvmDiagnostics ? new RealBenchmarkForkDriver(BenchmarkJvmDiagnostics::start)
                    : mode.driver(applicationJar, artifact);
            int forkNumber = Integer.parseInt(System.getProperty(FORK_PROPERTY, "1"));
            assertThat(forkNumber).as("the diagnostic fork number").isBetween(1, 5);
            boolean pilot = Boolean.getBoolean("tapstate.e2e.benchmark-smoke.steady-pilot");
            boolean pacedCalibration = Boolean.getBoolean("tapstate.e2e.benchmark-smoke.paced-calibration");
            boolean settlingCalibration = Boolean.getBoolean("tapstate.e2e.benchmark-smoke.cdc-settling-calibration");
            boolean fullSettlingCalibration = Boolean.getBoolean("tapstate.e2e.benchmark-smoke.full-cdc-settling-calibration");
            if (pacedCalibration && !pilot) { throw new AssertionError("paced calibration requires the declared larger profile"); }
            if ((settlingCalibration || fullSettlingCalibration)
                    && (!pilot || pacedCalibration || (settlingCalibration && fullSettlingCalibration))) {
                throw new AssertionError("settling calibration requires the original larger schedule and cannot mix calibrations");
            }
            if (dualGcDiagnostics && (!pilot || !fullSettlingCalibration
                    || settlingCalibration || pacedCalibration || forkOutput == null)) {
                throw new AssertionError("owned dual GC diagnostics require the unchanged full-settling profile and exact fork output");
            }
            var workload = fullSettlingCalibration ? BenchmarkWorkloadDefinitions.cdcFullSettlingCalibration(workloadId)
                    : settlingCalibration ? BenchmarkWorkloadDefinitions.cdcSettlingCalibration(workloadId)
                    : pacedCalibration ? BenchmarkWorkloadDefinitions.pacedCalibration(workloadId)
                    : pilot ? BenchmarkWorkloadDefinitions.steadyPilot(workloadId)
                    : BenchmarkWorkloadDefinitions.byId(workloadId);
            Instant startedAt = Instant.now();
            PipelineBenchmarkHarness.ForkResult result = driver.run(
                    workload, arm,
                    forkNumber, applicationJar);
            System.out.println("benchmark-source-schedule=" + JsonWriter.write(Map.of(
                    "profile", fullSettlingCalibration ? "FIXED_FULL_CDC_SETTLING_CALIBRATION"
                            : settlingCalibration ? "FIXED_FIRST_QUARTER_CDC_SETTLING_CALIBRATION"
                            : pacedCalibration ? "FIXED_PACING_CALIBRATION_5MS_50MS" : "ORIGINAL_BATCH_SCHEDULE",
                    "rows", workload.rows(), "phases", workload.phases().stream().filter(
                            BenchmarkWorkloadDefinitions.Phase::measured).map(phase -> Map.of(
                                    "id", phase.id(), "batchIntervalMillis", phase.batchInterval().toMillis(),
                                    "sourceStatementsPerBatch", phase.statementsPerBatch())).toList(),
                    "performanceAcceptanceEligible", false)));

            BenchmarkAckOracle.verify(List.of(result.correctness()));
            int expectedMeasured = (workloadId.equals("stateful") ? 3 : 1) * workload.rows() / (pilot ? 2 : 1);
            int expectedPhysical = switch (workloadId) {
                case "copy" -> workload.rows() + 1;
                case "stateless" -> workload.rows() + 2;
                case "stateful" -> workload.rows() * 3 + 2;
                default -> throw new AssertionError("unrecognized benchmark workload " + workloadId);
            };
            assertThat(result.measurement().deliveryNanos()).hasSize(expectedMeasured);
            assertThat(result.measurement().recordsOutPerSecond()).isPositive();
            assertThat(result.correctness().errorTotal()).isZero();
            assertThat(driver.evidence()).singleElement().satisfies(evidence -> {
                assertThat(evidence.phases()).hasSize(workloadId.equals("stateful") ? 2 : 1);
                assertThat(evidence.resources().sampleCount()).isGreaterThan(1);
                assertThat(evidence.mongoCommands().totalCommands()).isPositive();
                assertThat(evidence.observedTargetCoverage()).hasSize(expectedPhysical);
                assertThat(evidence.observedTargetCoverage().values()).containsOnly(1L);
                evidence.phases().forEach(phase -> assertThat(phase.reportedRecordsOut())
                        .as("replayed sink work remains visible as a cost beside logical delivery")
                        .isGreaterThanOrEqualTo(phase.acknowledgedOutputs()));
                evidence.phases().forEach(phase -> {
                    if (rootCpuDiagnostics) {
                        var rootCpu = (Map<?, ?>) phase.targetClockEvidence().get("rootCpuDiagnostics");
                        assertThat(rootCpu.get("state")).isEqualTo("RECORDED_DIAGNOSTIC");
                        assertThat((List<?>) rootCpu.get("readings")).hasSize(3);
                        assertThat((List<?>) rootCpu.get("operations")).hasSize(4);
                        assertThat(((Map<?, ?>) rootCpu.get("accountingErrorAllowance")).get("state")).isEqualTo("UNKNOWN");
                        assertThat(((Map<?, ?>) rootCpu.get("collectionCpuUpperBound")).get("state")).isEqualTo("UNKNOWN");
                        for (String flag : List.of("accountingErrorBoundQualified", "wholeMethodCostQualified",
                                "collectionCostUpperBoundQualified", "samplingCostQualified", "causalOverheadQualified",
                                "costAcceptanceEligible", "performanceAcceptanceEligible", "formalPerformance")) {
                            assertThat(rootCpu.get(flag)).isEqualTo(false);
                        }
                    } else {
                        assertThat(phase.targetClockEvidence()).doesNotContainKeys("rootCpuDiagnostics", "rootCpuDiagnosticScope");
                    }
                });
                if (writeReturnDiagnostics) {
                    evidence.phases().forEach(phase -> {
                        @SuppressWarnings("unchecked")
                        Map<String, Object> capture = (Map<String, Object>) phase.targetClockEvidence()
                                .get("writeReturnDiagnostics");
                        assertThat(capture).containsEntry("performanceAcceptanceEligible", false)
                                .containsEntry("samplingCostQualified", false);
                        if (methodControl || collectorCalibration == RealBenchmarkForkDriver.CollectorCalibration.OFF) {
                            assertThat(capture).containsEntry("state", collectorCalibration == RealBenchmarkForkDriver.CollectorCalibration.OFF
                                            ? "RETURN_COLLECTOR_CALIBRATION_OFF" : "RETURN_METHOD_COST_CONTROL")
                                    .containsEntry("actualProducerEnabled", false)
                                    .containsEntry("actualPeriodicClockEnabled", false);
                            assertThat(capture).doesNotContainKeys("clockSamples", "pagesBase64", "fullRows", "fixedCohortRows");
                            assertThat((Map<?, ?>) capture.get("p99LatencyNanos")).isEqualTo(Map.of(
                                    "state", "UNAVAILABLE", "reason", collectorCalibration == RealBenchmarkForkDriver.CollectorCalibration.OFF
                                            ? "RETURN_COLLECTOR_CALIBRATION_DISABLED_PRODUCER" : "RETURN_METHOD_COST_CONTROL_DISABLED_PRODUCER"));
                        } else {
                            assertThat(capture).containsEntry("clockSamplingMode", returnClockMode.name());
                            if (nominalReturnDiagnostics) {
                                assertThat(capture).containsEntry("state", "CONDITIONAL_UNROUNDED_NOMINAL_RETURN_BOUNDS")
                                        .containsEntry("returnTimeCoordinate", "UNROUNDED_NOMINAL_COUNTER_ENCLOSURES");
                                for (String flag : BenchmarkNativeClockEvidence.FLAGS) {
                                    assertThat(capture.get(flag)).isEqualTo(false);
                                }
                            }
                        }
                        if (costStages) {
                            assertThat(((Map<?, ?>) capture.get("producerCostStages")).get("state"))
                                    .isEqualTo("RECORDED");
                        } else { assertThat(capture).doesNotContainKey("producerCostStages"); }
                        if (!methodControl && collectorCalibration != RealBenchmarkForkDriver.CollectorCalibration.OFF
                                && returnClockMode == BenchmarkReturnClockSampler.Mode.FIRST_FINAL_CONTROL) {
                            assertThat((List<?>) capture.get("clockSamples")).hasSize(2);
                            assertThat((Map<?, ?>) capture.get("p99LatencyNanos")).isEqualTo(Map.of(
                                    "state", "UNAVAILABLE", "reason", "FIRST_FINAL_CLOCK_COST_CONTROL"));
                        }
                    });
                }
                long sourceChanges = evidence.phases().stream()
                        .mapToLong(RealBenchmarkForkDriver.MeasuredPhase::expectedSourceChanges).sum();
                long sourceIssueNanos = evidence.phases().stream().mapToLong(phase ->
                        phase.sourceIssueDurationNanos().orElseThrow(() ->
                                new AssertionError("the actual source batch issue clock is unavailable"))).sum();
                assertThat(sourceIssueNanos).as("source issue time must be measured independently of target ACK")
                        .isPositive();
                System.out.printf("benchmark-real-fork acceptanceEvaluated=false id=%s jar=%s throughput=%s"
                                + " sourceIssueRate=%s acked=%s reportedOut=%s"
                                + " samples=%s mongoCommands=%s observedKeys=%s checksum=%s%n",
                        evidence.forkId(), evidence.applicationJar(), result.measurement().recordsOutPerSecond(),
                        sourceChanges * 1_000_000_000.0 / sourceIssueNanos,
                        evidence.phases().stream().mapToLong(
                                RealBenchmarkForkDriver.MeasuredPhase::acknowledgedOutputs).sum(),
                        evidence.phases().stream().mapToLong(
                                RealBenchmarkForkDriver.MeasuredPhase::reportedRecordsOut).sum(),
                        evidence.resources().sampleCount(), evidence.mongoCommands().totalCommands(),
                        evidence.observedTargetCoverage().size(), evidence.checksum());
                assertThat(evidence.telemetry().isPresent())
                        .isEqualTo(mode != BenchmarkCaptureCalibrationLiveRunIT.Mode.PLAIN);
                System.out.println("benchmark-real-telemetry mode=" + mode + " evidence="
                        + JsonWriter.write(PipelineBenchmarkLiveRunIT.telemetryEvidence(evidence.telemetry())));
                System.out.println("benchmark-real-table-confirmations=" + JsonWriter.write(Map.of(
                        "receipts", evidence.terminalMetaReceipts(), "sourceProofs", result.correctness().chains().stream()
                                .map(chain -> PipelineBenchmarkLiveRunIT.tableConfirmation(chain.tableConfirmation())).toList())));
                evidence.phases().forEach(phase -> {
                    BenchmarkForkEnvironment.ClockAnchor anchor = phase.clockAnchor();
                    BenchmarkResourceSampler.SamplingDiagnostics sampling = phase.resources().sampling()
                            .orElseThrow(() -> new AssertionError("a diagnostic phase needs sampling attempts"));
                    assertThat(phase.sourceBatches()).as("the retained source issue batches").isNotEmpty();
                    System.out.printf("benchmark-real-phase acceptanceEvaluated=false fork=%s phase=%s"
                                    + " anchorUtc=%s anchorBeforeNanos=%d anchorAfterNanos=%d"
                                    + " firstIssueUtcEarliest=%s firstIssueUtcLatest=%s"
                                    + " firstIssuedAtNanos=%d sourceCompletedAtNanos=%d completedAckAtNanos=%d"
                                    + " sourceBatches=%s sampling=%s%n",
                            evidence.forkId(), phase.id(), anchor.utc(), anchor.beforeNanos(), anchor.afterNanos(),
                            anchor.earliestUtc(phase.firstIssuedAtNanos()), anchor.latestUtc(phase.firstIssuedAtNanos()),
                            phase.firstIssuedAtNanos(), phase.sourceCompletedAtNanos(), phase.completedAckAtNanos(),
                            phase.sourceBatches(), sampling);
                    System.out.println("benchmark-real-confirmation-timing="
                            + JsonWriter.write(Map.of("fork", evidence.forkId(), "phase", phase.id(),
                                    "timing", PipelineBenchmarkLiveRunIT.phaseEvidence(phase).get("confirmationTiming"))));
                    if (pilot) {
                        System.out.println("benchmark-real-pilot-phase="
                                + JsonWriter.write(Map.of("fork", evidence.forkId(), "rows", workload.rows(),
                                        "phase", PipelineBenchmarkLiveRunIT.phaseEvidence(phase))));
                    }
                });
            });
            if (forkOutput != null) {
                var evidence = driver.evidence().getFirst();
                String profile = fullSettlingCalibration ? "FIXED_FULL_CDC_SETTLING_CALIBRATION"
                        : settlingCalibration ? "FIXED_FIRST_QUARTER_CDC_SETTLING_CALIBRATION"
                        : pacedCalibration ? "FIXED_PACING_CALIBRATION_5MS_50MS" : "ORIGINAL_BATCH_SCHEDULE";
                var output = new java.util.LinkedHashMap<String, Object>(Map.of(
                        "forkOutputSchemaVersion", 3,
                        "dualGcDiagnostics", dualGc == null
                                ? Map.of("schemaVersion", 1, "enabled", false) : dualGc.evidence(),
                        "formalPerformance", false,
                        "performanceAcceptanceEligible", false,
                        "acceptanceEvaluated", false,
                        "captureMode", mode.name(),
                        "jvmGapDiagnostics", jvmDiagnostics,
                        "profile", profile,
                        "measurement", PipelineBenchmarkLiveRunIT.fork(evidence, result, startedAt)));
                if (compilationDiagnostics) {
                    output.put("compilationDiagnostics", true);
                }
                if (writeReturnDiagnostics) {
                    output.put("writeReturnDiagnostics", true);
                    output.put("writeReturnMethodControl", methodControl);
                    output.put("writeReturnClockSamplingMode", returnClockMode.name());
                    output.put("writeReturnPerformanceAcceptanceEligible", false);
                    if (costStages) { output.put("writeReturnCostStages", true); }
                    if (nativeLibrary != null) { output.put("nativeClockDiagnostic", true); }
                    if (nativeCounterDomain) { output.put("nativeCounterDomainDiagnostic", true); }
                    if (nominalReturnDiagnostics) {
                        output.put("nominalReturnEnclosureDiagnostic", true);
                        output.put("nominalReturnPerformanceAcceptanceEligible", false);
                    }
                    if (collectorCalibration != null) {
                        output.put("returnCollectorCalibration", collectorCalibration.name());
                        output.put("returnCollectorCalibrationAcceptanceEligible", false);
                    }
                    output.put("retainedLegacyMeasurementEndpoint", "OPERATION_DATE_AND_OBSERVER_DIAGNOSTICS");
                }
                if (threadPointDiagnostics) {
                    output.put("threadPointDiagnostics", true);
                }
                if (evidence.phases().stream().anyMatch(phase ->
                        Boolean.TRUE.equals(phase.targetClockEvidence().get("targetWitnessDeferred")))) {
                    output.put("targetWitnessDeferred", true);
                    output.put("localDeliveryLatencyPerformanceEligible", false);
                }
                String json = JsonWriter.write(output);
                Files.writeString(forkOutput, json + "\n", StandardCharsets.UTF_8,
                        StandardOpenOption.CREATE_NEW, StandardOpenOption.WRITE);
                System.out.println("benchmark-real-fork-output=" + forkOutput);
            }
            Map<String, Object> runtime = driver.evidence().getFirst().runtimeEvidence();
            assertThat(runtime.get("status"))
                    .as("owned startup, namespace, JVM and exit facts are complete: %s", runtime.get("reasons"))
                    .isEqualTo("QUALIFIED");
        }
    }

    static boolean requireRootCpuRunServices(String workload, Runnable services) {
        boolean enabled = requireRootCpuAdmission(workload);
        boolean nominal = requireNominalReturnAdmission(workload);
        if (enabled || nominal) { services.run(); }
        return enabled;
    }

    static boolean requireNominalReturnAdmission(String workload) {
        String value = System.getProperty(RealBenchmarkForkDriver.NOMINAL_RETURN_DIAGNOSTICS_PROPERTY);
        if (value == null) { return false; }
        if (!"true".equals(value)) { return RealBenchmarkForkDriver.nominalReturnDiagnostics(value, false, null); }
        boolean diagnostics = Boolean.getBoolean(RealBenchmarkForkDriver.WRITE_RETURN_DIAGNOSTICS_PROPERTY);
        boolean pilot = Boolean.getBoolean("tapstate.e2e.benchmark-smoke.steady-pilot");
        var clocks = RealBenchmarkForkDriver.writeReturnClockMode(
                System.getProperty(RealBenchmarkForkDriver.WRITE_RETURN_CLOCK_CONTROL_PROPERTY), diagnostics, pilot);
        var arm = PipelineBenchmarkComparison.Arm.valueOf(System.getProperty(ARM_PROPERTY, "A"));
        boolean plain = "PLAIN".equals(System.getProperty(MODE_PROPERTY, "PLAIN"));
        boolean output = RealBenchmarkForkDriver.explicitCalibrationOutput();
        var calibration = RealBenchmarkForkDriver.collectorCalibration(
                System.getProperty(RealBenchmarkForkDriver.RETURN_COLLECTOR_CALIBRATION_PROPERTY), workload, arm,
                diagnostics, pilot, clocks, System.getProperty(RealBenchmarkForkDriver.NATIVE_CLOCK_LIBRARY_PROPERTY),
                plain, output, RealBenchmarkForkDriver.conflictingCalibrationControls());
        String nativeLibrary = RealBenchmarkForkDriver.nativeClockLibrary(
                System.getProperty(RealBenchmarkForkDriver.NATIVE_CLOCK_LIBRARY_PROPERTY), diagnostics, pilot, clocks,
                !"stateless".equals(workload) || !RealBenchmarkForkDriver.nativeClockArmAllowed(arm,
                        Boolean.getBoolean(RealBenchmarkForkDriver.NATIVE_COUNTER_DOMAIN_PROPERTY))
                        || !plain || !output || RealBenchmarkForkDriver.conflictingReturnControls());
        boolean nativeDomain = RealBenchmarkForkDriver.nativeCounterDomain(
                System.getProperty(RealBenchmarkForkDriver.NATIVE_COUNTER_DOMAIN_PROPERTY), nativeLibrary, diagnostics,
                pilot, clocks, RealBenchmarkForkDriver.conflictingReturnControls());
        return RealBenchmarkForkDriver.nominalReturnDiagnostics(value, nativeDomain, calibration);
    }

    static boolean requireRootCpuAdmission(String workload) {
        String value = System.getProperty(RealBenchmarkForkDriver.ROOT_CPU_DIAGNOSTICS_PROPERTY);
        if (value == null) { return false; }
        if (!"true".equals(value)) { return RealBenchmarkForkDriver.rootCpuDiagnostics(value, null); }
        boolean diagnostics = Boolean.getBoolean(RealBenchmarkForkDriver.WRITE_RETURN_DIAGNOSTICS_PROPERTY);
        boolean pilot = Boolean.getBoolean("tapstate.e2e.benchmark-smoke.steady-pilot");
        var clocks = RealBenchmarkForkDriver.writeReturnClockMode(
                System.getProperty(RealBenchmarkForkDriver.WRITE_RETURN_CLOCK_CONTROL_PROPERTY), diagnostics, pilot);
        var calibration = RealBenchmarkForkDriver.collectorCalibration(
                System.getProperty(RealBenchmarkForkDriver.RETURN_COLLECTOR_CALIBRATION_PROPERTY), workload,
                PipelineBenchmarkComparison.Arm.valueOf(System.getProperty(ARM_PROPERTY, "A")), diagnostics, pilot,
                clocks, System.getProperty(RealBenchmarkForkDriver.NATIVE_CLOCK_LIBRARY_PROPERTY),
                "PLAIN".equals(System.getProperty(MODE_PROPERTY, "PLAIN")), RealBenchmarkForkDriver.explicitCalibrationOutput(),
                RealBenchmarkForkDriver.conflictingCalibrationControls());
        return RealBenchmarkForkDriver.rootCpuDiagnostics(value, calibration);
    }

    static List<String> returnArguments(boolean methodControl, boolean costStages, String nativeLibrary) {
        var arguments = new java.util.ArrayList<>(RealBenchmarkForkDriver.returnJvmArguments(methodControl, costStages));
        if (nativeLibrary != null) {
            if (methodControl || costStages) { throw new AssertionError("native clock cannot mix other return controls"); }
            arguments.add("-D" + RealBenchmarkForkDriver.NATIVE_CLOCK_LIBRARY_PROPERTY + "=" + nativeLibrary);
        }
        return List.copyOf(arguments);
    }

    static List<String> collectorCalibrationArguments(RealBenchmarkForkDriver.CollectorCalibration mode, String nativeLibrary) {
        if (mode == null || nativeLibrary == null) { throw new AssertionError("collector calibration child arguments require their explicit mode and library"); }
        RealBenchmarkForkDriver.nativeClockLibrary(nativeLibrary, true, true, BenchmarkReturnClockSampler.Mode.PERIODIC, false);
        return List.of("-Dtapstate.benchmark.write-return=" + (mode == RealBenchmarkForkDriver.CollectorCalibration.ON),
                "-D" + RealBenchmarkForkDriver.NATIVE_CLOCK_LIBRARY_PROPERTY + "=" + nativeLibrary);
    }

    private static Path forkOutput() throws Exception {
        String configured = System.getProperty(OUTPUT_PROPERTY);
        if (configured == null) { return null; }
        Path output = Path.of(configured);
        if (!output.isAbsolute() || output.getFileName() == null) {
            throw new IllegalArgumentException("diagnostic fork output must be an absolute file path");
        }
        output = output.normalize();
        if (!Files.isDirectory(output.getParent())) {
            throw new IllegalArgumentException("diagnostic fork output requires an existing parent directory");
        }
        if (Files.exists(output)) {
            throw new IllegalArgumentException("diagnostic fork output already exists");
        }
        PipelineBenchmarkLiveRunIT.requireSafeOutput(output, PipelineBenchmarkLiveRunIT.harnessRoot());
        return output;
    }
}
