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

    @BeforeAll
    static void requireServices() {
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
        Path forkOutput = forkOutput();
        PipelineBenchmarkComparison.Arm arm = PipelineBenchmarkComparison.Arm.valueOf(
                System.getProperty(ARM_PROPERTY, "A"));
        var mode = BenchmarkCaptureCalibrationLiveRunIT.Mode.valueOf(System.getProperty(MODE_PROPERTY, "PLAIN"));
        Path applicationJar = Path.of(System.getProperty(BOOT_JAR_PROPERTY));
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
            boolean jvmDiagnostics = Boolean.getBoolean("tapstate.e2e.benchmark-smoke.jvm-gap-diagnostics");
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
            RealBenchmarkForkDriver driver = dualGc != null
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
                        "forkOutputSchemaVersion", 2,
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
        }
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
