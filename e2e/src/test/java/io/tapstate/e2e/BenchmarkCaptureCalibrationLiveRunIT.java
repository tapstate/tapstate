package io.tapstate.e2e;

import com.mongodb.ConnectionString;
import io.tapstate.testsupport.RequiresDocker;
import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.Test;

import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Instant;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

/** Runs the unchanged real COPY workload in three capture modes without evaluating a performance gate. */
@RequiresDocker
class BenchmarkCaptureCalibrationLiveRunIT {
    private static final String PREFIX = "tapstate.e2e.capture-calibration.";
    private static final int FORKS_PER_MODE = 5;
    private static final List<List<Mode>> GROUPS = List.of(
            List.of(Mode.PLAIN, Mode.PASSIVE_JDWP, Mode.ACTIVE_CAPTURE),
            List.of(Mode.ACTIVE_CAPTURE, Mode.PASSIVE_JDWP, Mode.PLAIN),
            List.of(Mode.PASSIVE_JDWP, Mode.ACTIVE_CAPTURE, Mode.PLAIN),
            List.of(Mode.PLAIN, Mode.ACTIVE_CAPTURE, Mode.PASSIVE_JDWP),
            List.of(Mode.ACTIVE_CAPTURE, Mode.PLAIN, Mode.PASSIVE_JDWP));

    enum Mode {
        PLAIN, PASSIVE_JDWP, ACTIVE_CAPTURE;

        RealBenchmarkForkDriver driver(Path applicationJar, BenchmarkJdiCostObserver.Artifact artifact) {
            if (this == PLAIN) { return new RealBenchmarkForkDriver(); }
            if (artifact == null) { throw new IllegalArgumentException("capture mode requires a pinned artifact"); }
            return new RealBenchmarkForkDriver((storeUri, operatorDatabase, jar) -> {
                if (!jar.equals(applicationJar)) { throw new AssertionError("calibration artifact changed"); }
                var session = BenchmarkJdiTelemetrySession.launch(artifact, storeUri,
                        new ConnectionString(storeUri).getDatabase(), operatorDatabase,
                        BenchmarkJdiTelemetrySession.Mode.valueOf(name()));
                return new BenchmarkForkEnvironment.OwnedBoot(session.server(), session);
            });
        }
    }

    @Test
    void oneImmutableArtifactKeepsFiveRealForksForEachCaptureMode() throws Exception {
        boolean requested = List.of("jar", "output").stream()
                .anyMatch(name -> System.getProperty(PREFIX + name) != null);
        Assumptions.assumeTrue(requested, "no capture-calibration properties supplied; this diagnostic is opt-in");
        Path jar = Path.of(required("jar")).toAbsolutePath().normalize();
        if (!Files.isRegularFile(jar)) { throw new IllegalArgumentException("calibration jar is not a regular file"); }
        requireConnectors();
        Path root = PipelineBenchmarkLiveRunIT.harnessRoot();
        Path output = Path.of(required("output"));
        PipelineBenchmarkLiveRunIT.requireSafeOutput(output, root);
        Map<String, Object> revision = PipelineBenchmarkLiveRunIT.harnessRevision(root);
        BenchmarkLiveReport report = new BenchmarkLiveReport(output);
        try {
            String sha = PipelineBenchmarkLiveRunIT.sha256(jar);
            Map<String, Object> connectors = new LinkedHashMap<>();
            for (String id : List.of("mysql", "postgres", "mongodb")) {
                connectors.put(id, PipelineBenchmarkLiveRunIT.artifact(ConnectorJars.pathFor(id)));
            }
            report.begin(Map.of("purpose", "CAPTURE_CALIBRATION", "performanceAcceptanceEligible", false,
                    "application", PipelineBenchmarkLiveRunIT.artifact(jar), "harness", revision,
                    "connectors", connectors, "apiVersion", System.getProperty("api.version", "UNAVAILABLE"),
                    "forksPerMode", FORKS_PER_MODE, "schedule", GROUPS.stream()
                            .map(group -> group.stream().map(Enum::name).toList()).toList()),
                    PipelineBenchmarkLiveRunIT.environment(), PipelineBenchmarkLiveRunIT.workloads().stream()
                            .filter(workload -> "copy".equals(workload.get("id"))).toList());
            List<BenchmarkAckOracle.Fork> correctness = new ArrayList<>();
            try (var artifact = BenchmarkJdiCostObserver.Artifact.open(jar,
                    BenchmarkJdiCostObserver.Arm.OBSERVABILITY)) {
                for (int group = 0; group < GROUPS.size(); group++) {
                    int forkNumber = group + 1;
                    for (Mode mode : GROUPS.get(group)) {
                        requireUnchangedArtifacts(jar, sha, connectors);
                        Instant startedAt = Instant.now();
                        RealBenchmarkForkDriver driver = mode.driver(jar, artifact);
                        var result = driver.run(BenchmarkWorkloadDefinitions.byId("copy"),
                                PipelineBenchmarkComparison.Arm.B, forkNumber, jar);
                        var evidence = driver.evidence().getLast();
                        String expectedId = "copy-B-" + forkNumber;
                        if (!expectedId.equals(evidence.forkId())
                                || !expectedId.equals(result.correctness().id())) {
                            throw new AssertionError("calibration fork has the wrong identity");
                        }
                        var recorded = new LinkedHashMap<>(PipelineBenchmarkLiveRunIT.fork(evidence, result, startedAt));
                        String id = mode.name() + "-" + expectedId;
                        recorded.put("id", id);
                        recorded.put("driverForkId", expectedId);
                        recorded.put("calibrationMode", mode.name());
                        recorded.put("group", forkNumber);
                        recorded.put("sequence", correctness.size() + 1);
                        recorded.put("artifactSha256", sha);
                        recorded.put("performanceAcceptanceEligible", false);
                        recorded.put("telemetry", PipelineBenchmarkLiveRunIT.telemetryEvidence(evidence.telemetry()));
                        report.addFork(recorded);
                        verifyCapture(mode, sha, evidence.telemetry());
                        var source = result.correctness();
                        correctness.add(new BenchmarkAckOracle.Fork(id, source.chains(), source.logicalCoverage(),
                                source.checksum(), source.errorTotal()));
                        BenchmarkAckOracle.verify(correctness);
                        requireUnchangedArtifacts(jar, sha, connectors);
                        System.out.printf("capture-calibration completed=%d/%d mode=%s group=%d throughput=%s%n",
                                correctness.size(), FORKS_PER_MODE * Mode.values().length,
                                mode, forkNumber, result.measurement().recordsOutPerSecond());
                    }
                }
            }
            report.completeDiagnostic(Map.of("completedForks", correctness.size(),
                    "correctness", "ALL_RUN_RELATIVE_TERMINAL_ACKS_COVERAGE_CHECKSUMS_AND_ERRORS_MATCHED",
                    "performanceAcceptanceEligible", false,
                    "representativeness", "NOT_ESTABLISHED_BY_COMPLETING_CALIBRATION"));
        } catch (Exception | Error failure) {
            try { report.fail(failure); } catch (RuntimeException writeFailure) { failure.addSuppressed(writeFailure); }
            throw failure;
        }
    }

    private static void requireUnchangedArtifacts(Path jar, String sha, Map<String, Object> connectors)
            throws Exception {
        if (!sha.equals(PipelineBenchmarkLiveRunIT.sha256(jar))) {
            throw new AssertionError("calibration application artifact changed");
        }
        for (var entry : connectors.entrySet()) {
            if (!entry.getValue().equals(PipelineBenchmarkLiveRunIT.artifact(ConnectorJars.pathFor(entry.getKey())))) {
                throw new AssertionError("calibration connector artifact changed: " + entry.getKey());
            }
        }
    }

    static void verifyCapture(Mode mode, String sha, Optional<BenchmarkJdiTelemetrySession.Evidence> recorded) {
        if (recorded.isPresent() != (mode != Mode.PLAIN)) {
            throw new AssertionError("calibration capture availability disagrees with its mode");
        }
        recorded.ifPresent(capture -> {
            if (!mode.name().equals(capture.mode().name()) || !sha.equals(capture.artifactSha256())
                    || (mode == Mode.ACTIVE_CAPTURE && !capture.fullyDrained())) {
                throw new AssertionError("calibration capture mode, artifact or drain is invalid");
            }
        });
    }

    static void requireConnectors() {
        if (!ConnectorJars.directoryNamed()) {
            throw new IllegalArgumentException("capture calibration requires tapstate.e2e.connectors-dir");
        }
        RealConnectorGate.require("mysql", "postgres", "mongodb");
    }

    private static String required(String suffix) {
        String value = System.getProperty(PREFIX + suffix);
        if (value == null || value.isBlank()) {
            throw new IllegalArgumentException("capture calibration requires -D" + PREFIX + suffix);
        }
        return value;
    }
}
