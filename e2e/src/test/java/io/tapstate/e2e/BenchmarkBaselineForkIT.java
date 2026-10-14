package io.tapstate.e2e;

import io.tapstate.core.common.JsonWriter;
import io.tapstate.testsupport.DockerGate;
import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.net.ServerSocket;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.OptionalInt;
import java.util.TreeMap;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * One business baseline fork of a fixed workload, recording published facts around each measured phase.
 *
 * <p>{@code FIXED_OFFER} runs the steady pilot at its fixed offer and reports the pooled delivery p50/p95/p99
 * from source issue to local target observation. {@code BACKLOG_DRAIN} writes each measured phase whole while
 * the pipelines hold, then reports records-out per second over the fixed middle cohort's target operations,
 * under the same ten-bin, five-percent half-trend rule; an unsteady drain is reported rather than failing the
 * fork, so the comparison can refuse the slot. Process facts are scraped from a Prometheus endpoint opened
 * only for this fork. The facts contract is checked after the report is written, so a refusal keeps its data.
 */
class BenchmarkBaselineForkIT {

    private static final String BOOT_JAR_PROPERTY = "tapstate.e2e.benchmark-smoke.jar";
    private static final String ARM_PROPERTY = "tapstate.e2e.benchmark-smoke.arm";
    private static final String FORK_PROPERTY = "tapstate.e2e.benchmark-smoke.fork";
    private static final String REGIME_PROPERTY = "tapstate.e2e.benchmark-baseline.regime";
    private static final String OUTPUT_PROPERTY = "tapstate.e2e.benchmark-baseline.fork-output";

    @BeforeAll
    static void requireServices() {
        String output = System.getProperty(OUTPUT_PROPERTY);
        Assumptions.assumeTrue(output != null && !output.isBlank(), "no -D" + OUTPUT_PROPERTY + ": skipping a baseline fork");
        DockerGate.require();
        RealConnectorGate.require("mysql", "postgres", "mongodb");
        String configured = System.getProperty(BOOT_JAR_PROPERTY);
        Assumptions.assumeTrue(configured != null && !configured.isBlank(),
                "no -D" + BOOT_JAR_PROPERTY + ": skipping an explicit-JAR baseline fork");
        assertThat(Files.isRegularFile(Path.of(configured))).isTrue();
    }

    @Test
    void copyBaseline() throws Exception {
        run("copy");
    }

    @Test
    void statelessBaseline() throws Exception {
        run("stateless");
    }

    @Test
    void statefulBaseline() throws Exception {
        run("stateful");
    }

    private static void run(String workloadId) throws Exception {
        Path output = Path.of(System.getProperty(OUTPUT_PROPERTY));
        assertThat(output.isAbsolute()).as("the baseline fork output path").isTrue();
        var regime = RealBenchmarkForkDriver.FactRegime.valueOf(System.getProperty(REGIME_PROPERTY, ""));
        assertThat(regime).as("the baseline fact regime").isNotEqualTo(RealBenchmarkForkDriver.FactRegime.NONE);
        boolean draining = regime == RealBenchmarkForkDriver.FactRegime.BACKLOG_DRAIN;
        Path applicationJar = Path.of(System.getProperty(BOOT_JAR_PROPERTY));
        var arm = PipelineBenchmarkComparison.Arm.valueOf(System.getProperty(ARM_PROPERTY, "A"));
        int forkNumber = Integer.parseInt(System.getProperty(FORK_PROPERTY, "1"));
        assertThat(forkNumber).as("the baseline fork number").isBetween(1, 5);
        int processPort;
        try (ServerSocket socket = new ServerSocket(0)) { processPort = socket.getLocalPort(); }
        List<String> exportArguments = List.of("--tapstate.metrics.export.prometheus.host=127.0.0.1",
                "--tapstate.metrics.export.prometheus.port=" + processPort);
        RealBenchmarkForkDriver driver = RealBenchmarkForkDriver.recordingFacts((store, operator, jar) ->
                new BenchmarkForkEnvironment.OwnedBoot(RealProcessServer.start(store, operator, jar, exportArguments), null),
                regime, OptionalInt.of(processPort));
        var workload = draining ? BenchmarkWorkloadDefinitions.drainPilot(workloadId)
                : BenchmarkWorkloadDefinitions.steadyPilot(workloadId);
        Instant startedAt = Instant.now();
        PipelineBenchmarkHarness.ForkResult result = driver.run(workload, arm, forkNumber, applicationJar);
        BenchmarkAckOracle.verify(List.of(result.correctness()));
        assertThat(result.correctness().errorTotal()).isZero();
        var recorded = driver.evidence().getLast();
        var facts = driver.phaseFactEvidence();
        assertThat(facts).hasSameSizeAs(recorded.phases());

        List<Map<String, Object>> phases = new ArrayList<>();
        long pooledDeliveries = 0;
        long pooledNanos = 0;
        boolean steady = true;
        for (int i = 0; i < recorded.phases().size(); i++) {
            var phase = recorded.phases().get(i);
            var definition = workload.phase(phase.id());
            var window = phase.deliveryTimeline().orElseThrow().operationWindow();
            String verdict = "STEADY";
            try { BenchmarkSteadyOutputWindow.requireSteady(window); }
            catch (AssertionError unsteady) { verdict = "NONSTEADY: " + unsteady.getMessage(); steady = false; }
            pooledDeliveries = Math.addExact(pooledDeliveries, window.completedDeliveries());
            pooledNanos = Math.addExact(pooledNanos, window.endedAtNanos() - window.startedAtNanos());
            Map<String, Object> entry = new LinkedHashMap<>();
            entry.put("id", phase.id());
            entry.put("expectedSourceChanges", phase.expectedSourceChanges());
            entry.put("observedDeliveries", phase.observedDeliveries());
            entry.put("reportedRecordsOut", phase.reportedRecordsOut());
            entry.put("cohortDeliveries", phase.acknowledgedOutputs());
            entry.put("sourceIssue", sourceIssue(definition, phase.sourceBatches()));
            entry.put("operationWindow", Map.of("startedAtNanos", window.startedAtNanos(), "endedAtNanos", window.endedAtNanos(),
                    "completedDeliveries", window.completedDeliveries(), "fixedBins", window.fixedBins(),
                    "halfTrend", window.halfTrend(), "recordsPerSecond", window.recordsPerSecond(),
                    "scope", "FIXED_MIDDLE_COHORT_TARGET_OPERATION_WALL_TIMES_COMMON_INTERVAL"));
            entry.put("steady", verdict);
            entry.put("resources", PipelineBenchmarkLiveRunIT.resourceEvidence(phase.resources(), phase.clockAnchor()));
            entry.put("facts", facts.get(i));
            phases.add(entry);
        }
        Map<String, Object> report = new LinkedHashMap<>();
        report.put("kind", "BUSINESS_BASELINE_FORK");
        report.put("regime", regime.name());
        report.put("id", recorded.forkId());
        report.put("workload", workloadId);
        report.put("arm", arm.name());
        report.put("ordinal", forkNumber);
        report.put("applicationJar", applicationJar.toString());
        report.put("startedAt", startedAt.toString());
        report.put("completedAt", Instant.now().toString());
        report.put("steady", steady);
        report.put("throughputRecordsPerSecond", pooledNanos <= 0 ? -1.0 : pooledDeliveries * 1e9 / pooledNanos);
        if (!draining) {
            report.put("deliveryNanos", percentiles(result.measurement().deliveryNanos()));
        }
        report.put("phases", phases);
        report.put("resources", PipelineBenchmarkLiveRunIT.resourceEvidence(recorded.resources(),
                recorded.phases().getFirst().clockAnchor()));
        report.put("mongoCommands", Map.of("byCommand", new TreeMap<>(recorded.mongoCommands().byCommand()),
                "elapsedMillis", recorded.mongoCommands().elapsedMillis()));
        report.put("checksum", recorded.checksum());
        report.put("errorTotal", recorded.errorTotal());
        report.put("runtimeEvidence", recorded.runtimeEvidence());
        report.put("performanceAcceptanceEligible", false);
        Files.writeString(output, JsonWriter.write(report) + "\n", StandardCharsets.UTF_8,
                StandardOpenOption.CREATE_NEW, StandardOpenOption.WRITE);
        Map<String, Object> summary = new LinkedHashMap<>();
        summary.put("id", recorded.forkId());
        summary.put("regime", regime.name());
        summary.put("steady", steady);
        summary.put("throughputRecordsPerSecond", report.get("throughputRecordsPerSecond"));
        summary.put("output", output.toString());
        System.out.println("benchmark-baseline-fork=" + JsonWriter.write(summary));
        for (Map<String, Object> phase : facts) {
            assertThat(phase.get("factsContract")).as("phase %s facts contract", phase.get("phase"))
                    .isInstanceOfSatisfying(Map.class, contract -> assertThat(contract.get("state")).isEqualTo("PASS"));
        }
    }

    /** The pooled cohort's delivery quantiles at the same rank rule as the paced pilot's p99: the ceil(q*n)-th smallest. */
    static Map<String, Object> percentiles(long[] deliveryNanos) {
        if (deliveryNanos.length < 10_000) { throw new AssertionError("a delivery quantile needs ten thousand deliveries"); }
        long[] sorted = deliveryNanos.clone();
        Arrays.sort(sorted);
        Map<String, Object> out = new LinkedHashMap<>();
        for (int percent : new int[] {50, 95, 99}) {
            int rank = (int) ((percent * (long) sorted.length + 99) / 100) - 1;
            out.put("p" + percent, sorted[rank]);
        }
        out.put("deliveries", sorted.length);
        out.put("scope", "SOURCE_ISSUE_TO_LOCAL_TARGET_OBSERVATION_FIXED_MIDDLE_COHORT");
        return out;
    }

    /** The issue span against the declared offer; a fixed-offer phase holds its pace within five percent. */
    static Map<String, Object> sourceIssue(BenchmarkWorkloadDefinitions.Phase phase,
            List<BenchmarkForkEnvironment.BatchResult> batches) {
        Map<String, Object> out = new LinkedHashMap<>();
        long span = batches.getLast().issuedAtNanos() - batches.getFirst().issuedAtNanos();
        out.put("batches", batches.size());
        out.put("issueSpanNanos", span);
        out.put("sourceCompletedSpanNanos", batches.getLast().completedAtNanos() - batches.getFirst().issuedAtNanos());
        long interval = phase.batchInterval().toNanos();
        if (interval > 0) {
            long nominal = Math.multiplyExact(interval, batches.size() - 1L);
            out.put("nominalSpanNanos", nominal);
            out.put("paceHeld", span >= nominal * 0.95 && span <= nominal * 1.05);
        }
        return out;
    }
}
