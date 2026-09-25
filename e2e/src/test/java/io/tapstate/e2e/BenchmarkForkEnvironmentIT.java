package io.tapstate.e2e;

import io.tapstate.testsupport.DockerGate;
import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/** Real-connector correctness smoke for one fresh fork of every frozen workload. */
class BenchmarkForkEnvironmentIT {

    private static final String BOOT_JAR_PROPERTY = "tapstate.e2e.benchmark-smoke.jar";

    @BeforeAll
    static void requireDockerConnectorsAndJar() {
        DockerGate.require();
        RealConnectorGate.require("mysql", "postgres", "mongodb");
        String configured = System.getProperty(BOOT_JAR_PROPERTY);
        Assumptions.assumeTrue(configured != null && !configured.isBlank(),
                "no -D" + BOOT_JAR_PROPERTY + ": skipping an explicit-JAR benchmark smoke");
        assertThat(Files.isRegularFile(Path.of(configured))).as("the configured boot JAR exists").isTrue();
    }

    @Test
    void copySnapshotAndCdcReachEveryFrozenTargetAnswer() throws Exception {
        run("copy");
    }

    @Test
    void statelessSnapshotAndCdcReachEveryFrozenTargetAnswer() throws Exception {
        run("stateless");
    }

    @Test
    void statefulJoinAndNestReachEveryFrozenTargetAnswer() throws Exception {
        run("stateful");
    }

    private static void run(String workloadId) throws Exception {
        BenchmarkWorkloadDefinitions.Workload workload = BenchmarkWorkloadDefinitions.byId(workloadId);
        Path jar = Path.of(System.getProperty(BOOT_JAR_PROPERTY));
        try (BenchmarkForkEnvironment fork = BenchmarkForkEnvironment.open(
                workload, jar, workloadId + "-reference-smoke")) {
            // This checks the exact target data on a real process and real connectors. The phase
            // pacing is deliberately disabled here; these durations are not benchmark evidence.
            List<BenchmarkForkEnvironment.PhaseResult> results = fork.runAllPhases(false);
            assertThat(results).hasSize(workload.phases().size());
            for (BenchmarkForkEnvironment.PhaseResult result : results) {
                assertThat(result.targets()).allSatisfy(target -> assertThat(target.matches())
                        .as(workloadId + " / " + result.phase().id() + " / "
                                + target.expectation().table())
                        .isTrue());
                System.out.printf("benchmark-correctness-smoke workload=%s phase=%s"
                                + " pacing=disabled targets=%s%n",
                        workloadId, result.phase().id(), result.targets().stream()
                                .map(target -> target.expectation().table() + ":" + target.rows()
                                        + ":" + target.checksum())
                                .toList());
            }
        }
    }
}
