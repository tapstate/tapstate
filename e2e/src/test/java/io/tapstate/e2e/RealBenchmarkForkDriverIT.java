package io.tapstate.e2e;

import io.tapstate.testsupport.DockerGate;
import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/** A real-process fork proves that external delivery timing and terminal ACK agree. */
class RealBenchmarkForkDriverIT {

    private static final String BOOT_JAR_PROPERTY = "tapstate.e2e.benchmark-smoke.jar";

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

    private static void run(String workloadId) throws Exception {
        RealBenchmarkForkDriver driver = new RealBenchmarkForkDriver();
        PipelineBenchmarkHarness.ForkResult result = driver.run(
                BenchmarkWorkloadDefinitions.byId(workloadId), PipelineBenchmarkComparison.Arm.A,
                1, Path.of(System.getProperty(BOOT_JAR_PROPERTY)));

        BenchmarkAckOracle.verify(List.of(result.correctness()));
        assertThat(result.measurement().deliveryNanos()).hasSize(12_000);
        assertThat(result.measurement().recordsOutPerSecond()).isPositive();
        assertThat(result.correctness().errorTotal()).isZero();
        assertThat(driver.evidence()).singleElement().satisfies(evidence -> {
            assertThat(evidence.phases()).hasSize(1);
            assertThat(evidence.resources().sampleCount()).isGreaterThan(1);
            assertThat(evidence.mongoCommands().totalCommands()).isPositive();
            assertThat(evidence.observedTargetCoverage())
                    .hasSize(workloadId.equals("copy") ? 12_001 : 12_002);
            assertThat(evidence.observedTargetCoverage().values()).containsOnly(1L);
            System.out.printf("benchmark-real-fork id=%s jar=%s throughput=%s"
                            + " acked=%s samples=%s mongoCommands=%s observedKeys=%s checksum=%s%n",
                    evidence.forkId(), evidence.applicationJar(), result.measurement().recordsOutPerSecond(),
                    evidence.phases().getFirst().acknowledgedOutputs(),
                    evidence.resources().sampleCount(), evidence.mongoCommands().totalCommands(),
                    evidence.observedTargetCoverage().size(), evidence.checksum());
        });
    }
}
