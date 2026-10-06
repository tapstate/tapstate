package io.tapstate.e2e;

import io.tapstate.testsupport.DockerGate;
import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.Connection;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.UUID;

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

    private record LogicalSlot(String name, boolean active) { }

    @Test
    void anUnconfirmedExternalBorrowerPreservesNativeAndControlledSlotsAfterTheOwnedBootEnds() throws Exception {
        var workload = BenchmarkWorkloadDefinitions.byId("stateless");
        Path jar = Path.of(System.getProperty(BOOT_JAR_PROPERTY));
        Map<String, Object> application = PipelineBenchmarkLiveRunIT.artifact(jar);
        Map<String, Map<String, Object>> connectors = Map.of(
                "postgres", PipelineBenchmarkLiveRunIT.artifact(ConnectorJars.pathFor("postgres")),
                "mongodb", PipelineBenchmarkLiveRunIT.artifact(ConnectorJars.pathFor("mongodb")));
        var fork = BenchmarkForkEnvironment.open(workload, jar, "stateless-external-borrower-control");
        Map<String, Object> source = fork.sourceSettings();
        List<String> controlledNames = new ArrayList<>();
        Throwable primary = null;
        try (fork) {
            List<LogicalSlot> nativeSlots = Await.answered("the actual stateless fork's native logical slot",
                    Duration.ofSeconds(60), () -> {
                        var slots = logicalSlots(source);
                        return slots.size() == 1 && slots.getFirst().active()
                                ? java.util.Optional.of(slots) : java.util.Optional.empty();
                    });
            controlledNames.add(nativeSlots.getFirst().name());
            fork.registerUnconfirmedExternalPostgresBorrower("controlled external pgoutput owner without terminal receipt");
            String externalName = "fixture_external_" + UUID.randomUUID().toString().replace("-", "").substring(0, 12);
            try (Connection connection = SharedPostgres.connect(source);
                    var create = connection.prepareStatement("SELECT * FROM pg_create_logical_replication_slot(?, 'pgoutput')")) {
                create.setString(1, externalName);
                create.setQueryTimeout(10);
                create.execute();
                controlledNames.add(externalName);
            }
            assertThat(logicalSlots(source).stream().map(LogicalSlot::name).toList())
                    .as("both the genuine native fork slot and the independent controlled SQL slot exist")
                    .containsExactlyInAnyOrderElementsOf(controlledNames);
            fork.close();
            assertThat(fork.server().terminated()).as("the actual owned fork process ended normally").isTrue();
            // This inventory assertion, before any wait, is the discriminator when only the borrower
            // registration above is removed: a successful boot close must not drop either real slot.
            assertThat(logicalSlots(source).stream().map(LogicalSlot::name).toList())
                    .as("unconfirmed external ownership preserves both actual slots after owned boot close")
                    .containsExactlyInAnyOrderElementsOf(controlledNames);
            Await.until("the ended native fork backend to release its retained logical slot", Duration.ofSeconds(60),
                    () -> {
                        var slots = logicalSlots(source);
                        assertThat(slots.stream().map(LogicalSlot::name).toList()).containsExactlyInAnyOrderElementsOf(controlledNames);
                        return slots.stream().noneMatch(LogicalSlot::active);
                    }, () -> String.valueOf(logicalSlots(source)));
            var retained = logicalSlots(source);
            assertThat(retained).allMatch(slot -> !slot.active());
            System.out.printf("postgres-external-borrower-control database=%s slots=%s ownedBootTerminated=%s%n",
                    source.get("database"), retained, fork.server().terminated());
            assertThat(PipelineBenchmarkLiveRunIT.artifact(jar)).isEqualTo(application);
            connectors.forEach((id, expected) -> assertThat(PipelineBenchmarkLiveRunIT.artifact(ConnectorJars.pathFor(id)))
                    .as("the actual connector input remains byte-identical: %s", id).isEqualTo(expected));
        } catch (Exception | Error failure) {
            primary = failure;
            throw failure;
        } finally {
            try {
                if (!fork.server().terminated()) {
                    throw new AssertionError("controlled slot cleanup cannot run while the actual owned fork is alive");
                }
                releaseControlledSlots(source, controlledNames);
            } catch (Exception | Error cleanup) {
                if (primary == null) { throw cleanup; }
                if (primary != cleanup) { primary.addSuppressed(cleanup); }
            }
        }
    }

    private static List<LogicalSlot> logicalSlots(Map<String, Object> settings) {
        List<LogicalSlot> slots = new ArrayList<>();
        try (Connection connection = SharedPostgres.connect(settings);
                var query = connection.prepareStatement("SELECT slot_name, active FROM pg_replication_slots "
                        + "WHERE database = ? AND slot_type = 'logical' ORDER BY slot_name")) {
            query.setString(1, String.valueOf(settings.get("database")));
            query.setQueryTimeout(10);
            query.setMaxRows(33);
            try (var rows = query.executeQuery()) {
                while (rows.next()) { slots.add(new LogicalSlot(rows.getString(1), rows.getBoolean(2))); }
            }
            assertThat(slots.size()).isLessThanOrEqualTo(32);
            return List.copyOf(slots);
        } catch (java.sql.SQLException failure) {
            throw new AssertionError("cannot read the exact controlled fork database's logical slots", failure);
        }
    }

    private static void releaseControlledSlots(Map<String, Object> settings, List<String> ownedNames) throws Exception {
        List<LogicalSlot> inventory = logicalSlots(settings);
        for (LogicalSlot slot : inventory) {
            if (!ownedNames.contains(slot.name())) { continue; }
            assertThat(slot.active()).as("the control never drops an active backend's slot").isFalse();
            try (Connection connection = SharedPostgres.connect(settings);
                    var drop = connection.prepareStatement("SELECT pg_drop_replication_slot(?)")) {
                drop.setString(1, slot.name());
                drop.setQueryTimeout(10);
                drop.execute();
            }
        }
        assertThat(logicalSlots(settings).stream().map(LogicalSlot::name).toList())
                .as("the control releases only its ended slots after the retention oracle")
                .doesNotContainAnyElementsOf(ownedNames);
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
