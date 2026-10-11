package io.tapstate.e2e;

import io.tapstate.adapters.pdk.ConnectorError;
import io.tapstate.core.lifecycle.LifecycleVerb;
import io.tapstate.core.lifecycle.PipelineState;
import io.tapstate.runtime.engine.EngineError;
import io.tapstate.testsupport.DockerGate;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import static org.assertj.core.api.Assertions.assertThat;

/** A connector failure remains an operator's FAILED run when its actuation claim changes hands. */
class AnIndependentFailureStaysFailedAfterItsDriverLeavesIT {

    private static final String PIPELINE = "failed_before_handover";
    private static final String SOURCE = "failed_before_handover_src";
    private static final String TARGET = "failed_before_handover_tgt";
    private static final String TABLE = "orders";
    private static final Duration TAKEOVER = Duration.ofMinutes(3);
    private static final String REJECTED_WRITE =
            "the 'fail_writes' setting makes this sink reject every write";

    @BeforeAll
    static void requireDocker() {
        DockerGate.require();
    }

    @Test
    void aFailedSinkIsNotRetriedWhenItsDriverLeavesLater(@TempDir Path directory) throws IOException {
        byte[] connector = Files.readAllBytes(E2eConnectorJar.buildInto(directory));
        Path source = Files.createDirectories(directory.resolve("src"));
        Path target = Files.createDirectories(directory.resolve("tgt"));

        try (FileEndpoints files = new FileEndpoints()) {
            files.seed(EndpointAddress.uri(source.toString()), TABLE, SeedRows.generated(3));
            String store = SharedMongo.replicaSetUrl("e2e_independent_failure_handover");
            try (TwoMemberCluster cluster = TwoMemberCluster.start(store, "e2e-independent-failure")) {
                cluster.awaitBothMembers();
                ControlPlane control = cluster.first();
                control.registerConnector(E2eConnectorJar.CONNECTOR_ID, connector);
                control.discoverSchema(SOURCE, E2eConnectorJar.CONNECTOR_ID,
                        Map.of("uri", source.toString()));
                control.apply(resources(source, target));
                control.lifecycle(PIPELINE, LifecycleVerb.START);

                Await.until("the sink's own failure to be recorded", Duration.ofMinutes(1),
                        () -> control.state(PIPELINE).filter(PipelineState.FAILED::equals).isPresent(),
                        () -> "state = " + control.state(PIPELINE));
                ControlPlane.Failure failure = control.failure(PIPELINE).orElseThrow();
                String driver = control.pipelineControllerOf(PIPELINE).orElseThrow();
                Map<String, Boolean> sinkFailureReported = new LinkedHashMap<>();
                for (String node : List.of(TwoMemberCluster.NODE_A, TwoMemberCluster.NODE_B)) {
                    String actualLog = Files.readString(cluster.processCarrying(node).output());
                    sinkFailureReported.put(node, actualLog.lines().anyMatch(line ->
                            line.contains(ConnectorError.WRITE_FAILED.code()) && line.contains(REJECTED_WRITE)));
                }
                assertThat(failure.code())
                        .as("the published failure before handover; controller=%s, rejected-write reports=%s",
                                driver, sinkFailureReported)
                        .isIn(ConnectorError.WRITE_FAILED.code(), EngineError.JOB_FAILED.code());
                if (ConnectorError.WRITE_FAILED.code().equals(failure.code())) {
                    assertThat(failure.params()).containsEntry("connector", E2eConnectorJar.CONNECTOR_ID)
                            .containsEntry("detail", REJECTED_WRITE);
                } else {
                    // Jet can discard a remote cause. The actuation owner need not run the failing writer.
                    assertThat(failure.params()).containsEntry("pipeline", PIPELINE);
                    assertThat(sinkFailureReported)
                            .as("an actual member must report the configured write rejection before its controller leaves")
                            .containsValue(true);
                }
                long failedExecution = control.executionGenerationOf(PIPELINE).orElseThrow();
                String newHolder = TwoMemberCluster.NODE_A.equals(driver)
                        ? TwoMemberCluster.NODE_B : TwoMemberCluster.NODE_A;
                ControlPlane survivor = cluster.memberOtherThan(driver);

                cluster.processCarrying(driver).kill();

                Await.until("the failed pipeline's claim to change hands", TAKEOVER,
                        () -> survivor.pipelineControllerOf(PIPELINE).filter(newHolder::equals).isPresent(),
                        () -> "controller = " + survivor.pipelineControllerOf(PIPELINE));
                Await.until("the new holder to publish its own FAILED observation", TAKEOVER,
                        () -> survivor.observedAgeMillis(PIPELINE).filter(age -> age < 1_000).isPresent(),
                        () -> "observation age = " + survivor.observedAgeMillis(PIPELINE));

                assertThat(survivor.state(PIPELINE)).contains(PipelineState.FAILED);
                assertThat(survivor.executionGenerationOf(PIPELINE))
                        .as("the failure preceded the handover, so the new holder must not submit a run")
                        .contains(failedExecution);
            }
        }
    }

    private static Map<String, String> resources(Path source, Path target) {
        Map<String, String> resources = new LinkedHashMap<>();
        resources.put(SOURCE + ".tap.yml", """
                version: tapstate/v1
                kind: source
                id: %s
                connector: %s
                config: { uri: "%s" }
                mode: cdc
                tables: [ orders ]
                """.formatted(SOURCE, E2eConnectorJar.CONNECTOR_ID, source));
        resources.put(TARGET + ".tap.yml", """
                version: tapstate/v1
                kind: source
                id: %s
                connector: %s
                config: { uri: "%s", fail_writes: true }
                """.formatted(TARGET, E2eConnectorJar.CONNECTOR_ID, target));
        resources.put(PIPELINE + ".tap.yml", """
                version: tapstate/v1
                kind: pipeline
                id: %s
                source: %s
                settings: { read_mode: snapshot_only }
                transforms:
                  - { id: snapshot_rows, from: [orders], type: filter, expr: "op == 'r'" }
                serve:
                  from: snapshot_rows
                  sync:
                    - source: %s
                """.formatted(PIPELINE, SOURCE, TARGET));
        return resources;
    }
}
