package io.tapstate.e2e;

import io.tapstate.core.lifecycle.LifecycleVerb;
import io.tapstate.core.lifecycle.PipelineState;
import io.tapstate.spi.store.SrsConsumerId;
import io.tapstate.testsupport.DockerGate;
import org.bson.Document;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/** An explicit position move after a completed load must allow both sinks to start again. */
class AMovedMultiSinkPositionCanRestartAfterItsLoadIT {

    private static final String PIPELINE = "moved_two_sink_pipeline";
    private static final String SOURCE = "moved_two_sink_source";
    private static final String TABLE = "orders";
    private static final String CONSUMER = SrsConsumerId.of(PIPELINE, SOURCE).value();

    @BeforeAll
    static void requireDocker() {
        DockerGate.require();
    }

    @Test
    void aPositionMovedAfterBothSinksLoadIsAnAcceptedStartingPoint(@TempDir Path temporary)
            throws Exception {
        Path source = Files.createDirectory(temporary.resolve("source"));
        Path firstTarget = Files.createDirectory(temporary.resolve("first-target"));
        Path secondTarget = Files.createDirectory(temporary.resolve("second-target"));
        EndpointAddress sourceAddress = EndpointAddress.uri(source.toString());
        EndpointAddress firstAddress = EndpointAddress.uri(firstTarget.toString());
        EndpointAddress secondAddress = EndpointAddress.uri(secondTarget.toString());
        FileEndpoints files = new FileEndpoints();
        files.seed(sourceAddress, TABLE, SeedRows.generated(1));

        String store = SharedMongo.replicaSetUrl("moved_two_sink_position");
        try (ServerHandle server = Tiers.IN_PROCESS.launch(store);
                StoreDocuments documents = StoreDocuments.at(store)) {
            ControlPlane control = new ControlPlane(server.baseUrl());
            control.bootstrapAndLogin("e2e", "e2e-password");
            control.registerConnector(E2eConnectorJar.CONNECTOR_ID,
                    Files.readAllBytes(E2eConnectorJar.buildInto(temporary)));
            control.apply(workspace(source, firstTarget, secondTarget));
            control.discoverSchema(SOURCE, E2eConnectorJar.CONNECTOR_ID, Map.of("uri", source.toString()));
            control.lifecycle(PIPELINE, LifecycleVerb.START);
            awaitState(control, PipelineState.RUNNING);

            Map<String, String> earlier = Await.answered("the loaded pipeline's resume point", () -> {
                Map<String, String> point = control.resumePoint(PIPELINE);
                return point.isEmpty() ? java.util.Optional.empty() : java.util.Optional.of(point);
            });
            String chain = earlier.get("chainId");
            Await.until("both sinks to finish their load with per-writer accounting", () -> {
                Document consumer = documents.consumerOffset(chain, CONSUMER);
                return files.count(firstAddress, TABLE) == 1
                        && files.count(secondAddress, TABLE) == 1
                        && consumer != null
                        && consumer.getList("snapshotCompletedTables", String.class, List.of()).contains(TABLE)
                        && consumer.get("writerRun") instanceof Document;
            }, () -> String.valueOf(documents.consumerOffset(chain, CONSUMER))
                    + ", state=" + control.state(PIPELINE) + ", logs=" + control.logs(PIPELINE));
            earlier = control.resumePoint(PIPELINE);
            String token = earlier.get("token");

            // The file connector issues a token at the load seam, but none with its changes. A fresh
            // load samples a newer seam, leaving the earlier product-issued token available to move to.
            control.stop(PIPELINE, true);
            awaitState(control, PipelineState.STOPPED);
            files.cdc(sourceAddress, TABLE, CdcOp.INSERT, 1);
            control.lifecycle(PIPELINE, LifecycleVerb.START);
            awaitState(control, PipelineState.RUNNING);
            Await.until("both sinks to finish a load at a position beyond the saved point", () -> {
                Document consumer = documents.consumerOffset(chain, CONSUMER);
                return files.count(firstAddress, TABLE) == 2
                        && files.count(secondAddress, TABLE) == 2
                        && consumer != null
                        && consumer.getList("snapshotCompletedTables", String.class, List.of()).contains(TABLE)
                        && consumer.get("writerRun") instanceof Document
                        && !token.equals(control.resumePoint(PIPELINE).get("token"));
            },
                    () -> control.positionReading(PIPELINE) + ", consumer="
                            + documents.consumerOffset(chain, CONSUMER)
                            + ", first=" + files.count(firstAddress, TABLE)
                            + ", second=" + files.count(secondAddress, TABLE));

            control.stop(PIPELINE, false);
            awaitState(control, PipelineState.STOPPED);
            assertThat(documents.consumerOffset(chain, CONSUMER).get("writerRun"))
                    .as("the stopped pipeline retained progress recorded separately for its writers")
                    .isInstanceOf(Document.class);
            control.writeBackPosition(PIPELINE, chain, token);
            assertThat(control.resumePoint(PIPELINE)).containsEntry("token", token);
            Document moved = documents.consumerOffset(chain, CONSUMER);
            assertThat(moved.getList("snapshotCompletedTables", String.class))
                    .as("moving the change position preserves the completed load")
                    .contains(TABLE);

            control.lifecycle(PIPELINE, LifecycleVerb.START);
            Await.until("the next start to finish planning", () ->
                    control.state(PIPELINE).filter(state -> state == PipelineState.RUNNING
                            || state == PipelineState.FAILED).isPresent(),
                    () -> "state=" + control.state(PIPELINE) + ", failure=" + control.failureCode(PIPELINE));
            assertThat(control.state(PIPELINE))
                    .as("the explicit position move accepts the next start; failure=%s, consumer after move=%s",
                            control.failureCode(PIPELINE), moved)
                    .contains(PipelineState.RUNNING);
        }
    }

    private static void awaitState(ControlPlane control, PipelineState expected) {
        Await.until("pipeline state " + expected, () -> control.state(PIPELINE).filter(expected::equals).isPresent(),
                () -> "state=" + control.state(PIPELINE) + ", failure=" + control.failureCode(PIPELINE)
                        + ", logs=" + control.logs(PIPELINE));
    }

    private static Map<String, String> workspace(Path source, Path firstTarget, Path secondTarget) {
        Map<String, String> resources = new LinkedHashMap<>();
        Map<String, Path> endpoints = new LinkedHashMap<>();
        endpoints.put(SOURCE, source);
        endpoints.put("first_target", firstTarget);
        endpoints.put("second_target", secondTarget);
        endpoints.forEach((id, directory) -> resources.put(id + ".tap.yml", """
                version: tapstate/v1
                kind: source
                id: %s
                connector: %s
                config: { uri: "%s" }
                mode: cdc
                tables: [ %s ]
                """.formatted(id, E2eConnectorJar.CONNECTOR_ID, directory, TABLE)));
        resources.put(PIPELINE + ".tap.yml", """
                version: tapstate/v1
                kind: pipeline
                id: %s
                source: [ { id: %s, srs: true } ]
                settings: { read_mode: snapshot_and_cdc }
                serve:
                  from: %s
                  sync:
                    - { id: first, source: first_target, execution: { parallelism: 1 } }
                    - { id: second, source: second_target, execution: { parallelism: 1 } }
                """.formatted(PIPELINE, SOURCE, TABLE));
        return resources;
    }
}
