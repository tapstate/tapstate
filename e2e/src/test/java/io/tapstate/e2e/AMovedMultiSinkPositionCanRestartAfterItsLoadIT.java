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
        EndpointAddress firstAddress = EndpointAddress.uri(firstTarget.toString());
        EndpointAddress secondAddress = EndpointAddress.uri(secondTarget.toString());
        FileEndpoints files = new FileEndpoints();
        FileEndpoints.replaceTable(source.resolve(TABLE + ".csv"), "id,priority\n1,Low\n");
        CdcRecoveryFixture.change(source, 1, TABLE, "1", "Low", "Low");

        try (files; PreparedNativeStore storage = PreparedNativeStore.create();
                NativeSrsDocuments documents = new NativeSrsDocuments(storage);
                ServerHandle server = Tiers.IN_PROCESS.launch(storage)) {
            ControlPlane control = new ControlPlane(server.baseUrl());
            storage.loginAndAwaitBusiness(control);
            control.registerConnector(E2eConnectorJar.CONNECTOR_ID,
                    CdcRecoveryFixture.connectorJar(E2eConnectorJar.buildInto(temporary)));
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
                Document consumer = currentConsumer(documents, chain);
                return files.countRows(firstAddress, TABLE) == 1
                        && files.countRows(secondAddress, TABLE) == 1
                        && consumer != null
                        && consumer.get("progress", Document.class).getList("snapshotCompletedTables", String.class, List.of()).contains(TABLE)
                        && consumer.get("writerRun") instanceof Document;
            }, () -> String.valueOf(currentConsumer(documents, chain))
                    + ", state=" + control.state(PIPELINE) + ", logs=" + control.logs(PIPELINE));
            earlier = control.resumePoint(PIPELINE);
            String token = earlier.get("token");
            long originalGeneration = currentConsumer(documents, chain).getLong("stateGeneration");

            // The journal supplies a real CDC position after the next load. A newer snapshot seam
            // alone would advance the sink acknowledgement without advancing the shared capture floor.
            control.stop(PIPELINE, true);
            awaitState(control, PipelineState.STOPPED);
            FileEndpoints.replaceTable(source.resolve(TABLE + ".csv"), "id,priority\n1,Low\n2,later\n");
            CdcRecoveryFixture.change(source, 2, TABLE, "2", "", "later");
            control.lifecycle(PIPELINE, LifecycleVerb.START);
            awaitState(control, PipelineState.RUNNING);
            Await.until("both sinks to finish a load at a position beyond the saved point", () -> {
                Document consumer = currentConsumer(documents, chain);
                return files.countRows(firstAddress, TABLE) == 2
                        && files.countRows(secondAddress, TABLE) == 2
                        && consumer != null
                        && consumer.getLong("stateGeneration") > originalGeneration
                        && consumer.get("progress", Document.class).getList("snapshotCompletedTables", String.class, List.of()).contains(TABLE)
                        && consumer.get("writerRun") instanceof Document
                        && !token.equals(control.resumePoint(PIPELINE).get("token"));
            },
                    () -> control.positionReading(PIPELINE) + ", consumer="
                            + currentConsumer(documents, chain)
                            + ", first=" + files.countRows(firstAddress, TABLE)
                            + ", second=" + files.countRows(secondAddress, TABLE));

            long reloadedGeneration = currentConsumer(documents, chain).getLong("stateGeneration");
            for (Path target : List.of(firstTarget, secondTarget)) {
                assertThat(Files.readAllLines(target.resolve(TABLE + ".csv")))
                        .containsExactlyInAnyOrder("id,priority", "1,Low", "2,later");
            }
            assertThat(CdcRecoveryFixture.streams(source).stream()
                    .flatMap(path -> CdcRecoveryFixture.lines(path).stream()))
                    .as("the source delivered the newer position rather than only sampling a snapshot seam")
                    .anyMatch(line -> line.startsWith("BATCH 1 2 "));
            control.stop(PIPELINE, false);
            awaitState(control, PipelineState.STOPPED);
            assertThat(currentConsumer(documents, chain).getLong("stateGeneration")).isEqualTo(reloadedGeneration);
            assertThat(currentConsumer(documents, chain).get("writerRun"))
                    .as("the stopped pipeline retained progress recorded separately for its writers")
                    .isInstanceOf(Document.class);
            control.writeBackPosition(PIPELINE, chain, token);
            assertThat(control.resumePoint(PIPELINE)).containsEntry("token", token);
            Document moved = currentConsumer(documents, chain);
            assertThat(moved.getLong("stateGeneration")).isEqualTo(reloadedGeneration);
            assertThat(moved.get("progress", Document.class).getList("snapshotCompletedTables", String.class))
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

    /** The current native membership and cursor must describe the same physical state generation. */
    private static Document currentConsumer(NativeSrsDocuments documents, String chain) {
        Document member = documents.pipelineMembership(PIPELINE, SOURCE);
        if (member == null) return null;
        assertThat(member.get("_id", Document.class).getString("chain")).isEqualTo(chain);
        Document cursor = documents.consumerOffset(chain, CONSUMER);
        if (cursor == null || !member.get("stateGeneration").equals(cursor.get("stateGeneration"))
                || !member.get("revision").equals(cursor.get("membershipRevision"))) return null;
        return cursor;
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
