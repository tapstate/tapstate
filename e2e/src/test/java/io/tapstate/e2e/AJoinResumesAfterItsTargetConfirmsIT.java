package io.tapstate.e2e;

import static org.assertj.core.api.Assertions.assertThat;

import io.tapstate.core.lifecycle.LifecycleVerb;
import io.tapstate.core.lifecycle.PipelineState;
import io.tapstate.testsupport.DockerGate;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

/** The specification vocabulary cannot read load confirmations or the restarted run's snapshot count. */
class AJoinResumesAfterItsTargetConfirmsIT {

    private static final String PIPELINE = "join_resume";
    private static final Duration BOUND = Duration.ofSeconds(60);

    @BeforeAll
    static void requireDocker() {
        DockerGate.require();
    }

    @ParameterizedTest
    @EnumSource(Tiers.class)
    void aRestartSkipsBothJoinedTablesOnceTheirRowsHaveLanded(Tiers tier, @TempDir Path directory)
            throws Exception {
        Path source = Files.createDirectory(directory.resolve("source"));
        Path target = Files.createDirectory(directory.resolve("target"));
        FileEndpoints.replaceTable(source.resolve("orders.csv"), "id,seq\n10,1\n11,1\n");
        FileEndpoints.replaceTable(source.resolve("customers.csv"), "id,seq\n1,7\n");
        try (PreparedNativeStore storage = PreparedNativeStore.create();
             ServerHandle server = tier.launch(storage); FileEndpoints files = new FileEndpoints()) {
            ControlPlane control = new ControlPlane(server.baseUrl());
            storage.loginAndAwaitBusiness(control);
            control.registerConnector(E2eConnectorJar.CONNECTOR_ID,
                    Files.readAllBytes(E2eConnectorJar.buildInto(directory)));
            control.apply(workspace(source, target));
            control.discoverSchema("src", E2eConnectorJar.CONNECTOR_ID, Map.of("uri", source.toString()));
            control.lifecycle(PIPELINE, LifecycleVerb.START);
            EndpointAddress destination = EndpointAddress.uri(target.toString());
            awaitJoined(files, destination, 7L);
            Await.until("both joined loads to be confirmed by the target", BOUND,
                    () -> control.snapshotTables(PIPELINE).keySet().containsAll(List.of("orders", "customers"))
                            && control.snapshotTables(PIPELINE).values().stream().allMatch(reading -> reading.landed()),
                    () -> "snapshot=" + control.snapshotTables(PIPELINE)
                            + ", position=" + control.positionReading(PIPELINE));
            control.stop(PIPELINE, false);
            Await.until("the pipeline to stop with its positions kept", BOUND,
                    () -> control.state(PIPELINE).filter(PipelineState.STOPPED::equals).isPresent(),
                    () -> String.valueOf(control.state(PIPELINE)));
            control.lifecycle(PIPELINE, LifecycleVerb.START);
            Await.until("the replacement run to start", BOUND,
                    () -> control.state(PIPELINE).filter(PipelineState.RUNNING::equals).isPresent(),
                    () -> String.valueOf(control.state(PIPELINE)));

            // This connector's tail emits inserts. New rows on both inputs prove the replacement is live.
            FileEndpoints.replaceTable(source.resolve("customers.csv"), "id,seq\n1,7\n2,8\n");
            FileEndpoints.replaceTable(source.resolve("orders.csv"), "id,seq\n10,1\n11,1\n12,2\n");
            Await.until("a new order to join a customer inserted after the restart", BOUND,
                    () -> files.count(destination, "orders") == 3
                            && files.fetch(destination, "orders", Map.of("id", 12L))
                                    .map(row -> row.get("seq") instanceof Number number && number.longValue() == 8L)
                                    .orElse(false),
                    () -> "new order=" + files.fetch(destination, "orders", Map.of("id", 12L)));
            Await.until("the replacement's snapshot reading to be published", BOUND,
                    () -> control.snapshotRowsRead(PIPELINE).keySet().containsAll(List.of("orders", "customers")),
                    () -> String.valueOf(control.snapshotRowsRead(PIPELINE)));
            assertThat(control.snapshotRowsRead(PIPELINE))
                    .as("the replacement reads neither joined table's completed snapshot again")
                    .containsEntry("orders", 0L).containsEntry("customers", 0L);
        }
    }

    private static void awaitJoined(FileEndpoints files, EndpointAddress destination, long sequence) {
        Await.until("both orders to carry the current customer value", BOUND,
                () -> files.count(destination, "orders") == 2
                        && List.of(10L, 11L).stream().allMatch(id -> files.fetch(destination, "orders", Map.of("id", id))
                                .map(row -> row.get("seq") instanceof Number number && number.longValue() == sequence)
                                .orElse(false)),
                () -> "order 10=" + files.fetch(destination, "orders", Map.of("id", 10L))
                        + ", order 11=" + files.fetch(destination, "orders", Map.of("id", 11L)));
    }

    private static Map<String, String> workspace(Path source, Path target) {
        Map<String, String> resources = new LinkedHashMap<>();
        resources.put("src.tap.yml", """
                version: tapstate/v1
                kind: source
                id: src
                connector: %s
                config: { uri: "%s" }
                mode: cdc
                tables: [ orders, customers ]
                """.formatted(E2eConnectorJar.CONNECTOR_ID, source));
        resources.put("target.tap.yml", Workspaces.targetYaml("target", target));
        resources.put("pipeline.tap.yml", """
                version: tapstate/v1
                kind: pipeline
                id: join_resume
                source: [ { id: src, srs: true } ]
                settings: { read_mode: snapshot_and_cdc }
                transforms:
                  - id: joined
                    type: join
                    from: { o: orders, c: customers }
                    engine: builtin
                    sql: |
                      SELECT o.id AS id, c.seq AS seq
                      FROM o JOIN c ON o.seq = c.id
                serve:
                  from: joined
                  sync: [ { id: out, source: target } ]
                """);
        return resources;
    }
}
