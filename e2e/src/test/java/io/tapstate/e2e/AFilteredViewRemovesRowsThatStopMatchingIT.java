package io.tapstate.e2e;

import static org.assertj.core.api.Assertions.assertThat;

import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoCollection;
import io.tapstate.core.lifecycle.LifecycleVerb;
import io.tapstate.core.lifecycle.PipelineState;
import io.tapstate.testsupport.DockerGate;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import org.bson.Document;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

/** The file connector's declarative tail emits inserts only; this journal supplies real before/after updates. */
class AFilteredViewRemovesRowsThatStopMatchingIT {

    private static final String PIPELINE = "in_transit_shipments_pipeline";
    private static final String VIEW = "in_transit_shipments";

    @BeforeAll
    static void requireDocker() {
        DockerGate.require();
    }

    @ParameterizedTest
    @EnumSource(Tiers.class)
    void aShipmentLeavesTheViewWhenItStopsMatching(Tiers tier, @TempDir Path directory) throws Exception {
        Path source = Files.createDirectory(directory.resolve("source"));
        FileEndpoints.replaceTable(source.resolve("shipments.csv"),
                "id,priority\n2,in_transit\n6,in_transit\n8,delivered\n");
        Path delegate = E2eConnectorJar.buildInto(Files.createDirectory(directory.resolve("connector")));
        byte[] mongoConnector = ConnectorJars.managedMongoBytes();
        try (PreparedNativeStore storage = PreparedNativeStore.create();
             ServerHandle server = tier.launch(storage); var targetClient = MongoClients.create(storage.masterUri())) {
            ControlPlane control = new ControlPlane(server.baseUrl());
            storage.loginAndAwaitBusiness(control);
            control.registerConnector("mongodb",mongoConnector);
            control.registerConnector(E2eConnectorJar.CONNECTOR_ID, CdcRecoveryFixture.connectorJar(delegate));
            var target = targetClient.getDatabase(storage.masterDatabase()).getCollection(VIEW);
            Map<String, String> resources = workspace(source);
            control.apply(Map.of("fulfillment_db.tap.yml", resources.get("fulfillment_db.tap.yml")));
            control.discoverSchema("fulfillment_db", E2eConnectorJar.CONNECTOR_ID, Map.of("uri", source.toString()));
            control.apply(resources);
            control.lifecycle(PIPELINE, LifecycleVerb.START);
            awaitRows(target, control, Set.of("2,in_transit", "6,in_transit"));

            CdcRecoveryFixture.change(source, 1, "shipments", "2", "in_transit", "delivered");
            awaitRows(target, control, Set.of("6,in_transit"));

            CdcRecoveryFixture.change(source, 2, "shipments", "7", "", "in_transit");
            awaitRows(target, control, Set.of("6,in_transit", "7,in_transit"));

            CdcRecoveryFixture.change(source, 3, "shipments", "2", "delivered", "in_transit");
            awaitRows(target, control, Set.of("2,in_transit", "6,in_transit", "7,in_transit"),
                    () -> CdcRecoveryFixture.streams(source).stream().flatMap(path -> CdcRecoveryFixture.lines(path).stream())
                            .anyMatch(line -> line.startsWith("BATCH 2 3 1 ")));
            assertThat(control.state(PIPELINE)).contains(PipelineState.RUNNING);
        }
    }

    private static void awaitRows(MongoCollection<Document> target, ControlPlane control, Set<String> expected) {
        awaitRows(target,control,expected,() -> true);
    }

    private static void awaitRows(MongoCollection<Document> target,ControlPlane control,Set<String> expected,
            java.util.function.BooleanSupplier delivered) {
        Await.until("the filtered view to contain exactly " + expected, Duration.ofSeconds(30),
                () -> rows(target).equals(expected) && delivered.getAsBoolean(),
                () -> "view rows=" + rows(target) + "; pipeline logs=" + control.logs(PIPELINE));
    }

    private static Set<String> rows(MongoCollection<Document> target) {
        return target.find().into(new java.util.ArrayList<>()).stream()
                .map(row -> row.get("id")+","+row.get("priority")).collect(Collectors.toSet());
    }

    private static Map<String, String> workspace(Path source) {
        Map<String, String> resources = new LinkedHashMap<>();
        resources.put("fulfillment_db.tap.yml", """
                version: tapstate/v1
                kind: source
                id: fulfillment_db
                connector: e2e_file
                config: { uri: "%s" }
                mode: cdc
                tables: [ shipments ]
                """.formatted(source));
        resources.put("pipeline.tap.yml", """
                version: tapstate/v1
                kind: pipeline
                id: in_transit_shipments_pipeline
                source: fulfillment_db
                settings: { read_mode: snapshot_and_cdc }
                transforms:
                  - { id: in_transit_only, from: shipments, type: filter, expr: "after.priority == 'in_transit'" }
                view:
                  id: in_transit_shipments
                  from: in_transit_only
                  primary_key: id
                """);
        return resources;
    }
}
