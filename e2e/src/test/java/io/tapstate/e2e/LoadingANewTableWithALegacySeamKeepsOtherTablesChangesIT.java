package io.tapstate.e2e;

import static org.assertj.core.api.Assertions.assertThat;

import com.mongodb.ConnectionString;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoDatabase;
import io.tapstate.adapters.mongostore.MongoStorePort;
import io.tapstate.core.lifecycle.LifecycleVerb;
import io.tapstate.core.lifecycle.PipelineState;
import io.tapstate.spi.store.SrsConsumerId;
import io.tapstate.testsupport.DockerGate;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import org.bson.Document;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * A partial load over a generation-zero seam preserves changes of the tables it does not reload.
 * The specification vocabulary cannot seed a seam without its snapshot generation. This case removes
 * only snapshotEpoch from the shipped path's source-qualified cursor; it does not migrate pipeline-id
 * progress from an older release.
 */
class LoadingANewTableWithALegacySeamKeepsOtherTablesChangesIT {

    private static final String SOURCE = "legacy_seam_source";
    private static final String TARGET = "legacy_seam_target";
    private static final String PIPELINE = "legacy_seam_pipeline";
    private static final String ORDERS = "orders";
    private static final String CUSTOMERS = "customers";
    private static final Duration TIMEOUT = Duration.ofSeconds(90);

    @BeforeAll
    static void requireDocker() {
        DockerGate.require();
    }

    @Test
    void anOrdersChangeWhileStoppedStillLandsAfterTheNewCustomersLoad(@TempDir Path temporary)
            throws Exception {
        Path source = Files.createDirectory(temporary.resolve("source"));
        Path target = Files.createDirectory(temporary.resolve("target"));
        FileEndpoints.replaceTable(source.resolve(ORDERS + ".csv"), "id,priority\n1,initial\n");
        FileEndpoints.replaceTable(source.resolve(CUSTOMERS + ".csv"), "id,priority\n1,new-customer\n");
        byte[] connector = CdcRecoveryFixture.connectorJar(E2eConnectorJar.buildInto(
                Files.createDirectory(temporary.resolve("connectors"))));
        String storeUri = SharedMongo.replicaSetUrl("e2e_legacy_seam_partial_load");

        try (MongoClient reader = MongoClients.create(storeUri);
                ServerHandle server = InProcessServer.start(storeUri)) {
            MongoDatabase database = reader.getDatabase(new ConnectionString(storeUri).getDatabase());
            ControlPlane control = new ControlPlane(server.baseUrl());
            control.bootstrapAndLogin("e2e", "e2e-password");
            control.registerConnector(E2eConnectorJar.CONNECTOR_ID, connector);
            control.apply(resources(source, target, ORDERS));
            control.discoverSchema(SOURCE, E2eConnectorJar.CONNECTOR_ID, Map.of("uri", source.toString()));
            control.lifecycle(PIPELINE, LifecycleVerb.START);
            awaitState(control, PipelineState.RUNNING);
            awaitPriority(control, target, ORDERS, "1", "initial");
            Await.until("orders to be confirmed and its direct tail to open", TIMEOUT,
                    () -> completedOrders(consumer(database)) && CdcRecoveryFixture.streams(source).size() == 1,
                    () -> "consumer=" + consumer(database) + ", streams=" + streamText(source));

            FileEndpoints.replaceTable(source.resolve(ORDERS + ".csv"), "id,priority\n1,confirmed\n");
            CdcRecoveryFixture.change(source, 1, ORDERS, "1", "initial", "confirmed");
            awaitPriority(control, target, ORDERS, "1", "confirmed");
            Await.until("the confirmed orders change to advance the chain past its first seam", TIMEOUT,
                    () -> hasAdvancedCheckpoint(database),
                    () -> "consumer=" + consumer(database) + ", chain=" + chain(database));
            String chainId = consumer(database).getString("miningChainId");
            List<Path> firstStreams = CdcRecoveryFixture.streams(source);

            control.stop(PIPELINE, false);
            awaitState(control, PipelineState.STOPPED);
            Await.until("the stopped direct tail to return", TIMEOUT,
                    () -> firstStreams.stream().allMatch(path -> CdcRecoveryFixture.lines(path).stream()
                            .anyMatch(line -> line.startsWith("END "))),
                    () -> streamText(source));
            Document retained = consumer(database);
            assertThat(retained.getList("snapshotCompletedTables", String.class)).containsExactly(ORDERS);
            assertThat(database.getCollection(MongoStorePort.SRS_CONSUMER_OFFSETS)
                    .updateOne(new Document("_id", retained.get("_id")),
                            new Document("$unset", new Document("snapshotEpoch", "")))
                    .getModifiedCount()).isEqualTo(1L);
            assertThat(consumer(database)).doesNotContainKey("snapshotEpoch");

            // The new table samples position 2, while orders still needs the change after position 1.
            FileEndpoints.replaceTable(source.resolve(ORDERS + ".csv"), "id,priority\n1,while-stopped\n");
            CdcRecoveryFixture.change(source, 2, ORDERS, "1", "confirmed", "while-stopped");
            control.apply(resources(source, target, ORDERS + ", " + CUSTOMERS));
            control.lifecycle(PIPELINE, LifecycleVerb.START);
            awaitState(control, PipelineState.RUNNING);
            awaitPriority(control, target, CUSTOMERS, "1", "new-customer");
            Await.until("the replacement direct tail to open", TIMEOUT,
                    () -> CdcRecoveryFixture.streams(source).size() == 2,
                    () -> streamText(source));
            assertThat(consumer(database).getString("miningChainId")).isEqualTo(chainId);

            // A later change on the same table also proves the earlier orders change has been processed.
            CdcRecoveryFixture.change(source, 3, ORDERS, "2", "", "after-restart");
            awaitPriority(control, target, ORDERS, "2", "after-restart");
            Await.until("the current run's per-table snapshot metrics to be published", TIMEOUT,
                    () -> Long.valueOf(1L).equals(control.snapshotRowsRead(PIPELINE).get(CUSTOMERS)),
                    () -> String.valueOf(control.snapshotRowsRead(PIPELINE)));
            assertThat(control.snapshotRowsRead(PIPELINE)).containsEntry(ORDERS, 0L).containsEntry(CUSTOMERS, 1L);
            assertThat(priority(target, ORDERS, "1"))
                    .as("the direct tail must carry orders' stopped-period change before its later orders change")
                    .isEqualTo("while-stopped");
        }
    }

    private static Map<String, String> resources(Path source, Path target, String selectedTables) {
        Map<String, String> resources = new LinkedHashMap<>();
        resources.put(SOURCE + ".tap.yml", """
                version: tapstate/v1
                kind: source
                id: %s
                connector: e2e_file
                config: { uri: "%s" }
                mode: cdc
                tables: [ orders, customers ]
                """.formatted(SOURCE, source));
        resources.put(TARGET + ".tap.yml", Workspaces.targetYaml(TARGET, target));
        resources.put(PIPELINE + ".tap.yml", """
                version: tapstate/v1
                kind: pipeline
                id: %s
                source:
                  - { id: %s, srs: false }
                settings: { read_mode: snapshot_and_cdc }
                transforms:
                  - { id: all_rows, from: [ %s ], type: filter, expr: "true" }
                serve:
                  from: all_rows
                  sync:
                    - source: %s
                """.formatted(PIPELINE, SOURCE, selectedTables, TARGET));
        return resources;
    }

    private static Document consumer(MongoDatabase database) {
        return database.getCollection(MongoStorePort.SRS_CONSUMER_OFFSETS)
                .find(new Document("pipelineId", SrsConsumerId.of(PIPELINE, SOURCE).value())).first();
    }

    private static Document chain(MongoDatabase database) {
        Document consumer = consumer(database);
        return consumer == null ? null : database.getCollection(MongoStorePort.SRS_META)
                .find(new Document("_id", consumer.getString("miningChainId"))).first();
    }

    private static boolean completedOrders(Document consumer) {
        return consumer != null && consumer.getList("snapshotCompletedTables", String.class, List.of())
                .contains(ORDERS);
    }

    private static boolean hasAdvancedCheckpoint(MongoDatabase database) {
        Document consumer = consumer(database);
        Document chain = chain(database);
        if (consumer == null || chain == null) return false;
        String readOffset = chain.getString("sourceReadOffset");
        return readOffset != null && readOffset.equals(consumer.getString("sinkAckedSrcpos"))
                && !readOffset.equals(consumer.getString("cdcStartPosition"));
    }

    private static String priority(Path target, String table, String id) {
        List<String> lines = CdcRecoveryFixture.lines(target.resolve(table + ".csv"));
        if (lines.isEmpty()) return null;
        List<String> columns = List.of(lines.getFirst().split(",", -1));
        int idColumn = columns.indexOf("id");
        int priorityColumn = columns.indexOf("priority");
        for (String line : lines.subList(1, lines.size())) {
            String[] row = line.split(",", -1);
            if (idColumn >= 0 && priorityColumn >= 0 && row.length > Math.max(idColumn, priorityColumn)
                    && id.equals(row[idColumn])) return row[priorityColumn];
        }
        return null;
    }

    private static void awaitState(ControlPlane control, PipelineState state) {
        Await.until(PIPELINE + " to reach " + state, TIMEOUT,
                () -> control.state(PIPELINE).filter(state::equals).isPresent(),
                () -> control.state(PIPELINE) + ", logs=" + control.logs(PIPELINE));
    }

    private static void awaitPriority(ControlPlane control, Path target, String table, String id, String expected) {
        Await.until(table + " row " + id + " to hold " + expected, TIMEOUT,
                () -> expected.equals(priority(target, table, id)),
                () -> "rows=" + CdcRecoveryFixture.lines(target.resolve(table + ".csv"))
                        + ", state=" + control.state(PIPELINE) + ", logs=" + control.logs(PIPELINE));
    }

    private static String streamText(Path source) {
        return CdcRecoveryFixture.streams(source).stream()
                .map(path -> path.getFileName() + ":" + CdcRecoveryFixture.lines(path)).toList().toString();
    }
}
