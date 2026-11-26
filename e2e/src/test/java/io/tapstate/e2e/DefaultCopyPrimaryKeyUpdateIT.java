package io.tapstate.e2e;

import com.mongodb.ConnectionString;
import com.mongodb.client.MongoClients;
import com.mongodb.client.model.Collation;
import com.mongodb.client.model.IndexOptions;
import org.bson.Document;
import io.tapstate.core.common.JsonWriter;
import io.tapstate.core.lifecycle.LifecycleVerb;
import io.tapstate.core.lifecycle.PipelineState;
import io.tapstate.testsupport.DockerGate;
import org.junit.jupiter.api.Test;

import java.sql.Statement;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;

class DefaultCopyPrimaryKeyUpdateIT {

    private static final String PIPELINE = "acc_copy_mysql";
    private static final String COMPOSITE = "mysql_ACC_COMPOSITE";
    private static final String OTHER = "mysql_ACC_OTHER";
    private static final Map<String, Object> ORIGINAL_KEY = Map.of("K1", 2, "K2", "k1");
    private static final Map<String, Object> UPDATED_KEY = Map.of("K1", 2, "K2", "r1");
    private static final Map<String, Object> OTHER_KEY = Map.of("ID", 1);

    private enum Install { FRESH, LEGACY, UNSUPPORTED_INDEX }

    @Test
    void aPrimaryKeyUpdateDoesNotStopChangeDeliveryToAnotherTable() throws Exception {
        verifyKeyMove(Map.of(), Install.FRESH);
    }

    @Test
    void fourExplicitWritersPreserveTheMovedRowAndTheReplacement() throws Exception {
        verifyKeyMove(Map.of("parallelism", 4, "batch", Map.of("max_records", 32, "max_wait", "20ms")), Install.FRESH);
    }

    @Test
    void aLegacyInstallWritesOrdinaryChangesAndRefusesOnlyTheMove() throws Exception {
        verifyKeyMove(Map.of(), Install.LEGACY);
    }

    @Test
    void aMoveRefusedBeforeDispatchCanBeRetriedAfterCorrectingTheIndex() throws Exception {
        verifyKeyMove(Map.of(), Install.UNSUPPORTED_INDEX);
    }

    private static ServerHandle launch(PreparedNativeStore store, boolean legacy) throws Exception {
        if (legacy) {
            try (var client = MongoClients.create(store.configurationUri())) {
                var config = client.getDatabase(store.configurationDatabase());
                config.getCollection("schema_migrations").updateOne(new Document("_id", "config"),
                        new Document("$unset", new Document("pipelineKeyMoveWindowVersion", "")));
                config.getCollection("pipeline_key_move_windows").drop();
            }
        }
        return Tiers.IN_PROCESS.launch(store);
    }

    private void verifyKeyMove(Map<String, Object> execution, Install install) throws Exception {
        DockerGate.require();
        RealConnectorGate.require("mysql", "mongodb");
        var mysql = SharedMySql.settings("key_change_" + UUID.randomUUID().toString().replace("-", ""));
        try (var connection = SharedMySql.connect(mysql); Statement sql = connection.createStatement();
                var storage = PreparedNativeStore.create(); ServerHandle server = launch(storage, install == Install.LEGACY);
                var mongo = new MongoEndpoints(); var targetClient = MongoClients.create(storage.masterUri())) {
            sql.execute("CREATE TABLE ACC_COMPOSITE (K1 INT, K2 VARCHAR(20), VAL INT, " +
                    "OWNER VARCHAR(20) UNIQUE, PRIMARY KEY (K1, K2))");
            sql.execute("CREATE TABLE ACC_OTHER (ID INT PRIMARY KEY, STATUS VARCHAR(20))");
            sql.executeUpdate("INSERT INTO ACC_COMPOSITE VALUES (2, 'k1', 20, 'first')");
            sql.executeUpdate("INSERT INTO ACC_COMPOSITE VALUES (3, 'third', 30, 'claimed')");
            sql.executeUpdate("INSERT INTO ACC_COMPOSITE VALUES (4, 'nullable', 40, NULL)");
            sql.executeUpdate("INSERT INTO ACC_OTHER VALUES (1, 'snapshot')");

            var control = new ControlPlane(server.baseUrl());
            storage.loginAndAwaitBusiness(control);
            control.registerConnector("mysql", ConnectorJars.bytesFor("mysql"));
            control.registerConnector("mongodb", ConnectorJars.bytesFor("mongodb"));
            control.apply(Map.of("source.tap.yml", """
                    version: tapstate/v1
                    kind: source
                    id: acc-mysql
                    connector: mysql
                    config: %s
                    mode: cdc
                    tables: [ACC_COMPOSITE, ACC_OTHER]
                    """.formatted(JsonWriter.write(mysql)), "target.tap.yml", """
                    version: tapstate/v1
                    kind: source
                    id: atlas-store
                    connector: mongodb
                    config: { uri: "%s" }
                    """.formatted(storage.masterUri())));
            control.discoverSchema("acc-mysql", "mysql", mysql);
            var sync = new LinkedHashMap<String, Object>(Map.of("source", "atlas-store", "write_mode", "upsert",
                    "on_full_load", "clear", "rename", Map.of("prefix", "mysql_")));
            if (!execution.isEmpty()) sync.put("execution", execution);
            control.sourceRequest("POST", "/api/pipelines", Map.of("id", PIPELINE, "sources", List.of("acc-mysql"),
                    "serve", Map.of("from", "/.*/", "sync", List.of(sync))),
                    null, 201);
            var physical = targetClient.getDatabase(new ConnectionString(storage.masterUri()).getDatabase())
                    .getCollection(COMPOSITE);
            physical.createIndex(new Document("OWNER", 1), new IndexOptions().unique(true));
            if (install == Install.UNSUPPORTED_INDEX) {
                physical.createIndex(new Document("OWNER", 1), new IndexOptions().name("owner_non_simple")
                        .unique(true).collation(Collation.builder().locale("en").build()));
            }
            control.lifecycle(PIPELINE, LifecycleVerb.START);

            var target = EndpointAddress.uri(storage.masterUri());
            Await.until("the two snapshots to reach MongoDB",
                    () -> mongo.count(target, COMPOSITE) == 3 && mongo.count(target, OTHER) == 1,
                    () -> control.pipelineStatus(PIPELINE).toString());
            assertThat(physical.listIndexes().into(new java.util.ArrayList<>())).anyMatch(index ->
                    new Document("OWNER", 1).equals(index.get("key")) && Boolean.TRUE.equals(index.get("unique")));
            assertThat(mongo.fetch(target, COMPOSITE, ORIGINAL_KEY).orElseThrow())
                    .containsKeys("K1", "K2", "VAL");
            assertThat(mongo.fetch(target, OTHER, OTHER_KEY).orElseThrow()).containsEntry("STATUS", "snapshot");

            assertThat(sql.executeUpdate("UPDATE ACC_COMPOSITE SET VAL=41 WHERE K1=4 AND K2='nullable'")).isEqualTo(1);
            Await.until("an ordinary update with a null unique value to reach MongoDB", () -> mongo
                    .fetch(target, COMPOSITE, Map.of("K1", 4, "K2", "nullable"))
                    .filter(row -> "41".equals(String.valueOf(row.get("VAL"))))
                    .isPresent(), () -> control.pipelineStatus(PIPELINE).toString());

            if (install == Install.UNSUPPORTED_INDEX) {
                assertThat(sql.executeUpdate("UPDATE ACC_COMPOSITE SET K2='r1' WHERE K1=2 AND K2='k1'")).isEqualTo(1);
                Await.until("the non-simple index to refuse the move before dispatch",
                        () -> control.failure(PIPELINE).isPresent(),
                        () -> control.pipelineStatus(PIPELINE).toString());
                try (var configClient = MongoClients.create(storage.configurationUri())) {
                    var windows = configClient.getDatabase(storage.configurationDatabase()).getCollection("pipeline_key_move_windows");
                    assertThat(windows.countDocuments(new Document("_id", PIPELINE)))
                            .as("rejecting before dispatch leaves no uncertain target write").isZero();
                }
                var original = physical.find(new Document("K1", 2).append("K2", "k1")).first();
                assertThat(original).isNotNull();
                assertThat(mongo.fetch(target, COMPOSITE, UPDATED_KEY)).isEmpty();
                assertThat(control.failure(PIPELINE).orElseThrow().code()).isEqualTo("TS0440017");
                control.stop(PIPELINE, false);
                physical.dropIndex("owner_non_simple");
                control.lifecycle(PIPELINE, LifecycleVerb.START);
                Await.until("the retained source position to replay the move with the repaired index",
                        () -> mongo.fetch(target, COMPOSITE, UPDATED_KEY).isPresent()
                                && mongo.fetch(target, COMPOSITE, ORIGINAL_KEY).isEmpty(),
                        () -> control.pipelineStatus(PIPELINE).toString());
                var moved = physical.find(new Document("K1", 2).append("K2", "r1")).first();
                assertThat(moved).isNotNull();
                assertThat(moved.get("_id")).as("a resumed move keeps the original physical document")
                        .isEqualTo(original.get("_id"));
                control.stop(PIPELINE, false);
                return;
            }
            if (install == Install.LEGACY) {
                assertThat(sql.executeUpdate("UPDATE ACC_COMPOSITE SET K2='r1' WHERE K1=2 AND K2='k1'")).isEqualTo(1);
                Await.until("the legacy installation to refuse the move before dispatch",
                        () -> control.failure(PIPELINE).filter(f -> f.code().equals("TS0480044")).isPresent(),
                        () -> control.pipelineStatus(PIPELINE).toString());
                assertThat(mongo.fetch(target, COMPOSITE, ORIGINAL_KEY)).isPresent();
                assertThat(mongo.fetch(target, COMPOSITE, UPDATED_KEY)).isEmpty();
                return;
            }
            assertThat(sql.executeUpdate("UPDATE ACC_COMPOSITE SET OWNER='free' WHERE K1=3")).isEqualTo(1);
            assertThat(sql.executeUpdate("UPDATE ACC_COMPOSITE SET K2='r1', OWNER='claimed' WHERE K1=2 AND K2='k1'")).isEqualTo(1);
            assertThat(sql.executeUpdate("UPDATE ACC_COMPOSITE SET VAL=22 WHERE K1=2 AND K2='r1'")).isEqualTo(1);
            assertThat(sql.executeUpdate("INSERT INTO ACC_COMPOSITE VALUES (2, 'k1', 99, 'first')")).isEqualTo(1);
            assertThat(sql.executeUpdate("UPDATE ACC_OTHER SET STATUS='after' WHERE ID=1")).isEqualTo(1);

            Await.until("the composite row to move to its new key and the unrelated update to reach MongoDB", () -> {
                var status = control.pipelineStatus(PIPELINE);
                var other = mongo.fetch(target, OTHER, OTHER_KEY);
                assertThat(status.get("state"))
                        .as("pipeline status after the primary-key change %s, unrelated MongoDB row %s", status, other)
                        .isNotEqualTo(PipelineState.FAILED.name());
                return mongo.fetch(target, COMPOSITE, UPDATED_KEY)
                        .filter(row -> "22".equals(String.valueOf(row.get("VAL")))).isPresent()
                        && mongo.fetch(target, COMPOSITE, ORIGINAL_KEY)
                                .filter(row -> "99".equals(String.valueOf(row.get("VAL")))).isPresent()
                        && other.filter(row -> "after".equals(row.get("STATUS"))).isPresent();
            }, () -> control.pipelineStatus(PIPELINE)
                    + " composite row at original key " + mongo.fetch(target, COMPOSITE, ORIGINAL_KEY)
                    + " composite row at updated key " + mongo.fetch(target, COMPOSITE, UPDATED_KEY)
                    + " unrelated MongoDB row " + mongo.fetch(target, OTHER, OTHER_KEY));
            assertThat(mongo.fetch(target, COMPOSITE, Map.of("K1", 3)).orElseThrow()).containsEntry("OWNER", "free");
            assertThat(mongo.fetch(target, COMPOSITE, UPDATED_KEY).orElseThrow()).containsEntry("OWNER", "claimed");
            assertThat(mongo.fetch(target, COMPOSITE, ORIGINAL_KEY).orElseThrow()).containsEntry("OWNER", "first");
            control.stop(PIPELINE, false);
        }
    }
}
