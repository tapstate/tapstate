package io.tapstate.e2e;

import com.mongodb.ConnectionString;
import com.mongodb.client.MongoClients;
import com.mongodb.client.model.IndexOptions;
import io.tapstate.core.common.JsonWriter;
import io.tapstate.core.lifecycle.LifecycleVerb;
import io.tapstate.testsupport.DockerGate;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import org.bson.Document;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

class PostgresKeyOnlyDeleteIT {
    private static final String PIPELINE = "postgres_key_only_delete";

    @Test
    void aKeyOnlyDeleteReleasesItsUniqueValueBeforeAnotherWriterReusesIt() throws Exception {
        DockerGate.require();
        RealConnectorGate.require("postgres", "mongodb");
        var postgres = SharedPostgres.settings("key_only_delete_" + UUID.randomUUID().toString().replace("-", ""));
        try (var connection = SharedPostgres.connect(postgres); Statement sql = connection.createStatement();
                var storage = PreparedNativeStore.create(); var server = Tiers.IN_PROCESS.launch(storage);
                var targetClient = MongoClients.create(storage.masterUri())) {
            sql.execute("CREATE TABLE orders (id BIGINT PRIMARY KEY, email TEXT UNIQUE, label TEXT)");
            sql.execute("ALTER TABLE orders REPLICA IDENTITY DEFAULT");
            sql.executeUpdate("INSERT INTO orders VALUES (1, 'shared', 'old')");

            var control = new ControlPlane(server.baseUrl());
            storage.loginAndAwaitBusiness(control);
            control.registerConnector("postgres", ConnectorJars.bytesFor("postgres"));
            control.registerConnector("mongodb", ConnectorJars.bytesFor("mongodb"));
            var config = new LinkedHashMap<>(postgres);
            config.put("user", config.remove("username"));
            config.put("schema", "public");
            control.apply(Map.of("source.tap.yml", """
                    version: tapstate/v1
                    kind: source
                    id: delete-postgres
                    connector: postgres
                    config: %s
                    mode: cdc
                    tables: [orders]
                    """.formatted(JsonWriter.write(config)), "target.tap.yml", """
                    version: tapstate/v1
                    kind: source
                    id: delete-mongo
                    connector: mongodb
                    config: { uri: "%s" }
                    """.formatted(storage.masterUri())));
            control.discoverSchema("delete-postgres", "postgres", config);
            control.sourceRequest("POST", "/api/pipelines", Map.of("id", PIPELINE,
                    "sources", List.of("delete-postgres"), "serve", Map.of("from", "/.*/", "sync", List.of(
                            Map.of("source", "delete-mongo", "write_mode", "upsert", "on_full_load", "clear",
                                    "execution", Map.of("parallelism", 4))))), null, 201);
            var rows = targetClient.getDatabase(new ConnectionString(storage.masterUri()).getDatabase())
                    .getCollection("orders");
            rows.createIndex(new Document("email", 1), new IndexOptions().unique(true));
            control.lifecycle(PIPELINE, LifecycleVerb.START);

            Await.until("the PostgreSQL snapshot to reach MongoDB", () -> {
                assertThat(control.failure(PIPELINE)).isEmpty();
                return rows.countDocuments() == 1;
            }, () -> control.pipelineStatus(PIPELINE).toString());
            assertThat(rows.listIndexes().into(new ArrayList<>())).anyMatch(index ->
                    new Document("email", 1).equals(index.get("key")) && Boolean.TRUE.equals(index.get("unique")));

            assertThat(sql.executeUpdate("UPDATE orders SET label='updated' WHERE id=1")).isEqualTo(1);
            Await.until("a key-stable update with a partial before image to reach MongoDB", () -> {
                assertThat(control.failure(PIPELINE)).isEmpty();
                return rows.countDocuments(new Document("id", 1L).append("label", "updated")) == 1;
            }, () -> control.pipelineStatus(PIPELINE).toString());

            assertThat(sql.executeUpdate("DELETE FROM orders WHERE id=1")).isEqualTo(1);
            assertThat(sql.executeUpdate("INSERT INTO orders VALUES (2, 'shared', 'replacement')")).isEqualTo(1);
            Await.until("the replacement to acquire the deleted row's unique value", () -> {
                assertThat(control.failure(PIPELINE)).isEmpty();
                return rows.countDocuments(new Document("id", 2L).append("email", "shared")
                        .append("label", "replacement")) == 1;
            }, () -> control.pipelineStatus(PIPELINE).toString());
            assertThat(rows.find().projection(new Document("_id", 0)).into(new ArrayList<>()))
                    .containsExactly(new Document("id", 2L).append("email", "shared").append("label", "replacement"));
            assertThat(control.failure(PIPELINE)).isEmpty();
            control.stop(PIPELINE, false);
        }
    }
}
