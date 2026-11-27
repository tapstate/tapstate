package io.tapstate.e2e;

import com.mongodb.ConnectionString;
import com.mongodb.client.MongoClients;
import io.tapstate.core.common.JsonWriter;
import io.tapstate.core.lifecycle.LifecycleVerb;
import io.tapstate.testsupport.DockerGate;
import java.sql.Statement;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.UUID;
import org.bson.Document;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

class SerialMapKeyMovePreflightIT {
    private static final String PIPELINE = "serial_map_key_move";
    private static final String TABLE = "mapped_MAP_MOVE";

    @Test
    void aMoveRejectedAfterASerialMapDoesNotLeaveAnIntervalAndCanBeRetried() throws Exception {
        verifySerialMap(false);
    }

    @Test
    void aSerialMapCanRemoveAChangedUniqueValueBeforeTheParallelSink() throws Exception {
        verifySerialMap(true);
    }

    private static void verifySerialMap(boolean stableOutput) throws Exception {
        DockerGate.require();
        RealConnectorGate.require("mysql", "mongodb");
        var mysql = SharedMySql.settings("serial_move_" + UUID.randomUUID().toString().replace("-", ""));
        try (var connection = SharedMySql.connect(mysql); Statement sql = connection.createStatement();
                var storage = PreparedNativeStore.create(); var server = Tiers.IN_PROCESS.launch(storage);
                var mongo = new MongoEndpoints(); var targetClient = MongoClients.create(storage.masterUri());
                var configClient = MongoClients.create(storage.configurationUri())) {
            sql.execute("CREATE TABLE MAP_MOVE (ID INT PRIMARY KEY, OWNER VARCHAR(20) UNIQUE, VAL INT)");
            sql.executeUpdate("INSERT INTO MAP_MOVE VALUES (1, 'first', 20)");
            var control = new ControlPlane(server.baseUrl());
            storage.loginAndAwaitBusiness(control);
            control.registerConnector("mysql", ConnectorJars.bytesFor("mysql"));
            control.registerConnector("mongodb", ConnectorJars.bytesFor("mongodb"));
            var files = new LinkedHashMap<>(Map.of("source.tap.yml", """
                    version: tapstate/v1
                    kind: source
                    id: map-mysql
                    connector: mysql
                    config: %s
                    mode: cdc
                    tables: [MAP_MOVE]
                    """.formatted(JsonWriter.write(mysql)), "target.tap.yml", """
                    version: tapstate/v1
                    kind: source
                    id: map-target
                    connector: mongodb
                    config: { uri: "%s" }
                    """.formatted(storage.masterUri())));
            control.apply(files);
            control.discoverSchema("map-mysql", "mysql", mysql);
            files.put("pipeline.tap.yml", """
                    version: tapstate/v1
                    kind: pipeline
                    id: serial_map_key_move
                    source: map-mysql
                    transforms:
                      - id: mapped
                        from: map-mysql.MAP_MOVE
                        type: map
                        fields: %s
                        execution: { parallelism: 1 }
                    serve:
                      from: mapped
                      sync:
                        - source: map-target
                          write_mode: upsert
                          on_full_load: clear
                          rename: { prefix: mapped_ }
                          execution: { parallelism: 2, batch: { max_records: 32, max_wait: 20ms } }
                    """.formatted(stableOutput ? "{ OWNER: fixed }" : "{ VAL: $VAL }"));
            control.apply(files);
            control.lifecycle(PIPELINE, LifecycleVerb.START);
            var address = EndpointAddress.uri(storage.masterUri());
            Await.until("the serial Map snapshot to reach MongoDB", () -> mongo.count(address, TABLE) == 1,
                    () -> control.pipelineStatus(PIPELINE).toString());
            var rows = targetClient.getDatabase(new ConnectionString(storage.masterUri()).getDatabase()).getCollection(TABLE);
            var windows = configClient.getDatabase(storage.configurationDatabase()).getCollection("pipeline_key_move_windows");
            if (stableOutput) {
                assertThat(sql.executeUpdate("UPDATE MAP_MOVE SET OWNER='second', VAL=21 WHERE ID=1")).isEqualTo(1);
                Await.until("the Map to preserve its stable target key and projected unique value",
                        () -> mongo.fetch(address, TABLE, Map.of("ID", 1))
                                .filter(row -> "21".equals(String.valueOf(row.get("VAL")))
                                        && "fixed".equals(row.get("OWNER"))).isPresent(),
                        () -> control.pipelineStatus(PIPELINE).toString());
                assertThat(control.failure(PIPELINE)).isEmpty();
                assertThat(mongo.count(address, TABLE)).isEqualTo(1);
                control.stop(PIPELINE, false);
                return;
            }
            var snapshot = rows.find(new Document("ID", 1)).first();
            assertThat(snapshot).isNotNull();
            assertThat(sql.executeUpdate("UPDATE MAP_MOVE SET ID=2, VAL=21 WHERE ID=1")).isEqualTo(1);
            for (int attempt = 0; attempt < 2; attempt++) {
                Await.until("the parallel sink to reject the actual Map output",
                        () -> control.failure(PIPELINE).isPresent(),
                        () -> control.pipelineStatus(PIPELINE).toString());
                assertThat(control.failure(PIPELINE).orElseThrow().code()).isEqualTo("TS0440013");
                assertThat(windows.countDocuments(new Document("_id", PIPELINE)))
                        .as("a rejection before dispatch must not leave an ambiguous interval").isZero();
                assertThat(rows.find(new Document("ID", 1)).first()).isEqualTo(snapshot);
                assertThat(rows.find(new Document("ID", 2)).first()).isNull();
                control.stop(PIPELINE, false);
                if (attempt == 0) control.lifecycle(PIPELINE, LifecycleVerb.START);
            }
        }
    }
}
