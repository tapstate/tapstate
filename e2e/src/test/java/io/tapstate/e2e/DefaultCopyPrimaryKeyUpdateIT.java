package io.tapstate.e2e;

import io.tapstate.core.common.JsonWriter;
import io.tapstate.core.lifecycle.LifecycleVerb;
import io.tapstate.core.lifecycle.PipelineState;
import io.tapstate.testsupport.DockerGate;
import org.junit.jupiter.api.Test;

import java.sql.Statement;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;

class DefaultCopyPrimaryKeyUpdateIT {

    private static final String PIPELINE = "acc_copy_mysql";
    private static final String COMPOSITE = "mysql_ACC_COMPOSITE";
    private static final String OTHER = "mysql_ACC_OTHER";
    private static final Map<String, Object> ORIGINAL_KEY = Map.of("K1", 2, "K2", "k1");
    private static final Map<String, Object> OTHER_KEY = Map.of("ID", 1);

    @Test
    void aPrimaryKeyUpdateDoesNotStopChangeDeliveryToAnotherTable() throws Exception {
        DockerGate.require();
        RealConnectorGate.require("mysql", "mongodb");
        var mysql = SharedMySql.settings("key_change_" + UUID.randomUUID().toString().replace("-", ""));
        try (var connection = SharedMySql.connect(mysql); Statement sql = connection.createStatement();
                var storage = PreparedNativeStore.create(); ServerHandle server = Tiers.IN_PROCESS.launch(storage);
                var mongo = new MongoEndpoints()) {
            sql.execute("CREATE TABLE ACC_COMPOSITE (K1 INT, K2 VARCHAR(20), VAL INT, PRIMARY KEY (K1, K2))");
            sql.execute("CREATE TABLE ACC_OTHER (ID INT PRIMARY KEY, STATUS VARCHAR(20))");
            sql.executeUpdate("INSERT INTO ACC_COMPOSITE VALUES (2, 'k1', 20)");
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
            control.sourceRequest("POST", "/api/pipelines", Map.of("id", PIPELINE, "sources", List.of("acc-mysql"),
                    "serve", Map.of("from", "/.*/", "sync", List.of(Map.of("source", "atlas-store",
                            "write_mode", "upsert", "on_full_load", "clear", "rename", Map.of("prefix", "mysql_"))))),
                    null, 201);
            control.lifecycle(PIPELINE, LifecycleVerb.START);

            var target = EndpointAddress.uri(storage.masterUri());
            Await.until("the two snapshots to reach MongoDB",
                    () -> mongo.count(target, COMPOSITE) == 1 && mongo.count(target, OTHER) == 1,
                    () -> control.pipelineStatus(PIPELINE).toString());
            assertThat(mongo.fetch(target, COMPOSITE, ORIGINAL_KEY).orElseThrow())
                    .containsKeys("K1", "K2", "VAL");
            assertThat(mongo.fetch(target, OTHER, OTHER_KEY).orElseThrow()).containsEntry("STATUS", "snapshot");

            assertThat(sql.executeUpdate("UPDATE ACC_COMPOSITE SET VAL=21 WHERE K1=2 AND K2='k1'")).isEqualTo(1);
            Await.until("an ordinary update to reach the composite-key collection", () -> mongo
                    .fetch(target, COMPOSITE, ORIGINAL_KEY).filter(row -> "21".equals(String.valueOf(row.get("VAL"))))
                    .isPresent(), () -> control.pipelineStatus(PIPELINE).toString());

            assertThat(sql.executeUpdate("UPDATE ACC_COMPOSITE SET K2='r1' WHERE K1=2 AND K2='k1'")).isEqualTo(1);
            assertThat(sql.executeUpdate("UPDATE ACC_OTHER SET STATUS='after' WHERE ID=1")).isEqualTo(1);

            Await.until("the unrelated table's update to reach MongoDB after the primary-key change", () -> {
                var status = control.pipelineStatus(PIPELINE);
                var other = mongo.fetch(target, OTHER, OTHER_KEY);
                assertThat(status.get("state"))
                        .as("pipeline status after the primary-key change %s, unrelated MongoDB row %s", status, other)
                        .isNotEqualTo(PipelineState.FAILED.name());
                return other.filter(row -> "after".equals(row.get("STATUS"))).isPresent();
            }, () -> control.pipelineStatus(PIPELINE) + " unrelated MongoDB row " + mongo.fetch(target, OTHER, OTHER_KEY));
        }
    }
}
