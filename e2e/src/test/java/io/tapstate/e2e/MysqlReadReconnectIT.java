package io.tapstate.e2e;

import static org.assertj.core.api.Assertions.assertThat;

import io.tapstate.core.lifecycle.LifecycleVerb;
import io.tapstate.core.lifecycle.PipelineState;
import io.tapstate.testsupport.DockerGate;
import io.tapstate.testsupport.TcpResetProxy;
import java.sql.Connection;
import java.sql.Statement;
import java.time.Duration;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

class MysqlReadReconnectIT {
    @BeforeAll
    static void requireConnectors() {
        DockerGate.require();
        RealConnectorGate.require("mysql", "mongodb");
    }

    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    void resumesFromTheOriginalCheckpointAfterLosingTheBinlogConnection(boolean shared) throws Exception {
        String suffix = shared ? "shared" : "direct";
        String pipeline = "retry_" + suffix;
        String table = "orders_" + suffix;
        Map<String, Object> mysql = SharedMySql.settings("retry_cdc_" + suffix);
        sql(mysql, "CREATE TABLE " + table + " (id INT PRIMARY KEY, name VARCHAR(64))",
                "INSERT INTO " + table + " VALUES (1, 'snapshot')");
        String targetUri = SharedMongo.replicaSetUrl("retry_target_" + suffix);
        EndpointAddress target = EndpointAddress.uri(targetUri);

        try (PreparedNativeStore storage = PreparedNativeStore.create(); MongoEndpoints mongo = new MongoEndpoints();
             ServerHandle server = Tiers.IN_PROCESS.launch(storage); TcpResetProxy proxy = new TcpResetProxy(
                     mysql.get("host").toString(), Integer.parseInt(mysql.get("port").toString()))) {
            ControlPlane control = new ControlPlane(server.baseUrl());
            storage.loginAndAwaitBusiness(control);
            var defaults = control.retrySettings();
            assertThat(defaults.body()).contains("60000", "28800000");
            var policy = Map.<String, Object>of("enabled", true, "intervalMillis", 2000L, "maxDurationMillis", 45000L);
            assertThat(control.replaceRetrySettings(0, policy).statusCode()).isEqualTo(200);
            assertThat(control.replaceRetrySettings(0, policy).statusCode()).isEqualTo(409);
            control.registerConnector("mysql", ConnectorJars.bytesFor("mysql"));
            control.registerConnector("mongodb", ConnectorJars.bytesFor("mongodb"));
            var proxied = new LinkedHashMap<>(mysql);
            proxied.put("host", "127.0.0.1");
            proxied.put("port", proxy.port());
            apply(control, proxied, targetUri, pipeline, "retry_source", table, shared);
            awaitIds(control, pipeline, mongo, target, table, List.of(1L));
            sql(mysql, "INSERT INTO " + table + " VALUES (2, 'before-fault')",
                    "INSERT INTO " + table + " VALUES (4, 'before-fault-control')");
            awaitIds(control, pipeline, mongo, target, table, List.of(1L, 2L, 4L));
            Await.answered("the confirmed pre-fault CDC prefix",
                    () -> control.ackedChangeSeq(pipeline).filter(seq -> seq > 0));
            var original = control.resumePoint(pipeline);
            assertThat(original).containsKeys("chainId", "token");

            proxy.resetFor(Duration.ofMillis(1500));
            sql(mysql, "DELETE FROM " + table + " WHERE id=2",
                    "INSERT INTO " + table + " VALUES (3, 'after-fault')");

            awaitIds(control, pipeline, mongo, target, table, List.of(1L, 3L, 4L));
            Await.until("the post-fault target confirmation on the original chain",
                    () -> {
                        var current = control.resumePoint(pipeline);
                        return original.get("chainId").equals(current.get("chainId"))
                                && current.get("token") != null && !original.get("token").equals(current.get("token"));
                    }, () -> control.positionReading(pipeline));
        }
    }

    private static void apply(ControlPlane control, Map<String, Object> mysql, String targetUri, String pipeline,
            String source, String table, boolean shared) {
        Map<String, String> resources = new LinkedHashMap<>();
        resources.put("target.tap.yml", """
                version: tapstate/v1
                kind: source
                id: reader_target
                connector: mongodb
                config: { uri: "%s" }
                """.formatted(targetUri));
        resources.put(source + ".tap.yml", """
                version: tapstate/v1
                kind: source
                id: %s
                connector: mysql
                config: { host: %s, port: %s, database: %s, username: %s, password: %s }
                mode: cdc
                tables: [ %s ]
                """.formatted(source, mysql.get("host"), mysql.get("port"), mysql.get("database"),
                        mysql.get("username"), mysql.get("password"), table));
        resources.put(pipeline + ".tap.yml", """
                version: tapstate/v1
                kind: pipeline
                id: %s
                source:
                  - { id: %s, srs: %s }
                settings: { read_mode: snapshot_and_cdc }
                transforms:
                  - { id: selected_rows, from: [%s], type: filter, expr: "true" }
                serve:
                  from: selected_rows
                  sync:
                    - source: reader_target
                """.formatted(pipeline, source, shared, table));
        control.apply(resources);
        control.discoverSchema(source, "mysql", mysql);
        control.lifecycle(pipeline, LifecycleVerb.START);
    }

    private static void awaitIds(ControlPlane control, String pipeline, MongoEndpoints mongo,
            EndpointAddress target, String table, List<Long> expected) {
        Await.until("rows in " + table + " from " + pipeline,
                () -> ids(mongo, target, table).equals(expected)
                        && control.state(pipeline).filter(PipelineState.RUNNING::equals).isPresent(),
                () -> "rows=" + ids(mongo, target, table) + ", state=" + control.state(pipeline)
                        + ", logs=" + control.logs(pipeline));
        assertThat(control.state(pipeline)).contains(PipelineState.RUNNING);
    }

    private static List<Long> ids(MongoEndpoints mongo, EndpointAddress target, String table) {
        return mongo.documents(target, table).stream()
                .map(row -> ((Number) row.get("id")).longValue()).sorted().toList();
    }

    private static void sql(Map<String, Object> mysql, String... statements) throws Exception {
        try (Connection connection = SharedMySql.connect(mysql); Statement statement = connection.createStatement()) {
            for (String sql : statements) {
                statement.execute(sql);
            }
        }
    }
}
