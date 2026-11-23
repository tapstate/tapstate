package io.tapstate.e2e;

import static org.assertj.core.api.Assertions.assertThat;

import io.tapstate.core.lifecycle.LifecycleVerb;
import io.tapstate.core.lifecycle.PipelineState;
import io.tapstate.testsupport.DockerGate;
import java.sql.Connection;
import java.sql.Statement;
import java.time.Duration;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
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
             ServerHandle server = Tiers.IN_PROCESS.launch(storage); ResetProxy proxy = new ResetProxy(mysql)) {
            ControlPlane control = new ControlPlane(server.baseUrl());
            storage.loginAndAwaitBusiness(control);
            var defaults = control.retrySettings();
            assertThat(defaults.body()).contains("60000", "28800000");
            var policy = Map.<String, Object>of("enabled", true, "intervalMillis", 2000L, "maxDurationMillis", 45000L);
            assertThat(control.replaceRetrySettings(0, policy).statusCode()).isEqualTo(200);
            assertThat(control.replaceRetrySettings(0, policy).statusCode()).isEqualTo(409);
            control.registerConnector("mysql", ConnectorJars.bytesFor("mysql"));
            control.registerConnector("mongodb", ConnectorJars.bytesFor("mongodb"));
            apply(control, proxy.settings(mysql), targetUri, pipeline, "retry_source", table, shared);
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

    private static final class ResetProxy implements AutoCloseable {
        private final java.net.ServerSocket listener;
        private final String host;
        private final int port;
        private final java.util.Set<Hop> hops = java.util.concurrent.ConcurrentHashMap.newKeySet();
        private volatile boolean available = true;

        ResetProxy(Map<String, Object> settings) throws java.io.IOException {
            host = settings.get("host").toString();
            port = Integer.parseInt(settings.get("port").toString());
            listener = new java.net.ServerSocket(0, 16, java.net.InetAddress.getByName("127.0.0.1"));
            Thread.ofVirtual().start(this::accept);
        }

        Map<String, Object> settings(Map<String, Object> original) {
            var wrapped = new LinkedHashMap<>(original);
            wrapped.put("host", "127.0.0.1");
            wrapped.put("port", listener.getLocalPort());
            return wrapped;
        }

        void resetFor(Duration duration) throws Exception {
            available = false;
            assertThat(hops.size()).isGreaterThan(0);
            for (Hop hop : hops) hop.reset();
            // Restoration is a fixture event that ends the configured network outage.
            var restoration = CompletableFuture.runAsync(() -> available = true,
                    CompletableFuture.delayedExecutor(duration.toMillis(), TimeUnit.MILLISECONDS));
            restoration.get(duration.plusSeconds(10).toMillis(), TimeUnit.MILLISECONDS);
        }

        private void accept() {
            while (!listener.isClosed()) {
                try {
                    var front = listener.accept();
                    if (!available) {
                        front.setSoLinger(true, 0);
                        front.close();
                        continue;
                    }
                    var back = new java.net.Socket(host, port);
                    var hop = new Hop(front, back);
                    hops.add(hop);
                    Thread.ofVirtual().start(() -> pump(hop, front, back));
                    Thread.ofVirtual().start(() -> pump(hop, back, front));
                } catch (java.io.IOException closedOrUnavailable) {
                    if (!listener.isClosed()) System.out.println("TCP_PROXY_ERROR " + closedOrUnavailable);
                }
            }
        }

        private void pump(Hop hop, java.net.Socket input, java.net.Socket output) {
            try {
                input.getInputStream().transferTo(output.getOutputStream());
            } catch (java.io.IOException closed) {
                // Reset and fixture teardown both close active connections.
            } finally {
                hops.remove(hop);
                hop.close();
            }
        }

        public void close() throws java.io.IOException {
            listener.close();
            for (Hop hop : hops) hop.close();
        }

        private record Hop(java.net.Socket front, java.net.Socket back) {
            void reset() {
                try { front.setSoLinger(true, 0); } catch (java.io.IOException alreadyClosed) {}
                close();
            }

            void close() {
                try { front.close(); } catch (java.io.IOException alreadyClosed) {}
                try { back.close(); } catch (java.io.IOException alreadyClosed) {}
            }
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
