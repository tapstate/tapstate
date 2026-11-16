package io.tapstate.e2e;

import com.mongodb.client.MongoClients;
import io.tapstate.adapters.mongostore.SystemCollections;
import io.tapstate.core.common.JsonReader;
import io.tapstate.testsupport.RequiresDocker;
import org.bson.Document;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

@RequiresDocker
class ConnectionAuthenticationDiagnosticsIT {

    @Test
    void mysqlPublishesReadableChecksAndRetainsTheSameReport(@TempDir Path home) throws Exception {
        RealConnectorGate.require("mysql");
        var settings = SharedMySql.settings("connection_auth_diagnostic");

        var report = failedAuthentication("mysql", settings, home);

        assertThat(report.toString())
                .contains("The connection could not be established.", "The database could not be reached",
                        "Check the host, port, database and credentials")
                .doesNotContain("pdk.connection.fail", "pdk.connection.reason", "pdk.connection.solution");
        assertThat((List<?>) report.get("checks")).anySatisfy(value -> {
            var check = (Map<?, ?>) value;
            assertThat(check.get("status")).isEqualTo("FAILED");
            assertThat(check.get("diagnostic").toString()).contains("The connection could not be established.");
        });
    }

    @Test
    void mysqlRendersTheUnreachableHostCheckAndRetainsItsOrder() throws Exception {
        RealConnectorGate.require("mysql");
        var settings = SharedMySql.settings("connection_unreachable_diagnostic");
        try (var store = PreparedNativeStore.create();
             var server = InProcessServer.startWithConnectionDiagnostics(store)) {
            var control = new ControlPlane(server.baseUrl());
            store.loginAndAwaitBusiness(control);
            control.registerConnector("mysql", ConnectorJars.bytesFor("mysql"));
            String id = "unreachable_mysql";
            settings.put("port", unusedLoopbackPort());
            var request = Map.of("id", id, "connectorId", "mysql", "settings", settings);
            var immediate = control.sourceRequest("POST", "/api/connections:test", request, null, 200);
            var latest = control.sourceRequest("GET", "/api/connections/" + id + "/test-result", null, null, 200);
            var report = (Map<?, ?>) JsonReader.parse(immediate.body());
            assertThat(JsonReader.parse(latest.body())).isEqualTo(report);
            assertThat(report.get("outcome")).isEqualTo("FAILED");
            assertThat((List<?>) report.get("checks")).first().satisfies(value -> {
                var check = (Map<?, ?>) value;
                assertThat(check.get("status")).isEqualTo("FAILED");
                assertThat(check.get("message")).isInstanceOf(String.class);
                assertThat(check.get("message").toString()).doesNotContain("pdk.connection.");
            });
            assertThat(immediate.body()).doesNotContain("pdk.connection.");
        }
    }

    @Test
    void postgresPublishesRejectedCredentialsAsAFailedConnectionCheck(@TempDir Path home) throws Exception {
        RealConnectorGate.require("postgres");
        var settings = SharedPostgres.settings("connection_auth_diagnostic");
        settings.put("user", settings.remove("username"));
        settings.put("schema", "public");

        var report = failedAuthentication("postgres", settings, home);

        assertThat((List<?>) report.get("checks")).anySatisfy(value -> {
            var check = (Map<?, ?>) value;
            assertThat(check.get("status")).isEqualTo("FAILED");
            assertThat(check.get("connectorErrorCode")).isEqualTo("28P01");
            assertThat((String) check.get("message")).contains("password authentication failed");
            var diagnostic = (Map<?, ?>) check.get("diagnostic");
            assertThat(diagnostic.get("cause").toString()).contains("28P01", "password authentication failed");
        });
    }

    @Test
    void mongodbDefaultSourceRetainsItsManagedFormatAndTerminalPublication() {
        RealConnectorGate.require("mongodb");
        try (var store = PreparedNativeStore.create();
             var server = InProcessServer.startWithConnectionDiagnostics(store);
             var client = MongoClients.create(store.cacheUri())) {
            var control = new ControlPlane(server.baseUrl());
            store.loginAndAwaitBusiness(control);
            control.registerConnector("mongodb", ConnectorJars.bytesFor("mongodb"));
            var settings = Map.<String, Object>of("isUri", true, "uri", store.masterUri());
            var immediate = control.sourceRequest("POST", "/api/connections:test",
                    Map.of("id", "views", "connectorId", "mongodb", "settings", settings), null, 200);
            var latest = control.sourceRequest("GET", "/api/connections/views/test-result", null, null, 200);
            var report = (Map<?, ?>) JsonReader.parse(immediate.body());
            assertThat(report.get("outcome")).isEqualTo("PASSED");
            assertThat(JsonReader.parse(latest.body())).isEqualTo(report);
            var row = SystemCollections.CACHE_CONNECTION_TEST_RESULTS.on(client.getDatabase(store.cacheDatabase()))
                    .find(new Document("_id", "views")).first();
            assertThat(row).containsEntry("formatVersion", 2).containsEntry("state", "PASSED")
                    .containsKeys("cacheScope", "attemptId", "sourceHash", "nodeId", "bootId", "sessionGeneration", "deadlineAt", "report");
        }
    }

    @Test
    void mongodbDefaultSourcePublishesRejectedCredentialsWithinTheManagedAttempt() {
        RealConnectorGate.require("mongodb");
        String cluster = "connection-auth-" + java.util.UUID.randomUUID();
        String password = "wrong-default-source-password";
        String encodedUsername = "fixture-default%3Credacted%3E-source-user";
        String username = "fixture-default<redacted>-source-user";
        String uri = SharedMongo.replicaSetUrl("default_source_connection_test").replace(
                "mongodb://", "mongodb://" + encodedUsername + ":" + password + "@");
        var settings = Map.<String, Object>of("isUri", true, "uri", uri);
        var source = new io.tapstate.core.model.SourceResource("views", null, "mongodb", settings, null, null, null, null);
        try (var store = PreparedNativeStore.withBorrowedMaster(cluster, "e2e", "e2e-password", source);
             var server = InProcessServer.startWithConnectionDiagnostics(store);
             var client = MongoClients.create(store.cacheUri())) {
            var control = new ControlPlane(server.baseUrl());
            store.loginAndAwaitBusiness(control);
            control.registerConnector("mongodb", ConnectorJars.bytesFor("mongodb"));
            var immediate = control.sourceRequest("POST", "/api/connections:test",
                    Map.of("id", "views", "connectorId", "mongodb", "settings", settings), null, 200);
            var latest = control.sourceRequest("GET", "/api/connections/views/test-result", null, null, 200);
            var report = (Map<?, ?>) JsonReader.parse(immediate.body());
            assertThat(report.get("outcome")).isEqualTo("FAILED");
            assertThat(JsonReader.parse(latest.body())).isEqualTo(report);
            assertThat(latest.body()).contains("diagnostic")
                    .doesNotContain(username, encodedUsername, password);
            var row = SystemCollections.CACHE_CONNECTION_TEST_RESULTS.on(client.getDatabase(store.cacheDatabase()))
                    .find(new Document("_id", "views")).first();
            assertThat(row).containsEntry("formatVersion", 2).containsEntry("state", "FAILED")
                    .containsKeys("attemptId", "cacheScope", "deadlineAt", "report");
            assertThat(row.toJson()).doesNotContain(username, encodedUsername, password);
        }
    }

    @Test
    void postgresCapturesNetworkAndTlsRejectionFromTheRealDriver() throws Exception {
        RealConnectorGate.require("postgres");
        var settings = SharedPostgres.settings("connection_contact_diagnostic");
        settings.put("user", settings.remove("username"));
        settings.put("schema", "public");
        try (var store = PreparedNativeStore.create(); var server = InProcessServer.startWithConnectionDiagnostics(store)) {
            var control = new ControlPlane(server.baseUrl());
            store.loginAndAwaitBusiness(control);
            control.registerConnector("postgres", ConnectorJars.bytesFor("postgres"));
            var network = new LinkedHashMap<>(settings);
            network.put("port", unusedLoopbackPort());
            var tls = new LinkedHashMap<>(settings);
            tls.put("useSSL", true);
            var statuses = new org.assertj.core.api.SoftAssertions();
            for (var probe : Map.of("network", network, "tls", tls).entrySet()) {
                String id = "invalid_postgres_" + probe.getKey();
                var response = control.sourceResponse("POST", "/api/connections:test",
                        Map.of("id", id, "connectorId", "postgres", "settings", probe.getValue()), null);
                statuses.assertThat(response.statusCode()).as("%s result %s", probe.getKey(), response.body()).isEqualTo(200);
                if (response.statusCode() != 200) continue;
                var report = (Map<?, ?>) JsonReader.parse(response.body());
                assertThat(report.get("outcome")).isEqualTo("FAILED");
                assertThat(report.get("checks").toString()).contains("diagnostic", probe.getKey().equals("tls")
                        ? "08004" : "Connection refused");
                var latest = control.sourceRequest("GET", "/api/connections/" + id + "/test-result", null, null, 200);
                assertThat(JsonReader.parse(latest.body())).isEqualTo(report);
            }
            statuses.assertAll();
        }
    }

    private static int unusedLoopbackPort() throws java.io.IOException {
        try (var socket = new java.net.ServerSocket(0, 1, java.net.InetAddress.getLoopbackAddress())) {
            return socket.getLocalPort();
        }
    }

    private static Map<?, ?> failedAuthentication(String connector, Map<String, Object> settings, Path home) throws Exception {
        try (var store = PreparedNativeStore.create()) {
            String valid = "valid_" + connector;
            Object legacy;
            try (var server = Tiers.IN_PROCESS.launch(store)) {
                var control = new ControlPlane(server.baseUrl());
                store.loginAndAwaitBusiness(control);
                control.registerConnector(connector, ConnectorJars.bytesFor(connector));
                assertThat(control.testConnection(valid, connector, settings).outcome()).isEqualTo("PASSED");
                var earlier = control.sourceRequest("GET", "/api/connections/" + valid + "/test-result", null, null, 200);
                assertThat(earlier.body()).doesNotContain("\"diagnostic\":");
                legacy = JsonReader.parse(earlier.body());
            }
            Map<?, ?> report;
            String id = "invalid_" + connector;
            try (var server = InProcessServer.startWithConnectionDiagnostics(store)) {
                var control = new ControlPlane(server.baseUrl());
                store.loginAndAwaitBusiness(control);
                var earlier = control.sourceRequest("GET", "/api/connections/" + valid + "/test-result", null, null, 200);
                assertThat(JsonReader.parse(earlier.body())).isEqualTo(legacy);

                String badPassword = "wrong-fixture-password";
                var invalid = new LinkedHashMap<>(settings);
                invalid.put("password", badPassword);
                var immediate = control.sourceResponse("POST", "/api/connections:test",
                        Map.of("id", id, "connectorId", connector, "settings", invalid), null, "en");
                assertThat(immediate.statusCode()).isEqualTo(200);
                var latest = control.sourceResponse("GET", "/api/connections/" + id + "/test-result", null, null, "zh-TW");
                assertThat(latest.statusCode()).isEqualTo(200);
                report = (Map<?, ?>) JsonReader.parse(immediate.body());
                assertThat(report.get("outcome")).isEqualTo("FAILED");
                assertThat((List<?>) report.get("checks")).isNotEmpty();
                assertThat(JsonReader.parse(latest.body())).isEqualTo(report);
                assertThat(immediate.body()).doesNotContain(badPassword);
                assertThat(latest.body()).doesNotContain(badPassword);
                if (connector.equals("postgres")) {
                    ConnectionDiagnosticProcesses.verify(home, store, server.baseUrl(), control, id, report);
                }
            }
            Tiers compatible = connector.equals("postgres") ? Tiers.REAL_PROCESS : Tiers.IN_PROCESS;
            try (var recovered = compatible.launch(store)) {
                var control = new ControlPlane(recovered.baseUrl());
                store.loginAndAwaitBusiness(control);
                var latest = control.sourceRequest("GET", "/api/connections/" + id + "/test-result", null, null, 200);
                assertThat(JsonReader.parse(latest.body())).isEqualTo(report);
            }
            return report;
        }
    }

}
