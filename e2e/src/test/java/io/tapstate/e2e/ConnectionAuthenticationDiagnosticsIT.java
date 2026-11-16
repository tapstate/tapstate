package io.tapstate.e2e;

import io.tapstate.core.common.JsonReader;
import io.tapstate.testsupport.RequiresDocker;
import org.junit.jupiter.api.Test;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

@RequiresDocker
class ConnectionAuthenticationDiagnosticsIT {

    @Test
    void mysqlPublishesReadableChecksAndRetainsTheSameReport() {
        RealConnectorGate.require("mysql");
        var settings = SharedMySql.settings("connection_auth_diagnostic");

        var report = failedAuthentication("mysql", settings);

        assertThat(report.toString())
                .contains("The connection could not be established.", "The database could not be reached",
                        "Check the host, port, database and credentials")
                .doesNotContain("pdk.connection.fail", "pdk.connection.reason", "pdk.connection.solution");
    }

    @Test
    void postgresPublishesRejectedCredentialsAsAFailedConnectionCheck() {
        RealConnectorGate.require("postgres");
        var settings = SharedPostgres.settings("connection_auth_diagnostic");
        settings.put("user", settings.remove("username"));
        settings.put("schema", "public");

        var report = failedAuthentication("postgres", settings);

        assertThat((List<?>) report.get("checks")).anySatisfy(value -> {
            var check = (Map<?, ?>) value;
            assertThat(check.get("status")).isEqualTo("FAILED");
            assertThat(check.get("connectorErrorCode")).isEqualTo("28P01");
            assertThat((String) check.get("message")).contains("password authentication failed");
        });
    }

    private static Map<?, ?> failedAuthentication(String connector, Map<String, Object> settings) {
        try (var store = PreparedNativeStore.create();
             var server = Tiers.IN_PROCESS.launch(store)) {
            var control = new ControlPlane(server.baseUrl());
            store.loginAndAwaitBusiness(control);
            control.registerConnector(connector, ConnectorJars.bytesFor(connector));
            assertThat(control.testConnection("valid_" + connector, connector, settings).outcome())
                    .isEqualTo("PASSED");

            String id = "invalid_" + connector;
            String badPassword = "wrong-fixture-password";
            var invalid = new LinkedHashMap<>(settings);
            invalid.put("password", badPassword);
            var immediate = control.sourceRequest("POST", "/api/connections:test",
                    Map.of("id", id, "connectorId", connector, "settings", invalid), null, 200);
            var latest = control.sourceRequest("GET", "/api/connections/" + id + "/test-result", null, null, 200);
            var report = (Map<?, ?>) JsonReader.parse(immediate.body());
            assertThat(report.get("outcome")).isEqualTo("FAILED");
            assertThat((List<?>) report.get("checks")).isNotEmpty();
            assertThat(JsonReader.parse(latest.body())).isEqualTo(report);
            assertThat(immediate.body()).doesNotContain(badPassword);
            assertThat(latest.body()).doesNotContain(badPassword);
            return report;
        }
    }
}
