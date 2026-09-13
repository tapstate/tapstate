package io.tapstate.e2e;

import io.tapstate.testsupport.RequiresDocker;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Independently verifies the new stores' seed and readback paths before any connector is driven. */
@RequiresDocker
class EnterpriseEndpointsIT {
    @BeforeAll
    static void requireArtifacts() {
        RealConnectorGate.require("oracle", "sqlserver");
    }

    @ParameterizedTest
    @ValueSource(strings = {"ORACLE", "SQLSERVER"})
    void newStoreCanSeedReadUpdateAndDelete(String name) throws Exception {
        DatabaseKind kind = DatabaseKind.valueOf(name);
        try (ProvisionedStores stores = ProvisionedStores.provision(
                Map.of("src", new DatabaseRequest(kind)), "enterprise_driver_" + name.toLowerCase(java.util.Locale.ROOT))) {
            Map<String, Object> settings = new java.util.LinkedHashMap<>();
            stores.environment().forEach((key, value) -> settings.put(
                    key.substring("SRC_".length()).toLowerCase(java.util.Locale.ROOT), value));
            EndpointAddress address = new EndpointAddress("src", settings);
            assertThat(stores.storeHolding(address)).contains("src");
            Endpoints driver = stores.driversByConnector().get(kind.connectorId());
            assertThat(driver.count(address, "missing")).isZero();
            driver.seed(address, "orders", List.of(Map.of("id", 1L, "seq", 10L)));
            try (var connection = ((EnterpriseJdbcEndpoints) driver).connect(address);
                 var query = connection.createStatement();
                 var flags = query.executeQuery(kind == DatabaseKind.ORACLE
                         ? "SELECT LOG_MODE, SUPPLEMENTAL_LOG_DATA_MIN FROM v$database"
                         : "SELECT is_cdc_enabled FROM sys.databases WHERE name = DB_NAME()")) {
                assertThat(flags.next()).isTrue();
                if (kind == DatabaseKind.ORACLE) {
                    assertThat(flags.getString(1)).isEqualTo("ARCHIVELOG");
                    assertThat(flags.getString(2)).isIn("YES", "IMPLICIT");
                } else {
                    assertThat(flags.getBoolean(1)).isTrue();
                }
            }
            try (ProvisionedStores other = ProvisionedStores.provision(
                    Map.of("src", new DatabaseRequest(kind)), "enterprise_isolated_" + name)) {
                assertThat(other.count("src", "orders")).isZero();
            }
            assertThat(driver.count(address, "orders")).isEqualTo(1);
            driver.update(address, "orders", Map.of("id", 1L), Map.of("seq", 20L));
            assertThat(driver.fetch(address, "orders", Map.of("id", 1L)).orElseThrow().get("seq"))
                    .isInstanceOfSatisfying(Number.class, value -> assertThat(value.longValue()).isEqualTo(20));
            driver.insert(address, "orders", List.of(Map.of("id", 2L, "seq", 2L)));
            driver.cdc(address, "orders", CdcOp.INSERT, 2);
            assertThat(driver.count(address, "orders")).isEqualTo(4);
            driver.cdc(address, "orders", CdcOp.UPDATE, 1);
            assertThat(driver.fetch(address, "orders", Map.of("id", 1L)).orElseThrow().get("seq"))
                    .isInstanceOfSatisfying(Number.class, value -> assertThat(value.longValue()).isEqualTo(-1));
            driver.delete(address, "orders", Map.of("id", 1L));
            driver.cdc(address, "orders", CdcOp.DELETE, 3);
            assertThat(driver.count(address, "orders")).isZero();
            assertThatThrownBy(() -> driver.update(address, "orders", Map.of("id", 1L), Map.of("seq", 3L)))
                    .isInstanceOf(EnvelopeException.class).hasMessageContaining("exactly one row");
        }
    }
}
