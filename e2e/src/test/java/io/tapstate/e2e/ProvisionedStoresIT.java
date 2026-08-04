package io.tapstate.e2e;

import io.tapstate.testsupport.DockerGate;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.util.LinkedHashMap;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Holds the provisioning seam to the only promise a specification can make of it: that the address it
 * publishes reaches a store that is really there, and that the driver handed out really reads it.
 *
 * <p>Checked here rather than through a published example, because an example proves this only in passing
 * and only once one exists. A seam that published a plausible-looking but wrong address would fail far
 * away from its cause - as a pipeline that never moves a row - and the reading that would have named the
 * fault is exactly the one this test takes.
 *
 * <p>Both readings go through the address as published, character for character, rather than through the
 * container handle the harness happens to hold. Reading through the handle would agree with the harness by
 * construction and would keep agreeing while the published address pointed at nothing.
 */
class ProvisionedStoresIT {

    @BeforeAll
    static void requireDocker() {
        DockerGate.require();
    }

    @Test
    void aRequestedMySqlAnswersAtTheAddressItPublishes() {
        Map<String, DatabaseRequest> asked = Map.of("src", new DatabaseRequest(DatabaseKind.MYSQL));

        try (ProvisionedStores stores = ProvisionedStores.provision(asked, "mysql_reachable")) {
            Map<String, String> environment = stores.environment();
            assertThat(environment)
                    .as("a JDBC store has no single setting that is its address, so all five are published")
                    .containsKeys("SRC_HOST", "SRC_PORT", "SRC_DATABASE", "SRC_USERNAME", "SRC_PASSWORD");

            Endpoints driver = stores.driversByConnector().get("mysql");
            assertThat(driver).as("the kind decides the driver, and mysql is reached with mysql").isNotNull();

            // Dial exactly what a resource would have interpolated, and nothing the harness kept back.
            EndpointAddress asAResourceWouldWriteIt = addressFrom(environment, "SRC", "src_mysql");
            driver.seed(asAResourceWouldWriteIt, "orders", SeedRows.generated(3));

            assertThat(driver.count(asAResourceWouldWriteIt, "orders")).isEqualTo(3L);
        }
    }

    @Test
    void aRequestedMongoAnswersAtTheAddressItPublishes() {
        Map<String, DatabaseRequest> asked = Map.of("tgt", new DatabaseRequest(DatabaseKind.MONGO));

        try (ProvisionedStores stores = ProvisionedStores.provision(asked, "mongo_reachable")) {
            assertThat(stores.environment())
                    .as("a Mongo store does answer on one string, so one is published")
                    .containsOnlyKeys("TGT_URI");

            Endpoints driver = stores.driversByConnector().get("mongodb");
            EndpointAddress address = EndpointAddress.uri(stores.environment().get("TGT_URI"));
            driver.seed(address, "orders", SeedRows.generated(2));

            assertThat(driver.count(address, "orders")).isEqualTo(2L);
        }
    }

    /**
     * Two stores asked for together get databases of their own. Sharing one would let a specification
     * count, in its target, the rows the harness seeded into its source - green on the first poll, with
     * the product never consulted.
     */
    @Test
    void twoStoresAskedForTogetherDoNotShareData() {
        Map<String, DatabaseRequest> asked = new LinkedHashMap<>();
        asked.put("src", new DatabaseRequest(DatabaseKind.MYSQL));
        asked.put("tgt", new DatabaseRequest(DatabaseKind.MYSQL));

        try (ProvisionedStores stores = ProvisionedStores.provision(asked, "two_stores")) {
            Map<String, String> environment = stores.environment();
            assertThat(environment.get("SRC_DATABASE"))
                    .as("each store takes a database of its own on the shared server")
                    .isNotEqualTo(environment.get("TGT_DATABASE"));

            Endpoints driver = stores.driversByConnector().get("mysql");
            EndpointAddress source = addressFrom(environment, "SRC", "src_mysql");
            EndpointAddress target = addressFrom(environment, "TGT", "tgt_mysql");

            driver.seed(source, "orders", SeedRows.generated(4));

            assertThat(driver.count(source, "orders")).isEqualTo(4L);
            assertThat(driver.count(target, "orders"))
                    .as("the target holds nothing the source was seeded with")
                    .isZero();
        }
    }

    /**
     * A guard that must not trust interpolation still has to learn where a resource points; the only
     * honest witness is the database name, which this run minted and which appears in an address if and
     * only if that store's published references were interpolated into it. An address naming no store of
     * this run answers empty rather than guessing.
     */
    @Test
    void theStoreHoldingAnAddressIsToldByTheDatabaseTheAddressNames() {
        Map<String, DatabaseRequest> asked = new LinkedHashMap<>();
        asked.put("src", new DatabaseRequest(DatabaseKind.MYSQL));
        asked.put("tgt", new DatabaseRequest(DatabaseKind.MONGO));

        try (ProvisionedStores stores = ProvisionedStores.provision(asked, "who_holds_what")) {
            Map<String, String> environment = stores.environment();
            EndpointAddress source = addressFrom(environment, "SRC", "src_mysql");
            EndpointAddress target = new EndpointAddress(
                    "tgt_mongo", Map.of("uri", environment.get("TGT_URI")));

            assertThat(stores.storeHolding(source)).contains("src");
            assertThat(stores.storeHolding(target)).contains("tgt");
            assertThat(stores.storeHolding(EndpointAddress.uri("mongodb://elsewhere/e2e_no_such_db")))
                    .as("an address naming none of this run's databases belongs to none of its stores")
                    .isEmpty();
        }
    }

    /**
     * The count taken by store name reads over the handle this run kept for itself, so it agrees with
     * the published address when both point at the same store - and it is the reading a guard falls back
     * on precisely because no example could have named it.
     */
    @Test
    void aStoreIsCountedByTheHandleTheRunKeptForItself() {
        Map<String, DatabaseRequest> asked = Map.of("src", new DatabaseRequest(DatabaseKind.MYSQL));

        try (ProvisionedStores stores = ProvisionedStores.provision(asked, "counted_by_own_handle")) {
            EndpointAddress published = addressFrom(stores.environment(), "SRC", "src_mysql");
            stores.driversByConnector().get("mysql").seed(published, "orders", SeedRows.generated(3));

            assertThat(stores.count("src", "orders")).isEqualTo(3L);
        }
    }

    /** The address a resource writes, assembled from the published references exactly as it would. */
    private static EndpointAddress addressFrom(Map<String, String> environment, String prefix, String id) {
        Map<String, Object> settings = new LinkedHashMap<>();
        for (String setting : new String[] {"host", "port", "database", "username", "password"}) {
            settings.put(setting, environment.get(prefix + "_" + setting.toUpperCase(java.util.Locale.ROOT)));
        }
        return new EndpointAddress(id, settings);
    }
}
