package io.tapstate.e2e;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

class OracleNamespaceTest {
    @Test
    void generatedSchemasStayWithinTheLogMinerIdentifierLimit() {
        String database = ProvisionedStores.database("src_ora",
                "e2e_an_oracle_table_reaches_the_managed_store_in_process");
        assertThat(database).hasSizeGreaterThan(30);
        assertThat(SharedOracle.schemaName(database)).hasSize(30).matches("[A-Z0-9_]+");
    }

    @Test
    void shorteningKeepsRunsWithTheSamePrefixIsolated() {
        String prefix = "e2e_src_ora_a_long_example_name_shared_by_both_execution_tiers_";
        String first = SharedOracle.schemaName(prefix + "in_process");
        String second = SharedOracle.schemaName(prefix + "real_process");
        assertThat(first).isNotEqualTo(second);
        assertThat(first).hasSize(30);
        assertThat(second).hasSize(30);
    }

    @Test
    void aShortSchemaKeepsItsReadableName() {
        assertThat(SharedOracle.schemaName("e2e_orders_a12")).isEqualTo("E2E_ORDERS_A12");
    }
}
