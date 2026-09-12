package io.tapstate.e2e;

import org.junit.jupiter.api.Test;

import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

class EnterpriseEndpointsTest {
    @Test
    void oracleSidAddressDoesNotRequireAServiceName() {
        EndpointAddress address = new EndpointAddress("source", Map.of(
                "host", "database.example", "port", 1521, "thinType", "SID", "sid", "ORCL"));
        assertThat(new OracleEndpoints().url(address)).isEqualTo("jdbc:oracle:thin:@database.example:1521:ORCL");
    }

    @Test
    void oraclePdbAndSchemaComeFromTheResource() {
        EndpointAddress address = new EndpointAddress("source", Map.of(
                "host", "database.example", "port", 1522, "database", "FREE", "pdb", "PDB_OTHER", "schema", "OTHER"));
        OracleEndpoints endpoints = new OracleEndpoints();
        assertThat(endpoints.url(address)).isEqualTo("jdbc:oracle:thin:@//database.example:1522/PDB_OTHER");
        assertThat(endpoints.table(address, "orders")).isEqualTo("\"OTHER\".\"orders\"");
    }

    @Test
    void sqlServerDatabaseAndSchemaComeFromTheResource() {
        EndpointAddress address = new EndpointAddress("source", Map.of(
                "host", "database.example", "port", 1434, "database", "other", "schema", "audit"));
        SqlServerEndpoints endpoints = new SqlServerEndpoints();
        assertThat(endpoints.url(address)).contains("database.example:1434;databaseName={other};");
        assertThat(endpoints.table(address, "orders")).isEqualTo("\"audit\".\"orders\"");
    }
}
