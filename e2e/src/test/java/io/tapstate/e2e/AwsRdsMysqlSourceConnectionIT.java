package io.tapstate.e2e;

import io.tapstate.core.common.JsonWriter;
import io.tapstate.testsupport.RequiresDocker;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.sql.Connection;
import java.sql.Statement;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Drives a real AWS RDS MySQL PDK artifact against a local MySQL fixture through the product's
 * HTTP connection-test and schema-discovery verbs. This proves the source wiring and real PDK callbacks,
 * not compatibility with an Amazon RDS instance, its network, TLS settings or binlog policy.
 */
@RequiresDocker
class AwsRdsMysqlSourceConnectionIT {

    @BeforeAll
    static void requireRealArtifact() {
        RealConnectorGate.require("aws-rds-mysql");
    }

    @Test
    void registeredArtifactTestsAndDiscoversALocalMysqlSource() throws Exception {
        Map<String, Object> settings = SharedMySql.settings("aws_rds_source_connection");
        try (Connection mysql = SharedMySql.connect(settings); Statement statement = mysql.createStatement()) {
            statement.execute("DROP TABLE IF EXISTS orders");
            statement.execute("CREATE TABLE orders (id BIGINT PRIMARY KEY, name VARCHAR(64))");
            statement.execute("INSERT INTO orders (id, name) VALUES (1, 'first')");
        }

        String storeUri = SharedMongo.replicaSetUrl("aws_rds_source_connection_store");
        try (ServerHandle server = Tiers.IN_PROCESS.launch(storeUri)) {
            ControlPlane control = new ControlPlane(server.baseUrl());
            control.bootstrapAndLogin("e2e", "e2e-password");
            control.registerConnector("aws-rds-mysql", ConnectorJars.bytesFor("aws-rds-mysql"));
            control.apply(Map.of("rds.tap.yml", sourceYaml(settings)));

            ControlPlane.ConnectionTest result = control.testConnection(
                    "rds_source", "aws-rds-mysql", settings);
            assertThat(result.outcome()).as("actual connector connectionTest checks: %s", result.statusByCheck())
                    .isEqualTo("PASSED");
            assertThat(result.statusByCheck()).isNotEmpty();

            control.discoverSchema("rds_source", "aws-rds-mysql", settings);
            assertThat(control.sourceSchemaTables("rds_source")).contains("orders");
            assertThat(control.sourceSchemaFields("rds_source", "orders")).containsAll(List.of("id", "name"));
        }
    }

    private static String sourceYaml(Map<String, Object> settings) {
        return """
                version: tapstate/v1
                kind: source
                id: rds_source
                connector: aws-rds-mysql
                config: %s
                mode: cdc
                tables: [ orders ]
                """.formatted(JsonWriter.write(settings));
    }
}
