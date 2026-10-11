package io.tapstate.e2e;

import io.tapstate.core.common.JsonWriter;
import io.tapstate.core.lifecycle.LifecycleVerb;
import io.tapstate.testsupport.RequiresDocker;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.sql.Connection;
import java.sql.Statement;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Drives a real AWS RDS MySQL PDK artifact against a local MySQL fixture through the product's
 * HTTP connection-test and schema-discovery verbs, then snapshot and change capture into MongoDB.
 * This proves the source wiring and real PDK callbacks, not compatibility with an Amazon RDS instance,
 * its network, TLS settings or binlog policy.
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

    @Test
    void registeredArtifactRoutesSnapshotAndChangesFromLocalMysqlToMongo() throws Exception {
        RealConnectorGate.require("mongodb");
        Map<String, Object> settings = SharedMySql.settings("aws_rds_source_data_flow");
        try (Connection mysql = SharedMySql.connect(settings); Statement statement = mysql.createStatement()) {
            statement.execute("DROP TABLE IF EXISTS orders");
            statement.execute("CREATE TABLE orders (id BIGINT PRIMARY KEY, name VARCHAR(64))");
            statement.execute("INSERT INTO orders (id, name) VALUES (1, 'seeded')");
        }

        String storeUri = SharedMongo.replicaSetUrl("aws_rds_data_flow_store");
        String targetUri = SharedMongo.replicaSetUrl("aws_rds_data_flow_target");
        EndpointAddress target = EndpointAddress.uri(targetUri);
        try (MongoEndpoints mongo = new MongoEndpoints(); ServerHandle server = Tiers.IN_PROCESS.launch(storeUri)) {
            ControlPlane control = new ControlPlane(server.baseUrl());
            control.bootstrapAndLogin("e2e", "e2e-password");
            control.registerConnector("aws-rds-mysql", ConnectorJars.bytesFor("aws-rds-mysql"));
            control.registerConnector("mongodb", ConnectorJars.bytesFor("mongodb"));

            Map<String, String> resources = new LinkedHashMap<>();
            resources.put("rds.tap.yml", sourceYaml(settings));
            resources.put("mongo.tap.yml", """
                    version: tapstate/v1
                    kind: source
                    id: mongo_target
                    connector: mongodb
                    config: { uri: "%s" }
                    """.formatted(targetUri));
            resources.put("pipeline.tap.yml", """
                    version: tapstate/v1
                    kind: pipeline
                    id: rds_to_mongo
                    source: rds_source
                    settings: { read_mode: snapshot_and_cdc }
                    transforms:
                      - { id: all_rows, from: [orders], type: filter, expr: "true" }
                    serve:
                      from: all_rows
                      sync:
                        - source: mongo_target
                    """);
            control.apply(resources);
            control.discoverSchema("rds_source", "aws-rds-mysql", settings);
            control.lifecycle("rds_to_mongo", LifecycleVerb.START);

            awaitNames(mongo, target, List.of("seeded"), "RDS PDK snapshot");
            try (Connection mysql = SharedMySql.connect(settings); Statement statement = mysql.createStatement()) {
                statement.execute("UPDATE orders SET name = 'updated' WHERE id = 1");
                statement.execute("INSERT INTO orders (id, name) VALUES (2, 'new')");
            }
            awaitNames(mongo, target, List.of("new", "updated"), "RDS PDK update and insert");
            try (Connection mysql = SharedMySql.connect(settings); Statement statement = mysql.createStatement()) {
                statement.execute("DELETE FROM orders WHERE id = 2");
            }
            awaitNames(mongo, target, List.of("updated"), "RDS PDK delete");
        }
    }

    private static void awaitNames(MongoEndpoints mongo, EndpointAddress target, List<String> expected, String phase) {
        Await.until(phase, () -> names(mongo, target).equals(expected),
                () -> names(mongo, target).toString());
    }

    private static List<String> names(MongoEndpoints mongo, EndpointAddress target) {
        return mongo.documents(target, "orders").stream().map(document -> document.getString("name"))
                .sorted().toList();
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
