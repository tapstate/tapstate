package io.tapstate.e2e;

import com.mongodb.client.MongoClients;
import io.tapstate.testsupport.DockerGate;
import org.bson.Document;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Date;
import java.util.Map;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * HTTP discovery reclaims persisted abandoned generations without sweeping active writers.
 * The declarative vocabulary cannot seed unpublished schema generations or inspect writer leases,
 * so this case uses Mongo only for fault-state setup and persistence assertions.
 */
class SchemaReclamationIT {

    private static final String SOURCE_ID = "src_reclamation";

    @TempDir
    private Path connectorJars;

    @TempDir
    private Path sourceDirectory;

    @BeforeAll
    static void requireDocker() {
        DockerGate.require();
    }

    @Test
    void rediscoveryReclaimsAbandonedTablesAndPreservesActiveAndCurrentTables() throws Exception {
        Path connectorJar = E2eConnectorJar.buildInto(connectorJars);
        Files.writeString(sourceDirectory.resolve("orders.csv"), "id,amount\n1,12\n");
        String database = "e2e_schema_reclamation_" + UUID.randomUUID().toString().replace("-", "");
        String storeUri = SharedMongo.replicaSetUrl(database);
        String previous = System.setProperty("tapstate.e2e.connectors-dir", connectorJars.toString());
        try (var client = MongoClients.create(storeUri);
             ServerHandle server = InProcessServer.start(storeUri)) {
            ControlPlane control = new ControlPlane(server.baseUrl());
            control.bootstrapAndLogin("e2e", "e2e-password");
            control.registerConnector(E2eConnectorJar.CONNECTOR_ID, Files.readAllBytes(connectorJar));
            control.apply(Map.of("source.tap.yml", """
                    version: tapstate/v1
                    kind: source
                    id: src_reclamation
                    connector: e2e_file
                    config: { uri: "%s" }
                    mode: cdc
                    tables: [ orders ]
                    """.formatted(sourceDirectory)));
            control.discoverSchema(SOURCE_ID, E2eConnectorJar.CONNECTOR_ID,
                    Map.of("uri", sourceDirectory.toString()));

            var schemas = client.getDatabase(database).getCollection("source_schemas");
            Document table = schemas.find(new Document("name", "orders")).first();
            assertThat(table).isNotNull();
            String abandoned = UUID.randomUUID().toString();
            String active = UUID.randomUUID().toString();
            // Persist the state left after table insertion but before envelope publication. Copy a
            // real discovered table so the fixture retains the product's physical schema payload.
            schemas.insertOne(unpublished(table, abandoned));
            schemas.insertOne(unpublished(table, active));
            schemas.updateOne(new Document("_id", SOURCE_ID),
                    new Document("$set", new Document("_writers." + abandoned, new Date(0)))
                            .append("$currentDate", new Document("_writers." + active, true))
                            .append("$inc", new Document("_revision", 1L)));
            assertThat(schemas.countDocuments()).isEqualTo(4);
            assertThat(control.connectionSchemaTables(SOURCE_ID)).containsExactly("orders");

            control.discoverSchema(SOURCE_ID, E2eConnectorJar.CONNECTOR_ID,
                    Map.of("uri", sourceDirectory.toString()));

            assertThat(control.connectionSchemaTables(SOURCE_ID)).containsExactly("orders");
            assertThat(control.sourceSchemaTables(SOURCE_ID)).containsExactly("orders");
            assertThat(schemas.countDocuments(new Document("generation", abandoned))).isZero();
            assertThat(schemas.countDocuments(new Document("generation", active))).isEqualTo(1);
            assertThat(schemas.countDocuments()).isEqualTo(3);
        } finally {
            if (previous == null) {
                System.clearProperty("tapstate.e2e.connectors-dir");
            } else {
                System.setProperty("tapstate.e2e.connectors-dir", previous);
            }
        }
    }

    private static Document unpublished(Document table, String generation) {
        return new Document(table)
                .append("_id", SOURCE_ID + "." + generation + "." + table.getInteger("order"))
                .append("generation", generation);
    }
}
