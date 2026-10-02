package io.tapstate.e2e;

import io.tapstate.testsupport.DockerGate;
import org.bson.Document;
import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Every MongoDB-family connector a pipeline can write to reads rows through the data browser with its own
 * real connector: one row by the bounded read, and the collection's size by its statistics.
 *
 * <p>Those two reads are how a start looks at a target before a new full load goes into it, and the data
 * browser serves them only to connectors it was told can answer them. Telling it so is a claim about each
 * connector, and only that connector, driven for real, can back it: a connector that registered the command
 * the read travels on but cannot read would refuse inside the driver, and the start would never ask.
 *
 * <p>Gated on Docker and on each connector's real jar, which only this witness needs: the real-connector
 * lane stages one connector per kind of database, and these three are MongoDB's siblings rather than kinds
 * of their own. A connector whose jar is not in the directory is skipped and says so, wherever it runs.
 * Run it with a directory holding the three jars, named {@code <id>-connector-<version>.jar}:
 *
 * <pre>
 *   mvn -pl e2e -am verify -Dapi.version=1.44 \
 *     -Dtapstate.e2e.connectors-dir=/path/to/mongodb-family-connectors \
 *     -Dit.test=MongoFamilyTargetsAreBrowsableIT -Dfailsafe.failIfNoSpecifiedTests=false
 * </pre>
 */
class MongoFamilyTargetsAreBrowsableIT {

    private static final String COLLECTION = "orders";
    private static final String DOCUMENT_ID = "order-1";

    @BeforeAll
    static void requireDocker() {
        DockerGate.require();
    }

    @ParameterizedTest
    @ValueSource(strings = {"mongodb-atlas", "aliyun-db-mongodb", "tencent-db-mongodb"})
    void readsOneRowAndTheCollectionSizeThroughTheConnector(String connectorId) {
        Assumptions.assumeTrue(ConnectorJars.directoryNamed() && ConnectorJars.resolves(connectorId),
                () -> "no " + connectorId + " connector jar in -Dtapstate.e2e.connectors-dir: this witness runs "
                        + "where the MongoDB family's jars are staged, which the real-connector lane does not do");
        String database = "e2e_family_" + connectorId.replace('-', '_');
        String dataUri = SharedMongo.replicaSetUrl(database);

        try (ServerHandle server = InProcessServer.start(SharedMongo.replicaSetUrl(database + "_store"));
                MongoEndpoints mongo = new MongoEndpoints()) {
            mongo.insert(EndpointAddress.uri(dataUri), COLLECTION,
                    new Document("_id", DOCUMENT_ID).append("status", "paid"));

            ControlPlane control = new ControlPlane(server.baseUrl());
            control.bootstrapAndLogin("e2e", "e2e-password");
            control.registerConnector(connectorId, ConnectorJars.bytesFor(connectorId));
            control.apply(Map.of("warehouse.tap.yml", sourceYaml(connectorId, dataUri, database)));

            Map<String, Object> first = control.find("warehouse", COLLECTION, Map.of("limit", 1));
            assertThat(idsOf(first))
                    .as("the one row a bounded read through %s answers with", connectorId)
                    .containsExactly(DOCUMENT_ID);

            Map<String, Object> stats = control.stats("warehouse", COLLECTION);
            assertThat(((Number) stats.get("numOfRows")).longValue())
                    .as("the collection's size as %s reports it", connectorId)
                    .isEqualTo(1L);
        }
    }

    @SuppressWarnings("unchecked")
    private static List<Object> idsOf(Map<String, Object> answer) {
        return ((List<Map<String, Object>>) answer.get("rows")).stream().map(row -> row.get("_id")).toList();
    }

    private static String sourceYaml(String connectorId, String dataUri, String database) {
        return """
                version: tapstate/v1
                kind: source
                id: warehouse
                connector: %s
                config: { uri: "%s", database: %s }
                """
                .formatted(connectorId, dataUri, database);
    }
}
