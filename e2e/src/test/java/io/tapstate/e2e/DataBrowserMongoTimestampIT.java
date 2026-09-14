package io.tapstate.e2e;

import io.tapstate.testsupport.DockerGate;
import org.bson.BsonTimestamp;
import org.bson.Document;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * A BSON timestamp reads from its seconds half rather than treating those seconds as milliseconds.
 *
 * <p>The value is written straight into MongoDB and read through the public data-browser face after
 * the real MongoDB connector is registered. A synthetic connector would only repeat this repository's
 * opinion about the conversion; the real connector is what produces the seconds-as-milliseconds value
 * the shared translator has to recognize.
 *
 * <p>The exact rendered value is the contract this face currently exposes. Reverting the translator
 * correction changes its seconds to {@code 1767225} and its nanoseconds to {@code 600000000}, so this
 * assertion cannot pass on the pre-fix behavior.
 *
 * <p>Gated on Docker and on the real connector jar. Run it with:
 *
 * <pre>
 *   mvn -pl e2e -am verify -Dapi.version=1.44 \
 *     -Dtapstate.e2e.connectors-dir=/path/to/connectors \
 *     -Dit.test=DataBrowserMongoTimestampIT -Dfailsafe.failIfNoSpecifiedTests=false
 * </pre>
 */
class DataBrowserMongoTimestampIT {

    private static final String SOURCE_ID = "src_mongo";
    private static final String COLLECTION = "orders";
    private static final String DATABASE = "e2e_timestamp";
    private static final String DOCUMENT_ID = "order-259";
    private static final String TIMESTAMP_FIELD = "at";

    @BeforeAll
    static void requireDockerAndTheRealConnector() {
        DockerGate.require();
        RealConnectorGate.require("mongodb");
    }

    @Test
    void readsTheTimestampAsTheInstantNamedByItsEpochSeconds() {
        String storeUri = SharedMongo.replicaSetUrl("e2e_timestamp_store");
        String dataUri = SharedMongo.replicaSetUrl(DATABASE);

        try (ServerHandle server = InProcessServer.start(storeUri);
                MongoEndpoints mongo = new MongoEndpoints()) {
            mongo.insert(EndpointAddress.uri(dataUri), COLLECTION,
                    new Document("_id", DOCUMENT_ID)
                            .append(TIMESTAMP_FIELD, new BsonTimestamp(1_767_225_600, 7)));

            ControlPlane control = new ControlPlane(server.baseUrl());
            control.bootstrapAndLogin("e2e", "e2e-password");
            control.registerConnector("mongodb", ConnectorJars.bytesFor("mongodb"));
            control.apply(Map.of("src_mongo.tap.yml", sourceYaml(dataUri)));

            assertThat(timestampOf(control.find(SOURCE_ID, COLLECTION, Map.of())))
                    .as("the timestamp returned by the public browse face")
                    .isEqualTo("DateTime nano 0 seconds 1767225600 timeZone null");
        }
    }

    @SuppressWarnings("unchecked")
    private static Object timestampOf(Map<String, Object> answer) {
        assertThat(answer).containsKey("rows");
        for (Map<String, Object> row : (List<Map<String, Object>>) answer.get("rows")) {
            if (DOCUMENT_ID.equals(row.get("_id"))) {
                return row.get(TIMESTAMP_FIELD);
            }
        }
        throw new AssertionError("the browse face carried no row identified as " + DOCUMENT_ID
                + "; it answered with " + answer.get("rows"));
    }

    private static String sourceYaml(String dataUri) {
        return """
                version: tapstate/v1
                kind: source
                id: src_mongo
                connector: mongodb
                config: { uri: "%s", database: %s }
                """
                .formatted(dataUri, DATABASE);
    }
}
