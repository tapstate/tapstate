package io.tapstate.e2e;

import io.tapstate.core.lifecycle.LifecycleVerb;
import io.tapstate.testsupport.DockerGate;
import org.bson.Document;
import org.bson.types.Decimal128;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * A MongoDB 128-bit decimal keeps every significant digit through a pipeline to another MongoDB.
 *
 * <p>The document is written directly with the driver, carried through the shipped pipeline, and read
 * directly from the target with the driver. The real connector's read conversion narrows the source
 * value to a double, so a synthetic connector would only repeat this repository's opinion about the
 * bug. Reading the target outside the product also prevents its read path from agreeing with its own
 * narrowed write.
 *
 * <p>Java rather than a declarative example because the specification vocabulary cannot seed a BSON
 * {@code Decimal128} or require that exact stored type at the target. A text rendering could look
 * decimal-shaped after the value had already been narrowed, so the assertion compares the target's
 * actual 128-bit decimal digit for digit.
 *
 * <p>Gated on Docker and on the real connector jar. Run it with:
 *
 * <pre>
 *   mvn -pl e2e -am verify -Dapi.version=1.44 \
 *     -Dtapstate.e2e.connectors-dir=/path/to/connectors \
 *     -Dit.test=MongoDecimal128RoundTripIT -Dfailsafe.failIfNoSpecifiedTests=false
 * </pre>
 */
class MongoDecimal128RoundTripIT {

    private static final String SOURCE_ID = "src_mongo";
    private static final String TARGET_ID = "tgt_mongo";
    private static final String PIPELINE_ID = "decimal128_round_trip";
    private static final String COLLECTION = "orders";
    private static final String SOURCE_DATABASE = "e2e_decimal128_src";
    private static final String TARGET_DATABASE = "e2e_decimal128_tgt";
    private static final String DOCUMENT_ID = "exact-decimal";
    private static final Decimal128 AMOUNT =
            Decimal128.parse("1234567890.123456789012345678901234");
    private static final Duration TIMEOUT = Duration.ofSeconds(30);

    @BeforeAll
    static void requireDockerAndTheRealConnector() {
        DockerGate.require();
        RealConnectorGate.require("mongodb");
    }

    @Test
    void keepsEverySignificantDigitAtTheTarget() {
        String storeUri = SharedMongo.replicaSetUrl("e2e_decimal128_store");
        String sourceUri = SharedMongo.replicaSetUrl(SOURCE_DATABASE);
        String targetUri = SharedMongo.replicaSetUrl(TARGET_DATABASE);
        EndpointAddress source = EndpointAddress.uri(sourceUri);
        EndpointAddress target = EndpointAddress.uri(targetUri);

        try (ServerHandle server = InProcessServer.start(storeUri);
                MongoEndpoints mongo = new MongoEndpoints()) {
            mongo.insert(source, COLLECTION,
                    new Document("_id", DOCUMENT_ID).append("amount", AMOUNT));

            ControlPlane control = new ControlPlane(server.baseUrl());
            control.bootstrapAndLogin("e2e", "e2e-password");
            control.registerConnector("mongodb", ConnectorJars.bytesFor("mongodb"));
            control.apply(Map.of(
                    "src_mongo.tap.yml", sourceYaml(sourceUri),
                    "tgt_mongo.tap.yml", targetYaml(targetUri),
                    "pipeline.tap.yml", pipelineYaml()));
            control.discoverSchema(SOURCE_ID, "mongodb",
                    Map.of("uri", sourceUri, "database", SOURCE_DATABASE));
            control.lifecycle(PIPELINE_ID, LifecycleVerb.START);

            Await.until("the decimal document to reach the target", TIMEOUT,
                    () -> !mongo.documents(target, COLLECTION).isEmpty(),
                    () -> reading(control, mongo.documents(target, COLLECTION)));

            List<Document> written = mongo.documents(target, COLLECTION);
            assertThat(written).as("the documents the target holds").hasSize(1);
            Object amount = written.getFirst().get("amount");
            assertThat(amount)
                    .as("the decimal column at the target")
                    .isInstanceOf(Decimal128.class);
            assertThat(((Decimal128) amount).bigDecimalValue())
                    .as("every significant digit of the decimal column")
                    .isEqualTo(AMOUNT.bigDecimalValue());
        }
    }

    private static String reading(ControlPlane control, List<Document> rows) {
        return "target rows=" + rows
                + ", state=" + control.state(PIPELINE_ID)
                + ", logs=" + control.logs(PIPELINE_ID);
    }

    private static String sourceYaml(String uri) {
        return """
                version: tapstate/v1
                kind: source
                id: %s
                connector: mongodb
                config: { uri: "%s", database: %s }
                mode: cdc
                tables: [ %s ]
                """
                .formatted(SOURCE_ID, uri, SOURCE_DATABASE, COLLECTION);
    }

    private static String targetYaml(String uri) {
        return """
                version: tapstate/v1
                kind: source
                id: %s
                connector: mongodb
                config: { uri: "%s", database: %s }
                """
                .formatted(TARGET_ID, uri, TARGET_DATABASE);
    }

    private static String pipelineYaml() {
        return """
                version: tapstate/v1
                kind: pipeline
                id: %s
                source: %s
                settings: { read_mode: snapshot_and_cdc }
                serve:
                  from: %s
                  sync:
                    - source: %s
                """
                .formatted(PIPELINE_ID, SOURCE_ID, COLLECTION, TARGET_ID);
    }
}
