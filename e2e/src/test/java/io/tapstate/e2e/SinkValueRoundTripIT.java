package io.tapstate.e2e;

import io.tapstate.core.lifecycle.LifecycleVerb;
import io.tapstate.testsupport.DockerGate;
import org.bson.Document;
import org.bson.types.Binary;
import org.bson.types.ObjectId;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * A value written to a target of the same kind as its source arrives as the type it left as.
 *
 * <p>The read path converts a driver's own type into something portable so that everything between
 * the two ends can hold it. The write path has to undo that, or a key that travelled as text is
 * written as text: the row lands, the write reports success, and only the target's own column type is
 * wrong - which nothing on the read side can see, and no count or row comparison would move.
 *
 * <p>Three columns, each opening a different hole:
 *
 * <ul>
 *   <li>the <b>identity</b>, which comes back only if the write side hands the target the object the
 *       row carried rather than the text it travelled as. Without that it is a plain string, and a
 *       collection keyed by string identities behaves correctly right up until something joins it;</li>
 *   <li>a <b>binary</b> column, which says whether the write side runs the target's conversion at all
 *       - unrun, it arrives as the intermediate the read side produced;</li>
 *   <li>a plain <b>integer</b>, which says the two lanes did not get mixed up. An ordinary box takes
 *       the bare lane and must stay bare; an implementation that boxed it too for the sake of a
 *       uniform row would find no way back for that box and write null - a column that is there, has
 *       the right name, and holds nothing.</li>
 * </ul>
 *
 * <p>A 128-bit decimal is deliberately not among them. Its conversion loses digits on the way in and
 * that loss is accepted knowingly, so asserting it round-trips would be asserting something already
 * decided against.
 *
 * <p>Java rather than a declarative example, and the reason is a missing word rather than a
 * preference: the claim is about the storage type of a column at the target, and the specification
 * vocabulary can say what a document holds but not what type the store holds it as. Seeding is the
 * same gap from the other end - there is no way to write "an identity the driver generates" or "a
 * binary column" as a seed value.
 *
 * <p>Gated on Docker and on real connector jars. Run it with:
 *
 * <pre>
 *   mvn -pl e2e -am verify -Dapi.version=1.44 \
 *     -Dtapstate.e2e.connectors-dir=/path/to/connectors \
 *     -Dit.test=SinkValueRoundTripIT -Dfailsafe.failIfNoSpecifiedTests=false
 * </pre>
 */
class SinkValueRoundTripIT {

    private static final String SOURCE_ID = "src_mongo";
    private static final String PIPELINE_ID = "mongo2mongo";
    private static final String COLLECTION = "orders";
    private static final String SOURCE_DATABASE = "e2e_round_trip_src";
    private static final String TARGET_DATABASE = "e2e_round_trip_tgt";

    private static final byte[] BYTES = {1, 2, 3, 4};
    private static final int QUANTITY = 42;

    @BeforeAll
    static void requireDockerAndTheRealConnector() {
        DockerGate.require();
        RealConnectorGate.require("mongodb");
    }

    @Test
    void anIdentityABinaryColumnAndAnIntegerKeepTheirTypesAcrossTheChain() {
        String storeUri = SharedMongo.replicaSetUrl("e2e_round_trip_store");
        String sourceUri = SharedMongo.replicaSetUrl(SOURCE_DATABASE);
        String targetUri = SharedMongo.replicaSetUrl(TARGET_DATABASE);
        EndpointAddress source = EndpointAddress.uri(sourceUri);
        EndpointAddress target = EndpointAddress.uri(targetUri);

        try (ServerHandle server = InProcessServer.start(storeUri);
                MongoEndpoints mongo = new MongoEndpoints()) {
            // No identity of its own, so the driver assigns one - the ordinary case, and the only one
            // that exercises the way back at all.
            Document seeded = new Document()
                    .append("bin", new Binary(BYTES))
                    .append("qty", QUANTITY);
            mongo.insert(source, COLLECTION, seeded);
            ObjectId identity = seeded.getObjectId("_id");

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

            Await.until("the seeded document to reach the target",
                    () -> !mongo.documents(target, COLLECTION).isEmpty(),
                    () -> String.valueOf(mongo.documents(target, COLLECTION)));

            // Read back with the driver rather than through the product: what is under test is what the
            // store holds, and reading it through the same code that wrote it would agree with itself.
            List<Document> written = mongo.documents(target, COLLECTION);
            assertThat(written).as("the documents the target holds").hasSize(1);
            Document arrived = written.getFirst();

            assertThat(arrived.get("_id"))
                    .as("the identity at the target, which is text unless the write side restored it")
                    .isInstanceOf(ObjectId.class)
                    .isEqualTo(identity);
            assertThat(arrived.get("bin"))
                    .as("the binary column at the target")
                    .isInstanceOf(Binary.class);
            assertThat(((Binary) arrived.get("bin")).getData())
                    .as("the bytes of the binary column, which no rendering of it would preserve")
                    .containsExactly(BYTES);
            assertThat(arrived.get("qty"))
                    .as("the ordinary integer column, which must have stayed on the bare lane")
                    // 64-bit rather than the 32-bit it was seeded as, and that is the value model
                    // working: the type namespace names one integer width, so the boundary that
                    // resolves a column's type delivers a value of that width. What this asserts is
                    // the half that would break: an implementation that boxed an ordinary value the
                    // way it boxes a driver type would find no way back for that box and write null.
                    .isEqualTo((long) QUANTITY)
                    .isNotInstanceOf(String.class);
        }
    }

    private static String sourceYaml(String uri) {
        return """
                version: tapstate/v1
                kind: source
                id: src_mongo
                connector: mongodb
                config: { uri: "%s", database: %s }
                mode: cdc
                tables: [ %s ]
                """
                .formatted(uri, SOURCE_DATABASE, COLLECTION);
    }

    private static String targetYaml(String uri) {
        return """
                version: tapstate/v1
                kind: source
                id: tgt_mongo
                connector: mongodb
                config: { uri: "%s", database: %s }
                """
                .formatted(uri, TARGET_DATABASE);
    }

    private static String pipelineYaml() {
        return """
                version: tapstate/v1
                kind: pipeline
                id: %s
                source: %s
                settings: { read_mode: snapshot_and_cdc }
                transforms:
                  - { id: rows_through, from: [ %s ], type: filter, expr: "op == 'r' || op == 'i'" }
                serve:
                  from: rows_through
                  sync:
                    - source: tgt_mongo
                """
                .formatted(PIPELINE_ID, SOURCE_ID, COLLECTION);
    }
}
