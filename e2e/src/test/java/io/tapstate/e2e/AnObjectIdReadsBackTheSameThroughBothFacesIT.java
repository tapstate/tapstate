package io.tapstate.e2e;

import io.tapstate.testsupport.DockerGate;
import org.bson.Document;
import org.bson.types.ObjectId;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * One document, read through both faces of the same source, reads as one document.
 *
 * <p>It did not. The follow face rendered a key as the hexadecimal string everybody knows it by; the
 * browse face copied the driver's own object through untouched, and whatever serialized the answer
 * above turned it into a two-field object. Nothing was lost on either side and neither face failed -
 * they disagreed, so the same row looked like two rows depending on which face was opened. A reader
 * comparing them, or an agent keying off the value, has no way to tell that from two rows.
 *
 * <p>Why a real connector rather than a synthetic one: the shape of the disagreement is a property of
 * a driver type nobody wrote for this, and a stand-in type would only ever disagree in the way it was
 * built to. The identity here is one the driver generates - a document seeded without one, which is
 * how a collection is ordinarily written - because a string identity is exactly the case that misses
 * this: an earlier probe used one and both faces agreed, which is what let the disagreement stand.
 *
 * <p>The discovery assertion rides along on the same fixture and is not decoration. Discovery on this
 * connector ran into a mapping the host never handed it, threw inside the field walk, and was caught
 * somewhere above - so the read carried on and the schema came back holding the one field the walk
 * had reached before it threw. In the log that reads as a stack nobody owns; in the product it is a
 * source that reports one column out of sixteen. A count is the assertion because the failure is
 * positional: the collection is seeded with more fields than the walk reached, so a run that still
 * threw stops short of the count while one that never resolved anything stops at zero.
 *
 * <p>Gated on Docker and on real connector jars. Run it with:
 *
 * <pre>
 *   mvn -pl e2e -am verify -Dapi.version=1.44 \
 *     -Dtapstate.e2e.connectors-dir=/path/to/connectors \
 *     -Dit.test=AnObjectIdReadsBackTheSameThroughBothFacesIT -Dfailsafe.failIfNoSpecifiedTests=false
 * </pre>
 */
class AnObjectIdReadsBackTheSameThroughBothFacesIT {

    private static final String SOURCE_ID = "src_mongo";
    private static final String COLLECTION = "orders";
    private static final String DATABASE = "e2e_both_faces";

    private static final Duration TIMEOUT = Duration.ofSeconds(30);

    /** The columns seeded beside the identity, enough of them that the field walk cannot reach the end early. */
    private static final int COLUMNS = 15;

    @BeforeAll
    static void requireDockerAndTheRealConnector() {
        DockerGate.require();
        RealConnectorGate.require("mongodb");
    }

    @Test
    void theSameDocumentCarriesTheSameIdentityOnTheBrowseFaceAndTheFollowFace() {
        String storeUri = SharedMongo.replicaSetUrl("e2e_both_faces_store");
        String dataUri = SharedMongo.replicaSetUrl(DATABASE);
        EndpointAddress data = EndpointAddress.uri(dataUri);

        try (ServerHandle server = InProcessServer.start(storeUri);
                MongoEndpoints mongo = new MongoEndpoints()) {
            // Seeded with no identity of its own, so the driver assigns one - which is what an ordinary
            // collection holds and what neither face could agree on.
            Document before = columns();
            mongo.insert(data, COLLECTION, before);
            ObjectId seededId = before.getObjectId("_id");
            assertThat(seededId).as("the driver's own identity, which is the value under test").isNotNull();

            ControlPlane control = new ControlPlane(server.baseUrl());
            control.bootstrapAndLogin("e2e", "e2e-password");
            control.registerConnector("mongodb", ConnectorJars.bytesFor("mongodb"));
            control.apply(Map.of("src_mongo.tap.yml", sourceYaml(dataUri)));
            control.discoverSchema(SOURCE_ID, "mongodb", Map.of("uri", dataUri, "database", DATABASE));

            // (1) Every column the collection holds is reported, not the one the walk reached before it
            // threw. Asserted as a count over the seeded names rather than "not empty": one field came
            // back before this was fixed, and "not empty" was already true of that.
            assertThat(discoveredFields(control))
                    .as("the fields discovery reports for a collection holding %d columns and an identity",
                            COLUMNS)
                    .hasSize(COLUMNS + 1)
                    .contains("_id", "a01", "a15");

            // (2) Both faces, on one document. The follow is opened first and the document written after,
            // so the same write is what each face answers about - reading the seeded one on the browse
            // face and a different one on the follow face would compare two identities and pass on any
            // implementation that rendered both the same way.
            try (ControlPlane.Follow following = control.follow(SOURCE_ID, COLLECTION, null)) {
                Document written = columns();
                mongo.insert(data, COLLECTION, written);
                ObjectId writtenId = written.getObjectId("_id");

                String onTheFollowFace = lastIdentityOf(following.awaitFrame(
                        frame -> identityOf(frame) != null, TIMEOUT,
                        "the change the follow face carries for the document just written"));

                String onTheBrowseFace = browsedIdentity(control, writtenId);

                assertThat(onTheBrowseFace)
                        .as("the identity of one document, read through both faces of the same source")
                        .isEqualTo(onTheFollowFace)
                        // Held to the driver's own spelling as well, so that two faces agreeing on some
                        // third rendering - both handing back the object's own text, say - does not read
                        // as agreement on the value.
                        .isEqualTo(writtenId.toHexString());
            }
        }
    }

    /** The identity a follow frame carries, from whichever side of the change the store described. */
    private static String identityOf(Map<String, Object> frame) {
        Object row = frame.get("after") != null ? frame.get("after") : frame.get("before");
        return row instanceof Map<?, ?> fields && fields.get("_id") != null
                ? String.valueOf(fields.get("_id"))
                : null;
    }

    /** The identity the last frame delivered carries, which is the one the write under test produced. */
    private static String lastIdentityOf(List<Map<String, Object>> delivered) {
        String identity = null;
        for (Map<String, Object> frame : delivered) {
            String carried = identityOf(frame);
            if (carried != null) {
                identity = carried;
            }
        }
        return identity;
    }

    /** The identity the browse face answers with for the document just written. */
    @SuppressWarnings("unchecked")
    private static String browsedIdentity(ControlPlane control, ObjectId written) {
        Map<String, Object> answer = control.find(SOURCE_ID, COLLECTION, Map.of("limit", 100));
        assertThat(answer).containsKey("rows");
        for (Map<String, Object> row : (List<Map<String, Object>>) answer.get("rows")) {
            String identity = String.valueOf(row.get("_id"));
            if (written.toHexString().equals(identity)) {
                return identity;
            }
        }
        // Answering with what was actually there: a browse face rendering the identity some other way
        // has no matching row, and the message has to show the shape rather than say "not found".
        throw new AssertionError("the browse face carried no row identified as " + written.toHexString()
                + "; it answered with " + answer.get("rows"));
    }

    /** The field names discovery recorded for the seeded collection. */
    @SuppressWarnings("unchecked")
    private static List<String> discoveredFields(ControlPlane control) {
        for (Map<String, Object> collection : control.collections(SOURCE_ID)) {
            if (COLLECTION.equals(collection.get("name")) && collection.get("fields") instanceof List<?> fields) {
                return fields.stream()
                        .map(field -> field instanceof Map<?, ?> named
                                ? String.valueOf(((Map<String, Object>) named).get("name"))
                                : String.valueOf(field))
                        .toList();
            }
        }
        throw new AssertionError("discovery recorded no fields for " + COLLECTION
                + "; the listing was " + control.collections(SOURCE_ID));
    }

    /** A document of ordinary columns and no identity, so the driver assigns one. */
    private static Document columns() {
        Document document = new Document();
        for (int column = 1; column <= COLUMNS; column++) {
            document.append(String.format("a%02d", column), column);
        }
        return document;
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
