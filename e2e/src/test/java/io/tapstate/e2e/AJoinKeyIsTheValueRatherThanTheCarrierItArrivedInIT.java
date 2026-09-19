package io.tapstate.e2e;

import io.tapstate.core.lifecycle.LifecycleVerb;
import io.tapstate.testsupport.DockerGate;

import org.bson.Document;
import org.bson.types.ObjectId;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.util.LinkedHashMap;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * A join matches a dimension row whose key value the source connector converted against a fact row that
 * names the same identifier as plain text.
 *
 * <p>It did not. A value the connector's own registered conversion produced travels inside a carrier
 * holding the converted value and the name the source's schema gives it, and both places a join builds a
 * match key read the value straight off the row. A carrier never equals the plain value inside it, so the
 * two sides keyed on different names and never met. Nothing reported it: the job ran, every fact row was
 * published, and the joined columns stayed empty - which is indistinguishable from a dimension row that
 * is genuinely not there.
 *
 * <p><b>Why a real connector rather than a synthetic one.</b> A carrier exists only for a value one of
 * the connector's own registered conversions produced, so the pairing needs a seeded value of the source
 * driver's own type. A document store's key is that type here, and it is the one this repository already
 * seeds directly in {@code AnObjectIdReadsBackTheSameThroughBothFacesIT}. A stand-in type would only
 * disagree in the way it was built to.
 *
 * <p><b>Why Java rather than a published example.</b> {@code seed:} lays down YAML scalars, and the
 * specification vocabulary has no word for "seed this column a value of the driver's own type", nor one
 * for "and name that same value, as text, from the other side of the join". That is the missing word.
 *
 * <p>What each reading has to discriminate:
 *
 * <ul>
 *   <li><b>Three rows reach the target.</b> The statement's join is outer, so every fact row is published
 *       whether or not it found a customer. Without this, a run that never started and a run whose join
 *       matched nothing read the same, and every reading below would be about a row that is not there.
 *   <li><b>Order 10 carries its customer's name.</b> This is the whole defect: the dimension row is keyed
 *       by the driver's own type and the fact row names it as text, and before the repair this column was
 *       empty with no error anywhere.
 *   <li><b>Order 11 carries a different customer's name.</b> A second matched row, on a different key, so
 *       that one lucky collision cannot pass for matching.
 *   <li><b>Order 12 carries no name at all.</b> Its text names a key no customer holds. An implementation
 *       that dropped the key and joined everything to everything, or that filled the column from whatever
 *       dimension row it saw last, satisfies both readings above and fails this one.
 * </ul>
 *
 * <p><b>Which of the two sites this case holds.</b> A join reads a key at two boundaries: the one a
 * change is matched by, and the one the edge into the join vertex routes by. This run has a single
 * member, which owns every partition, so a key derived two ways still meets itself and the routing
 * boundary cannot be discriminated here. What this case holds is the matching boundary, which is the
 * one an operator sees: reverting it alone leaves all three rows published with the joined column
 * empty, and this case times out on order 10 reading null. The routing boundary is held by the
 * engine-level regression, which asserts the name the edge routes by directly.
 *
 * <p>Gated on Docker and on real connector jars. Run it with:
 *
 * <pre>
 *   mvn -pl e2e -am verify -Dapi.version=1.44 \
 *     -Dtapstate.e2e.connectors-dir=/path/to/connectors \
 *     -Dit.test=AJoinKeyIsTheValueRatherThanTheCarrierItArrivedInIT \
 *     -Dfailsafe.failIfNoSpecifiedTests=false
 * </pre>
 */
class AJoinKeyIsTheValueRatherThanTheCarrierItArrivedInIT {

    private static final String SOURCE_ID = "shop_db";
    private static final String DATABASE = "join_carrier_db";
    private static final String PIPELINE_ID = "order_pipeline";
    private static final String TARGET = "order_state";

    private static final String ORDERS = "orders";
    private static final String CUSTOMERS = "customers";

    @BeforeAll
    static void requireDockerAndTheRealConnector() {
        DockerGate.require();
        RealConnectorGate.require("mongodb");
    }

    @Test
    void aDimensionKeyedByTheDriversOwnTypeMatchesTheFactThatNamesItAsText() {
        String storeUri = SharedMongo.replicaSetUrl("join_carrier_store");
        String dataUri = SharedMongo.replicaSetUrl(DATABASE);
        String targetUri = SharedMongo.replicaSetUrl("join_carrier_target");
        EndpointAddress data = EndpointAddress.uri(dataUri);
        EndpointAddress target = EndpointAddress.uri(targetUri);

        // The driver's own key type, generated the way a collection ordinarily holds one. The fact side
        // names the very same identifier as the text everybody knows it by, which is the pairing.
        ObjectId ada = new ObjectId();
        ObjectId bo = new ObjectId();
        ObjectId nobody = new ObjectId();

        try (MongoEndpoints mongo = new MongoEndpoints();
                ServerHandle server = Tiers.IN_PROCESS.launch(storeUri)) {
            mongo.insert(data, CUSTOMERS, new Document("customer_key", ada).append("name", "ada"));
            mongo.insert(data, CUSTOMERS, new Document("customer_key", bo).append("name", "bo"));
            mongo.insert(data, ORDERS,
                    new Document("_id", 10).append("customer_ref", ada.toHexString()));
            mongo.insert(data, ORDERS,
                    new Document("_id", 11).append("customer_ref", bo.toHexString()));
            // Names a key no customer holds, so it must stay unmatched however the others go.
            mongo.insert(data, ORDERS,
                    new Document("_id", 12).append("customer_ref", nobody.toHexString()));

            ControlPlane control = new ControlPlane(server.baseUrl());
            control.bootstrapAndLogin("e2e", "e2e-password");
            control.registerConnector("mongodb", ConnectorJars.bytesFor("mongodb"));

            Map<String, String> resources = new LinkedHashMap<>();
            resources.put("shop_db.tap.yml", sourceYaml(dataUri));
            resources.put("views.tap.yml", targetYaml(targetUri));
            resources.put("join_pipeline.tap.yml", pipelineYaml());
            control.apply(resources);
            control.discoverSchema(SOURCE_ID, "mongodb", Map.of("uri", dataUri));
            control.lifecycle(PIPELINE_ID, LifecycleVerb.START);

            Await.until("every fact row to reach the target",
                    () -> mongo.count(target, TARGET) == 3,
                    () -> mongo.count(target, TARGET) + " rows");

            awaitName(mongo, target, 10, "ada",
                    "a dimension row keyed by the driver's own type to be joined in");
            assertThat(nameOf(mongo, target, 11))
                    .as("a second fact row, on a different key, so one collision cannot pass for matching")
                    .isEqualTo("bo");
            assertThat(nameOf(mongo, target, 12))
                    .as("a fact row naming a key no customer holds keeps its joined column empty")
                    .isNull();
        }
    }

    private static void awaitName(
            MongoEndpoints mongo, EndpointAddress target, int orderId, String expected, String what) {
        Await.until(what, () -> expected.equals(nameOf(mongo, target, orderId)),
                () -> String.valueOf(nameOf(mongo, target, orderId)));
    }

    /** One published row's joined customer name, or null before the row is there or with nothing joined. */
    private static String nameOf(MongoEndpoints mongo, EndpointAddress target, int orderId) {
        return mongo.fetch(target, TARGET, Map.of("order_id", orderId))
                .map(document -> document.get("customer_name"))
                .map(String::valueOf)
                .orElse(null);
    }

    private static String sourceYaml(String dataUri) {
        return """
                version: tapstate/v1
                kind: source
                id: shop_db
                connector: mongodb
                config: { uri: "%s" }
                mode: cdc
                tables: [ orders, customers ]
                """
                .formatted(dataUri);
    }

    private static String targetYaml(String targetUri) {
        return """
                version: tapstate/v1
                kind: source
                id: views
                connector: mongodb
                config: { uri: "%s" }
                """
                .formatted(targetUri);
    }

    /** One join step, one dimension, outer so that an unmatched fact row is published rather than dropped. */
    private static String pipelineYaml() {
        return """
                version: tapstate/v1
                kind: pipeline
                id: order_pipeline
                source: shop_db
                settings: { read_mode: snapshot_and_cdc }
                transforms:
                  - id: widen
                    type: join
                    from: { o: orders, c: customers }
                    engine: builtin
                    sql: |
                      SELECT o._id AS order_id, c.name AS customer_name
                      FROM o LEFT JOIN c ON o.customer_ref = c.customer_key
                view:
                  id: order_state
                  from: widen
                  primary_key: order_id
                """;
    }
}
