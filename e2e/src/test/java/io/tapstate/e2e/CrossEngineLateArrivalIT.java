package io.tapstate.e2e;

import io.tapstate.core.lifecycle.LifecycleVerb;
import io.tapstate.core.lifecycle.PipelineState;
import io.tapstate.testsupport.DockerGate;

import org.bson.Document;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

import java.sql.Connection;
import java.sql.Statement;
import java.time.Duration;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * One engine's stream running far behind the other's still assembles the right object.
 *
 * <p>The two halves of the object come from two databases that cannot see each other, so nothing
 * synchronises them: one stream can be minutes ahead of the other, and which one leads is not the
 * product's to choose. What has to hold is that the object ends up the same either way. This drives the
 * lag deliberately - wide enough that the leading stream has not merely got there first but has already
 * had its documents written out - and does it in both directions, because they fail differently:
 *
 * <ul>
 *   <li><b>Children after the root.</b> The roots are assembled and written before a single child row
 *       exists. An implementation that treats a document as finished once it has gone out leaves the
 *       arrays empty forever, and every other reading stays healthy while it does.
 *   <li><b>Children before the root.</b> Child rows arrive naming a root that does not exist yet. An
 *       implementation that attaches a child to whatever root is present drops these on the floor -
 *       there is nothing to attach them to at the moment they arrive - and the root, when it does come,
 *       is written with an empty array that nothing ever fills.
 * </ul>
 *
 * <p><b>Those children are seeded before the run rather than written during it</b>, so that the snapshot
 * reads them while the root they name still does not exist. Writing them during the run and then writing
 * the root orders the two <em>writes</em>, which says nothing about the order the two engines' streams
 * reach the assembly: the engines are independent, and measured here, the root won that race and the
 * case went on passing against an implementation that drops what preceded a root. Ordering by
 * construction is what makes this half discriminate; the alternative was a fixed pause guessing at how
 * long a read takes, which is the settle this module keeps a gate against.
 *
 * <p><b>What the assertions have to discriminate.</b> Counting documents sees neither failure: the roots
 * arrive whether or not their children did, so a count is satisfied by an implementation that honoured
 * only the leading stream. The arrays are therefore read by length, and the lengths are deliberately
 * different from one another - two, one, three - so that a constant, and an implementation that hangs
 * every child on the first root it saw, each satisfy one root and fail the others. The root's own column
 * is read back beside the array, because a document whose array is right and whose scalar fields were
 * dropped is one nobody can use and no assertion about the array would notice.
 *
 * <p><b>The value carried by the lagging stream is asserted on the field it changed</b>, not on a count.
 * A change that arrives after its document was written has to reach that document's contents; an
 * implementation that counts it and does not apply it leaves the length right and the value stale, which
 * is the one failure a size assertion cannot see. So the carrier is read back, and the value it replaced
 * is asserted absent - present-and-also-still-there is what an append rather than an update looks like.
 *
 * <p>Gated on Docker and on a directory of real connector jars, like its siblings. Run it with:
 *
 * <pre>
 *   mvn -pl e2e -am verify -Dapi.version=1.44 \
 *     -Dtapstate.e2e.connectors-dir=/path/to/connectors \
 *     -Dit.test=CrossEngineLateArrivalIT -Dfailsafe.failIfNoSpecifiedTests=false
 * </pre>
 */
class CrossEngineLateArrivalIT {

    /**
     * Wider than the shared default, for the reason {@link Await} gives for taking a bound at all: this
     * drives two real engines through a snapshot and a change stream before the first reading is due.
     */
    private static final Duration BOUND = Duration.ofSeconds(180);

    private static final String ROOT_TABLE = "orders";
    private static final String CHILD_TABLE = "shipments";

    /** The root that receives its children only after its document has already been written out. */
    private static final int LATE_CHILDREN_ROOT = 1;

    /** The root whose children arrive before it does, and wait for it. */
    private static final int LATE_ROOT = 4;

    @BeforeAll
    static void requireDockerAndRealConnectors() {
        DockerGate.require();
        RealConnectorGate.require("mysql", "postgres", "mongodb");
    }

    @ParameterizedTest
    @EnumSource(Tiers.class)
    void aStreamArrivingLongAfterTheOtherStillReachesTheRightObject(Tiers tier) throws Exception {
        try (SharedPostgres.Fixture postgresFixture = SharedPostgres.fixture()) {
            String suffix = tier.name().toLowerCase(Locale.ROOT);
            String pipelineId = "cross_engine_late_" + suffix;

            Map<String, Object> orders = SharedMySql.settings("late_orders_" + suffix);
            Map<String, Object> shipments = postgresFixture.settings("late_shipments_" + suffix);
            createTables(orders, shipments);
            seedRoots(orders);
            // The children of a root that does not exist, seeded before the run so the snapshot reads them.
            // Ordering by construction rather than by waiting: these are read while the run starts, and the
            // root they name is not written until several assertions later. Inserting them at that later
            // point instead would only order the two writes, which says nothing about the order the two
            // engines' streams reach the assembly - measured: with the writes ordered and nothing else, the
            // root wins the race and the case stops discriminating at all.
            insertShipments(shipments, List.of(shipment(7, LATE_ROOT, "dhl"), shipment(8, LATE_ROOT, "ups")));

            String storeUri = SharedMongo.replicaSetUrl("late_store_" + suffix);
            String targetUri = SharedMongo.replicaSetUrl("late_target_" + suffix);

            try (ServerHandle server = postgresFixture.launch(() -> tier.launch(storeUri));
                    MongoEndpoints mongo = new MongoEndpoints()) {
                ControlPlane control = new ControlPlane(server.baseUrl());
                control.bootstrapAndLogin("e2e", "e2e-password");

                control.registerConnector("mysql", ConnectorJars.bytesFor("mysql"));
                control.registerConnector("postgres", ConnectorJars.bytesFor("postgres"));
                control.registerConnector("mongodb", ConnectorJars.bytesFor("mongodb"));

                Map<String, String> resources = new LinkedHashMap<>();
                resources.put("src_orders.tap.yml", mysqlSourceYaml(orders));
                resources.put("src_shipments.tap.yml", postgresSourceYaml(shipments));
                resources.put("tgt_mongo.tap.yml", targetYaml(targetUri));
                resources.put("pipeline.tap.yml", pipelineYaml(pipelineId));
                control.apply(resources);

                control.discoverSchema("src_orders", "mysql", orders);
                control.discoverSchema("src_shipments", "postgres", postgresDiscoveryConfig(shipments));

                control.lifecycle(pipelineId, LifecycleVerb.START);

                // The leading stream runs to completion and its documents are written out, with the other
                // engine's table still empty. Waiting for the scalar column - not merely for three documents
                // - is what makes the rest of this a statement about a document that already exists.
                // Three, not four: the children seeded for the fourth root have been read by now, and a
                // document is not written for a key whose root row has never arrived.
                Await.until("the three roots to be assembled and written", BOUND,
                        () -> documentsIn(mongo, targetUri).size() == 3,
                        () -> String.valueOf(documentsIn(mongo, targetUri)));
                assertThat(scalarOf(mongo, targetUri, LATE_CHILDREN_ROOT, "customer"))
                        .as("the roots have to be assembled and written before any child exists, or the lag "
                                + "this drives is not a lag at all")
                        .isEqualTo("alice");
                assertThat(arrayOf(mongo, targetUri, LATE_CHILDREN_ROOT))
                        .as("no child row has been written yet, so nothing can be hanging under a root")
                        .isEmpty();

                // Direction one: the children of roots that were written out long ago.
                insertShipments(shipments, List.of(
                        shipment(1, 1, "dhl"),
                        shipment(2, 1, "ups"),
                        shipment(3, 2, "fedex"),
                        shipment(4, 3, "dhl"),
                        shipment(5, 3, "ups"),
                        shipment(6, 3, "fedex")));

                Await.until("the late children to reach the documents already written", BOUND,
                        () -> sizeOf(mongo, targetUri, 1) == 2
                                && sizeOf(mongo, targetUri, 2) == 1
                                && sizeOf(mongo, targetUri, 3) == 3,
                        () -> String.valueOf(documentsIn(mongo, targetUri)));

                assertThat(List.of(sizeOf(mongo, targetUri, 1), sizeOf(mongo, targetUri, 2),
                                sizeOf(mongo, targetUri, 3)))
                        .as("children reaching a document that was written out before they existed. The three "
                                + "lengths differ so that a constant, and an implementation hanging every child "
                                + "on the first root, each fail at least one of them.%n  documents: %s",
                                documentsIn(mongo, targetUri))
                        .containsExactly(2, 1, 3);

                assertThat(scalarOf(mongo, targetUri, LATE_CHILDREN_ROOT, "customer"))
                        .as("the root's own column, read back beside the array: a document whose array is "
                                + "right and whose scalars were dropped on the way through is unusable, and "
                                + "no assertion about the array notices it")
                        .isEqualTo("alice");

                // Direction two: the root of the children that were read at the start of the run.
                insertRoot(orders, LATE_ROOT, "dave");

                Await.until("the children that arrived before their root to appear under it", BOUND,
                        () -> sizeOf(mongo, targetUri, LATE_ROOT) == 2,
                        () -> String.valueOf(documentsIn(mongo, targetUri)));
                assertThat(sizeOf(mongo, targetUri, LATE_ROOT))
                        .as("children that arrived before their root have to be waiting for it, not dropped. "
                                + "An implementation attaching a child to whichever root is present has "
                                + "nothing to attach these to at the moment they arrive.%n  documents: %s",
                                documentsIn(mongo, targetUri))
                        .isEqualTo(2);
                assertThat(scalarOf(mongo, targetUri, LATE_ROOT, "customer")).isEqualTo("dave");

                // A value carried by the lagging stream, asserted on the field it changed.
                updateShipmentCarrier(shipments, 1, "maersk");

                Await.until("the changed carrier to reach the document", BOUND,
                        () -> carriersOf(mongo, targetUri, LATE_CHILDREN_ROOT).contains("maersk"),
                        () -> String.valueOf(documentsIn(mongo, targetUri)));
                assertThat(carriersOf(mongo, targetUri, LATE_CHILDREN_ROOT))
                        .as("the changed field itself, in the document the change had to reach. The size stays "
                                + "two either way, so a length assertion cannot see this one; and the value it "
                                + "replaced has to be gone, since still-there-as-well is what an append rather "
                                + "than an update looks like.%n  documents: %s", documentsIn(mongo, targetUri))
                        .containsExactlyInAnyOrder("maersk", "ups");

                // Read after the assertions, so a run that died on the way cannot satisfy them by having
                // stopped for the wrong reason.
                assertThat(control.state(pipelineId))
                        .as("the run has to be alive for any of the readings above to mean anything")
                        .contains(PipelineState.RUNNING);
                assertThat(control.errorCount(pipelineId)).contains(0L);
            }
        }
    }

    // ---- readings -----------------------------------------------------------------------

    private static List<Document> documentsIn(MongoEndpoints mongo, String targetUri) {
        return mongo.documents(EndpointAddress.uri(targetUri), ROOT_TABLE);
    }

    private static Document documentOf(MongoEndpoints mongo, String targetUri, int id) {
        for (Document document : documentsIn(mongo, targetUri)) {
            if (String.valueOf(document.get("id")).equals(String.valueOf(id))) {
                return document;
            }
        }
        return null;
    }

    private static String scalarOf(MongoEndpoints mongo, String targetUri, int id, String field) {
        Document document = documentOf(mongo, targetUri, id);
        return document == null ? null : String.valueOf(document.get(field));
    }

    @SuppressWarnings("unchecked")
    private static List<Document> arrayOf(MongoEndpoints mongo, String targetUri, int id) {
        Document document = documentOf(mongo, targetUri, id);
        if (document == null) {
            return List.of();
        }
        Object array = document.get(CHILD_TABLE);
        return array instanceof List<?> list ? (List<Document>) list : List.of();
    }

    private static int sizeOf(MongoEndpoints mongo, String targetUri, int id) {
        return arrayOf(mongo, targetUri, id).size();
    }

    private static List<String> carriersOf(MongoEndpoints mongo, String targetUri, int id) {
        List<String> carriers = new ArrayList<>();
        for (Document element : arrayOf(mongo, targetUri, id)) {
            carriers.add(String.valueOf(element.get("carrier")));
        }
        return carriers;
    }

    // ---- fixtures -----------------------------------------------------------------------

    private record Shipment(int id, int orderId, String carrier) {
    }

    private static Shipment shipment(int id, int orderId, String carrier) {
        return new Shipment(id, orderId, carrier);
    }

    private static void createTables(Map<String, Object> orders, Map<String, Object> shipments)
            throws Exception {
        try (Connection connection = SharedMySql.connect(orders);
                Statement statement = connection.createStatement()) {
            statement.execute("DROP TABLE IF EXISTS " + ROOT_TABLE);
            statement.execute(
                    "CREATE TABLE " + ROOT_TABLE + " (id INT PRIMARY KEY, customer VARCHAR(64))");
        }
        try (Connection connection = SharedPostgres.connect(shipments);
                Statement statement = connection.createStatement()) {
            statement.execute("DROP TABLE IF EXISTS " + CHILD_TABLE);
            statement.execute("CREATE TABLE " + CHILD_TABLE
                    + " (id INT PRIMARY KEY, order_id INT, carrier VARCHAR(64))");
            // The whole previous row on an update, so a change to a child can be placed by what it was
            // as well as by what it became. PostgreSQL sends the key alone unless told otherwise.
            statement.execute("ALTER TABLE " + CHILD_TABLE + " REPLICA IDENTITY FULL");
        }
    }

    private static void seedRoots(Map<String, Object> orders) throws Exception {
        try (Connection connection = SharedMySql.connect(orders);
                Statement statement = connection.createStatement()) {
            statement.execute("INSERT INTO " + ROOT_TABLE + " (id, customer) VALUES "
                    + "(1, 'alice'), (2, 'bob'), (3, 'carol')");
        }
    }

    private static void insertRoot(Map<String, Object> orders, int id, String customer)
            throws Exception {
        try (Connection connection = SharedMySql.connect(orders);
                Statement statement = connection.createStatement()) {
            statement.execute("INSERT INTO " + ROOT_TABLE + " (id, customer) VALUES ("
                    + id + ", '" + customer + "')");
        }
    }

    private static void insertShipments(Map<String, Object> shipments, List<Shipment> rows)
            throws Exception {
        try (Connection connection = SharedPostgres.connect(shipments);
                Statement statement = connection.createStatement()) {
            for (Shipment row : rows) {
                statement.execute("INSERT INTO " + CHILD_TABLE + " (id, order_id, carrier) VALUES ("
                        + row.id() + ", " + row.orderId() + ", '" + row.carrier() + "')");
            }
        }
    }

    private static void updateShipmentCarrier(Map<String, Object> shipments, int id, String carrier)
            throws Exception {
        try (Connection connection = SharedPostgres.connect(shipments);
                Statement statement = connection.createStatement()) {
            statement.execute("UPDATE " + CHILD_TABLE + " SET carrier = '" + carrier + "' WHERE id = " + id);
        }
    }

    /**
     * The settings a discovery call needs, spelled the way this connector spells them.
     *
     * <p>Two differences from what the shared server hands back, and both are the connector's own naming
     * rather than a choice made here: the account is "user" where MySQL says "username", and a table is
     * addressed by schema as well as by database. The harness publishes the account under one uniform
     * name across every store, which is what makes a fixture readable; a discovery call is given the
     * connector's settings instead, so the translation happens here. Passing the uniform spelling
     * through reaches the connector as no account at all, and it falls back to the operating system
     * user - which fails as a password error naming a user nobody configured.
     */
    private static Map<String, Object> postgresDiscoveryConfig(Map<String, Object> settings) {
        Map<String, Object> config = new LinkedHashMap<>();
        config.put("host", settings.get("host"));
        config.put("port", settings.get("port"));
        config.put("database", settings.get("database"));
        config.put("schema", "public");
        config.put("user", settings.get("username"));
        config.put("password", settings.get("password"));
        return config;
    }
    // ---- resources ----------------------------------------------------------------------

    private static String mysqlSourceYaml(Map<String, Object> settings) {
        return """
                version: tapstate/v1
                kind: source
                id: src_orders
                connector: mysql
                config: { host: %s, port: %s, database: %s, username: %s, password: %s }
                mode: cdc
                tables: [ %s ]
                """
                .formatted(settings.get("host"), settings.get("port"), settings.get("database"),
                        settings.get("username"), settings.get("password"), ROOT_TABLE);
    }

    private static String postgresSourceYaml(Map<String, Object> settings) {
        // The account is "user" where MySQL says "username", and a table is addressed by schema as well
        // as by database - both this connector's own spelling rather than a choice made here.
        return """
                version: tapstate/v1
                kind: source
                id: src_shipments
                connector: postgres
                config: { host: %s, port: %s, database: %s, schema: public, user: %s, password: %s }
                mode: cdc
                tables: [ %s ]
                """
                .formatted(settings.get("host"), settings.get("port"), settings.get("database"),
                        settings.get("username"), settings.get("password"), CHILD_TABLE);
    }

    private static String targetYaml(String targetUri) {
        return """
                version: tapstate/v1
                kind: source
                id: tgt_mongo
                connector: mongodb
                config: { uri: "%s" }
                """
                .formatted(targetUri);
    }

    private static String pipelineYaml(String pipelineId) {
        return """
                version: tapstate/v1
                kind: pipeline
                id: %s
                source: [ src_orders, src_shipments ]
                settings: { read_mode: snapshot_and_cdc }
                transforms:
                  - id: order_doc
                    type: nest
                    from: { o: %s, s: %s }
                    root:
                      from: o
                      key: [ id ]
                      embed:
                        - { from: s, on: { order_id: id }, as: array, path: %s, arrayKey: [ id ] }
                serve:
                  from: order_doc
                  sync:
                    - source: tgt_mongo
                """
                .formatted(pipelineId, ROOT_TABLE, CHILD_TABLE, CHILD_TABLE);
    }

}
