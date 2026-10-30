package io.tapstate.e2e;

import io.tapstate.core.lifecycle.LifecycleVerb;
import io.tapstate.core.lifecycle.PipelineState;
import io.tapstate.testsupport.DockerGate;

import org.bson.Document;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;
import org.testcontainers.containers.MySQLContainer;
import org.testcontainers.utility.DockerImageName;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.Statement;
import java.time.Duration;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.function.BooleanSupplier;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * A row arriving under a middle-level parent that has been deleted is counted as unplaceable, not
 * dropped - and the record of that deletion is what makes the difference.
 *
 * <p>Deleting a row in the middle of a tree ends a mapping that everything below it was routed by. What
 * the level does with that mapping decides what happens to the rows still coming: keep a record that the
 * key is gone and a later child can be recognised as belonging nowhere; remove the mapping outright and
 * that same child looks exactly like one whose parent has not arrived yet, so it is held - forever, since
 * nothing will ever declare that key again - and the position it came in at is held with it.
 *
 * <p><b>Why the target says nothing here.</b> The child was never going to appear in any document: its
 * parent is gone, so a run that discarded it, a run still holding it, and a run that never saw it all
 * leave byte-identical documents behind. Every assertion about the target is satisfied by all three. That
 * is exactly what the count of what a nest could not place exists for, and it is the reading this rests
 * on. A run holding the row reports zero.
 *
 * <p>The other half is that the run is still alive while that reading is taken. Ending the job would
 * satisfy a count of one and destroy the property: a source deleting a row whose children are still in
 * flight is ordinary, and the whole point is that ordinary data does not stop a pipeline. So the state and
 * the error count are read in the same breath, and the surviving document is read too - the deletion has
 * to have taken the middle row out of it without taking anything else.
 *
 * <p>Gated on Docker and on a directory of real connector jars, like its siblings. Run it with:
 *
 * <pre>
 *   mvn -pl e2e -am verify -Dapi.version=1.44 \
 *     -Dtapstate.e2e.connectors-dir=/path/to/connectors \
 *     -Dit.test=NestMiddleRowDeleteDoesNotDropWaitingIT -Dfailsafe.failIfNoSpecifiedTests=false
 * </pre>
 */
class NestMiddleRowDeleteDoesNotDropWaitingIT {

    private static final Duration TIMEOUT = Duration.ofSeconds(180);
    private static final Duration POLL = Duration.ofMillis(250);
    private static final String ROOT_TABLE = "customers";
    private static final String MIDDLE_TABLE = "orders";
    private static final String LEAF_TABLE = "order_items";
    private static final String PIPELINE_ID = "orphaned_middle";

    /**
     * This invocation's pipeline id, which carries the tier so the two tiers do not share a nest's state,
     * and a base of its own so no two witnesses share one either.
     */
    private String pipelineId;

    private static final String MIDDLE_PATH = "orders";
    private static final String LEAF_PATH = "items";

    private static final long ROOT_ID = 1;
    private static final long DELETED_MIDDLE = 10;
    private static final long SURVIVING_MIDDLE = 11;
    private static final long LATE_LEAF = 100;

    @BeforeAll
    static void requireDockerAndRealConnectors() {
        DockerGate.require();
        RealConnectorGate.require("mysql", "mongodb");
    }

    @ParameterizedTest
    @EnumSource(Tiers.class)
    void aChildArrivingUnderADeletedMiddleRowIsCountedRatherThanHeld(Tiers tier) throws Exception {
        try (MySQLContainer<?> mysql = new MySQLContainer<>(DockerImageName.parse("mysql:8.0"))) {
            mysql.start();
            seedMysql(mysql);
            grantReplication(mysql);

            String suffix = tier.name().toLowerCase(Locale.ROOT);
            pipelineId = PIPELINE_ID + "_" + suffix;
            String targetUri = SharedMongo.replicaSetUrl("middle_target_" + suffix);

            try (PreparedNativeStore storage = PreparedNativeStore.create(); ServerHandle server = tier.launch(storage);
                    MongoEndpoints mongo = new MongoEndpoints()) {
                ControlPlane control = new ControlPlane(server.baseUrl());
                storage.loginAndAwaitBusiness(control);

                control.registerConnector("mysql", ConnectorJars.bytesFor("mysql"));
                control.registerConnector("mongodb", ConnectorJars.bytesFor("mongodb"));

                Map<String, Object> mysqlConfig = mysqlConfig(mysql);
                Map<String, String> resources = new LinkedHashMap<>();
                resources.put("src_customers.tap.yml", sourceYaml("src_customers", ROOT_TABLE, mysqlConfig));
                resources.put("src_orders.tap.yml", sourceYaml("src_orders", MIDDLE_TABLE, mysqlConfig));
                resources.put("src_items.tap.yml", sourceYaml("src_items", LEAF_TABLE, mysqlConfig));
                resources.put("tgt_mongo.tap.yml", targetYaml(targetUri));
                resources.put("pipeline.tap.yml", pipelineYaml(pipelineId));
                control.apply(resources);

                control.discoverSchema("src_customers", "mysql", mysqlConfig);
                control.discoverSchema("src_orders", "mysql", mysqlConfig);
                control.discoverSchema("src_items", "mysql", mysqlConfig);

                control.lifecycle(pipelineId, LifecycleVerb.START);

                // Both middle rows have to be in the document before one of them is deleted, or the
                // deletion would be witnessing a row that had never arrived.
                await(() -> middleIds(mongo.documents(EndpointAddress.uri(targetUri), ROOT_TABLE))
                        .equals(List.of(DELETED_MIDDLE, SURVIVING_MIDDLE)));
                if (!middleIds(mongo.documents(EndpointAddress.uri(targetUri), ROOT_TABLE))
                        .equals(List.of(DELETED_MIDDLE, SURVIVING_MIDDLE))) {
                    throw new AssertionError(diagnose(control, mongo, targetUri,
                            "the middle rows never assembled under their root"));
                }

                deleteMiddleRow(mysql);
                await(() -> middleIds(mongo.documents(EndpointAddress.uri(targetUri), ROOT_TABLE))
                        .equals(List.of(SURVIVING_MIDDLE)));

                // The row that can never be placed, arriving after its parent is gone.
                insertLateLeaf(mysql);
                await(() -> control.deadLettered(pipelineId).orElse(0L) >= 1L);

                assertThat(control.deadLettered(pipelineId).orElse(0L))
                        .as("changes the nest could not place: the leaf naming middle row %d, whose parent "
                                + "is gone. A level that removed the mapping instead of recording the "
                                + "deletion holds this row forever and reports zero.%n  documents: %s%n"
                                + "  metrics: %s", DELETED_MIDDLE,
                                mongo.documents(EndpointAddress.uri(targetUri), ROOT_TABLE), control.metrics(pipelineId))
                        .isGreaterThanOrEqualTo(1L);

                assertThat(control.state(pipelineId))
                        .as("a source deleting a row whose children are in flight is ordinary")
                        .contains(PipelineState.RUNNING);
                assertThat(control.errorCount(pipelineId))
                        .as("nor a reason to count an error")
                        .contains(0L);
                assertThat(middleIds(mongo.documents(EndpointAddress.uri(targetUri), ROOT_TABLE)))
                        .as("the deletion took the row it named out of the document, and nothing else")
                        .containsExactly(SURVIVING_MIDDLE);
                assertThat(leafIds(mongo.documents(EndpointAddress.uri(targetUri), ROOT_TABLE), SURVIVING_MIDDLE))
                        .as("the leaf that arrived late names the deleted row, so it belongs nowhere - "
                                + "least of all under the row that survived")
                        .isEmpty();
            }
        }
    }

    private static void await(BooleanSupplier reached) {
        long deadline = System.nanoTime() + TIMEOUT.toNanos();
        while (System.nanoTime() - deadline < 0) {
            if (reached.getAsBoolean()) {
                return;
            }
            sleep();
        }
    }

    /** The ids of the middle-level elements under the root, in array order. */
    private static List<Long> middleIds(List<Document> documents) {
        Document root = documentOrNull(documents);
        return root == null ? List.of() : idsOf(elementsAt(root, MIDDLE_PATH));
    }

    /** The ids of the leaves under one middle element, in array order. */
    private static List<Long> leafIds(List<Document> documents, long middleId) {
        Document root = documentOrNull(documents);
        if (root == null) {
            return List.of();
        }
        for (Document middle : elementsAt(root, MIDDLE_PATH)) {
            if (numberOf(identityOf(middle)) == middleId) {
                return idsOf(elementsAt(middle, LEAF_PATH));
            }
        }
        return List.of();
    }

    private static List<Long> idsOf(List<Document> elements) {
        List<Long> ids = new ArrayList<>(elements.size());
        for (Document element : elements) {
            ids.add(numberOf(identityOf(element)));
        }
        java.util.Collections.sort(ids);
        return ids;
    }

    private String diagnose(ControlPlane control, MongoEndpoints mongo, String targetUri, String what) {
        return what + ": '" + ROOT_TABLE + "' holds " + mongo.documents(EndpointAddress.uri(targetUri), ROOT_TABLE)
                + System.lineSeparator() + "  pipeline state: " + control.state(pipelineId)
                + ", error count: " + control.errorCount(pipelineId)
                + System.lineSeparator() + "  metrics: " + control.metrics(pipelineId)
                + System.lineSeparator() + "  logs: " + control.logs(pipelineId);
    }

    private static Document documentOrNull(List<Document> documents) {
        for (Document document : documents) {
            if (numberOf(identityOf(document)) == ROOT_ID) {
                return document;
            }
        }
        return null;
    }

    private static List<Document> elementsAt(Document parent, String path) {
        Object embedded = parent.get(path);
        if (!(embedded instanceof List<?> list)) {
            return List.of();
        }
        List<Document> elements = new ArrayList<>(list.size());
        for (Object element : list) {
            if (element instanceof Document document) {
                elements.add(document);
            }
        }
        return elements;
    }

    private static Object identityOf(Document document) {
        Object id = document.get("id");
        return id != null ? id : document.get("_id");
    }

    private static long numberOf(Object value) {
        if (value instanceof Number number) {
            return number.longValue();
        }
        return Long.MIN_VALUE;
    }

    /** One root and two middle rows under it; the leaf comes later, after one of them is gone. */
    private static void seedMysql(MySQLContainer<?> mysql) throws Exception {
        try (Connection connection =
                DriverManager.getConnection(mysql.getJdbcUrl(), mysql.getUsername(), mysql.getPassword())) {
            try (Statement statement = connection.createStatement()) {
                statement.execute("CREATE TABLE " + ROOT_TABLE + " (id INT PRIMARY KEY, name VARCHAR(64))");
                statement.execute("CREATE TABLE " + MIDDLE_TABLE
                        + " (id INT PRIMARY KEY, customer_id INT, code VARCHAR(64))");
                // No foreign key: a leaf naming a row that is gone has to be expressible.
                statement.execute("CREATE TABLE " + LEAF_TABLE
                        + " (id INT PRIMARY KEY, order_id INT, sku VARCHAR(64))");
            }
            try (PreparedStatement insert = connection.prepareStatement(
                    "INSERT INTO " + ROOT_TABLE + " (id, name) VALUES (?, ?)")) {
                insert.setLong(1, ROOT_ID);
                insert.setString(2, "customer-" + ROOT_ID);
                insert.executeUpdate();
            }
            try (PreparedStatement insert = connection.prepareStatement(
                    "INSERT INTO " + MIDDLE_TABLE + " (id, customer_id, code) VALUES (?, ?, ?)")) {
                for (long id : List.of(DELETED_MIDDLE, SURVIVING_MIDDLE)) {
                    insert.setLong(1, id);
                    insert.setLong(2, ROOT_ID);
                    insert.setString(3, "order-" + id);
                    insert.addBatch();
                }
                insert.executeBatch();
            }
        }
    }

    private static void deleteMiddleRow(MySQLContainer<?> mysql) throws Exception {
        try (Connection connection =
                        DriverManager.getConnection(mysql.getJdbcUrl(), mysql.getUsername(), mysql.getPassword());
                PreparedStatement delete = connection.prepareStatement(
                        "DELETE FROM " + MIDDLE_TABLE + " WHERE id = ?")) {
            delete.setLong(1, DELETED_MIDDLE);
            delete.executeUpdate();
        }
    }

    /** The row that can never be placed: it names the middle row the source has already removed. */
    private static void insertLateLeaf(MySQLContainer<?> mysql) throws Exception {
        try (Connection connection =
                        DriverManager.getConnection(mysql.getJdbcUrl(), mysql.getUsername(), mysql.getPassword());
                PreparedStatement insert = connection.prepareStatement(
                        "INSERT INTO " + LEAF_TABLE + " (id, order_id, sku) VALUES (?, ?, ?)")) {
            insert.setLong(1, LATE_LEAF);
            insert.setLong(2, DELETED_MIDDLE);
            insert.setString(3, "sku-late");
            insert.executeUpdate();
        }
    }

    private static void grantReplication(MySQLContainer<?> mysql) throws Exception {
        try (Connection root = DriverManager.getConnection(mysql.getJdbcUrl(), "root", mysql.getPassword());
                Statement statement = root.createStatement()) {
            statement.execute("GRANT REPLICATION SLAVE, REPLICATION CLIENT, RELOAD, SELECT ON *.* TO '"
                    + mysql.getUsername() + "'@'%'");
            statement.execute("FLUSH PRIVILEGES");
        }
    }

    private static Map<String, Object> mysqlConfig(MySQLContainer<?> mysql) {
        Map<String, Object> config = new LinkedHashMap<>();
        config.put("host", mysql.getHost());
        config.put("port", mysql.getMappedPort(MySQLContainer.MYSQL_PORT));
        config.put("database", mysql.getDatabaseName());
        config.put("username", mysql.getUsername());
        config.put("password", mysql.getPassword());
        return config;
    }

    private static String sourceYaml(String id, String table, Map<String, Object> config) {
        return """
                version: tapstate/v1
                kind: source
                id: %s
                connector: mysql
                config: { host: %s, port: %s, database: %s, username: %s, password: %s }
                mode: cdc
                tables: [ %s ]
                """
                .formatted(
                        id,
                        config.get("host"),
                        config.get("port"),
                        config.get("database"),
                        config.get("username"),
                        config.get("password"),
                        table);
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
                source: [ src_customers, src_orders, src_items ]
                settings: { read_mode: snapshot_and_cdc }
                transforms:
                  - id: customer_doc
                    type: nest
                    from: { c: customers, o: orders, i: order_items }
                    root:
                      from: c
                      key: [ id ]
                      embed:
                        - from: o
                          on: { customer_id: id }
                          as: array
                          path: orders
                          arrayKey: [ id ]
                          embed:
                            - { from: i, on: { order_id: id }, as: array, path: items, arrayKey: [ id ] }
                serve:
                  from: customer_doc
                  sync:
                    - source: tgt_mongo
                """
                .formatted(pipelineId);
    }

    private static void sleep() {
        try {
            Thread.sleep(POLL.toMillis());
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new AssertionError("interrupted while waiting for the deletion to be carried", e);
        }
    }
}
