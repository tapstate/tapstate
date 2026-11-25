package io.tapstate.e2e;

import io.tapstate.core.lifecycle.LifecycleVerb;
import io.tapstate.core.lifecycle.PipelineState;
import io.tapstate.adapters.mongostore.SystemCollections;
import io.tapstate.runtime.engine.nest.NestElement;
import io.tapstate.runtime.engine.nest.ResolverState;
import io.tapstate.testsupport.DockerGate;

import org.bson.Document;
import org.bson.types.Binary;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;
import org.testcontainers.containers.MySQLContainer;
import org.testcontainers.utility.DockerImageName;

import java.sql.Connection;
import java.io.ByteArrayInputStream;
import java.io.ObjectInputStream;
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
 * A row arriving under a deleted middle parent waits durably and is released when that parent returns.
 *
 * <p>A deletion does not prove that a parent key will never be declared again. The original deletion
 * order and later child must survive in the state store. A newer declaration restores the mapping and
 * releases that child without rereading it from the source or replacing its original position.
 *
 * <p>The target alone cannot distinguish waiting from dropping while the parent is absent. This case
 * first reads the actual durable waiting child, then restores only the parent and requires that child
 * in the target. A run that drops the child cannot satisfy both checks.
 *
 * <p>The run must stay alive without errors or dead letters while waiting and after release. The
 * surviving middle row is checked before and after the newer declaration, so deleting one parent cannot
 * remove its neighbour or route the waiting child under the wrong parent.
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
    void aChildArrivingUnderADeletedMiddleRowWaitsDurablyForItsReturn(Tiers tier) throws Exception {
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

                // The child is still placeable if a newer parent declaration arrives.
                insertLateLeaf(mysql);
                await(() -> waitingLeaf(mongo, storage, pipelineId) != null);
                NestElement waiting = waitingLeaf(mongo, storage, pipelineId);
                assertThat(waiting).as("the deleted parent's child is in durable waiting").isNotNull();
                assertThat(waiting.positions()).as("the waiting child retains its original source position").isNotEmpty();
                assertThat(control.deadLettered(pipelineId)).contains(0L);

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
                        .as("the late leaf waits for its deleted parent and cannot appear under the survivor")
                        .isEmpty();

                restoreMiddleRow(mysql);
                await(() -> leafIds(mongo.documents(EndpointAddress.uri(targetUri), ROOT_TABLE), DELETED_MIDDLE)
                        .equals(List.of(LATE_LEAF)));
                assertThat(middleIds(mongo.documents(EndpointAddress.uri(targetUri), ROOT_TABLE)))
                        .containsExactly(DELETED_MIDDLE, SURVIVING_MIDDLE);
                assertThat(leafIds(mongo.documents(EndpointAddress.uri(targetUri), ROOT_TABLE), DELETED_MIDDLE))
                        .as("only restoring the parent releases the original waiting child").containsExactly(LATE_LEAF);
                assertThat(leafIds(mongo.documents(EndpointAddress.uri(targetUri), ROOT_TABLE), SURVIVING_MIDDLE)).isEmpty();
                assertThat(control.deadLettered(pipelineId)).contains(0L);
                assertThat(control.errorCount(pipelineId)).contains(0L);
                assertThat(control.state(pipelineId)).contains(PipelineState.RUNNING);
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

    private static void restoreMiddleRow(MySQLContainer<?> mysql) throws Exception {
        try (Connection connection = DriverManager.getConnection(mysql.getJdbcUrl(), mysql.getUsername(), mysql.getPassword());
                PreparedStatement insert = connection.prepareStatement(
                        "INSERT INTO " + MIDDLE_TABLE + " (id, customer_id, code) VALUES (?, ?, ?)")) {
            insert.setLong(1, DELETED_MIDDLE);
            insert.setLong(2, ROOT_ID);
            insert.setString(3, "order-returned");
            insert.executeUpdate();
        }
    }

    private static NestElement waitingLeaf(MongoEndpoints mongo, PreparedNativeStore storage, String pipeline) {
        for (Document row : mongo.documents(EndpointAddress.uri(storage.cacheUri()),
                SystemCollections.CACHE_NEST_STATE.collectionName())) {
            if (!(row.get("_id") instanceof Document id) || !(id.get("placement") instanceof Document placement)
                    || !(placement.get("scope") instanceof Document scope) || !pipeline.equals(scope.getString("pipelineId"))) continue;
            Binary bytes = row.get("state", Binary.class);
            try (var input = new ObjectInputStream(new ByteArrayInputStream(bytes.getData()))) {
                if (!(input.readObject() instanceof ResolverState state) || !state.deleted()) continue;
                for (NestElement child : state.waiting()) {
                    if (child.fields().get("id") instanceof Number value && value.longValue() == LATE_LEAF) return child;
                }
            } catch (java.io.IOException | ClassNotFoundException invalid) {
                throw new AssertionError("the native waiting state could not be read", invalid);
            }
        }
        return null;
    }

    /** The late child names a parent that is absent until the newer insert below. */
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
