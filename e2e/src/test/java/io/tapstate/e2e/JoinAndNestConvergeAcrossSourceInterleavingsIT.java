package io.tapstate.e2e;

import static org.assertj.core.api.Assertions.assertThat;

import io.tapstate.core.lifecycle.LifecycleVerb;
import io.tapstate.core.lifecycle.PipelineState;
import io.tapstate.testsupport.DockerGate;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.Statement;
import java.time.Duration;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import org.bson.Document;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.MySQLContainer;
import org.testcontainers.utility.DockerImageName;

/**
 * A nest and a join reach the same result whichever of their two tables brings a key first, on a cluster of three
 * members, and go on doing so after one of the members dies and the run is replaced.
 *
 * <p>Two tables are read by two chains and nothing orders one against the other, so each case feeds every key both
 * ways round: a child before its parent and a parent before its child, a row before the row it joins to and after it.
 * Some of each arrive with the load, some as changes, and some after a member is killed, so the run that replaces the
 * dead one takes them up part way - replaying whatever it has not confirmed. What is compared is the result, the
 * whole of it, against what the two tables say: a document or a joined row carrying the wrong side, or missing one
 * that arrived early, differs from it however it was produced.
 *
 * <p>Nothing may be published ahead of what it depends on either: children with no parent are not a document, and
 * an order whose customer has not arrived is no row of an inner join. That is watched before the missing side is
 * written, bounded rather than proved, and it is the other half's result that carries the weight.
 *
 * <p>The nest runs once for the cluster, as a node given no width does, with stand-ins on the other members; so the
 * nest case also holds its loads to landing, which a level fed by two tables can do on more than one member only if
 * every member of it says how far each table has got.
 */
class JoinAndNestConvergeAcrossSourceInterleavingsIT {

    private static final String THIRD = "node-c";
    private static final Duration CONVERGE = Duration.ofMinutes(3);
    private static final Duration NOTHING_YET = Duration.ofSeconds(15);

    @BeforeAll
    static void requireDockerAndRealConnectors() {
        DockerGate.require();
        RealConnectorGate.require("mysql", "mongodb");
    }

    @Test
    void aNestReachesTheSameDocumentsWhicheverOfItsTablesArrivesFirst() throws Exception {
        String pipeline = "interleaved_nest";
        try (MySQLContainer<?> mysql = new MySQLContainer<>(DockerImageName.parse("mysql:8.0"))) {
            mysql.start();
            grantReplication(mysql);
            execute(mysql,
                    "CREATE TABLE orders (id INT PRIMARY KEY, name VARCHAR(64))",
                    "CREATE TABLE order_items (id INT PRIMARY KEY, order_id INT, sku VARCHAR(64))",
                    // Both sides of order 1 with the load; order 2's items with the load and order 2 after it;
                    // order 3 with the load and its items after it.
                    "INSERT INTO orders VALUES (1, 'order-1'), (3, 'order-3')",
                    "INSERT INTO order_items VALUES (11, 1, 'sku-11'), (12, 1, 'sku-12'), (21, 2, 'sku-21'),"
                            + " (22, 2, 'sku-22')");
            String store = SharedMongo.replicaSetUrl("e2e_interleaved_nest_cluster");
            String target = SharedMongo.replicaSetUrl("e2e_interleaved_nest_target");
            try (TwoMemberCluster cluster = TwoMemberCluster.start(store, "e2e-interleaved-nest");
                    MongoEndpoints mongo = new MongoEndpoints()) {
                RealProcessServer third = threeMembers(cluster);
                try {
                    ControlPlane control = cluster.first();
                    Map<String, Object> config = mysqlConfig(mysql);
                    Map<String, String> resources = sources(config, "orders", "order_items");
                    resources.put("tgt_mongo.tap.yml", """
                            version: tapstate/v1
                            kind: source
                            id: tgt_mongo
                            connector: mongodb
                            config: { uri: "%s" }
                            """.formatted(target));
                    resources.put(pipeline + ".tap.yml", """
                            version: tapstate/v1
                            kind: pipeline
                            id: %s
                            source: [ src_orders, src_order_items ]
                            settings: { read_mode: snapshot_and_cdc }
                            transforms:
                              - id: order_doc
                                type: nest
                                from: { o: orders, i: order_items }
                                root:
                                  from: o
                                  key: [ id ]
                                  embed:
                                    - { from: i, on: { order_id: id }, as: array, path: items, arrayKey: [ id ] }
                            serve:
                              from: order_doc
                              sync:
                                - source: tgt_mongo
                            """.formatted(pipeline));
                    start(control, config, resources, pipeline, "src_orders", "src_order_items");
                    EndpointAddress documents = EndpointAddress.uri(target);

                    Map<Long, List<Long>> expected = new TreeMap<>(Map.of(1L, List.of(11L, 12L), 3L, List.of()));
                    awaitDocuments(control, mongo, documents, pipeline, expected);
                    watchNothingFor(mongo, documents, 2L, "items with no order are no document");
                    Await.until("both tables' loads to land", CONVERGE,
                            () -> landed(control, pipeline, "orders") && landed(control, pipeline, "order_items"),
                            () -> "the snapshot read says " + control.snapshotTables(pipeline));

                    // A parent after its children, and children after their parent, as changes.
                    execute(mysql, "INSERT INTO orders VALUES (2, 'order-2')",
                            "INSERT INTO order_items VALUES (31, 3, 'sku-31'), (32, 3, 'sku-32')");
                    expected.put(2L, List.of(21L, 22L));
                    expected.put(3L, List.of(31L, 32L));
                    awaitDocuments(control, mongo, documents, pipeline, expected);

                    replaceTheRunByKillingAMember(cluster, third, control, pipeline);

                    // Both ways round again, for the run that replaced the dead one.
                    execute(mysql, "INSERT INTO order_items VALUES (41, 4, 'sku-41')",
                            "INSERT INTO orders VALUES (4, 'order-4'), (5, 'order-5')",
                            "INSERT INTO order_items VALUES (51, 5, 'sku-51')");
                    expected.put(4L, List.of(41L));
                    expected.put(5L, List.of(51L));
                    awaitDocuments(cluster.first(), mongo, documents, pipeline, expected);
                } finally {
                    third.close();
                }
            }
        }
    }

    @Test
    void aJoinReachesTheSameRowsWhicheverOfItsTablesArrivesFirst() throws Exception {
        String pipeline = "interleaved_join";
        try (MySQLContainer<?> mysql = new MySQLContainer<>(DockerImageName.parse("mysql:8.0"))) {
            mysql.start();
            grantReplication(mysql);
            execute(mysql,
                    "CREATE TABLE customers (id INT PRIMARY KEY, name VARCHAR(64))",
                    "CREATE TABLE orders (id INT PRIMARY KEY, customer_id INT, qty INT)",
                    // Both sides of order 100 with the load; order 200 with the load and its customer after it;
                    // customer 3 with the load and its order after it.
                    "INSERT INTO customers VALUES (1, 'ada'), (3, 'cy')",
                    "INSERT INTO orders VALUES (100, 1, 1), (200, 2, 2)");
            String store = SharedMongo.replicaSetUrl("e2e_interleaved_join_cluster");
            try (TwoMemberCluster cluster = TwoMemberCluster.start(store, "e2e-interleaved-join");
                    MongoEndpoints mongo = new MongoEndpoints()) {
                RealProcessServer third = threeMembers(cluster);
                try {
                    ControlPlane control = cluster.first();
                    Map<String, Object> config = mysqlConfig(mysql);
                    Map<String, String> resources = sources(config, "orders", "customers");
                    resources.put("views.tap.yml", """
                            version: tapstate/v1
                            kind: source
                            id: views
                            connector: mongodb
                            config: { uri: "%s" }
                            """.formatted(store));
                    resources.put(pipeline + ".tap.yml", """
                            version: tapstate/v1
                            kind: pipeline
                            id: %s
                            source: [ src_orders, src_customers ]
                            settings: { read_mode: snapshot_and_cdc }
                            transforms:
                              - id: widen
                                type: join
                                from: { o: orders, c: customers }
                                engine: builtin
                                sql: |
                                  SELECT o.id AS order_id, c.name AS customer_name
                                  FROM o JOIN c ON o.customer_id = c.id
                            view:
                              id: interleaved_order_state
                              from: widen
                              primary_key: order_id
                            """.formatted(pipeline));
                    start(control, config, resources, pipeline, "src_orders", "src_customers");
                    EndpointAddress rows = EndpointAddress.uri(store);

                    Map<Long, String> expected = new TreeMap<>(Map.of(100L, "ada"));
                    awaitRows(control, mongo, rows, pipeline, expected);
                    watchNothingFor(mongo, rows, 200L, "an order whose customer has not arrived is no joined row");

                    // A customer after its order, and an order after its customer, as changes.
                    execute(mysql, "INSERT INTO customers VALUES (2, 'bo')", "INSERT INTO orders VALUES (300, 3, 3)");
                    expected.put(200L, "bo");
                    expected.put(300L, "cy");
                    awaitRows(control, mongo, rows, pipeline, expected);

                    replaceTheRunByKillingAMember(cluster, third, control, pipeline);

                    // Both ways round again for the run that replaced the dead one, and a change to a side both
                    // before and after the kill have joined to.
                    execute(mysql, "INSERT INTO orders VALUES (400, 4, 4)",
                            "INSERT INTO customers VALUES (4, 'di'), (5, 'ed')",
                            "INSERT INTO orders VALUES (500, 5, 5)",
                            "UPDATE customers SET name = 'ada-2' WHERE id = 1");
                    expected.put(400L, "di");
                    expected.put(500L, "ed");
                    expected.put(100L, "ada-2");
                    awaitRows(cluster.first(), mongo, rows, pipeline, expected);
                } finally {
                    third.close();
                }
            }
        }
    }

    /** Brings the cluster to three members, and answers the third, which the caller closes. */
    private static RealProcessServer threeMembers(TwoMemberCluster cluster) {
        cluster.awaitBothMembers();
        RealProcessServer third = cluster.launching(THIRD);
        cluster.awaitMembers(3);
        ControlPlane control = cluster.first();
        Await.until(THIRD + " to be committed to the cluster", Duration.ofMinutes(2),
                () -> control.clusterMembers().stream().anyMatch(member ->
                        THIRD.equals(member.nodeId()) && "ACTIVE".equals(member.state())),
                () -> "the members were " + control.clusterMembers());
        return third;
    }

    private static void start(ControlPlane control, Map<String, Object> config, Map<String, String> resources,
            String pipeline, String... sources) {
        control.registerConnector("mysql", ConnectorJars.bytesFor("mysql"));
        control.registerConnector("mongodb", ConnectorJars.bytesFor("mongodb"));
        control.apply(resources);
        for (String source : sources) {
            control.discoverSchema(source, "mysql", config);
        }
        control.lifecycle(pipeline, LifecycleVerb.START);
        Await.until(pipeline + " to reach " + PipelineState.RUNNING, Duration.ofMinutes(2),
                () -> control.state(pipeline).filter(PipelineState.RUNNING::equals).isPresent(),
                () -> control.state(pipeline) + ", failure " + control.failure(pipeline));
    }

    /**
     * Kills a member the pipeline is not driven from, and waits for the run it was part of to be replaced: the
     * changes made after this are the replacement's to carry.
     */
    private static void replaceTheRunByKillingAMember(TwoMemberCluster cluster, RealProcessServer third,
            ControlPlane control, String pipeline) {
        String driver = Await.answered("the cluster to name the member driving " + pipeline,
                () -> control.pipelineControllerOf(pipeline));
        long generation = control.executionGenerationOf(pipeline).orElseThrow();
        if (!THIRD.equals(driver)) {
            third.kill();
        } else {
            cluster.processCarrying(TwoMemberCluster.NODE_B).kill();
        }
        ControlPlane survivor = THIRD.equals(driver) ? cluster.first() : cluster.memberOtherThan(TwoMemberCluster.NODE_B);
        Await.until("the run to be replaced after a member died", CONVERGE,
                () -> survivor.executionGenerationOf(pipeline).filter(next -> next > generation).isPresent()
                        && survivor.state(pipeline).filter(PipelineState.RUNNING::equals).isPresent(),
                () -> "the pipeline is " + survivor.state(pipeline) + " at generation "
                        + survivor.executionGenerationOf(pipeline) + ", was " + generation);
    }

    private static void awaitDocuments(ControlPlane control, MongoEndpoints mongo, EndpointAddress target,
            String pipeline, Map<Long, List<Long>> expected) {
        Await.until("the documents to be " + expected, CONVERGE,
                () -> documents(mongo, target).equals(expected),
                () -> "the documents are " + documents(mongo, target) + "; the pipeline is " + control.state(pipeline)
                        + ", failure " + control.failure(pipeline));
    }

    private static void awaitRows(ControlPlane control, MongoEndpoints mongo, EndpointAddress view, String pipeline,
            Map<Long, String> expected) {
        Await.until("the joined rows to be " + expected, CONVERGE,
                () -> joined(mongo, view).equals(expected),
                () -> "the joined rows are " + joined(mongo, view) + "; the pipeline is " + control.state(pipeline)
                        + ", failure " + control.failure(pipeline));
    }

    /**
     * Watches for a while that {@code key} is published nowhere. Taken after the result holding everything else has
     * been seen, so the pipeline has had every row of the load in hand for at least that long.
     */
    private static void watchNothingFor(MongoEndpoints mongo, EndpointAddress where, long key, String why) {
        long watchedUntil = System.nanoTime() + NOTHING_YET.toNanos();
        Await.until("the watch on " + key + " to run its course", NOTHING_YET.plusMinutes(1),
                () -> {
                    assertThat(keysPublished(mongo, where)).as(why).doesNotContain(key);
                    return System.nanoTime() - watchedUntil >= 0;
                },
                () -> "published: " + keysPublished(mongo, where));
    }

    /** Every order document, as its id and the ids of the items it carries, in order. */
    private static Map<Long, List<Long>> documents(MongoEndpoints mongo, EndpointAddress target) {
        Map<Long, List<Long>> byOrder = new TreeMap<>();
        for (Document document : mongo.documents(target, "orders")) {
            List<Long> items = new ArrayList<>();
            if (document.get("items") instanceof List<?> embedded) {
                for (Object element : embedded) {
                    if (element instanceof Document item) {
                        items.add(numberOf(identityOf(item)));
                    }
                }
            }
            items.sort(Long::compare);
            byOrder.put(numberOf(identityOf(document)), items);
        }
        return byOrder;
    }

    /** Every joined row, as its order id and the customer name it carries. */
    private static Map<Long, String> joined(MongoEndpoints mongo, EndpointAddress view) {
        Map<Long, String> rows = new TreeMap<>();
        for (Document row : mongo.documents(view, "interleaved_order_state")) {
            rows.put(numberOf(row.get("order_id")), String.valueOf(row.get("customer_name")));
        }
        return rows;
    }

    private static List<Long> keysPublished(MongoEndpoints mongo, EndpointAddress where) {
        List<Long> keys = new ArrayList<>(documents(mongo, where).keySet());
        for (Document row : mongo.documents(where, "interleaved_order_state")) {
            keys.add(numberOf(row.get("order_id")));
        }
        return keys;
    }

    private static boolean landed(ControlPlane control, String pipeline, String table) {
        return control.snapshotTables(pipeline).containsKey(table)
                && control.snapshotTables(pipeline).get(table).landed();
    }

    /** A row's identity as the target holds it, whether the sink kept the source field or mapped it. */
    private static Object identityOf(Document document) {
        Object id = document.get("id");
        return id != null ? id : document.get("_id");
    }

    /** MySQL integers reach Mongo as one of several widths; compare them as one. */
    private static long numberOf(Object value) {
        return value instanceof Number number ? number.longValue() : Long.MIN_VALUE;
    }

    /** One source per table: two single-table sources are what puts two chains into one job. */
    private static Map<String, String> sources(Map<String, Object> config, String... tables) {
        Map<String, String> resources = new LinkedHashMap<>();
        for (String table : tables) {
            resources.put("src_" + table + ".tap.yml", """
                    version: tapstate/v1
                    kind: source
                    id: src_%s
                    connector: mysql
                    config: { host: %s, port: %s, database: %s, username: %s, password: %s }
                    mode: cdc
                    tables: [ %s ]
                    """.formatted(table, config.get("host"), config.get("port"), config.get("database"),
                    config.get("username"), config.get("password"), table));
        }
        return resources;
    }

    private static void execute(MySQLContainer<?> mysql, String... statements) throws Exception {
        try (Connection connection =
                        DriverManager.getConnection(mysql.getJdbcUrl(), mysql.getUsername(), mysql.getPassword());
                Statement statement = connection.createStatement()) {
            for (String sql : statements) {
                statement.execute(sql);
            }
        }
    }

    /** Binlog CDC needs replication privileges the default test user lacks; grant them as root. */
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
}
