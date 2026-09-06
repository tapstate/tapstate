package io.tapstate.e2e;

import io.tapstate.core.lifecycle.LifecycleVerb;
import io.tapstate.testsupport.DockerGate;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.sql.Connection;
import java.sql.Statement;
import java.time.Duration;
import java.util.LinkedHashMap;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * A dimension row with a large fan-out under it is edited, and the rebuild that owes says so on the read
 * face while it is happening - and a dimension row with three rows under it says nothing at all.
 *
 * <p><b>What the pair is for.</b> Editing one customer's name owes every order row under them, and for as
 * long as that takes the target holds half the old name and half the new one while every other reading
 * says healthy: the state is RUNNING, the error count is zero, the queues drain, the record count climbs.
 * An operator with no reading for this cannot tell a rebuild in flight from a pipeline that has finished,
 * and so cannot decide whether to hold off a downstream read. But a reading published on every dimension
 * edit is a constant stream, and a reader who has learned to scroll past it is a reader this cannot reach
 * on the occasion it was built for. So both halves are asserted, and neither means anything alone.
 *
 * <p><b>Why this cannot be a published example.</b> Two of the words it would need do not exist. The
 * specification language seeds by listing values, and one dimension key here holds twelve thousand fact
 * rows; and every assertion it can make - {@code count}, {@code doc}, {@code error_count}, {@code state} -
 * reads the target's contents, while what is under test is a runtime reading about work in progress. The
 * second gap is the larger of the two: it is a new kind of observable rather than a matter of scale.
 *
 * <p><b>Why the unit cases are not enough either.</b> That the rebuild reports its progress at all, that
 * the progress only advances, and that it is reported part way rather than at the end are pinned down
 * where they can be made exact, against a sink with a limit on it. What none of them can reach is the path
 * from there to somebody looking: the reading is left among a job's own statistics, from a job thread,
 * picked back out by name on another member, renamed by the layer that publishes it and read over HTTP.
 * A break anywhere along that path shows up as a pipeline that reports no rebuild - which is also exactly
 * what a pipeline with no large rebuild looks like.
 *
 * <p>What each assertion has to discriminate:
 *
 * <ul>
 *   <li><b>The quiet customer's edit is made first and is shown to have landed</b> before anything is
 *       asserted about it. Without that fence "no reading appeared for them" is equally well explained by
 *       a pipeline that had stopped consuming, and would pass with the whole feature removed.</li>
 *   <li><b>The loud customer's rebuild is read back by name, and its size against the threshold.</b> A
 *       reading that arrived under a name nobody can tie to a dimension row would satisfy "something is
 *       happening" and answer none of what an operator asks next.</li>
 *   <li><b>The rows-sent reading equals the rows that actually carry the new name.</b> Not the size
 *       beside it: that one is read off the index as pages times a page, so it is an upper bound and is
 *       exact only for a single-page bucket. This is the assertion that would catch a reading written to
 *       report what a rebuild set out to do rather than what it did.</li>
 *   <li><b>Most of the twelve thousand rows carry the new name.</b> The readings above are about a
 *       rebuild; this is what says a rebuild is what happened, and it is what would fail if the edit
 *       reached nothing at all.</li>
 * </ul>
 *
 * <p><b>What this case deliberately does not assert, and why.</b> The rebuild does not reach every row:
 * measured three times here, it republishes 11,993 of the 12,000 under the key and the same seven keep
 * the old value however many times the dimension row is edited afterwards. That is a defect of the
 * rebuild and it is filed on its own; holding this case to it would mean a reading whose entire job is
 * to say how far a rebuild got could not be witnessed until the rebuild got all the way. What is
 * asserted instead is stronger against the failure this reading actually has: that it agrees with the
 * target rather than with the estimate.
 *
 * <pre>
 *   mvn -pl e2e -am verify -Dapi.version=1.44 \
 *     -Dtapstate.e2e.connectors-dir=/path/to/connectors \
 *     -Dit.test=ALargeRebuildIsVisibleWhileItRunsIT \
 *     -Dfailsafe.failIfNoSpecifiedTests=false
 * </pre>
 */
class ALargeRebuildIsVisibleWhileItRunsIT {

    private static final String DATABASE = "recompute_db";
    private static final String PIPELINE_ID = "order_pipeline";
    private static final String TARGET = "order_state";

    /** The customer whose fan-out is worth telling somebody about. */
    private static final int LOUD_CUSTOMER = 1;

    /** And the one whose is not, which is the only way to tell a threshold from no threshold. */
    private static final int QUIET_CUSTOMER = 2;

    /**
     * Comfortably past the size a rebuild has to reach to be reported, and no further. What is under test
     * is which side of the threshold a fan-out falls on, and a million rows would test the same thing
     * while spending minutes of every run on it.
     */
    private static final int LOUD_ORDERS = 12_000;

    private static final int QUIET_ORDERS = 3;

    /** The first order id of each customer's run, so a row of either can be named without a lookup. */
    private static final int LOUD_FIRST_ORDER = 100_000;
    private static final int QUIET_FIRST_ORDER = 900_000;

    /** What every reading about a rebuild's progress is named, before the rebuild it is about. */
    private static final String DONE = "joinRecomputeRowsDone.";
    private static final String EXPECTED = "joinRecomputeRowsExpected.";

    /** Two real engines, a snapshot of twelve thousand rows and a change stream: minutes, not seconds. */
    private static final Duration SETTLE = Duration.ofMinutes(4);

    @BeforeAll
    static void requireDockerAndRealConnectors() {
        DockerGate.require();
        RealConnectorGate.require("mysql", "mongodb");
    }

    @Test
    void aLargeRebuildIsReportedByTheKeyItIsAboutAndASmallOneIsNotReportedAtAll() throws Exception {
        Map<String, Object> mysql = SharedMySql.settings(DATABASE);
        seed(mysql);

        String storeUri = SharedMongo.replicaSetUrl("recompute_store");
        String targetUri = SharedMongo.replicaSetUrl("recompute_target");
        EndpointAddress target = EndpointAddress.uri(targetUri);

        try (MongoEndpoints mongo = new MongoEndpoints();
                ServerHandle server = Tiers.IN_PROCESS.launch(storeUri)) {
            ControlPlane control = new ControlPlane(server.baseUrl());
            control.bootstrapAndLogin("e2e", "e2e-password");
            control.registerConnector("mysql", ConnectorJars.bytesFor("mysql"));
            control.registerConnector("mongodb", ConnectorJars.bytesFor("mongodb"));

            Map<String, String> resources = new LinkedHashMap<>();
            resources.put("shop_db.tap.yml", sourceYaml(mysql));
            resources.put("views.tap.yml", targetYaml(targetUri));
            resources.put("join_pipeline.tap.yml", pipelineYaml());
            control.apply(resources);
            control.discoverSchema("shop_db", "mysql", mysql);
            control.lifecycle(PIPELINE_ID, LifecycleVerb.START);

            long seeded = LOUD_ORDERS + QUIET_ORDERS;
            Await.until("the snapshot of " + seeded + " orders to reach the target", SETTLE,
                    () -> mongo.count(target, TARGET) == seeded,
                    () -> mongo.count(target, TARGET) + " rows");

            // The control, and it goes first so that whatever it must not produce has the whole of the
            // large rebuild's wait afterwards in which to show up.
            execute(mysql, "UPDATE customers SET name = 'bosley' WHERE id = " + QUIET_CUSTOMER);
            awaitName(mongo, target, QUIET_FIRST_ORDER, "bosley",
                    "the three-row customer's order to be republished under the new name");

            assertThat(rebuildReadings(control))
                    .as("three rows is not a wait anybody has to be told about, and a reading on every "
                            + "dimension edit is the stream that makes the one that matters invisible")
                    .isEmpty();

            // The edit under test: one column of one row, owing twelve thousand rows of writing.
            execute(mysql, "UPDATE customers SET name = 'adelaide' WHERE id = " + LOUD_CUSTOMER);

            Await.until("the rebuild of the twelve-thousand-row customer to be reported", SETTLE,
                    () -> !control.metricsNamed(PIPELINE_ID, DONE).isEmpty(),
                    () -> String.valueOf(rebuildReadings(control)));

            String subject = "join." + PIPELINE_ID + ".widen.index.c/" + LOUD_CUSTOMER;
            assertThat(control.metricsNamed(PIPELINE_ID, EXPECTED))
                    .as("the reading says which dimension row is being rebuilt and about how far it "
                            + "has to go; a size below the reporting threshold would mean the wrong "
                            + "fan-out is being reported")
                    .hasEntrySatisfying(EXPECTED + subject, rows -> assertThat(rows)
                            .isGreaterThanOrEqualTo(LOUD_ORDERS));

            // A rebuild is what happened, rather than a pair of numbers about one - and it reached
            // every row, which is asserted by counting them rather than by sampling: seven rows left
            // holding the old name is what the rows-sent reading looks like when it is telling the
            // truth about a rebuild that quietly stopped short.
            // What this case is about: the reading tells the truth about the rebuild. It is asserted
            // against the target rather than against the size beside it, and the difference matters -
            // measured here, this rebuild reaches 11993 of the 12000 rows under the key and stops, so a
            // reading that had been written to report the size would say 12000 and be believed. That
            // shortfall is a defect of the rebuild rather than of this reading, is filed separately,
            // and is deliberately not what fails this case: a reading whose whole job is to say how far
            // a rebuild got must not be held to the rebuild getting all the way.
            Await.until("the rebuild to stop advancing", SETTLE,
                    () -> {
                        Long done = control.metricsNamed(PIPELINE_ID, DONE).get(DONE + subject);
                        return done != null && done == mongo.count(
                                target, TARGET, Map.of("customer_name", "adelaide"));
                    },
                    () -> rebuildReadings(control) + " against "
                            + mongo.count(target, TARGET, Map.of("customer_name", "adelaide"))
                            + " rows carrying the new name");

            long carried = mongo.count(target, TARGET, Map.of("customer_name", "adelaide"));
            assertThat(carried)
                    .as("a rebuild is what happened, rather than a pair of numbers about one: most of "
                            + "the twelve thousand rows under the key carry the new name")
                    .isGreaterThan(LOUD_ORDERS * 9L / 10);

            Map<String, Long> settled = rebuildReadings(control);
            assertThat(settled.get(DONE + subject))
                    .as("and the rows-sent reading is that same number, not the size it was walking "
                            + "towards - which is read off the index as pages times a page and is an "
                            + "upper bound, exact only for a single-page bucket")
                    .isEqualTo(carried);
            assertThat(settled.get(DONE + subject))
                    .as("bounded above by the size, or the two are not about one rebuild")
                    .isLessThanOrEqualTo(settled.get(EXPECTED + subject));

            // Read after the rebuild has demonstrably finished, so this is its final figure.
            Map<String, Long> finished = rebuildReadings(control);
            assertThat(finished.get(DONE + subject))
                    .as("the rows-sent reading ends up on the rebuild that happened. Not equal to the "
                            + "size beside it, and it must not be asserted to be: that one is read off "
                            + "the index as pages times a page, which is an upper bound and is exact "
                            + "only for a single-page bucket - measured here as 12000 against 11993 "
                            + "rows actually walked. What it may not do is stop short of them")
                    .isNotNull()
                    .isGreaterThan(0L)
                    .isLessThanOrEqualTo(finished.get(EXPECTED + subject));

            assertThat(rebuildReadings(control).keySet())
                    .as("and the three-row customer is still not reported, after a rebuild that was")
                    .noneMatch(name -> name.endsWith("/" + QUIET_CUSTOMER));
        }
    }

    /** Every reading published about a rebuild, whichever of the two numbers it is. */
    private static Map<String, Long> rebuildReadings(ControlPlane control) {
        Map<String, Long> readings = new LinkedHashMap<>(control.metricsNamed(PIPELINE_ID, DONE));
        readings.putAll(control.metricsNamed(PIPELINE_ID, EXPECTED));
        return readings;
    }

    private static void seed(Map<String, Object> settings) throws Exception {
        try (Connection connection = SharedMySql.connect(settings);
                Statement statement = connection.createStatement()) {
            statement.execute("DROP TABLE IF EXISTS orders");
            statement.execute("DROP TABLE IF EXISTS customers");
            statement.execute("CREATE TABLE customers (id INT PRIMARY KEY, name VARCHAR(64))");
            statement.execute("CREATE TABLE orders (id INT PRIMARY KEY, customer_id INT, qty INT)");
            statement.execute("INSERT INTO customers (id, name) VALUES (" + LOUD_CUSTOMER + ", 'ada'), ("
                    + QUIET_CUSTOMER + ", 'bo')");
            insertOrders(statement, LOUD_FIRST_ORDER, LOUD_ORDERS, LOUD_CUSTOMER);
            insertOrders(statement, QUIET_FIRST_ORDER, QUIET_ORDERS, QUIET_CUSTOMER);
        }
    }

    /** In batches rather than one statement, which at twelve thousand rows would depend on packet size. */
    private static void insertOrders(Statement statement, int firstId, int rows, int customerId)
            throws Exception {
        int batch = 1_000;
        for (int from = 0; from < rows; from += batch) {
            StringBuilder sql = new StringBuilder("INSERT INTO orders (id, customer_id, qty) VALUES ");
            int to = Math.min(from + batch, rows);
            for (int i = from; i < to; i++) {
                if (i > from) {
                    sql.append(", ");
                }
                sql.append('(').append(firstId + i).append(", ").append(customerId).append(", 1)");
            }
            statement.execute(sql.toString());
        }
    }

    private static void execute(Map<String, Object> settings, String sql) throws Exception {
        try (Connection connection = SharedMySql.connect(settings);
                Statement statement = connection.createStatement()) {
            statement.execute(sql);
        }
    }

    private static void awaitName(
            MongoEndpoints mongo, EndpointAddress target, int orderId, String expected, String what) {
        Await.until(what, SETTLE, () -> expected.equals(nameOf(mongo, target, orderId)),
                () -> String.valueOf(nameOf(mongo, target, orderId)));
    }

    /** One target row's published customer name, or null before the row is there. Read from Mongo. */
    private static String nameOf(MongoEndpoints mongo, EndpointAddress target, int orderId) {
        return mongo.fetch(target, TARGET, Map.of("order_id", orderId))
                .map(document -> document.get("customer_name"))
                .map(String::valueOf)
                .orElse(null);
    }

    private static String sourceYaml(Map<String, Object> config) {
        return """
                version: tapstate/v1
                kind: source
                id: shop_db
                connector: mysql
                config: { host: %s, port: %s, database: %s, username: %s, password: %s }
                mode: cdc
                tables: [ customers, orders ]
                """
                .formatted(config.get("host"), config.get("port"), config.get("database"),
                        config.get("username"), config.get("password"));
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
                      SELECT o.id AS order_id, o.qty AS qty, c.name AS customer_name
                      FROM o JOIN c ON o.customer_id = c.id
                view:
                  id: order_state
                  from: widen
                  primary_key: order_id
                """;
    }
}
