package io.tapstate.e2e;

import io.tapstate.core.lifecycle.LifecycleVerb;
import io.tapstate.testsupport.DockerGate;

import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.MySQLContainer;
import org.testcontainers.utility.DockerImageName;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.Statement;
import java.time.Duration;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * A metric broken down by chain or by nest namespace carries that dimension as an attribute of its points,
 * and its name says nothing about which chain or namespace a point is about. Read off the metrics face of
 * a real run, over the wire, which is where a monitoring backend will read it.
 *
 * <p>What this discriminates is the migration done as a rename: a family that kept appending its dimension
 * to a prefix, under a new prefix, would still be a name per value that nothing can group or filter. Two
 * assertions catch it from either side, and each would be red on the shape this replaced: a point is
 * selected <em>by</em> the attribute, which a family with the dimension in its name cannot answer; and the
 * name is shown to carry no chain and no namespace, which a family with the dimension in its name cannot
 * pass. The flat face beside the facts still spells the same reading under the key it always used, and
 * the value under that key is the value on the point - so the two faces are one measurement, not two.
 *
 * <p>A nest over two real sources is the smallest run that has both: two chains, and a namespace per level
 * of the nest. The rebuild readings of a join carry their dimension the same way and are witnessed where a
 * rebuild actually happens, in the large-rebuild specification.
 *
 * <pre>
 *   mvn -pl e2e -am verify -Dapi.version=1.44 \
 *     -Dtapstate.e2e.connectors-dir=/path/to/connectors \
 *     -Dit.test=AttributesAreAttributesNotNameSuffixesIT \
 *     -Dfailsafe.failIfNoSpecifiedTests=false
 * </pre>
 */
class AttributesAreAttributesNotNameSuffixesIT {

    private static final Duration SETTLE = Duration.ofSeconds(120);
    private static final String PIPELINE_ID = "attributes_are_attributes";
    private static final String PARENT_TABLE = "orders";
    private static final String CHILD_TABLE = "order_items";
    private static final String NEST_STEP = "order_doc";

    private static final String FRONTIER_GAP = "tapstate.pipeline.frontier.gap";
    private static final String NEST_ENTRIES = "tapstate.pipeline.nest.entries";
    private static final String CHAIN = "tapstate.chain.id";
    private static final String NAMESPACE = "tapstate.nest.namespace";

    /** How the flat face spells the same two readings, which every reader of that face was built against. */
    private static final String FRONTIER_GAP_FLAT = "frontierGap.";
    private static final String NEST_ENTRIES_FLAT = "nestStateEntries.";

    /**
     * The prefixes the families carried their dimension under before it became an attribute. None of them
     * is a fact any more: a fact under one of these would be the migration done as a rename.
     */
    private static final List<String> RETIRED_FACT_PREFIXES = List.of(
            "frontierGap.", "frontierStalledMillis.", "nestStateEntries.", "nestStateAccesses.",
            "nestStateBackfills.", "nestStateBackfillMillis.", "nestStatePendingHighWater.",
            "nestStateStored.", "nestDeadLettered.", "joinRecomputeRowsDone.", "joinRecomputeRowsExpected.");

    @BeforeAll
    static void requireDockerAndRealConnectors() {
        DockerGate.require();
        RealConnectorGate.require("mysql", "mongodb");
    }

    @Test
    void aChainAndANamespaceAreSelectedByAttributeAndAppearInNoName() throws Exception {
        try (MySQLContainer<?> mysql = new MySQLContainer<>(DockerImageName.parse("mysql:8.0"))) {
            mysql.start();
            seed(mysql);
            grantReplication(mysql);

            String storeUri = SharedMongo.replicaSetUrl("attributes_store");
            String targetUri = SharedMongo.replicaSetUrl("attributes_target");

            try (ServerHandle server = Tiers.IN_PROCESS.launch(storeUri)) {
                ControlPlane control = new ControlPlane(server.baseUrl());
                control.bootstrapAndLogin("e2e", "e2e-password");
                control.registerConnector("mysql", ConnectorJars.bytesFor("mysql"));
                control.registerConnector("mongodb", ConnectorJars.bytesFor("mongodb"));

                Map<String, Object> config = mysqlConfig(mysql);
                Map<String, String> resources = new LinkedHashMap<>();
                resources.put("src_orders.tap.yml", sourceYaml("src_orders", PARENT_TABLE, config));
                resources.put("src_items.tap.yml", sourceYaml("src_items", CHILD_TABLE, config));
                resources.put("tgt_mongo.tap.yml", targetYaml(targetUri));
                resources.put("pipeline.tap.yml", pipelineYaml());
                control.apply(resources);
                control.discoverSchema("src_orders", "mysql", config);
                control.discoverSchema("src_items", "mysql", config);
                control.lifecycle(PIPELINE_ID, LifecycleVerb.START);

                // Both families arrive with the first collection of the running job's statistics, and not
                // before: a run that has not published either is waited out, never read as "no attributes".
                Await.until("the frontier and nest readings of the running job to be published", SETTLE,
                        () -> fact(control, FRONTIER_GAP).isPresent() && fact(control, NEST_ENTRIES).isPresent(),
                        () -> control.metrics(PIPELINE_ID));

                Map<String, Object> gap = fact(control, FRONTIER_GAP).orElseThrow();
                assertThat(pointWhere(gap, CHAIN, PARENT_TABLE))
                        .as("the frontier distance of the orders chain is selected by its chain attribute; "
                                + "a family that still spelled the chain into its name has no point to select")
                        .isPresent();
                Map<String, Object> ordersPoint = pointWhere(gap, CHAIN, PARENT_TABLE).orElseThrow();
                assertThat((String) gap.get("name"))
                        .as("and the name says which quantity this is, not which chain: a name carrying the "
                                + "chain is one name per chain, which nothing can group or filter")
                        .doesNotContain(PARENT_TABLE)
                        .doesNotContain(CHILD_TABLE);
                assertThat(pointWhere(gap, CHAIN, CHILD_TABLE))
                        .as("the second chain is another point of the same fact, not another fact")
                        .isPresent();

                Map<String, Object> entries = fact(control, NEST_ENTRIES).orElseThrow();
                String rootNamespace = "nest." + PIPELINE_ID + "." + NEST_STEP + ".";
                Map<String, Object> rootPoint = ((List<Map<String, Object>>) entries.get("points")).stream()
                        .filter(point -> attributeOf(point, NAMESPACE).map(ns -> ns.startsWith(rootNamespace))
                                .orElse(false))
                        .findFirst()
                        .orElseThrow(() -> new AssertionError(
                                "no nest entries point carries a namespace of this pipeline's nest step: "
                                        + entries));
                assertThat((String) entries.get("name"))
                        .as("the nest level a point is about is on the point, and the name carries none of it")
                        .doesNotContain(PIPELINE_ID)
                        .doesNotContain(NEST_STEP);

                // The flat face is untouched by the migration: the same readings sit under the keys every
                // reader of that face was built against, and each carries the number that is on the point.
                Map<String, Long> flatGaps = control.metricsNamed(PIPELINE_ID, FRONTIER_GAP_FLAT);
                assertThat(flatGaps)
                        .as("the flat face still spells the orders chain's distance under its old key")
                        .containsEntry(FRONTIER_GAP_FLAT + PARENT_TABLE, ((Number) ordersPoint.get("value")).longValue());
                String rootNamespaceValue = attributeOf(rootPoint, NAMESPACE).orElseThrow();
                assertThat(control.metricsNamed(PIPELINE_ID, NEST_ENTRIES_FLAT))
                        .as("and the nest level's entries under its old key, with the value that is on the point")
                        .containsEntry(NEST_ENTRIES_FLAT + rootNamespaceValue,
                                ((Number) rootPoint.get("value")).longValue());

                assertThat(control.metricFacts(PIPELINE_ID))
                        .extracting(fact -> (String) fact.get("name"))
                        .as("no fact is left under a prefix that spelled its dimension into its name")
                        .noneMatch(name -> RETIRED_FACT_PREFIXES.stream().anyMatch(name::startsWith));
            }
        }
    }

    /** The fact named {@code name} on the metrics face right now, or empty while it is not published. */
    private static Optional<Map<String, Object>> fact(ControlPlane control, String name) {
        return control.metricFacts(PIPELINE_ID).stream()
                .filter(fact -> name.equals(fact.get("name")))
                .findFirst();
    }

    /** The point of {@code fact} whose attribute {@code key} is {@code value}, selected by that attribute alone. */
    @SuppressWarnings("unchecked")
    private static Optional<Map<String, Object>> pointWhere(Map<String, Object> fact, String key, String value) {
        return ((List<Map<String, Object>>) fact.get("points")).stream()
                .filter(candidate -> attributeOf(candidate, key).map(value::equals).orElse(false))
                .findFirst();
    }

    @SuppressWarnings("unchecked")
    private static Optional<String> attributeOf(Map<String, Object> point, String key) {
        Object attributes = point.get("attributes");
        if (!(attributes instanceof Map<?, ?> shaped)) {
            return Optional.empty();
        }
        Object value = ((Map<String, Object>) shaped).get(key);
        return value instanceof String string ? Optional.of(string) : Optional.empty();
    }

    /** Two orders with a couple of items: enough for a root and an embedded level to hold something. */
    private static void seed(MySQLContainer<?> mysql) throws Exception {
        try (Connection connection = DriverManager.getConnection(
                        mysql.getJdbcUrl(), mysql.getUsername(), mysql.getPassword());
                Statement statement = connection.createStatement()) {
            statement.execute("CREATE TABLE " + PARENT_TABLE + " (id INT PRIMARY KEY, name VARCHAR(64))");
            statement.execute("CREATE TABLE " + CHILD_TABLE
                    + " (id INT PRIMARY KEY, order_id INT, sku VARCHAR(64))");
            statement.execute("INSERT INTO " + PARENT_TABLE + " (id, name) VALUES (1, 'order-1'), (2, 'order-2')");
            statement.execute("INSERT INTO " + CHILD_TABLE
                    + " (id, order_id, sku) VALUES (1, 1, 'sku-1'), (2, 1, 'sku-2'), (3, 2, 'sku-3')");
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

    /** One capture unit per table: two single-table sources are what puts two chains into one job. */
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
                .formatted(id, config.get("host"), config.get("port"), config.get("database"),
                        config.get("username"), config.get("password"), table);
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

    private static String pipelineYaml() {
        return """
                version: tapstate/v1
                kind: pipeline
                id: %s
                source: [ src_orders, src_items ]
                settings: { read_mode: snapshot_and_cdc }
                transforms:
                  - id: %s
                    type: nest
                    from: { o: orders, i: order_items }
                    root:
                      from: o
                      key: [ id ]
                      embed:
                        - { from: i, on: { order_id: id }, as: array, path: items, arrayKey: [ id ] }
                serve:
                  from: %s
                  sync:
                    - source: tgt_mongo
                """
                .formatted(PIPELINE_ID, NEST_STEP, NEST_STEP);
    }
}
