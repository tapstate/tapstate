package io.tapstate.e2e;

import io.tapstate.core.lifecycle.LifecycleVerb;
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
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * The witness that a declared view materializes into a real store and that the collection it creates
 * carries the index it is read by.
 *
 * <p>The declarative examples prove the rows arrive, but a synthetic file connector has no indexes to
 * speak of, so nothing there can say whether the index was created. Only a real store can answer that,
 * and it has to be asked directly: the assertion reads Mongo's own index list rather than anything the
 * product reports, because an index the product believes in and never created is the failure being
 * looked for.
 *
 * <p><strong>Why the index matters enough to hold a container for.</strong> A reader paging the
 * collection needs a sort key that is indexed and unique. Missing, the sort is held in memory and fails
 * only once the data outgrows it - so it passes at every size a demo or a test reaches and breaks in
 * front of a user. Non-unique, a range start is ambiguous and pages silently skip or repeat rows.
 * Neither failure is visible from row counts, which is why this asserts the index itself rather than
 * inferring it from a query that happens to work.
 *
 * <p>No serve block anywhere in the pipeline: the view is the whole instruction, and the collection it
 * lands in is named for the view rather than for the source table.
 * The snapshot-and-CDC source still needs replication privileges even though the transform admits
 * only snapshot rows into the view.
 *
 * <p>Gated on Docker and on a directory of real connector jars, exactly like its siblings. Naming no
 * directory skips it, so the default build stays green; naming one whose jars do not resolve fails
 * rather than skips. Run it with:
 *
 * <pre>
 *   mvn -pl e2e -am verify -Dapi.version=1.44 \
 *     -Dtapstate.e2e.connectors-dir=/path/to/connectors
 * </pre>
 */
class RealMysqlToMongoViewIndexIT {

    private static final Duration TIMEOUT = Duration.ofSeconds(60);
    private static final Duration POLL = Duration.ofMillis(250);
    private static final long SEEDED_ROWS = 5;
    private static final String TABLE = "orders";
    private static final String KEY_COLUMN = "id";
    /** Declared under storage.warm.indexes, so the store should carry it and it should not be unique. */
    private static final String DECLARED_INDEX_COLUMN = "name";
    /** The view's id, which is also the collection it materializes into when it declares no storage. */
    private static final String VIEW_ID = "order_state";
    private static final String PIPELINE_ID = "mysql2view";

    @BeforeAll
    static void requireDockerAndRealConnectors() {
        DockerGate.require();
        RealConnectorGate.require("mysql", "mongodb");
    }

    @ParameterizedTest
    @EnumSource(Tiers.class)
    void aDeclaredViewMaterializesAndCarriesItsKeyIndex(Tiers tier) throws Exception {
        try (MySQLContainer<?> mysql = new MySQLContainer<>(DockerImageName.parse("mysql:8.0"))) {
            mysql.start();
            SharedMySql.grantReplication(mysql);
            seedMysqlOrders(mysql, SEEDED_ROWS);

            String suffix = tier.name().toLowerCase(Locale.ROOT);
            String storeUri = SharedMongo.replicaSetUrl("real_view_store_" + suffix);
            String targetUri = SharedMongo.replicaSetUrl("real_view_target_" + suffix);

            try (ServerHandle server = tier.launch(storeUri);
                    MongoEndpoints mongo = new MongoEndpoints()) {
                ControlPlane control = new ControlPlane(server.baseUrl());
                control.bootstrapAndLogin("e2e", "e2e-password");

                control.registerConnector("mysql", ConnectorJars.bytesFor("mysql"));
                control.registerConnector("mongodb", ConnectorJars.bytesFor("mongodb"));

                Map<String, Object> mysqlConfig = mysqlConfig(mysql);
                Map<String, String> resources = new LinkedHashMap<>();
                resources.put("src_mysql.tap.yml", sourceYaml(mysqlConfig));
                // The store a view lands in is the deployment's rather than the pipeline's, so it is
                // registered under the id the product resolves for every view - the pipeline never names it.
                resources.put("views.tap.yml", stateStoreYaml(targetUri));
                resources.put("pipeline.tap.yml", pipelineYaml());
                control.apply(resources);

                control.discoverSchema("src_mysql", "mysql", mysqlConfig);
                control.lifecycle(PIPELINE_ID, LifecycleVerb.START);

                EndpointAddress target = EndpointAddress.uri(targetUri);
                awaitCount(mongo, target, VIEW_ID, SEEDED_ROWS);

                List<Document> indexes = mongo.indexes(target, VIEW_ID);
                assertThat(indexes)
                        .as("indexes Mongo itself reports on the materialized view %s", VIEW_ID)
                        .anySatisfy(index -> {
                            assertThat(index.get("key", Document.class)).containsKey(KEY_COLUMN);
                            assertThat(index.getBoolean("unique", false))
                                    .as("the key index must be unique, or it cannot be paged by")
                                    .isTrue();
                        });

                // The declared index, held to the same standard as the key one: read from what Mongo
                // reports, not from what the product recorded. Until this ran, storage.warm.indexes was
                // covered only by a unit test of the resolver - which says what the product intends to
                // ask for, and would keep saying it if nothing ever asked.
                assertThat(indexes)
                        .as("the index declared under storage.warm.indexes, as Mongo reports it")
                        .anySatisfy(index -> {
                            assertThat(index.get("key", Document.class)).containsKey(DECLARED_INDEX_COLUMN);
                            assertThat(index.getBoolean("unique", false))
                                    .as("a declared index is an index, not a uniqueness constraint - "
                                            + "making it unique would refuse rows the source allows")
                                    .isFalse();
                        });
            }
        }
    }

    /** Reads the target the way a user would, from outside the product, until the rows are all there. */
    private static void awaitCount(MongoEndpoints mongo, EndpointAddress target, String collection, long expected) {
        long deadline = System.nanoTime() + TIMEOUT.toNanos();
        long last = -1;
        while (System.nanoTime() - deadline < 0) {
            last = mongo.count(target, collection);
            if (last == expected) {
                return;
            }
            sleep();
        }
        assertThat(last)
                .as("rows materialized into the view %s from %d real MySQL rows", collection, expected)
                .isEqualTo(expected);
    }

    private static void seedMysqlOrders(MySQLContainer<?> mysql, long rows) throws Exception {
        try (Connection connection =
                DriverManager.getConnection(mysql.getJdbcUrl(), mysql.getUsername(), mysql.getPassword())) {
            try (Statement statement = connection.createStatement()) {
                statement.execute("CREATE TABLE " + TABLE + " (id INT PRIMARY KEY, name VARCHAR(64))");
            }
            try (PreparedStatement insert =
                    connection.prepareStatement("INSERT INTO " + TABLE + " (id, name) VALUES (?, ?)")) {
                for (long id = 1; id <= rows; id++) {
                    insert.setLong(1, id);
                    insert.setString(2, "order-" + id);
                    insert.addBatch();
                }
                insert.executeBatch();
            }
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

    private static String sourceYaml(Map<String, Object> config) {
        return """
                version: tapstate/v1
                kind: source
                id: src_mysql
                connector: mysql
                config: { host: %s, port: %s, database: %s, username: %s, password: %s }
                mode: cdc
                tables: [ orders ]
                """
                .formatted(
                        config.get("host"),
                        config.get("port"),
                        config.get("database"),
                        config.get("username"),
                        config.get("password"));
    }

    private static String stateStoreYaml(String targetUri) {
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
                id: mysql2view
                source: src_mysql
                settings: { read_mode: snapshot_and_cdc }
                transforms:
                  - { id: snapshot_rows, from: [orders], type: filter, expr: "op == 'r'" }
                view:
                  id: order_state
                  from: snapshot_rows
                  primary_key: id
                  storage:
                    warm:
                      collection: order_state
                      indexes: [ name ]
                """;
    }

    private static void sleep() {
        try {
            Thread.sleep(POLL.toMillis());
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new AssertionError("interrupted while waiting for the view to materialize", e);
        }
    }
}
