package io.tapstate.e2e;

import com.mongodb.ConnectionString;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.changestream.FullDocumentBeforeChange;
import io.tapstate.core.lifecycle.LifecycleVerb;
import io.tapstate.core.lifecycle.PipelineState;
import io.tapstate.testsupport.DockerGate;
import org.bson.Document;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;
import org.testcontainers.containers.MySQLContainer;
import org.testcontainers.utility.DockerImageName;

import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.Statement;
import java.sql.Timestamp;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.Calendar;
import java.util.Date;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.TimeZone;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Real connector crossing with a test-only call observer. Declarative document matchers cannot see
 * the TapTable handed to a connector; Mongo documents alone stay correct even with a stale model.
 */
class RealMysqlToMongoDroppedColumnIT {

    @TempDir Path directory;

    private static final long SEEDED_ROWS = 3;
    private static final String TABLE = "orders";
    private static final String TARGET_COLLECTION = "orders";
    private static final String PIPELINE_ID = "mysql2mongo";
    private static final String TIMESTAMP_COLUMN = "created_at";
    private static final Instant CREATED_AT = Instant.parse("2026-08-12T13:08:43.123Z");

    @BeforeAll
    static void requireDockerAndRealConnectors() {
        DockerGate.require();
        RealConnectorGate.require("mysql", "mongodb");
    }

    @ParameterizedTest
    @EnumSource(Tiers.class)
    void every_real_mongo_write_receives_the_projected_model(Tiers tier) throws Exception {
        try (MySQLContainer<?> mysql = new MySQLContainer<>(DockerImageName.parse("mysql:8.0"))) {
            mysql.start();
            SharedMySql.grantReplication(mysql);
            seedMysqlOrders(mysql, SEEDED_ROWS);

            // One store and one target per tier: sharing them would let a later tier read the rows an
            // earlier one already landed and pass on the first poll without the connector writing a thing.
            String suffix = tier.name().toLowerCase(Locale.ROOT);
            String storeUri = SharedMongo.replicaSetUrl("dropped_column_store_" + suffix);
            String targetUri = SharedMongo.replicaSetUrl("dropped_column_target_" + suffix);

            Path witness = directory.resolve("writes-" + suffix + ".tsv");
            assertThat(witness).doesNotExist();
            try (ServerHandle server = tier.launch(storeUri);
                    MongoEndpoints mongo = new MongoEndpoints()) {
                ControlPlane control = new ControlPlane(server.baseUrl());
                control.bootstrapAndLogin("e2e", "e2e-password");

                control.registerConnector("mysql", ConnectorJars.bytesFor("mysql"));
                control.registerConnector("mongodb",
                        ObservedMongoConnectorJar.build(ConnectorJars.bytesFor("mongodb"), witness));

                Map<String, Object> mysqlConfig = mysqlConfig(mysql);
                Map<String, String> resources = new LinkedHashMap<>();
                resources.put("src_mysql.tap.yml", sourceYaml(mysqlConfig));
                resources.put("tgt_mongo.tap.yml", targetYaml(targetUri));
                resources.put("pipeline.tap.yml", pipelineYaml());
                control.apply(resources);

                // The target model and its key are read out of the source's own schema, so discovery has
                // to run before the sink is asked to create a collection and upsert into it.
                control.discoverSchema("src_mysql", "mysql", mysqlConfig);

                try (var client = MongoClients.create(targetUri)) {
                    MongoDatabase database = client.getDatabase(new ConnectionString(targetUri).getDatabase());
                    database.getCollection(TARGET_COLLECTION).drop();
                    database.createCollection(TARGET_COLLECTION);
                    assertThat(database.getCollection(TARGET_COLLECTION).countDocuments()).isZero();
                    database.runCommand(new Document("collMod", TARGET_COLLECTION)
                            .append("changeStreamPreAndPostImages", new Document("enabled", true)));
                    assertPreimageEnabled(database, "before-start", tier);
                }
                control.lifecycle(PIPELINE_ID, LifecycleVerb.START);

                // Dialled as the uri this test already holds: the target is a store it created itself,
                // not a resource it applied and can be handed the settings of.
                EndpointAddress target = EndpointAddress.uri(targetUri);
                awaitCount(mongo, target, TARGET_COLLECTION, SEEDED_ROWS);
                for (int id = 1; id <= SEEDED_ROWS; id++) {
                    assertThat(mongo.fetch(target, TARGET_COLLECTION, Map.of("id", id)).orElseThrow())
                            .containsEntry("id", (long) id).containsEntry("name", "order-" + id)
                            .containsEntry(TIMESTAMP_COLUMN, Date.from(CREATED_AT))
                            .doesNotContainKey("secret");
                }
                control.stop(PIPELINE_ID, true);
                Await.until("pipeline to stop before reading all write attempts",
                        () -> control.state(PIPELINE_ID).filter(PipelineState.STOPPED::equals).isPresent(),
                        () -> String.valueOf(control.state(PIPELINE_ID)));
                assertEveryWriteModel(witness);
                try (var client = MongoClients.create(targetUri)) {
                    MongoDatabase database = client.getDatabase(new ConnectionString(targetUri).getDatabase());
                    assertPreimageEnabled(database, "after-start", tier);
                    // A harness update proves pre-images are active; this is not pipeline CDC.
                    var collection = database.getCollection(TARGET_COLLECTION);
                    try (var cursor = collection.watch()
                            .fullDocumentBeforeChange(FullDocumentBeforeChange.REQUIRED)
                            .maxAwaitTime(1, TimeUnit.SECONDS).cursor()) {
                        assertThat(collection.updateOne(new Document("id", 1),
                                new Document("$set", new Document("name", "probe-updated"))).getMatchedCount())
                                .isEqualTo(1);
                        var change = cursor.tryNext();
                        long deadline = System.nanoTime() + Duration.ofSeconds(10).toNanos();
                        while (change == null && System.nanoTime() < deadline) {
                            change = cursor.tryNext();
                        }
                        assertThat(change).isNotNull();
                        assertThat(change.getFullDocumentBeforeChange()).isNotNull()
                                .containsEntry("name", "order-1").doesNotContainKey("secret");
                    }
                    assertPreimageEnabled(database, "after-direct-update", tier);
                }
            }
        }
    }

    private static void assertEveryWriteModel(Path witness) throws Exception {
        assertThat(witness).isRegularFile();
        List<String> calls = Files.readAllLines(witness);
        assertThat(calls).as("real Mongo write callback must have executed").isNotEmpty();
        long records = 0;
        for (String call : calls) {
            String[] parts = call.split("\\t", -1);
            assertThat(parts).hasSize(3);
            assertThat(parts[0]).as("table passed to the real Mongo callback").isEqualTo(TARGET_COLLECTION);
            int count = Integer.parseInt(parts[1]);
            assertThat(count).isPositive();
            records += count;
            assertThat(parts[2].split(",")).as("fields passed to every real Mongo writeRecord callback")
                    .containsExactly("created_at", "id", "name");
        }
        assertThat(records).as("observed writes cover every seeded source row").isGreaterThanOrEqualTo(SEEDED_ROWS);
    }

    private static void assertPreimageEnabled(MongoDatabase database, String phase, Tiers tier) {
        Document collection = database.listCollections().filter(new Document("name", TARGET_COLLECTION)).first();
        assertThat(collection).as("collection exists %s on %s", phase, tier).isNotNull();
        Document options = collection.get("options", Document.class);
        assertThat(options.get("changeStreamPreAndPostImages", Document.class).getBoolean("enabled"))
                .as("pre-images enabled %s on %s", phase, tier).isTrue();
    }

    /** Reads the target the way a user would, from outside the product, until the rows are all there. */
    private static void awaitCount(
            MongoEndpoints mongo, EndpointAddress target, String collection, long expected) {
        Await.until("all snapshot rows in the real Mongo target",
                () -> mongo.count(target, collection) == expected,
                () -> "rows=" + mongo.count(target, collection));
    }

    private static void seedMysqlOrders(MySQLContainer<?> mysql, long rows) throws Exception {
        try (Connection connection =
                DriverManager.getConnection(mysql.getJdbcUrl(), mysql.getUsername(), mysql.getPassword())) {
            try (Statement statement = connection.createStatement()) {
                statement.execute("SET time_zone = '+00:00'");
                statement.execute("CREATE TABLE " + TABLE + " (id INT PRIMARY KEY, name VARCHAR(64), secret VARCHAR(64), "
                        + TIMESTAMP_COLUMN + " TIMESTAMP(3) NOT NULL)");
            }
            try (PreparedStatement insert =
                    connection.prepareStatement("INSERT INTO " + TABLE + " (id, name, secret, " + TIMESTAMP_COLUMN
                            + " ) VALUES (?, ?, ?, ?)")) {
                for (long id = 1; id <= rows; id++) {
                    insert.setLong(1, id);
                    insert.setString(2, "order-" + id);
                    insert.setString(3, "only-in-source-" + id);
                    insert.setTimestamp(4, Timestamp.from(CREATED_AT), utcCalendar());
                    insert.addBatch();
                }
                insert.executeBatch();
            }
        }
    }

    private static Calendar utcCalendar() {
        return Calendar.getInstance(TimeZone.getTimeZone(ZoneOffset.UTC));
    }

    /**
     * The connection settings the product hands the connector, keyed as its own spec names them. The port
     * is a number, not a string: the connector's config bean holds it as a numeric field, and handing it a
     * string is a cast failure inside the connector's config load.
     */
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
                id: mysql2mongo
                source: src_mysql
                settings: { read_mode: snapshot_and_cdc }
                transforms:
                  - { id: snapshot_rows, from: [orders], type: filter, expr: "op == 'r'" }
                  - { id: without_secret, from: [snapshot_rows], type: map, fields: { secret: false } }
                serve:
                  from: without_secret
                  sync:
                    - source: tgt_mongo
                      rename:
                        map: { orders: orders }
                """;
    }

}
