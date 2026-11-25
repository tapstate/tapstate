package io.tapstate.e2e;

import com.mongodb.ConnectionString;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.changestream.FullDocumentBeforeChange;
import io.tapstate.core.lifecycle.LifecycleVerb;
import io.tapstate.core.lifecycle.PipelineState;
import io.tapstate.spi.store.ContentHash;
import io.tapstate.testsupport.DockerGate;
import org.bson.Document;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;
import org.testcontainers.containers.MongoDBContainer;
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
        try (var mysql = new MySQLContainer<>(DockerImageName.parse("mysql:8.0"));
                var store = new MongoDBContainer(DockerImageName.parse("mongo:7.0"))
                        .withCommand("--replSet", "docker-rs", "--bind_ip_all", "--setParameter", "enableTestCommands=1")) {
            store.start();
            mysql.start();
            SharedMySql.grantReplication(mysql);
            seedMysqlOrders(mysql, SEEDED_ROWS);

            // One store and one target per tier: sharing them would let a later tier read the rows an
            // earlier one already landed and pass on the first poll without the connector writing a thing.
            String suffix = tier.name().toLowerCase(Locale.ROOT);
            String targetUri = SharedMongo.replicaSetUrl("dropped_column_target_" + suffix);

            Path witness = directory.resolve("writes-" + suffix + ".tsv");
            assertThat(witness).doesNotExist();
            try (PreparedNativeStore storage = PreparedNativeStore.createOn(store); ServerHandle server = tier.launch(storage);
                    MongoEndpoints mongo = new MongoEndpoints()) {
                ControlPlane control = new ControlPlane(server.baseUrl());
                storage.loginAndAwaitBusiness(control);

                byte[] mysqlJar = ConnectorJars.bytesFor("mysql");
                byte[] mongoJar = ConnectorJars.bytesFor("mongodb");
                byte[] observedJar = ObservedMongoConnectorJar.build(mongoJar, witness, "0.0.1");
                control.registerConnector("mysql", mysqlJar);
                control.registerConnector("mongodb", observedJar);

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
                assertThat(connectorHashes(control.pipelineStatus(PIPELINE_ID)))
                        .containsExactlyInAnyOrder(ContentHash.of(mysqlJar), ContentHash.of(observedJar));
                // Register the successor while the first generation still owns the instrumented artifact.
                control.registerConnector("mongodb", mongoJar);
                assertThat(connectorHashes(control.pipelineStatus(PIPELINE_ID)))
                        .containsExactlyInAnyOrder(ContentHash.of(mysqlJar), ContentHash.of(observedJar));
                assertRebuildStatus(control, storage);
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
                control.startAnswering(PIPELINE_ID, "target-not-empty", "keep");
                Await.until("the successor generation to run with the new Mongo artifact",
                        () -> control.state(PIPELINE_ID).filter(PipelineState.RUNNING::equals).isPresent(),
                        () -> String.valueOf(control.pipelineStatus(PIPELINE_ID)));
                assertThat(connectorHashes(control.pipelineStatus(PIPELINE_ID)))
                        .containsExactlyInAnyOrder(ContentHash.of(mysqlJar), ContentHash.of(mongoJar));
            }
        }
    }

    private static void assertRebuildStatus(ControlPlane control, PreparedNativeStore storage) {
        try (var client = MongoClients.create(storage.configurationUri())) {
            var mappings = client.getDatabase(storage.configurationDatabase()).getCollection("operator_generations");
            var key = new Document("_id", PIPELINE_ID);
            var admin = client.getDatabase("admin");
            long entered = admin.runCommand(new Document("configureFailPoint", "failCommand")
                    .append("mode", new Document("times", 1))
                    .append("data", new Document("failCommands", List.of("update"))
                            .append("namespace", storage.cacheDatabase() + ".operator_cleanup")
                            .append("blockConnection", true).append("blockTimeMS", 5000)))
                    .get("count", Number.class).longValue();
            try {
                control.stop(PIPELINE_ID, true);
                admin.runCommand(new Document("waitForFailPoint", "failCommand")
                        .append("timesEntered", entered + 1).append("maxTimeMS", 30000));
                var pending = mappings.find(key).first();
                assertThat(pending).containsEntry("phase", "REBUILDING").containsEntry("stateGeneration", 1L);
                var status = control.pipelineStatus(PIPELINE_ID);
                assertThat(status).containsEntry("displayState", "STOPPING").containsEntry("desiredState", "STOPPED")
                        .doesNotContainKeys("state", "observedState");
                assertThat(connectorHashes(status)).isEmpty();
                assertThat(mappings.find(key).first()).isEqualTo(pending);
            } finally {
                admin.runCommand(new Document("configureFailPoint", "failCommand").append("mode", "off"));
            }
            Await.until("the replacement generation to finish stopping", () -> {
                var mapping = mappings.find(key).first();
                return Long.valueOf(2).equals(mapping.getLong("stateGeneration")) && "ACTIVE".equals(mapping.getString("phase"))
                        && control.state(PIPELINE_ID).filter(PipelineState.STOPPED::equals).isPresent();
            }, () -> String.valueOf(mappings.find(key).first()));
        }
        var stopped = control.pipelineStatus(PIPELINE_ID);
        assertThat(stopped).containsEntry("state", "STOPPED");
        assertThat(connectorHashes(stopped)).isEmpty();
    }

    private static List<String> connectorHashes(Map<String, Object> status) {
        return ((List<?>) status.get("connectorVersions")).stream()
                .map(row -> (String) ((Map<?, ?>) row).get("sha256"))
                .toList();
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
