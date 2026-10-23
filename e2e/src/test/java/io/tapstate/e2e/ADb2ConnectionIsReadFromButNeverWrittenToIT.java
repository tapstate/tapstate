package io.tapstate.e2e;

import com.mongodb.client.MongoClients;
import io.tapstate.adapters.mongostore.SystemCollections;
import io.tapstate.core.catalog.TapstateCatalog;
import io.tapstate.testsupport.DockerGate;

import org.bson.Document;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Both Db2 previews can be read but cannot be sync targets, including after editing a stored target
 * or loading a legacy writable catalog row. The server applies the source-only policy before it
 * connects, so these cases need neither a live Db2 system nor a connector JAR.
 */
class ADb2ConnectionIsReadFromButNeverWrittenToIT {

    private static final String UNSUPPORTED_TARGET_CONNECTOR = "dsl.unsupported-target-connector";

    private static final String DB2_SOURCE = """
            version: tapstate/v1
            kind: source
            id: src_db2
            connector: %s
            config: %s
            mode: cdc
            tables: [ ORDERS ]
            """;

    private static final String MONGO_TARGET = """
            version: tapstate/v1
            kind: source
            id: tgt_mongo
            connector: mongodb
            config: { uri: "mongodb://10.30.0.11:27017/ods" }
            """;

    private static final String READ_FROM_DB2 = """
            version: tapstate/v1
            kind: pipeline
            id: orders_from_db2
            source: src_db2
            serve:
              from: ORDERS
              sync: [ { id: out, source: tgt_mongo, write_mode: upsert } ]
            """;

    private static final String MYSQL_SOURCE = """
            version: tapstate/v1
            kind: source
            id: src_orders
            connector: mysql
            config: { host: 10.10.0.5, username: u, password: p }
            mode: cdc
            tables: [ orders ]
            """;

    private static final String DB2_TARGET = """
            version: tapstate/v1
            kind: source
            id: tgt_db2
            connector: %s
            config: %s
            """;

    /** The connection the installed pipeline writes to, re-applied on its own with Db2 underneath. */
    private static final String MONGO_TARGET_MOVED_TO_DB2 = """
            version: tapstate/v1
            kind: source
            id: tgt_mongo
            connector: %s
            config: %s
            """;

    private static final String WRITE_INTO_DB2 = """
            version: tapstate/v1
            kind: pipeline
            id: orders_into_db2
            source: src_orders
            serve:
              from: orders
              sync: [ { id: out, source: tgt_db2, write_mode: upsert } ]
            """;

    private static String sourceConfig(String connector) {
        return "db2i".equals(connector)
                ? "{ host: 10.10.0.7, database: ORDERSLIB, journalLibraryName: ORDERSLIB, journalName: QSQJRN }"
                : "{ host: 10.10.0.7, port: 50000, database: SAMPLE, schema: APP, rawLogServerHost: 10.10.0.8 }";
    }

    private static String targetConfig(String connector) {
        return "db2i".equals(connector)
                ? "{ host: 10.30.0.9, database: ODSLIB }"
                : "{ host: 10.30.0.9, port: 50000, database: SAMPLE, schema: APP }";
    }

    @BeforeAll
    static void requireDocker() {
        DockerGate.require();
    }

    @ParameterizedTest(name = "{0}")
    @ValueSource(strings = {"db2", "db2i"})
    void aStoredWritableDb2RowFromAnEarlierReleaseCannotEnableASyncTarget(String connector) throws Exception {
        try (PreparedNativeStore storage = PreparedNativeStore.create();
                NativeSrsDocuments documents = new NativeSrsDocuments(storage)) {
            // Seed the persisted shape directly: the running release must not derive the legacy fixture.
            try (var resource = TapstateCatalog.class.getResourceAsStream("/catalog/" + connector + ".json");
                    var client = MongoClients.create(storage.cacheUri())) {
                Document legacy = Document.parse(new String(resource.readAllBytes(), StandardCharsets.UTF_8));
                legacy.put("_id", connector);
                var scope = documents.scope();
                legacy.put("cacheScope", new Document("clusterId", scope.clusterId())
                        .append("bindingVersion", scope.bindingVersion()).append("cacheIncarnation", scope.cacheIncarnation()));
                legacy.put("sink", new Document("capable", true)
                        .append("writeSemantics", List.of("upsert", "append")));
                SystemCollections.CACHE_CONNECTOR_CATALOG.on(client.getDatabase(storage.cacheDatabase())).insertOne(legacy);
            }

            try (ServerHandle server = Tiers.IN_PROCESS.launch(storage)) {
                ControlPlane control = new ControlPlane(server.baseUrl());
                storage.loginAndAwaitBusiness(control);
                control.apply(Map.of(
                        "src_db2.tap.yml", DB2_SOURCE.formatted(connector, sourceConfig(connector)),
                        "tgt_mongo.tap.yml", MONGO_TARGET,
                        "orders_from_db2.tap.yml", READ_FROM_DB2));
                assertThat(control.artifactIds()).contains("orders_from_db2", "src_db2", "tgt_mongo");

                ControlPlane.Refusal refusal = control.applyExpectingRefusal(Map.of(
                        "src_orders.tap.yml", MYSQL_SOURCE,
                        "tgt_db2.tap.yml", DB2_TARGET.formatted(connector, targetConfig(connector)),
                        "orders_into_db2.tap.yml", WRITE_INTO_DB2));

                assertThat(refusal.code()).isEqualTo(UNSUPPORTED_TARGET_CONNECTOR);
                assertThat(refusal.params()).containsEntry("connector", connector).containsEntry("source", "tgt_db2");
                assertThat(control.artifactIds()).doesNotContain("orders_into_db2", "tgt_db2", "src_orders");
                // Reading current support policy must not rewrite the stored document or require re-registration.
                try (var client = MongoClients.create(storage.cacheUri())) {
                    Document persisted = SystemCollections.CACHE_CONNECTOR_CATALOG.on(client.getDatabase(storage.cacheDatabase()))
                            .find(new Document("_id", connector)).first();
                    assertThat(persisted.get("sink", Document.class).getBoolean("capable")).isTrue();
                }
            }
        }
    }

    @ParameterizedTest(name = "{0}")
    @ValueSource(strings = {"db2", "db2i"})
    void aPipelineReadingDb2IsInstalledAndOneWritingIntoDb2IsRefused(String connector) {
        try (PreparedNativeStore storage = PreparedNativeStore.create();
                ServerHandle server = Tiers.IN_PROCESS.launch(storage)) {
            ControlPlane control = new ControlPlane(server.baseUrl());
            storage.loginAndAwaitBusiness(control);

            control.apply(Map.of(
                    "src_db2.tap.yml", DB2_SOURCE.formatted(connector, sourceConfig(connector)),
                    "tgt_mongo.tap.yml", MONGO_TARGET,
                    "orders_from_db2.tap.yml", READ_FROM_DB2));

            assertThat(control.artifactIds())
                    .as("a pipeline reading a db2 connection is installed")
                    .contains("orders_from_db2", "src_db2", "tgt_mongo");

            ControlPlane.Refusal refusal = control.applyExpectingRefusal(Map.of(
                    "src_orders.tap.yml", MYSQL_SOURCE,
                    "tgt_db2.tap.yml", DB2_TARGET.formatted(connector, targetConfig(connector)),
                    "orders_into_db2.tap.yml", WRITE_INTO_DB2));

            assertThat(refusal.code())
                    .as("the code refusing a sync onto a source-only connector")
                    .isEqualTo(UNSUPPORTED_TARGET_CONNECTOR);
            assertThat(refusal.params())
                    .as("the connector and the connection naming it are what send the author to the fix")
                    .containsEntry("connector", connector)
                    .containsEntry("source", "tgt_db2");
            assertThat(control.artifactIds())
                    .as("what the server holds after refusing the batch")
                    .doesNotContain("orders_into_db2", "tgt_db2", "src_orders");

            String installedTarget = control.contentHash("tgt_mongo");
            ControlPlane.Refusal moved = control.applyExpectingRefusal(Map.of(
                    "tgt_mongo.tap.yml", MONGO_TARGET_MOVED_TO_DB2.formatted(connector, targetConfig(connector))));

            assertThat(moved.code())
                    .as("the code refusing an edit that would leave an installed sync writing into db2")
                    .isEqualTo(UNSUPPORTED_TARGET_CONNECTOR);
            assertThat(moved.params())
                    .as("the pipeline that would write into it is named, not only the connection")
                    .containsEntry("connector", connector)
                    .containsEntry("source", "tgt_mongo")
                    .containsEntry("resource", "orders_from_db2");
            assertThat(control.contentHash("tgt_mongo"))
                    .as("the connection the installed pipeline writes to, after the refused edit")
                    .isEqualTo(installedTarget);
        }
    }
}
