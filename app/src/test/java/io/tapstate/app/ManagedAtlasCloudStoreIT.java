package io.tapstate.app;

import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import io.tapstate.adapters.mongostore.SystemCollections;
import io.tapstate.core.model.SourceResource;
import io.tapstate.spi.store.OperatorStateStores;
import io.tapstate.spi.store.StorePort;
import org.bson.Document;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.springframework.boot.builder.SpringApplicationBuilder;
import org.springframework.context.ConfigurableApplicationContext;

import java.net.URI;
import java.net.URLDecoder;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

/** Real Atlas witness for the four-value Cloud store, its derived databases, and restart recovery. */
class ManagedAtlasCloudStoreIT {

    private static final String NAMESPACE = "cloud-store-witness";

    @Test
    void cloudMetadataOperatorStateAndViewsStayIsolatedAndRecoverTogether(@TempDir Path work) {
        String baseUri = System.getenv("TAPSTATE_ATLAS_TEST_URI");
        assumeTrue(baseUri != null && !baseUri.isBlank(), "a controlled Atlas URI is required");
        assumeTrue(System.getProperty("tapstate.test.cloud-release-dir") != null,
                "the verified Cloud connector release is required");

        String metadataDatabase = "ts_plan_cloud_" + UUID.randomUUID().toString().substring(0, 12);
        String operatorDatabase = metadataDatabase + "_operator";
        String viewsDatabase = metadataDatabase + "_views";
        String metadataUri = inDatabase(baseUri, metadataDatabase);
        Path plugins = work.resolve("plugins");

        try {
            try (ConfigurableApplicationContext first = start(metadataUri, plugins)) {
                StorePort store = first.getBean(StorePort.class);
                assertThat(first.getBean(OperatorStateStores.class).defaultDatabase())
                        .isEqualTo(operatorDatabase);
                store.keyedState().save(NAMESPACE, "key", "durable-state".getBytes(StandardCharsets.UTF_8));
                SourceResource views = (SourceResource) store.artifacts().get("views").orElseThrow();
                String viewsUri = String.valueOf(views.config().get("uri"));
                assertThat(databaseName(viewsUri)).isEqualTo(viewsDatabase);
                try (MongoClient viewClient = MongoClients.create(viewsUri)) {
                    viewClient.getDatabase(viewsDatabase).getCollection("witness")
                            .insertOne(new Document("_id", "view-row").append("value", 1));
                }
                assertEncryptedAndSeparated(metadataUri, metadataDatabase, operatorDatabase, viewsDatabase);
            }

            try (ConfigurableApplicationContext restarted = start(metadataUri, plugins)) {
                StorePort store = restarted.getBean(StorePort.class);
                assertThat(store.keyedState().load(NAMESPACE, "key"))
                        .hasValueSatisfying(bytes -> assertThat(bytes)
                                .isEqualTo("durable-state".getBytes(StandardCharsets.UTF_8)));
                SourceResource views = (SourceResource) store.artifacts().get("views").orElseThrow();
                String viewsUri = String.valueOf(views.config().get("uri"));
                assertThat(databaseName(viewsUri)).isEqualTo(viewsDatabase);
                try (MongoClient viewClient = MongoClients.create(viewsUri)) {
                    assertThat(viewClient.getDatabase(viewsDatabase).getCollection("witness")
                            .countDocuments(new Document("_id", "view-row"))).isEqualTo(1);
                }
            }
        } finally {
            dropDatabases(metadataUri, metadataDatabase, operatorDatabase, viewsDatabase);
        }
    }

    private static ConfigurableApplicationContext start(String metadataUri, Path plugins) {
        List<String> arguments = new ArrayList<>(List.of(
                "--server.address=127.0.0.1", "--server.port=0",
                "--tapstate.hz.member-port=0", "--tapstate.hz.jet.cooperative-thread-count=2",
                "--logging.level.root=ERROR", "--SDK_STATUS_SENDER_ENABLED=false",
                "--tapstate.cloud.base-url=https://cloud.example.invalid",
                "--tapstate.cloud.token=cloud-store-fixture-token",
                "--tapstate.cloud.atlas-uri=" + metadataUri,
                "--tapstate.cloud.cluster-id=cloud-store-fixture-cluster",
                "--tapstate.store.mongo.uri=mongodb://127.0.0.1:1/onprem_must_not_open",
                "--tapstate.store.mongo.operator-state-database=onprem_operator_must_not_open",
                "--tapstate.store.mongo.server-selection-timeout=20s",
                "--tapstate.connectors.plugins-dir=" + plugins,
                "--tapstate.connectors.seed-dir=" + CloudConnectorTestInputs.seedDirectory()));
        return new SpringApplicationBuilder(Bootstrap.class).environment(CloudFixtureEnvironment.isolated())
                .run(arguments.toArray(String[]::new));
    }

    private static void assertEncryptedAndSeparated(
            String metadataUri, String metadataDatabase, String operatorDatabase, String viewsDatabase) {
        try (MongoClient raw = MongoClients.create(metadataUri)) {
            Document storedViews = raw.getDatabase(metadataDatabase)
                    .getCollection(SystemCollections.ARTIFACTS.collectionName())
                    .find(new Document("_id", "views")).first();
            assertThat(storedViews).isNotNull();
            assertThat(storedViews.get("body", Document.class).get("config"))
                    .isInstanceOf(String.class).asString().startsWith("tscfg:1:");
            assertThat(raw.getDatabase(operatorDatabase)
                    .getCollection(SystemCollections.OPERATOR_STATE.collectionName())
                    .countDocuments(new Document("_id.ns", NAMESPACE))).isEqualTo(1);
            assertThat(raw.getDatabase(metadataDatabase)
                    .getCollection(SystemCollections.OPERATOR_STATE.collectionName()).countDocuments()).isZero();
            assertThat(raw.getDatabase(viewsDatabase)
                    .getCollection(SystemCollections.ARTIFACTS.collectionName()).countDocuments()).isZero();
        }
    }

    private static void dropDatabases(
            String metadataUri, String metadataDatabase, String operatorDatabase, String viewsDatabase) {
        try (MongoClient cleanup = MongoClients.create(metadataUri)) {
            cleanup.getDatabase(metadataDatabase).drop();
            cleanup.getDatabase(operatorDatabase).drop();
            cleanup.getDatabase(viewsDatabase).drop();
            assertThat(cleanup.listDatabaseNames().into(new ArrayList<>()))
                    .doesNotContain(metadataDatabase, operatorDatabase, viewsDatabase);
        }
    }

    private static String databaseName(String uri) {
        String path = URI.create(uri).getRawPath();
        if (path == null || path.length() <= 1 || path.indexOf('/', 1) >= 0) {
            throw new AssertionError("managed store URI does not name one database");
        }
        return URLDecoder.decode(path.substring(1).replace("+", "%2B"), StandardCharsets.UTF_8);
    }

    private static String inDatabase(String uri, String database) {
        int schemeEnd = uri.indexOf("://");
        int slash = schemeEnd < 0 ? -1 : uri.indexOf('/', schemeEnd + 3);
        if (slash < 0) throw new IllegalArgumentException("Atlas test URI must include a database path");
        int options = uri.indexOf('?', slash);
        return uri.substring(0, slash + 1) + database + (options < 0 ? "" : uri.substring(options));
    }
}
