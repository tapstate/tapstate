package io.tapstate.app;

import com.mongodb.MongoException;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoDatabase;
import io.tapstate.adapters.mongostore.SystemCollections;
import io.tapstate.adapters.mongostore.migration.MigrationRunner;
import io.tapstate.core.lifecycle.RateSample;
import io.tapstate.core.model.SourceResource;
import io.tapstate.core.model.canonical.CanonicalHash;
import io.tapstate.spi.store.ConnectorRegistration;
import io.tapstate.spi.store.StorePort;
import org.bson.Document;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.springframework.boot.builder.SpringApplicationBuilder;
import org.springframework.boot.context.event.ApplicationReadyEvent;
import org.springframework.context.ApplicationListener;
import org.springframework.context.ConfigurableApplicationContext;

import java.net.URI;
import java.net.URISyntaxException;
import java.net.URLDecoder;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.MessageDigest;
import java.time.Duration;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.HexFormat;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

/** Real Atlas system storage and context restart, not Cloud provisioning or user Pipeline evidence. */
class RealManagedAtlasStoreStartupIT {

    private static final String NAMESPACE = "managed-atlas-bootstrap";
    private static final String COLLECTION = "bootstrap_proof";
    private static final String ATLAS_CONNECTOR = "mongodb-atlas";
    private static final String AUTHORED_SOURCE = "authored-proof";
    private static final List<String> CONNECTORS = List.of(
            "mysql", "mongodb", "postgres", "oracle", "sqlserver", "mongodb-atlas", "aws-rds-mysql", "db2");
    private static final byte[] COLD_STATE = "stored-before-restart".getBytes(StandardCharsets.UTF_8);

    @Test
    void cloudBootstrapAndRestartRecoverAtlasStorageAndChangeHistoryRetention(@TempDir Path work) throws Exception {
        String baseUri = System.getenv("TAPSTATE_ATLAS_TEST_URI");
        assumeTrue(baseUri != null && !baseUri.isBlank(), "a controlled Atlas URI is required");
        Path seedDirectory = CloudConnectorTestInputs.seedDirectory();
        String metadata = "ts_plan_cb_" + UUID.randomUUID().toString().substring(0, 8);
        String operator = metadata + "_operator";
        String views = metadata + "_views";
        List<String> databases = List.of(metadata, operator, views);
        for (String database : databases) {
            assertThat(database.getBytes(StandardCharsets.UTF_8).length).isLessThan(38);
        }
        String metadataUri = inDatabase(baseUri, metadata);
        Path plugins = work.resolve("plugins");

        try (MongoClient observer = MongoClients.create(metadataUri)) {
            // A collision is refused before registering cleanup ownership; never drop an existing fixture.
            for (String database : databases) {
                assertThat(observer.getDatabase(database).listCollectionNames().first() == null)
                        .as("the allocated system database is empty before this witness owns it").isTrue();
            }
            try (AutoCloseable cleanup = () -> cleanup(observer, databases)) {
                try {
                String sourceHash;
                List<ConnectorRegistration> registrations;
                Document collectionBefore;
                RateSample sample;
                AtomicInteger firstReady = new AtomicInteger();
                try (ConfigurableApplicationContext first = start(metadataUri, metadata, plugins,
                        seedDirectory, "1h", firstReady)) {
                    StorePort store = readyStore(first, firstReady, operator);
                    store.keyedState().save(NAMESPACE, "key", COLD_STATE);
                    // Source ciphertext is proved with authored fixture config, never with the
                    // deployment's actual Atlas connection. Views database access is observed separately.
                    store.artifacts().create(new SourceResource(AUTHORED_SOURCE, null, "mongodb",
                            Map.of("isUri", true, "uri", "mongodb://source-user:source-secret@user.example/owned"),
                            null, null, null, null));
                    SourceResource authored = authoredSource(store);
                    String viewUri = ViewStoreSeedRunner.viewsUri(metadataUri, views);
                    try (MongoClient materialization = MongoClients.create(viewUri)) {
                        materialization.getDatabase(views).getCollection(COLLECTION)
                                .insertOne(new Document("_id", "before-restart").append("value", 1));
                    }
                    Instant observedAt = Instant.now().minusSeconds(60).truncatedTo(ChronoUnit.MILLIS);
                    sample = new RateSample("bootstrap-proof", observedAt,
                            Map.of("records.out", 3L), Map.of(), observedAt.minusSeconds(60));
                    store.rateHistory().append(sample);
                    assertHistory(observer.getDatabase(metadata), store, sample, Duration.ofHours(1));
                    assertSystemStorage(observer, databases, store, metadataUri);
                    sourceHash = CanonicalHash.of(authored);
                    registrations = assertConnectors(observer.getDatabase(metadata), store, seedDirectory);
                    collectionBefore = historyCollection(observer.getDatabase(metadata));
                }

                AtomicInteger restartedReady = new AtomicInteger();
                try (ConfigurableApplicationContext restarted = start(metadataUri, metadata, plugins,
                        seedDirectory, "30m", restartedReady)) {
                    StorePort store = readyStore(restarted, restartedReady, operator);
                    assertThat(store.keyedState().load(NAMESPACE, "key"))
                            .hasValueSatisfying(bytes -> assertThat(bytes).isEqualTo(COLD_STATE));
                    SourceResource authored = authoredSource(store);
                    assertThat(CanonicalHash.of(authored)).isEqualTo(sourceHash);
                    try (MongoClient materialization = MongoClients.create(ViewStoreSeedRunner.viewsUri(metadataUri, views))) {
                        assertThat(materialization.getDatabase(views).getCollection(COLLECTION)
                                .countDocuments(new Document("_id", "before-restart").append("value", 1)))
                                .isEqualTo(1);
                    }
                    assertHistory(observer.getDatabase(metadata), store, sample, Duration.ofMinutes(30));
                    assertThat(historyCollection(observer.getDatabase(metadata))).isEqualTo(collectionBefore);
                    assertSystemStorage(observer, databases, store, metadataUri);
                    assertThat(assertConnectors(observer.getDatabase(metadata), store, seedDirectory))
                            .containsExactlyInAnyOrderElementsOf(registrations);
                }
                } catch (MongoException failure) {
                    // Sanitize before resource cleanup so its safe failures remain suppressed on this error.
                    throw observationFailure(failure);
                }
            }
        } catch (MongoException failure) {
            // Independent driver observations must not publish connection options or response bodies.
            throw observationFailure(failure);
        }
    }

    private static ConfigurableApplicationContext start(String metadataUri, String cluster, Path plugins,
            Path seedDirectory, String retention, AtomicInteger ready) {
        return new SpringApplicationBuilder(Bootstrap.class).environment(CloudFixtureEnvironment.isolated())
                .listeners((ApplicationListener<ApplicationReadyEvent>) event -> ready.incrementAndGet())
                .run("--server.address=127.0.0.1", "--server.port=0", "--tapstate.hz.member-port=0",
                        "--tapstate.hz.jet.cooperative-thread-count=2", "--logging.level.root=ERROR",
                        "--SDK_STATUS_SENDER_ENABLED=false", "--tapstate.cloud.base-url=https://cloud.example.invalid",
                        "--tapstate.cloud.token=controlled-bootstrap-token", "--tapstate.cloud.atlas-uri=" + metadataUri,
                        "--tapstate.cloud.cluster-id=" + cluster,
                        "--tapstate.store.mongo.uri=mongodb://127.0.0.1:1/onprem_must_not_open",
                        "--tapstate.store.mongo.operator-state-database=onprem_operator_must_not_open",
                        "--tapstate.store.mongo.server-selection-timeout=20s",
                        "--tapstate.connectors.plugins-dir=" + plugins, "--tapstate.connectors.seed-dir=" + seedDirectory,
                        "--tapstate.metrics.history.retention=" + retention);
    }

    private static StorePort readyStore(ConfigurableApplicationContext context, AtomicInteger ready, String operator) {
        assertThat(ready.get()).as("the real Bootstrap context published its ready event").isEqualTo(1);
        assertThat(context.getBean(CloudRuntimeSettings.class).cloud()).isTrue();
        StorePort store = context.getBean(StorePort.class);
        assertThat(store.operatorStateStores().defaultDatabase()).isEqualTo(operator);
        assertThat(store.artifacts().get(ViewTargetResolver.STATE_STORE_SOURCE_ID)).isEmpty();
        return store;
    }

    private static SourceResource authoredSource(StorePort store) {
        var resource = store.artifacts().get(AUTHORED_SOURCE)
                .orElseThrow(() -> new AssertionError("the authored fixture Source is absent"));
        assertThat(resource instanceof SourceResource).as("the authored resource is a Source").isTrue();
        return (SourceResource) resource;
    }

    private static void assertSystemStorage(MongoClient observer, List<String> databases, StorePort store, String metadataUri) {
        MongoDatabase metadata = observer.getDatabase(databases.get(0));
        Document schema = SystemCollections.SYSTEM_META.on(metadata).find(new Document("_id", "schema")).first();
        assertThat(schema != null).as("migration persisted its schema version").isTrue();
        assertThat(schema.getInteger("installedVersion")).isEqualTo(MigrationRunner.SUPPORTED_VERSION);
        Document source = SystemCollections.ARTIFACTS.on(metadata)
                .find(new Document("_id", AUTHORED_SOURCE)).first();
        assertThat(source != null && source.get("body") instanceof Document).as("the authored Source is persisted").isTrue();
        assertThat(SystemCollections.ARTIFACTS.on(metadata)
                .countDocuments(new Document("_id", ViewTargetResolver.STATE_STORE_SOURCE_ID))).isZero();
        Object config = source.get("body", Document.class).get("config");
        assertThat(config instanceof String).as("the whole saved config is a ciphertext string").isTrue();
        String envelope = (String) config;
        assertThat(envelope.startsWith("tscfg:1:")).as("the saved config has its authenticated envelope").isTrue();
        assertThat(envelope).doesNotContain("source-user", "source-secret", "user.example");
        String authority = parsedUri(metadataUri).getRawAuthority();
        int at = authority == null ? -1 : authority.lastIndexOf('@');
        String rawPassword = at < 0 ? null : authority.substring(0, at);
        if (rawPassword != null && rawPassword.contains(":")) {
            rawPassword = rawPassword.substring(rawPassword.indexOf(':') + 1);
            if (!rawPassword.isEmpty()) {
                assertThat(envelope.contains(rawPassword)).as("ciphertext contains no encoded password").isFalse();
                assertThat(envelope.contains(decoded(rawPassword))).as("ciphertext contains no decoded password").isFalse();
            }
        }
        assertThat(CanonicalHash.of(authoredSource(store)).equals(source.getString("contentHash")))
                .as("the persisted Source retains its logical content hash").isTrue();
        assertThat(SystemCollections.OPERATOR_STATE.on(observer.getDatabase(databases.get(1)))
                .countDocuments(new Document("_id.ns", NAMESPACE))).isEqualTo(1);
        assertThat(SystemCollections.OPERATOR_STATE.on(metadata).countDocuments()).isZero();
        assertThat(SystemCollections.ARTIFACTS.on(observer.getDatabase(databases.get(2))).countDocuments()).isZero();
        assertThat(metadata.getCollection(COLLECTION).countDocuments()).isZero();
    }

    private static void assertHistory(MongoDatabase metadata, StorePort store, RateSample sample, Duration retention) {
        assertThat(store.rateHistory().retention()).isEqualTo(retention);
        assertThat(store.rateHistory().readPage(sample.pipelineId(), sample.observedAt().minusSeconds(1),
                sample.observedAt().plusSeconds(1), null, 1).entries()).singleElement()
                .satisfies(entry -> assertThat(entry.sample()).isEqualTo(sample));
        Document row = SystemCollections.PIPELINE_RATE_HISTORY.on(metadata)
                .find(new Document("pipelineId", sample.pipelineId())).first();
        assertThat(row != null && row.get("observedAt") instanceof java.util.Date)
                .as("history uses a BSON Date in metadata storage").isTrue();
        assertThat(row.getDate("observedAt").toInstant()).isEqualTo(sample.observedAt());
        Document ttl = SystemCollections.PIPELINE_RATE_HISTORY.on(metadata).listIndexes().into(new ArrayList<>()).stream()
                .filter(index -> "observedAt_idx".equals(index.getString("name"))).findFirst().orElseThrow();
        assertThat(ttl.get("expireAfterSeconds", Number.class).longValue()).isEqualTo(retention.toSeconds());
    }

    private static Document historyCollection(MongoDatabase metadata) {
        Document collection = metadata.listCollections()
                .filter(new Document("name", SystemCollections.PIPELINE_RATE_HISTORY.collectionName())).first();
        assertThat(collection != null).as("history owns a physical collection").isTrue();
        return collection;
    }

    private static List<ConnectorRegistration> assertConnectors(MongoDatabase metadata, StorePort store, Path seeds) throws Exception {
        List<ConnectorRegistration> registrations = store.connectors().list();
        assertThat(registrations).extracting(ConnectorRegistration::connectorId)
                .containsExactlyInAnyOrderElementsOf(CONNECTORS);
        for (ConnectorRegistration registration : registrations) {
            assertThat(store.connectors().hasArtifact(registration.contentHash())).isTrue();
        }
        ConnectorRegistration atlas = registrations.stream().filter(entry -> ATLAS_CONNECTOR.equals(entry.connectorId()))
                .findFirst().orElseThrow();
        byte[] stored = store.connectors().artifact(atlas.contentHash())
                .orElseThrow(() -> new AssertionError("the persisted Atlas connector bytes are absent"));
        Path seed = seeds.resolve(ATLAS_CONNECTOR + "-connector.jar");
        assertThat(stored.length).isEqualTo(Files.size(seed));
        assertThat(HexFormat.of().formatHex(MessageDigest.getInstance("SHA-256").digest(stored))).isEqualTo(atlas.contentHash());
        MessageDigest expected = MessageDigest.getInstance("SHA-256");
        try (var input = Files.newInputStream(seed)) {
            byte[] buffer = new byte[8192];
            for (int length; (length = input.read(buffer)) != -1;) expected.update(buffer, 0, length);
        }
        assertThat(HexFormat.of().formatHex(expected.digest())).isEqualTo(atlas.contentHash());
        assertThat(metadata.getCollection("connector_artifacts.files").countDocuments()).isEqualTo(CONNECTORS.size());
        assertThat(metadata.getCollection("connector_artifacts.chunks").countDocuments()).isGreaterThan(CONNECTORS.size());
        return registrations;
    }

    private static void cleanup(MongoClient observer, List<String> databases) {
        AssertionError failure = null;
        for (String database : databases) {
            try { observer.getDatabase(database).drop(); }
            catch (MongoException rejected) {
                AssertionError safe = new AssertionError("Could not clean the allocated database " + database
                        + " with driver code " + rejected.getCode());
                if (failure == null) failure = safe;
                else failure.addSuppressed(safe);
            }
        }
        if (failure != null) throw failure;
    }

    private static AssertionError observationFailure(MongoException failure) {
        return new AssertionError("The real Atlas storage observation failed with driver code " + failure.getCode());
    }

    private static String inDatabase(String uri, String database) {
        databaseName(uri);
        int slash = uri.indexOf('/', uri.indexOf("://") + 3);
        int query = uri.indexOf('?', slash);
        return uri.substring(0, slash + 1) + database + (query < 0 ? "" : uri.substring(query));
    }

    private static String databaseName(String uri) {
        String path = parsedUri(uri).getRawPath();
        assertThat(path != null && path.length() > 1 && path.indexOf('/', 1) < 0)
                .as("the controlled connection names one database").isTrue();
        return decoded(path.substring(1));
    }

    private static URI parsedUri(String uri) {
        try {
            URI parsed = new URI(uri);
            assertThat(parsed.getRawFragment() == null).as("the controlled connection has no fragment").isTrue();
            return parsed;
        } catch (URISyntaxException invalid) {
            throw new AssertionError("The controlled Atlas URI has invalid syntax");
        }
    }

    private static String decoded(String value) {
        try { return URLDecoder.decode(value.replace("+", "%2B"), StandardCharsets.UTF_8); }
        catch (IllegalArgumentException invalid) { throw new AssertionError("The controlled URI has an invalid encoded component"); }
    }
}
