package io.tapstate.app;

import io.tapstate.testsupport.JvmTestEnvironment;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.mongodb.client.MongoClients;
import com.sun.net.httpserver.HttpServer;
import io.tapstate.adapters.mongostore.SystemCollections;
import io.tapstate.adapters.mongostore.MongoConnectionSettings;
import io.tapstate.adapters.mongostore.MongoConfigurationDomain;
import io.tapstate.adapters.mongostore.StoreError;
import io.tapstate.spi.store.StorageBinding;
import io.tapstate.control.restapi.RuntimeVersion;
import io.tapstate.core.model.Metadata;
import io.tapstate.core.model.SourceResource;
import io.tapstate.spi.store.ArtifactMutation;
import io.tapstate.spi.store.ContentHash;
import io.tapstate.testsupport.RequiresDocker;
import org.bson.Document;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.EnumSource;
import org.junit.jupiter.params.provider.MethodSource;
import org.testcontainers.containers.MongoDBContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.utility.DockerImageName;

import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.ArrayList;
import java.util.EnumMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.LockSupport;
import java.util.regex.Pattern;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;

/** Exercises external configuration through the shipped JAR in a separate, real JVM process. */
@RequiresDocker
class CloudExternalConfigStartupIT {

    private static final String TOKEN = "cloud-startup-token-sentinel";
    private static final String CLUSTER = "external-config-cluster";
    private static final String READY = "Tapstate application is ready";
    private static final String PASSWORD = NativeAppStorage.PASSWORD;
    private static final Set<String> CONNECTORS = Set.of(
            "mysql", "mongodb", "postgres", "oracle", "sqlserver", "mongodb-atlas", "aws-rds-mysql", "db2");
    private static final ObjectMapper JSON = new ObjectMapper();
    private static final Pattern HTTP_PORT = Pattern.compile("Tomcat started on port (\\d+)");
    private static final HttpClient HTTP = HttpClient.newBuilder()
            .connectTimeout(Duration.ofSeconds(1)).build();

    @Container
    private static final MongoDBContainer MONGO =
            new MongoDBContainer(DockerImageName.parse("mongo:7.0"));

    @TempDir static Path fixtureDirectory;
    private static final Map<Distribution, Path> JARS = new EnumMap<>(Distribution.class);

    @TempDir
    Path work;
    private final List<Running> processes = new ArrayList<>();
    private final AtomicInteger cloudRequests = new AtomicInteger();
    private final ConcurrentLinkedQueue<String> cloudPaths = new ConcurrentLinkedQueue<>();
    private final ConcurrentLinkedQueue<StatusReport> statusReports = new ConcurrentLinkedQueue<>();
    private final String operatorDatabase = uniqueDatabase("operator_state");
    private HttpServer cloud;

    enum Carrier { FILE, ENVIRONMENT, JVM }

    enum Entry { SERVER, MIGRATE }

    enum Distribution {
        CLOUD("cloud"), ON_PREM("onprem"), MISSING(null), UNKNOWN("unsupported");

        private final String profile;

        Distribution(String profile) {
            this.profile = profile;
        }
    }

    enum Mismatch {
        CLOUD_EMPTY(Distribution.CLOUD, 0, "boot.web-profile-mode-mismatch"),
        CLOUD_PARTIAL(Distribution.CLOUD, 1, "boot.cloud-config-incomplete"),
        ON_PREM_COMPLETE(Distribution.ON_PREM, 4, "boot.web-profile-mode-mismatch"),
        ON_PREM_PARTIAL(Distribution.ON_PREM, 1, "boot.cloud-config-incomplete"),
        MISSING_PROFILE(Distribution.MISSING, 4, "boot.web-profile-invalid"),
        UNKNOWN_PROFILE(Distribution.UNKNOWN, 4, "boot.web-profile-invalid");

        private final Distribution distribution;
        private final int settings;
        private final String code;

        Mismatch(Distribution distribution, int settings, String code) {
            this.distribution = distribution;
            this.settings = settings;
            this.code = code;
        }
    }

    @BeforeAll
    static void prepareExecutableProfileFixtures() throws IOException {
        String configured = System.getProperty("tapstate.app.boot-jar");
        assertThat(configured).as("Failsafe supplies the real packaged Boot JAR").isNotBlank();
        Path bootJar = Path.of(configured).toAbsolutePath();
        assertThat(bootJar).isRegularFile();
        for (Distribution distribution : Distribution.values()) {
            JARS.put(distribution, BootJarWebFixture.create(bootJar,
                    fixtureDirectory.resolve(distribution.name() + "-boot.jar"), distribution.profile));
        }
    }

    @BeforeEach
    void serveCloudRequestCounter() throws IOException {
        cloud = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
        cloud.createContext("/", exchange -> {
            cloudRequests.incrementAndGet();
            cloudPaths.add(exchange.getRequestMethod() + " " + exchange.getRequestURI().getPath());
            if (("/v1/api/clusters/" + CLUSTER + "/status-report").equals(exchange.getRequestURI().getPath())) {
                statusReports.add(new StatusReport(exchange.getRequestMethod(),
                        exchange.getRequestHeaders().getFirst("Authorization"),
                        JSON.readTree(exchange.getRequestBody())));
                byte[] response = JSON.writeValueAsBytes(Map.of("opId", "startup-status", "code", "ok",
                        "msg", "ok", "data", Map.of("status", "accepted")));
                exchange.getResponseHeaders().set("Content-Type", "application/json");
                exchange.sendResponseHeaders(200, response.length);
                exchange.getResponseBody().write(response);
            } else {
                exchange.sendResponseHeaders(404, -1);
            }
            exchange.close();
        });
        cloud.start();
    }

    @AfterEach
    void stopOwnedProcesses() throws Exception {
        try {
            for (Running running : processes) {
                running.close();
            }
        } finally {
            cloud.stop(0);
        }
    }

    @ParameterizedTest
    @EnumSource(Carrier.class)
    void everyCarrierSelectsCloudMetadataAndRestartReadsTheSameConfiguration(Carrier carrier)
            throws Exception {
        String selected = uniqueDatabase("cloud_selected");
        String ignored = uniqueDatabase("onprem_ignored");
        var storage = prepareNative(selected, true);
        Map<String, String> values = cloudValues(storage.configurationUri());
        String seed = "--tapstate.connectors.seed-dir=" + CloudConnectorTestInputs.seedDirectory();
        Running first = start(carrier, values, MONGO.getReplicaSetUrl(ignored), seed);
        first.awaitReady();
        assertVersion(first);
        awaitStatusReport(0, first);
        Document storedWitness;
        String keyring;
        SourceResource witness = new SourceResource("restart_witness",
                new Metadata(Map.of("fixture", "external-config-startup"), "restart witness", true, "fixture-user"),
                "mongodb", Map.of("uri", MONGO.getReplicaSetUrl(selected + "_pipeline_input")),
                null, null, null, null);
        try (var raw = MongoClients.create(MONGO.getReplicaSetUrl())) {
            var selectedDb = raw.getDatabase(selected);
            assertThat(selectedDb.listCollectionNames().into(new ArrayList<>()))
                    .contains(SystemCollections.SOURCES.collectionName());
            assertDefaultSource(selectedDb, StorageBinding.Mode.CLOUD);
            assertConnectors(raw.getDatabase(storage.cacheDatabase()), CONNECTORS);
            assertThat(raw.getDatabase(ignored).listCollectionNames().into(new ArrayList<>())).isEmpty();
            try (var connection = metadataConnection(selected)) {
                assertThat(connection.artifacts().create(witness)).isEqualTo(ArtifactMutation.CREATED);
                assertThat(connection.artifacts().get(witness.id())).contains(witness);
            }
            storedWitness = selectedDb.getCollection(SystemCollections.SOURCES.collectionName())
                    .find(new Document("_id", witness.id())).first();
            assertThat(storedWitness).isNotNull();
            keyring = keyringFingerprint(selectedDb);
        }
        first.close();
        int previousReports = statusReports.size();
        Running restarted = start(carrier, values, MONGO.getReplicaSetUrl(ignored), seed);
        restarted.awaitReady();
        assertVersion(restarted);
        try (var raw = MongoClients.create(MONGO.getReplicaSetUrl())) {
            assertThat(raw.getDatabase(selected).getCollection(SystemCollections.SOURCES.collectionName())
                    .find(new Document("_id", witness.id())).first()).isEqualTo(storedWitness);
            assertThat(raw.getDatabase(selected).getCollection(SystemCollections.SOURCES.collectionName())
                    .find(new Document("_id", StorageBinding.defaultSourceId(StorageBinding.Mode.OP))).first()).isNull();
            assertThat(raw.getDatabase(selected).getCollection(SystemCollections.SOURCES.collectionName())
                    .find(new Document("kind", "source")).into(new ArrayList<>()))
                    .extracting(document -> document.getString("_id")).containsExactlyInAnyOrder(
                            StorageBinding.defaultSourceId(StorageBinding.Mode.CLOUD), witness.id());
            assertThat(keyringFingerprint(raw.getDatabase(selected))).isEqualTo(keyring);
            assertThat(raw.getDatabase(ignored).listCollectionNames().into(new ArrayList<>())).isEmpty();
            assertConnectors(raw.getDatabase(storage.cacheDatabase()), CONNECTORS);
        }
        try (var connection = metadataConnection(selected)) {
            assertThat(connection.artifacts().get(witness.id()))
                    .contains(witness);
        }
        awaitStatusReport(previousReports, restarted);
        assertSafeOutput(first, values.get("tapstate.cloud.atlas-uri"));
        assertSafeOutput(restarted, values.get("tapstate.cloud.atlas-uri"));
    }

    private static String mongoDiagnostic() {
        boolean running = MONGO.isRunning();
        String status = "fixture Mongo running=" + running;
        if (running) {
            try (var raw = MongoClients.create(new com.mongodb.ConnectionString(
                    MONGO.getReplicaSetUrl() + "?serverSelectionTimeoutMS=5000&connectTimeoutMS=5000"))) {
                Document hello = raw.getDatabase("admin").runCommand(new Document("hello", 1));
                status += ", writablePrimary=" + hello.getBoolean("isWritablePrimary");
            } catch (RuntimeException unavailable) {
                status += ", observer=" + unavailable.getClass().getSimpleName();
            }
        }
        String logs = MONGO.getLogs();
        return status + ", containerTail=" + logs.lines().skip(Math.max(0, logs.lines().count() - 12))
                .collect(java.util.stream.Collectors.joining("\n"));
    }

    private static AssertionError withMongoDiagnostic(AssertionError failure) {
        try {
            failure.addSuppressed(new AssertionError(mongoDiagnostic()));
        } catch (RuntimeException observerFailure) {
            // A failed diagnostic must not replace the startup assertion or expose a driver message.
            failure.addSuppressed(new AssertionError("Mongo diagnostic observer failed: "
                    + observerFailure.getClass().getSimpleName()));
        }
        return failure;
    }

    @ParameterizedTest
    @EnumSource(value = Distribution.class, names = {"CLOUD", "ON_PREM"})
    void aHealthyDelayedMongoHelloUsesTheNormalStartupBound(Distribution distribution) throws Exception {
        String database = uniqueDatabase("delayed_hello");
        var storage = prepareNative(database, distribution == Distribution.CLOUD);
        try (var gate = delayMongoHello()) {
            String uri = "mongodb://127.0.0.1:" + gate.port() + "/" + database + "?directConnection=true";
            Map<String, String> values = distribution == Distribution.CLOUD ? cloudValues(uri) : Map.of();
            String[] extra = distribution == Distribution.CLOUD
                    ? new String[]{"--tapstate.connectors.seed-dir=" + CloudConnectorTestInputs.seedDirectory()}
                    : new String[0];
            long started = System.nanoTime();
            Running running = start(Carrier.FILE, values, uri, extra);
            try {
                gate.awaitFirstReply(Duration.ofSeconds(15));
                running.awaitMongoSelection();
                // Hold a real, already returned hello beyond the accelerated rejection bound. This is
                // deliberate fault injection, not a guessed sleep for fixture or application readiness.
                long releaseAt = System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(750);
                while (System.nanoTime() < releaseAt) {
                    LockSupport.parkNanos(TimeUnit.MILLISECONDS.toNanos(10));
                }
                assertThat(running.safeOutput()).doesNotContain(READY);
                assertThat(cloudRequests.get()).as("unexpected Cloud calls: %s", cloudPaths).isZero();
                gate.release();
                running.awaitReady();
                if (distribution == Distribution.ON_PREM) assertLocalLogin(running);
                // Keep the original launch-to-readiness bound; later C2 and database observations
                // have their own bounds and must not retroactively make a healthy startup late.
                Duration readyElapsed = Duration.ofNanos(System.nanoTime() - started);
                System.out.println("Delayed hello " + distribution + " Ready after "
                        + readyElapsed);
                assertThat(readyElapsed)
                        .withFailMessage("Packaged %s startup took %s before Ready and HTTP health; "
                                        + "expected less than 90 seconds. Child output:%n%s",
                                distribution, readyElapsed, running.safeOutput())
                        .isLessThan(Duration.ofSeconds(90));
                assertVersion(running);
                if (distribution == Distribution.CLOUD) awaitStatusReport(0, running);
                else assertThat(cloudRequests.get()).as("unexpected Cloud calls: %s", cloudPaths).isZero();
                try (var raw = MongoClients.create(MONGO.getReplicaSetUrl(database))) {
                    assertThat(raw.getDatabase(database).listCollectionNames().into(new ArrayList<>()))
                            .contains(SystemCollections.SOURCES.collectionName());
                    assertConnectors(raw.getDatabase(storage.cacheDatabase()),
                            distribution == Distribution.CLOUD ? CONNECTORS : Set.of());
                    assertDefaultSource(raw.getDatabase(database), distribution == Distribution.CLOUD
                            ? StorageBinding.Mode.CLOUD : StorageBinding.Mode.OP);
                }
                assertSafeOutput(running, uri);
                System.out.println("Delayed hello " + distribution + " observations complete after "
                        + Duration.ofNanos(System.nanoTime() - started));
            } finally {
                running.close();
            }
        }
    }

    @Test
    void anExplicitShortMongoSelectionBoundKeepsBusinessUnavailableWithoutStatus() throws Exception {
        String database = uniqueDatabase("expired_hello");
        try (var gate = delayMongoHello()) {
            String uri = "mongodb://127.0.0.1:" + gate.port() + "/" + database + "?directConnection=true";
            Running running = start(Carrier.FILE, cloudValues(uri), uri,
                    "--tapstate.connectors.seed-dir=" + CloudConnectorTestInputs.seedDirectory(),
                    "--tapstate.store.mongo.server-selection-timeout=500ms");
            try {
                // A short-bound client may close before the upstream reply is fully captured. Hold
                // all first replies anyway; do not turn that expected abort into a fixture timeout.
                running.awaitMongoSelection();
                long releaseAt = System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(750);
                while (System.nanoTime() < releaseAt) {
                    LockSupport.parkNanos(TimeUnit.MILLISECONDS.toNanos(10));
                }
                gate.release();
                running.awaitUnavailable("store.unreachable", "UNAVAILABLE");
                assertThat(running.get("/version").body()).contains("\"dataVersion\":null");
                assertDatabaseUntouched(database);
                assertDatabaseUntouched(database + "_operator");
                assertDatabaseUntouched(database + "_views");
                assertOnlyDeniedConfigurationClaim();
                assertSafeOutput(running, uri);
            } finally {
                running.close();
            }
        }
    }

    private static MongoHandshakeGate delayMongoHello() throws IOException {
        assertThat(InetAddress.getByName(MONGO.getHost()).isLoopbackAddress())
                .as("the controlled delay only forwards to the owned local Mongo fixture").isTrue();
        return new MongoHandshakeGate("127.0.0.1", MONGO.getMappedPort(27017));
    }

    @ParameterizedTest
    @EnumSource(Carrier.class)
    void everyCarrierKeepsOnPremStoreAndLocalAuthenticationAcrossRestartWithoutCloud(Carrier carrier)
            throws Exception {
        String database = uniqueDatabase("onprem");
        var storage = prepareNative(database, false);
        Running running = start(carrier, Map.of(), MONGO.getReplicaSetUrl(database));
        running.awaitReady();
        assertVersion(running);
        assertThat(running.post("/auth/bootstrap", "{\"username\":\"" + NativeAppStorage.USERNAME + "\",\"password\":\""
                + PASSWORD + "\"}").statusCode()).isEqualTo(403);
        assertLocalLogin(running);
        Document managedView;
        String keyring;
        try (var raw = MongoClients.create(MONGO.getReplicaSetUrl())) {
            assertThat(raw.getDatabase(database).listCollectionNames().into(new ArrayList<>()))
                    .contains(SystemCollections.SOURCES.collectionName());
            managedView = assertDefaultSource(raw.getDatabase(database), StorageBinding.Mode.OP);
            keyring = keyringFingerprint(raw.getDatabase(database));
            assertConnectors(raw.getDatabase(storage.cacheDatabase()), Set.of());
        }
        running.close();
        Running restarted = start(carrier, Map.of(), MONGO.getReplicaSetUrl(database));
        restarted.awaitReady();
        assertVersion(restarted);
        assertLocalLogin(restarted);
        try (var raw = MongoClients.create(MONGO.getReplicaSetUrl())) {
            assertThat(assertDefaultSource(raw.getDatabase(database), StorageBinding.Mode.OP)).isEqualTo(managedView);
            assertThat(keyringFingerprint(raw.getDatabase(database))).isEqualTo(keyring);
            assertConnectors(raw.getDatabase(storage.cacheDatabase()), Set.of());
        }
        assertThat(cloudRequests.get()).as("unexpected Cloud calls: %s", cloudPaths).isZero();
        assertSafeOutput(running, null);
        assertSafeOutput(restarted, null);
    }

    @Test
    void cloudCannotStartWithoutItsMandatoryConnectorRelease() throws Exception {
        String uri = prepareNative(uniqueDatabase("missing_cloud_seeds"), true).configurationUri();
        Running running = start(Carrier.FILE, cloudValues(uri), uri);
        running.awaitUnavailable("boot.cloud-connectors-invalid", "LOGIN");
        assertThat(cloudRequests.get()).as("unexpected Cloud calls: %s", cloudPaths).isZero();
        assertSafeOutput(running, uri);
    }

    @ParameterizedTest(name = "{0} refuses {1} before any network or store initialization")
    @MethodSource("mismatches")
    void packagedProfilesAndExternalSettingsMustMatchForBothEntryPoints(Entry entry, Mismatch mismatch)
            throws Exception {
        String ignored = uniqueDatabase("mismatch_ignored");
        String selected = uniqueDatabase("mismatch_selected");
        Map<String, String> values = switch (mismatch.settings) {
            case 0 -> Map.of();
            case 1 -> Map.of("tapstate.cloud.token", TOKEN);
            default -> cloudValues(MONGO.getReplicaSetUrl(selected));
        };
        Running running = start(mismatch.distribution, entry, Carrier.FILE, values,
                MONGO.getReplicaSetUrl(ignored));
        running.awaitFailure(mismatch.code);
        assertThat(running.safeOutput()).doesNotContain(READY, "Members {", "MongoClient with metadata", "carries:");
        assertUntouched(ignored);
        assertUntouched(selected);
        assertSafeOutput(running, values.get("tapstate.cloud.atlas-uri"));
    }

    private static Stream<Arguments> mismatches() {
        return Stream.of(Entry.values()).flatMap(entry -> Stream.of(Mismatch.values())
                .map(mismatch -> Arguments.of(entry, mismatch)));
    }

    @ParameterizedTest
    @EnumSource(Carrier.class)
    void missingClusterIdFailsBeforeOpeningEitherMetadataStore(Carrier carrier) throws Exception {
        String ignored = uniqueDatabase("missing_cluster_id_ignored");
        String selected = uniqueDatabase("missing_cluster_id_selected");
        Map<String, String> values = cloudValues(MONGO.getReplicaSetUrl(selected));
        values.remove("tapstate.cloud.cluster-id");
        Running running = start(carrier, values, MONGO.getReplicaSetUrl(ignored));
        running.awaitFailure("boot.cloud-config-incomplete");
        assertUntouched(ignored);
        assertUntouched(selected);
        assertSafeOutput(running, values.get("tapstate.cloud.atlas-uri"));
    }

    @Test
    void invalidCloudBaseUrlFailsWithoutEchoingEmbeddedCredentials() throws Exception {
        String ignored = uniqueDatabase("invalid_ignored");
        Map<String, String> values = cloudValues(MONGO.getReplicaSetUrl(uniqueDatabase("invalid_selected")));
        values.put("tapstate.cloud.base-url", "https://accidental-user:base-password-sentinel@cloud.example");
        Running running = start(Carrier.JVM, values, MONGO.getReplicaSetUrl(ignored));
        running.awaitFailure("boot.cloud-base-url-invalid");
        assertUntouched(ignored);
        assertSafeOutput(running, values.get("tapstate.cloud.atlas-uri"));
        assertAbsent(running, "base-password-sentinel", "accidental-user");
    }

    @Test
    void unavailableCloudStoreFailsRatherThanFallingBackToTheWorkingOnPremStore() throws Exception {
        String ignored = uniqueDatabase("unreachable_ignored");
        String uri = "mongodb://unreachable-user:atlas-password-sentinel@127.0.0.1:1/metadata";
        Running running = start(Carrier.ENVIRONMENT, cloudValues(uri), MONGO.getReplicaSetUrl(ignored),
                "--tapstate.store.mongo.server-selection-timeout=500ms");
        running.awaitUnavailable("store.unreachable", "UNAVAILABLE");
        assertDatabaseUntouched(ignored);
        assertOnlyDeniedConfigurationClaim();
        assertSafeOutput(running, uri);
        assertAbsent(running, "atlas-password-sentinel");
    }

    @Test
    void aCloudProcessCannotUseTheOnPremSwitchToDisableMetadata() throws Exception {
        String ignored = uniqueDatabase("disabled_ignored");
        String selected = uniqueDatabase("disabled_selected");
        Running running = start(Carrier.FILE, cloudValues(MONGO.getReplicaSetUrl(selected)),
                MONGO.getReplicaSetUrl(ignored), "--tapstate.store.mongo.enabled=false");
        running.awaitFailure("boot.cloud-store-required");
        assertUntouched(ignored);
        assertUntouched(selected);
        assertSafeOutput(running, MONGO.getReplicaSetUrl(selected));
    }

    @Test
    void anInvalidAtlasUriFailsBeforeOpeningMetadataAndWithoutEchoingItsSecret() throws Exception {
        String ignored = uniqueDatabase("invalid_atlas_ignored");
        String uri = "https://accidental-user:atlas-uri-password-sentinel@atlas.example/metadata";
        Running running = start(Carrier.ENVIRONMENT, cloudValues(uri), MONGO.getReplicaSetUrl(ignored));
        running.awaitFailure("boot.cloud-atlas-uri-invalid");
        assertUntouched(ignored);
        assertSafeOutput(running, uri);
        assertAbsent(running, "atlas-uri-password-sentinel");
    }

    @Test
    void aRefusedConfigurationPreparationKeepsItsSafeDiagnosticWhenCloudEntryResumes() throws Exception {
        String selected = uniqueDatabase("diagnostic_selected");
        String ignored = uniqueDatabase("diagnostic_ignored");
        String uri = MONGO.getReplicaSetUrl(selected);
        Document witness = new Document("_id", "retained").append("value", "must-remain");
        try (var raw = MongoClients.create(MONGO.getReplicaSetUrl())) {
            raw.getDatabase(selected).getCollection("diagnostic_witness").insertOne(witness);
        }
        String claim = "/v1/api/clusters/" + CLUSTER + "/configuration-initialization/claim";
        cloud.createContext(claim, exchange -> {
            cloudRequests.incrementAndGet();
            cloudPaths.add(exchange.getRequestMethod() + " " + exchange.getRequestURI().getPath());
            assertThat(exchange.getRequestHeaders().getFirst("Authorization")).isEqualTo("Bearer " + TOKEN);
            var body = JSON.readTree(exchange.getRequestBody());
            byte[] response = JSON.writeValueAsBytes(Map.of("code", "ok", "data", Map.of(
                    "clusterId", CLUSTER, "operationId", "fcb93dc8-ae8d-4e6a-9c06-44cc92a9bcf8",
                    "phase", "CLAIMED", "configurationHash", body.path("configurationHash").asText(), "creationGranted", true)));
            exchange.getResponseHeaders().set("Content-Type", "application/json");
            exchange.sendResponseHeaders(200, response.length);
            exchange.getResponseBody().write(response); exchange.close();
        });

        Running running = start(Carrier.ENVIRONMENT, cloudValues(uri), MONGO.getReplicaSetUrl(ignored));
        running.awaitUnavailable(StoreError.INITIALIZATION_REFUSED.code(), "UNAVAILABLE");
        assertThat(running.get("/auth/exchange?code=unconsumed-diagnostic-code").statusCode()).isEqualTo(503);
        assertThat(running.safeOutput()).contains("stage=prepare", "stage=resume", "creationGranted=true", "creationInvoked=true")
                .doesNotContain("unconsumed-diagnostic-code");
        assertSafeOutput(running, uri);
        assertThat(cloudPaths).containsExactly("POST " + claim);
        try (var raw = MongoClients.create(MONGO.getReplicaSetUrl())) {
            var database = raw.getDatabase(selected);
            assertThat(database.listCollectionNames().into(new ArrayList<>())).containsExactly("diagnostic_witness");
            assertThat(database.getCollection("diagnostic_witness").find().into(new ArrayList<>())).containsExactly(witness);
            assertThat(raw.getDatabase(ignored).listCollectionNames().into(new ArrayList<>())).isEmpty();
        }
    }

    @Test
    void anOnPremProcessCannotEnableCloudStatusReporting() throws Exception {
        Running running = start(Carrier.FILE, Map.of(), MONGO.getReplicaSetUrl(uniqueDatabase("status_onprem")),
                "--SDK_STATUS_SENDER_ENABLED=true");
        running.awaitFailure("boot.cloud-status-mode-required");
        assertThat(cloudRequests.get()).as("unexpected Cloud calls: %s", cloudPaths).isZero();
        assertSafeOutput(running, null);
    }

    @Test
    void cloudModeUsesTheActualSdkReporterWithoutASeparateEnableFlag() throws Exception {
        String uri = prepareNative(uniqueDatabase("status_cloud"), true).configurationUri();
        Running running = start(Carrier.ENVIRONMENT, cloudValues(uri), uri,
                "--tapstate.connectors.seed-dir=" + CloudConnectorTestInputs.seedDirectory());
        running.awaitReady();
        awaitStatusReport(0, running);
        assertSafeOutput(running, uri);
    }

    private void awaitStatusReport(int previousCount, Running running) throws IOException {
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(20);
        while (statusReports.size() <= previousCount && running.process().isAlive() && System.nanoTime() < deadline) {
            LockSupport.parkNanos(TimeUnit.MILLISECONDS.toNanos(20));
        }
        assertThat(statusReports.size())
                .withFailMessage("the real SDK did not send a new startup report:%n%s", running.safeOutput())
                .isGreaterThan(previousCount);
        StatusReport report = new ArrayList<>(statusReports).get(previousCount);
        assertThat(report.method()).isEqualTo("POST");
        assertThat(("Bearer " + TOKEN).equals(report.authorization())).as("C2 uses the configured static token").isTrue();
        assertThat(report.body().path("nonce").asText()).isNotBlank();
        assertThat(report.body().path("runtimeVersion").asText()).isEqualTo(RuntimeVersion.current());
        assertThat(report.body().path("uptimeMs").isIntegralNumber()).as("C2 explicitly carries integral uptimeMs").isTrue();
        assertThat(report.body().path("uptimeMs").canConvertToLong()).isTrue();
        assertThat(report.body().path("uptimeMs").asLong()).isGreaterThanOrEqualTo(0);
        assertThat(report.body().path("activePipelines").isIntegralNumber())
                .as("C2 explicitly carries integral activePipelines").isTrue();
        assertThat(report.body().path("activePipelines").canConvertToInt()).isTrue();
        assertThat(report.body().path("activePipelines").asInt()).isZero();
        assertThat(report.body().toString()).doesNotContain(TOKEN, "mongodb:", "mongodb+srv:", PASSWORD);
    }

    private void assertUntouched(String database) {
        assertDatabaseUntouched(database);
        assertThat(cloudRequests.get()).as("unexpected Cloud calls: %s", cloudPaths).isZero();
    }

    private void assertOnlyDeniedConfigurationClaim() {
        assertThat(statusReports).isEmpty();
        assertThat(cloudPaths).containsExactly("POST /v1/api/clusters/" + CLUSTER + "/configuration-initialization/claim");
    }

    private static void assertDatabaseUntouched(String database) {
        try (var raw = MongoClients.create(MONGO.getReplicaSetUrl())) {
            assertThat(raw.getDatabase(database).listCollectionNames().into(new ArrayList<>())).isEmpty();
        }
    }

    private static void assertVersion(Running running) throws Exception {
        HttpResponse<String> response = running.get("/version");
        assertThat(response.statusCode()).isEqualTo(200);
        assertThat(response.body()).contains("\"dataVersion\"");
    }

    private static void assertLocalLogin(Running running) throws Exception {
        HttpResponse<String> response = running.post("/auth/login", "{\"username\":\"" + NativeAppStorage.USERNAME + "\",\"password\":\""
                + PASSWORD + "\"}");
        assertThat(response.statusCode()).isEqualTo(200);
        String token = JSON.readTree(response.body()).path("token").asText();
        assertThat(token).isNotBlank();
        running.awaitBusiness(token);
    }

    private static void assertConnectors(com.mongodb.client.MongoDatabase database, Set<String> expected) {
        assertThat(database.getCollection(SystemCollections.CACHE_CONNECTOR_ARTIFACTS.collectionName() + ".files")
                .find().into(new ArrayList<>()))
                .extracting(document -> document.get("metadata", Document.class).getString("connectorId"))
                .containsExactlyInAnyOrderElementsOf(expected);
    }

    private static Document assertDefaultSource(com.mongodb.client.MongoDatabase database, StorageBinding.Mode mode) {
        var sources = database.getCollection(SystemCollections.SOURCES.collectionName());
        Document source = sources.find(new Document("_id", StorageBinding.defaultSourceId(mode))).first();
        assertThat(source).isNotNull();
        assertThat(source.getString("kind")).isEqualTo("source");
        assertThat(source.get("body", Document.class).getString("connector")).isEqualTo("mongodb");
        assertThat(sources.countDocuments(new Document("systemRole", "DEFAULT_TARGET"))).isEqualTo(1);
        return source;
    }

    private static String keyringFingerprint(com.mongodb.client.MongoDatabase database) {
        Document keyring = database.getCollection(SystemCollections.CLUSTER_KEYS.collectionName())
                .find(new Document("_id", "cluster")).first();
        assertThat(keyring).isNotNull();
        return ContentHash.of(keyring.toJson().getBytes(StandardCharsets.UTF_8));
    }

    private static MongoConfigurationDomain metadataConnection(String database) {
        return MongoConfigurationDomain.openExisting(new MongoConnectionSettings(
                MONGO.getReplicaSetUrl(database), null, Duration.ofSeconds(5)));
    }

    private static void assertAbsent(Running running, String... secrets) throws IOException {
        String output = running.output();
        for (String secret : secrets) {
            assertThat(output.contains(secret)).as("startup output contains no credential sentinel").isFalse();
        }
    }

    private static void assertSafeOutput(Running running, String uri) throws IOException {
        assertThat(running.output().contains(TOKEN)).as("startup log never contains the Cloud token").isFalse();
        assertThat(running.output().contains(PASSWORD)).as("startup log never contains the local password").isFalse();
        if (uri != null) {
            assertThat(running.output().contains(uri)).as("startup log never contains the full metadata URI").isFalse();
        }
    }

    private Map<String, String> cloudValues(String uri) {
        Map<String, String> values = new LinkedHashMap<>();
        values.put("tapstate.cloud.base-url", "http://127.0.0.1:" + cloud.getAddress().getPort());
        values.put("tapstate.cloud.token", TOKEN);
        values.put("tapstate.cloud.atlas-uri", uri);
        values.put("tapstate.cloud.cluster-id", CLUSTER);
        return values;
    }

    private Running start(Carrier carrier, Map<String, String> values, String onPremUri, String... extraArgs)
            throws IOException {
        return start(values.isEmpty() ? Distribution.ON_PREM : Distribution.CLOUD, Entry.SERVER,
                carrier, values, onPremUri, extraArgs);
    }

    private Running start(Distribution distribution, Entry entry, Carrier carrier,
            Map<String, String> values, String onPremUri, String... extraArgs) throws IOException {
        int ordinal = processes.size();
        Path directory = Files.createDirectories(work.resolve("process-" + ordinal));
        Path log = directory.resolve("startup.log");
        Path bootJar = JARS.get(distribution);
        assertThat(bootJar).isRegularFile();
        // The Cloud fixture loads the bundled Db2 runtime; preserve the smaller on-prem budget.
        String heap = distribution == Distribution.CLOUD ? "-Xmx1024m" : "-Xmx384m";
        List<String> command = new ArrayList<>(List.of(
                Path.of(System.getProperty("java.home"), "bin", "java").toString(), heap));
        if (carrier == Carrier.JVM) {
            values.forEach((key, value) -> command.add("-D" + key + "=" + value));
        }
        command.addAll(List.of("-jar", bootJar.toString()));
        if (entry == Entry.MIGRATE) command.addAll(List.of("migrate", "--list"));
        command.addAll(List.of("--server.address=127.0.0.1", "--server.port=0",
                "--tapstate.hz.member-port=0", "--tapstate.hz.jet.cooperative-thread-count=2",
                // The handshake gate waits for the driver's INFO server-selection diagnostic.
                "--logging.level.org.mongodb.driver.cluster=INFO",
                "--tapstate.connectors.plugins-dir=" + work.resolve("plugins"),
                "--tapstate.store.mongo.uri=" + onPremUri,
                "--tapstate.store.mongo.operator-state-database=" + operatorDatabase));
        command.addAll(List.of(extraArgs));
        if (carrier == Carrier.FILE) {
            Path config = directory.resolve("application.properties");
            StringBuilder contents = new StringBuilder();
            values.forEach((key, value) -> contents.append(key).append('=').append(value).append('\n'));
            Files.writeString(config, contents.toString());
            command.add("--spring.config.additional-location=" + config.toUri());
        }
        ProcessBuilder builder = new ProcessBuilder(command).directory(directory.toFile())
                .redirectErrorStream(true).redirectOutput(log.toFile());
        // Do not inherit a developer's Cloud configuration, Java injection options, or external Spring file.
        builder.environment().keySet().removeIf(key -> key.startsWith("TAPSTATE_") || key.startsWith("SPRING_")
                || key.equals("SDK_STATUS_SENDER_ENABLED") || key.equals("CLUSTER_ID"));
        JvmTestEnvironment.configure(builder, Map.of());
        if (carrier == Carrier.ENVIRONMENT) {
            values.forEach((key, value) -> builder.environment().put(
                    key.toUpperCase(java.util.Locale.ROOT).replace('.', '_').replace('-', '_'), value));
        }
        List<String> sensitive = new ArrayList<>(List.of(TOKEN, PASSWORD, onPremUri,
                "base-password-sentinel", "atlas-password-sentinel", "atlas-uri-password-sentinel", "accidental-user"));
        sensitive.addAll(values.values());
        long startedAt = System.nanoTime();
        Running running = new Running(builder.start(), log, List.copyOf(sensitive), startedAt);
        processes.add(running);
        return running;
    }

    private static NativeAppStorage prepareNative(String database, boolean cloud) {
        return NativeAppStorage.prepare(MONGO.getReplicaSetUrl(database),
                MONGO.getReplicaSetUrl(database + "_cache"), MONGO.getReplicaSetUrl(database + "_master"),
                cloud ? StorageBinding.Mode.CLOUD : StorageBinding.Mode.OP, CLUSTER);
    }

    private static String uniqueDatabase(String prefix) {
        return prefix + "_" + Long.toUnsignedString(System.nanoTime(), 16);
    }

    private record StatusReport(String method, String authorization, JsonNode body) {
    }

    private record Running(Process process, Path log, List<String> sensitive, long startedAt) implements AutoCloseable {
        String output() throws IOException {
            return Files.readString(log);
        }

        String safeOutput() throws IOException {
            return redact(output());
        }

        private String redact(String value) {
            for (String secret : sensitive) {
                if (!secret.isEmpty()) value = value.replace(secret, "<redacted>");
            }
            return value;
        }

        private String threadDiagnostic() throws IOException, InterruptedException {
            if (!process.isAlive()) return "Child process already exited; no live thread dump is available.";
            Path tool = Path.of(System.getProperty("java.home"), "bin", "jcmd");
            if (!Files.isExecutable(tool)) return "JDK thread diagnostic tool is unavailable.";
            Path dump = log.resolveSibling("startup-threads.log");
            Process diagnostic = new ProcessBuilder(tool.toString(), Long.toString(process.pid()), "Thread.print")
                    .redirectErrorStream(true).redirectOutput(dump.toFile()).start();
            try {
                if (!diagnostic.waitFor(5, TimeUnit.SECONDS)) {
                    return "Owned child thread diagnostic exceeded its five-second bound.";
                }
                return redact(Files.readString(dump));
            } finally {
                // Stop only this owned diagnostic command, never the child under observation.
                if (diagnostic.isAlive()) {
                    diagnostic.destroyForcibly();
                    diagnostic.waitFor(5, TimeUnit.SECONDS);
                }
            }
        }

        HttpResponse<String> get(String path) throws Exception {
            return request(path, null);
        }

        HttpResponse<String> post(String path, String json) throws Exception {
            return request(path, json);
        }

        private HttpResponse<String> request(String path, String json) throws Exception {
            return request(path, json, null);
        }

        private HttpResponse<String> request(String path, String json, String token) throws Exception {
            var match = HTTP_PORT.matcher(output());
            assertThat(match.find()).as("the child process published its bound HTTP port").isTrue();
            var request = HttpRequest.newBuilder(URI.create("http://127.0.0.1:" + match.group(1) + path))
                    .timeout(Duration.ofSeconds(2));
            if (token != null) request.header("Authorization", "Bearer " + token);
            if (json == null) request.GET();
            else request.header("Content-Type", "application/json").POST(HttpRequest.BodyPublishers.ofString(json));
            return HTTP.send(request.build(), HttpResponse.BodyHandlers.ofString());
        }

        void awaitReady() throws Exception {
            long deadline = startedAt + TimeUnit.SECONDS.toNanos(90);
            while (System.nanoTime() < deadline && process.isAlive()) {
                if (output().contains(READY) && HTTP_PORT.matcher(output()).find()) {
                    try {
                        HttpResponse<String> response = get("/healthz");
                        if (response.statusCode() == 200 && "ok".equals(response.body())) {
                            return;
                        }
                    } catch (IOException notListeningYet) {
                        // A transient loopback failure must not bypass the complete readiness gate.
                    }
                }
                LockSupport.parkNanos(TimeUnit.MILLISECONDS.toNanos(40));
            }
            AssertionError failure = new AssertionError(
                    "Boot process did not finish startup and become healthy: " + safeOutput());
            if (process.isAlive()) {
                try {
                    failure.addSuppressed(new AssertionError("Owned child thread diagnostic: " + threadDiagnostic()));
                } catch (IOException unavailable) {
                    failure.addSuppressed(new AssertionError("Owned child thread diagnostic unavailable: "
                            + unavailable.getClass().getSimpleName()));
                } catch (InterruptedException interrupted) {
                    Thread.currentThread().interrupt();
                    failure.addSuppressed(new AssertionError("Owned child thread diagnostic was interrupted."));
                }
            }
            throw withMongoDiagnostic(failure);
        }

        void awaitMongoSelection() throws Exception {
            long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(15);
            while (System.nanoTime() < deadline) {
                if (output().contains("Waiting for server to become available for operation hello")) return;
                if (!process.isAlive()) break;
                LockSupport.parkNanos(TimeUnit.MILLISECONDS.toNanos(10));
            }
            throw withMongoDiagnostic(new AssertionError(
                    "The child did not select Mongo for its initial hello: " + safeOutput()));
        }

        void awaitBusiness(String token) throws Exception {
            long deadline = startedAt + TimeUnit.SECONDS.toNanos(90);
            String last = "no authenticated snapshot";
            while (System.nanoTime() < deadline && process.isAlive()) {
                try {
                    var status = request("/api/storage/status", "{}", token);
                    last = status.body();
                    assertThat(status.statusCode()).isEqualTo(200);
                    if ("READY".equals(JSON.readTree(last).path("businessPhase").asText())) return;
                } catch (java.net.http.HttpTimeoutException pending) {
                    // Like liveness polling, a read timeout cannot extend the shared launch deadline.
                    last = "authenticated snapshot request timed out";
                    System.out.println("Retrying a timed-out native business snapshot within the original launch budget");
                }
                LockSupport.parkNanos(TimeUnit.MILLISECONDS.toNanos(40));
            }
            throw withMongoDiagnostic(new AssertionError("Native business did not start within the original launch budget: "
                    + last + "\n" + safeOutput()));
        }

        void awaitUnavailable(String code, String entry) throws Exception {
            awaitReady();
            assertThat(process.isAlive()).as("the diagnostic root stays alive").isTrue();
            var response = get("/auth/storage-entry");
            assertThat(response.statusCode()).isEqualTo(200);
            assertThat(JSON.readTree(response.body()).path("entry").asText()).isEqualTo(entry);
            String output = safeOutput();
            assertThat(output).contains(code);
            if (output.contains("Node session maintenance adopted")) {
                assertThat(output.lastIndexOf("is SHUTDOWN"))
                        .as("a failed child releases the actual member it started")
                        .isGreaterThan(output.lastIndexOf("is STARTED"));
            }
            assertThat(post("/auth/bootstrap", "{}").statusCode()).isEqualTo(403);
        }

        void awaitFailure(String code) throws Exception {
            try {
                assertThat(process.waitFor(45, TimeUnit.SECONDS)).as("startup failed without hanging").isTrue();
                assertThat(process.exitValue()).isNotZero();
                assertThat(safeOutput()).contains(code).doesNotContain("Tomcat started on port", READY);
            } catch (AssertionError failure) {
                throw withMongoDiagnostic(failure);
            }
        }

        @Override
        public void close() throws InterruptedException {
            if (process.isAlive()) {
                process.destroy();
                if (!process.waitFor(15, TimeUnit.SECONDS)) {
                    process.destroyForcibly();
                    assertThat(process.waitFor(5, TimeUnit.SECONDS)).as("only the owned child was terminated").isTrue();
                }
            }
        }
    }
}
