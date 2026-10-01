package io.tapstate.app;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.mongodb.client.MongoClients;
import com.sun.net.httpserver.HttpServer;
import io.tapstate.adapters.mongostore.SystemCollections;
import io.tapstate.control.restapi.RuntimeVersion;
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
import java.net.InetSocketAddress;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.file.Files;
import java.nio.file.Path;
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
    private static final String PASSWORD = "startup-onprem-password-sentinel";
    private static final Set<String> CONNECTORS = Set.of(
            "mysql", "mongodb", "postgres", "oracle", "sqlserver", "mongodb-atlas", "aws-rds-mysql");
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
        Map<String, String> values = cloudValues(MONGO.getReplicaSetUrl(selected));
        String seed = "--tapstate.connectors.seed-dir=" + CloudConnectorTestInputs.seedDirectory();
        Running first = start(carrier, values, MONGO.getReplicaSetUrl(ignored), seed);
        first.awaitReady();
        assertVersion(first);
        awaitStatusReport(0, first);
        Document managedView;
        try (var raw = MongoClients.create(MONGO.getReplicaSetUrl())) {
            var selectedDb = raw.getDatabase(selected);
            assertThat(selectedDb.listCollectionNames().into(new ArrayList<>()))
                    .contains(SystemCollections.ARTIFACTS.collectionName());
            managedView = selectedDb.getCollection(SystemCollections.ARTIFACTS.collectionName())
                    .find().first();
            assertThat(managedView).isNotNull();
            assertConnectors(raw.getDatabase(selected), CONNECTORS);
            assertThat(raw.getDatabase(ignored).listCollectionNames().into(new ArrayList<>())).isEmpty();
        }
        first.close();
        int previousReports = statusReports.size();
        Running restarted = start(carrier, values, MONGO.getReplicaSetUrl(ignored), seed);
        restarted.awaitReady();
        assertVersion(restarted);
        try (var raw = MongoClients.create(MONGO.getReplicaSetUrl())) {
            assertThat(raw.getDatabase(selected).getCollection(SystemCollections.ARTIFACTS.collectionName())
                    .find(new Document("_id", managedView.get("_id"))).first()).isEqualTo(managedView);
            assertThat(raw.getDatabase(ignored).listCollectionNames().into(new ArrayList<>())).isEmpty();
            assertConnectors(raw.getDatabase(selected), CONNECTORS);
        }
        awaitStatusReport(previousReports, restarted);
        assertSafeOutput(first, values.get("tapstate.cloud.atlas-uri"));
        assertSafeOutput(restarted, values.get("tapstate.cloud.atlas-uri"));
    }

    @ParameterizedTest
    @EnumSource(Carrier.class)
    void everyCarrierKeepsOnPremStoreAndLocalAuthenticationAcrossRestartWithoutCloud(Carrier carrier)
            throws Exception {
        String database = uniqueDatabase("onprem");
        Running running = start(carrier, Map.of(), MONGO.getReplicaSetUrl(database));
        running.awaitReady();
        assertVersion(running);
        assertThat(running.post("/auth/bootstrap", "{\"username\":\"startup-admin\",\"password\":\""
                + PASSWORD + "\"}").statusCode()).isEqualTo(204);
        assertLocalLogin(running);
        try (var raw = MongoClients.create(MONGO.getReplicaSetUrl())) {
            assertThat(raw.getDatabase(database).listCollectionNames().into(new ArrayList<>()))
                    .contains(SystemCollections.ARTIFACTS.collectionName());
            assertConnectors(raw.getDatabase(database), Set.of());
        }
        running.close();
        Running restarted = start(carrier, Map.of(), MONGO.getReplicaSetUrl(database));
        restarted.awaitReady();
        assertVersion(restarted);
        assertLocalLogin(restarted);
        assertThat(cloudRequests.get()).isZero();
        assertSafeOutput(running, null);
        assertSafeOutput(restarted, null);
    }

    @Test
    void cloudCannotStartWithoutItsMandatoryConnectorRelease() throws Exception {
        String uri = MONGO.getReplicaSetUrl(uniqueDatabase("missing_cloud_seeds"));
        Running running = start(Carrier.FILE, cloudValues(uri), uri);
        running.awaitFailure("boot.cloud-connectors-invalid");
        assertThat(cloudRequests.get()).isZero();
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
        Running running = start(Carrier.ENVIRONMENT, cloudValues(uri), MONGO.getReplicaSetUrl(ignored));
        running.awaitFailure("store.unreachable");
        assertUntouched(ignored);
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
    void anOnPremProcessCannotEnableCloudStatusReporting() throws Exception {
        Running running = start(Carrier.FILE, Map.of(), MONGO.getReplicaSetUrl(uniqueDatabase("status_onprem")),
                "--SDK_STATUS_SENDER_ENABLED=true");
        running.awaitFailure("boot.cloud-status-mode-required");
        assertThat(cloudRequests.get()).isZero();
        assertSafeOutput(running, null);
    }

    @Test
    void cloudModeUsesTheActualSdkReporterWithoutASeparateEnableFlag() throws Exception {
        String uri = MONGO.getReplicaSetUrl(uniqueDatabase("status_cloud"));
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
        try (var raw = MongoClients.create(MONGO.getReplicaSetUrl())) {
            assertThat(raw.getDatabase(database).listCollectionNames().into(new ArrayList<>())).isEmpty();
        }
        assertThat(cloudRequests.get()).isZero();
    }

    private static void assertVersion(Running running) throws Exception {
        HttpResponse<String> response = running.get("/version");
        assertThat(response.statusCode()).isEqualTo(200);
        assertThat(response.body()).contains("\"dataVersion\"");
    }

    private static void assertLocalLogin(Running running) throws Exception {
        HttpResponse<String> response = running.post("/auth/login", "{\"username\":\"startup-admin\",\"password\":\""
                + PASSWORD + "\"}");
        assertThat(response.statusCode()).isEqualTo(200);
        assertThat(JSON.readTree(response.body()).path("token").asText()).isNotBlank();
    }

    private static void assertConnectors(com.mongodb.client.MongoDatabase database, Set<String> expected) {
        assertThat(database.getCollection(SystemCollections.CONNECTOR_ARTIFACTS.collectionName() + ".files")
                .find().into(new ArrayList<>()))
                .extracting(document -> document.get("metadata", Document.class).getString("connectorId"))
                .containsExactlyInAnyOrderElementsOf(expected);
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
        List<String> command = new ArrayList<>(List.of(
                Path.of(System.getProperty("java.home"), "bin", "java").toString(), "-Xmx384m"));
        if (carrier == Carrier.JVM) {
            values.forEach((key, value) -> command.add("-D" + key + "=" + value));
        }
        command.addAll(List.of("-jar", bootJar.toString()));
        if (entry == Entry.MIGRATE) command.addAll(List.of("migrate", "--list"));
        command.addAll(List.of("--server.address=127.0.0.1", "--server.port=0",
                "--tapstate.hz.member-port=0", "--tapstate.hz.jet.cooperative-thread-count=2",
                "--tapstate.connectors.plugins-dir=" + work.resolve("plugins"),
                "--tapstate.store.mongo.uri=" + onPremUri,
                "--tapstate.store.mongo.operator-state-database=" + operatorDatabase,
                "--tapstate.store.mongo.server-selection-timeout=500ms"));
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
                || key.equals("SDK_STATUS_SENDER_ENABLED") || key.equals("CLUSTER_ID")
                || key.equals("JAVA_TOOL_OPTIONS") || key.equals("JDK_JAVA_OPTIONS") || key.equals("_JAVA_OPTIONS"));
        if (carrier == Carrier.ENVIRONMENT) {
            values.forEach((key, value) -> builder.environment().put(
                    key.toUpperCase(java.util.Locale.ROOT).replace('.', '_').replace('-', '_'), value));
        }
        List<String> sensitive = new ArrayList<>(List.of(TOKEN, PASSWORD, onPremUri,
                "base-password-sentinel", "atlas-password-sentinel", "atlas-uri-password-sentinel", "accidental-user"));
        sensitive.addAll(values.values());
        Running running = new Running(builder.start(), log, List.copyOf(sensitive));
        processes.add(running);
        return running;
    }

    private static String uniqueDatabase(String prefix) {
        return prefix + "_" + Long.toUnsignedString(System.nanoTime(), 16);
    }

    private record StatusReport(String method, String authorization, JsonNode body) {
    }

    private record Running(Process process, Path log, List<String> sensitive) implements AutoCloseable {
        String output() throws IOException {
            return Files.readString(log);
        }

        String safeOutput() throws IOException {
            String value = output();
            for (String secret : sensitive) {
                if (!secret.isEmpty()) value = value.replace(secret, "<redacted>");
            }
            return value;
        }

        HttpResponse<String> get(String path) throws Exception {
            return request(path, null);
        }

        HttpResponse<String> post(String path, String json) throws Exception {
            return request(path, json);
        }

        private HttpResponse<String> request(String path, String json) throws Exception {
            var match = HTTP_PORT.matcher(output());
            assertThat(match.find()).as("the child process published its bound HTTP port").isTrue();
            var request = HttpRequest.newBuilder(URI.create("http://127.0.0.1:" + match.group(1) + path))
                    .timeout(Duration.ofSeconds(2));
            if (json == null) request.GET();
            else request.header("Content-Type", "application/json").POST(HttpRequest.BodyPublishers.ofString(json));
            return HTTP.send(request.build(), HttpResponse.BodyHandlers.ofString());
        }

        void awaitReady() throws Exception {
            long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(90);
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
            throw new AssertionError("Boot process did not finish startup and become healthy: " + safeOutput());
        }

        void awaitFailure(String code) throws Exception {
            assertThat(process.waitFor(45, TimeUnit.SECONDS)).as("startup failed without hanging").isTrue();
            assertThat(process.exitValue()).isNotZero();
            assertThat(safeOutput()).contains(code).doesNotContain("Tomcat started on port", READY);
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
