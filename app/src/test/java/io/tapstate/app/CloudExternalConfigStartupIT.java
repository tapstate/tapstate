package io.tapstate.app;

import com.mongodb.client.MongoClients;
import com.sun.net.httpserver.HttpServer;
import io.tapstate.adapters.mongostore.SystemCollections;
import io.tapstate.testsupport.RequiresDocker;
import org.bson.Document;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;
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
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.LockSupport;
import java.util.regex.Pattern;

import static org.assertj.core.api.Assertions.assertThat;

/** Exercises external configuration through the shipped JAR in a separate, real JVM process. */
@RequiresDocker
class CloudExternalConfigStartupIT {

    private static final String TOKEN = "cloud-startup-token-sentinel";
    private static final Pattern HTTP_PORT = Pattern.compile("Tomcat started on port (\\d+)");
    private static final HttpClient HTTP = HttpClient.newBuilder()
            .connectTimeout(Duration.ofSeconds(1)).build();

    @Container
    private static final MongoDBContainer MONGO =
            new MongoDBContainer(DockerImageName.parse("mongo:7.0"));

    @TempDir
    Path work;
    private final List<Running> processes = new ArrayList<>();
    private final AtomicInteger cloudRequests = new AtomicInteger();
    private HttpServer cloud;

    enum Carrier { FILE, ENVIRONMENT, JVM }

    @BeforeEach
    void serveCloudRequestCounter() throws IOException {
        cloud = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
        cloud.createContext("/", exchange -> {
            cloudRequests.incrementAndGet();
            exchange.sendResponseHeaders(503, -1);
            exchange.close();
        });
        cloud.start();
    }

    @AfterEach
    void stopOwnedProcesses() throws Exception {
        for (Running running : processes) {
            running.close();
        }
        cloud.stop(0);
    }

    @ParameterizedTest
    @EnumSource(Carrier.class)
    void everyCarrierSelectsCloudMetadataAndRestartReadsTheSameConfiguration(Carrier carrier)
            throws Exception {
        String selected = uniqueDatabase("cloud_selected");
        String ignored = uniqueDatabase("onprem_ignored");
        Map<String, String> values = cloudValues(MONGO.getReplicaSetUrl(selected));
        Running first = start(carrier, values, MONGO.getReplicaSetUrl(ignored));
        first.awaitHealth();
        assertVersion(first);
        Document managedView;
        try (var raw = MongoClients.create(MONGO.getReplicaSetUrl())) {
            var selectedDb = raw.getDatabase(selected);
            assertThat(selectedDb.listCollectionNames().into(new ArrayList<>()))
                    .contains(SystemCollections.ARTIFACTS.collectionName());
            managedView = selectedDb.getCollection(SystemCollections.ARTIFACTS.collectionName())
                    .find().first();
            assertThat(managedView).isNotNull();
            assertThat(raw.getDatabase(ignored).listCollectionNames().into(new ArrayList<>())).isEmpty();
        }
        first.close();
        Running restarted = start(carrier, values, MONGO.getReplicaSetUrl(ignored));
        restarted.awaitHealth();
        assertVersion(restarted);
        try (var raw = MongoClients.create(MONGO.getReplicaSetUrl())) {
            assertThat(raw.getDatabase(selected).getCollection(SystemCollections.ARTIFACTS.collectionName())
                    .find(new Document("_id", managedView.get("_id"))).first()).isEqualTo(managedView);
            assertThat(raw.getDatabase(ignored).listCollectionNames().into(new ArrayList<>())).isEmpty();
        }
        assertThat(cloudRequests.get()).as("no SDK adapter is installed yet").isZero();
        assertSafeOutput(first, values.get("tapstate.cloud.atlas-uri"));
        assertSafeOutput(restarted, values.get("tapstate.cloud.atlas-uri"));
    }

    @Test
    void noCloudValuesKeepOnPremStoreAndMakeNoCloudRequests() throws Exception {
        String database = uniqueDatabase("onprem");
        Running running = start(Carrier.FILE, Map.of(), MONGO.getReplicaSetUrl(database));
        running.awaitHealth();
        assertVersion(running);
        try (var raw = MongoClients.create(MONGO.getReplicaSetUrl())) {
            assertThat(raw.getDatabase(database).listCollectionNames().into(new ArrayList<>()))
                    .contains(SystemCollections.ARTIFACTS.collectionName());
        }
        assertThat(cloudRequests.get()).isZero();
        assertSafeOutput(running, null);
    }

    @Test
    void partialCloudConfigurationFailsBeforeOpeningTheOtherwiseWorkingOnPremStore() throws Exception {
        String ignored = uniqueDatabase("partial_ignored");
        Running running = start(Carrier.FILE,
                Map.of("tapstate.cloud.token", TOKEN), MONGO.getReplicaSetUrl(ignored));
        running.awaitFailure("boot.cloud-config-incomplete");
        assertUntouched(ignored);
        assertSafeOutput(running, null);
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
        assertThat(running.output()).doesNotContain("base-password-sentinel", "accidental-user");
    }

    @Test
    void unavailableCloudStoreFailsRatherThanFallingBackToTheWorkingOnPremStore() throws Exception {
        String ignored = uniqueDatabase("unreachable_ignored");
        String uri = "mongodb://unreachable-user:atlas-password-sentinel@127.0.0.1:1/metadata";
        Running running = start(Carrier.ENVIRONMENT, cloudValues(uri), MONGO.getReplicaSetUrl(ignored));
        running.awaitFailure("store.unreachable");
        assertUntouched(ignored);
        assertSafeOutput(running, uri);
        assertThat(running.output()).doesNotContain("atlas-password-sentinel");
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
        assertThat(running.output()).doesNotContain("atlas-uri-password-sentinel");
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
    void anEnabledCloudStatusProcessRequiresTheActualSdkReporter() throws Exception {
        String uri = MONGO.getReplicaSetUrl(uniqueDatabase("status_cloud"));
        Running running = start(Carrier.ENVIRONMENT, cloudValues(uri), uri,
                "--SDK_STATUS_SENDER_ENABLED=true");
        running.awaitFailure("boot.cloud-status-sdk-required");
        assertThat(cloudRequests.get()).as("missing SDK is not replaced with successful fake heartbeats").isZero();
        assertSafeOutput(running, uri);
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

    private static void assertSafeOutput(Running running, String uri) throws IOException {
        assertThat(running.output()).doesNotContain(TOKEN);
        if (uri != null) {
            assertThat(running.output()).doesNotContain(uri);
        }
    }

    private Map<String, String> cloudValues(String uri) {
        Map<String, String> values = new LinkedHashMap<>();
        values.put("tapstate.cloud.base-url", "http://127.0.0.1:" + cloud.getAddress().getPort());
        values.put("tapstate.cloud.token", TOKEN);
        values.put("tapstate.cloud.atlas-uri", uri);
        return values;
    }

    private Running start(Carrier carrier, Map<String, String> values, String onPremUri, String... extraArgs)
            throws IOException {
        int ordinal = processes.size();
        Path directory = Files.createDirectories(work.resolve("process-" + ordinal));
        Path log = directory.resolve("startup.log");
        Path bootJar = Path.of(System.getProperty("tapstate.app.boot-jar")).toAbsolutePath();
        assertThat(bootJar).isRegularFile();
        List<String> command = new ArrayList<>(List.of(
                Path.of(System.getProperty("java.home"), "bin", "java").toString(), "-Xmx384m"));
        if (carrier == Carrier.JVM) {
            values.forEach((key, value) -> command.add("-D" + key + "=" + value));
        }
        command.addAll(List.of("-jar", bootJar.toString(), "--server.address=127.0.0.1", "--server.port=0",
                "--tapstate.hz.member-port=0", "--tapstate.hz.jet.cooperative-thread-count=2",
                "--tapstate.store.mongo.uri=" + onPremUri,
                "--tapstate.store.mongo.operator-state-database=" + uniqueDatabase("operator_state"),
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
                || key.equals("SDK_STATUS_SENDER_ENABLED")
                || key.equals("JAVA_TOOL_OPTIONS") || key.equals("JDK_JAVA_OPTIONS") || key.equals("_JAVA_OPTIONS"));
        if (carrier == Carrier.ENVIRONMENT) {
            values.forEach((key, value) -> builder.environment().put(
                    key.toUpperCase(java.util.Locale.ROOT).replace('.', '_').replace('-', '_'), value));
        }
        Running running = new Running(builder.start(), log);
        processes.add(running);
        return running;
    }

    private static String uniqueDatabase(String prefix) {
        return prefix + "_" + Long.toUnsignedString(System.nanoTime(), 16);
    }

    private record Running(Process process, Path log) implements AutoCloseable {
        String output() throws IOException {
            return Files.readString(log);
        }

        HttpResponse<String> get(String path) throws Exception {
            var match = HTTP_PORT.matcher(output());
            assertThat(match.find()).as("the child process published its bound HTTP port").isTrue();
            return HTTP.send(HttpRequest.newBuilder(URI.create("http://127.0.0.1:" + match.group(1) + path))
                    .timeout(Duration.ofSeconds(2)).GET().build(), HttpResponse.BodyHandlers.ofString());
        }

        void awaitHealth() throws Exception {
            long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(45);
            while (System.nanoTime() < deadline && process.isAlive()) {
                if (HTTP_PORT.matcher(output()).find()) {
                    try {
                        HttpResponse<String> response = get("/healthz");
                        if (response.statusCode() == 200 && "ok".equals(response.body())) {
                            return;
                        }
                    } catch (IOException notListeningYet) {
                        // The log line can precede completion of the application startup runners.
                    }
                }
                LockSupport.parkNanos(TimeUnit.MILLISECONDS.toNanos(40));
            }
            throw new AssertionError("Boot process did not become healthy: " + output().replace(TOKEN, "<redacted>"));
        }

        void awaitFailure(String code) throws Exception {
            assertThat(process.waitFor(45, TimeUnit.SECONDS)).as("startup failed without hanging").isTrue();
            assertThat(process.exitValue()).isNotZero();
            assertThat(output()).contains(code).doesNotContain("Tomcat started on port");
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
