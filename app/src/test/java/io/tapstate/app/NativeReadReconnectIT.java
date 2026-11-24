package io.tapstate.app;

import static io.tapstate.app.NativeIdlePipelineCostIT.request;
import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoCollection;
import io.tapstate.core.dsl.DslParser;
import io.tapstate.core.model.SourceMode;
import io.tapstate.core.model.SourceResource;
import io.tapstate.core.model.TableRef;
import io.tapstate.spi.store.StorageBinding;
import io.tapstate.spi.store.StorePort;
import io.tapstate.testsupport.RequiresDocker;
import io.tapstate.testsupport.TcpResetProxy;
import java.net.http.HttpClient;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import org.bson.Document;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.springframework.boot.builder.SpringApplicationBuilder;
import org.springframework.boot.web.server.context.WebServerApplicationContext;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.MongoDBContainer;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.utility.DockerImageName;

/** Runs the original CONFIG and durable capture paths in the test JVM. */
@RequiresDocker
class NativeReadReconnectIT {
    private static final String PASSWORD = "retry-fixture-only";
    private static final String DATABASE = "retry_fixture";
    private static final Duration TIMEOUT = Duration.ofSeconds(75);

    @TempDir Path work;

    @Test
    void activeSharedAndPrivateReadersResumeWithTheClusterPolicy() throws Exception {
        try (var mongo = new MongoDBContainer(DockerImageName.parse("mongo:7.0"));
             var mysql = mysql()) {
            mongo.start();
            mysql.start();
            var storage = NativeAppStorage.createOnPremWithAdmin(mongo, "retry", "admin", PASSWORD);
            var seeds = seedConnectors();
            try (var app = new SpringApplicationBuilder(Bootstrap.class).environment(CloudFixtureEnvironment.isolated()).run(
                    "--server.address=127.0.0.1", "--server.port=0", "--logging.level.root=ERROR",
                    "--tapstate.store.mongo.uri=" + storage.configurationUri(),
                    "--tapstate.hz.member-port=0", "--tapstate.hz.jet.cooperative-thread-count=2",
                    "--tapstate.connectors.seed-dir=" + seeds, "--tapstate.connectors.plugins-dir=" + work.resolve("plugins"),
                    "--tapstate.control.auth.jwt-secret=retry-diagnostic-signing-key");
                 var http = HttpClient.newHttpClient();
                 var target = MongoClients.create(storage.masterUri())) {
                NativeAppStorage.awaitBusiness(app);
                var port = NativeAppStorage.business(app).getBean(StorePort.class);
                String origin = "http://127.0.0.1:" + ((WebServerApplicationContext) app).getWebServer().getPort();
                String token = request(http, origin, null, "POST", "/auth/login",
                        Map.of("username", "admin", "password", PASSWORD), 200).get("token").toString();
                configure(http, origin, token);
                for (boolean shared : List.of(true, false)) {
                    verify(new Runtime(http, origin, token, port), mysql,
                            target.getDatabase(storage.masterDatabase()).getCollection(table(shared)), shared);
                }
            }
        }
    }

    private static void configure(HttpClient http, String origin, String token) throws Exception {
        String settings = "/api/retry-settings";
        var original = request(http, origin, token, "GET", settings, null, 200);
        assertThat(original.get("revision")).isEqualTo(0L);
        var defaults = Map.of("enabled", true, "intervalMillis", 1000L, "maxDurationMillis", 45_000L);
        var update = Map.of("revision", 0, "defaults", defaults, "operations", Map.of());
        request(http, origin, token, "PUT", settings, update, 200);
        request(http, origin, token, "PUT", settings, update, 409);
    }

    private record Runtime(HttpClient http, String origin, String token, StorePort port) {
        Map<String, Object> get(String path) throws Exception {
            return request(http, origin, token, "GET", path, null, 200);
        }

        void post(String path, Map<String, Object> input) throws Exception {
            request(http, origin, token, "POST", path, input, 200);
        }
    }

    private static void verify(Runtime runtime, GenericContainer<?> mysql, MongoCollection<Document> target, boolean shared)
            throws Exception {
        String table = table(shared);
        String pipeline = "retry-" + table;
        String path = "/api/pipelines/" + pipeline;
        sql(mysql, "CREATE TABLE " + table + " (id BIGINT PRIMARY KEY, name VARCHAR(64)); "
                + "INSERT INTO " + table + " VALUES (1, 'snapshot');");
        try (var proxy = new TcpResetProxy(mysql.getHost(), mysql.getMappedPort(3306))) {
            prepareSource(runtime, table, pipeline, proxy.port(), shared);
            runtime.post(path + ":start", Map.of());
            await().atMost(TIMEOUT).until(() -> ids(target).equals(List.of(1L)));
            sql(mysql, "INSERT INTO " + table + " VALUES (2, 'before-reset'); "
                    + "INSERT INTO " + table + " VALUES (4, 'before-reset-control');");
            await().atMost(TIMEOUT).untilAsserted(() -> {
                assertThat(ids(target)).containsExactly(1L, 2L, 4L);
                assertThat(((Number) ack(runtime.get(path + "/position")).get("seq")).longValue())
                        .isGreaterThanOrEqualTo(shared ? 1L : 3L);
            });
            var original = resume(runtime.get(path + "/position"));
            proxy.resetFor(shared ? Duration.ofSeconds(10) : Duration.ofMillis(1500));
            sql(mysql, "DELETE FROM " + table + " WHERE id=2; "
                    + "INSERT INTO " + table + " VALUES (3, 'after-reset');");
            await().atMost(TIMEOUT).untilAsserted(() -> {
                assertThat(ids(target)).containsExactly(1L, 3L, 4L);
                assertThat(runtime.get(path + "/status").get("state")).isEqualTo("RUNNING");
                assertThat(resume(runtime.get(path + "/position"))).isNotEqualTo(original);
            });
            runtime.post(path + ":stop", Map.of("purgeState", false));
        }
    }

    private static void prepareSource(Runtime runtime, String table, String pipeline, int port, boolean shared)
            throws Exception {
        String id = table + "-source";
        var settings = Map.<String, Object>of("host", "127.0.0.1", "port", port, "database", DATABASE,
                "username", "root", "password", PASSWORD);
        runtime.port().artifacts().save(new SourceResource(id, null, "mysql", settings, SourceMode.CDC,
                List.of(TableRef.literal(table)), null, null));
        runtime.post("/api/connections:discover-schema", Map.of("id", id, "connectorId", "mysql", "settings", settings));
        runtime.port().artifacts().save(new DslParser().parse("""
                version: tapstate/v1
                kind: pipeline
                id: %s
                source:
                  - { id: %s, srs: %s }
                settings: { read_mode: snapshot_and_cdc }
                serve:
                  from: %s
                  sync:
                    - source: %s
                """.formatted(pipeline, id, shared, table, StorageBinding.defaultSourceId(StorageBinding.Mode.OP))));
    }

    private static Map<?, ?> ack(Map<String, Object> position) {
        return (Map<?, ?>) chain(position).get("targetAcked");
    }

    private static String resume(Map<String, Object> position) {
        return (String) ((Map<?, ?>) chain(position).get("resumeFrom")).get("token");
    }

    private static Map<?, ?> chain(Map<String, Object> position) {
        return (Map<?, ?>) ((List<?>) position.get("chains")).getFirst();
    }

    private static List<Long> ids(MongoCollection<Document> collection) {
        return collection.find().map(row -> ((Number) row.get("id")).longValue())
                .into(new java.util.ArrayList<>()).stream().sorted().toList();
    }

    private static String table(boolean shared) {
        return shared ? "shared_orders" : "direct_orders";
    }

    private Path seedConnectors() throws Exception {
        Path seeds = Files.createDirectory(work.resolve("seeds"));
        for (String id : List.of("mysql", "mongodb")) {
            String pinned = System.getProperty("tapstate.pdk.it." + id + "-jar");
            String name = id + "-connector.jar";
            Path jar = pinned == null ? CloudConnectorTestInputs.seedDirectory().resolve(name) : Path.of(pinned);
            Files.copy(jar, seeds.resolve(name));
        }
        return seeds;
    }

    private static GenericContainer<?> mysql() {
        return new GenericContainer<>(DockerImageName.parse("mysql:8.0"))
                .withEnv("MYSQL_ROOT_PASSWORD", PASSWORD).withEnv("MYSQL_DATABASE", DATABASE)
                .withExposedPorts(3306).withStartupTimeout(Duration.ofSeconds(75))
                .withCommand("--innodb-buffer-pool-size=64M", "--max-connections=20", "--server-id=1",
                        "--log-bin=mysql-bin", "--binlog-format=ROW", "--binlog-row-image=FULL")
                .waitingFor(Wait.forLogMessage(".*mysqld: ready for connections\\..*port: 3306.*\\n", 1));
    }

    private static void sql(GenericContainer<?> mysql, String statement) throws Exception {
        var result = mysql.execInContainer("mysql", "--protocol=TCP", "--host=127.0.0.1", "--user=root",
                "--password=" + PASSWORD, "--batch", DATABASE, "-e", statement);
        assertThat(result.getExitCode()).as(result.getStderr()).isZero();
    }
}
