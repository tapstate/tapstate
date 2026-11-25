package io.tapstate.app;

import ch.qos.logback.classic.Logger;
import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.classic.spi.ThrowableProxyUtil;
import ch.qos.logback.core.read.ListAppender;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import io.tapstate.adapters.mongostore.SystemCollections;
import io.tapstate.control.core.ControlError;
import io.tapstate.control.core.ControlOperations;
import io.tapstate.control.core.SourceConfigRevealRequest;
import io.tapstate.control.core.SourceConfigRevealService;
import io.tapstate.control.restapi.ArtifactList;
import io.tapstate.core.common.TapstateException;
import io.tapstate.core.dsl.DslParser;
import io.tapstate.core.model.SourceResource;
import io.tapstate.spi.store.ArtifactStore;
import io.tapstate.spi.store.IoError;
import io.tapstate.testsupport.RequiresDocker;
import org.bson.Document;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;
import org.slf4j.LoggerFactory;
import org.springframework.boot.builder.SpringApplicationBuilder;
import org.springframework.boot.web.server.context.WebServerApplicationContext;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpMethod;
import org.springframework.http.MediaType;
import org.springframework.http.client.JdkClientHttpRequestFactory;
import org.springframework.web.client.RestClient;
import org.testcontainers.containers.MongoDBContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.utility.DockerImageName;
import tools.jackson.databind.json.JsonMapper;

import java.io.PrintWriter;
import java.io.StringWriter;
import java.net.http.HttpClient;
import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Base64;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Damaged live storage cannot reach a connection probe; the reserved reveal seam never reads it. */
@RequiresDocker
class SourceConfigRuntimeRefusalIT {

    private static final JsonMapper JSON = JsonMapper.builder().build();
    private static final DslParser PARSER = new DslParser();
    private static final String ID = "runtime_damaged_source";
    private static final String HEALTHY_ID = "runtime_healthy_source";
    private static final String SECRET = "runtime-config-secret-sentinel";
    private static final String UNMARKED = "runtime-unmarked-config-sentinel";
    private static final String INVALID_INPUT = "runtime-invalid-ciphertext-sentinel";
    private static final Map<String, Object> DISPLAY_SETTINGS = Map.of("isUri", true);

    @Container
    private static final MongoDBContainer MONGO = new MongoDBContainer(DockerImageName.parse("mongo:7.0"));

    @TempDir Path work;

    @ParameterizedTest
    @EnumSource(CloudRuntimeSettings.Mode.class)
    void liveCorruptionRefusesStrictHttpAndStoredConnectionDeliveryWithoutChangingData(
            CloudRuntimeSettings.Mode mode) {
        try (Fixture fixture = start(mode); Logs logs = new Logs()) {
            seed(fixture);
            connection(fixture, ID, "/api/connections:test", 200);
            connection(fixture, ID, "/api/connections:discover-schema", 200);
            var probes = fixture.context.getBean(SharedConnectorSourceModesIT.RecordingProbes.class);
            assertThat(probes.tested.settings()).containsEntry("uri", uri());
            assertThat(probes.discovered.settings()).containsEntry("uri", uri());
            Document original = fixture.document(ID);
            Document healthy = fixture.document(HEALTHY_ID);
            List<Document> results = snapshot(SystemCollections.CONNECTION_TEST_RESULTS.on(fixture.database));
            List<Document> schemas = snapshot(SystemCollections.SOURCE_SCHEMAS.on(fixture.database));
            List<Document> audit = snapshot(SystemCollections.AUDIT.on(fixture.database));
            logs.clear();

            for (Damage damage : Damage.values()) {
                Document damaged = damage.apply(original, healthy);
                assertThat(fixture.artifacts.replaceOne(new Document("_id", ID), damaged).getMatchedCount())
                        .as("the corruption must reach the actual stored document")
                        .isEqualTo(1);
                int callsBefore = probes.testCalls.get();
                var testedBefore = probes.tested;
                var discoveredBefore = probes.discovered;

                assertThatThrownBy(() -> fixture.store.get(ID))
                        .isInstanceOfSatisfying(TapstateException.class, failure -> {
                            assertThat(failure.code()).isEqualTo(IoError.DOCUMENT_UNREADABLE);
                            assertThat(failure.getCause()).isNull();
                            StringWriter trace = new StringWriter();
                            failure.printStackTrace(new PrintWriter(trace));
                            safe(trace.toString());
                        });
                refusal(fixture, HttpMethod.GET, "/api/sources/" + ID, null, null);
                refusal(fixture, HttpMethod.GET, "/api/artifacts/" + ID, null, null);
                connection(fixture, ID, "/api/connections:test", 500);
                connection(fixture, ID, "/api/connections:discover-schema", 500);
                // Even a valid old ETag must not let a typed edit read damaged config as plaintext.
                refusal(fixture, HttpMethod.PUT, "/api/sources/" + ID,
                        Map.of("id", ID, "connector", "mongodb-atlas", "config", DISPLAY_SETTINGS),
                        '"' + original.getString("contentHash") + '"');

                ArtifactList inventory = fixture.client.get().uri("/api/artifacts").retrieve().body(ArtifactList.class);
                assertThat(inventory).isNotNull();
                var row = inventory.artifacts().stream().filter(entry -> ID.equals(entry.id())).findFirst().orElseThrow();
                assertThat(row.readable()).isFalse();
                assertThat(row.contentHash()).isEqualTo(damaged.getString("contentHash"));
                if ("source".equals(damaged.getString("kind"))) {
                    assertThat(row.canonicalForm()).isEqualTo("<redacted-source>");
                } else {
                    assertThat(row.canonicalForm()).isNull();
                }
                safe(JSON.writeValueAsString(inventory));
                String sources = fixture.client.get().uri("/api/sources").retrieve().body(String.class);
                assertThat(sources).contains(HEALTHY_ID).doesNotContain(ID);
                noSecrets(sources);
                assertThat(probes.testCalls.get()).isEqualTo(callsBefore);
                assertThat(probes.tested).isSameAs(testedBefore);
                assertThat(probes.discovered).isSameAs(discoveredBefore);
                assertThat(snapshot(SystemCollections.CONNECTION_TEST_RESULTS.on(fixture.database))).isEqualTo(results);
                assertThat(snapshot(SystemCollections.SOURCE_SCHEMAS.on(fixture.database))).isEqualTo(schemas);
                assertThat(snapshot(SystemCollections.AUDIT.on(fixture.database))).isEqualTo(audit);
                assertThat(fixture.document(ID)).isEqualTo(damaged);
                assertThat(fixture.document(HEALTHY_ID)).isEqualTo(healthy);
            }

            // A damaged row must not disable valid saved connections or their protected reads.
            connection(fixture, HEALTHY_ID, "/api/connections:test", 200);
            connection(fixture, HEALTHY_ID, "/api/connections:discover-schema", 200);
            assertThat(probes.tested.id()).isEqualTo(HEALTHY_ID);
            assertThat(probes.discovered.id()).isEqualTo(HEALTHY_ID);
            noSecrets(fixture.client.get().uri("/api/sources/" + HEALTHY_ID).retrieve().body(String.class));
            if (mode == CloudRuntimeSettings.Mode.CLOUD) {
                assertThat(logs.text()).contains(IoError.DOCUMENT_UNREADABLE.code());
            } else {
                assertThat(logs.text()).doesNotContain("Cloud HTTP request");
            }
            safe(logs.text());
        }
    }

    @ParameterizedTest
    @EnumSource(CloudRuntimeSettings.Mode.class)
    void productionRevealDeniesEveryPrincipalBeforeAnyArtifactReadOrDecryption(
            CloudRuntimeSettings.Mode mode) {
        try (Fixture fixture = start(mode)) {
            seed(fixture);
            var service = fixture.context.getBean(SourceConfigRevealService.class);
            assertThat(ControlOperations.registry().resolve("source.reveal-config").exposure()).isEmpty();
            // Profiling is restricted to this disposable metadata database. A positive control proves
            // that the real production store read is observable; no test-only authorizer is installed.
            fixture.database.runCommand(new Document("profile", 2));
            try {
                long before = artifactReads(fixture.database);
                assertThat(fixture.store.get(ID)).isPresent();
                long observed = artifactReads(fixture.database);
                assertThat(observed).isGreaterThan(before);
                for (String principal : List.of("admin", "machine-token", "mcp-caller", "cloud-user", "anonymous")) {
                    for (String grant : List.of("", "forged-one-time-grant")) {
                        for (String sourceId : List.of(ID, "absent-source")) {
                            assertThatThrownBy(() -> service.reveal(
                                    principal, new SourceConfigRevealRequest(sourceId, grant)))
                                    .isInstanceOfSatisfying(TapstateException.class, failure -> {
                                        assertThat(failure.code()).isEqualTo(ControlError.SOURCE_CONFIG_REVEAL_UNAVAILABLE);
                                        assertThat(failure.args()).isEmpty();
                                        assertThat(failure.getCause()).isNull();
                                    });
                        }
                    }
                }
                assertThat(artifactReads(fixture.database)).isEqualTo(observed);
                // A corrupt envelope distinguishes deny-before-read from read/decrypt-then-deny too.
                fixture.artifacts.updateOne(new Document("_id", ID), new Document("$set",
                        new Document("body.config", "tscfg:1:unavailable-key:" + INVALID_INPUT)));
                assertThatThrownBy(() -> service.reveal("admin", new SourceConfigRevealRequest(ID, "grant")))
                        .isInstanceOfSatisfying(TapstateException.class,
                                failure -> assertThat(failure.code()).isEqualTo(ControlError.SOURCE_CONFIG_REVEAL_UNAVAILABLE));
                assertThat(artifactReads(fixture.database)).isEqualTo(observed);
                assertThatThrownBy(() -> fixture.store.get(ID)).isInstanceOfSatisfying(TapstateException.class,
                        failure -> assertThat(failure.code()).isEqualTo(IoError.DOCUMENT_UNREADABLE));
                assertThat(artifactReads(fixture.database)).isGreaterThan(observed);
            } finally {
                fixture.database.runCommand(new Document("profile", 0));
            }
        }
    }

    private static void seed(Fixture fixture) {
        fixture.store.save(source(ID));
        fixture.store.save(source(HEALTHY_ID));
        assertThat(fixture.document(ID).get("body", Document.class).get("config"))
                .isInstanceOf(String.class).asString().startsWith("tscfg:1:");
        assertThat(fixture.document(ID).toJson()).doesNotContain(SECRET, UNMARKED, "runtime-user");
    }

    private static SourceResource source(String id) {
        return (SourceResource) PARSER.parse("""
                version: tapstate/v1
                kind: source
                id: %s
                connector: mongodb-atlas
                config:
                  isUri: true
                  uri: %s
                  nested: { extra: %s }
                """.formatted(id, uri(), UNMARKED));
    }

    private static String uri() {
        return "mongodb+srv://runtime-user:" + SECRET + "@cluster.example/rows";
    }

    private static void connection(Fixture fixture, String id, String path, int status) {
        Reply reply = request(fixture, HttpMethod.POST, path,
                Map.of("id", id, "connectorId", "mongodb-atlas", "settings", DISPLAY_SETTINGS), null);
        assertThat(reply.status).isEqualTo(status);
        safe(reply.body);
        if (status == 500) assertThat(JSON.readTree(reply.body).path("code").asText())
                .isEqualTo(IoError.DOCUMENT_UNREADABLE.code());
        else assertThat(reply.body).contains(path.endsWith(":test") ? "PASSED" : "probe");
    }

    private static void refusal(Fixture fixture, HttpMethod method, String path, Object body, String etag) {
        Reply reply = request(fixture, method, path, body, etag);
        assertThat(reply.status).isEqualTo(500);
        assertThat(JSON.readTree(reply.body).path("code").asText()).isEqualTo(IoError.DOCUMENT_UNREADABLE.code());
        safe(reply.body);
    }

    private static Reply request(Fixture fixture, HttpMethod method, String path, Object body, String etag) {
        var request = fixture.client.method(method).uri(path);
        if (body != null) request.contentType(MediaType.APPLICATION_JSON).body(body);
        if (etag != null) request.header(HttpHeaders.IF_MATCH, etag);
        return request.exchange((sent, response) -> new Reply(response.getStatusCode().value(), response.bodyTo(String.class)));
    }

    private static List<Document> snapshot(MongoCollection<Document> collection) {
        return collection.find().sort(new Document("_id", 1)).into(new ArrayList<>());
    }

    private static long artifactReads(MongoDatabase database) {
        return database.getCollection("system.profile").countDocuments(
                new Document("command.find", SystemCollections.ARTIFACTS.collectionName()));
    }

    private static void safe(String text) {
        assertThat(text).doesNotContain(SECRET, UNMARKED, INVALID_INPUT, "runtime-user", "tscfg:");
    }

    private static void noSecrets(String text) {
        // Typed Source views retain non-secret authoring fields, unlike generic config-omitting reads.
        assertThat(text).doesNotContain(SECRET, INVALID_INPUT, "runtime-user", "tscfg:");
    }

    private Fixture start(CloudRuntimeSettings.Mode mode) {
        boolean cloud = mode == CloudRuntimeSettings.Mode.CLOUD;
        String database = "runtime_config_" + Long.toUnsignedString(System.nanoTime(), 16);
        String uri = MONGO.getReplicaSetUrl(database);
        List<String> args = new ArrayList<>(List.of("--server.address=127.0.0.1", "--server.port=0",
                "--logging.level.root=ERROR", "--logging.level.io.tapstate.app.CloudHttpDiagnosticsFilter=INFO",
                "--tapstate.test.source-modes=true", "--SDK_STATUS_SENDER_ENABLED=false",
                "--spring.config.location=optional:classpath:/application.properties",
                "--tapstate.store.mongo.operator-state-database=" + database + "_ops",
                "--tapstate.connectors.plugins-dir=" + work.resolve(database).resolve("plugins"),
                "--tapstate.connectors.seed-dir=" + CloudConnectorTestInputs.seedDirectory()));
        if (cloud) args.addAll(List.of("--tapstate.cloud.base-url=https://cloud.example",
                "--tapstate.cloud.token=controlled-outbound-token", "--tapstate.cloud.atlas-uri=" + uri,
                "--tapstate.cloud.cluster-id=shared-source-cluster"));
        else args.add("--tapstate.store.mongo.uri=" + uri);
        var context = new SpringApplicationBuilder(SharedConnectorSourceModesIT.Assembly.class)
                .environment(CloudFixtureEnvironment.isolated()).run(args.toArray(String[]::new));
        MongoClient raw = null;
        try {
            raw = MongoClients.create(uri);
            return authenticatedFixture(context, raw, database, cloud);
        } catch (RuntimeException | Error failure) {
            try { context.close(); } catch (RuntimeException | Error cleanup) { failure.addSuppressed(cleanup); }
            if (raw != null) {
                try { raw.close(); } catch (RuntimeException | Error cleanup) { failure.addSuppressed(cleanup); }
            }
            throw failure;
        }
    }

    private static Fixture authenticatedFixture(ConfigurableApplicationContext context,
            MongoClient raw, String database, boolean cloud) {
        RestClient anonymous = RestClient.builder().baseUrl("http://127.0.0.1:"
                        + ((WebServerApplicationContext) context).getWebServer().getPort())
                .requestFactory(new JdkClientHttpRequestFactory(HttpClient.newBuilder()
                        .connectTimeout(Duration.ofSeconds(5)).followRedirects(HttpClient.Redirect.NEVER).build()))
                .build();
        String credential;
        if (cloud) credential = anonymous.get().uri("/auth/exchange?code=controlled-code")
                .exchange((request, response) -> {
                    assertThat(response.getStatusCode().value()).isEqualTo(302);
                    String cookie = response.getHeaders().getFirst(HttpHeaders.SET_COOKIE);
                    assertThat(cookie).contains("HttpOnly", "Secure");
                    return cookie.substring(0, cookie.indexOf(';'));
                });
        else {
            anonymous.post().uri("/auth/bootstrap").contentType(MediaType.APPLICATION_JSON)
                    .body(Map.of("username", "admin", "password", "controlled-local-password"))
                    .retrieve().toBodilessEntity();
            String login = anonymous.post().uri("/auth/login").contentType(MediaType.APPLICATION_JSON)
                    .body(Map.of("username", "admin", "password", "controlled-local-password"))
                    .retrieve().body(String.class);
            credential = JSON.readTree(login).path("token").asText();
        }
        RestClient authorized = anonymous.mutate()
                .defaultHeader(cloud ? HttpHeaders.COOKIE : HttpHeaders.AUTHORIZATION,
                        cloud ? credential : "Bearer " + credential).build();
        return new Fixture(context, raw, database, authorized);
    }

    private enum Damage {
        TAMPERED, UNKNOWN_VERSION, UNKNOWN_KEY, INVALID_ENCODING, COPIED_SOURCE, CHANGED_CONNECTOR,
        PLAINTEXT, WRONG_HASH, INDEXED_KIND;

        Document apply(Document original, Document other) {
            Document result = Document.parse(original.toJson());
            Document body = result.get("body", Document.class);
            String envelope = body.getString("config");
            switch (this) {
                case TAMPERED -> {
                    String[] parts = envelope.split(":", -1);
                    byte[] payload = Base64.getUrlDecoder().decode(parts[3]);
                    payload[payload.length - 1] ^= 1;
                    body.put("config", String.join(":", parts[0], parts[1], parts[2],
                            Base64.getUrlEncoder().withoutPadding().encodeToString(payload)));
                }
                case UNKNOWN_VERSION -> body.put("config", envelope.replace("tscfg:1:", "tscfg:77:"));
                case UNKNOWN_KEY -> body.put("config", "tscfg:1:unavailable-key:" + envelope.split(":", -1)[3]);
                case INVALID_ENCODING -> body.put("config", envelope.substring(0, envelope.lastIndexOf(':') + 1)
                        + "[" + INVALID_INPUT);
                case COPIED_SOURCE -> body.put("config", other.get("body", Document.class).getString("config"));
                case CHANGED_CONNECTOR -> body.put("connector", "mongodb");
                case PLAINTEXT -> body.put("config", new Document("password", SECRET).append("nested", UNMARKED));
                case WRONG_HASH -> result.put("contentHash", "0".repeat(64));
                case INDEXED_KIND -> result.put("kind", "pipeline");
            }
            return result;
        }
    }

    private record Reply(int status, String body) { }

    private static final class Fixture implements AutoCloseable {
        final ConfigurableApplicationContext context;
        final MongoClient raw;
        final MongoDatabase database;
        final MongoCollection<Document> artifacts;
        final ArtifactStore store;
        final RestClient client;

        Fixture(ConfigurableApplicationContext context, MongoClient raw, String database, RestClient client) {
            this.context = context;
            this.raw = raw;
            this.database = raw.getDatabase(database);
            this.artifacts = SystemCollections.ARTIFACTS.on(this.database);
            this.store = context.getBean(ArtifactStore.class);
            this.client = client;
        }

        Document document(String id) { return artifacts.find(new Document("_id", id)).first(); }

        @Override public void close() {
            try { context.close(); } finally { raw.close(); }
        }
    }

    private static final class Logs implements AutoCloseable {
        private final Logger root = (Logger) LoggerFactory.getLogger(Logger.ROOT_LOGGER_NAME);
        private final ListAppender<ILoggingEvent> events = new ListAppender<>();

        Logs() { events.start(); root.addAppender(events); }
        void clear() { synchronized (events) { events.list.clear(); } }

        String text() {
            List<ILoggingEvent> snapshot;
            synchronized (events) { snapshot = List.copyOf(events.list); }
            return snapshot.stream().map(event -> event.getFormattedMessage()
                            + (event.getThrowableProxy() == null ? "" : ThrowableProxyUtil.asString(event.getThrowableProxy())))
                    .collect(java.util.stream.Collectors.joining("\n"));
        }

        @Override public void close() { root.detachAppender(events); events.stop(); }
    }
}
