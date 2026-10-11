package io.tapstate.app;

import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.model.IndexOptions;
import io.tapstate.adapters.mongostore.SystemCollections;
import io.tapstate.control.core.ApplyResult;
import io.tapstate.control.core.ArtifactOutcome;
import io.tapstate.control.core.SourceView;
import io.tapstate.control.core.StoredArtifact;
import io.tapstate.core.dsl.DslParser;
import io.tapstate.core.model.SourceResource;
import io.tapstate.core.model.canonical.CanonicalHash;
import io.tapstate.spi.store.ArtifactStore;
import io.tapstate.testsupport.RequiresDocker;
import org.bson.Document;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;
import org.springframework.boot.builder.SpringApplicationBuilder;
import org.springframework.boot.web.server.context.WebServerApplicationContext;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.http.HttpHeaders;
import org.springframework.http.MediaType;
import org.springframework.http.client.JdkClientHttpRequestFactory;
import org.springframework.web.client.RestClient;
import org.testcontainers.containers.MongoDBContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.utility.DockerImageName;
import tools.jackson.databind.json.JsonMapper;

import java.net.http.HttpClient;
import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;

/** Real HTTP and encrypted Mongo writes; Cloud identity and connection probes remain controlled. */
@RequiresDocker
class SourceConfigEncryptionMutationIT {

    private static final JsonMapper JSON = JsonMapper.builder().build();
    private static final DslParser PARSER = new DslParser();
    private static final String OLD_SECRET = "mutation-original-secret-sentinel";
    private static final String ALPHA_SECRET = "mutation-alpha-secret-sentinel";
    private static final String BETA_SECRET = "mutation-beta-secret-sentinel";
    private static final String PUBLIC_VALUE = "mutation-public-config-sentinel";
    private static final String ROLLBACK_INDEX = "source_mutation_rollback_only";

    @Container
    private static final MongoDBContainer MONGO = new MongoDBContainer(DockerImageName.parse("mongo:7.0"));

    @TempDir Path work;

    @ParameterizedTest
    @EnumSource(CloudRuntimeSettings.Mode.class)
    void genericHttpReplayPreserveReplaceAndEmptyClearKeepLogicalHashesAndEncryptedStorage(
            CloudRuntimeSettings.Mode mode) {
        try (Fixture fixture = start(mode)) {
            String id = "generic_mutation_source";
            Map<String, Object> originalConfig = Map.of("uri", uri(OLD_SECRET), "auth_source", PUBLIC_VALUE);
            ApplyResult created = apply(fixture, draft(source(id, originalConfig, "initial"), null));
            String initialHash = outcome(created, id, ArtifactOutcome.Change.CREATED);
            assertState(fixture, id, originalConfig, initialHash);
            Document originalDocument = encrypted(fixture, id);
            StoredArtifact display = publicSource(fixture, id);

            ApplyResult replay = apply(fixture, draft(display.canonicalForm(), display.contentHash()));
            assertThat(outcome(replay, id, ArtifactOutcome.Change.UNCHANGED)).isEqualTo(initialHash);
            assertState(fixture, id, originalConfig, initialHash);
            sameDocument(fixture, id, originalDocument);

            String partial = """
                    version: tapstate/v1
                    kind: source
                    id: generic_mutation_source
                    connector: mongodb
                    metadata: { description: metadata-only }
                    """;
            ApplyResult preserved = apply(fixture, draft(partial, initialHash));
            String preservedHash = outcome(preserved, id, ArtifactOutcome.Change.UPDATED);
            assertThat(preservedHash).isNotEqualTo(initialHash);
            assertState(fixture, id, originalConfig, preservedHash);
            assertThat(fixture.source(id).metadata().description()).isEqualTo("metadata-only");

            Map<String, Object> replacementConfig = Map.of("uri", uri(ALPHA_SECRET));
            ApplyResult replaced = apply(fixture, draft(sourceWithOnlyConfig(id, replacementConfig), preservedHash));
            String replacementHash = outcome(replaced, id, ArtifactOutcome.Change.UPDATED);
            assertState(fixture, id, replacementConfig, replacementHash);
            assertThat(fixture.source(id).config()).doesNotContainKey("auth_source");
            assertThat(fixture.source(id).metadata().description()).isEqualTo("metadata-only");

            ApplyResult cleared = apply(fixture, draft(sourceWithOnlyConfig(id, Map.of()), replacementHash));
            String clearedHash = outcome(cleared, id, ArtifactOutcome.Change.UPDATED);
            assertState(fixture, id, Map.of(), clearedHash);
            Document emptyDocument = encrypted(fixture, id);
            StoredArtifact emptyDisplay = publicSource(fixture, id);
            ApplyResult emptyReplay = apply(fixture, draft(emptyDisplay.canonicalForm(), clearedHash));
            assertThat(outcome(emptyReplay, id, ArtifactOutcome.Change.UNCHANGED)).isEqualTo(clearedHash);
            sameDocument(fixture, id, emptyDocument);
        }
    }

    @ParameterizedTest
    @EnumSource(CloudRuntimeSettings.Mode.class)
    void concurrentTypedHttpReplacesWithOneEtagPersistOnlyTheWinningEncryptedConfiguration(
            CloudRuntimeSettings.Mode mode) throws Exception {
        try (Fixture fixture = start(mode)) {
            String id = "typed_mutation_race";
            Map<String, Object> originalConfig = atlasConfig(OLD_SECRET);
            Reply created = post(fixture.client, "/api/sources", Map.of(
                    "id", id, "connector", "mongodb-atlas", "config", originalConfig,
                    "metadata", Map.of("description", "initial")));
            assertThat(created.status()).isEqualTo(201);
            assertThat(created.etag()).matches("\"[0-9a-f]{64}\"");
            SourceView initialView = JSON.readValue(created.body(), SourceView.class);
            assertState(fixture, id, originalConfig, initialView.contentHash());
            assertThat(created.etag()).isEqualTo('"' + initialView.contentHash() + '"');

            CountDownLatch ready = new CountDownLatch(2);
            CountDownLatch release = new CountDownLatch(1);
            ExecutorService callers = Executors.newFixedThreadPool(2);
            List<Reply> replies;
            try {
                Future<Reply> alpha = callers.submit(() -> replaceAtBarrier(
                        fixture, initialView, created.etag(), ALPHA_SECRET, "alpha", ready, release));
                Future<Reply> beta = callers.submit(() -> replaceAtBarrier(
                        fixture, initialView, created.etag(), BETA_SECRET, "beta", ready, release));
                assertThat(ready.await(10, TimeUnit.SECONDS)).as("both owned HTTP callers reached the barrier").isTrue();
                release.countDown();
                replies = List.of(alpha.get(20, TimeUnit.SECONDS), beta.get(20, TimeUnit.SECONDS));
            } finally {
                release.countDown();
                callers.shutdownNow();
                assertThat(callers.awaitTermination(10, TimeUnit.SECONDS))
                        .as("the owned HTTP caller threads terminated").isTrue();
            }
            assertThat(replies).extracting(Reply::status).containsExactlyInAnyOrder(200, 412);
            Reply success = replies.stream().filter(reply -> reply.status() == 200).findFirst().orElseThrow();
            Reply conflict = replies.stream().filter(reply -> reply.status() == 412).findFirst().orElseThrow();
            assertThat(JSON.readTree(conflict.body()).path("code").asText()).isEqualTo("source.version-conflict");
            SourceView winner = JSON.readValue(success.body(), SourceView.class);
            String winningSecret = switch (winner.metadata().description()) {
                case "alpha" -> ALPHA_SECRET;
                case "beta" -> BETA_SECRET;
                default -> throw new AssertionError("A successful replacement must identify the winning request");
            };
            assertThat(winner.id()).isEqualTo(id);
            assertThat(winner.connector()).isEqualTo("mongodb-atlas");
            assertThat(success.etag()).isEqualTo('"' + winner.contentHash() + '"');
            assertThat(success.etag()).isNotEqualTo(created.etag());
            assertState(fixture, id, atlasConfig(winningSecret), winner.contentHash());
            assertThat(fixture.source(id).metadata().description()).isEqualTo(winner.metadata().description());

            Document winningDocument = encrypted(fixture, id);
            Reply stale = put(fixture.client, "/api/sources/" + id, created.etag(),
                    typedReplacement(initialView, atlasConfig(OLD_SECRET), "stale"));
            assertThat(stale.status()).isEqualTo(412);
            assertThat(JSON.readTree(stale.body()).path("code").asText()).isEqualTo("source.version-conflict");
            sameDocument(fixture, id, winningDocument);
        }
    }

    @ParameterizedTest
    @EnumSource(CloudRuntimeSettings.Mode.class)
    void staleGenericBatchesAndAMidTransactionMongoFailureLeaveNoPartialEncryptedWrites(
            CloudRuntimeSettings.Mode mode) {
        try (Fixture fixture = start(mode)) {
            String existing = "batch_existing_source";
            Map<String, Object> originalConfig = Map.of("uri", uri(OLD_SECRET));
            String hash = outcome(apply(fixture, draft(source(existing, originalConfig, "initial"), null)),
                    existing, ArtifactOutcome.Change.CREATED);
            Document originalDocument = encrypted(fixture, existing);
            assertState(fixture, existing, originalConfig, hash);

            Reply stale = post(fixture.client, "/api/artifacts:apply", Map.of("drafts", List.of(
                    draft(source("batch_valid_sibling", Map.of("uri", uri(ALPHA_SECRET)), "valid"), null),
                    draft(sourceWithOnlyConfig(existing, Map.of("uri", uri(BETA_SECRET))), "0".repeat(64)))));
            assertThat(stale.status()).isEqualTo(412);
            assertThat(JSON.readTree(stale.body()).path("code").asText()).isEqualTo("artifact.version-conflict");
            assertThat(fixture.artifacts.countDocuments(new Document("_id", "batch_valid_sibling"))).isZero();
            sameDocument(fixture, existing, originalDocument);

            String rollbackFirst = "rollback_first_source";
            String rollbackSecond = "rollback_second_source";
            Map<String, Object> firstConfig = Map.of("uri", uri(ALPHA_SECRET));
            Map<String, Object> secondConfig = Map.of("uri", uri(BETA_SECRET));
            List<Map<String, Object>> rollbackDrafts = List.of(
                    draft(rollbackSource(rollbackFirst, firstConfig), null),
                    draft(rollbackSource(rollbackSecond, secondConfig), null));
            // Only these two test Sources match the index; the managed views Source is unaffected.
            // Both drafts are valid, so the second same-kind insert fails inside the Mongo transaction.
            fixture.artifacts.createIndex(new Document("kind", 1), new IndexOptions().name(ROLLBACK_INDEX)
                    .unique(true).partialFilterExpression(new Document("body.metadata.labels.rollback", "collision")));
            try {
                Reply failed = post(fixture.client, "/api/artifacts:apply", Map.of("drafts", rollbackDrafts));
                assertThat(failed.status()).isEqualTo(500);
                var error = JSON.readTree(failed.body());
                assertThat(error.path("code").asText()).isEqualTo("io.store-unavailable");
                assertThat(error.path("params").path("detail").asText()).contains("code=11000");
                assertThat(fixture.artifacts.countDocuments(new Document("_id", rollbackFirst))).isZero();
                assertThat(fixture.artifacts.countDocuments(new Document("_id", rollbackSecond))).isZero();
                sameDocument(fixture, existing, originalDocument);
            } finally {
                fixture.artifacts.dropIndex(ROLLBACK_INDEX);
            }

            // The same batch commits once the controlled database fault is removed.
            ApplyResult recovered = apply(fixture, rollbackDrafts);
            assertThat(recovered.outcomes()).extracting(ArtifactOutcome::id)
                    .containsExactly(rollbackFirst, rollbackSecond);
            assertState(fixture, rollbackFirst, firstConfig,
                    outcome(recovered, rollbackFirst, ArtifactOutcome.Change.CREATED));
            assertState(fixture, rollbackSecond, secondConfig,
                    outcome(recovered, rollbackSecond, ArtifactOutcome.Change.CREATED));
            sameDocument(fixture, existing, originalDocument);
        }
    }

    private Fixture start(CloudRuntimeSettings.Mode mode) {
        boolean cloud = mode == CloudRuntimeSettings.Mode.CLOUD;
        String database = "config_mutations_" + Long.toUnsignedString(System.nanoTime(), 16);
        String uri = MONGO.getReplicaSetUrl(database);
        List<String> args = new ArrayList<>(List.of(
                "--server.address=127.0.0.1", "--server.port=0", "--logging.level.root=ERROR",
                "--tapstate.test.source-modes=true", "--SDK_STATUS_SENDER_ENABLED=false",
                "--spring.config.location=optional:classpath:/application.properties",
                "--tapstate.store.mongo.operator-state-database=" + database + "_ops",
                "--tapstate.connectors.plugins-dir=" + work.resolve(database).resolve("plugins"),
                "--tapstate.connectors.seed-dir=" + CloudConnectorTestInputs.seedDirectory()));
        if (cloud) args.addAll(List.of("--tapstate.cloud.base-url=https://cloud.example",
                "--tapstate.cloud.token=controlled-outbound-token", "--tapstate.cloud.atlas-uri=" + uri,
                "--tapstate.cloud.cluster-id=shared-source-cluster"));
        else args.add("--tapstate.store.mongo.uri=" + uri);
        ConfigurableApplicationContext context = new SpringApplicationBuilder(SharedConnectorSourceModesIT.Assembly.class)
                .environment(CloudFixtureEnvironment.isolated()).run(args.toArray(String[]::new));
        MongoClient raw = null;
        try {
            JdkClientHttpRequestFactory requests = new JdkClientHttpRequestFactory(HttpClient.newBuilder()
                    .connectTimeout(Duration.ofSeconds(2)).followRedirects(HttpClient.Redirect.NEVER).build());
            requests.setReadTimeout(Duration.ofSeconds(10));
            RestClient client = RestClient.builder().baseUrl("http://127.0.0.1:"
                            + ((WebServerApplicationContext) context).getWebServer().getPort())
                    .requestFactory(requests).build();
            String credential = login(client, cloud);
            RestClient authorized = client.mutate().defaultHeader(cloud ? HttpHeaders.COOKIE : HttpHeaders.AUTHORIZATION,
                    cloud ? credential : "Bearer " + credential).build();
            raw = MongoClients.create(uri);
            return new Fixture(context, raw, SystemCollections.ARTIFACTS.on(raw.getDatabase(database)), authorized);
        } catch (RuntimeException | Error failure) {
            try {
                context.close();
            } finally {
                if (raw != null) raw.close();
            }
            throw failure;
        }
    }

    private static String login(RestClient client, boolean cloud) {
        if (cloud) return client.get().uri("/auth/exchange?code=controlled-code")
                .exchange((request, response) -> {
                    assertThat(response.getStatusCode().value()).isEqualTo(302);
                    String cookie = response.getHeaders().getFirst(HttpHeaders.SET_COOKIE);
                    assertThat(cookie).startsWith("__Host-tapstate-cloud-session=tcs_")
                            .contains("Secure", "HttpOnly", "SameSite=Lax");
                    return cookie.substring(0, cookie.indexOf(';'));
                });
        client.post().uri("/auth/bootstrap").contentType(MediaType.APPLICATION_JSON)
                .body(Map.of("username", "mutation-admin", "password", "mutation-login-password"))
                .exchange((request, response) -> {
                    assertThat(response.getStatusCode().value()).isEqualTo(204);
                    return null;
                });
        String body = client.post().uri("/auth/login").contentType(MediaType.APPLICATION_JSON)
                .body(Map.of("username", "mutation-admin", "password", "mutation-login-password"))
                .retrieve().body(String.class);
        String token = JSON.readTree(body).path("token").asText();
        assertThat(token).isNotBlank();
        return token;
    }

    @SafeVarargs
    private static ApplyResult apply(Fixture fixture, Map<String, Object>... drafts) {
        return apply(fixture, List.of(drafts));
    }

    private static ApplyResult apply(Fixture fixture, List<Map<String, Object>> drafts) {
        Reply reply = post(fixture.client, "/api/artifacts:apply", Map.of("drafts", drafts));
        assertThat(reply.status()).isEqualTo(200);
        ApplyResult result = JSON.readValue(reply.body(), ApplyResult.class);
        assertThat(result.outcomes()).hasSize(drafts.size());
        return result;
    }

    private static String outcome(ApplyResult result, String id, ArtifactOutcome.Change change) {
        assertThat(result.outcomes()).filteredOn(value -> value.id().equals(id)).singleElement().satisfies(value -> {
            assertThat(value.kind()).isEqualTo("source");
            assertThat(value.change()).isEqualTo(change);
            assertThat(value.contentHash()).matches("[0-9a-f]{64}");
        });
        return result.outcomes().stream().filter(value -> value.id().equals(id)).findFirst().orElseThrow().contentHash();
    }

    private static Map<String, Object> draft(String content, String hash) {
        Map<String, Object> draft = new LinkedHashMap<>(Map.of("content", content));
        if (hash != null) draft.put("expectedContentHash", hash);
        return draft;
    }

    private static String source(String id, Map<String, Object> config, String description) {
        return sourceWithOnlyConfig(id, config) + "metadata: { description: " + description + " }\n";
    }

    private static String sourceWithOnlyConfig(String id, Map<String, Object> config) {
        return "version: tapstate/v1\nkind: source\nid: " + id + "\nconnector: mongodb\nconfig: "
                + JSON.writeValueAsString(config) + "\n";
    }

    private static String rollbackSource(String id, Map<String, Object> config) {
        return sourceWithOnlyConfig(id, config) + "metadata: { labels: { rollback: collision } }\n";
    }

    private static String uri(String secret) {
        return "mongodb://mutation-user:" + secret + "@mongo.example/rows";
    }

    private static Map<String, Object> atlasConfig(String secret) {
        return Map.of("isUri", true, "uri", "mongodb+srv://mutation-user:" + secret + "@cluster.example/rows");
    }

    private static Map<String, Object> typedReplacement(SourceView original, Map<String, Object> config, String description) {
        Map<String, Object> metadata = new LinkedHashMap<>(Map.of("description", description));
        if (Boolean.TRUE.equals(original.metadata().cloud())) {
            metadata.put("cloud", true);
            metadata.put("user_id", original.metadata().userId());
        }
        return Map.of("id", original.id(), "connector", original.connector(), "config", config,
                "metadata", metadata, "clearSecrets", List.of());
    }

    private static Reply replaceAtBarrier(Fixture fixture, SourceView original, String etag, String secret,
            String description, CountDownLatch ready, CountDownLatch release) throws InterruptedException {
        ready.countDown();
        assertThat(release.await(10, TimeUnit.SECONDS)).as("the owned replacement barrier was released").isTrue();
        return put(fixture.client, "/api/sources/" + original.id(), etag,
                typedReplacement(original, atlasConfig(secret), description));
    }

    private static Reply post(RestClient client, String path, Object body) {
        return client.post().uri(path).contentType(MediaType.APPLICATION_JSON).body(body)
                .exchange((request, response) -> reply(response.getStatusCode().value(),
                        response.bodyTo(String.class), response.getHeaders().getETag()));
    }

    private static Reply put(RestClient client, String path, String etag, Object body) {
        return client.put().uri(path).header(HttpHeaders.IF_MATCH, etag).contentType(MediaType.APPLICATION_JSON).body(body)
                .exchange((request, response) -> reply(response.getStatusCode().value(),
                        response.bodyTo(String.class), response.getHeaders().getETag()));
    }

    private static Reply reply(int status, String body, String etag) {
        assertThat(body).isNotBlank();
        noPlaintext(body);
        return new Reply(status, body, etag);
    }

    private static void assertState(Fixture fixture, String id, Map<String, Object> config, String hash) {
        SourceResource source = fixture.source(id);
        assertThat(source.id()).isEqualTo(id);
        assertThat(source.config()).containsExactlyInAnyOrderEntriesOf(config);
        assertThat(CanonicalHash.of(source)).isEqualTo(hash);
        assertThat(encrypted(fixture, id).getString("contentHash")).isEqualTo(hash);
        assertThat(publicSource(fixture, id).contentHash()).isEqualTo(hash);
    }

    private static StoredArtifact publicSource(Fixture fixture, String id) {
        StoredArtifact view = fixture.client.get().uri("/api/artifacts/" + id).retrieve().body(StoredArtifact.class);
        assertThat(view).isNotNull();
        assertThat(view.id()).isEqualTo(id);
        assertThat(view.kind()).isEqualTo("source");
        assertThat(view.contentHash()).isEqualTo(CanonicalHash.of(fixture.source(id)));
        assertThat(view.canonicalForm()).isNotBlank().doesNotContain("config:", "tscfg:", "<redacted-source>");
        noPlaintext(view.canonicalForm());
        SourceResource projection = (SourceResource) PARSER.parse(view.canonicalForm());
        assertThat(projection.id()).isEqualTo(id);
        assertThat(projection.connector()).isEqualTo(fixture.source(id).connector());
        assertThat(projection.config()).isEmpty();
        return view;
    }

    private static Document encrypted(Fixture fixture, String id) {
        Document document = fixture.artifacts.find(new Document("_id", id)).first();
        assertThat(document).as("the mutation persisted its Source").isNotNull();
        assertThat(document.get("body", Document.class).get("config"))
                .isInstanceOf(String.class).asString().startsWith("tscfg:1:");
        noPlaintext(document.toJson());
        return document;
    }

    private static void sameDocument(Fixture fixture, String id, Document expected) {
        assertThat(expected.equals(fixture.artifacts.find(new Document("_id", id)).first()))
                .as("a no-op or rejected mutation leaves the entire encrypted Source document untouched").isTrue();
    }

    private static void noPlaintext(String text) {
        assertThat(text).isNotNull();
        for (String marker : List.of(OLD_SECRET, ALPHA_SECRET, BETA_SECRET, PUBLIC_VALUE, "mutation-user")) {
            assertThat(text.contains(marker)).as("public responses and stored BSON contain no config plaintext").isFalse();
        }
    }

    private record Reply(int status, String body, String etag) {
        @Override public String toString() { return "Reply[status=" + status + ", etag=" + etag + "]"; }
    }

    private static final class Fixture implements AutoCloseable {
        private final ConfigurableApplicationContext context;
        private final MongoClient raw;
        private final MongoCollection<Document> artifacts;
        private final RestClient client;

        Fixture(ConfigurableApplicationContext context, MongoClient raw,
                MongoCollection<Document> artifacts, RestClient client) {
            this.context = context;
            this.raw = raw;
            this.artifacts = artifacts;
            this.client = client;
        }

        SourceResource source(String id) {
            return (SourceResource) context.getBean(ArtifactStore.class).get(id).orElseThrow();
        }

        @Override public void close() {
            try {
                context.close();
            } finally {
                raw.close();
            }
        }
    }
}
