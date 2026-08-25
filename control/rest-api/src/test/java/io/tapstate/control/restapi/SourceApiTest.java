package io.tapstate.control.restapi;

import tools.jackson.databind.JsonNode;
import tools.jackson.databind.ObjectMapper;
import io.tapstate.control.core.AuditGate;
import io.tapstate.control.core.ApplyService;
import io.tapstate.control.core.ArtifactMutationService;
import io.tapstate.control.core.ArtifactQueryService;
import io.tapstate.control.core.ControlOperations;
import io.tapstate.control.core.CredentialAuthenticator;
import io.tapstate.control.core.DataBrowserFollows;
import io.tapstate.control.core.GeneratedSecret;
import io.tapstate.control.core.OperationRegistry;
import io.tapstate.control.core.Scope;
import io.tapstate.control.core.PlanAdvisories;
import io.tapstate.control.core.TokenSecrets;
import io.tapstate.control.core.TokenService;
import io.tapstate.control.core.TokenSigner;
import io.tapstate.control.core.VerifiedToken;
import io.tapstate.core.catalog.TapstateCatalog;
import io.tapstate.core.model.PipelineResource;
import io.tapstate.core.model.Resource;
import io.tapstate.core.model.canonical.CanonicalHash;
import io.tapstate.core.model.canonical.CanonicalWriter;
import io.tapstate.spi.store.ArtifactMutation;
import io.tapstate.spi.store.ArtifactStore;
import io.tapstate.spi.store.AuditRecord;
import io.tapstate.spi.store.AuditStore;
import io.tapstate.spi.store.TokenRecord;
import io.tapstate.spi.store.TokenStore;
import io.tapstate.spi.store.SchemaStore;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.boot.SpringBootConfiguration;
import org.springframework.boot.autoconfigure.EnableAutoConfiguration;
import org.springframework.boot.builder.SpringApplicationBuilder;
import org.springframework.boot.jackson.autoconfigure.JsonMapperBuilderCustomizer;
import org.springframework.boot.web.server.context.WebServerApplicationContext;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Import;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpStatus;
import org.springframework.http.HttpStatusCode;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.web.client.RestClient;

import java.time.Clock;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import static org.assertj.core.api.Assertions.assertThat;

class SourceApiTest {

    private static final String SECRET = "sentinel-secret-value";
    private static final ObjectMapper JSON = new ObjectMapper();
    private static ConfigurableApplicationContext context;
    private static int port;

    @BeforeAll
    static void startServer() {
        context = new SpringApplicationBuilder(TestApp.class).properties("server.port=0").run();
        port = ((WebServerApplicationContext) context).getWebServer().getPort();
    }

    @AfterAll
    static void stopServer() {
        if (context != null) {
            context.close();
        }
    }

    @BeforeEach
    void reset() {
        context.getBean(InMemoryArtifactStore.class).clear();
        context.getBean(RecordingAuditStore.class).reset();
    }

    @Test
    void createListAndGetExposeTheExactStructuredSecretFreeContract() throws Exception {
        ResponseEntity<String> created = request("writer")
                .post().uri("/api/sources")
                .contentType(MediaType.APPLICATION_JSON).body(sourceJson("zeta", "before"))
                .retrieve().toEntity(String.class);

        assertThat(created.getStatusCode()).isEqualTo(HttpStatus.CREATED);
        assertThat(created.getHeaders().getLocation().getPath()).isEqualTo("/api/sources/zeta");
        assertThat(created.getHeaders().getETag()).matches("\"[0-9a-f]{64}\"");
        JsonNode createdView = JSON.readTree(created.getBody());
        assertSourceView(createdView, "zeta", "before");
        String etag = created.getHeaders().getETag();
        assertThat(createdView.path("contentHash").asText())
                .isEqualTo(etag.substring(1, etag.length() - 1));
        assertThat(created.getBody()).doesNotContain(SECRET).doesNotContain("clearSecrets");
        assertThat(context.getBean(RecordingAuditStore.class).records)
                .singleElement().extracting(AuditRecord::principal).isEqualTo("writer");

        create("alpha", "first");
        ResponseEntity<String> listed = request("reader").get().uri("/api/sources")
                .retrieve().toEntity(String.class);
        JsonNode list = JSON.readTree(listed.getBody());
        assertThat(list.propertyNames()).containsExactly("items");
        assertThat(List.of(
                list.path("items").get(0).path("id").asText(),
                list.path("items").get(1).path("id").asText())).containsExactly("alpha", "zeta");
        assertThat(listed.getBody()).doesNotContain(SECRET);

        ResponseEntity<String> got = request("reader").get().uri("/api/sources/zeta")
                .retrieve().toEntity(String.class);
        assertThat(got.getHeaders().getETag()).isEqualTo(created.getHeaders().getETag());
        assertThat(JSON.readTree(got.getBody())).isEqualTo(JSON.readTree(created.getBody()));
    }

    @Test
    void draftUsesReadScopeAndLeavesArtifactAndAuditStoresUntouched() throws Exception {
        ResponseEntity<String> drafted = request("reader")
                .post().uri("/api/sources:draft")
                .contentType(MediaType.APPLICATION_JSON).body(sourceJson("orders", "draft"))
                .retrieve().toEntity(String.class);

        assertThat(drafted.getStatusCode()).isEqualTo(HttpStatus.OK);
        assertThat(JSON.readTree(drafted.getBody()).path("yaml").asText())
                .contains("version: tapstate/v1", "kind: source", "id: orders", "password: " + SECRET);
        assertThat(context.getBean(InMemoryArtifactStore.class).list()).isEmpty();
        assertThat(context.getBean(RecordingAuditStore.class).records).isEmpty();
    }

    @Test
    void replaceRequiresOneStrongQuotedEtagAndDeleteUsesTheNewVersion() throws Exception {
        String etag = create("orders", "before").getHeaders().getETag();

        assertError(request("writer").delete().uri("/api/sources/orders"),
                HttpStatus.PRECONDITION_REQUIRED, "source.precondition-required");

        for (String malformed : List.of("", etag.substring(1, etag.length() - 1), "W/" + etag,
                "*", etag + ", " + etag, "\"short\"")) {
            ApiError error = put("orders", sourceJson("orders", "after"), malformed);
            assertThat(error.code()).isEqualTo("source.precondition-required");
        }

        ApiError stale = put("orders", sourceJson("orders", "after"), "\"" + "0".repeat(64) + "\"");
        assertThat(stale.code()).isEqualTo("source.version-conflict");

        assertError(request("writer").put().uri("/api/sources/missing")
                        .header(HttpHeaders.IF_MATCH, "\"" + "0".repeat(64) + "\"")
                        .contentType(MediaType.APPLICATION_JSON).body(sourceJson("missing", "after")),
                HttpStatus.NOT_FOUND, "source.not-found");

        ResponseEntity<String> replaced = request("writer").put().uri("/api/sources/orders")
                .header(HttpHeaders.IF_MATCH, etag).contentType(MediaType.APPLICATION_JSON)
                .body(sourceJson("orders", "after")).retrieve().toEntity(String.class);
        assertThat(replaced.getStatusCode()).isEqualTo(HttpStatus.OK);
        assertThat(replaced.getHeaders().getETag()).matches("\"[0-9a-f]{64}\"").isNotEqualTo(etag);
        assertThat(JSON.readTree(replaced.getBody()).path("metadata").path("description").asText())
                .isEqualTo("after");

        HttpStatusCode deleted = request("writer").delete().uri("/api/sources/orders")
                .header(HttpHeaders.IF_MATCH, replaced.getHeaders().getETag())
                .exchange((req, res) -> res.getStatusCode());
        assertThat(deleted).isEqualTo(HttpStatus.NO_CONTENT);
        assertError(request("reader").get().uri("/api/sources/orders"), HttpStatus.NOT_FOUND, "source.not-found");
    }

    @Test
    void concurrentReplacesWithTheSameEtagAllowExactlyOneWinner() throws Exception {
        String etag = create("orders", "before").getHeaders().getETag();
        CountDownLatch ready = new CountDownLatch(2);
        CountDownLatch start = new CountDownLatch(1);

        try (ExecutorService callers = Executors.newFixedThreadPool(2)) {
            Future<RaceResult> alpha = callers.submit(
                    () -> replaceAtBarrier(etag, "alpha-winner", ready, start));
            Future<RaceResult> beta = callers.submit(
                    () -> replaceAtBarrier(etag, "beta-winner", ready, start));
            ready.await();
            start.countDown();

            List<RaceResult> outcomes = List.of(alpha.get(), beta.get());
            assertThat(outcomes).extracting(RaceResult::status)
                    .containsExactlyInAnyOrder(HttpStatus.OK, HttpStatus.PRECONDITION_FAILED);
            assertThat(outcomes).filteredOn(result -> result.status() == HttpStatus.PRECONDITION_FAILED)
                    .singleElement().extracting(RaceResult::code).isEqualTo("source.version-conflict");
            String winner = outcomes.stream()
                    .filter(result -> result.status() == HttpStatus.OK)
                    .findFirst().orElseThrow().description();

            String stored = request("reader").get().uri("/api/sources/orders")
                    .retrieve().body(String.class);
            assertThat(JSON.readTree(stored).path("metadata").path("description").asText())
                    .isEqualTo(winner);
        }
    }

    @Test
    void concurrentCreatesOfTheSameIdAllowExactlyOneWinner() throws Exception {
        CountDownLatch ready = new CountDownLatch(2);
        CountDownLatch start = new CountDownLatch(1);

        try (ExecutorService callers = Executors.newFixedThreadPool(2)) {
            Future<RaceResult> alpha = callers.submit(
                    () -> createAtBarrier("alpha", ready, start));
            Future<RaceResult> beta = callers.submit(
                    () -> createAtBarrier("beta", ready, start));
            ready.await();
            start.countDown();

            List<RaceResult> outcomes = List.of(alpha.get(), beta.get());
            assertThat(outcomes).extracting(RaceResult::status)
                    .containsExactlyInAnyOrder(HttpStatus.CREATED, HttpStatus.CONFLICT);
            assertThat(outcomes).filteredOn(result -> result.status() == HttpStatus.CONFLICT)
                    .singleElement().extracting(RaceResult::code).isEqualTo("source.already-exists");
            String winner = outcomes.stream()
                    .filter(result -> result.status() == HttpStatus.CREATED)
                    .findFirst().orElseThrow().description();
            String stored = request("reader").get().uri("/api/sources/orders")
                    .retrieve().body(String.class);
            assertThat(JSON.readTree(stored).path("metadata").path("description").asText())
                    .isEqualTo(winner);
        }
    }

    @Test
    void mapsDomainConflictsAndRejectsResponseOnlyOrUnknownRequestFields() {
        ResponseEntity<String> created = create("orders", "before");
        assertError(request("writer").post().uri("/api/sources")
                        .contentType(MediaType.APPLICATION_JSON).body(sourceJson("orders", "again")),
                HttpStatus.CONFLICT, "source.already-exists");

        assertError(request("writer").put().uri("/api/sources/orders")
                        .header(HttpHeaders.IF_MATCH, created.getHeaders().getETag())
                        .contentType(MediaType.APPLICATION_JSON).body(sourceJson("other", "after")),
                HttpStatus.BAD_REQUEST, "source.id-mismatch");

        for (String field : List.of("configuredSecrets", "contentHash", "unknownField")) {
            String body = sourceJson("bad", "bad").replaceFirst("\\}$", ",\"" + field + "\":[]}");
            assertError(request("writer").post().uri("/api/sources")
                            .contentType(MediaType.APPLICATION_JSON).body(body),
                    HttpStatus.BAD_REQUEST, "control.malformed-request");
        }
        assertError(request("writer").post().uri("/api/sources")
                        .contentType(MediaType.APPLICATION_JSON).body("{\"id\":"),
                HttpStatus.BAD_REQUEST, "control.malformed-request");

        context.getBean(InMemoryArtifactStore.class).save(
                new PipelineResource("pipeline", null, List.of("orders"), null, null, null, null, null));
        assertError(request("writer").delete().uri("/api/sources/orders")
                        .header(HttpHeaders.IF_MATCH, created.getHeaders().getETag()),
                HttpStatus.CONFLICT, "source.in-use");
    }

    @Test
    void sourceCreateRejectsMissingLiveConnectorConfigBeforePersisting() {
        String missingDatabase = sourceJson("missing-config", "bad")
                .replace("\"database\":\"orders\",", "");

        assertError(request("writer").post().uri("/api/sources")
                        .contentType(MediaType.APPLICATION_JSON).body(missingDatabase),
                HttpStatus.BAD_REQUEST, "dsl.config-required");
        assertThat(context.getBean(InMemoryArtifactStore.class).get("missing-config")).isEmpty();
    }

    @Test
    void authenticationScopesAndAuditGateProtectEveryOperation() {
        assertError(RestClient.create(base()).get().uri("/api/sources"),
                HttpStatus.UNAUTHORIZED, "control.unauthenticated");
        assertError(request("invalid").get().uri("/api/sources"),
                HttpStatus.UNAUTHORIZED, "control.unauthenticated");
        assertError(request("reader").post().uri("/api/sources")
                        .contentType(MediaType.APPLICATION_JSON).body(sourceJson("orders", "before")),
                HttpStatus.FORBIDDEN, "control.forbidden");

        context.getBean(RecordingAuditStore.class).fail = true;
        assertError(request("writer").post().uri("/api/sources")
                        .contentType(MediaType.APPLICATION_JSON).body(sourceJson("orders", "before")),
                HttpStatus.INTERNAL_SERVER_ERROR, "control.audit-blocked");
        assertThat(context.getBean(InMemoryArtifactStore.class).get("orders")).isEmpty();
    }

    private static void assertSourceView(JsonNode view, String id, String description) {
        assertThat(view.propertyNames()).containsExactlyInAnyOrder(
                "id", "metadata", "connector", "config", "configuredSecrets", "mode", "tables",
                "options", "srs", "experimental", "contentHash");
        assertThat(view.path("id").asText()).isEqualTo(id);
        assertThat(view.path("metadata").path("labels").propertyNames()).containsExactly("team");
        assertThat(view.path("metadata").path("labels").path("team").asText()).isEqualTo("finance");
        assertThat(view.path("metadata").path("description").asText()).isEqualTo(description);
        assertThat(view.path("connector").asText()).isEqualTo("mysql");
        assertThat(view.path("mode").asText()).isEqualTo("cdc");
        assertThat(view.path("config").propertyNames())
                .containsExactlyInAnyOrder("host", "port", "database", "username");
        assertThat(view.path("config").path("host").asText()).isEqualTo("localhost");
        assertThat(view.path("config").path("port").isIntegralNumber()).isTrue();
        assertThat(view.path("config").path("port").asInt()).isEqualTo(3306);
        assertThat(view.path("config").path("database").asText()).isEqualTo("orders");
        assertThat(view.path("config").path("username").asText()).isEqualTo("app");
        assertThat(view.path("configuredSecrets").get(0).asText()).isEqualTo("password");
        assertThat(view.path("tables").get(0).propertyNames()).containsExactly("type", "name");
        assertThat(view.path("tables").get(0).path("type").asText()).isEqualTo("literal");
        assertThat(view.path("tables").get(0).path("name").asText()).isEqualTo("orders");
        assertThat(view.path("tables").get(1).propertyNames()).containsExactly("type", "pattern");
        assertThat(view.path("tables").get(1).path("type").asText()).isEqualTo("regex");
        assertThat(view.path("tables").get(1).path("pattern").asText()).isEqualTo("audit_.*");
        assertThat(view.path("tables").get(2).propertyNames())
                .containsExactly("type", "name", "filter", "pk", "options");
        assertThat(view.path("tables").get(2).path("type").asText()).isEqualTo("spec");
        assertThat(view.path("tables").get(2).path("name").asText()).isEqualTo("customers");
        assertThat(view.path("tables").get(2).path("filter").asText()).isEqualTo("active == true");
        assertThat(view.path("tables").get(2).path("pk").get(0).asText()).isEqualTo("id");
        assertThat(view.path("tables").get(2).path("options").isEmpty()).isTrue();
        JsonNode srs = view.path("srs");
        assertThat(srs.propertyNames()).containsExactlyInAnyOrder(
                "key", "retention", "schemaEvolution", "queryable", "enabled");
        assertThat(srs.path("key").asText()).isEqualTo("mysql-primary");
        assertThat(srs.path("retention").asText()).isEqualTo("24h");
        assertThat(srs.path("schemaEvolution").asText()).isEqualTo("track");
        assertThat(srs.path("queryable").isBoolean()).isTrue();
        assertThat(srs.path("queryable").asBoolean()).isFalse();
        assertThat(srs.path("enabled").isBoolean()).isTrue();
        assertThat(srs.path("enabled").asBoolean()).isTrue();
        assertThat(view.path("options").isEmpty()).isTrue();
        assertThat(view.path("experimental").isEmpty()).isTrue();
        assertThat(view.path("contentHash").asText()).matches("[0-9a-f]{64}");
    }

    private RaceResult replaceAtBarrier(
            String etag, String description, CountDownLatch ready, CountDownLatch start) throws Exception {
        ready.countDown();
        start.await();
        return request("writer").put().uri("/api/sources/orders")
                .header(HttpHeaders.IF_MATCH, etag)
                .contentType(MediaType.APPLICATION_JSON)
                .body(sourceJson("orders", description))
                .exchange((req, res) -> {
                    HttpStatus status = HttpStatus.valueOf(res.getStatusCode().value());
                    if (status == HttpStatus.OK) {
                        String body = res.bodyTo(String.class);
                        String storedDescription = JSON.readTree(body)
                                .path("metadata").path("description").asText();
                        return new RaceResult(status, null, storedDescription);
                    }
                    ApiError error = res.bodyTo(ApiError.class);
                    return new RaceResult(status, error.code(), null);
                });
    }

    private RaceResult createAtBarrier(
            String description, CountDownLatch ready, CountDownLatch start) throws Exception {
        ready.countDown();
        start.await();
        return request("writer").post().uri("/api/sources")
                .contentType(MediaType.APPLICATION_JSON)
                .body(sourceJson("orders", description))
                .exchange((req, res) -> {
                    HttpStatus status = HttpStatus.valueOf(res.getStatusCode().value());
                    if (status == HttpStatus.CREATED) {
                        String body = res.bodyTo(String.class);
                        String storedDescription = JSON.readTree(body)
                                .path("metadata").path("description").asText();
                        return new RaceResult(status, null, storedDescription);
                    }
                    ApiError error = res.bodyTo(ApiError.class);
                    return new RaceResult(status, error.code(), null);
                });
    }

    private ResponseEntity<String> create(String id, String description) {
        return request("writer").post().uri("/api/sources")
                .contentType(MediaType.APPLICATION_JSON).body(sourceJson(id, description))
                .retrieve().toEntity(String.class);
    }

    private ApiError put(String id, String body, String ifMatch) {
        RestClient.RequestBodySpec request = request("writer").put().uri("/api/sources/" + id)
                .contentType(MediaType.APPLICATION_JSON).body(body);
        if (!ifMatch.isEmpty()) {
            request.header(HttpHeaders.IF_MATCH, ifMatch);
        }
        return request.exchange((req, res) -> {
            assertThat(res.getStatusCode()).isIn(HttpStatus.PRECONDITION_REQUIRED, HttpStatus.PRECONDITION_FAILED);
            return res.bodyTo(ApiError.class);
        });
    }

    private static void assertError(RestClient.RequestHeadersSpec<?> request, HttpStatus status, String code) {
        ApiError error = request.exchange((req, res) -> {
            assertThat(res.getStatusCode()).isEqualTo(status);
            return res.bodyTo(ApiError.class);
        });
        assertThat(error.code()).isEqualTo(code);
    }

    private RestClient request(String token) {
        return RestClient.builder().baseUrl(base())
                .defaultHeader(HttpHeaders.AUTHORIZATION, "Bearer " + token).build();
    }

    private static String base() {
        return "http://localhost:" + port;
    }

    private static String sourceJson(String id, String description) {
        return """
                {"id":"%s","metadata":{"labels":{"team":"finance"},"description":"%s"},
                 "connector":"mysql","config":{"host":"localhost","port":3306,"database":"orders","username":"app","password":"%s"},
                 "mode":"cdc","tables":[{"type":"literal","name":"orders"},
                 {"type":"regex","pattern":"audit_.*"},
                 {"type":"spec","name":"customers","filter":"active == true","pk":["id"],"options":{}}],
                 "options":{},"srs":{"key":"mysql-primary","retention":"24h",
                 "schemaEvolution":"track","queryable":false,"enabled":true},
                 "experimental":{},"clearSecrets":[]}
                """.formatted(id, description, SECRET);
    }

    @SpringBootConfiguration
    @EnableAutoConfiguration
    @Import({RestApiConfiguration.class, RestApiSecurityConfiguration.class,
            SourceDraftTestConfiguration.class, SourceProjectionServiceTestConfiguration.class,
            SourceController.class,
            SourceDraftController.class,
            ApiExceptionHandler.class})
    static class TestApp {
        @Bean InMemoryArtifactStore artifactStore() { return new InMemoryArtifactStore(); }
        @Bean RecordingAuditStore auditStore() { return new RecordingAuditStore(); }
        @Bean Clock clock() { return Clock.fixed(Instant.parse("2026-07-13T00:00:00Z"), ZoneOffset.UTC); }
        @Bean AuditGate auditGate(AuditStore auditStore, Clock clock) {
            return new AuditGate(auditStore, clock);
        }
        @Bean ApplyService applyService(ArtifactStore store, AuditGate auditGate) {
            return new ApplyService(TapstateCatalog::load, store, auditGate, new EmptySchemaStore(),
                    PlanAdvisories.none());
        }
        @Bean ArtifactQueryService artifactQueryService(ArtifactStore store) {
            return new ArtifactQueryService(store);
        }
        @Bean ArtifactMutationService artifactMutationService(ArtifactStore store, AuditGate auditGate) {
            return new ArtifactMutationService(
                    store, NoReclaimStores.desired(), NoReclaimStores.state(),
                    NoReclaimStores.observations(), NoReclaimStores.srsMeta(), auditGate, DataBrowserFollows.NONE);
        }
        @Bean OperationRegistry operationRegistry() { return ControlOperations.registry(); }
        @Bean TokenStore tokenStore() { return new EmptyTokenStore(); }
        @Bean TokenSecrets tokenSecrets() { return new EmptyTokenSecrets(); }
        @Bean TokenService tokenService(TokenStore store, TokenSecrets secrets, Clock clock) {
            return new TokenService(store, secrets, clock);
        }
        @Bean TokenSigner tokenSigner() { return new FixedSigner(); }
        @Bean JsonMapperBuilderCustomizer sourceJsonContract() {
            return new ControlHttpFace().sourceJsonContract();
        }
    }

    private static final class RecordingAuditStore implements AuditStore {
        private final List<AuditRecord> records = new ArrayList<>();
        private boolean fail;
        public void record(AuditRecord record) { if (fail) throw new IllegalStateException("down"); records.add(record); }
        void reset() { records.clear(); fail = false; }
    }

    private record RaceResult(HttpStatus status, String code, String description) {
    }

    private static final class FixedSigner implements TokenSigner {
        public String issue(String subject, Scope scope) { return subject; }
        public Optional<VerifiedToken> verify(String token) {
            return switch (token) {
                case "reader" -> Optional.of(new VerifiedToken("reader", Scope.READ));
                case "writer" -> Optional.of(new VerifiedToken("writer", Scope.WRITE));
                default -> Optional.empty();
            };
        }
    }

    private static final class EmptyTokenStore implements TokenStore {
        public void save(TokenRecord record) { }
        public Optional<TokenRecord> find(String tokenId) { return Optional.empty(); }
        public void revoke(String tokenId) { }
        public List<TokenRecord> list() { return List.of(); }
    }

    private static final class EmptyTokenSecrets implements TokenSecrets {
        public GeneratedSecret generate() { throw new UnsupportedOperationException(); }
        public boolean matches(String presentedSecret, String storedHash) { return false; }
    }

    private static final class InMemoryArtifactStore implements ArtifactStore {
        private final Map<String, Resource> byId = new LinkedHashMap<>();
        synchronized void clear() { byId.clear(); }
        public synchronized ArtifactMutation create(Resource artifact) {
            if (byId.containsKey(artifact.id())) return ArtifactMutation.ALREADY_EXISTS;
            byId.put(artifact.id(), artifact); return ArtifactMutation.CREATED;
        }
        public synchronized ArtifactMutation replace(String id, String hash, Resource replacement) {
            Resource existing = byId.get(id);
            if (existing == null) return ArtifactMutation.NOT_FOUND;
            if (!hash(existing).equals(hash)) return ArtifactMutation.VERSION_CONFLICT;
            byId.put(id, replacement); return ArtifactMutation.REPLACED;
        }
        public synchronized ArtifactMutation delete(String id, String hash) {
            Resource existing = byId.get(id);
            if (existing == null) return ArtifactMutation.NOT_FOUND;
            if (!hash(existing).equals(hash)) return ArtifactMutation.VERSION_CONFLICT;
            byId.remove(id); return ArtifactMutation.DELETED;
        }
        public synchronized void saveAll(List<Resource> resources) { resources.forEach(r -> byId.put(r.id(), r)); }
        public synchronized Optional<Resource> get(String id) { return Optional.ofNullable(byId.get(id)); }
        public synchronized List<Resource> list() { return new ArrayList<>(byId.values()); }
        private static String hash(Resource resource) {
            return CanonicalHash.of(new CanonicalWriter().write(resource));
        }
    }
}
