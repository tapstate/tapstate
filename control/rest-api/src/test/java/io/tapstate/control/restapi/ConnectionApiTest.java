package io.tapstate.control.restapi;

import io.tapstate.control.core.AuditGate;
import io.tapstate.control.core.ConnectionTestReport;
import io.tapstate.control.core.ConnectionTestResultQueryService;
import io.tapstate.control.core.ConnectionTestService;
import io.tapstate.control.core.ControlOperations;
import io.tapstate.control.core.CredentialAuthenticator;
import io.tapstate.control.core.GeneratedSecret;
import io.tapstate.control.core.OperationRegistry;
import io.tapstate.control.core.SchemaDiscoveryService;
import io.tapstate.control.core.SchemaQueryService;
import io.tapstate.control.core.SchemaReport;
import io.tapstate.control.core.Scope;
import io.tapstate.control.core.TokenSecrets;
import io.tapstate.control.core.TokenService;
import io.tapstate.control.core.TokenSigner;
import io.tapstate.control.core.VerifiedToken;
import io.tapstate.runtime.probe.ConnectionProbe;
import io.tapstate.runtime.probe.SchemaDiscoveryProbe;
import io.tapstate.spi.store.AuditRecord;
import io.tapstate.spi.store.AuditStore;
import io.tapstate.spi.store.ConnectionConfig;
import io.tapstate.spi.store.ConnectionTestItem;
import io.tapstate.spi.store.ConnectionTestResult;
import io.tapstate.spi.store.ConnectionTestResultStore;
import io.tapstate.spi.store.DiscoveredSourceModel;
import io.tapstate.spi.store.SchemaStore;
import io.tapstate.spi.store.SourceField;
import io.tapstate.spi.store.SourceModel;
import io.tapstate.spi.store.SourceTable;
import io.tapstate.spi.store.TokenRecord;
import io.tapstate.spi.store.TokenStore;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.boot.SpringBootConfiguration;
import org.springframework.boot.autoconfigure.EnableAutoConfiguration;
import org.springframework.boot.builder.SpringApplicationBuilder;
import org.springframework.boot.web.server.context.WebServerApplicationContext;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Import;
import org.springframework.http.HttpStatus;
import org.springframework.http.HttpStatusCode;
import org.springframework.http.MediaType;
import org.springframework.web.client.RestClient;

import java.time.Clock;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * The connection-plane verbs projected onto HTTP, exercised end to end through a real embedded server: an
 * authenticated caller posts a connection to test or to discover, the controller drives it through the
 * (fake) runtime probe under the audit gate, persists the result / the discovered model as that
 * connection's latest, and returns it. These are the two synchronous control-to-runtime verbs the
 * whitelist opens.
 *
 * <p>The probe and the stores are in-memory fakes so the test needs no connector or PDK; the authentication
 * stack, the audit gate and the controller wiring are real, so the principal the operation is audited to is
 * the authenticated session's — read from the guard, not from the request body. The context is booted
 * programmatically so the module stays on the reactor's JUnit line.
 */
class ConnectionApiTest {

    private static final Instant NOW = Instant.parse("2026-07-11T12:00:00Z");

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
    void resetStores() {
        context.getBean(RecordingConnectionProbe.class).clear();
        context.getBean(RecordingSchemaDiscoveryProbe.class).clear();
        context.getBean(InMemoryConnectionTestResultStore.class).clear();
        context.getBean(InMemorySchemaStore.class).clear();
        context.getBean(RecordingAuditStore.class).clear();
        context.getBean(FakeTokenStore.class).clear();
    }

    private RestClient client() {
        return RestClient.create("http://127.0.0.1:" + port);
    }

    /** Mints a machine token of the given grade through the real token service and returns the bearer string. */
    private String token(Scope scope) {
        return context.getBean(TokenService.class).create(scope);
    }

    private static Map<String, Object> testBody(String id, String connectorId, Map<String, Object> settings) {
        Map<String, Object> body = new LinkedHashMap<>();
        body.put("id", id);
        body.put("connectorId", connectorId);
        body.put("settings", settings);
        return body;
    }

    // ---- the verb drives the probe and projects its result onto HTTP ----

    @Test
    void drivesTheProbeWithThePostedConnectionAndReturnsItsResult() {
        Map<String, Object> settings = Map.of("host", "10.20.0.15", "username", "cdc_user");

        ConnectionTestReport result = client().post().uri("/api/connections:test")
                .header("Authorization", "Bearer " + token(Scope.WRITE))
                .contentType(MediaType.APPLICATION_JSON)
                .body(testBody("conn_ora", "oracle", settings))
                .retrieve().toEntity(ConnectionTestReport.class).getBody();

        // The response is the control-ring report: overall outcome plus the per-check reports.
        assertThat(result.connectionId()).isEqualTo("conn_ora");
        assertThat(result.connectorId()).isEqualTo("oracle");
        assertThat(result.outcome()).isEqualTo(ConnectionTestReport.Outcome.PASSED);
        assertThat(result.checks()).singleElement().satisfies(check -> {
            assertThat(check.name()).isEqualTo("Connection");
            assertThat(check.status()).isEqualTo(ConnectionTestReport.Check.Status.PASSED);
        });

        // The controller drove the probe with exactly the posted connection — id, connector and settings.
        ConnectionConfig probed = context.getBean(RecordingConnectionProbe.class).captured();
        assertThat(probed.id()).isEqualTo("conn_ora");
        assertThat(probed.connectorId()).isEqualTo("oracle");
        assertThat(probed.settings()).isEqualTo(settings);
    }

    @Test
    void persistsTheResultAsTheConnectionsLatest() {
        client().post().uri("/api/connections:test")
                .header("Authorization", "Bearer " + token(Scope.WRITE))
                .contentType(MediaType.APPLICATION_JSON)
                .body(testBody("conn_ora", "oracle", Map.of()))
                .retrieve().toBodilessEntity();

        Optional<ConnectionTestResult> stored =
                context.getBean(InMemoryConnectionTestResultStore.class).find("conn_ora");
        assertThat(stored).isPresent();
        assertThat(stored.get().outcome()).isEqualTo(ConnectionTestResult.Outcome.PASSED);
    }

    @Test
    void isAuditedToTheAuthenticatedPrincipalNotTheRequestBody() {
        String bearer = token(Scope.WRITE);
        // The principal the audit must be attributed to is the machine token's verified subject, never
        // anything the request body could forge.
        String expectedPrincipal = context.getBean(TokenService.class).authenticate(bearer).orElseThrow().subject();

        client().post().uri("/api/connections:test")
                .header("Authorization", "Bearer " + bearer)
                .contentType(MediaType.APPLICATION_JSON)
                .body(testBody("conn_ora", "oracle", Map.of()))
                .retrieve().toBodilessEntity();

        assertThat(context.getBean(RecordingAuditStore.class).records()).singleElement().satisfies(record -> {
            assertThat(record.operationId()).isEqualTo("connection.test");
            assertThat(record.principal()).isEqualTo(expectedPrincipal);
            assertThat(record.resourceId()).isEqualTo("conn_ora");
        });
    }

    // ---- the persisted result is queryable through a read verb ----

    @Test
    void returnsThePersistedResultForATestedConnection() {
        // Run a test to persist a result, then read it back through the read verb: a plain READ credential
        // suffices (the read-back is read-scoped, unlike the write that produced it).
        client().post().uri("/api/connections:test")
                .header("Authorization", "Bearer " + token(Scope.WRITE))
                .contentType(MediaType.APPLICATION_JSON)
                .body(testBody("conn_ora", "oracle", Map.of()))
                .retrieve().toBodilessEntity();

        ConnectionTestReport report = client().get().uri("/api/connections/conn_ora/test-result")
                .header("Authorization", "Bearer " + token(Scope.READ))
                .retrieve().toEntity(ConnectionTestReport.class).getBody();

        assertThat(report.connectionId()).isEqualTo("conn_ora");
        assertThat(report.connectorId()).isEqualTo("oracle");
        assertThat(report.outcome()).isEqualTo(ConnectionTestReport.Outcome.PASSED);
        assertThat(report.checks()).singleElement().satisfies(check -> {
            assertThat(check.name()).isEqualTo("Connection");
            assertThat(check.status()).isEqualTo(ConnectionTestReport.Check.Status.PASSED);
        });
    }

    @Test
    void isNotFoundForAConnectionThatWasNeverTested() {
        // A connection with no stored result is a 404 (never tested), the same absent semantics as artifact.get.
        HttpStatusCode status = client().get().uri("/api/connections/never_tested/test-result")
                .header("Authorization", "Bearer " + token(Scope.READ))
                .exchange((request, response) -> response.getStatusCode());

        assertThat(status).isEqualTo(HttpStatus.NOT_FOUND);
    }

    // ---- the verb is a write, guarded like every other ----

    @Test
    void requiresAWriteCredential() {
        ApiError body = client().post().uri("/api/connections:test")
                .header("Authorization", "Bearer " + token(Scope.READ))
                .contentType(MediaType.APPLICATION_JSON)
                .body(testBody("conn_ora", "oracle", Map.of()))
                .exchange((request, response) -> {
                    assertThat(response.getStatusCode()).isEqualTo(HttpStatus.FORBIDDEN);
                    return response.bodyTo(ApiError.class);
                });

        assertThat(body.code()).isEqualTo("control.forbidden");
        assertThat(body.params()).containsEntry("op", "connection.test").containsEntry("required", "write");
    }

    @Test
    void anUnauthenticatedCallerCannotReachTheVerb() {
        HttpStatusCode status = client().post().uri("/api/connections:test")
                .contentType(MediaType.APPLICATION_JSON)
                .body(testBody("conn_ora", "oracle", Map.of()))
                .exchange((request, response) -> response.getStatusCode());

        assertThat(status).isEqualTo(HttpStatus.UNAUTHORIZED);
    }

    // ---- a structurally malformed body is refused at the boundary, not deeper as a bare crash ----

    @Test
    void aBlankConnectorIdIsABadRequestWithACodedBody() {
        // A blank connectorId would otherwise trip the ConnectionConfig invariant guard as a bare 500; it is
        // a client-attributable malformed request, refused at the boundary as a coded 400.
        ApiError body = client().post().uri("/api/connections:test")
                .header("Authorization", "Bearer " + token(Scope.WRITE))
                .contentType(MediaType.APPLICATION_JSON)
                .body(testBody("conn_ora", "", Map.of()))
                .exchange((request, response) -> {
                    assertThat(response.getStatusCode()).isEqualTo(HttpStatus.BAD_REQUEST);
                    return response.bodyTo(ApiError.class);
                });

        assertThat(body.code()).isEqualTo("control.malformed-request");
        assertThat(body.params()).containsKey("reason");
    }

    // ---- the discover-schema verb drives the discovery probe and persists the model ----

    @Test
    void drivesTheDiscoveryProbeWithThePostedConnectionAndReturnsItsReport() {
        Map<String, Object> settings = Map.of("host", "10.20.0.15");

        SchemaReport report = client().post().uri("/api/connections:discover-schema")
                .header("Authorization", "Bearer " + token(Scope.WRITE))
                .contentType(MediaType.APPLICATION_JSON)
                .body(testBody("conn_ora", "oracle", settings))
                .retrieve().toEntity(SchemaReport.class).getBody();

        // The response is the control-ring report: the discovered tables plus the discovery time.
        assertThat(report.connectionId()).isEqualTo("conn_ora");
        assertThat(report.connectorId()).isEqualTo("oracle");
        assertThat(report.discoveredAt()).isEqualTo(NOW.toEpochMilli());
        assertThat(report.tables()).singleElement().satisfies(table -> {
            assertThat(table.name()).isEqualTo("orders");
            assertThat(table.fields()).extracting(SchemaReport.Field::name).containsExactly("id");
            assertThat(table.primaryKey()).containsExactly("id");
        });

        // The controller drove the discovery probe with exactly the posted connection.
        ConnectionConfig probed = context.getBean(RecordingSchemaDiscoveryProbe.class).captured();
        assertThat(probed.id()).isEqualTo("conn_ora");
        assertThat(probed.connectorId()).isEqualTo("oracle");
        assertThat(probed.settings()).isEqualTo(settings);
    }

    @Test
    void persistsTheDiscoveredModelAsTheConnectionsLatest() {
        client().post().uri("/api/connections:discover-schema")
                .header("Authorization", "Bearer " + token(Scope.WRITE))
                .contentType(MediaType.APPLICATION_JSON)
                .body(testBody("conn_ora", "oracle", Map.of()))
                .retrieve().toBodilessEntity();

        Optional<DiscoveredSourceModel> stored = context.getBean(InMemorySchemaStore.class).get("conn_ora");
        assertThat(stored).isPresent();
        assertThat(stored.get().connectorId()).isEqualTo("oracle");
        assertThat(stored.get().model().tables()).extracting(SourceTable::name).containsExactly("orders");
    }

    @Test
    void auditsTheDiscoveryToTheAuthenticatedPrincipal() {
        String bearer = token(Scope.WRITE);
        String expectedPrincipal = context.getBean(TokenService.class).authenticate(bearer).orElseThrow().subject();

        client().post().uri("/api/connections:discover-schema")
                .header("Authorization", "Bearer " + bearer)
                .contentType(MediaType.APPLICATION_JSON)
                .body(testBody("conn_ora", "oracle", Map.of()))
                .retrieve().toBodilessEntity();

        assertThat(context.getBean(RecordingAuditStore.class).records()).singleElement().satisfies(record -> {
            assertThat(record.operationId()).isEqualTo("connection.discover-schema");
            assertThat(record.principal()).isEqualTo(expectedPrincipal);
            assertThat(record.resourceId()).isEqualTo("conn_ora");
        });
    }

    @Test
    void discoveryRequiresAWriteCredential() {
        ApiError body = client().post().uri("/api/connections:discover-schema")
                .header("Authorization", "Bearer " + token(Scope.READ))
                .contentType(MediaType.APPLICATION_JSON)
                .body(testBody("conn_ora", "oracle", Map.of()))
                .exchange((request, response) -> {
                    assertThat(response.getStatusCode()).isEqualTo(HttpStatus.FORBIDDEN);
                    return response.bodyTo(ApiError.class);
                });

        assertThat(body.code()).isEqualTo("control.forbidden");
        assertThat(body.params()).containsEntry("op", "connection.discover-schema").containsEntry("required", "write");
    }

    @Test
    void anUnauthenticatedCallerCannotReachTheDiscoverSchemaVerb() {
        HttpStatusCode status = client().post().uri("/api/connections:discover-schema")
                .contentType(MediaType.APPLICATION_JSON)
                .body(testBody("conn_ora", "oracle", Map.of()))
                .exchange((request, response) -> response.getStatusCode());

        assertThat(status).isEqualTo(HttpStatus.UNAUTHORIZED);
    }

    @Test
    void aDiscoveryBodyWithABlankConnectorIdIsABadRequestWithACodedBody() {
        ApiError body = client().post().uri("/api/connections:discover-schema")
                .header("Authorization", "Bearer " + token(Scope.WRITE))
                .contentType(MediaType.APPLICATION_JSON)
                .body(testBody("conn_ora", "", Map.of()))
                .exchange((request, response) -> {
                    assertThat(response.getStatusCode()).isEqualTo(HttpStatus.BAD_REQUEST);
                    return response.bodyTo(ApiError.class);
                });

        assertThat(body.code()).isEqualTo("control.malformed-request");
        assertThat(body.params()).containsKey("reason");
    }

    // ---- the persisted model is queryable through a read verb ----

    @Test
    void returnsThePersistedModelForADiscoveredConnection() {
        client().post().uri("/api/connections:discover-schema")
                .header("Authorization", "Bearer " + token(Scope.WRITE))
                .contentType(MediaType.APPLICATION_JSON)
                .body(testBody("conn_ora", "oracle", Map.of()))
                .retrieve().toBodilessEntity();

        SchemaReport report = client().get().uri("/api/connections/conn_ora/schema")
                .header("Authorization", "Bearer " + token(Scope.READ))
                .retrieve().toEntity(SchemaReport.class).getBody();

        assertThat(report.connectionId()).isEqualTo("conn_ora");
        assertThat(report.connectorId()).isEqualTo("oracle");
        assertThat(report.tables()).singleElement().satisfies(table ->
                assertThat(table.name()).isEqualTo("orders"));
    }

    @Test
    void isNotFoundForAConnectionThatWasNeverDiscovered() {
        // A connection with no stored model is a 404 (never discovered), the same absent semantics as
        // the test-result read-back.
        HttpStatusCode status = client().get().uri("/api/connections/never_discovered/schema")
                .header("Authorization", "Bearer " + token(Scope.READ))
                .exchange((request, response) -> response.getStatusCode());

        assertThat(status).isEqualTo(HttpStatus.NOT_FOUND);
    }

    /**
     * A minimal boot config: auto-configures Web MVC + the embedded servlet container, imports the path
     * prefix configuration ({@link RestApiConfiguration}), Spring Security, the connection controller and the
     * coded-error advice, so the verb surface is authenticated exactly as in production. The connection-test
     * service is real, composed over
     * an in-memory probe and stores.
     */
    @SpringBootConfiguration
    @EnableAutoConfiguration
    @Import({RestApiConfiguration.class, RestApiSecurityConfiguration.class,
            ConnectionController.class, ApiExceptionHandler.class})
    static class TestApp {

        @Bean
        Clock clock() {
            return Clock.fixed(NOW, ZoneOffset.UTC);
        }

        @Bean
        OperationRegistry operationRegistry() {
            return ControlOperations.registry();
        }

        @Bean
        FakeTokenStore tokenStore() {
            return new FakeTokenStore();
        }

        @Bean
        TokenSecrets tokenSecrets() {
            return new FakeTokenSecrets();
        }

        @Bean
        TokenSigner tokenSigner() {
            return new FakeSigner();
        }

        @Bean
        TokenService tokenService(TokenStore store, TokenSecrets secrets, Clock clock) {
            return new TokenService(store, secrets, clock);
        }

        @Bean
        RecordingConnectionProbe connectionProbe() {
            return new RecordingConnectionProbe();
        }

        @Bean
        InMemoryConnectionTestResultStore connectionTestResultStore() {
            return new InMemoryConnectionTestResultStore();
        }

        @Bean
        RecordingAuditStore auditStore() {
            return new RecordingAuditStore();
        }

        @Bean
        AuditGate auditGate(AuditStore store, Clock clock) {
            return new AuditGate(store, clock);
        }

        @Bean
        ConnectionTestService connectionTestService(
                ConnectionProbe probe, ConnectionTestResultStore resultStore, AuditGate auditGate) {
            return new ConnectionTestService(probe, resultStore, auditGate);
        }

        @Bean
        ConnectionTestResultQueryService connectionTestResultQueryService(ConnectionTestResultStore resultStore) {
            return new ConnectionTestResultQueryService(resultStore);
        }

        @Bean
        RecordingSchemaDiscoveryProbe schemaDiscoveryProbe() {
            return new RecordingSchemaDiscoveryProbe();
        }

        @Bean
        InMemorySchemaStore schemaStore() {
            return new InMemorySchemaStore();
        }

        @Bean
        SchemaDiscoveryService schemaDiscoveryService(
                SchemaDiscoveryProbe probe, SchemaStore schemaStore, AuditGate auditGate, Clock clock) {
            return new SchemaDiscoveryService(probe, schemaStore, auditGate, clock);
        }

        @Bean
        SchemaQueryService schemaQueryService(SchemaStore schemaStore) {
            return new SchemaQueryService(schemaStore);
        }
    }

    // ---- fakes ----

    /** A probe that captures the config it was driven with and returns a canned passing result for it. */
    private static final class RecordingConnectionProbe implements ConnectionProbe {
        private ConnectionConfig captured;

        void clear() {
            captured = null;
        }

        ConnectionConfig captured() {
            return captured;
        }

        @Override
        public ConnectionTestResult probe(ConnectionConfig config) {
            this.captured = config;
            return new ConnectionTestResult(config.id(), config.connectorId(), ConnectionTestResult.Outcome.PASSED,
                    List.of(new ConnectionTestItem("Connection", ConnectionTestItem.Status.PASSED,
                            "connected", null, null, null)),
                    NOW.toEpochMilli());
        }
    }

    /** A discovery probe that captures the config it was driven with and returns a canned model for it. */
    private static final class RecordingSchemaDiscoveryProbe implements SchemaDiscoveryProbe {
        private ConnectionConfig captured;

        void clear() {
            captured = null;
        }

        ConnectionConfig captured() {
            return captured;
        }

        @Override
        public SourceModel discover(ConnectionConfig config) {
            this.captured = config;
            return new SourceModel(List.of(new SourceTable(
                    "orders", List.of(new SourceField("id", "bigint")), List.of("id"), List.of())));
        }
    }

    /** An in-memory latest-only schema store keyed by connection id. */
    private static final class InMemorySchemaStore implements SchemaStore {
        private final Map<String, DiscoveredSourceModel> byId = new LinkedHashMap<>();

        void clear() {
            byId.clear();
        }

        @Override
        public void save(DiscoveredSourceModel discovered) {
            byId.put(discovered.connectionId(), discovered);
        }

        @Override
        public Optional<DiscoveredSourceModel> get(String connectionId) {
            return Optional.ofNullable(byId.get(connectionId));
        }
    }

    /** An in-memory latest-only result store keyed by connection id. */
    private static final class InMemoryConnectionTestResultStore implements ConnectionTestResultStore {
        private final Map<String, ConnectionTestResult> byId = new LinkedHashMap<>();

        void clear() {
            byId.clear();
        }

        @Override
        public void save(ConnectionTestResult result) {
            byId.put(result.connectionId(), result);
        }

        @Override
        public Optional<ConnectionTestResult> find(String connectionId) {
            return Optional.ofNullable(byId.get(connectionId));
        }
    }

    /** A capturing audit sink, so the test can read back what the operation was audited to. */
    private static final class RecordingAuditStore implements AuditStore {
        private final List<AuditRecord> records = new ArrayList<>();

        void clear() {
            records.clear();
        }

        List<AuditRecord> records() {
            return records;
        }

        @Override
        public void record(AuditRecord record) {
            records.add(record);
        }
    }

    /** An in-memory token store keyed by token id. */
    private static final class FakeTokenStore implements TokenStore {
        private final Map<String, TokenRecord> byId = new LinkedHashMap<>();

        void clear() {
            byId.clear();
        }

        @Override
        public void save(TokenRecord record) {
            byId.put(record.tokenId(), record);
        }

        @Override
        public Optional<TokenRecord> find(String tokenId) {
            return Optional.ofNullable(byId.get(tokenId));
        }

        @Override
        public void revoke(String tokenId) {
            TokenRecord existing = byId.get(tokenId);
            if (existing != null) {
                byId.put(tokenId, new TokenRecord(existing.tokenId(), existing.scope(),
                        existing.secretHash(), true, existing.createdAt()));
            }
        }

        @Override
        public List<TokenRecord> list() {
            return new ArrayList<>(byId.values());
        }
    }

    /** A deterministic secret minter: tok-N / sec-N with a reversible hash. */
    private static final class FakeTokenSecrets implements TokenSecrets {
        private int counter;

        @Override
        public GeneratedSecret generate() {
            counter++;
            return new GeneratedSecret("tok-" + counter, "sec-" + counter, "hash:sec-" + counter);
        }

        @Override
        public boolean matches(String presentedSecret, String storedHash) {
            return storedHash.equals("hash:" + presentedSecret);
        }
    }

    /** A signer whose token is a reversible {@code subject|SCOPE} encoding (unused here but required to wire). */
    private static final class FakeSigner implements TokenSigner {
        @Override
        public String issue(String subject, Scope scope) {
            return subject + "|" + scope.name();
        }

        @Override
        public Optional<VerifiedToken> verify(String token) {
            int bar = token.indexOf('|');
            if (bar < 0) {
                return Optional.empty();
            }
            return Optional.of(new VerifiedToken(token.substring(0, bar), Scope.valueOf(token.substring(bar + 1))));
        }
    }
}
