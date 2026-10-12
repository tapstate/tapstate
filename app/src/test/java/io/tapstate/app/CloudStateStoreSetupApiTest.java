package io.tapstate.app;

import io.tapstate.control.core.ApplyService;
import io.tapstate.control.core.ArtifactDraft;
import io.tapstate.control.core.ArtifactMutationService;
import io.tapstate.control.core.ArtifactQueryService;
import io.tapstate.control.core.AuditGate;
import io.tapstate.control.core.AuthenticationMode;
import io.tapstate.control.core.CloudAuthenticationService;
import io.tapstate.control.core.CloudLoginIdentity;
import io.tapstate.control.core.CloudSessionService;
import io.tapstate.control.core.ConnectionTestService;
import io.tapstate.control.core.ControlError;
import io.tapstate.control.core.ControlOperations;
import io.tapstate.control.core.CredentialAuthenticator;
import io.tapstate.control.core.GeneratedSecret;
import io.tapstate.control.core.OperationRegistry;
import io.tapstate.control.core.PlanAdvisories;
import io.tapstate.control.core.ResourceAttributionPolicy;
import io.tapstate.control.core.SchemaDerivation;
import io.tapstate.control.core.Scope;
import io.tapstate.control.core.SourceProjectionService;
import io.tapstate.control.core.SourceRepresentation;
import io.tapstate.control.core.SourceSchemaQueryService;
import io.tapstate.control.core.StateDatabasePolicy;
import io.tapstate.control.core.StateStoreSetupService;
import io.tapstate.control.core.TokenAdminService;
import io.tapstate.control.core.TokenSecrets;
import io.tapstate.control.restapi.ControlHttpFace;
import io.tapstate.control.restapi.RestApiConfiguration;
import io.tapstate.core.catalog.TapstateCatalog;
import io.tapstate.core.common.TapstateException;
import io.tapstate.core.model.SourceResource;
import io.tapstate.core.model.canonical.CanonicalHash;
import io.tapstate.runtime.probe.ConnectionProbe;
import io.tapstate.spi.store.AuditRecord;
import io.tapstate.spi.store.CloudSessionIdentity;
import io.tapstate.spi.store.CloudSessionRecord;
import io.tapstate.spi.store.CloudSessionStore;
import io.tapstate.spi.store.ConnectionTestResult;
import io.tapstate.spi.store.ConnectionTestResultStore;
import io.tapstate.spi.store.SchemaStore;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.support.AbstractBeanDefinition;
import org.springframework.beans.factory.support.BeanDefinitionRegistry;
import org.springframework.beans.factory.support.RootBeanDefinition;
import org.springframework.boot.SpringBootConfiguration;
import org.springframework.boot.autoconfigure.EnableAutoConfiguration;
import org.springframework.boot.builder.SpringApplicationBuilder;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.boot.web.server.context.WebServerApplicationContext;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.context.annotation.FilterType;
import org.springframework.context.annotation.Import;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.http.client.JdkClientHttpRequestFactory;
import org.springframework.web.client.RestClient;

import java.net.http.HttpClient;
import java.time.Clock;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verifyNoInteractions;

/** Real Cloud sessions, HTTP authorization and managed Source writes over controlled persistence and PDK ports. */
class CloudStateStoreSetupApiTest {
    private static final Instant NOW = Instant.parse("2026-10-09T00:00:00Z");
    private static final CloudSessionIdentity IDENTITY = new CloudSessionIdentity(
            "https://state-store.example.test", "store-org", "store-cluster");
    private static final String PASSWORD = "state-store-password-sentinel";
    private static final Map<String, Object> SETTINGS = Map.of("isUri", true,
            "uri", "mongodb+srv://fixture:" + PASSWORD + "@fixture.example.test/user_views");
    private ConfigurableApplicationContext context;
    private RestClient client;
    private String origin;

    @BeforeEach
    void start() {
        context = new SpringApplicationBuilder(TestApp.class).initializers(application -> {
            application.getBeanFactory().registerSingleton("stateStoreFactory", new ControlPlaneConfiguration());
            RootBeanDefinition setup = new RootBeanDefinition(StateStoreSetupService.class);
            setup.setFactoryBeanName("stateStoreFactory");
            setup.setFactoryMethodName("stateStoreSetupService");
            setup.setAutowireMode(AbstractBeanDefinition.AUTOWIRE_CONSTRUCTOR);
            ((BeanDefinitionRegistry) application.getBeanFactory()).registerBeanDefinition("stateStoreSetupService", setup);
        }).run("--server.address=127.0.0.1", "--server.port=0",
                "--tapstate.cloud.base-url=" + IDENTITY.issuer(), "--tapstate.cloud.token=runtime-token-sentinel",
                "--tapstate.cloud.atlas-uri=mongodb://127.0.0.1:1/store_metadata",
                "--tapstate.cloud.cluster-id=" + IDENTITY.clusterId());
        origin = "http://127.0.0.1:" + ((WebServerApplicationContext) context).getWebServer().getPort();
        client = RestClient.builder().baseUrl(origin).requestFactory(new JdkClientHttpRequestFactory(
                HttpClient.newBuilder().followRedirects(HttpClient.Redirect.NEVER).build())).build();
    }

    @AfterEach
    void stop() {
        if (context != null) context.close();
    }

    @Test
    void aCloudWriterConnectsTheDedicatedStoreWithVerifiedAttributionAndAudit() {
        String cookie = enter("write-code");
        Map<?, ?> body = connect(cookie, HttpStatus.OK);
        assertThat(body.get("id")).isEqualTo("atlas-store");
        assertThat(body.toString()).doesNotContain(PASSWORD);
        SourceResource source = (SourceResource) artifacts().get("atlas-store").orElseThrow();
        assertThat(source.metadata().labels()).containsEntry("store", "true");
        assertThat(source.metadata().cloud()).isTrue();
        assertThat(source.metadata().userId()).isEqualTo("store-writer");
        assertThat(probe().calls.get()).isEqualTo(1);
        assertThat(context.getBean(Audits.class).records).anySatisfy(record -> {
            assertThat(record.operationId()).isEqualTo("state-store.connect");
            assertThat(record.principal()).isEqualTo("store-writer");
        });
    }

    @Test
    void readersAndAnonymousCallersCannotProbeOrCreateAStore() {
        connect(enter("read-code"), HttpStatus.FORBIDDEN);
        connect(null, HttpStatus.UNAUTHORIZED);
        assertThat(probe().calls.get()).isZero();
        assertThat(artifacts().list()).isEmpty();
    }

    @Test
    void aCloudWriterStillCannotInvokeTokenAdministration() {
        String cookie = enter("write-code");
        client.get().uri("/api/tokens").header(HttpHeaders.COOKIE, cookie).exchange((request, response) -> {
            assertThat(response.getStatusCode()).isEqualTo(HttpStatus.FORBIDDEN);
            assertThat(response.bodyTo(Map.class).get("code")).isEqualTo("control.forbidden");
            return null;
        });
        verifyNoInteractions(context.getBean(TokenAdminService.class));
        assertThat(probe().calls.get()).isZero();
    }

    @Test
    void ordinarySourceCreationCannotForgeTheStoreMarker() {
        String cookie = enter("write-code");
        client.post().uri("/api/sources").header(HttpHeaders.ORIGIN, origin).header(HttpHeaders.COOKIE, cookie)
                .contentType(MediaType.APPLICATION_JSON).body(Map.of("id", "forged-store", "connector", "mongodb-atlas",
                        "metadata", Map.of("labels", Map.of("store", "true")), "config", SETTINGS))
                .exchange((request, response) -> {
                    assertThat(response.getStatusCode()).isEqualTo(HttpStatus.BAD_REQUEST);
                    assertThat(response.bodyTo(Map.class).get("code")).isEqualTo("control.malformed-request");
                    return null;
                });
        assertThat(artifacts().list()).isEmpty();
        assertThat(probe().calls.get()).isZero();
    }

    @Test
    void genericArtifactApplyCannotForgeTheStoreMarker() {
        enter("write-code");
        String content = """
                version: tapstate/v1
                kind: source
                id: forged-store
                connector: mongodb-atlas
                metadata: {labels: {store: "true"}}
                config: {isUri: true, uri: "mongodb+srv://fixture.example.test/user_views"}
                """;
        assertThatThrownBy(() -> context.getBean(ApplyService.class).apply("store-writer",
                List.of(new ArtifactDraft(null, content))))
                .isInstanceOfSatisfying(TapstateException.class,
                        failure -> assertThat(failure.code()).isEqualTo(ControlError.MALFORMED_REQUEST));
        assertThat(artifacts().list()).isEmpty();
        assertThat(probe().calls.get()).isZero();
    }

    @Test
    void aSecondStoreCannotReplaceTheFirstOrRunAnotherProbe() {
        String cookie = enter("write-code");
        connect(cookie, HttpStatus.OK);
        String before = CanonicalHash.of(artifacts().get("atlas-store").orElseThrow());
        connect(cookie, HttpStatus.BAD_REQUEST);
        assertThat(artifacts().list()).hasSize(1);
        assertThat(CanonicalHash.of(artifacts().get("atlas-store").orElseThrow())).isEqualTo(before);
        assertThat(probe().calls.get()).isEqualTo(1);
    }

    @Test
    void aFailedPdkProbeRecordsItsFailureWithoutCreatingAStore() {
        probe().outcome = ConnectionTestResult.Outcome.FAILED;
        connect(enter("write-code"), HttpStatus.BAD_REQUEST);
        assertThat(probe().calls.get()).isEqualTo(1);
        assertThat(artifacts().list()).isEmpty();
        assertThat(context.getBean(Results.class).find("atlas-store")).hasValueSatisfying(result ->
                assertThat(result.outcome()).isEqualTo(ConnectionTestResult.Outcome.FAILED));
    }

    private String enter(String code) {
        return client.get().uri("/auth/exchange?code=" + code).exchange((request, response) -> {
            assertThat(response.getStatusCode()).isEqualTo(HttpStatus.FOUND);
            assertThat(response.getHeaders().getLocation().toString()).isEqualTo("/");
            return response.getHeaders().getFirst(HttpHeaders.SET_COOKIE).split(";", 2)[0];
        });
    }

    private Map<?, ?> connect(String cookie, HttpStatus expected) {
        var request = client.post().uri("/api/state-store").header(HttpHeaders.ORIGIN, origin)
                .contentType(MediaType.APPLICATION_JSON);
        if (cookie != null) request.header(HttpHeaders.COOKIE, cookie);
        return request.body(SETTINGS).exchange((input, response) -> {
            assertThat(response.getStatusCode()).isEqualTo(expected);
            return response.bodyTo(Map.class);
        });
    }

    private InMemoryArtifactStore artifacts() { return context.getBean(InMemoryArtifactStore.class); }
    private Probe probe() { return context.getBean(Probe.class); }

    @SpringBootConfiguration
    @EnableAutoConfiguration
    @EnableConfigurationProperties(CloudProperties.class)
    @Import(RestApiConfiguration.class)
    @ComponentScan(basePackageClasses = ControlHttpFace.class, useDefaultFilters = false,
            includeFilters = @ComponentScan.Filter(type = FilterType.REGEX, pattern =
                    "io\\.tapstate\\.control\\.restapi\\.(CloudAuthController|StateStoreController|SourceController|TokenController|RestApiSecurityConfiguration|ApiExceptionHandler)"))
    static class TestApp {
        @Bean CloudRuntimeSettings cloud(CloudProperties properties) { return CloudRuntimeSettings.resolve(properties); }
        @Bean AuthenticationMode mode() { return AuthenticationMode.CLOUD; }
        @Bean OperationRegistry registry() { return ControlOperations.registry(); }
        @Bean Clock clock() { return Clock.fixed(NOW, ZoneOffset.UTC); }
        @Bean InMemoryArtifactStore artifacts() { return new InMemoryArtifactStore(); }
        @Bean Audits audits() { return new Audits(); }
        @Bean AuditGate auditGate(Audits audits, Clock clock) { return new AuditGate(audits.records::add, clock); }
        @Bean SchemaStore schemas() { return mock(SchemaStore.class); }
        @Bean SourceRepresentation representation() { return new SourceRepresentation(TapstateCatalog::load); }
        @Bean ArtifactQueryService queries(InMemoryArtifactStore artifacts) { return new ArtifactQueryService(artifacts); }
        @Bean ApplyService apply(InMemoryArtifactStore artifacts, AuditGate audit, SchemaStore schemas) {
            return new ApplyService(TapstateCatalog::load, artifacts, audit, schemas, PlanAdvisories.none(),
                    SchemaDerivation.none(), null, ResourceAttributionPolicy.managedCloud(), StateDatabasePolicy.CLOUD);
        }
        @Bean SourceProjectionService sources(ApplyService apply, ArtifactQueryService queries, SourceRepresentation representation) {
            return new SourceProjectionService(apply, queries, mock(ArtifactMutationService.class), representation);
        }
        @Bean SourceSchemaQueryService sourceSchemas(InMemoryArtifactStore artifacts, SchemaStore schemas) {
            return new SourceSchemaQueryService(artifacts, schemas);
        }
        @Bean Results results() { return new Results(); }
        @Bean Probe probe() { return new Probe(); }
        @Bean ConnectionTestService connections(Probe probe, Results results, AuditGate audit) {
            return new ConnectionTestService(probe, results, audit);
        }
        @Bean TokenAdminService tokenAdministration() { return mock(TokenAdminService.class); }
        @Bean CloudAuthenticationService authentication(Clock clock) {
            TokenSecrets secrets = new TokenSecrets() {
                private final AtomicInteger sequence = new AtomicInteger();
                @Override public GeneratedSecret generate() {
                    String secret = "session-secret-sentinel-" + sequence.incrementAndGet();
                    return new GeneratedSecret("session", secret, hash(secret));
                }
                @Override public String hash(String raw) { return "hash-" + raw; }
                @Override public boolean matches(String raw, String hash) { return hash(raw).equals(hash); }
            };
            CloudSessionService sessions = new CloudSessionService(new Sessions(), IDENTITY, secrets, clock);
            return new CloudAuthenticationService((code, cluster) -> "fixture-jwt-" + code,
                    (jwt, deployment, audience) -> {
                        Scope scope = jwt.equals("fixture-jwt-write-code") ? Scope.WRITE
                                : jwt.equals("fixture-jwt-read-code") ? Scope.READ : null;
                        if (scope == null || !"127.0.0.1".equals(audience)) return Optional.empty();
                        return Optional.of(new CloudLoginIdentity(deployment,
                                scope == Scope.WRITE ? "store-writer" : "store-reader",
                                "jti-" + scope.name(), scope, NOW.plusSeconds(900)));
                    }, sessions);
        }
        @Bean CredentialAuthenticator credentials(CloudAuthenticationService authentication) {
            return new CredentialAuthenticator(authentication::authenticate);
        }
    }

    private static final class Audits { final List<AuditRecord> records = new ArrayList<>(); }
    private static final class Probe implements ConnectionProbe {
        final AtomicInteger calls = new AtomicInteger();
        volatile ConnectionTestResult.Outcome outcome = ConnectionTestResult.Outcome.PASSED;
        @Override public ConnectionTestResult probe(io.tapstate.spi.store.ConnectionConfig config) {
            calls.incrementAndGet();
            return new ConnectionTestResult(config.id(), config.connectorId(), outcome, List.of(), NOW.toEpochMilli());
        }
    }
    private static final class Results implements ConnectionTestResultStore {
        final Map<String, ConnectionTestResult> values = new HashMap<>();
        @Override public void save(ConnectionTestResult result) { values.put(result.connectionId(), result); }
        @Override public Optional<ConnectionTestResult> find(String id) { return Optional.ofNullable(values.get(id)); }
    }
    private record SessionKey(CloudSessionIdentity identity, String jti) { }
    private static final class Sessions implements CloudSessionStore {
        final Map<SessionKey, CloudSessionRecord> values = new HashMap<>();
        @Override public synchronized boolean create(CloudSessionRecord record) {
            return values.putIfAbsent(new SessionKey(record.identity(), record.jwtId()), record) == null;
        }
        @Override public synchronized Optional<CloudSessionRecord> find(CloudSessionIdentity identity, String jti) {
            return Optional.ofNullable(values.get(new SessionKey(identity, jti)));
        }
        @Override public synchronized Optional<CloudSessionRecord> authenticate(CloudSessionIdentity identity, String jti,
                String hash, Instant now, Instant expiry) {
            SessionKey key = new SessionKey(identity, jti);
            CloudSessionRecord record = values.get(key);
            if (record == null || record.revoked() || !record.secretHash().equals(hash)
                    || !record.idleExpiresAt().isAfter(now)) return Optional.empty();
            CloudSessionRecord touched = new CloudSessionRecord(identity, jti, hash, record.userId(), record.scope(),
                    false, record.createdAt(), now, expiry, record.clusterContext());
            values.put(key, touched);
            return Optional.of(touched);
        }
        @Override public synchronized boolean logout(CloudSessionIdentity identity, String jti, String hash, Instant now) {
            return values.remove(new SessionKey(identity, jti)) != null;
        }
        @Override public synchronized void invalidate(CloudSessionIdentity identity, String jti, Instant now) {
            values.remove(new SessionKey(identity, jti));
        }
    }
}
