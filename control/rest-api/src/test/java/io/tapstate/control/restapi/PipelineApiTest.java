package io.tapstate.control.restapi;

import io.tapstate.control.core.ApplyService;
import io.tapstate.control.core.AccessTokenService;
import io.tapstate.control.core.ArtifactMutationService;
import io.tapstate.control.core.PlanAdvisories;
import io.tapstate.control.core.ArtifactQueryService;
import io.tapstate.control.core.AuditGate;
import io.tapstate.control.core.BootstrapService;
import io.tapstate.control.core.ConnectionTestResultQueryService;
import io.tapstate.control.core.ConnectionTestService;
import io.tapstate.spi.store.ConnectorRegistration;
import io.tapstate.spi.store.ConnectorRegistry;
import io.tapstate.spi.store.ConnectorSpecStore;
import io.tapstate.spi.store.RegistrationOutcome;
import io.tapstate.spi.store.RegistrationSource;
import io.tapstate.control.core.ConnectorCatalogView;
import io.tapstate.control.core.ConnectorRegisterService;
import io.tapstate.control.core.ClusterIdentityService;
import io.tapstate.control.core.ControlOperations;
import io.tapstate.control.core.CredentialAuthenticator;
import io.tapstate.control.core.DataBrowserFollows;
import io.tapstate.control.core.DataBrowserService;
import io.tapstate.control.core.GeneratedSecret;
import io.tapstate.control.core.LoginService;
import io.tapstate.control.core.OperationRegistry;
import io.tapstate.control.core.PasswordHasher;
import io.tapstate.control.core.PipelineLifecycleService;
import io.tapstate.control.core.PipelineLogQueryService;
import io.tapstate.control.core.PipelineObservationQueryService;
import io.tapstate.control.core.SchemaDiscoveryService;
import io.tapstate.control.core.SchemaQueryService;
import io.tapstate.control.core.SessionService;
import io.tapstate.control.core.Scope;
import io.tapstate.control.core.SourceService;
import io.tapstate.control.core.TokenSecrets;
import io.tapstate.control.core.TokenService;
import io.tapstate.control.core.TokenSigner;
import io.tapstate.control.core.VerifiedToken;
import io.tapstate.core.catalog.TapstateCatalog;
import io.tapstate.core.dsl.DslParser;
import io.tapstate.core.lifecycle.DesiredState;
import io.tapstate.core.lifecycle.PipelineState;
import io.tapstate.core.model.Resource;
import io.tapstate.core.lifecycle.Observation;
import io.tapstate.core.model.canonical.CanonicalHash;
import io.tapstate.core.model.canonical.CanonicalWriter;
import io.tapstate.runtime.probe.ConnectionProbe;
import io.tapstate.runtime.probe.SchemaDiscoveryProbe;
import io.tapstate.spi.store.ArtifactStore;
import io.tapstate.spi.store.AuditRecord;
import io.tapstate.spi.store.AuditStore;
import io.tapstate.spi.store.ConnectionTestResult;
import io.tapstate.spi.store.ConnectionTestResultStore;
import io.tapstate.spi.store.ClusterIdentity;
import io.tapstate.spi.store.ClusterIdentityStore;
import io.tapstate.spi.store.DesiredStore;
import io.tapstate.spi.store.DiscoveredSourceModel;
import io.tapstate.core.logging.LogSink;
import io.tapstate.core.logging.RingBufferLogSink;
import io.tapstate.spi.store.ObservationStore;
import io.tapstate.spi.store.SchemaStore;
import io.tapstate.core.model.PipelineResource;
import io.tapstate.spi.store.TokenRecord;
import io.tapstate.spi.store.TokenStore;
import io.tapstate.spi.store.SessionRecord;
import io.tapstate.spi.store.SessionStore;
import io.tapstate.spi.store.User;
import io.tapstate.spi.store.UserStore;
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
import org.springframework.http.MediaType;
import org.springframework.web.client.RestClient;
import org.springframework.web.method.HandlerMethod;
import org.springframework.web.servlet.mvc.method.annotation.RequestMappingHandlerMapping;

import java.time.Clock;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * The pipeline lifecycle write verbs projected onto the authenticated {@code /api} surface: start / stop /
 * pause / resume, each a custom method on a pipeline instance ({@code POST /api/pipelines/{id}:start}). It
 * proves the four verbs round-trip through the real control-core service (over in-memory fakes), that the
 * state-machine and revision refusals surface as their proper 4xx coded bodies rather than a bare 500, that
 * the grade check guards them like any write verb, and that the authenticated caller becomes the audit
 * principal recorded for the audited write. The context is booted programmatically so the module stays on
 * the reactor's JUnit line, and it imports the whole {@link ControlHttpFace} bundle so it exercises the
 * exact assembly the running server uses.
 */
class PipelineApiTest {

    /** A follow probe that is never driven: nothing here streams, and one that answered
     * would let a case pass by following instead of reading. */
    private static final io.tapstate.runtime.probe.DataBrowserTailProbe NO_FOLLOWS =
            (config, request, listener) -> {
                throw new AssertionError("no case here opens a follow");
            };

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
    void resetState() {
        context.getBean(FakeUserStore.class).clear();
        context.getBean(FakeTokenStore.class).clear();
        context.getBean(FakeDesiredStore.class).clear();
        context.getBean(RecordingAuditStore.class).clear();
        FakeArtifactStore artifacts = context.getBean(FakeArtifactStore.class);
        artifacts.clear();
        // Every test starts from one applied, never-run pipeline (state NEW).
        artifacts.seed(PIPELINE_V1);
    }

    private RestClient client() {
        return RestClient.create("http://localhost:" + port);
    }

    /** Mints a machine token of the given grade through the real token service and returns the bearer string. */
    private String machineToken(Scope scope) {
        return context.getBean(TokenService.class).create(scope);
    }

    // ---- the four verbs round-trip through the service ----

    @Test
    void startWithAWriteCredentialWritesRunningDesiredAtTheLatestRevision() {
        DesiredState body = client().post().uri("/api/pipelines/pl1:start")
                .header("Authorization", "Bearer " + machineToken(Scope.WRITE))
                .retrieve().toEntity(DesiredState.class).getBody();

        assertThat(body).isEqualTo(new DesiredState("pl1", PipelineState.RUNNING, revisionOf(PIPELINE_V1)));
        assertThat(context.getBean(FakeDesiredStore.class).read("pl1")).contains(body);
    }

    @Test
    void theFourVerbsDriveTheFullLifecycleOverHttp() {
        String token = machineToken(Scope.WRITE);
        String rev = revisionOf(PIPELINE_V1);

        assertThat(verb(token, "pl1", "start")).isEqualTo(new DesiredState("pl1", PipelineState.RUNNING, rev));
        assertThat(verb(token, "pl1", "pause")).isEqualTo(new DesiredState("pl1", PipelineState.PAUSED, rev));
        assertThat(verb(token, "pl1", "resume")).isEqualTo(new DesiredState("pl1", PipelineState.RUNNING, rev));
        assertThat(verb(token, "pl1", "stop")).isEqualTo(new DesiredState("pl1", PipelineState.STOPPED, rev));

        assertThat(context.getBean(FakeDesiredStore.class).read("pl1"))
                .contains(new DesiredState("pl1", PipelineState.STOPPED, rev));
    }

    private DesiredState verb(String token, String id, String verb) {
        return client().post().uri("/api/pipelines/" + id + ":" + verb)
                .header("Authorization", "Bearer " + token)
                .retrieve().toEntity(DesiredState.class).getBody();
    }

    // ---- lifecycle refusals surface as their proper 4xx coded bodies, never a bare 500 ----

    @Test
    void anUnknownPipelineIsNotFoundWithACodedBody() {
        ApiError body = client().post().uri("/api/pipelines/ghost:start")
                .header("Authorization", "Bearer " + machineToken(Scope.WRITE))
                .exchange((request, response) -> {
                    assertThat(response.getStatusCode()).isEqualTo(HttpStatus.NOT_FOUND);
                    return response.bodyTo(ApiError.class);
                });

        assertThat(body.code()).isEqualTo("lifecycle.unknown-pipeline");
        assertThat(body.params()).containsEntry("pipeline", "ghost");
    }

    @Test
    void anIllegalTransitionIsAConflictWithACodedBody() {
        // pause is legal only from RUNNING; the seeded pipeline is NEW, so pausing it is refused.
        ApiError body = client().post().uri("/api/pipelines/pl1:pause")
                .header("Authorization", "Bearer " + machineToken(Scope.WRITE))
                .exchange((request, response) -> {
                    assertThat(response.getStatusCode()).isEqualTo(HttpStatus.CONFLICT);
                    return response.bodyTo(ApiError.class);
                });

        assertThat(body.code()).isEqualTo("lifecycle.illegal-transition");
        assertThat(body.params()).containsEntry("from", "NEW").containsEntry("verb", "pause");
    }

    @Test
    void resumeAtAStaleRevisionIsAConflictWithACodedBody() {
        String token = machineToken(Scope.WRITE);
        verb(token, "pl1", "start"); // RUNNING at v1
        verb(token, "pl1", "pause"); // PAUSED at v1
        // The pipeline is re-applied while paused, so the latest revision moves to v2.
        context.getBean(FakeArtifactStore.class).seed(PIPELINE_V2);

        ApiError body = client().post().uri("/api/pipelines/pl1:resume")
                .header("Authorization", "Bearer " + token)
                .exchange((request, response) -> {
                    assertThat(response.getStatusCode()).isEqualTo(HttpStatus.CONFLICT);
                    return response.bodyTo(ApiError.class);
                });

        assertThat(body.code()).isEqualTo("lifecycle.incompatible-revision");
        assertThat(body.params())
                .containsEntry("requested", revisionOf(PIPELINE_V1))
                .containsEntry("latest", revisionOf(PIPELINE_V2));
    }

    // ---- the grade check guards a lifecycle verb like any other write ----

    @Test
    void aReadCredentialIsForbiddenFromALifecycleVerb() {
        ApiError body = client().post().uri("/api/pipelines/pl1:start")
                .header("Authorization", "Bearer " + machineToken(Scope.READ))
                .exchange((request, response) -> {
                    assertThat(response.getStatusCode()).isEqualTo(HttpStatus.FORBIDDEN);
                    return response.bodyTo(ApiError.class);
                });

        assertThat(body.code()).isEqualTo("control.forbidden");
        assertThat(body.params()).containsEntry("op", "pipeline.start").containsEntry("required", "write");
    }

    @Test
    void anUnauthenticatedCallerIsUnauthorized() {
        ApiError body = client().post().uri("/api/pipelines/pl1:start")
                .exchange((request, response) -> {
                    assertThat(response.getStatusCode()).isEqualTo(HttpStatus.UNAUTHORIZED);
                    return response.bodyTo(ApiError.class);
                });

        assertThat(body.code()).isEqualTo("control.unauthenticated");
    }

    // ---- the authenticated caller becomes the audit principal of the audited write ----

    @Test
    void theAuthenticatedCallerBecomesTheAuditPrincipal() {
        // A human session token carries the username as its subject, so the audited write records that name.
        seedUser("alice", "s3cret", "admin");
        String session = client().post().uri("/auth/login")
                .contentType(MediaType.APPLICATION_JSON)
                .body(new LoginRequest("alice", "s3cret"))
                .retrieve().toEntity(LoginResponse.class).getBody().token();

        DesiredState body = client().post().uri("/api/pipelines/pl1:start")
                .header("Authorization", "Bearer " + session)
                .retrieve().toEntity(DesiredState.class).getBody();

        assertThat(body.targetState()).isEqualTo(PipelineState.RUNNING);
        List<AuditRecord> records = context.getBean(RecordingAuditStore.class).records;
        assertThat(records).singleElement().satisfies(r -> {
            assertThat(r.operationId()).isEqualTo("pipeline.start");
            assertThat(r.principal()).isEqualTo("alice");
            assertThat(r.resourceId()).isEqualTo("pl1");
        });
    }

    // ---- the endpoint table is a derivation of the registry: the pipeline verbs project onto /api ----

    @Test
    void everyPipelineVerbProjectsARegisteredCliExposedVerb() {
        Set<String> cliExposed = ControlOperations.registry()
                .exposedOn(io.tapstate.control.core.Frontend.CLI).stream()
                .map(io.tapstate.control.core.Operation::id).collect(Collectors.toSet());

        RequestMappingHandlerMapping mapping =
                context.getBean("requestMappingHandlerMapping", RequestMappingHandlerMapping.class);

        List<String> projectedPipelineVerbs = new ArrayList<>();
        mapping.getHandlerMethods().forEach((info, handler) -> {
            Verb verb = handler.getMethodAnnotation(Verb.class);
            if (verb != null && verb.value().startsWith("pipeline.")) {
                projectedPipelineVerbs.add(verb.value());
                assertThat(cliExposed)
                        .as("a projected pipeline verb must be a registered, CLI-exposed operation")
                        .contains(verb.value());
            }
        });

        assertThat(projectedPipelineVerbs)
                .as("the full pipeline surface — four lifecycle writes and four observation reads — projects "
                        + "onto the authenticated /api surface (this test boots the whole face bundle)")
                .containsExactlyInAnyOrder(
                        "pipeline.start", "pipeline.stop", "pipeline.pause", "pipeline.resume",
                        "pipeline.status", "pipeline.metrics", "pipeline.snapshot", "pipeline.logs");
    }

    // ---- fixtures ----

    private static Resource parse(String dsl) {
        return new DslParser().parse(dsl);
    }

    /** The revision of a pipeline is the content hash of its canonical form — the value apply stamps. */
    private static String revisionOf(String dsl) {
        return CanonicalHash.of(new CanonicalWriter().write(parse(dsl)));
    }

    private void seedUser(String username, String password, String role) {
        FakeHasher hasher = context.getBean(FakeHasher.class);
        context.getBean(FakeUserStore.class).save(new User(username, hasher.hash(password), role));
    }

    private static final String PIPELINE_V1 = """
            version: tapstate/v1
            kind: pipeline
            id: pl1
            source: src_x
            settings: { read_mode: snapshot_and_cdc }
            serve:
              from: /.*/
              sync:
                - id: sink1
                  source: tgt_x
                  write_mode: upsert
                  ddl: apply
            """;

    /** The same pipeline re-applied with a changed setting, so its canonical form — and revision — differ. */
    private static final String PIPELINE_V2 = """
            version: tapstate/v1
            kind: pipeline
            id: pl1
            source: src_x
            settings: { read_mode: cdc_only }
            serve:
              from: /.*/
              sync:
                - id: sink1
                  source: tgt_x
                  write_mode: upsert
                  ddl: apply
            """;

    /**
     * A minimal boot config: auto-configures Web MVC + the embedded servlet container, imports the whole
     * HTTP control face as one bundle ({@link ControlHttpFace}), and constructs the real control-core
     * services over in-memory fakes — including the pipeline lifecycle service over a fake desired store and
     * a recording audit store, so the audited write can be observed.
     */
    @SpringBootConfiguration
    @EnableAutoConfiguration
    @Import({ControlHttpFace.class, SourceDraftTestConfiguration.class, SourceServiceTestConfiguration.class,
            AuditedSourceServiceTestConfiguration.class})
    static class TestApp {

        @Bean
        Clock clock() {
            return Clock.fixed(NOW, ZoneOffset.UTC);
        }

        @Bean
        FakeUserStore userStore() {
            return new FakeUserStore();
        }

        @Bean
        FakeTokenStore tokenStore() {
            return new FakeTokenStore();
        }

        @Bean
        RecordingAuditStore auditStore() {
            return new RecordingAuditStore();
        }

        @Bean
        FakeArtifactStore artifactStore() {
            return new FakeArtifactStore();
        }

        @Bean
        FakeDesiredStore desiredStore() {
            return new FakeDesiredStore();
        }

        @Bean
        FakeHasher passwordHasher() {
            return new FakeHasher();
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
        ClusterIdentityService clusterIdentityService() {
            ClusterIdentity identity = new ClusterIdentity("01J5PIPELINEFIXTURE");
            ClusterIdentityStore store = new ClusterIdentityStore() {
                @Override
                public Optional<ClusterIdentity> find() {
                    return Optional.of(identity);
                }

                @Override
                public ClusterIdentity createIfAbsent(ClusterIdentity proposed) {
                    return identity;
                }
            };
            return new ClusterIdentityService(store, () -> "unused");
        }

        @Bean
        SessionStore sessionStore() {
            return new SessionStore() {
                @Override
                public void save(SessionRecord record) {
                }

                @Override
                public Optional<SessionRecord> find(String sessionId) {
                    return Optional.empty();
                }

                @Override
                public Optional<SessionRecord> exchange(
                        String sessionId, String secretHash, String issuer, Instant now, Instant idleExpiresAt) {
                    return Optional.empty();
                }

                @Override
                public boolean revoke(String sessionId, String secretHash, String issuer, Instant now) {
                    return false;
                }
            };
        }

        @Bean
        AccessTokenService accessTokenService(TokenSigner signer, Clock clock) {
            return new AccessTokenService(signer, clock);
        }

        @Bean
        SessionService sessionService(
                SessionStore store, TokenSecrets secrets, AccessTokenService accessTokens, Clock clock) {
            return new SessionService(store, secrets, accessTokens, clock);
        }

        @Bean
        TokenService tokenService(TokenStore store, TokenSecrets secrets, Clock clock) {
            return new TokenService(store, secrets, clock);
        }

        @Bean
        LoginService loginService(UserStore users, PasswordHasher hasher, AccessTokenService accessTokens) {
            return new LoginService(users, hasher, accessTokens);
        }

        @Bean
        AuditGate auditGate(AuditStore store, Clock clock) {
            return new AuditGate(store, clock);
        }

        @Bean
        BootstrapService bootstrapService(UserStore users, PasswordHasher hasher, AuditGate gate) {
            return new BootstrapService(users, hasher, gate);
        }

        @Bean
        OperationRegistry operationRegistry() {
            return ControlOperations.registry();
        }

        @Bean
        CredentialAuthenticator credentialAuthenticator(TokenService tokens, TokenSigner signer) {
            return new CredentialAuthenticator(tokens, signer);
        }

        @Bean
        ApplyService applyService(ArtifactStore store, AuditGate auditGate) {
            return new ApplyService(TapstateCatalog::load, store, auditGate, new EmptySchemaStore(),
                    PlanAdvisories.none());
        }

        @Bean
        ArtifactQueryService artifactQueryService(ArtifactStore store) {
            return new ArtifactQueryService(store);
        }

        // The removal controller comes in with the whole ControlHttpFace bundle, so its service must be
        // present for the context to stand up. This suite exercises the lifecycle verbs, not the removal,
        // so the dependent stores refuse rather than pretend: a reclaim reached from here is a defect.
        @Bean
        ArtifactMutationService artifactMutationService(ArtifactStore store, AuditGate auditGate) {
            return new ArtifactMutationService(
                    store, NoReclaimStores.desired(), NoReclaimStores.state(),
                    NoReclaimStores.observations(), NoReclaimStores.srsMeta(), auditGate, DataBrowserFollows.NONE);
        }

        // The data-browser controller is bundled too, so its service must be present for the context to
        // stand up; this suite exercises the pipeline verbs, not the reads, so every probe is inert (their
        // behaviour is proven in DataBrowserApiTest).
        @Bean
        DataBrowserService dataBrowserService(ArtifactStore store) {
            return new DataBrowserService(
                    store,
                    new NoDiscoveries(),
                    config -> {
                        throw new UnsupportedOperationException(
                                "data-browser.collections is not exercised in this test");
                    },
                    (config, collection) -> {
                        throw new UnsupportedOperationException(
                                "data-browser.stats is not exercised in this test");
                    },
                    (config, query) -> {
                        throw new UnsupportedOperationException(
                                "data-browser.find is not exercised in this test");
                    }, NO_FOLLOWS);
        }

        // The connection / connector controllers come in with the whole ControlHttpFace bundle, so their
        // services must be present for the context to stand up. This suite exercises the pipeline verbs, not
        // the probe / registration, so these only need to construct — their probes and stores are inert.
        @Bean
        ConnectionTestService connectionTestService(AuditGate auditGate) {
            ConnectionProbe probe = config -> {
                throw new UnsupportedOperationException("connection.test is not exercised in this test");
            };
            ConnectionTestResultStore resultStore = new ConnectionTestResultStore() {
                @Override
                public void save(ConnectionTestResult result) {
                }

                @Override
                public Optional<ConnectionTestResult> find(String connectionId) {
                    return Optional.empty();
                }
            };
            return new ConnectionTestService(probe, resultStore, auditGate);
        }

        @Bean
        ConnectionTestResultQueryService connectionTestResultQueryService() {
            return new ConnectionTestResultQueryService(new ConnectionTestResultStore() {
                @Override
                public void save(ConnectionTestResult result) {
                }

                @Override
                public Optional<ConnectionTestResult> find(String connectionId) {
                    return Optional.empty();
                }
            });
        }

        @Bean
        SchemaDiscoveryService schemaDiscoveryService(AuditGate auditGate) {
            SchemaDiscoveryProbe probe = config -> {
                throw new UnsupportedOperationException("connection.discover-schema is not exercised in this test");
            };
            return new SchemaDiscoveryService(probe, new SchemaStore() {
                @Override
                public void save(DiscoveredSourceModel discovered) {
                }

                @Override
                public Optional<DiscoveredSourceModel> get(String connectionId) {
                    return Optional.empty();
                }
            }, auditGate, Clock.systemUTC());
        }

        @Bean
        SchemaQueryService schemaQueryService() {
            return new SchemaQueryService(new SchemaStore() {
                @Override
                public void save(DiscoveredSourceModel discovered) {
                }

                @Override
                public Optional<DiscoveredSourceModel> get(String connectionId) {
                    return Optional.empty();
                }
            });
        }

        @Bean
        ConnectorRegisterService connectorRegisterService(AuditGate auditGate) {
            io.tapstate.spi.store.ConnectorRegistrar registrar = (artifact, source) -> {
                throw new UnsupportedOperationException("connector.register is not exercised in this test");
            };
            return new ConnectorRegisterService(registrar, auditGate);
        }

        @Bean
        ConnectorCatalogView connectorCatalogView() {
            io.tapstate.spi.store.ConnectorCatalogStore store = new io.tapstate.spi.store.ConnectorCatalogStore() {
                @Override
                public void upsert(io.tapstate.core.catalog.ConnectorCatalogEntry entry) {
                }

                @Override
                public Optional<io.tapstate.core.catalog.ConnectorCatalogEntry> get(String connectorId) {
                    return Optional.empty();
                }

                @Override
                public List<io.tapstate.core.catalog.ConnectorCatalogEntry> list() {
                    return List.of();
                }
            };
            return new ConnectorCatalogView(
                    TapstateCatalog.load(), store, emptySpecStore(), emptyConnectorRegistry());
        }

        @Bean
        PipelineLifecycleService pipelineLifecycleService(
                ArtifactQueryService artifacts, DesiredStore desired, AuditGate auditGate) {
            return new PipelineLifecycleService(artifacts, desired, auditGate);
        }

        @Bean
        ObservationStore observationStore() {
            // The lifecycle write tests do not read observations; the empty store is enough to bring the read
            // controller up so the full-face bundle boots. The read faces are proven in PipelineObservationApiTest.
            return new ObservationStore() {
                @Override
                public void delete(String pipelineId) {
                    throw new UnsupportedOperationException("removal is not exercised by this double");
                }

                @Override
                public void save(Observation observation) {
                }

                @Override
                public Optional<Observation> read(String pipelineId) {
                    return Optional.empty();
                }
            };
        }

        @Bean
        PipelineObservationQueryService pipelineObservationQueryService(ObservationStore observations) {
            return new PipelineObservationQueryService(new ArtifactQueryService(appliedPipelines()), observations);
        }

        @Bean
        LogSink logSink() {
            // The lifecycle write tests do not read logs; an empty node-local sink is enough to bring the
            // logs controller up so the full-face bundle boots. The logs face is proven in PipelineLogsApiTest.
            return new RingBufferLogSink(64, 200);
        }

        @Bean
        PipelineLogQueryService pipelineLogQueryService(LogSink sink) {
            return new PipelineLogQueryService(sink);
        }

    }

    // ---- fakes ----

    /** An in-memory artifact store holding resources by id, seedable from pipeline DSL. */
    static final class FakeArtifactStore implements ArtifactStore {
        private final Map<String, Resource> byId = new LinkedHashMap<>();

        void clear() {
            byId.clear();
        }

        void seed(String dsl) {
            Resource r = parse(dsl);
            byId.put(r.id(), r);
        }

        @Override
        public void saveAll(List<Resource> artifacts) {
            artifacts.forEach(r -> byId.put(r.id(), r));
        }

        @Override
        public Optional<Resource> get(String id) {
            return Optional.ofNullable(byId.get(id));
        }

        @Override
        public List<Resource> list() {
            return List.copyOf(byId.values());
        }
    }

    /** An in-memory desired store, last write wins per pipeline id. */
    static final class FakeDesiredStore implements DesiredStore {
        @Override
        public void delete(String pipelineId) {
            throw new UnsupportedOperationException("removal is not exercised by this double");
        }

        private final Map<String, DesiredState> byId = new LinkedHashMap<>();

        void clear() {
            byId.clear();
        }

        @Override
        public void save(DesiredState desired) {
            byId.put(desired.pipelineId(), desired);
        }

        @Override
        public Optional<DesiredState> read(String pipelineId) {
            return Optional.ofNullable(byId.get(pipelineId));
        }

        @Override
        public List<String> pipelineIds() {
            return List.copyOf(byId.keySet());
        }
    }

    /** An audit store that captures every record written through it. */
    static final class RecordingAuditStore implements AuditStore {
        final List<AuditRecord> records = new ArrayList<>();

        void clear() {
            records.clear();
        }

        @Override
        public void record(AuditRecord record) {
            records.add(record);
        }
    }

    /** An in-memory user store keyed by username. */
    static final class FakeUserStore implements UserStore {
        private final Map<String, User> users = new LinkedHashMap<>();

        void clear() {
            users.clear();
        }

        @Override
        public Optional<User> find(String username) {
            return Optional.ofNullable(users.get(username));
        }

        @Override
        public void save(User user) {
            users.put(user.username(), user);
        }

        @Override
        public boolean isEmpty() {
            return users.isEmpty();
        }
    }

    /** An in-memory token store keyed by token id. */
    static final class FakeTokenStore implements TokenStore {
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

    /** A deterministic password hasher (hash:raw). */
    static final class FakeHasher implements PasswordHasher {
        @Override
        public String hash(String raw) {
            return "hash:" + raw;
        }

        @Override
        public boolean matches(String raw, String storedHash) {
            return storedHash.equals(hash(raw));
        }
    }

    /** A deterministic secret minter: tok-N / sec-N with a reversible hash. */
    static final class FakeTokenSecrets implements TokenSecrets {
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

    /** A signer whose token is a reversible {@code subject|SCOPE} encoding, so a session token round-trips. */
    static final class FakeSigner implements TokenSigner {
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

    /**
     * An artifact store that answers for every id: this context is not about telling an applied pipeline
     * from an unapplied one, so every read of an unobserved pipeline stays the transient window it was
     * before the two were told apart.
     */
    private static ArtifactStore appliedPipelines() {
        return new ArtifactStore() {
            @Override
            public void saveAll(List<Resource> artifacts) {
            }

            @Override
            public Optional<Resource> get(String id) {
                return Optional.of(new PipelineResource(id, null, List.of("src_x"), null, null, null, null, null));
            }

            @Override
            public List<Resource> list() {
                return List.of();
            }
        };
    }

    /** Spec sources and registrations are irrelevant here: this suite drives authorization, not catalog reads. */
    private static ConnectorSpecStore emptySpecStore() {
        return new ConnectorSpecStore() {
            @Override
            public void put(String contentHash, byte[] spec) {
            }

            @Override
            public java.util.Optional<byte[]> get(String contentHash) {
                return java.util.Optional.empty();
            }
        };
    }

    private static ConnectorRegistry emptyConnectorRegistry() {
        return new ConnectorRegistry() {
            @Override
            public RegistrationOutcome register(String id, String pdkApiVersion, RegistrationSource source, byte[] artifact) {
                throw new UnsupportedOperationException();
            }

            @Override
            public java.util.List<ConnectorRegistration> list() {
                return java.util.List.of();
            }

            @Override
            public java.util.Optional<byte[]> artifact(String contentHash) {
                return java.util.Optional.empty();
            }

            @Override
            public boolean hasArtifact(String contentHash) {
                return false;
            }
        };
    }
}
