package io.tapstate.control.restapi;

import io.tapstate.control.core.ArtifactQueryService;
import io.tapstate.control.core.ControlOperations;
import io.tapstate.control.core.CredentialAuthenticator;
import io.tapstate.control.core.Frontend;
import io.tapstate.control.core.GeneratedSecret;
import io.tapstate.control.core.Operation;
import io.tapstate.control.core.OperationRegistry;
import io.tapstate.control.core.PipelineMetrics;
import io.tapstate.control.core.PipelineObservationQueryService;
import io.tapstate.control.core.PipelineSnapshot;
import io.tapstate.control.core.PipelineStatus;
import io.tapstate.control.core.Scope;
import io.tapstate.control.core.TokenSecrets;
import io.tapstate.control.core.TokenService;
import io.tapstate.control.core.TokenSigner;
import io.tapstate.control.core.VerifiedToken;
import io.tapstate.core.lifecycle.Observation;
import io.tapstate.core.lifecycle.ObservationFailure;
import io.tapstate.core.lifecycle.PipelineState;
import io.tapstate.core.lifecycle.TableSnapshot;
import io.tapstate.spi.store.ObservationStore;
import io.tapstate.core.model.PipelineResource;
import io.tapstate.core.model.Resource;
import io.tapstate.spi.store.ArtifactStore;
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
import org.springframework.core.ParameterizedTypeReference;
import org.springframework.http.HttpStatus;
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
 * The three store-backed observation read faces projected onto the authenticated {@code /api} surface:
 * status / metrics / snapshot, each a {@code GET} on a pipeline instance ({@code GET
 * /api/pipelines/{id}/status}). It proves the reads round-trip through the real query service (over an
 * in-memory observation store), that a read of a pipeline that has published no observation surfaces as a
 * 404 with the {@code monitor.no-observation} coded body rather than a bare 500, that the interceptor
 * guards a read like any other verb, and that the three read endpoints are a derivation of the registry.
 * The context is booted programmatically so the module stays on the reactor's JUnit line; it imports the
 * read controller with the path configuration and the interceptor, so it exercises the guarded read path.
 */
class PipelineObservationApiTest {

    private static final Instant NOW = Instant.parse("2026-07-12T12:00:00Z");

    /** One running pipeline with a couple of metrics and one in-progress snapshot table. */
    private static final Observation PL1 = new Observation("pl1", PipelineState.RUNNING,
            Map.of("recordCount", 42L, "errorCount", 0L),
            Map.of("orders", new TableSnapshot(10, 100L, 10)));

    /** One running pipeline whose sink-acked source positions have advanced, to exercise perTableOffset. */
    private static final Observation PL_POS = new Observation("pl2", PipelineState.RUNNING,
            Map.of("recordCount", 6L, "errorCount", 0L),
            Map.of(),
            Map.of("orders", "w7"));

    /** One pipeline whose job died, to exercise the coded reason the status face carries. */
    private static final Observation PL_DEAD = new Observation("pl3", PipelineState.FAILED,
            Map.of("errorCount", 1L), Map.of(), Map.of(),
            new ObservationFailure("engine.job-failed",
                    Map.of("pipeline", "pl3", "cause", "the sink rejected the batch")));

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
    void resetStore() {
        FakeObservationStore observations = context.getBean(FakeObservationStore.class);
        observations.clear();
        observations.save(PL1);
        observations.save(PL_POS);
        observations.save(PL_DEAD);
    }

    private RestClient client() {
        return RestClient.create("http://127.0.0.1:" + port);
    }

    /** Mints a machine token of the given grade through the real token service and returns the bearer string. */
    private String machineToken(Scope scope) {
        return context.getBean(TokenService.class).create(scope);
    }

    // ---- the three read faces round-trip through the query service ----

    @Test
    void statusReturnsTheLifecycleStateForAReadCredential() {
        PipelineStatus body = client().get().uri("/api/pipelines/pl1/status")
                .header("Authorization", "Bearer " + machineToken(Scope.READ))
                .retrieve().toEntity(PipelineStatus.class).getBody();

        assertThat(body).isEqualTo(new PipelineStatus("pl1", PipelineState.RUNNING));
    }

    @Test
    void statusOfAFailedPipelineCarriesTheCodedReasonAndItsRenderedMessage() {
        // The code is the stable machine identity and the params are the variable data; the message is
        // rendered here, where the catalog lives, so every face prints one wording rather than each
        // inventing its own.
        Map<String, Object> body = client().get().uri("/api/pipelines/pl3/status")
                .header("Authorization", "Bearer " + machineToken(Scope.READ))
                .retrieve().body(new ParameterizedTypeReference<Map<String, Object>>() {});

        assertThat(body.get("state")).isEqualTo("FAILED");
        Map<?, ?> failure = (Map<?, ?>) body.get("failure");
        assertThat(failure.get("code")).isEqualTo("engine.job-failed");
        assertThat((String) failure.get("message")).contains("the sink rejected the batch");
        assertThat((Map<String, Object>) failure.get("params"))
                .containsEntry("cause", "the sink rejected the batch");
    }

    @Test
    void statusOfAHealthyPipelineOmitsTheFailure() {
        Map<String, Object> body = client().get().uri("/api/pipelines/pl1/status")
                .header("Authorization", "Bearer " + machineToken(Scope.READ))
                .retrieve().body(new ParameterizedTypeReference<Map<String, Object>>() {});

        // Absent, not present-and-null: a client reading this must not have to tell those apart.
        assertThat(body).doesNotContainKey("failure");
    }

    @Test
    void metricsReturnsTheOpenStatMap() {
        PipelineMetrics body = client().get().uri("/api/pipelines/pl1/metrics")
                .header("Authorization", "Bearer " + machineToken(Scope.READ))
                .retrieve().toEntity(PipelineMetrics.class).getBody();

        assertThat(body.pipelineId()).isEqualTo("pl1");
        assertThat(body.metrics()).containsEntry("recordCount", 42L).containsEntry("errorCount", 0L);
    }

    @Test
    void metricsExposesPerTableOffsetWhenPositionsArePublished() {
        Map<String, Object> body = client().get().uri("/api/pipelines/pl2/metrics")
                .header("Authorization", "Bearer " + machineToken(Scope.READ))
                .retrieve().body(new ParameterizedTypeReference<Map<String, Object>>() {});

        assertThat(body.get("pipelineId")).isEqualTo("pl2");
        // A source position is a string, and the metrics map is numeric run statistics. Carrying the
        // positions as a sibling rather than nested inside that map keeps every metrics cell a number,
        // so a reader never has to type-test a cell before using it.
        assertThat(body.get("perTableOffset")).isEqualTo(Map.of("orders", "w7"));
        Map<String, Object> metrics = (Map<String, Object>) body.get("metrics");
        assertThat(metrics).doesNotContainKey("perTableOffset");
        assertThat(metrics.get("recordCount")).isNotNull();
    }

    @Test
    void metricsOmitsPerTableOffsetWhenNoPositionsArePublished() {
        Map<String, Object> body = client().get().uri("/api/pipelines/pl1/metrics")
                .header("Authorization", "Bearer " + machineToken(Scope.READ))
                .retrieve().body(new ParameterizedTypeReference<Map<String, Object>>() {});

        // Absent, not an empty object: no position has been acked, which is the same never-faked rule the
        // numeric metrics follow.
        assertThat(body).doesNotContainKey("perTableOffset");
    }

    @Test
    void snapshotReturnsPerTableProgress() {
        PipelineSnapshot body = client().get().uri("/api/pipelines/pl1/snapshot")
                .header("Authorization", "Bearer " + machineToken(Scope.READ))
                .retrieve().toEntity(PipelineSnapshot.class).getBody();

        assertThat(body.pipelineId()).isEqualTo("pl1");
        assertThat(body.snapshot()).containsEntry("orders", new TableSnapshot(10, 100L, 10));
    }

    // ---- a read of a pipeline with no published observation is a 404 coded body, never a bare 500 ----

    @Test
    void aReadOfAPipelineThatWasNeverAppliedIsNotFoundAndSaysSo() {
        ApiError body = client().get().uri("/api/pipelines/ghost/status")
                .header("Authorization", "Bearer " + machineToken(Scope.READ))
                .exchange((request, response) -> {
                    assertThat(response.getStatusCode()).isEqualTo(HttpStatus.NOT_FOUND);
                    return response.bodyTo(ApiError.class);
                });

        // Never applied is permanent: a caller that read this as the transient unconverged window would
        // wait out its whole bound on what is almost always a mistyped id.
        assertThat(body.code()).isEqualTo("lifecycle.unknown-pipeline");
        assertThat(body.params()).containsEntry("pipeline", "ghost");
    }

    @Test
    void aReadOfAnAppliedPipelineThatHasNotConvergedYetIsNotFoundAsTheTransientWindow() {
        context.getBean(FakeObservationStore.class).clear();

        ApiError body = client().get().uri("/api/pipelines/pl1/status")
                .header("Authorization", "Bearer " + machineToken(Scope.READ))
                .exchange((request, response) -> {
                    assertThat(response.getStatusCode()).isEqualTo(HttpStatus.NOT_FOUND);
                    return response.bodyTo(ApiError.class);
                });

        assertThat(body.code()).isEqualTo("monitor.no-observation");
        assertThat(body.params()).containsEntry("pipeline", "pl1");
    }

    // ---- the interceptor guards a read like any other verb ----

    @Test
    void anUnauthenticatedReadIsUnauthorized() {
        ApiError body = client().get().uri("/api/pipelines/pl1/status")
                .exchange((request, response) -> {
                    assertThat(response.getStatusCode()).isEqualTo(HttpStatus.UNAUTHORIZED);
                    return response.bodyTo(ApiError.class);
                });

        assertThat(body.code()).isEqualTo("control.unauthenticated");
    }

    // ---- the read endpoints are a derivation of the registry ----

    @Test
    void theThreeReadFacesProjectRegisteredCliExposedVerbs() {
        Set<String> cliExposed = ControlOperations.registry()
                .exposedOn(Frontend.CLI).stream()
                .map(Operation::id).collect(Collectors.toSet());

        RequestMappingHandlerMapping mapping =
                context.getBean("requestMappingHandlerMapping", RequestMappingHandlerMapping.class);

        List<String> projectedReadFaces = new ArrayList<>();
        mapping.getHandlerMethods().forEach((info, handler) -> {
            Verb verb = handler.getMethodAnnotation(Verb.class);
            if (verb != null && verb.value().startsWith("pipeline.")) {
                projectedReadFaces.add(verb.value());
                assertThat(cliExposed)
                        .as("a projected observation read face must be a registered, CLI-exposed operation")
                        .contains(verb.value());
            }
        });

        assertThat(projectedReadFaces)
                .as("the three observation read faces project onto the authenticated /api surface")
                .containsExactlyInAnyOrder("pipeline.status", "pipeline.metrics", "pipeline.snapshot");
    }

    /**
     * A focused boot config: auto-configures Web MVC + the embedded servlet container, imports the path
     * configuration, Spring Security, the read controller and the coded-error advice over an in-memory token
     * graph. The observation read side is the real
     * {@link PipelineObservationQueryService} over a fake observation store.
     */
    @SpringBootConfiguration
    @EnableAutoConfiguration
    @Import({RestApiConfiguration.class, RestApiSecurityConfiguration.class,
            PipelineObservationController.class, ApiExceptionHandler.class})
    static class TestApp {

        @Bean
        Clock clock() {
            return Clock.fixed(NOW, ZoneOffset.UTC);
        }

        @Bean
        FakeObservationStore observationStore() {
            return new FakeObservationStore();
        }

        @Bean
        PipelineObservationQueryService pipelineObservationQueryService(ObservationStore observations) {
            return new PipelineObservationQueryService(new ArtifactQueryService(appliedPipelines()), observations);
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
        OperationRegistry operationRegistry() {
            return ControlOperations.registry();
        }

    }

    // ---- fakes ----

    /** An in-memory observation store, last write wins per pipeline id, seedable. */
    static final class FakeObservationStore implements ObservationStore {
        @Override
        public void delete(String pipelineId) {
            throw new UnsupportedOperationException("removal is not exercised by this double");
        }

        private final Map<String, Observation> byId = new LinkedHashMap<>();

        void clear() {
            byId.clear();
        }

        @Override
        public void save(Observation observation) {
            byId.put(observation.pipelineId(), observation);
        }

        @Override
        public Optional<Observation> read(String pipelineId) {
            return Optional.ofNullable(byId.get(pipelineId));
        }
    }

    /** An in-memory token store keyed by token id. */
    static final class FakeTokenStore implements TokenStore {
        private final Map<String, TokenRecord> byId = new LinkedHashMap<>();

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

    /** A signer whose token is a reversible {@code subject|SCOPE} encoding. */
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
     * The applied pipelines this context knows about. A read of one of these that has published no
     * observation is the transient unconverged window; a read of any other id is a pipeline that was never
     * applied, and the two answer different codes.
     */
    private static ArtifactStore appliedPipelines() {
        return storeHolding("pl1", "pl2", "pl3");
    }

    /** An artifact store answering with a minimal pipeline resource for each of the given ids. */
    private static ArtifactStore storeHolding(String... ids) {
        Map<String, Resource> byId = new LinkedHashMap<>();
        for (String id : ids) {
            byId.put(id, new PipelineResource(id, null, List.of("src_x"), null, null, null, null, null));
        }
        return new ArtifactStore() {
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
        };
    }

}
