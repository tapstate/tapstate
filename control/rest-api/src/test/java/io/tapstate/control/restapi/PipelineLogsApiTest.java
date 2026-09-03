package io.tapstate.control.restapi;

import io.tapstate.control.core.ControlOperations;
import io.tapstate.control.core.CredentialAuthenticator;
import io.tapstate.control.core.Frontend;
import io.tapstate.control.core.GeneratedSecret;
import io.tapstate.control.core.Operation;
import io.tapstate.control.core.OperationRegistry;
import io.tapstate.control.core.PipelineLogQueryService;
import io.tapstate.control.core.PipelineLogs;
import io.tapstate.control.core.Scope;
import io.tapstate.control.core.TokenSecrets;
import io.tapstate.control.core.TokenService;
import io.tapstate.control.core.TokenSigner;
import io.tapstate.control.core.VerifiedToken;
import io.tapstate.core.logging.LogLine;
import io.tapstate.core.logging.LogSink;
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
 * The node-local logs read face projected onto the authenticated {@code /api} surface: {@code GET
 * /api/pipelines/{id}/logs}. It proves the tail round-trips through the real query service (over an
 * in-memory sink), that a read of a pipeline that has logged nothing is a benign empty tail with HTTP 200
 * (not the 404 the store-backed reads use for a missing observation), that the interceptor guards the read
 * like any other verb, and that the endpoint is a derivation of the registry. The context is booted
 * programmatically so the module stays on the reactor's JUnit line; it imports the logs controller with the
 * path configuration and the interceptor, so it exercises the guarded read path.
 */
class PipelineLogsApiTest {

    private static final Instant NOW = Instant.parse("2026-07-12T12:00:00Z");

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
    void seedSink() {
        FakeLogSink sink = context.getBean(FakeLogSink.class);
        sink.clear();
        sink.append("pl1", new LogLine(1_700_000_000_000L, "INFO", "submitted job"));
        sink.append("pl1", new LogLine(1_700_000_000_100L, "INFO", "converged to RUNNING"));
    }

    private RestClient client() {
        return RestClient.create("http://127.0.0.1:" + port);
    }

    private String machineToken(Scope scope) {
        return context.getBean(TokenService.class).create(scope);
    }

    // ---- the tail round-trips through the query service ----

    @Test
    void logsReturnsTheTailedLinesForAReadCredential() {
        PipelineLogs body = client().get().uri("/api/pipelines/pl1/logs")
                .header("Authorization", "Bearer " + machineToken(Scope.READ))
                .retrieve().toEntity(PipelineLogs.class).getBody();

        assertThat(body.pipelineId()).isEqualTo("pl1");
        assertThat(body.lines()).extracting(LogLine::message)
                .containsExactly("submitted job", "converged to RUNNING");
        assertThat(body.lines()).first()
                .satisfies(line -> {
                    assertThat(line.timestampMillis()).isEqualTo(1_700_000_000_000L);
                    assertThat(line.level()).isEqualTo("INFO");
                });
    }

    @Test
    void logsHonorsTheRequestedLineLimit() {
        PipelineLogs body = client().get().uri("/api/pipelines/pl1/logs?limit=1")
                .header("Authorization", "Bearer " + machineToken(Scope.READ))
                .retrieve().toEntity(PipelineLogs.class).getBody();

        assertThat(body.lines()).extracting(LogLine::message)
                .containsExactly("converged to RUNNING");
    }

    @Test
    void logsRejectsNonPositiveLimitsAsClientErrors() {
        for (String limit : List.of("0", "-1")) {
            ApiError body = client().get().uri("/api/pipelines/pl1/logs?limit=" + limit)
                    .header("Authorization", "Bearer " + machineToken(Scope.READ))
                    .exchange((request, response) -> {
                        assertThat(response.getStatusCode()).isEqualTo(HttpStatus.BAD_REQUEST);
                        return response.bodyTo(ApiError.class);
                    });

            assertThat(body.code()).isEqualTo("control.malformed-request");
        }
    }

    // ---- a read of a pipeline with no lines is a benign empty tail (200), never a 404 ----

    @Test
    void logsOfAPipelineWithNoLinesIsAnEmptyTailNotAnError() {
        PipelineLogs body = client().get().uri("/api/pipelines/ghost/logs")
                .header("Authorization", "Bearer " + machineToken(Scope.READ))
                .exchange((request, response) -> {
                    assertThat(response.getStatusCode()).isEqualTo(HttpStatus.OK);
                    return response.bodyTo(PipelineLogs.class);
                });

        assertThat(body.pipelineId()).isEqualTo("ghost");
        assertThat(body.lines()).isEmpty();
    }

    // ---- the interceptor guards the read like any other verb ----

    @Test
    void anUnauthenticatedReadIsUnauthorized() {
        ApiError body = client().get().uri("/api/pipelines/pl1/logs")
                .exchange((request, response) -> {
                    assertThat(response.getStatusCode()).isEqualTo(HttpStatus.UNAUTHORIZED);
                    return response.bodyTo(ApiError.class);
                });

        assertThat(body.code()).isEqualTo("control.unauthenticated");
    }

    // ---- the logs endpoint is a derivation of the registry ----

    @Test
    void theLogsFaceProjectsARegisteredCliExposedVerb() {
        Set<String> cliExposed = ControlOperations.registry()
                .exposedOn(Frontend.CLI).stream()
                .map(Operation::id).collect(Collectors.toSet());

        RequestMappingHandlerMapping mapping =
                context.getBean("requestMappingHandlerMapping", RequestMappingHandlerMapping.class);

        List<String> projected = new ArrayList<>();
        mapping.getHandlerMethods().forEach((info, handler) -> {
            Verb verb = handler.getMethodAnnotation(Verb.class);
            if (verb != null && verb.value().startsWith("pipeline.")) {
                projected.add(verb.value());
                assertThat(cliExposed)
                        .as("a projected logs face must be a registered, CLI-exposed operation")
                        .contains(verb.value());
            }
        });

        assertThat(projected)
                .as("only the logs face projects onto this focused context")
                .containsExactly("pipeline.logs");
    }

    /**
     * A focused boot config: auto-configures Web MVC + the embedded servlet container, imports the path
     * configuration, Spring Security, the logs controller and the coded-error advice over an in-memory token
     * graph. The logs read side is the real
     * {@link PipelineLogQueryService} over a fake, seedable node-local sink.
     */
    @SpringBootConfiguration
    @EnableAutoConfiguration
    @Import({RestApiConfiguration.class, RestApiSecurityConfiguration.class,
            PipelineLogsController.class, ApiExceptionHandler.class})
    static class TestApp {

        @Bean
        Clock clock() {
            return Clock.fixed(NOW, ZoneOffset.UTC);
        }

        @Bean
        FakeLogSink logSink() {
            return new FakeLogSink();
        }

        @Bean
        PipelineLogQueryService pipelineLogQueryService(LogSink sink) {
            return new PipelineLogQueryService(sink);
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

    /** An in-memory, seedable node-local log sink: append-ordered lines per pipeline id. */
    static final class FakeLogSink implements LogSink {
        private final Map<String, List<LogLine>> byId = new LinkedHashMap<>();

        void clear() {
            byId.clear();
        }

        @Override
        public void append(String pipelineId, LogLine line) {
            byId.computeIfAbsent(pipelineId, k -> new ArrayList<>()).add(line);
        }

        @Override
        public List<LogLine> tail(String pipelineId) {
            return List.copyOf(byId.getOrDefault(pipelineId, List.of()));
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
}
