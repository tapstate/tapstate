package io.tapstate.control.restapi;

import io.tapstate.control.core.ArtifactQueryService;
import io.tapstate.control.core.ControlOperations;
import io.tapstate.control.core.CredentialAuthenticator;
import io.tapstate.control.core.GeneratedSecret;
import io.tapstate.control.core.PipelineLogQueryService;
import io.tapstate.control.core.PipelineObservationQueryService;
import io.tapstate.control.core.OperationRegistry;
import io.tapstate.control.core.Scope;
import io.tapstate.control.core.TokenSecrets;
import io.tapstate.control.core.TokenService;
import io.tapstate.control.core.TokenSigner;
import io.tapstate.control.core.VerifiedToken;
import io.tapstate.core.common.JsonReader;
import io.tapstate.core.lifecycle.Observation;
import io.tapstate.core.lifecycle.ObservationFailure;
import io.tapstate.core.lifecycle.PipelineState;
import io.tapstate.core.logging.LogLine;
import io.tapstate.core.logging.LogSink;
import io.tapstate.messages.MessageCatalog;
import io.tapstate.spi.store.ObservationStore;
import io.tapstate.core.model.SourceRef;
import io.tapstate.core.model.PipelineResource;
import io.tapstate.core.model.Resource;
import io.tapstate.core.model.SourceResource;
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

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.WebSocket;
import java.net.http.WebSocketHandshakeException;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * The two thin streaming read faces projected onto a websocket: {@code /api/pipelines/{id}/status/watch}
 * and {@code /api/pipelines/{id}/logs/follow}. Each rides a per-connection poll of the same control-core
 * query service the one-shot {@code GET} uses; it is not a general push framework and adds no registry
 * operation — the read-scoped grade of the underlying {@code pipeline.status} / {@code pipeline.logs}
 * operation gates the handshake. This proves a state change reaches a watcher, a new log line reaches a
 * follower, and an unauthenticated handshake is refused. The context is booted programmatically over
 * fake, seedable stores; the poll interval is shortened so the assertions do not wait on the default.
 */
class PipelineStreamApiTest {

    private static ConfigurableApplicationContext context;
    private static int port;

    @BeforeAll
    static void startServer() {
        context = new SpringApplicationBuilder(TestApp.class)
                .properties("server.port=0", "tapstate.control.stream.poll-interval=PT0.05S")
                .run();
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
        context.getBean(FakeObservationStore.class).clear();
        context.getBean(FakeLogSink.class).clear();
    }

    private String readToken() {
        return context.getBean(TokenService.class).create(Scope.READ);
    }

    private WebSocket connect(String path, String token, FrameSink sink) {
        return HttpClient.newHttpClient().newWebSocketBuilder()
                .header("Authorization", "Bearer " + token)
                .buildAsync(URI.create("ws://localhost:" + port + path), sink)
                .join();
    }

    // ---- a watcher gets the current state, then the next state on change ----

    @Test
    void watchStreamsTheStateAndThenTheNextStateOnChange() throws Exception {
        FakeObservationStore observations = context.getBean(FakeObservationStore.class);
        observations.save(new Observation("pl1", PipelineState.RUNNING, null, null));

        FrameSink sink = new FrameSink();
        WebSocket ws = connect("/api/pipelines/pl1/status/watch", readToken(), sink);
        try {
            Map<?, ?> first = sink.nextFrame();
            assertThat(first.get("pipelineId")).isEqualTo("pl1");
            assertThat(first.get("state")).isEqualTo("RUNNING");

            observations.save(new Observation("pl1", PipelineState.PAUSED, null, null));

            Map<?, ?> second = sink.nextFrame();
            assertThat(second.get("state")).isEqualTo("PAUSED");
        } finally {
            ws.abort();
        }
    }

    // ---- the one frame reporting a death also carries why, over a real connection ----

    @Test
    void watchStreamsTheCodedFailureReasonInTheSameFrameThatReportsAPipelineDead() throws Exception {
        FakeObservationStore observations = context.getBean(FakeObservationStore.class);
        observations.save(new Observation("pl1", PipelineState.FAILED, Map.of("errorCount", 1L),
                Map.of(), Map.of(), new ObservationFailure(
                        "engine.job-failed", Map.of("pipeline", "pl1", "cause", "sink refused the batch"))));

        FrameSink sink = new FrameSink();
        WebSocket ws = connect("/api/pipelines/pl1/status/watch", readToken(), sink);
        try {
            Map<?, ?> frame = sink.nextFrame();
            assertThat(frame.get("state")).isEqualTo("FAILED");
            Object failure = frame.get("failure");
            assertThat(failure).isInstanceOf(Map.class);
            Map<?, ?> failureMap = (Map<?, ?>) failure;
            assertThat(failureMap.get("code")).isEqualTo("engine.job-failed");
            assertThat(failureMap.get("message")).isNotNull();
            Map<?, ?> params = (Map<?, ?>) failureMap.get("params");
            assertThat(params.get("cause")).isEqualTo("sink refused the batch");
        } finally {
            ws.abort();
        }
    }

    // ---- a follower gets existing lines, then a newly appended one ----

    @Test
    void followStreamsTheTailAndThenNewlyAppendedLines() throws Exception {
        FakeLogSink sink = context.getBean(FakeLogSink.class);
        sink.append("pl2", new LogLine(1_700_000_000_000L, "INFO", "submitted job"));

        FrameSink frames = new FrameSink();
        WebSocket ws = connect("/api/pipelines/pl2/logs/follow", readToken(), frames);
        try {
            Map<?, ?> first = frames.nextFrame();
            assertThat(first.get("pipelineId")).isEqualTo("pl2");
            assertThat(messages(first)).containsExactly("submitted job");

            sink.append("pl2", new LogLine(1_700_000_000_100L, "INFO", "converged to RUNNING"));

            Map<?, ?> second = frames.nextFrame();
            assertThat(messages(second)).containsExactly("converged to RUNNING");
        } finally {
            ws.abort();
        }
    }

    // ---- the handshake is guarded like every other read ----

    @Test
    void anUnauthenticatedHandshakeIsRefusedUnauthorized() {
        assertThatThrownBy(() -> HttpClient.newHttpClient().newWebSocketBuilder()
                .buildAsync(URI.create("ws://localhost:" + port + "/api/pipelines/pl1/status/watch"), new FrameSink())
                .join())
                .hasCauseInstanceOf(WebSocketHandshakeException.class)
                .cause()
                .satisfies(cause -> assertThat(((WebSocketHandshakeException) cause).getResponse().statusCode())
                        .isEqualTo(401));
    }

    @Test
    void anUnauthenticatedFollowHandshakeIsRefusedUnauthorized() {
        // The one gate this endpoint has, and the only thing that would report its absence. Without the
        // interceptor the upgrade simply succeeds and an anonymous caller is handed every change to the
        // collection -- there is no projection gate over websocket paths, and no arch rule that a path
        // carries an interceptor, so nothing else in the build would say a word.
        assertThatThrownBy(() -> HttpClient.newHttpClient().newWebSocketBuilder()
                .buildAsync(URI.create(
                        "ws://localhost:" + port + "/api/data-browser/views/order_state/tail"), new FrameSink())
                .join())
                .hasCauseInstanceOf(WebSocketHandshakeException.class)
                .cause()
                .satisfies(cause -> assertThat(((WebSocketHandshakeException) cause).getResponse().statusCode())
                        .as("a follow that upgrades without a credential is an open read of the data")
                        .isEqualTo(401));
    }

    @Test
    void anAuthenticatedFollowHandshakeReachesTheFollowHandler() {
        FollowFixture fixture = context.getBean(FollowFixture.class);
        fixture.setAnswering(true);
        try {
            WebSocket ws = connect(
                    "/api/data-browser/views/order_state/tail", readToken(), new FrameSink());
            try {
                assertThat(ws.isOutputClosed()).isFalse();
            } finally {
                ws.abort();
            }
        } finally {
            fixture.setAnswering(false);
        }
    }

    @SuppressWarnings("unchecked")
    private static List<String> messages(Map<?, ?> frame) {
        List<String> out = new ArrayList<>();
        for (Object line : (List<Object>) frame.get("lines")) {
            out.add((String) ((Map<?, ?>) line).get("message"));
        }
        return out;
    }

    // ---- a listener that decodes each text frame into a JSON map on a blocking queue ----

    static final class FrameSink implements WebSocket.Listener {
        private final LinkedBlockingQueue<Map<?, ?>> frames = new LinkedBlockingQueue<>();
        private final StringBuilder partial = new StringBuilder();

        @Override
        public CompletionStage<?> onText(WebSocket webSocket, CharSequence data, boolean last) {
            partial.append(data);
            if (last) {
                frames.add((Map<?, ?>) JsonReader.parse(partial.toString()));
                partial.setLength(0);
            }
            webSocket.request(1);
            return null;
        }

        Map<?, ?> nextFrame() throws InterruptedException {
            Map<?, ?> frame = frames.poll(5, TimeUnit.SECONDS);
            assertThat(frame).as("a frame arrived within the timeout").isNotNull();
            return frame;
        }
    }

    @SpringBootConfiguration
    @EnableAutoConfiguration
    @Import({RestApiSecurityConfiguration.class, PipelineStreamConfiguration.class, DataBrowserStreamConfiguration.class})
    static class TestApp {

        @Bean
        MessageCatalog messageCatalog() {
            return MessageCatalog.bundled();
        }

        @Bean
        FakeObservationStore observationStore() {
            return new FakeObservationStore();
        }

        @Bean
        FakeLogSink logSink() {
            return new FakeLogSink();
        }

        @Bean
        FollowFixture followFixture() {
            return new FollowFixture();
        }

        @Bean
        PipelineObservationQueryService pipelineObservationQueryService(ObservationStore store) {
            return new PipelineObservationQueryService(new ArtifactQueryService(appliedPipelines()), store);
        }

        @Bean
        io.tapstate.control.core.DataBrowserService dataBrowserService(FollowFixture fixture) {
            return new io.tapstate.control.core.DataBrowserService(
                    appliedPipelines(),
                    new NoDiscoveries(),
                    config -> {
                        if (!fixture.answering()) {
                            throw new AssertionError("the handshake must be refused before any read");
                        }
                        return List.of("order_state");
                    },
                    (config, collection) -> {
                        throw new AssertionError("the handshake must be refused before any read");
                    },
                    (config, query) -> {
                        throw new AssertionError("the handshake must be refused before any read");
                    },
                    (config, request, listener) -> {
                        if (!fixture.answering()) {
                            throw new AssertionError("the handshake must be refused before any read");
                        }
                        return () -> { };
                    });
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
        TokenService tokenService(TokenStore store, TokenSecrets secrets) {
            return new TokenService(store, secrets, java.time.Clock.systemUTC());
        }

        @Bean
        OperationRegistry operationRegistry() {
            return ControlOperations.registry();
        }

        @Bean
        CredentialAuthenticator credentialAuthenticator(TokenService tokens, TokenSigner signer) {
            return new CredentialAuthenticator(tokens, signer);
        }
    }

    static final class FollowFixture {
        private boolean answering;

        boolean answering() {
            return answering;
        }

        void setAnswering(boolean answering) {
            this.answering = answering;
        }
    }

    // ---- fakes ----

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
                if (id.equals("views")) {
                    return Optional.of(new SourceResource(id, null, "mongodb",
                            Map.of("uri", "mongodb://db.local"), null, null, null, null, null));
                }
                return Optional.of(new PipelineResource(id, null, List.of(SourceRef.bare("src_x")), null, null, null, null, null));
            }

            @Override
            public List<Resource> list() {
                return List.of();
            }
        };
    }

}
