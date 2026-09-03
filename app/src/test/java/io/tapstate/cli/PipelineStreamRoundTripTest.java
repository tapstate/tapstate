package io.tapstate.cli;

import io.tapstate.control.core.CredentialAuthenticator;
import io.tapstate.control.core.GeneratedSecret;
import io.tapstate.control.core.PipelineLogQueryService;
import io.tapstate.control.core.ArtifactQueryService;
import io.tapstate.control.core.PipelineObservationQueryService;
import io.tapstate.control.core.Scope;
import io.tapstate.control.core.TokenSecrets;
import io.tapstate.control.core.TokenService;
import io.tapstate.control.core.TokenSigner;
import io.tapstate.control.core.VerifiedToken;
import io.tapstate.control.restapi.PipelineStreamConfiguration;
import io.tapstate.core.lifecycle.Observation;
import io.tapstate.core.lifecycle.ObservationFailure;
import io.tapstate.core.lifecycle.PipelineState;
import io.tapstate.core.logging.LogLine;
import io.tapstate.core.logging.LogSink;
import io.tapstate.core.model.PipelineResource;
import io.tapstate.core.model.Resource;
import io.tapstate.messages.MessageCatalog;
import io.tapstate.spi.store.ArtifactStore;
import io.tapstate.spi.store.ObservationStore;
import io.tapstate.spi.store.TokenRecord;
import io.tapstate.spi.store.TokenStore;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.boot.SpringBootConfiguration;
import org.springframework.boot.autoconfigure.EnableAutoConfiguration;
import org.springframework.boot.builder.SpringApplicationBuilder;
import org.springframework.boot.web.server.context.WebServerApplicationContext;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Import;
import org.springframework.security.config.annotation.web.builders.HttpSecurity;
import org.springframework.security.config.annotation.web.configurers.AbstractHttpConfigurer;
import org.springframework.security.web.SecurityFilterChain;

import java.net.URI;
import java.time.Clock;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * The real cli-client to server websocket round-trip: the production {@link HttpControlPlaneClient}'s
 * streaming half — its connect loop and frame reassembly — driven against a real server, closing the gap
 * the server-side stream test leaves (it drives the wire with a test listener, not the production client).
 * A status change reaches the client's status stream and a newly appended log line reaches its log stream,
 * through the same code the REPL runs. The server is booted over fake, seedable stores and a shortened poll
 * so the assertions do not wait on the default cadence; the CLI client is a test-scope dependency here, so
 * the assembly root never ships it.
 */
class PipelineStreamRoundTripTest {

    private static ConfigurableApplicationContext context;
    private static int port;

    private final HttpControlPlaneClient client = new HttpControlPlaneClient();

    @BeforeAll
    static void startServer() {
        context = new SpringApplicationBuilder(TestApp.class)
                .properties("tapstate.control.stream.poll-interval=PT0.05S")
                // Both the address and the port are run arguments, not default properties: `properties(...)`
                // populates the lowest-ranked source Spring has, and the product's own application
                // configuration publishes 8080, so a port asked for there is silently overridden. The
                // address is the other half -- a free port alone binds the wildcard, and a wildcard bind
                // does not reserve 127.0.0.1:<port>, so a process already holding that port on the loopback
                // keeps receiving what this test sends.
                .run("--server.address=127.0.0.1", "--server.port=0");
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

    private URI baseUrl() {
        // The literal address, not the name: "localhost" resolves to both 127.0.0.1 and ::1, and only
        // one of those is the address bound above.
        return URI.create("http://127.0.0.1:" + port);
    }

    private String readToken() {
        return context.getBean(TokenService.class).create(Scope.READ);
    }

    @Test
    @DisplayName("watchStatus delivers the current state then the next state on change")
    void watchStatusDeliversTheCurrentStateThenTheChange() throws Exception {
        FakeObservationStore observations = context.getBean(FakeObservationStore.class);
        observations.save(new Observation("pl1", PipelineState.RUNNING, null, null));

        BlockingQueue<String> states = new LinkedBlockingQueue<>();
        AtomicBoolean stop = new AtomicBoolean(false);
        Thread watcher = new Thread(() -> client.watchStatus(
                baseUrl(), readToken(), "pl1", (id, state, failureCode, failureMessage) -> states.add(state), stop::get));
        watcher.setDaemon(true);
        watcher.start();
        try {
            assertThat(states.poll(5, TimeUnit.SECONDS)).isEqualTo("RUNNING");

            observations.save(new Observation("pl1", PipelineState.PAUSED, null, null));

            assertThat(states.poll(5, TimeUnit.SECONDS)).isEqualTo("PAUSED");
        } finally {
            stop.set(true);
            watcher.join(TimeUnit.SECONDS.toMillis(3));
        }
    }

    @Test
    @DisplayName("watchStatus delivers the coded failure reason alongside a FAILED state")
    void watchStatusDeliversTheFailureReasonAlongsideAFailedState() throws Exception {
        // The full stack, real server: app assembly -> control-core -> rest-api -> a real websocket -> the
        // production HttpControlPlaneClient. Proves the reason a pipeline died actually reaches a CLI-side
        // caller end to end, not just that some intermediate layer encodes it correctly in isolation.
        FakeObservationStore observations = context.getBean(FakeObservationStore.class);
        observations.save(new Observation("pl1", PipelineState.FAILED, Map.of("errorCount", 1L),
                Map.of(), Map.of(), new ObservationFailure(
                        "engine.job-failed", Map.of("pipeline", "pl1", "cause", "sink refused the batch"))));

        BlockingQueue<String[]> found = new LinkedBlockingQueue<>();
        AtomicBoolean stop = new AtomicBoolean(false);
        Thread watcher = new Thread(() -> client.watchStatus(baseUrl(), readToken(), "pl1",
                (id, state, failureCode, failureMessage) -> found.add(new String[] {state, failureCode, failureMessage}),
                stop::get));
        watcher.setDaemon(true);
        watcher.start();
        try {
            String[] frame = found.poll(5, TimeUnit.SECONDS);
            assertThat(frame).isNotNull();
            assertThat(frame[0]).isEqualTo("FAILED");
            assertThat(frame[1]).isEqualTo("engine.job-failed");
            assertThat(frame[2]).contains("sink refused the batch");
        } finally {
            stop.set(true);
            watcher.join(TimeUnit.SECONDS.toMillis(3));
        }
    }

    @Test
    @DisplayName("watchStatus of a never-applied id terminates with the coded refusal, not an endless reconnect")
    void watchStatusOfAnUnknownPipelineTerminatesWithTheCodedRefusal() throws Exception {
        // Full stack again, for the negative path: the server closes the websocket deliberately with the
        // permanent code, and the production client must treat that close as terminal. A client that read
        // it as a dropped connection would re-attach every backoff forever -- the watch would never return
        // and the join below would time out with the thread still alive.
        AtomicBoolean stop = new AtomicBoolean(false);
        AtomicReference<String> refusal = new AtomicReference<>();
        Thread watcher = new Thread(() -> refusal.set(client.watchStatus(
                baseUrl(), readToken(), "ghost", (id, state, failureCode, failureMessage) -> { }, stop::get)));
        watcher.setDaemon(true);
        watcher.start();

        watcher.join(TimeUnit.SECONDS.toMillis(5));

        assertThat(watcher.isAlive())
                .as("the watch must end on its own; a live thread here means it is reconnecting forever")
                .isFalse();
        assertThat(refusal.get()).isEqualTo("lifecycle.unknown-pipeline");
    }

    @Test
    @DisplayName("followLogs delivers the existing tail then a newly appended line")
    void followLogsDeliversTheTailThenNewLines() throws Exception {
        FakeLogSink logs = context.getBean(FakeLogSink.class);
        logs.append("pl2", new LogLine(1_700_000_000_000L, "INFO", "submitted job"));

        BlockingQueue<List<RemoteLogLine>> batches = new LinkedBlockingQueue<>();
        AtomicBoolean stop = new AtomicBoolean(false);
        Thread follower = new Thread(() -> client.followLogs(
                baseUrl(), readToken(), "pl2", (id, lines) -> batches.add(lines), stop::get));
        follower.setDaemon(true);
        follower.start();
        try {
            List<RemoteLogLine> first = batches.poll(5, TimeUnit.SECONDS);
            assertThat(first).extracting(RemoteLogLine::message).containsExactly("submitted job");

            logs.append("pl2", new LogLine(1_700_000_000_100L, "INFO", "converged to RUNNING"));

            List<RemoteLogLine> second = batches.poll(5, TimeUnit.SECONDS);
            assertThat(second).extracting(RemoteLogLine::message).containsExactly("converged to RUNNING");
        } finally {
            stop.set(true);
            follower.join(TimeUnit.SECONDS.toMillis(3));
        }
    }

    /**
     * A focused boot of the streaming server half: the public stream configuration over the real query
     * services, guarded by the real credential authenticator, all over fake seedable stores.
     */
    @SpringBootConfiguration
    @EnableAutoConfiguration
    @Import(PipelineStreamConfiguration.class)
    static class TestApp {

        @Bean
        SecurityFilterChain streamOnlyTestSecurity(HttpSecurity http) throws Exception {
            return http
                    .csrf(AbstractHttpConfigurer::disable)
                    .authorizeHttpRequests(authorize -> authorize.anyRequest().permitAll())
                    .build();
        }

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
        PipelineObservationQueryService pipelineObservationQueryService(ObservationStore store) {
            // Every id reads as applied except the "ghost" prefix, which resolves to nothing -- the one
            // case in this round trip that is about telling an applied pipeline from one never applied.
            return new PipelineObservationQueryService(new ArtifactQueryService(new ArtifactStore() {
                @Override
                public void saveAll(java.util.List<Resource> artifacts) {
                }

                @Override
                public java.util.Optional<Resource> get(String id) {
                    if (id.startsWith("ghost")) {
                        return java.util.Optional.empty();
                    }
                    return java.util.Optional.of(
                            new PipelineResource(id, null,
                                    java.util.List.of(io.tapstate.core.model.SourceRef.bare("src_x")),
                                    null, null, null, null, null));
                }

                @Override
                public java.util.List<Resource> list() {
                    return java.util.List.of();
                }
            }), store);
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
            return new TokenService(store, secrets, Clock.systemUTC());
        }

        @Bean
        CredentialAuthenticator credentialAuthenticator(TokenService tokens, TokenSigner signer) {
            return new CredentialAuthenticator(tokens, signer);
        }
    }

    // ---- fakes ----

    static final class FakeObservationStore implements ObservationStore {
        @Override
        public void delete(String pipelineId) {
            throw new UnsupportedOperationException("removal is not exercised by this double");
        }

        private final Map<String, Observation> byId = new ConcurrentHashMap<>();

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
        private final Map<String, List<LogLine>> byId = new ConcurrentHashMap<>();

        void clear() {
            byId.clear();
        }

        @Override
        public void append(String pipelineId, LogLine line) {
            byId.computeIfAbsent(pipelineId, k -> new CopyOnWriteArrayList<>()).add(line);
        }

        @Override
        public List<LogLine> tail(String pipelineId) {
            return List.copyOf(byId.getOrDefault(pipelineId, List.of()));
        }
    }

    static final class FakeTokenStore implements TokenStore {
        private final Map<String, TokenRecord> byId = new ConcurrentHashMap<>();

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
}
