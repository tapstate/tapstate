package io.tapstate.control.restapi;

import io.tapstate.control.core.AuditGate;
import io.tapstate.control.core.PipelinePreviewService;
import io.tapstate.control.core.Scope;
import io.tapstate.control.core.TapstatePrincipal;
import io.tapstate.control.core.VerifiedToken;
import io.tapstate.runtime.probe.PipelinePreviewEvent;
import io.tapstate.runtime.probe.PipelinePreviewStream;
import org.junit.jupiter.api.Test;
import org.springframework.boot.SpringBootConfiguration;
import org.springframework.boot.autoconfigure.EnableAutoConfiguration;
import org.springframework.boot.builder.SpringApplicationBuilder;
import org.springframework.boot.web.server.context.WebServerApplicationContext;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Import;
import org.springframework.security.authentication.UsernamePasswordAuthenticationToken;
import org.springframework.security.config.annotation.web.builders.HttpSecurity;
import org.springframework.security.config.annotation.web.configurers.AbstractHttpConfigurer;
import org.springframework.security.core.context.SecurityContextHolder;
import org.springframework.security.web.SecurityFilterChain;
import org.springframework.security.web.access.intercept.AuthorizationFilter;
import tools.jackson.databind.ObjectMapper;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.time.Clock;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;

/** A real servlet buffer must release a small preview prefix before its producer can finish. */
class PipelinePreviewStreamingSocketTest {
    @Test
    void theSocketReceivesEachPrefixBeforeTheTerminalBarrierIsReleased() throws Exception {
        try (ConfigurableApplicationContext context = new SpringApplicationBuilder(TestApp.class)
                .run("--server.address=127.0.0.1", "--server.port=0", "--spring.main.banner-mode=off");
             var readers = Executors.newSingleThreadExecutor(Thread.ofPlatform().daemon().factory());
             HttpClient client = HttpClient.newBuilder().connectTimeout(Duration.ofSeconds(3))
                     .version(HttpClient.Version.HTTP_1_1).build()) {
            Gate gate = context.getBean(Gate.class);
            int port = ((WebServerApplicationContext) context).getWebServer().getPort();
            String body = new ObjectMapper().writeValueAsString(Map.of("pipelineId", "preview",
                    "drafts", List.of(Map.of("content", SOURCE), Map.of("content", TARGET), Map.of("content", PIPELINE))));
            HttpRequest request = HttpRequest.newBuilder(URI.create("http://127.0.0.1:" + port + "/api/artifacts:preview"))
                    .timeout(Duration.ofSeconds(10)).header("Content-Type", "application/json")
                    .POST(HttpRequest.BodyPublishers.ofString(body)).build();
            var response = client.sendAsync(request, HttpResponse.BodyHandlers.ofInputStream());
            var prefix = readers.submit(() -> {
                HttpResponse<InputStream> received = response.get(3, TimeUnit.SECONDS);
                BufferedReader lines = new BufferedReader(new InputStreamReader(received.body(), StandardCharsets.UTF_8));
                return new Prefix(received, lines, lines.readLine(), lines.readLine());
            });
            try {
                assertThat(gate.entered.await(5, TimeUnit.SECONDS)).as("the producer reached its terminal barrier").isTrue();
                Prefix early;
                try {
                    early = prefix.get(3, TimeUnit.SECONDS);
                } catch (Exception failure) {
                    throw new AssertionError("The servlet held the small NDJSON prefix until preview completion", failure);
                }
                assertThat(early.response.statusCode()).isEqualTo(200);
                assertThat(early.response.headers().firstValue("Content-Type").orElseThrow()).startsWith("application/x-ndjson");
                assertThat(early.response.headers().firstValue("Cache-Control").orElseThrow()).contains("no-store");
                assertThat(early.first).contains("\"kind\":\"run.accepted\"");
                assertThat(early.second).contains("\"kind\":\"compile.completed\"");
                assertThat(gate.release.getCount()).as("no terminal event was available to fill or close the response").isEqualTo(1);
                assertThat(gate.cancelled).hasValue(0);
                gate.release.countDown();
                String terminal = readers.submit(early.lines::readLine).get(3, TimeUnit.SECONDS);
                assertThat(terminal).contains("\"kind\":\"run.completed\"");
                assertThat(readers.submit(early.lines::readLine).get(3, TimeUnit.SECONDS)).isNull();
                assertThat((early.first + early.second + terminal + "\n\n\n").getBytes(StandardCharsets.UTF_8).length)
                        .as("the full response is smaller than the default 8 KiB servlet buffer").isLessThan(8192);
                early.lines.close();
                assertThat(gate.cancelled).hasValue(1);
            } finally {
                gate.release.countDown();
                response.thenAccept(received -> {
                    try {
                        received.body().close();
                    } catch (IOException ignored) {
                        // The owned reader can race with this final close.
                    }
                });
                prefix.cancel(true);
            }
        }
    }

    private record Prefix(HttpResponse<InputStream> response, BufferedReader lines, String first, String second) {}

    static final class Gate {
        private final CountDownLatch entered = new CountDownLatch(1);
        private final CountDownLatch release = new CountDownLatch(1);
        private final AtomicInteger cancelled = new AtomicInteger();
    }

    @SpringBootConfiguration
    @EnableAutoConfiguration
    @Import({RestApiConfiguration.class, PipelinePreviewController.class})
    static class TestApp {
        @Bean
        Gate gate() {
            return new Gate();
        }

        @Bean
        PipelinePreviewService previews(Gate gate) {
            Clock clock = Clock.systemUTC();
            var apply = new PipelineApiTest.TestApp().applyService(new PipelineApiTest.FakeArtifactStore(),
                    new AuditGate(new PipelineApiTest.RecordingAuditStore(), clock));
            return new PipelinePreviewService(apply, coordinates -> new PipelinePreviewStream() {
                private boolean completed;
                @Override
                public PipelinePreviewEvent next() throws InterruptedException {
                    gate.entered.countDown();
                    if (!gate.release.await(10, TimeUnit.SECONDS)) {
                        throw new IllegalStateException("owned terminal barrier was not released");
                    }
                    if (completed) return null;
                    completed = true;
                    return new PipelinePreviewEvent(coordinates.runId(), coordinates.candidateHash(), 0,
                            "run.completed", clock.instant(), Map.of("rowCount", 0));
                }
                @Override
                public void cancel() {
                    gate.cancelled.incrementAndGet();
                    gate.release.countDown();
                }
            }, clock);
        }
        @Bean
        SecurityFilterChain testSecurity(HttpSecurity http) throws Exception {
            return http.csrf(AbstractHttpConfigurer::disable).authorizeHttpRequests(rules -> rules.anyRequest().permitAll())
                    .addFilterBefore((request, response, chain) -> {
                        var principal = TapstatePrincipal.humanJwt(new VerifiedToken("preview-author", Scope.READ));
                        SecurityContextHolder.getContext().setAuthentication(
                                UsernamePasswordAuthenticationToken.authenticated(principal, "unused", List.of()));
                        try {
                            chain.doFilter(request, response);
                        } finally {
                            SecurityContextHolder.clearContext();
                        }
                    }, AuthorizationFilter.class).build();
        }
    }

    private static final String SOURCE = """
            version: tapstate/v1
            kind: source
            id: input
            connector: mysql
            config: { host: preview-source.invalid, port: 3306, database: preview, username: fixture, password: fixture }
            mode: cdc
            """;
    private static final String TARGET = """
            version: tapstate/v1
            kind: source
            id: target
            connector: mongodb
            config: { isUri: true, uri: "mongodb://preview-target.invalid/preview" }
            """;
    private static final String PIPELINE = """
            version: tapstate/v1
            kind: pipeline
            id: preview
            source: input
            serve:
              from: /.*/
              sync:
                - id: output
                  source: target
                  write_mode: upsert
            """;
}
