package io.tapstate.control.restapi;

import io.tapstate.control.core.AiProviderClient;
import io.tapstate.control.core.AiProviderSettings;
import io.tapstate.control.core.PipelineDraftService;
import io.tapstate.control.core.SourceProjectionService;
import io.tapstate.control.core.SourceSchemaQueryService;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.ObjectProvider;
import org.springframework.boot.SpringBootConfiguration;
import org.springframework.boot.autoconfigure.EnableAutoConfiguration;
import org.springframework.boot.builder.SpringApplicationBuilder;
import org.springframework.boot.web.server.context.WebServerApplicationContext;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Import;
import org.springframework.security.config.annotation.web.builders.HttpSecurity;
import org.springframework.security.config.annotation.web.configurers.AbstractHttpConfigurer;
import org.springframework.security.web.SecurityFilterChain;
import tools.jackson.databind.ObjectMapper;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;

import static org.assertj.core.api.Assertions.assertThat;

/** A real MVC response must release small SSE frames while the provider remains active. */
class PipelineAssistantStreamingSocketTest {
    @Test void textArrivesBeforeTheProviderCompletes() throws Exception {
        try (var context = new SpringApplicationBuilder(TestApp.class).run("--server.address=127.0.0.1",
                "--server.port=0", "--spring.main.banner-mode=off");
             var client = HttpClient.newBuilder().connectTimeout(Duration.ofSeconds(3)).build();
             var readers = java.util.concurrent.Executors.newSingleThreadExecutor()) {
            Gate gate = context.getBean(Gate.class);
            int port = ((WebServerApplicationContext) context).getWebServer().getPort();
            var request = HttpRequest.newBuilder(URI.create("http://127.0.0.1:" + port + "/api/pipelines/orders/draft:assistant"))
                    .timeout(Duration.ofSeconds(15)).header("Content-Type", "application/json")
                    .POST(HttpRequest.BodyPublishers.ofString("{\"revision\":1,\"messages\":[{\"role\":\"user\",\"text\":\"hello\"}]}"))
                    .build();
            var response = client.sendAsync(request, HttpResponse.BodyHandlers.ofInputStream());
            var prefix = readers.submit(() -> {
                var received = response.get(5, TimeUnit.SECONDS);
                assertThat(received.statusCode()).isEqualTo(200);
                assertThat(received.headers().firstValue("Content-Type").orElseThrow()).startsWith("text/event-stream");
                var lines = new BufferedReader(new InputStreamReader(received.body(), StandardCharsets.UTF_8));
                for (String line; (line = lines.readLine()) != null;) {
                    if (line.contains("text-delta") && line.contains("early text")) return lines;
                }
                throw new AssertionError("No text delta arrived");
            });
            try {
                assertThat(gate.entered.await(5, TimeUnit.SECONDS)).isTrue();
                BufferedReader early;
                try { early = prefix.get(3, TimeUnit.SECONDS); }
                catch (Exception failure) {
                    throw new AssertionError("The servlet buffered SSE until provider completion", failure);
                }
                assertThat(gate.release.getCount()).as("provider completion is still blocked").isEqualTo(1);
                gate.release.countDown();
                String rest = readers.submit(() -> early.lines().reduce("", (a, b) -> a + b)).get(5, TimeUnit.SECONDS);
                assertThat(rest).contains("text-end", "[DONE]");
                early.close();
            } finally {
                gate.release.countDown();
                prefix.cancel(true);
                response.thenAccept(received -> { try { received.body().close(); } catch (java.io.IOException ignored) { } });
            }
        }
    }

    static final class Gate {
        final CountDownLatch entered = new CountDownLatch(1);
        final CountDownLatch release = new CountDownLatch(1);
    }

    @SpringBootConfiguration
    @EnableAutoConfiguration
    @Import(RestApiConfiguration.class)
    static class TestApp {
        @Bean Gate gate() { return new Gate(); }
        @Bean PipelineDraftService drafts() {
            var store = new PipelineApiTest.FakePipelineDraftStore();
            store.create(PipelineAssistantControllerTest.draft());
            return new PipelineDraftService(store);
        }
        @Bean PipelineAssistantController assistant(ObjectProvider<PipelineDraftService> drafts,
                ObjectProvider<SourceProjectionService> sources, ObjectProvider<SourceSchemaQueryService> schemas,
                ObjectMapper json, Gate gate) {
            AiProviderClient provider = new AiProviderClient() {
                public void check(AiProviderSettings settings) { }
                public Optional<Map<String, Object>> propose(AiProviderSettings settings, List<Map<String, String>> messages,
                        Map<String, Object> context, Consumer<String> text) {
                    text.accept("early text");
                    gate.entered.countDown();
                    try { gate.release.await(); }
                    catch (InterruptedException interrupted) { Thread.currentThread().interrupt(); }
                    return Optional.empty();
                }
            };
            return new PipelineAssistantController(drafts, sources, schemas,
                    PipelineAssistantControllerTest.providerController(provider, () -> {}), provider, json);
        }
        @Bean SecurityFilterChain testSecurity(HttpSecurity http) throws Exception {
            return http.csrf(AbstractHttpConfigurer::disable)
                    .authorizeHttpRequests(rules -> rules.anyRequest().permitAll()).build();
        }
    }
}
