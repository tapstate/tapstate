package io.tapstate.app;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.sun.net.httpserver.HttpServer;
import io.tapstate.control.core.CloudStatusReporter;
import io.tapstate.core.common.TapstateException;
import io.tapstate.core.lifecycle.DesiredState;
import io.tapstate.core.lifecycle.Observation;
import io.tapstate.core.lifecycle.ObservationFailure;
import io.tapstate.core.lifecycle.PipelineState;
import io.tapstate.spi.store.StorePort;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

import java.io.InputStream;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.time.Clock;
import java.time.Instant;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicReference;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/** Exercises the real status projection, reporter and SDK transport as one consumer boundary. */
class SdkStatusProjectionTest {

    private static final ObjectMapper JSON = new ObjectMapper();
    private static final String CLUSTER = "status-cluster";
    private static final String TOKEN = "status-static-token-sentinel";
    private static final Instant ERROR_AT = Instant.parse("2026-09-29T08:12:00Z");
    private static final Instant NOW = ERROR_AT.plusSeconds(60);
    private static final Instant STARTED = NOW.minusMillis(43_200_000);
    private static final String ACCEPTED =
            "{\"opId\":\"status-fixture\",\"code\":\"ok\",\"msg\":\"ok\",\"data\":{\"status\":\"accepted\"}}";

    @Test
    void storedObservationsReachTheSdkAsExactlyTheProviderRequestGolden() throws Exception {
        InMemoryStorePort storage = seededStorage();
        StorePort projectionStore = projectionOnly(storage);
        try (StatusEndpoint endpoint = new StatusEndpoint()) {
            CloudStatusReporter reporter = reporter(projectionStore, Clock.fixed(NOW, ZoneOffset.UTC),
                    endpoint, new ArrayDeque<>(List.of("550e8400-e29b-41d4-a716-446655440000")));

            reporter.report();

            JsonNode expected;
            try (InputStream golden = Objects.requireNonNull(
                    getClass().getResourceAsStream("/cloud/status-report-request.json"))) {
                expected = JSON.readTree(golden);
            }
            assertThat(endpoint.requests).hasSize(1);
            assertThat(endpoint.requests.getFirst()).isEqualTo(expected);
            assertThat(endpoint.authorizations).containsExactly("Bearer " + TOKEN);
            assertThat(endpoint.methods).containsExactly("POST");
            assertThat(endpoint.paths).containsExactly("/v1/api/clusters/" + CLUSTER + "/status-report");
            verify(projectionStore, never()).artifacts();
            verify(projectionStore, never()).catalog();
        }
    }

    @Test
    void theNextReportProjectsCurrentStoreFactsWithANewNonceAndNoStaleError() throws Exception {
        InMemoryStorePort storage = seededStorage();
        StorePort projectionStore = projectionOnly(storage);
        AtomicReference<Instant> time = new AtomicReference<>(NOW);
        try (StatusEndpoint endpoint = new StatusEndpoint()) {
            CloudStatusReporter reporter = reporter(projectionStore, clock(time, ZoneOffset.UTC), endpoint,
                    new ArrayDeque<>(List.of("first-status-nonce", "second-status-nonce")));
            reporter.report();

            for (String id : storage.desired().pipelineIds()) {
                observe(storage, id, PipelineState.STOPPED, null, NOW.plusSeconds(45));
            }
            // Orphaned observations and a desired-only running state cannot inflate the active count.
            storage.observations().save(new Observation("orphan-running", PipelineState.RUNNING,
                    Map.of(), Map.of(), Map.of(), null, NOW.plusSeconds(45)));
            storage.desired().save(new DesiredState("desired-only", PipelineState.RUNNING, "revision"));
            time.set(NOW.plusSeconds(45));
            reporter.report();

            assertThat(endpoint.requests).hasSize(2);
            JsonNode first = endpoint.requests.getFirst();
            JsonNode second = endpoint.requests.getLast();
            assertThat(first.path("nonce").asText()).isEqualTo("first-status-nonce");
            assertThat(first.path("activePipelines").asInt()).isEqualTo(3);
            assertThat(first.path("lastErrorAt").asText()).isEqualTo(ERROR_AT.toString());
            assertThat(second.path("nonce").asText()).isEqualTo("second-status-nonce");
            assertThat(second.path("runtimeVersion").asText()).isEqualTo("2.1.0");
            assertThat(second.has("uptimeMs")).isTrue();
            assertThat(second.get("uptimeMs").isIntegralNumber()).isTrue();
            assertThat(second.get("uptimeMs").longValue()).isEqualTo(43_245_000);
            assertThat(second.has("activePipelines")).isTrue();
            assertThat(second.get("activePipelines").isInt()).isTrue();
            assertThat(second.get("activePipelines").intValue()).isZero();
            assertThat(second.has("lastErrorAt")).isFalse();
            assertThat(fieldNames(second)).containsExactlyInAnyOrder(
                    "nonce", "runtimeVersion", "uptimeMs", "activePipelines");
            assertThat(endpoint.authorizations).containsExactly("Bearer " + TOKEN, "Bearer " + TOKEN);
            assertThat(endpoint.requests.toString()).doesNotContain(
                    TOKEN, "metadata-password-sentinel", "atlas.invalid",
                    "failure-password-sentinel", "failure-uri-sentinel", "pipeline.failed");
            verify(projectionStore, never()).artifacts();
            verify(projectionStore, never()).catalog();
        }
    }

    @ParameterizedTest
    @CsvSource({"401, sdk.token-invalid", "409, auth.nonce-replay"})
    void anHttpRejectionIsSafeAndDoesNotPreventTheNextReport(int status, String code) throws Exception {
        StorePort projectionStore = projectionOnly(seededStorage());
        try (StatusEndpoint endpoint = new StatusEndpoint()) {
            endpoint.response.set(new Response(status,
                    JSON.writeValueAsString(Map.of("opId", "status-failed", "code", code,
                            "msg", "provider-password-sentinel",
                            "data", Map.of("uri", "provider-uri-sentinel")))));
            CloudStatusReporter reporter = reporter(projectionStore, Clock.fixed(NOW, ZoneOffset.UTC),
                    endpoint, new ArrayDeque<>(List.of("rejected-status-nonce", "recovered-status-nonce")));

            assertThatThrownBy(reporter::report)
                    .isInstanceOfSatisfying(TapstateException.class, failure -> {
                        assertThat(failure.code()).isEqualTo(BootError.CLOUD_STATUS_SDK_REQUIRED);
                        assertThat(failure.args()).isEmpty();
                    })
                    .hasNoCause()
                    .hasMessageNotContaining("provider-password-sentinel")
                    .hasMessageNotContaining("provider-uri-sentinel")
                    .hasMessageNotContaining(TOKEN);
            assertThat(endpoint.requests).isNotEmpty().allSatisfy(request ->
                    assertThat(request.path("nonce").asText()).isEqualTo("rejected-status-nonce"));

            endpoint.response.set(new Response(200, ACCEPTED));
            reporter.report();

            assertThat(endpoint.requests.getLast().path("nonce").asText())
                    .isEqualTo("recovered-status-nonce");
            assertThat(endpoint.authorizations).allMatch(value -> value.equals("Bearer " + TOKEN));
            verify(projectionStore, never()).artifacts();
            verify(projectionStore, never()).catalog();
        }
    }

    @ParameterizedTest
    @CsvSource({"provider-code-sentinel, accepted", "ok, missing-data", "ok, provider-status-sentinel"})
    void anUnacknowledgedSuccessfulHttpResponseIsSafeAndTheNextReportCanRecover(
            String code, String acknowledgment) throws Exception {
        StorePort projectionStore = projectionOnly(seededStorage());
        try (StatusEndpoint endpoint = new StatusEndpoint()) {
            Map<String, Object> body = new LinkedHashMap<>(Map.of(
                    "opId", "status-unacknowledged", "code", code, "msg", "provider-password-sentinel"));
            if (!acknowledgment.equals("missing-data")) {
                body.put("data", Map.of("status", acknowledgment, "uri", "provider-uri-sentinel"));
            }
            endpoint.response.set(new Response(200, JSON.writeValueAsString(body)));
            CloudStatusReporter reporter = reporter(projectionStore, Clock.fixed(NOW, ZoneOffset.UTC),
                    endpoint, new ArrayDeque<>(List.of("unacknowledged-status-nonce", "recovered-status-nonce")));

            assertThatThrownBy(reporter::report)
                    .isInstanceOfSatisfying(TapstateException.class, failure -> {
                        assertThat(failure.code()).isEqualTo(BootError.CLOUD_STATUS_SDK_REQUIRED);
                        assertThat(failure.args()).isEmpty();
                    })
                    .hasNoCause()
                    .hasMessageNotContaining("provider-code-sentinel")
                    .hasMessageNotContaining("provider-password-sentinel")
                    .hasMessageNotContaining("provider-uri-sentinel")
                    .hasMessageNotContaining("provider-status-sentinel")
                    .hasMessageNotContaining(TOKEN);
            assertThat(endpoint.requests).hasSize(1);
            assertThat(endpoint.requests.getFirst().path("nonce").asText())
                    .isEqualTo("unacknowledged-status-nonce");

            endpoint.response.set(new Response(200, ACCEPTED));
            reporter.report();

            assertThat(endpoint.requests).hasSize(2);
            assertThat(endpoint.requests.getLast().path("nonce").asText())
                    .isEqualTo("recovered-status-nonce");
            assertThat(endpoint.authorizations).containsExactly("Bearer " + TOKEN, "Bearer " + TOKEN);
            assertThat(endpoint.methods).containsExactly("POST", "POST");
            assertThat(endpoint.paths).containsExactly(
                    "/v1/api/clusters/" + CLUSTER + "/status-report",
                    "/v1/api/clusters/" + CLUSTER + "/status-report");
            verify(projectionStore, never()).artifacts();
            verify(projectionStore, never()).catalog();
        }
    }

    private static InMemoryStorePort seededStorage() {
        InMemoryStorePort storage = new InMemoryStorePort();
        observe(storage, "running-one", PipelineState.RUNNING, null, NOW.minusSeconds(2));
        observe(storage, "running-two", PipelineState.RUNNING, null, NOW.minusSeconds(3));
        observe(storage, "paused", PipelineState.PAUSED, null, NOW.minusSeconds(4));
        ObservationFailure sensitiveFailure = new ObservationFailure("pipeline.failed", Map.of(
                "password", "failure-password-sentinel", "uri", "failure-uri-sentinel"));
        observe(storage, "failed-old", PipelineState.FAILED, sensitiveFailure, ERROR_AT.minusSeconds(30));
        observe(storage, "failed-new", PipelineState.FAILED, sensitiveFailure, ERROR_AT);
        observe(storage, "stopped", PipelineState.STOPPED, null, NOW.minusSeconds(1));
        storage.desired().save(new DesiredState("unobserved", PipelineState.RUNNING, "revision"));
        return storage;
    }

    private static StorePort projectionOnly(InMemoryStorePort storage) {
        StorePort projectionStore = mock(StorePort.class);
        when(projectionStore.desired()).thenReturn(storage.desired());
        when(projectionStore.observations()).thenReturn(storage.observations());
        return projectionStore;
    }

    private static CloudStatusReporter reporter(
            StorePort store, Clock clock, StatusEndpoint endpoint, ArrayDeque<String> nonces) {
        CloudProperties properties = new CloudProperties();
        properties.setBaseUrl(endpoint.baseUrl());
        properties.setToken(TOKEN);
        properties.setAtlasUri("mongodb://unused:metadata-password-sentinel@atlas.invalid/metadata");
        properties.setClusterId(CLUSTER);
        return new CloudStatusReporter(CLUSTER,
                new StoreBackedCloudRuntimeStatusProvider(store, "2.1.0", clock, STARTED),
                new CloudSdkBridge(CloudRuntimeSettings.resolve(properties)), nonces::removeFirst);
    }

    private static void observe(InMemoryStorePort storage, String id, PipelineState state,
            ObservationFailure failure, Instant observedAt) {
        storage.desired().save(new DesiredState(id, state, "revision"));
        storage.observations().save(new Observation(
                id, state, Map.of(), Map.of(), Map.of(), failure, observedAt));
    }

    private static List<String> fieldNames(JsonNode node) {
        List<String> names = new ArrayList<>();
        node.fieldNames().forEachRemaining(names::add);
        return names;
    }

    private static Clock clock(AtomicReference<Instant> time, ZoneId zone) {
        return new Clock() {
            @Override public ZoneId getZone() { return zone; }
            @Override public Clock withZone(ZoneId newZone) { return clock(time, newZone); }
            @Override public Instant instant() { return time.get(); }
        };
    }

    private static final class StatusEndpoint implements AutoCloseable {
        private final HttpServer server;
        private final AtomicReference<Response> response = new AtomicReference<>(new Response(200, ACCEPTED));
        private final List<JsonNode> requests = new CopyOnWriteArrayList<>();
        private final List<String> authorizations = new CopyOnWriteArrayList<>();
        private final List<String> methods = new CopyOnWriteArrayList<>();
        private final List<String> paths = new CopyOnWriteArrayList<>();

        StatusEndpoint() throws Exception {
            server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
            server.createContext("/", exchange -> {
                requests.add(JSON.readTree(exchange.getRequestBody()));
                authorizations.add(exchange.getRequestHeaders().getFirst("Authorization"));
                methods.add(exchange.getRequestMethod());
                paths.add(exchange.getRequestURI().getPath());
                Response reply = response.get();
                byte[] responseBytes = reply.body().getBytes(StandardCharsets.UTF_8);
                exchange.getResponseHeaders().set("Content-Type", "application/json");
                exchange.sendResponseHeaders(reply.status(), responseBytes.length);
                exchange.getResponseBody().write(responseBytes);
                exchange.close();
            });
            server.start();
        }

        String baseUrl() {
            return "http://127.0.0.1:" + server.getAddress().getPort();
        }

        @Override public void close() { server.stop(0); }
    }

    private record Response(int status, String body) {}
}
