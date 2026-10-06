package io.tapstate.control.client;

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpServer;
import io.tapstate.core.common.JsonReader;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class HttpControlClientTest {

    @Test
    void constructorRejectsNonPositiveTimeouts() {
        assertThatThrownBy(() -> new HttpControlClient(Duration.ZERO, Duration.ofSeconds(1)))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("lightTimeout");
        assertThatThrownBy(() -> new HttpControlClient(Duration.ofSeconds(1), Duration.ZERO))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("heavyTimeout");
    }

    @Test
    void postSendsBearerAndJsonAndReturnsTheDecodedTree() throws Exception {
        AtomicReference<String> authorization = new AtomicReference<>();
        AtomicReference<Object> body = new AtomicReference<>();
        HttpServer server = server(exchange -> {
            authorization.set(exchange.getRequestHeaders().getFirst("Authorization"));
            body.set(JsonReader.parse(new String(exchange.getRequestBody().readAllBytes(), StandardCharsets.UTF_8)));
            answer(exchange, 201, "{\"tokenId\":\"tok-1\"}");
        });
        try (HttpControlClient client = new HttpControlClient(Duration.ofSeconds(1), Duration.ofSeconds(2))) {
            ControlResponse response = client.post(
                    baseOf(server), "secret", "/api/tokens", Map.of("scope", "write"), RequestBudget.LIGHT);

            assertThat(response).isInstanceOfSatisfying(ControlResponse.Success.class, success -> {
                assertThat(success.status()).isEqualTo(201);
                assertThat(success.body()).isEqualTo(Map.of("tokenId", "tok-1"));
            });
            assertThat(authorization.get()).isEqualTo("Bearer secret");
            assertThat(body.get()).isEqualTo(Map.of("scope", "write"));
        } finally {
            server.stop(0);
        }
    }

    /**
     * The removal verb rides the same transport, so it must carry the same bearer and, additionally, the
     * precondition as a quoted entity tag. The method itself is asserted: a delete sent as a GET would
     * reach a server that answers the read face and look like a success here.
     */
    @Test
    void deleteSendsTheMethodTheBearerAndTheQuotedPrecondition() throws Exception {
        AtomicReference<String> method = new AtomicReference<>();
        AtomicReference<String> authorization = new AtomicReference<>();
        AtomicReference<String> ifMatch = new AtomicReference<>();
        HttpServer server = server(exchange -> {
            method.set(exchange.getRequestMethod());
            authorization.set(exchange.getRequestHeaders().getFirst("Authorization"));
            ifMatch.set(exchange.getRequestHeaders().getFirst("If-Match"));
            answer(exchange, 204, "");
        });
        try (HttpControlClient client = new HttpControlClient(Duration.ofSeconds(1), Duration.ofSeconds(2))) {
            String hash = "a".repeat(64);
            ControlResponse response = client.delete(
                    baseOf(server), "secret", "/api/artifacts/tgt_my", hash);

            assertThat(response).isInstanceOfSatisfying(ControlResponse.Success.class, success -> {
                assertThat(success.status()).isEqualTo(204);
                assertThat(success.body()).isNull();
            });
            assertThat(method.get()).isEqualTo("DELETE");
            assertThat(authorization.get()).isEqualTo("Bearer secret");
            assertThat(ifMatch.get()).isEqualTo("\"" + hash + "\"");
        } finally {
            server.stop(0);
        }
    }

    @Test
    void deleteWithoutAPreconditionSendsNoIfMatchRatherThanAnEmptyOne() throws Exception {
        // An empty or literal-null If-Match is a malformed precondition, which the server refuses as a
        // different thing than none at all. Omitting the header is what "the caller supplied nothing" means.
        AtomicReference<String> ifMatch = new AtomicReference<>("untouched");
        HttpServer server = server(exchange -> {
            ifMatch.set(exchange.getRequestHeaders().getFirst("If-Match"));
            answer(exchange, 428, "{\"code\":\"artifact.precondition-required\"}");
        });
        try (HttpControlClient client = new HttpControlClient(Duration.ofSeconds(1), Duration.ofSeconds(2))) {
            ControlResponse response = client.delete(baseOf(server), "secret", "/api/artifacts/tgt_my", null);

            assertThat(ifMatch.get()).isNull();
            assertThat(response).isInstanceOfSatisfying(ControlResponse.Rejected.class, rejected ->
                    assertThat(rejected.code()).isEqualTo("artifact.precondition-required"));
        } finally {
            server.stop(0);
        }
    }

    @Test
    void codedHttpFailureRetainsStatusCodeMessageAndParams() throws Exception {
        HttpServer server = server(exchange -> answer(exchange, 403,
                "{\"code\":\"control.forbidden\",\"message\":\"Forbidden.\","
                        + "\"params\":{\"required\":\"admin\"}}"));
        try (HttpControlClient client = new HttpControlClient(Duration.ofSeconds(1), Duration.ofSeconds(2))) {
            ControlResponse response = client.get(baseOf(server), "read-token", "/api/tokens");

            assertThat(response).isEqualTo(new ControlResponse.Rejected(
                    403, "control.forbidden", "Forbidden.", Map.of("required", "admin")));
        } finally {
            server.stop(0);
        }
    }

    @Test
    void aRefusalKeepsWhatItCarriedBesidesItsCodeMessageAndParams() throws Exception {
        HttpServer server = server(exchange -> answer(exchange, 409,
                "{\"code\":\"lifecycle.start-needs-confirmation\",\"message\":\"Answer first.\","
                        + "\"params\":{\"pipeline\":\"orders\"},\"startChecks\":{\"outcome\":\"NEEDS_CONFIRMATION\"}}"));
        try (HttpControlClient client = new HttpControlClient(Duration.ofSeconds(1), Duration.ofSeconds(2))) {
            ControlResponse response = client.post(baseOf(server), "token", "/api/pipelines/orders:start", null,
                    RequestBudget.HEAVY);

            assertThat(response).isEqualTo(new ControlResponse.Rejected(409, "lifecycle.start-needs-confirmation",
                    "Answer first.", Map.of("pipeline", "orders"),
                    Map.of("startChecks", Map.of("outcome", "NEEDS_CONFIRMATION"))));
        } finally {
            server.stop(0);
        }
    }

    @Test
    void aPostCarriesThePreconditionAsAnEntityTagAndNoneWhenThereIsNone() throws Exception {
        List<String> preconditions = Collections.synchronizedList(new ArrayList<>());
        HttpServer server = server(exchange -> {
            preconditions.add(String.valueOf(exchange.getRequestHeaders().getFirst("If-Match")));
            answer(exchange, 200, "{}");
        });
        try (HttpControlClient client = new HttpControlClient(Duration.ofSeconds(1), Duration.ofSeconds(2))) {
            client.post(baseOf(server), "token", "/api/pipelines/orders:start", Map.of("decisions", List.of()),
                    RequestBudget.HEAVY, "c".repeat(64));
            client.post(baseOf(server), "token", "/api/pipelines/orders:start", null, RequestBudget.HEAVY);

            assertThat(preconditions).containsExactly("\"" + "c".repeat(64) + "\"", "null");
        } finally {
            server.stop(0);
        }
    }

    @Test
    void successfulEmptyBodyIsRepresentedAsNull() throws Exception {
        HttpServer server = server(exchange -> answer(exchange, 200, ""));
        try (HttpControlClient client = new HttpControlClient(Duration.ofSeconds(5), Duration.ofSeconds(5))) {
            assertThat(client.get(baseOf(server), "token", "/api/tokens"))
                    .isEqualTo(new ControlResponse.Success(200, null));
        } finally {
            server.stop(0);
        }
    }

    @Test
    void malformedJsonBodyIsReturnedAsUnreachable() throws Exception {
        HttpServer server = server(exchange -> answer(exchange, 200, "not-json"));
        try (HttpControlClient client = new HttpControlClient(Duration.ofSeconds(1), Duration.ofSeconds(2))) {
            assertThat(client.get(baseOf(server), "token", "/api/tokens"))
                    .isEqualTo(new ControlResponse.Unreachable());
        } finally {
            server.stop(0);
        }
    }

    @Test
    void malformedNonSuccessBodyRetainsTheHttpRejectionStatus() throws Exception {
        HttpServer server = server(exchange -> answer(exchange, 502, "upstream gateway error"));
        try (HttpControlClient client = new HttpControlClient(Duration.ofSeconds(1), Duration.ofSeconds(2))) {
            assertThat(client.get(baseOf(server), "token", "/api/tokens"))
                    .isEqualTo(new ControlResponse.Rejected(
                            502, "", "The server refused the request.", Map.of()));
        } finally {
            server.stop(0);
        }
    }

    @Test
    void rejectionWithoutAStableCodeUsesTheGenericControlContract() throws Exception {
        HttpServer server = server(exchange -> answer(exchange, 503, "{}"));
        try (HttpControlClient client = new HttpControlClient(Duration.ofSeconds(1), Duration.ofSeconds(2))) {
            ControlResponse response = client.get(baseOf(server), "token", "/api/tokens");

            assertThat(response).isEqualTo(new ControlResponse.Rejected(
                    503, "", "The server refused the request.", Map.of()));
        } finally {
            server.stop(0);
        }
    }

    @Test
    void connectionFailureIsAStableUnreachableOutcome() {
        try (HttpControlClient client = new HttpControlClient(Duration.ofMillis(250), Duration.ofMillis(250))) {
            assertThat(client.get(URI.create("http://127.0.0.1:0"), "token", "/api/tokens"))
                    .isInstanceOf(ControlResponse.Unreachable.class);
        }
    }

    @Test
    void requestBudgetBoundsAStalledExchange() throws Exception {
        CountDownLatch received = new CountDownLatch(1);
        CountDownLatch release = new CountDownLatch(1);
        HttpServer server = server(exchange -> {
            received.countDown();
            try {
                release.await();
                answer(exchange, 200, "{}");
            } catch (InterruptedException error) {
                Thread.currentThread().interrupt();
            }
        });
        try (HttpControlClient client = new HttpControlClient(
                Duration.ofMillis(250), Duration.ofMillis(250))) {
            CompletableFuture<ControlResponse> response = new CompletableFuture<>();
            Thread.ofVirtual().start(() -> response.complete(
                    client.get(baseOf(server), "token", "/api/tokens")));

            assertThat(received.await(5, TimeUnit.SECONDS)).isTrue();
            assertThat(response.get(5, TimeUnit.SECONDS))
                    .isInstanceOf(ControlResponse.Unreachable.class);
        } finally {
            release.countDown();
            server.stop(0);
        }
    }

    @Test
    void closeCancelsAnInFlightExchange() throws Exception {
        CountDownLatch received = new CountDownLatch(1);
        CountDownLatch release = new CountDownLatch(1);
        HttpServer server = server(exchange -> {
            received.countDown();
            try {
                release.await();
                answer(exchange, 200, "{}");
            } catch (InterruptedException error) {
                Thread.currentThread().interrupt();
            }
        });
        HttpControlClient client = new HttpControlClient(Duration.ofSeconds(1), Duration.ofSeconds(30));
        try {
            CompletableFuture<ControlResponse> response = new CompletableFuture<>();
            Thread.ofVirtual().start(() -> response.complete(client.post(
                    baseOf(server), "token", "/api/artifacts:apply", Map.of(), RequestBudget.HEAVY)));

            assertThat(received.await(5, TimeUnit.SECONDS)).isTrue();
            client.close();

            assertThat(response.get(5, TimeUnit.SECONDS))
                    .isInstanceOf(ControlResponse.Unreachable.class);
        } finally {
            release.countDown();
            client.close();
            server.stop(0);
        }
    }

    private static HttpServer server(Handler handler) throws IOException {
        HttpServer server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
        server.setExecutor(command -> Thread.ofVirtual().name("http-control-test").start(command));
        server.createContext("/", exchange -> handler.handle(exchange));
        server.start();
        return server;
    }

    private static URI baseOf(HttpServer server) {
        return URI.create("http://127.0.0.1:" + server.getAddress().getPort());
    }

    private static void answer(HttpExchange exchange, int status, String body) throws IOException {
        byte[] bytes = body.getBytes(StandardCharsets.UTF_8);
        exchange.getResponseHeaders().set("Content-Type", "application/json");
        exchange.sendResponseHeaders(status, bytes.length);
        exchange.getResponseBody().write(bytes);
        exchange.close();
    }

    @FunctionalInterface
    private interface Handler {
        void handle(HttpExchange exchange) throws IOException;
    }
}
