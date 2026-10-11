package io.tapstate.cli;

import com.sun.net.httpserver.HttpServer;
import io.tapstate.core.common.JsonWriter;
import org.junit.jupiter.api.Test;

import java.net.InetSocketAddress;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;

import static org.assertj.core.api.Assertions.assertThat;

class ConnectionSettingsClientTest {

    @Test
    void readsTheExistingProtectedSourceEndpointWithoutUsingItsGenericExport() throws Exception {
        AtomicReference<String> authorization = new AtomicReference<>();
        AtomicReference<String> path = new AtomicReference<>();
        try (Peer peer = peer(200, JsonWriter.write(Map.of("id", "orders / one", "connector", "mysql",
                "config", Map.of("host", "db.example", "port", 3306, "nested", Map.of("extra", true)))),
                authorization, path);
                HttpControlPlaneClient client = new HttpControlPlaneClient()) {
            ConnectionSettingsOutcome outcome = client.connectionSettings(peer.base(), "protected-token", "orders / one");
            assertThat(outcome).isInstanceOfSatisfying(ConnectionSettingsOutcome.Found.class, found -> {
                assertThat(found.connector()).isEqualTo("mysql");
                assertThat(found.settings()).containsEntry("host", "db.example").containsEntry("port", 3306L)
                        .containsEntry("nested", Map.of("extra", true));
                assertThat(found.toString()).doesNotContain("db.example", "3306", "extra");
            });
            assertThat(path.get()).isEqualTo("/api/sources/orders%20%2F%20one");
            assertThat(authorization.get()).isEqualTo("Bearer protected-token");
        }
    }

    @Test
    void malformedSuccessfulSourceReadsDoNotBecomeUsableProbeSettings() throws Exception {
        for (String body : List.of("{broken", "{}", "[]",
                "{\"id\":\"other\",\"connector\":\"mysql\",\"config\":{}}",
                "{\"id\":\"orders\",\"connector\":\" \" ,\"config\":{}}",
                "{\"id\":\"orders\",\"connector\":\"mysql\",\"config\":[]}",
                "{\"id\":\"orders\",\"connector\":\"mysql\"}")) {
            try (Peer peer = peer(200, body, new AtomicReference<>(), new AtomicReference<>());
                    HttpControlPlaneClient client = new HttpControlPlaneClient()) {
                assertThat(client.connectionSettings(peer.base(), "token", "orders"))
                        .isInstanceOf(ConnectionSettingsOutcome.Unreachable.class);
            }
        }
    }

    @Test
    void missingAndRefusedSourcesRemainDistinctFromUnreachable() throws Exception {
        for (int status : List.of(404, 401, 500)) {
            try (Peer peer = peer(status,
                    "{\"code\":\"control.unauthenticated\",\"message\":\"Authentication required\"}",
                    new AtomicReference<>(), new AtomicReference<>());
                    HttpControlPlaneClient client = new HttpControlPlaneClient()) {
                ConnectionSettingsOutcome outcome = client.connectionSettings(peer.base(), "token", "orders");
                if (status == 404) assertThat(outcome).isInstanceOf(ConnectionSettingsOutcome.Absent.class);
                else assertThat(outcome).isEqualTo(new ConnectionSettingsOutcome.Rejected(
                        "control.unauthenticated", "Authentication required"));
            }
        }
        try (HttpControlPlaneClient client = new HttpControlPlaneClient()) {
            assertThat(client.connectionSettings(URI.create("http://127.0.0.1:0"), "token", "orders"))
                    .isInstanceOf(ConnectionSettingsOutcome.Unreachable.class);
        }
    }

    private static Peer peer(int status, String body, AtomicReference<String> auth,
            AtomicReference<String> path) throws Exception {
        HttpServer server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
        server.createContext("/", exchange -> {
            auth.set(exchange.getRequestHeaders().getFirst("Authorization"));
            path.set(exchange.getRequestURI().getRawPath());
            byte[] bytes = body.getBytes(StandardCharsets.UTF_8);
            exchange.sendResponseHeaders(status, bytes.length);
            try (var output = exchange.getResponseBody()) { output.write(bytes); }
            exchange.close();
        });
        server.start();
        return new Peer(server);
    }

    private record Peer(HttpServer server) implements AutoCloseable {
        URI base() { return URI.create("http://127.0.0.1:" + server.getAddress().getPort()); }
        @Override public void close() { server.stop(0); }
    }
}
