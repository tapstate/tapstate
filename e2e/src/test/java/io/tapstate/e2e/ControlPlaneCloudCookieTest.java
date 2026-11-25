package io.tapstate.e2e;

import com.sun.net.httpserver.HttpServer;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.net.InetSocketAddress;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Checks the harness wire independently: a Cookie must never become an Authorization Bearer. */
class ControlPlaneCloudCookieTest {

    @Test
    void aCloudCookieSurvivesAClientReplacementAndLocalLoginRestoresBearerAuthentication() throws Exception {
        HttpServer server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
        AtomicInteger exchanges = new AtomicInteger();
        AtomicReference<String> cookie = new AtomicReference<>();
        AtomicReference<String> authorization = new AtomicReference<>();
        AtomicReference<String> origin = new AtomicReference<>();
        server.createContext("/", exchange -> {
            String path = exchange.getRequestURI().getPath();
            String body = "{}";
            if (path.equals("/auth/exchange")) {
                exchanges.incrementAndGet();
                exchange.getResponseHeaders().set("Set-Cookie", "tapstate_cloud_session=local-cookie; Secure; HttpOnly");
                exchange.sendResponseHeaders(302, -1);
                exchange.close();
                return;
            }
            if (path.equals("/auth/login")) body = "{\"token\":\"local-token\"}";
            else {
                cookie.set(exchange.getRequestHeaders().getFirst("Cookie"));
                authorization.set(exchange.getRequestHeaders().getFirst("Authorization"));
                origin.set(exchange.getRequestHeaders().getFirst("Origin"));
                if (exchange.getRequestMethod().equals("GET")) exchange.getResponseHeaders().set("ETag", "\"version\"");
                if (exchange.getRequestMethod().equals("DELETE")) {
                    exchange.sendResponseHeaders(204, -1);
                    exchange.close();
                    return;
                }
            }
            byte[] bytes = body.getBytes(StandardCharsets.UTF_8);
            exchange.sendResponseHeaders(200, bytes.length);
            try (var output = exchange.getResponseBody()) { output.write(bytes); }
        });
        server.start();
        try {
            URI address = URI.create("http://127.0.0.1:" + server.getAddress().getPort());
            ControlPlane first = new ControlPlane(address);
            String retained = first.exchangeCloudCode("one-time-code");
            first.apply(Map.of());
            assertThat(cookie.get()).isEqualTo(retained);
            assertThat(authorization.get()).isNull();
            assertThat(origin.get()).isEqualTo(address.toString());
            first.deleteSource("source-one");
            assertThat(cookie.get()).isEqualTo(retained);
            assertThat(authorization.get()).isNull();
            assertThat(origin.get()).isEqualTo(address.toString());

            ControlPlane restarted = new ControlPlane(address);
            restarted.useCloudSessionCookie(retained);
            restarted.apply(Map.of());
            assertThat(cookie.get()).isEqualTo(retained);
            assertThat(authorization.get()).isNull();
            assertThat(exchanges.get()).isEqualTo(1);

            restarted.login("e2e", "local-password");
            restarted.apply(Map.of());
            assertThat(authorization.get()).isEqualTo("Bearer local-token");
            assertThat(cookie.get()).isNull();
            assertThat(origin.get()).isNull();
        } finally { server.stop(0); }
    }

    @ParameterizedTest
    @ValueSource(strings = {"session=value", "session=value; Secure", "session=value; HttpOnly",
            "session=SecureHttpOnly; Path=/", "session=value; Securely; HttpOnly", "session=value; Secure; NotHttpOnly"})
    void anUnprotectedHandoffCookieCannotPassTheHarness(String cookie) throws Exception {
        HttpServer server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
        server.createContext("/auth/exchange", exchange -> {
            exchange.getResponseHeaders().set("Set-Cookie", cookie);
            exchange.sendResponseHeaders(302, -1);
            exchange.close();
        });
        server.start();
        try {
            ControlPlane control = new ControlPlane(URI.create("http://127.0.0.1:" + server.getAddress().getPort()));
            assertThatThrownBy(() -> control.exchangeCloudCode("one-code"))
                    .isInstanceOf(AssertionError.class).hasMessage("the handoff returned an unprotected session cookie");
        } finally { server.stop(0); }
    }
}
