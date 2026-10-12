package io.tapstate.e2e;

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpServer;
import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * The bearer token a login returns expires after fifteen minutes, and a long benchmark fork outlives it.
 * The harness logs in again before handing out a credential old enough to be refused.
 */
class ControlPlaneCredentialTest {

    @Test
    void aCredentialOldEnoughToExpireIsRenewedByLoggingInAgain() throws Exception {
        var logins = new AtomicInteger();
        HttpServer server = HttpServer.create(new InetSocketAddress(InetAddress.getLoopbackAddress(), 0), 0);
        server.createContext("/auth/login", exchange -> respond(exchange, "{\"token\":\"token-" + logins.incrementAndGet() + "\"}"));
        server.start();
        try {
            var clock = new AtomicLong();
            var control = new ControlPlane(URI.create("http://127.0.0.1:" + server.getAddress().getPort()), clock::get);
            control.login("admin", "secret");
            assertThat(control.credential()).isEqualTo("token-1");

            clock.set(TimeUnit.MINUTES.toNanos(9));
            assertThat(control.credential()).isEqualTo("token-1");
            assertThat(logins).hasValue(1);

            clock.set(TimeUnit.MINUTES.toNanos(10));
            assertThat(control.credential()).isEqualTo("token-2");
            assertThat(logins).hasValue(2);

            clock.set(TimeUnit.MINUTES.toNanos(19));
            assertThat(control.credential()).isEqualTo("token-2");
            assertThat(logins).hasValue(2);
        } finally {
            server.stop(0);
        }
    }

    private static void respond(HttpExchange exchange, String body) throws IOException {
        byte[] bytes = body.getBytes(StandardCharsets.UTF_8);
        exchange.getResponseHeaders().add("Content-Type", "application/json");
        exchange.sendResponseHeaders(200, bytes.length);
        try (var output = exchange.getResponseBody()) { output.write(bytes); }
    }
}
