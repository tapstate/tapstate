package io.tapstate.e2e;

import com.sun.net.httpserver.HttpServer;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.time.Duration;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * A CLI newer than the server it is pointed at, saying so instead of dying.
 *
 * <p>The CLI puts every seed through anonymous issuer discovery before a credential can leave the
 * process. A server that predates that endpoint has nothing serving it, so its authentication filter
 * refuses the path like any other unknown one -- and that refusal used to reach an unchecked cast and
 * end the process with a Java stack trace and status 1. What is claimed here is what a reader gets
 * instead: the code, the seed, what the server itself said, and what to do about it.
 *
 * <p>The peer is a stub rather than a launched product, and that is forced rather than convenient:
 * the situation needs a server OLDER than this build, and every other case here launches the server
 * this build just produced, which answers discovery correctly. There is nothing to point at. The stub
 * is not invented either -- the published 0.3.0 image answers {@code /healthz} 200, {@code /version}
 * 401, and {@code /.well-known/tapstate} 401 with {@code control.unauthenticated}, which is exactly
 * what is served below. No container is needed to be that, so none is started.
 *
 * <p>Nothing in the declarative vocabulary reaches this. Its words -- {@code count}, {@code state},
 * {@code error_count} -- all describe what a pipeline did with rows, and this claim is about a
 * handshake that fails before any pipeline exists. There is no word missing; a specification about
 * which build the other end is would need a second subject entirely. Java, as the admission rule
 * provides for.
 *
 * <p>The CLI runs as its own process, which is the claim rather than an inconvenience: the complaint
 * was a process that died, and a method call has no exit status and no stderr to read.
 */
@DisplayName("a CLI newer than its server reports the refusal instead of crashing")
class AnOlderServerRefusingDiscoveryIsReportedIT {

    private static final String REFUSAL =
            "{\"code\":\"control.unauthenticated\",\"params\":{},"
                    + "\"message\":\"This operation requires authentication.\"}";

    @Test
    void aRefusedIssuerDiscoveryArrivesAsACodedDiagnostic() throws Exception {
        HttpServer server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
        server.createContext("/", exchange -> {
            boolean health = "/healthz".equals(exchange.getRequestURI().getPath());
            byte[] body = REFUSAL.getBytes(StandardCharsets.UTF_8);
            exchange.getResponseHeaders().add("Content-Type", "application/json");
            exchange.sendResponseHeaders(health ? 200 : 401, health ? -1 : body.length);
            if (!health) {
                try (OutputStream out = exchange.getResponseBody()) {
                    out.write(body);
                }
            }
            exchange.close();
        });
        server.start();
        try {
            URI seed = URI.create("http://127.0.0.1:" + server.getAddress().getPort());
            awaitReady(seed);

            CliOnce.Run run = CliOnce.runWithPassword(
                    "any-password", "-c", seed.toString(), "-u", "admin", "connectors");

            assertThat(run.stderr())
                    .as("the refusal is reported as a coded diagnostic carrying what the server said")
                    .contains("cli.issuer-discovery-rejected")
                    .contains(seed.toString())
                    .contains("control.unauthenticated")
                    // The whole of the original complaint: a stack trace where a sentence belonged.
                    .doesNotContain("ClassCastException")
                    .doesNotContain("Exception in thread");
            assertThat(run.exitCode())
                    .as("a refused seed is a diagnosed failure, not a success")
                    .isNotZero();
        } finally {
            server.stop(0);
        }
    }

    private static void awaitReady(URI seed) throws Exception {
        HttpResponse<Void> response = HttpClient.newHttpClient().send(
                HttpRequest.newBuilder(seed.resolve("/healthz"))
                        .timeout(Duration.ofSeconds(20)).GET().build(),
                HttpResponse.BodyHandlers.discarding());
        assertThat(response.statusCode()).as("stub server readiness").isEqualTo(200);
    }
}
