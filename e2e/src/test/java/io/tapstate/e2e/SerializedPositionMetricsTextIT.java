package io.tapstate.e2e;

import com.sun.net.httpserver.HttpServer;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.List;
import java.util.concurrent.atomic.AtomicReference;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * The launched CLI renders a serialized connector position received over its HTTP metrics face.
 * The declarative vocabulary reads pipeline metrics, but has no word for the CLI's text output.
 * The peer supplies the observed connector token because the connector jar is not in this build;
 * the CLI process, its HTTP client, and its rendering are the product path under test.
 */
class SerializedPositionMetricsTextIT {

    @Test
    void metricsShowsOpaquePositionMovementWithoutPrintingTheirBytes(@TempDir Path directory) throws Exception {
        // An observed Postgres CDC position: Java serialization of PostgresOffset with null fields.
        String serializedPosition = "rO0ABXNyADdpby50YXBkYXRhLmNvbm5lY3Rvci5wb3N0Z3Jlcy5jZGMub2Zmc2V0"
                + "LlBvc3RncmVzT2Zmc2V0KScGvwPaw5wCAANMAAtvZmZzZXRWYWx1ZXQAEExqYXZhL2xhbmcvTG9uZztM"
                + "AApzb3J0U3RyaW5ndAASTGphdmEvbGFuZy9TdHJpbmc7TAAMc291cmNlT2Zmc2V0cQB+AAJ4cHBwcA==";
        String nextPosition = "rO0ABXQAA3R3bw==";
        String response = """
                {"pipelineId":"pl1","metrics":{"recordCount":6},
                 "targetAckedPosition":{"orders":"%s","accounts":"%s","notes":"w7"},
                 "positionsNotCollected":["sourceHeadPosition"]}
                """;
        AtomicReference<String> metrics = new AtomicReference<>(
                response.formatted(serializedPosition, serializedPosition));
        AtomicReference<String> authorization = new AtomicReference<>();
        HttpServer server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
        server.createContext("/", exchange -> {
            String path = exchange.getRequestURI().getPath();
            String body;
            int status = 200;
            switch (path) {
                case "/healthz" -> body = "ok";
                case "/version" -> body = "{\"version\":\"0.5.0\"}";
                case "/.well-known/tapstate" -> body = """
                        {"issuer":"urn:tapstate:cluster:fixture","clusterId":"fixture",
                         "apiVersion":"tapstate/v1","authModes":["password","machine_token"]}
                        """;
                case "/api/pipelines/pl1/metrics" -> {
                    authorization.set(exchange.getRequestHeaders().getFirst("Authorization"));
                    body = metrics.get();
                }
                default -> {
                    status = 404;
                    body = "unexpected path: " + path;
                }
            }
            respond(exchange, status, body);
        });
        server.start();
        try {
            String seed = "http://127.0.0.1:" + server.getAddress().getPort();
            CliOnce.Run first = CliOnce.runWithPassword(null,
                    List.of("-Duser.home=" + directory),
                    "-c", seed, "--token", "fixture-token", "metrics", "pl1");

            assertThat(first.exitCode())
                    .as("stdout was:%n%s%nstderr was:%n%s", first.stdout(), first.stderr()).isZero();
            assertThat(authorization.get()).isEqualTo("Bearer fixture-token");
            assertThat(first.stdout())
                    .contains("recordCount  6", "sourceHeadPosition  not collected")
                    .contains("targetAckedPosition.notes  w7")
                    .contains("targetAckedPosition  recorded on 2 tables; 1 distinct opaque position"
                            + " (source coordinate unavailable)")
                    .containsPattern("targetAckedPosition.orders  opaque position fingerprint [0-9a-f]{16}")
                    .containsPattern("targetAckedPosition.accounts  opaque position fingerprint [0-9a-f]{16}")
                    .doesNotContain(serializedPosition);
            String firstFingerprint = printedFingerprint(first.stdout(), "orders");

            metrics.set(response.formatted(nextPosition, nextPosition));
            CliOnce.Run next = CliOnce.runWithPassword(null,
                    List.of("-Duser.home=" + directory),
                    "-c", seed, "--token", "fixture-token", "metrics", "pl1");
            assertThat(next.exitCode())
                    .as("stdout was:%n%s%nstderr was:%n%s", next.stdout(), next.stderr()).isZero();
            assertThat(next.stdout())
                    .contains("targetAckedPosition  recorded on 2 tables; 1 distinct opaque position"
                            + " (source coordinate unavailable)")
                    .containsPattern("targetAckedPosition.orders  opaque position fingerprint [0-9a-f]{16}")
                    .doesNotContain(serializedPosition, nextPosition);
            assertThat(printedFingerprint(next.stdout(), "orders")).isNotEqualTo(firstFingerprint);
        } finally {
            server.stop(0);
        }
    }

    private static String printedFingerprint(String output, String table) {
        Matcher line = Pattern.compile("(?m)^targetAckedPosition\\." + table
                + "  opaque position fingerprint ([0-9a-f]{16}) \\(source coordinate unavailable\\)$")
                .matcher(output);
        assertThat(line.find()).as("the table has an opaque position fingerprint").isTrue();
        return line.group(1);
    }

    private static void respond(com.sun.net.httpserver.HttpExchange exchange, int status, String body)
            throws IOException {
        byte[] bytes = body.getBytes(StandardCharsets.UTF_8);
        exchange.getResponseHeaders().add("Content-Type", "application/json");
        exchange.sendResponseHeaders(status, bytes.length);
        try (OutputStream output = exchange.getResponseBody()) {
            output.write(bytes);
        }
        exchange.close();
    }
}
