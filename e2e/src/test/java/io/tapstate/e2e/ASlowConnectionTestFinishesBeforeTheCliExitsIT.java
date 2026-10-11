package io.tapstate.e2e;

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpServer;
import io.tapstate.core.common.JsonWriter;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * A script waits for a slow connector and reads the actual CLI process status.
 * The specification vocabulary has no word for launching a CLI or delaying an HTTP response.
 * The server fixture isolates the client deadline from database and connector setup costs.
 */
class ASlowConnectionTestFinishesBeforeTheCliExitsIT {

    @Test
    @Timeout(90)
    void aSuccessfulConnectionTestAfterThirtySecondsStillSucceedsForTheScript(@TempDir Path home)
            throws Exception {
        String source = """
                version: tapstate/v1
                kind: source
                id: slow-db2
                connector: db2
                config:
                  host: db.internal
                  username: cdc
                """;
        CountDownLatch completed = new CountDownLatch(1);
        AtomicInteger tests = new AtomicInteger();
        AtomicLong completionNanos = new AtomicLong();
        HttpServer server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
        var connectorCompletion = Executors.newSingleThreadScheduledExecutor();
        server.setExecutor(command -> Thread.ofVirtual().start(command));
        server.createContext("/healthz", exchange -> reply(exchange, "{}"));
        server.createContext("/version", exchange -> reply(exchange, "{\"version\":null}"));
        server.createContext("/.well-known/tapstate", exchange -> reply(exchange, """
                {"issuer":"urn:tapstate:cluster:slow-test","clusterId":"slow-test",
                 "apiVersion":"tapstate/v1","authModes":["password","machine_token"]}
                """));
        server.createContext("/api/cluster/members", exchange -> reply(exchange, """
                {"clusterId":"slow-test","topologyRevision":1,"members":[],"pipelines":[]}
                """));
        server.createContext("/api/artifacts/slow-db2", exchange -> reply(exchange,
                JsonWriter.write(Map.of("id", "slow-db2", "kind", "source", "canonicalForm", source))));
        server.createContext("/api/connections:test", exchange -> {
            exchange.getRequestBody().readAllBytes();
            tests.incrementAndGet();
            long started = System.nanoTime();
            // Schedule an observable connector completion beyond the former client deadline.
            connectorCompletion.schedule(() -> {
                completionNanos.set(System.nanoTime() - started);
                completed.countDown();
                try {
                    reply(exchange, """
                            {"connectionId":"slow-db2","connectorId":"db2","outcome":"PASSED",
                             "checks":[],"testedAt":1752000000000}
                            """);
                } catch (IOException clientGone) {
                    // A reverted client deadline can close the socket before the connector finishes.
                    exchange.close();
                }
            }, 32, TimeUnit.SECONDS);
        });
        server.start();
        try {
            CliOnce.Run run = CliOnce.runWithPassword(null, List.of("-Duser.home=" + home),
                    "-c", "http://127.0.0.1:" + server.getAddress().getPort(),
                    "--token", "e2e-token", "test", "slow-db2");
            assertThat(completed.await(10, TimeUnit.SECONDS))
                    .as("the connector must have finished before assessing the CLI status; stdout:%n%s%nstderr:%n%s",
                            run.stdout(), run.stderr())
                    .isTrue();
            assertThat(completionNanos.get()).as("the connector must finish after the old client deadline")
                    .isGreaterThan(TimeUnit.SECONDS.toNanos(30));
            assertThat(tests.get()).as("the CLI must issue the operation exactly once").isEqualTo(1);
            assertThat(run.exitCode()).as("stdout:%n%s%nstderr:%n%s", run.stdout(), run.stderr()).isZero();
            assertThat(run.stdout()).contains("PASSED").contains("slow-db2");
            assertThat(run.stderr()).doesNotContain("cli.request-timed-out");
        } finally {
            connectorCompletion.shutdownNow();
            server.stop(0);
        }
    }

    private static void reply(HttpExchange exchange, String body) throws IOException {
        try (exchange) {
            byte[] bytes = body.getBytes(StandardCharsets.UTF_8);
            exchange.getResponseHeaders().set("Content-Type", "application/json");
            exchange.sendResponseHeaders(200, bytes.length);
            exchange.getResponseBody().write(bytes);
        }
    }
}
