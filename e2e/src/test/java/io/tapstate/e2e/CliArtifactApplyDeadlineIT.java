package io.tapstate.e2e;

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpServer;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * The shipped CLI accepts an apply response beyond its light deadline with one write.
 * The specification vocabulary cannot launch a CLI process or delay an HTTP response.
 */
class CliArtifactApplyDeadlineIT {

    @Test
    @Timeout(25)
    void aSlowApplyWithinTheDefaultHeavyBudgetSucceedsForTheScript(@TempDir Path home) throws Exception {
        Path draft = home.resolve("slow-source.tap.yml");
        Files.writeString(draft, "version: tapstate/v1\nkind: source\nid: slow-source\nconnector: dummy\n");
        AtomicInteger writes = new AtomicInteger();
        HttpServer server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
        var executor = Executors.newSingleThreadScheduledExecutor();
        server.setExecutor(executor);
        server.createContext("/healthz", exchange -> reply(exchange, "{}"));
        server.createContext("/version", exchange -> reply(exchange, "{\"version\":null}"));
        server.createContext("/.well-known/tapstate", exchange -> reply(exchange, """
                {"issuer":"urn:tapstate:cluster:slow-apply","clusterId":"slow-apply",
                 "apiVersion":"tapstate/v1","authModes":["password","machine_token"]}
                """));
        server.createContext("/api/cluster/members", exchange -> reply(exchange, """
                {"clusterId":"slow-apply","topologyRevision":1,"members":[],"pipelines":[]}
                """));
        server.createContext("/api/artifacts:apply", exchange -> {
            exchange.getRequestBody().readAllBytes();
            writes.incrementAndGet();
            // Finish the simulated apply after the 3-second light deadline and within the 30-second heavy deadline.
            executor.schedule(() -> {
                try {
                    reply(exchange, """
                            {"outcomes":[{"id":"slow-source","kind":"source","change":"CREATED","contentHash":"h1"}]}
                            """);
                } catch (IOException clientGone) {
                    // A client with the light deadline can close the socket before the response is ready.
                    exchange.close();
                }
            }, 4, TimeUnit.SECONDS);
        });
        server.start();
        try {
            CliOnce.Run run = CliOnce.runWithPassword(null, List.of("-Duser.home=" + home),
                    "-c", "http://127.0.0.1:" + server.getAddress().getPort(),
                    "--token", "e2e-token", "apply", draft.toString());

            assertThat(run.exitCode()).as("stdout:%n%s%nstderr:%n%s", run.stdout(), run.stderr()).isZero();
            assertThat(run.stdout()).contains("created", "source", "slow-source");
            assertThat(writes.get()).as("the apply write must be sent exactly once").isEqualTo(1);
        } finally {
            server.stop(0);
            executor.shutdownNow();
            executor.close();
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
