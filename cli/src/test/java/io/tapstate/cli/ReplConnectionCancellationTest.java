package io.tapstate.cli;

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpServer;
import org.jline.terminal.Terminal;
import org.jline.terminal.impl.DumbTerminal;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import picocli.CommandLine;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.PipedInputStream;
import java.io.PipedOutputStream;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.net.InetSocketAddress;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;

class ReplConnectionCancellationTest {

    @ParameterizedTest
    @ValueSource(strings = {"test", "discover-schema"})
    @Timeout(20)
    void terminalInterruptCancelsConnectionWorkAndKeepsTheSessionUsable(
            String verb, @TempDir Path workdir) throws Exception {
        String source = """
                version: tapstate/v1
                kind: source
                id: slow-db2
                connector: db2
                config:
                  host: db.internal
                  username: cdc
                """;
        String result = verb.equals("test") ? """
                {"connectionId":"slow-db2","connectorId":"db2","outcome":"PASSED",
                 "checks":[],"testedAt":1752000000000}
                """ : """
                {"connectionId":"slow-db2","connectorId":"db2",
                 "tables":[{"name":"orders","fields":[],"primaryKey":[],"indexes":[]}],
                 "discoveredAt":1752000000000}
                """;
        CountDownLatch started = new CountDownLatch(1);
        CountDownLatch release = new CountDownLatch(1);
        CountDownLatch finished = new CountDownLatch(1);
        AtomicInteger operations = new AtomicInteger();
        AtomicInteger healthProbes = new AtomicInteger();
        AtomicInteger versionReads = new AtomicInteger();
        HttpServer server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
        try (var executor = Executors.newVirtualThreadPerTaskExecutor();
             HttpControlPlaneClient client = new HttpControlPlaneClient();
             PipedInputStream input = new PipedInputStream();
             PipedOutputStream feed = new PipedOutputStream(input);
             Terminal terminal = new DumbTerminal(input, new ByteArrayOutputStream())) {
            server.setExecutor(executor);
            server.createContext("/api/artifacts/slow-db2", exchange -> reply(exchange,
                    JsonOut.write(Map.of("id", "slow-db2", "kind", "source", "canonicalForm", source))));
            server.createContext("/api/connections:" + verb, exchange -> {
                exchange.getRequestBody().readAllBytes();
                if (operations.incrementAndGet() == 1) {
                    started.countDown();
                    try {
                        release.await();
                    } catch (InterruptedException interrupted) {
                        Thread.currentThread().interrupt();
                        exchange.close();
                        return;
                    }
                }
                try {
                    reply(exchange, result);
                } catch (IOException cancelled) {
                    exchange.close();
                }
            });
            server.createContext("/healthz", exchange -> {
                healthProbes.incrementAndGet();
                reply(exchange, "{}");
            });
            server.createContext("/version", exchange -> {
                versionReads.incrementAndGet();
                reply(exchange, "{\"version\":\"terminal-cancellation-server\"}");
            });
            server.start();
            try {
                URI base = URI.create("http://127.0.0.1:" + server.getAddress().getPort());
                StringWriter output = new StringWriter();
                CommandLine commandLine = Cli.newCommandLine();
                commandLine.setOut(new PrintWriter(output));
                commandLine.setErr(new PrintWriter(output));
                Repl repl = new Repl(commandLine, workdir, client);
                repl.session().connect(List.of(base), base);
                repl.session().authenticate("probe-token", "probe-user", null, List.of(base));
                var interruptedAfterRun = executor.submit(() -> {
                    try {
                        repl.run(terminal);
                        return Thread.currentThread().isInterrupted();
                    } finally {
                        finished.countDown();
                    }
                });
                feed.write((verb + " slow-db2\n").getBytes(StandardCharsets.UTF_8));
                feed.flush();
                assertThat(started.await(5, TimeUnit.SECONDS)).as("the connection POST reached the server").isTrue();

                // Queue real interactive commands while the first response remains withheld.
                feed.write(("version\n" + verb + " slow-db2\nexit\n").getBytes(StandardCharsets.UTF_8));
                feed.flush();
                terminal.raise(Terminal.Signal.INT);

                assertThat(finished.await(5, TimeUnit.SECONDS))
                        .as("Ctrl-C must release the interactive loop before the server responds")
                        .isTrue();
                assertThat(interruptedAfterRun.get(1, TimeUnit.SECONDS)).isFalse();
                assertThat(release.getCount()).isEqualTo(1);
                assertThat(healthProbes.get()).as("cancellation must not trigger failover").isZero();
                assertThat(versionReads.get()).isEqualTo(1);
                assertThat(operations.get()).as("only the explicitly queued second command runs again").isEqualTo(2);
                assertThat(repl.session().isConnected()).isTrue();
                assertThat(repl.session().isAuthenticated()).isTrue();
                assertThat(repl.session().landingNode()).isEqualTo(base);
                assertThat(output.toString()).contains("request failed:", "terminal-cancellation-server",
                                verb.equals("test") ? "PASSED" : "orders")
                        .doesNotContain("cli.request-timed-out", "connection lost", "reconnected to");
            } finally {
                release.countDown();
                server.stop(0);
                executor.shutdownNow();
            }
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
