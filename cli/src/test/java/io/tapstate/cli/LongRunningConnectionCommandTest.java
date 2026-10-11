package io.tapstate.cli;

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpServer;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.io.TempDir;
import picocli.CommandLine;

import java.io.IOException;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.net.InetSocketAddress;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertAll;

class LongRunningConnectionCommandTest {

    @Test
    @Timeout(90)
    void successfulConnectionWorkBeyondThirtySecondsDoesNotFailTheCli(@TempDir Path workdir)
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
        String report = """
                {"connectionId":"slow-db2","connectorId":"db2","outcome":"PASSED",
                 "checks":[{"name":"ping","status":"PASSED","message":null,
                            "reason":null,"solution":null,"connectorErrorCode":null}],
                 "testedAt":1752000000000}
                """;
        String schema = """
                {"connectionId":"slow-db2","connectorId":"db2",
                 "tables":[{"name":"orders","fields":[{"name":"id","type":"BIGINT"}],
                            "primaryKey":["id"],"indexes":[]}],"discoveredAt":1752000000000}
                """;
        Map<String, String> stored = new ConcurrentHashMap<>();
        Map<String, AtomicInteger> calls = Map.of(
                "test", new AtomicInteger(), "discover-schema", new AtomicInteger());
        CountDownLatch started = new CountDownLatch(2);
        CountDownLatch completed = new CountDownLatch(2);
        AtomicReference<Throwable> serverFailure = new AtomicReference<>();
        HttpServer server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
        try (var executor = Executors.newVirtualThreadPerTaskExecutor()) {
            server.setExecutor(executor);
            server.createContext("/api/artifacts/slow-db2", exchange -> reply(exchange,
                    JsonOut.write(Map.of("id", "slow-db2", "kind", "source", "canonicalForm", source))));
            for (String verb : calls.keySet()) {
                String resultPath = "/api/connections/slow-db2/"
                        + (verb.equals("test") ? "test-result" : "schema");
                String body = verb.equals("test") ? report : schema;
                server.createContext(resultPath, exchange -> reply(exchange, stored.get(resultPath)));
                server.createContext("/api/connections:" + verb, exchange -> {
                    exchange.getRequestBody().readAllBytes();
                    calls.get(verb).incrementAndGet();
                    started.countDown();
                    try {
                        // Cross the real production budget without shortening or replacing the client.
                        Thread.sleep(Duration.ofSeconds(32));
                        stored.put(resultPath, body);
                        completed.countDown();
                    } catch (InterruptedException interrupted) {
                        serverFailure.set(interrupted);
                        Thread.currentThread().interrupt();
                        exchange.close();
                        return;
                    }
                    try {
                        reply(exchange, body);
                    } catch (IOException clientGone) {
                        // Work is already stored even when the timed-out client has closed its socket.
                        exchange.close();
                    }
                });
            }
            server.start();
            URI base = URI.create("http://127.0.0.1:" + server.getAddress().getPort());
            StringWriter testOutput = new StringWriter();
            StringWriter discoveryOutput = new StringWriter();
            try (HttpControlPlaneClient testClient = new HttpControlPlaneClient();
                 HttpControlPlaneClient discoveryClient = new HttpControlPlaneClient()) {
                Repl testRepl = authenticatedRepl(workdir, base, testClient, testOutput);
                Repl discoveryRepl = authenticatedRepl(workdir, base, discoveryClient, discoveryOutput);
                var testExit = executor.submit(() -> {
                    testRepl.dispatch(List.of("test", "slow-db2"), true);
                    return testRepl.lastExitCode();
                });
                var discoveryExit = executor.submit(() -> {
                    discoveryRepl.dispatch(List.of("discover-schema", "slow-db2"), true);
                    return discoveryRepl.lastExitCode();
                });
                assertThat(started.await(10, TimeUnit.SECONDS))
                        .as("both commands must reach their server operation before assessing the timeout")
                        .isTrue();
                int testStatus = testExit.get(60, TimeUnit.SECONDS);
                int discoveryStatus = discoveryExit.get(60, TimeUnit.SECONDS);
                assertThat(completed.await(10, TimeUnit.SECONDS))
                        .as("both server operations must finish and store their successful results")
                        .isTrue();
                assertThat(serverFailure.get()).isNull();
                assertThat(calls.get("test").get()).isEqualTo(1);
                assertThat(calls.get("discover-schema").get()).isEqualTo(1);
                assertThat(testClient.testResult(base, "probe-token", "slow-db2"))
                        .isInstanceOfSatisfying(ConnectionTestResultOutcome.Found.class,
                                found -> assertThat(found.report().outcome()).isEqualTo("PASSED"));
                assertThat(discoveryClient.schema(base, "probe-token", "slow-db2"))
                        .isInstanceOfSatisfying(ConnectionSchemaOutcome.Found.class,
                                found -> assertThat(found.schema().tables())
                                        .extracting(ConnectionSchema.Table::name).containsExactly("orders"));

                assertAll(
                        () -> assertThat(testStatus)
                                .as("test completed successfully on the server; CLI output:%n%s", testOutput)
                                .isEqualTo(Cli.EXIT_OK),
                        () -> assertThat(discoveryStatus)
                                .as("discovery stored its schema on the server; CLI output:%n%s", discoveryOutput)
                                .isEqualTo(Cli.EXIT_OK));
            } finally {
                server.stop(0);
                executor.shutdownNow();
            }
        }
    }

    private static Repl authenticatedRepl(Path workdir, URI base, HttpControlPlaneClient client,
                                          StringWriter output) {
        CommandLine commandLine = Cli.newCommandLine();
        commandLine.setOut(new PrintWriter(output));
        commandLine.setErr(new PrintWriter(output));
        Repl repl = new Repl(commandLine, workdir, client);
        repl.session().connect(List.of(base), base);
        repl.session().authenticate("probe-token", "probe-user", null, List.of(base));
        return repl;
    }

    private static void reply(HttpExchange exchange, String body) throws IOException {
        try (exchange) {
            byte[] bytes = body == null ? new byte[0] : body.getBytes(StandardCharsets.UTF_8);
            exchange.getResponseHeaders().set("Content-Type", "application/json");
            exchange.sendResponseHeaders(body == null ? 404 : 200, bytes.length == 0 ? -1 : bytes.length);
            if (bytes.length > 0) {
                exchange.getResponseBody().write(bytes);
            }
        }
    }
}
