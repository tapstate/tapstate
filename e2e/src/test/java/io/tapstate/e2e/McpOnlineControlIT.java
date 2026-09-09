package io.tapstate.e2e;

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpServer;
import io.tapstate.core.common.JsonReader;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.io.UncheckedIOException;
import java.io.Writer;
import java.net.InetSocketAddress;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.net.http.HttpTimeoutException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * The MCP stdio process drafting a Source through the remote control contract.
 *
 * <p>The declarative end-to-end vocabulary describes pipeline state and target rows. It has no word
 * for a JSON-RPC stdio process, tool discovery, or the HTTP request a tool sends, so this case drives
 * the shipped MCP entry point as a separate process. A fake HTTP peer keeps the case deterministic
 * while preserving the boundary under test: the MCP process can reach it only over the public wire.
 */
class McpOnlineControlIT {

    private static final String MCP_BOOT_JAR_PROPERTY = "tapstate.e2e.mcp-boot-jar";
    private static final String MACHINE_TOKEN = "mcp-e2e-machine-token";
    private static final String SENTINEL_SECRET = "mcp-e2e-sentinel-secret";
    private static final Duration READINESS_TIMEOUT = Duration.ofSeconds(5);

    /** A collection a view declares as where it materializes, and one nobody has ever mentioned. */
    private static final String DECLARED = "order_state";
    private static final String BY_HAND = "notes";

    /** What the declaring view says about it, so the agent gets a sentence rather than a name. */
    private static final String DESCRIBED_AS = "One row per order, as the pipeline leaves it.";

    @TempDir
    private Path temporaryDirectory;

    @Test
    void readinessProbeTimesOutWhenPeerDoesNotRespond() throws Exception {
        CountDownLatch requestReceived = new CountDownLatch(1);
        CountDownLatch release = new CountDownLatch(1);
        HttpServer server = server(exchange -> {
            requestReceived.countDown();
            try {
                release.await();
            } catch (InterruptedException error) {
                Thread.currentThread().interrupt();
            } finally {
                exchange.close();
            }
        });
        try {
            assertThatThrownBy(() -> awaitServer(server, Duration.ofSeconds(1)))
                    .isInstanceOf(HttpTimeoutException.class);
            assertThat(requestReceived.await(1, TimeUnit.SECONDS)).isTrue();
        } finally {
            release.countDown();
            server.stop(0);
        }
    }

    /**
     * The read face as an agent meets it: three global tools, answers that say what is known and leave
     * out what is not, and a listing that follows the registry inside one session.
     *
     * <p>The session is the point. Three tools that exist for the whole surface can be listed once and
     * used for anything; a tool per view would have to be listed again whenever the workspace changed,
     * and a client that had already listed them would go on not seeing a source applied a minute ago.
     * So a source is applied and deleted while this session stays open, and the answers have to follow.
     *
     * <p>Absence is asserted as absence, which is the other half. A field list nobody produced and a
     * description nobody wrote are left out rather than sent empty, because an agent reading
     * {@code fields: []} stops and reports a collection with no fields - a different statement from
     * "nobody looked", and one no person is on this face to correct.
     *
     * <p>Against the product rather than a fake peer, unlike its neighbours here: what is under test is
     * what the answers contain, and a fake peer would be this harness writing the answers it then
     * checks. The credential is a minted machine token for the same reason - it is what a peer is
     * actually given.
     *
     * <p>Deleting the source is this branch's verb for taking something out of the registry; the plan
     * names {@code artifact.delete}, which belongs to another line and is not here yet. The property
     * being watched - a derived answer following the registry rather than a snapshot - is the same
     * either way.
     */
    @Test
    void offersThreeReadToolsWhoseAnswersFollowTheRegistryWithoutRestartingTheSession() throws Exception {
        Path connectorJars = temporaryDirectory.resolve("connectors");
        Path data = temporaryDirectory.resolve("data");
        Path later = temporaryDirectory.resolve("data-later");
        Files.createDirectories(connectorJars);
        Files.createDirectories(data);
        Files.createDirectories(later);
        Files.writeString(data.resolve(DECLARED + ".csv"), "id,total\n1,10\n");
        Files.writeString(data.resolve(BY_HAND + ".csv"), "id,note\n1,nobody declared me\n");
        Files.writeString(later.resolve("arrivals.csv"), "id,note\n1,applied mid-session\n");

        // Packaged under the browsable id, because rows are served only to connectors the product
        // knows speak the request shape it asks in. Listing and sizing are answered for any connector;
        // reading rows is not, so a fixture filed under the plain test id can be listed and never read
        // - and a listing that is never read from is half the chain this specification is about.
        E2eConnectorJar.buildInto(connectorJars, E2eConnectorJar.BROWSABLE_CONNECTOR_ID);
        String previousConnectorsDir =
                System.setProperty("tapstate.e2e.connectors-dir", connectorJars.toString());
        Path stderr = temporaryDirectory.resolve("mcp-online.stderr");
        try (ServerHandle server = InProcessServer.start(SharedMongo.replicaSetUrl("e2e_mcp_browse"))) {
            ControlPlane control = new ControlPlane(server.baseUrl());
            control.bootstrapAndLogin("e2e", "e2e-password");
            control.registerConnector(E2eConnectorJar.BROWSABLE_CONNECTOR_ID,
                    ConnectorJars.bytesFor(E2eConnectorJar.BROWSABLE_CONNECTOR_ID));
            control.apply(Map.of(
                    "src.tap.yml", sourceYaml("src_browse", data),
                    "v_declared.tap.yml", declaringViewYaml(),
                    "pipeline.tap.yml", pipelineYaml()));

            Process process = startMcp(server.baseUrl(), control.mintToken("read"), stderr);
            try (Writer input = new OutputStreamWriter(process.getOutputStream(), StandardCharsets.UTF_8);
                    BufferedReader output = process.inputReader(StandardCharsets.UTF_8)) {
                send(input, """
                        {"jsonrpc":"2.0","id":1,"method":"initialize","params":{
                          "protocolVersion":"2025-06-18","capabilities":{},
                          "clientInfo":{"name":"tapstate-e2e","version":"1"}}}
                        """);
                receive(output);
                send(input, """
                        {"jsonrpc":"2.0","method":"notifications/initialized"}
                        """);

                // Exactly three, so a per-view surface fails here rather than at the next assertion:
                // that shape would offer one tool per declared collection, and there are two.
                send(input, """
                        {"jsonrpc":"2.0","id":2,"method":"tools/list","params":{}}
                        """);
                List<?> tools = (List<?>) ((Map<?, ?>) receive(output).get("result")).get("tools");
                assertThat(tools.stream()
                        .map(tool -> String.valueOf(((Map<?, ?>) tool).get("name")))
                        .filter(name -> name.startsWith("data_browser_"))
                        .sorted()
                        .toList())
                        .as("the read tools this surface offers an agent")
                        .containsExactly(
                                "data_browser_collections", "data_browser_find", "data_browser_stats");

                send(input, """
                        {"jsonrpc":"2.0","id":3,"method":"tools/call","params":{
                          "name":"source_list","arguments":{"limit":1,"offset":0}}}
                        """);
                Map<?, ?> sourceList = (Map<?, ?>) receive(output).get("result");
                assertThat(sourceList.get("isError")).as("listing Sources through MCP").isEqualTo(false);
                List<?> sources = (List<?>) ((Map<?, ?>) sourceList.get("structuredContent")).get("items");
                assertThat(sources.stream()
                        .map(source -> String.valueOf(((Map<?, ?>) source).get("id"))))
                        .as("the Source list returned by the existing server list API")
                        .containsExactly("src_browse");
                assertThat(((Map<?, ?>) sources.getFirst()).keySet().stream().map(String::valueOf).toList())
                        .containsExactlyInAnyOrder("id", "metadata", "connector");

                send(input, """
                        {"jsonrpc":"2.0","id":4,"method":"tools/call","params":{
                          "name":"pipeline_list","arguments":{"limit":1,"offset":0}}}
                        """);
                Map<?, ?> pipelineList = (Map<?, ?>) receive(output).get("result");
                assertThat(pipelineList.get("isError")).as("listing Pipelines through MCP").isEqualTo(false);
                List<?> pipelines = (List<?>) ((Map<?, ?>) pipelineList.get("structuredContent")).get("items");
                assertThat(pipelines.stream()
                        .map(pipeline -> String.valueOf(((Map<?, ?>) pipeline).get("id"))))
                        .as("the Pipeline list returned by the existing server list API")
                        .containsExactly("pipeline_browse");

                Map<String, Map<String, Object>> listed = collectionsByName(input, output, 5, "src_browse");
                assertThat(listed).containsKeys(DECLARED, BY_HAND);

                // What a workspace said about a collection reaches the agent...
                assertThat(listed.get(DECLARED))
                        .as("the entry for the collection a view declares")
                        .containsEntry("kind", "view")
                        .containsEntry("description", DESCRIBED_AS);
                // ...and where nobody said anything, the keys are not there at all. Sent empty they
                // would be answers - "no kind", "no description" - which is not what silence means.
                assertThat(listed.get(BY_HAND))
                        .as("the entry for the collection nobody declared")
                        .doesNotContainKey("kind")
                        .doesNotContainKey("description");
                // Nothing has been discovered on this connection, so no collection has a field list -
                // including the declared one, whose declaration says nothing about fields.
                assertThat(listed.get(DECLARED)).doesNotContainKey("fields");
                assertThat(listed.get(BY_HAND)).doesNotContainKey("fields");

                // And the collection the listing named is the one a read of it answers from. This is the
                // chain an agent actually walks - list, then read one of what was listed - and it is the
                // half that "three tools are offered" never covered: `find` was in the catalogue and had
                // never been called, so nothing here would have noticed it answering from somewhere else,
                // or not answering at all. The two seeded collections carry different columns on purpose:
                // a read wired to the other one comes back with `note` and fails on the last assertion
                // rather than passing on a row that merely looks plausible.
                send(input, findCall(6, "src_browse", DECLARED));
                Map<?, ?> found = (Map<?, ?>) receive(output).get("result");
                assertThat(found.get("isError"))
                        .as("reading the collection the listing named, in the same session")
                        .isEqualTo(false);
                List<?> rows = (List<?>) ((Map<?, ?>) found.get("structuredContent")).get("rows");
                assertThat(rows).as("the rows the declared collection holds").hasSize(1);
                Map<?, ?> row = (Map<?, ?>) rows.get(0);
                assertThat(String.valueOf(row.get("total")))
                        .as("the value held by the row of the collection that was listed")
                        .isEqualTo("10");
                assertThat(row.keySet().stream().map(String::valueOf).toList())
                        .as("and not the row of the collection nobody declared, whose column is another")
                        .doesNotContain("note");

                // Applied after the tools were listed, and read without listing them again.
                control.apply(Map.of("src_later.tap.yml", sourceYaml("src_later", later)));
                send(input, """
                        {"jsonrpc":"2.0","id":7,"method":"tools/call","params":{
                          "name":"source_list","arguments":{"limit":1,"offset":1}}}
                        """);
                Map<?, ?> laterSourceList = (Map<?, ?>) receive(output).get("result");
                assertThat(laterSourceList.get("isError")).as("reading the second Source page").isEqualTo(false);
                List<?> laterSources = (List<?>) ((Map<?, ?>) laterSourceList.get("structuredContent")).get("items");
                assertThat(laterSources.stream()
                        .map(source -> String.valueOf(((Map<?, ?>) source).get("id"))))
                        .as("the Source page after the first Source")
                        .containsExactly("src_later");
                assertThat(collectionsByName(input, output, 8, "src_later"))
                        .as("a source applied mid-session, read by a client that has not re-listed")
                        .containsKey("arrivals");

                // The other direction, and it costs nothing: taken out of the registry, gone from the
                // answer. The first source is asked again in the same breath, so "it disappeared" is
                // distinguishable from "the session broke".
                control.deleteSource("src_later");
                send(input, call(9, "src_later"));
                assertThat(((Map<?, ?>) receive(output).get("result")).get("isError"))
                        .as("reading a source that has been deleted, in a session that never restarted")
                        .isEqualTo(true);
                assertThat(collectionsByName(input, output, 10, "src_browse"))
                        .as("the source that was not deleted, asked right afterwards")
                        .containsKeys(DECLARED, BY_HAND);
            } finally {
                process.getOutputStream().close();
                if (!process.waitFor(5, TimeUnit.SECONDS)) {
                    process.destroyForcibly();
                }
            }
        } finally {
            if (previousConnectorsDir == null) {
                System.clearProperty("tapstate.e2e.connectors-dir");
            } else {
                System.setProperty("tapstate.e2e.connectors-dir", previousConnectorsDir);
            }
        }
    }

    /**
     * One {@code data_browser_collections} call, as a map from collection name to its whole entry.
     *
     * <p>Entries are re-keyed rather than decoded into anything: what several assertions here are about
     * is a key not being present, and a shape with a slot for it would fill that slot with a null.
     */
    private static Map<String, Map<String, Object>> collectionsByName(
            Writer input, BufferedReader output, int id, String sourceId) throws Exception {
        send(input, call(id, sourceId));
        Map<?, ?> result = (Map<?, ?>) receive(output).get("result");
        assertThat(result.get("isError")).as("listing the collections of %s", sourceId).isEqualTo(false);
        List<?> entries = (List<?>) ((Map<?, ?>) result.get("structuredContent")).get("collections");
        Map<String, Map<String, Object>> byName = new LinkedHashMap<>();
        for (Object entry : entries) {
            Map<String, Object> row = new LinkedHashMap<>();
            ((Map<?, ?>) entry).forEach((key, value) -> row.put(String.valueOf(key), value));
            byName.put(String.valueOf(row.get("name")), row);
        }
        return byName;
    }

    /** One {@code data_browser_find} call: the read an agent makes after a listing told it the name. */
    private static String findCall(int id, String sourceId, String collection) {
        return """
                {"jsonrpc":"2.0","id":%d,"method":"tools/call","params":{
                  "name":"data_browser_find","arguments":{"sourceId":"%s","collection":"%s"}}}
                """.formatted(id, sourceId, collection);
    }

    private static String call(int id, String sourceId) {
        return """
                {"jsonrpc":"2.0","id":%d,"method":"tools/call","params":{
                  "name":"data_browser_collections","arguments":{"sourceId":"%s"}}}
                """.formatted(id, sourceId);
    }

    private static String sourceYaml(String id, Path directory) {
        return """
                version: tapstate/v1
                kind: source
                id: %s
                connector: %s
                config: { uri: "%s" }
                """.formatted(id, E2eConnectorJar.BROWSABLE_CONNECTOR_ID, directory);
    }

    private static String declaringViewYaml() {
        return """
                version: tapstate/v1
                kind: view
                id: v_declared
                metadata: { description: "%s" }
                primary_key: id
                storage: { warm: { collection: %s } }
                """.formatted(DESCRIBED_AS, DECLARED);
    }

    private static String pipelineYaml() {
        return """
                version: tapstate/v1
                kind: pipeline
                id: pipeline_browse
                source: src_browse
                settings: { read_mode: snapshot_only }
                view:
                  id: v_pipeline
                  from: %s
                  primary_key: id
                """.formatted(DECLARED);
    }

    private static HttpServer server(ExchangeHandler handler) throws IOException {
        HttpServer server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
        server.createContext("/", handler::handle);
        server.start();
        return server;
    }

    private static Process startMcp(HttpServer server, Path stderr) throws IOException {
        return startMcp(URI.create("http://127.0.0.1:" + server.getAddress().getPort()),
                MACHINE_TOKEN, stderr);
    }

    /** The same process, pointed at whatever is at {@code serverUrl} - a fake peer or the product. */
    private static Process startMcp(URI serverUrl, String token, Path stderr) throws IOException {
        ProcessBuilder builder = new ProcessBuilder(
                Path.of(System.getProperty("java.home"), "bin", "java").toString(),
                "-jar", mcpBootJar().toString(),
                "--allow-write");
        builder.environment().put("TAPSTATE_SERVER_URL", serverUrl.toString());
        builder.environment().put("TAPSTATE_TOKEN", token);
        builder.redirectError(stderr.toFile());
        return builder.start();
    }

    private static void awaitServer(HttpServer server) throws Exception {
        awaitServer(server, READINESS_TIMEOUT);
    }

    private static void awaitServer(HttpServer server, Duration timeout) throws Exception {
        HttpClient client = HttpClient.newBuilder()
                .connectTimeout(timeout)
                .build();
        HttpResponse<Void> response = client.send(
                HttpRequest.newBuilder(URI.create(
                        "http://127.0.0.1:" + server.getAddress().getPort() + "/_ready"))
                        .timeout(timeout)
                        .GET()
                        .build(),
                HttpResponse.BodyHandlers.discarding());
        assertThat(response.statusCode()).isEqualTo(200);
    }

    private static Path mcpBootJar() {
        String file = System.getProperty(MCP_BOOT_JAR_PROPERTY);
        assertThat(file)
                .as("the build must set %s so the MCP process can be launched", MCP_BOOT_JAR_PROPERTY)
                .isNotBlank();
        Path jar = Path.of(file);
        assertThat(jar).isRegularFile();
        return jar;
    }

    private static void send(Writer input, String message) throws IOException {
        input.write(message.strip().replace("\n", ""));
        input.write('\n');
        input.flush();
    }

    private static Map<?, ?> receive(BufferedReader output) throws Exception {
        CompletableFuture<String> line = CompletableFuture.supplyAsync(() -> {
            try {
                return output.readLine();
            } catch (IOException error) {
                throw new UncheckedIOException(error);
            }
        });
        String response = line.get(Duration.ofSeconds(10).toMillis(), TimeUnit.MILLISECONDS);
        assertThat(response).isNotBlank();
        return (Map<?, ?>) JsonReader.parse(response);
    }

    private static void answer(HttpExchange exchange, int status, String body) throws IOException {
        byte[] bytes = body.getBytes(StandardCharsets.UTF_8);
        exchange.getResponseHeaders().set("Content-Type", "application/json");
        exchange.sendResponseHeaders(status, bytes.length);
        exchange.getResponseBody().write(bytes);
        exchange.close();
    }

    @FunctionalInterface
    private interface ExchangeHandler {
        void handle(HttpExchange exchange) throws IOException;
    }
}
