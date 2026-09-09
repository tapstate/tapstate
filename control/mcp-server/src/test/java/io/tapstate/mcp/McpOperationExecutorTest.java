package io.tapstate.mcp;

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpServer;
import io.tapstate.control.client.HttpControlClient;
import io.tapstate.control.core.ControlOperations;
import io.tapstate.control.core.Operation;
import io.tapstate.control.core.Scope;
import io.tapstate.core.common.JsonReader;
import io.tapstate.core.common.JsonWriter;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.Executors;

import static org.assertj.core.api.Assertions.assertThat;

class McpOperationExecutorTest {

    @Test
    void routesEveryMcpOperationThroughTheHttpControlContract() throws Exception {
        List<String> paths = Collections.synchronizedList(new ArrayList<>());
        HttpServer server = server(exchange -> {
            paths.add(exchange.getRequestURI().toString());
            if (exchange.getRequestMethod().equals("GET")
                    && exchange.getRequestURI().getPath().equals("/api/connectors/mysql")) {
                answer(exchange, 200, """
                        {"id":"mysql","origin":"registered","runtimeAvailable":true,
                         "config":[],
                         "spec":{"contentHash":"abc123","text":"{}","unavailable":null}}
                        """);
            } else {
                answer(exchange, 200, "{}");
            }
        });
        try (HttpControlClient client = new HttpControlClient(Duration.ofSeconds(1), Duration.ofSeconds(2))) {
            McpOperationExecutor executor = new McpOperationExecutor(
                    baseOf(server), "token", Map.of(), client);
            Map<String, Object> connection = Map.of(
                    "id", "orders", "connectorId", "mysql", "settings", Map.of());
            Map<String, Object> pipeline = Map.of("id", "orders");
            // A stop reaches the server only with the answer; without it this executor refuses at its
            // own end and no request is made, which is what the routing assertion below counts.
            Map<String, Object> stop = Map.of("id", "orders", "purgeState", true);
            Map<String, Object> logs = new LinkedHashMap<>(pipeline);
            logs.put("limit", 999);

            List<Map.Entry<io.tapstate.control.core.Operation, Map<String, Object>>> calls = List.of(
                    Map.entry(ControlOperations.SYSTEM_VERSION, Map.of()),
                    Map.entry(ControlOperations.CONNECTOR_LIST, Map.of()),
                    Map.entry(ControlOperations.CONNECTOR_GET, Map.of("id", "mysql")),
                    Map.entry(ControlOperations.SOURCE_DRAFT, Map.of(
                            "id", "orders", "connector", "mysql", "config", Map.of("host", "db"))),
                    Map.entry(ControlOperations.CONNECTION_TEST, connection),
                    Map.entry(ControlOperations.CONNECTION_TEST_RESULT, Map.of("id", "orders")),
                    Map.entry(ControlOperations.CONNECTION_DISCOVER_SCHEMA, connection),
                    Map.entry(ControlOperations.CONNECTION_SCHEMA, Map.of("id", "orders")),
                    Map.entry(ControlOperations.ARTIFACT_VALIDATE, Map.of("drafts", List.of())),
                    Map.entry(ControlOperations.ARTIFACT_APPLY, Map.of("drafts", List.of())),
                    Map.entry(ControlOperations.ARTIFACT_GET, Map.of("id", "orders")),
                    Map.entry(ControlOperations.ARTIFACT_DELETE,
                            Map.of("id", "orders", "expectedContentHash", "a".repeat(64))),
                    Map.entry(ControlOperations.PIPELINE_START, pipeline),
                    Map.entry(ControlOperations.PIPELINE_STOP, stop),
                    Map.entry(ControlOperations.PIPELINE_PAUSE, pipeline),
                    Map.entry(ControlOperations.PIPELINE_RESUME, pipeline),
                    Map.entry(ControlOperations.PIPELINE_STATUS, pipeline),
                    Map.entry(ControlOperations.PIPELINE_METRICS, pipeline),
                    Map.entry(ControlOperations.PIPELINE_SNAPSHOT, pipeline),
                    Map.entry(ControlOperations.PIPELINE_LOGS, logs),
                    Map.entry(ControlOperations.DATA_BROWSER_COLLECTIONS, Map.of("sourceId", "views")),
                    Map.entry(ControlOperations.DATA_BROWSER_STATS,
                            Map.of("sourceId", "views", "collection", "order_state")),
                    Map.entry(ControlOperations.DATA_BROWSER_FIND,
                            Map.of("sourceId", "views", "collection", "order_state")));

            // "Every" is derived, not counted by hand. The routing is a switch over operation ids, and
            // the failure it has is silent in exactly this shape: a verb marked open on this face with
            // no branch answers `unsupported MCP operation` to a caller who was told the tool exists.
            assertThat(calls.stream().map(Map.Entry::getKey))
                    .as("every operation the registry opens on MCP is exercised here")
                    .containsExactlyInAnyOrderElementsOf(McpToolCatalog.operations(true));

            for (Map.Entry<io.tapstate.control.core.Operation, Map<String, Object>> call : calls) {
                assertThat(executor.execute(call.getKey(), call.getValue()).error())
                        .as(call.getKey().id())
                        .isFalse();
            }

            assertThat(paths).contains(
                    // At the root, not under /api: the version answer is the anonymous endpoint the
                    // CLI also reads while connecting, and a second one would be a second truth.
                    "/version",
                    "/api/connectors", "/api/connectors/mysql", "/api/connections:test",
                    "/api/sources:draft",
                    "/api/connections/orders/test-result", "/api/connections:discover-schema",
                    "/api/connections/orders/schema", "/api/artifacts:validate", "/api/artifacts:apply",
                    "/api/artifacts/orders",
                    "/api/pipelines/orders:start", "/api/pipelines/orders:stop",
                    "/api/pipelines/orders:pause", "/api/pipelines/orders:resume",
                    "/api/pipelines/orders/status", "/api/pipelines/orders/metrics",
                    "/api/pipelines/orders/snapshot", "/api/pipelines/orders/logs?limit=200",
                    "/api/sources/views/collections",
                    "/api/sources/views/collections/order_state/stats",
                    "/api/sources/views/collections/order_state:find");
        } finally {
            server.stop(0);
        }
    }

    @Test
    void aReadThatAsksForNothingSendsNothingRatherThanThisFacesOwnDefaults() throws Exception {
        // The plan's claim is that the four surfaces share one request shape, which holds only while none
        // of them answers a question of its own. A face that filled in its own limit here would agree with
        // the control plane today and drift the day the control plane's answer changed — and the drift
        // would be invisible, because both sides would still be sending a number that works.
        AtomicReference<Map<?, ?>> posted = new AtomicReference<>();
        HttpServer server = server(exchange -> {
            posted.set((Map<?, ?>) JsonReader.parse(new String(
                    exchange.getRequestBody().readAllBytes(), StandardCharsets.UTF_8)));
            answer(exchange, 200, "{}");
        });
        try (HttpControlClient client = new HttpControlClient(Duration.ofSeconds(1), Duration.ofSeconds(2))) {
            McpOperationExecutor executor = new McpOperationExecutor(
                    baseOf(server), "token", Map.of(), client);

            executor.execute(ControlOperations.DATA_BROWSER_FIND,
                    Map.of("sourceId", "views", "collection", "order_state"));

            // Not "limit is 10" — an absent key, so the control plane's own answer is the only one there is.
            assertThat(posted.get()).isEmpty();
        } finally {
            server.stop(0);
        }
    }

    @Test
    void carriesOnlyWhatAReadActuallyAskedForAndLeavesTheTwoNamesInThePath() throws Exception {
        AtomicReference<Map<?, ?>> posted = new AtomicReference<>();
        AtomicReference<String> path = new AtomicReference<>();
        HttpServer server = server(exchange -> {
            path.set(exchange.getRequestURI().toString());
            posted.set((Map<?, ?>) JsonReader.parse(new String(
                    exchange.getRequestBody().readAllBytes(), StandardCharsets.UTF_8)));
            answer(exchange, 200, "{}");
        });
        try (HttpControlClient client = new HttpControlClient(Duration.ofSeconds(1), Duration.ofSeconds(2))) {
            McpOperationExecutor executor = new McpOperationExecutor(
                    baseOf(server), "token", Map.of(), client);
            Map<String, Object> arguments = new LinkedHashMap<>();
            arguments.put("sourceId", "views");
            arguments.put("collection", "order_state");
            arguments.put("filter", Map.of("field", "status", "op", "eq", "value", "paid"));
            arguments.put("limit", 25);

            executor.execute(ControlOperations.DATA_BROWSER_FIND, arguments);

            assertThat(path.get()).isEqualTo("/api/sources/views/collections/order_state:find");
            List<String> keys = new ArrayList<>();
            posted.get().keySet().forEach(key -> keys.add(String.valueOf(key)));
            assertThat(keys).containsExactlyInAnyOrder("filter", "limit");
            assertThat(posted.get().get("limit")).isEqualTo(25L);
        } finally {
            server.stop(0);
        }
    }

    @Test
    void unreachableServerIsReturnedAsStructuredCodedFailure() {
        try (HttpControlClient client = new HttpControlClient(Duration.ofMillis(200), Duration.ofMillis(200))) {
            McpOperationExecutor executor = new McpOperationExecutor(
                    URI.create("http://127.0.0.1:0"), "token", Map.of(), client);

            McpResult result = executor.execute(ControlOperations.PIPELINE_STATUS, Map.of("id", "orders"));

            assertThat(result.error()).isTrue();
            assertThat(result.body()).containsEntry("code", "control.unreachable");
        }
    }

    @Test
    void sourceDraftExpandsOnlyConfigBeforeSendingItToTheServer() throws Exception {
        SourceDraftExchange exchange = executeSourceDraft(
                Map.of("MYSQL_PASSWORD", "expanded-secret"),
                Map.of("id", "${MYSQL_PASSWORD}", "connector", "mysql", "mode", "snapshot",
                        "config", Map.of("password", "${MYSQL_PASSWORD}")),
                "{\"id\":\"mysql\",\"config\":[{\"name\":\"password\",\"secret\":true}]}",
                "{\"yaml\":\"version: tapstate/v1\\nkind: source\\nid: orders\\nconnector: mysql\\n"
                        + "config:\\n  password: expanded-secret\\n\"}");

        assertThat(exchange.result().error()).isFalse();
        assertThat(exchange.posted().get("mode")).isEqualTo("snapshot");
        assertThat(exchange.posted().get("id")).isEqualTo("${MYSQL_PASSWORD}");
        assertThat(((Map<?, ?>) exchange.posted().get("config")).get("password"))
                .isEqualTo("expanded-secret");
        assertThat(exchange.result().body().get("yaml")).isEqualTo("version: tapstate/v1\n"
                + "kind: source\n"
                + "id: orders\n"
                + "connector: mysql\n");
    }

    @Test
    void sourceDraftRestoresPlaceholdersForNonSecretConfigFields() throws Exception {
        SourceDraftExchange exchange = executeSourceDraft(
                Map.of("MYSQL_HOST", "expanded-host"),
                Map.of("id", "orders", "connector", "mysql", "mode", "snapshot",
                        "config", Map.of("host", "${MYSQL_HOST}")),
                "{\"id\":\"mysql\",\"config\":[{\"name\":\"host\",\"secret\":false}]}",
                "{\"yaml\":\"version: tapstate/v1\\nkind: source\\nid: orders\\n"
                        + "connector: mysql\\nconfig:\\n  host: expanded-host\\n\"}");

        assertThat(exchange.result().error()).isFalse();
        assertThat(((Map<?, ?>) exchange.posted().get("config")).get("host"))
                .isEqualTo("expanded-host");
        assertThat(exchange.result().body().get("yaml")).asString()
                .contains("host: ${MYSQL_HOST}")
                .doesNotContain("host: expanded-host");
    }

    @Test
    void sourceDraftFailsClosedForUnavailableConnectorMetadata() throws Exception {
        assertSourceDraftUnavailable(500, "{}", "{}");
        assertSourceDraftUnavailable(200, "{\"config\":{}}", "{}");
        assertSourceDraftUnavailable(200, "{\"config\":[{\"name\":\"password\"}]}", "{}");
    }

    @Test
    void sourceDraftFailsClosedForMalformedDraftResponses() throws Exception {
        String connector = "{\"config\":[{\"name\":\"password\",\"secret\":true}]}";
        assertSourceDraftUnavailable(200, connector, "{}");
        assertSourceDraftUnavailable(200, connector, "{\"yaml\":\"version: tapstate/v1\\n"
                + "kind: pipeline\\nid: not_source\\nsource: src_file\\n"
                + "settings: { read_mode: snapshot_and_cdc }\\n"
                + "transforms: []\\nserve: { from: src_file, sync: [] }\\n\"}");
        assertSourceDraftUnavailable(200, connector, "{\"yaml\":\"not valid yaml\"}");
    }

    @Test
    void connectionWritesExpandOnlySettingsBeforeSendingThemToTheServer() throws Exception {
        AtomicReference<Map<?, ?>> posted = new AtomicReference<>();
        HttpServer server = server(exchange -> {
            posted.set((Map<?, ?>) JsonReader.parse(new String(
                    exchange.getRequestBody().readAllBytes(), StandardCharsets.UTF_8)));
            answer(exchange, 200, "{\"id\":\"orders\"}");
        });
        try (HttpControlClient client = new HttpControlClient(Duration.ofSeconds(1), Duration.ofSeconds(2))) {
            McpOperationExecutor executor = new McpOperationExecutor(
                    baseOf(server), "token", Map.of("MYSQL_PASSWORD", "sentinel-secret"), client);
            Map<String, Object> request = Map.of(
                    "id", "${MYSQL_PASSWORD}",
                    "connectorId", "mysql",
                    "settings", Map.of("password", "${MYSQL_PASSWORD}"));

            McpResult tested = executor.execute(ControlOperations.CONNECTION_TEST, request);
            assertThat(tested.error()).isFalse();
            assertThat(posted.get().get("id")).isEqualTo("${MYSQL_PASSWORD}");
            assertThat(((Map<?, ?>) posted.get().get("settings")).get("password"))
                    .isEqualTo("sentinel-secret");

            McpResult discovered = executor.execute(ControlOperations.CONNECTION_DISCOVER_SCHEMA, request);
            assertThat(discovered.error()).isFalse();
            assertThat(((Map<?, ?>) posted.get().get("settings")).get("password"))
                    .isEqualTo("sentinel-secret");
        } finally {
            server.stop(0);
        }
    }

    /**
     * Every operation the registry opens on the MCP face must have a route here. Without this, adding a
     * tool to the registry publishes it to the model — the sidecar advertises it, the schema resolves,
     * the description reads fine — and only a call at runtime discovers there is no path behind it. The
     * enumerated routing test above cannot catch that: a route nobody remembered to add is also a row
     * nobody remembered to list.
     */
    @Test
    void everyOperationTheMcpFaceExposesHasARouteBehindIt() throws Exception {
        HttpServer server = server(exchange -> answer(exchange, 200, "{}"));
        try (HttpControlClient client = new HttpControlClient(Duration.ofSeconds(1), Duration.ofSeconds(2))) {
            McpOperationExecutor executor = new McpOperationExecutor(
                    baseOf(server), "token", Map.of(), client);

            for (io.tapstate.control.core.Operation operation : McpToolCatalog.operations(true)) {
                // Arguments are deliberately absent: a missing required argument is refused with its own
                // code, which is a route doing its job. What must not appear is the unsupported-operation
                // code, which means execute() fell through to its default.
                McpResult result = executor.execute(operation, Map.of());
                assertThat(String.valueOf(JsonWriter.write(result.body())))
                        .as(operation.id())
                        .doesNotContain("unsupported MCP operation");
            }
        } finally {
            server.stop(0);
        }
    }

    @Test
    void artifactDeleteSendsADeleteCarryingThePreconditionAsAnEntityTag() throws Exception {
        AtomicReference<String> method = new AtomicReference<>();
        AtomicReference<String> ifMatch = new AtomicReference<>();
        AtomicReference<String> path = new AtomicReference<>();
        HttpServer server = server(exchange -> {
            method.set(exchange.getRequestMethod());
            ifMatch.set(exchange.getRequestHeaders().getFirst("If-Match"));
            path.set(exchange.getRequestURI().toString());
            answer(exchange, 204, "");
        });
        try (HttpControlClient client = new HttpControlClient(Duration.ofSeconds(1), Duration.ofSeconds(2))) {
            McpOperationExecutor executor = new McpOperationExecutor(
                    baseOf(server), "token", Map.of(), client);
            String hash = "b".repeat(64);

            McpResult result = executor.execute(
                    ControlOperations.ARTIFACT_DELETE, Map.of("id", "orders", "expectedContentHash", hash));

            assertThat(result.error()).isFalse();
            // Routed as a real DELETE with the precondition attached. A GET to the same path would read
            // the artifact and answer 200, which this test would otherwise accept as a removal.
            assertThat(method.get()).isEqualTo("DELETE");
            assertThat(path.get()).isEqualTo("/api/artifacts/orders");
            assertThat(ifMatch.get()).isEqualTo("\"" + hash + "\"");
        } finally {
            server.stop(0);
        }
    }

    @Test
    void artifactGetReadsOneArtifactByIdAndReturnsWhatTheServerHolds() throws Exception {
        AtomicReference<String> method = new AtomicReference<>();
        AtomicReference<String> path = new AtomicReference<>();
        HttpServer server = server(exchange -> {
            method.set(exchange.getRequestMethod());
            path.set(exchange.getRequestURI().toString());
            answer(exchange, 200,
                    "{\"id\":\"orders\",\"kind\":\"pipeline\",\"canonicalForm\":\"x\",\"contentHash\":\""
                            + "c".repeat(64) + "\"}");
        });
        try (HttpControlClient client = new HttpControlClient(Duration.ofSeconds(1), Duration.ofSeconds(2))) {
            McpOperationExecutor executor = new McpOperationExecutor(
                    baseOf(server), "token", Map.of(), client);

            McpResult result = executor.execute(ControlOperations.ARTIFACT_GET, Map.of("id", "orders"));

            assertThat(result.error()).isFalse();
            assertThat(method.get()).isEqualTo("GET");
            assertThat(path.get()).isEqualTo("/api/artifacts/orders");
            // The hash is the whole reason this read is on the MCP face: a model that cannot see it here
            // has no route to the precondition artifact.delete demands.
            assertThat(String.valueOf(JsonWriter.write(result.body()))).contains("contentHash");
        } finally {
            server.stop(0);
        }
    }

    @Test
    void artifactGetEncodesAnIdThatWouldOtherwiseChangeThePath() throws Exception {
        // An id is user-chosen text on the way into a URL path. Without encoding, one containing a slash
        // reads as a different endpoint entirely and the read silently targets something else.
        AtomicReference<String> path = new AtomicReference<>();
        HttpServer server = server(exchange -> {
            path.set(exchange.getRequestURI().getRawPath());
            answer(exchange, 200, "{}");
        });
        try (HttpControlClient client = new HttpControlClient(Duration.ofSeconds(1), Duration.ofSeconds(2))) {
            McpOperationExecutor executor = new McpOperationExecutor(
                    baseOf(server), "token", Map.of(), client);

            executor.execute(ControlOperations.ARTIFACT_GET, Map.of("id", "a/b"));

            assertThat(path.get()).isEqualTo("/api/artifacts/a%2Fb");
        } finally {
            server.stop(0);
        }
    }

    @Test
    void artifactDeleteRefusesBeforeAnyRequestWhenThePreconditionIsMissing() {
        // The precondition is required, so a model that omits it must be refused here rather than have a
        // hash-less delete reach the server, where the refusal would be indistinguishable from a bug.
        try (HttpControlClient client = new HttpControlClient(Duration.ofSeconds(1), Duration.ofSeconds(2))) {
            McpOperationExecutor executor = new McpOperationExecutor(
                    URI.create("http://127.0.0.1:1"), "token", Map.of(), client);

            McpResult result = executor.execute(ControlOperations.ARTIFACT_DELETE, Map.of("id", "orders"));

            assertThat(result.error()).isTrue();
            assertThat(JsonWriter.write(result.body())).contains("expectedContentHash");
        }
    }

    @Test
    void artifactDeleteAnswersWithWhatItRemovedRatherThanTheServersEmptyBody() throws Exception {
        // The server answers 204, which carries no body, and this is the one operation on the surface
        // that cannot be undone and leaves nothing behind to read afterwards. An empty result gives a
        // model no content-level evidence the removal happened, and nothing to tell it apart from an
        // ambiguous one — which is exactly the state that invites a retry of an irreversible call.
        HttpServer server = server(exchange -> answer(exchange, 204, ""));
        try (HttpControlClient client = new HttpControlClient(Duration.ofSeconds(1), Duration.ofSeconds(2))) {
            McpOperationExecutor executor = new McpOperationExecutor(
                    baseOf(server), "token", Map.of(), client);

            McpResult result = executor.execute(ControlOperations.ARTIFACT_DELETE,
                    Map.of("id", "orders", "expectedContentHash", "a".repeat(64)));

            assertThat(result.error()).isFalse();
            assertThat(result.body()).containsEntry("id", "orders").containsEntry("removed", true);
            assertThat(result.body()).containsEntry("expectedContentHash", "a".repeat(64));
        } finally {
            server.stop(0);
        }
    }

    @Test
    void aRefusedArtifactDeleteIsStillReportedAsTheServerStatedIt() throws Exception {
        // The body added above must not swallow a refusal into a success: a delete the server rejected
        // reported as {"removed": true} is worse than the empty body it replaced.
        HttpServer server = server(exchange -> answer(exchange, 409,
                "{\"code\":\"artifact.version-conflict\",\"message\":\"it changed\"}"));
        try (HttpControlClient client = new HttpControlClient(Duration.ofSeconds(1), Duration.ofSeconds(2))) {
            McpOperationExecutor executor = new McpOperationExecutor(
                    baseOf(server), "token", Map.of(), client);

            McpResult result = executor.execute(ControlOperations.ARTIFACT_DELETE,
                    Map.of("id", "orders", "expectedContentHash", "a".repeat(64)));

            assertThat(result.error()).isTrue();
            assertThat(JsonWriter.write(result.body())).contains("artifact.version-conflict");
            // Asserted non-empty first: "does not contain" is satisfied by a body that carries nothing
            // at all, which is the outcome this whole test exists to rule out.
            assertThat(result.body()).isNotEmpty().doesNotContainKey("removed");
        } finally {
            server.stop(0);
        }
    }

    @Test
    void unsupportedOperationsAreReturnedAsStructuredFailures() {
        try (HttpControlClient client = new HttpControlClient()) {
            McpOperationExecutor executor = new McpOperationExecutor(
                    URI.create("http://127.0.0.1:1"), "token", Map.of(), client);

            McpResult result = executor.execute(
                    new Operation("test.unsupported", Scope.READ, false, null, Map.of()), Map.of());

            assertThat(result.error()).isTrue();
            assertThat(result.body()).containsEntry("code", "control.malformed-request");
        }
    }

    @Test
    void requiredPipelineIdIsValidatedBeforeAnyHttpRequest() {
        try (HttpControlClient client = new HttpControlClient()) {
            McpOperationExecutor executor = new McpOperationExecutor(
                    URI.create("http://127.0.0.1:1"), "token", Map.of(), client);

            McpResult missing = executor.execute(ControlOperations.PIPELINE_STATUS, Map.of());
            McpResult blank = executor.execute(ControlOperations.PIPELINE_STATUS, Map.of("id", " "));

            assertThat(missing.body()).containsEntry("code", "control.malformed-request");
            assertThat(blank.body()).containsEntry("code", "control.malformed-request");
            assertThat(missing.error()).isTrue();
            assertThat(blank.error()).isTrue();
        }
    }

    private static HttpServer server(Handler handler) throws IOException {
        HttpServer server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
        server.setExecutor(Executors.newCachedThreadPool(runnable -> {
            Thread thread = new Thread(runnable, "mcp-operation-test-http");
            thread.setDaemon(true);
            return thread;
        }));
        server.createContext("/", exchange -> handler.handle(exchange));
        server.start();
        return server;
    }

    private static URI baseOf(HttpServer server) {
        return URI.create("http://127.0.0.1:" + server.getAddress().getPort());
    }

    private static McpResult sourceDraftResult(
            int connectorStatus, String connectorBody, String draftBody) throws Exception {
        return executeSourceDraft(Map.of(),
                Map.of("id", "orders", "connector", "mysql", "config", Map.of()),
                connectorStatus, connectorBody, draftBody).result();
    }

    private static SourceDraftExchange executeSourceDraft(
            Map<String, String> environment,
            Map<String, Object> arguments,
            String connectorBody,
            String draftBody) throws Exception {
        return executeSourceDraft(environment, arguments, 200, connectorBody, draftBody);
    }

    private static SourceDraftExchange executeSourceDraft(
            Map<String, String> environment,
            Map<String, Object> arguments,
            int connectorStatus,
            String connectorBody,
            String draftBody) throws Exception {
        AtomicReference<Map<?, ?>> posted = new AtomicReference<>();
        HttpServer server = server(exchange -> {
            if (exchange.getRequestMethod().equals("GET")) {
                answer(exchange, connectorStatus, connectorBody);
            } else {
                posted.set((Map<?, ?>) JsonReader.parse(new String(
                        exchange.getRequestBody().readAllBytes(), StandardCharsets.UTF_8)));
                answer(exchange, 200, draftBody);
            }
        });
        try (HttpControlClient client = new HttpControlClient(Duration.ofSeconds(1), Duration.ofSeconds(2))) {
            McpOperationExecutor executor = new McpOperationExecutor(
                    baseOf(server), "token", environment, client);
            return new SourceDraftExchange(executor.execute(ControlOperations.SOURCE_DRAFT, arguments), posted.get());
        } finally {
            server.stop(0);
        }
    }

    private record SourceDraftExchange(McpResult result, Map<?, ?> posted) { }

    private static void assertSourceDraftUnavailable(
            int connectorStatus, String connectorBody, String draftBody) throws Exception {
        assertThat(sourceDraftResult(connectorStatus, connectorBody, draftBody).body())
                .containsEntry("code", "mcp.connector-spec-unavailable");
    }

    private static void answer(HttpExchange exchange, int status, String body) throws IOException {
        byte[] bytes = body.getBytes(StandardCharsets.UTF_8);
        exchange.getResponseHeaders().set("Content-Type", "application/json");
        exchange.sendResponseHeaders(status, bytes.length);
        exchange.getResponseBody().write(bytes);
        exchange.close();
    }

    @FunctionalInterface
    private interface Handler {
        void handle(HttpExchange exchange) throws IOException;
    }
}
