package io.tapstate.cli;

import com.sun.net.httpserver.HttpServer;
import io.tapstate.core.common.JsonReader;
import org.junit.jupiter.api.Test;

import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * The production HTTP reachability probe against a tiny in-JVM server: {@code GET /healthz} returning
 * 200 means healthy, any non-200 or any I/O failure (connection refused) means not healthy and never
 * throws. Uses the JDK's own {@code HttpServer} so the test carries no dependency.
 */
class HttpControlPlaneClientTest {

    private static HttpServer serverReplying(int status, String body) throws Exception {
        HttpServer server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
        server.createContext("/healthz", exchange -> {
            byte[] bytes = body == null ? new byte[0] : body.getBytes(StandardCharsets.UTF_8);
            exchange.sendResponseHeaders(status, bytes.length == 0 ? -1 : bytes.length);
            if (bytes.length > 0) {
                try (OutputStream os = exchange.getResponseBody()) {
                    os.write(bytes);
                }
            }
            exchange.close();
        });
        server.start();
        return server;
    }

    private static URI baseOf(HttpServer server) {
        return URI.create("http://127.0.0.1:" + server.getAddress().getPort());
    }

    @Test
    void healthyWhenHealthzReturns200() throws Exception {
        HttpServer server = serverReplying(200, "ok");
        try {
            assertThat(new HttpControlPlaneClient().isHealthy(baseOf(server))).isTrue();
        } finally {
            server.stop(0);
        }
    }

    @Test
    void notHealthyWhenHealthzReturnsNon200() throws Exception {
        HttpServer server = serverReplying(503, null);
        try {
            assertThat(new HttpControlPlaneClient().isHealthy(baseOf(server))).isFalse();
        } finally {
            server.stop(0);
        }
    }

    @Test
    void notHealthyForAnUnreachablePortWithoutThrowing() throws Exception {
        int closedPort;
        try (ServerSocket socket = new ServerSocket(0)) {
            closedPort = socket.getLocalPort();
        }   // the port is closed on scope exit -> a connect there is refused
        URI base = URI.create("http://127.0.0.1:" + closedPort);
        assertThat(new HttpControlPlaneClient().isHealthy(base)).isFalse();
    }

    @Test
    void notHealthyForAHostlessUriWithoutThrowing() {
        // `http://foo:bar` parses but a non-numeric port makes the authority registry-based, so it has
        // no host; building the request throws IllegalArgumentException, which must resolve to not
        // healthy rather than propagate, honoring the never-throws contract
        assertThat(new HttpControlPlaneClient().isHealthy(URI.create("http://foo:bar"))).isFalse();
    }

    @Test
    void issuerDiscoveryUsesTheWellKnownGetWithoutAnyAuthorizationHeader() throws Exception {
        AtomicReference<String> authorization = new AtomicReference<>("not-called");
        HttpServer server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
        server.createContext("/.well-known/tapstate", exchange -> {
            authorization.set(exchange.getRequestHeaders().getFirst("Authorization"));
            byte[] body = ("{\"issuer\":\"urn:tapstate:cluster:01J5FIXTURE\","
                    + "\"clusterId\":\"01J5FIXTURE\",\"apiVersion\":\"tapstate/v1\","
                    + "\"authModes\":[\"password\",\"machine_token\"]}")
                    .getBytes(StandardCharsets.UTF_8);
            exchange.sendResponseHeaders(200, body.length);
            try (OutputStream output = exchange.getResponseBody()) {
                output.write(body);
            }
            exchange.close();
        });
        server.start();
        try {
            assertThat(new HttpControlPlaneClient().discover(baseOf(server)))
                    .isEqualTo(new DiscoveryOutcome.Discovered(
                            "urn:tapstate:cluster:01J5FIXTURE", "01J5FIXTURE", "tapstate/v1",
                            List.of("password", "machine_token")));
            assertThat(authorization.get()).isNull();
        } finally {
            server.stop(0);
        }
    }

    @Test
    void malformedIssuerDiscoveryResponseIsReportedAsInvalidRatherThanUnreachable() throws Exception {
        HttpServer server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
        server.createContext("/.well-known/tapstate", exchange -> {
            byte[] body = "not-json".getBytes(StandardCharsets.UTF_8);
            exchange.sendResponseHeaders(200, body.length);
            try (OutputStream output = exchange.getResponseBody()) {
                output.write(body);
            }
            exchange.close();
        });
        server.start();
        try {
            assertThat(new HttpControlPlaneClient().discover(baseOf(server)))
                    .isEqualTo(new DiscoveryOutcome.Invalid("response-body"));
        } finally {
            server.stop(0);
        }
    }

    @Test
    void issuerDiscoveryRejectsANonStringAuthMode() throws Exception {
        HttpServer server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
        server.createContext("/.well-known/tapstate", exchange -> {
            byte[] body = ("{\"issuer\":\"urn:tapstate:cluster:01J5FIXTURE\","
                    + "\"clusterId\":\"01J5FIXTURE\",\"apiVersion\":\"tapstate/v1\","
                    + "\"authModes\":[\"password\",12,\"machine_token\"]}")
                    .getBytes(StandardCharsets.UTF_8);
            exchange.sendResponseHeaders(200, body.length);
            try (OutputStream output = exchange.getResponseBody()) {
                output.write(body);
            }
            exchange.close();
        });
        server.start();
        try {
            assertThat(new HttpControlPlaneClient().discover(baseOf(server)))
                    .isEqualTo(new DiscoveryOutcome.Invalid("response-contract"));
        } finally {
            server.stop(0);
        }
    }

    // --- stream endpoint URI: http(s) base -> ws(s) with the path appended ------------------------

    @Test
    void wsUriSwapsHttpForWsAndAppendsThePath() {
        assertThat(HttpControlPlaneClient.wsUri(URI.create("http://node1:7900"), "/api/pipelines/pl1/status/watch"))
                .isEqualTo(URI.create("ws://node1:7900/api/pipelines/pl1/status/watch"));
    }

    @Test
    void wsUriSwapsHttpsForWssAndToleratesATrailingSlash() {
        assertThat(HttpControlPlaneClient.wsUri(URI.create("https://node1:7900/"), "/api/pipelines/pl1/logs/follow"))
                .isEqualTo(URI.create("wss://node1:7900/api/pipelines/pl1/logs/follow"));
    }

    // --- login: POST /auth/login -----------------------------------------------------------------

    /** A server whose {@code /auth/login} records the request body and replies with a fixed status + body. */
    private static HttpServer loginServer(int status, String responseBody, AtomicReference<String> captured)
            throws Exception {
        HttpServer server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
        server.createContext("/auth/login", exchange -> {
            captured.set(new String(exchange.getRequestBody().readAllBytes(), StandardCharsets.UTF_8));
            byte[] bytes = responseBody == null ? new byte[0] : responseBody.getBytes(StandardCharsets.UTF_8);
            exchange.sendResponseHeaders(status, bytes.length == 0 ? -1 : bytes.length);
            if (bytes.length > 0) {
                try (OutputStream os = exchange.getResponseBody()) {
                    os.write(bytes);
                }
            }
            exchange.close();
        });
        server.start();
        return server;
    }

    @Test
    void loginReturnsSuccessWithTheTokenOn200() throws Exception {
        HttpServer server = loginServer(200, "{\"token\":\"jwt-xyz\"}", new AtomicReference<>());
        try {
            LoginOutcome outcome = new HttpControlPlaneClient().login(baseOf(server), "alice", "s3cret");
            assertThat(outcome).isEqualTo(new LoginOutcome.Success("jwt-xyz"));
        } finally {
            server.stop(0);
        }
    }

    @Test
    void loginSendsTheUsernameAndPasswordAsAProperlyEscapedJsonBody() throws Exception {
        AtomicReference<String> body = new AtomicReference<>();
        HttpServer server = loginServer(200, "{\"token\":\"t\"}", body);
        try {
            // a password with a quote must survive JSON escaping and round-trip back on the server side
            new HttpControlPlaneClient().login(baseOf(server), "alice", "p@ss\"word");
            Map<?, ?> sent = (Map<?, ?>) JsonReader.parse(body.get());
            assertThat(sent.get("username")).isEqualTo("alice");
            assertThat(sent.get("password")).isEqualTo("p@ss\"word");
        } finally {
            server.stop(0);
        }
    }

    @Test
    void loginReturnsRejectedWithTheServerCodeAndMessageOn401() throws Exception {
        String errorBody = "{\"code\":\"control.auth-failed\",\"params\":{},\"message\":\"Login failed.\"}";
        HttpServer server = loginServer(401, errorBody, new AtomicReference<>());
        try {
            LoginOutcome outcome = new HttpControlPlaneClient().login(baseOf(server), "alice", "wrong");
            assertThat(outcome).isEqualTo(new LoginOutcome.Rejected("control.auth-failed", "Login failed."));
        } finally {
            server.stop(0);
        }
    }

    @Test
    void loginTreatsANonCodedErrorBodyAsAGenericRejectionRevealingNothing() throws Exception {
        // a non-JSON error body (e.g. a container 500 page) must not crash login, and the raw body must
        // not leak to the user: it is refused with a fixed generic message, no code
        HttpServer server = loginServer(500, "<html>Internal Server Error</html>", new AtomicReference<>());
        try {
            LoginOutcome outcome = new HttpControlPlaneClient().login(baseOf(server), "a", "b");
            assertThat(outcome).isEqualTo(new LoginOutcome.Rejected("", "Login was refused by the server."));
        } finally {
            server.stop(0);
        }
    }

    @Test
    void loginTreatsA200WithoutAUsableTokenAsUnreachableNotASuccess() throws Exception {
        // a bodyless / tokenless 200 (a reverse proxy, captive portal, or non-Tapstate server) is not a
        // real login and must never authenticate the session
        HttpServer emptyObject = loginServer(200, "{}", new AtomicReference<>());
        try {
            assertThat(new HttpControlPlaneClient().login(baseOf(emptyObject), "a", "b"))
                    .isInstanceOf(LoginOutcome.Unreachable.class);
        } finally {
            emptyObject.stop(0);
        }
        HttpServer blankToken = loginServer(200, "{\"token\":\"\"}", new AtomicReference<>());
        try {
            assertThat(new HttpControlPlaneClient().login(baseOf(blankToken), "a", "b"))
                    .isInstanceOf(LoginOutcome.Unreachable.class);
        } finally {
            blankToken.stop(0);
        }
    }

    @Test
    void loginReturnsUnreachableWhenTheServerIsDownWithoutThrowing() throws Exception {
        int closedPort;
        try (ServerSocket socket = new ServerSocket(0)) {
            closedPort = socket.getLocalPort();
        }
        LoginOutcome outcome =
                new HttpControlPlaneClient().login(URI.create("http://127.0.0.1:" + closedPort), "a", "b");
        assertThat(outcome).isInstanceOf(LoginOutcome.Unreachable.class);
    }

    // --- online verbs: apply / get / list under /api, authenticated by a bearer credential ---------

    /** What the fake server saw for one request: method, path, query, the Authorization header, and body. */
    private record CapturedRequest(String method, String path, String query, String authorization, String body) {
    }

    /**
     * A server that answers one {@code context} with a fixed status + body and records the request it saw,
     * so a test can assert the client sent the right method, path, bearer credential and JSON body.
     */
    private static HttpServer apiServer(String context, int status, String responseBody,
            AtomicReference<CapturedRequest> captured) throws Exception {
        HttpServer server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
        server.createContext(context, exchange -> {
            String body = new String(exchange.getRequestBody().readAllBytes(), StandardCharsets.UTF_8);
            captured.set(new CapturedRequest(
                    exchange.getRequestMethod(),
                    exchange.getRequestURI().getRawPath(),
                    exchange.getRequestURI().getQuery(),
                    exchange.getRequestHeaders().getFirst("Authorization"),
                    body));
            byte[] bytes = responseBody == null ? new byte[0] : responseBody.getBytes(StandardCharsets.UTF_8);
            exchange.sendResponseHeaders(status, bytes.length == 0 ? -1 : bytes.length);
            if (bytes.length > 0) {
                try (OutputStream os = exchange.getResponseBody()) {
                    os.write(bytes);
                }
            }
            exchange.close();
        });
        server.start();
        return server;
    }

    /** A server that sleeps past the client's read timeout before answering {@code context}, so a request times out. */
    private static HttpServer slowServer(String context, long sleepMillis) throws Exception {
        HttpServer server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
        server.createContext(context, exchange -> {
            try {
                Thread.sleep(sleepMillis);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
            exchange.sendResponseHeaders(200, -1);
            exchange.close();
        });
        server.start();
        return server;
    }

    // ---- the removal verb over the real transport ----

    /**
     * A server for the removal verb that also records {@code If-Match}, which {@link CapturedRequest}
     * does not carry — and which is the whole of what distinguishes a conditional removal from an
     * unconditional one.
     */
    private static HttpServer deleteServer(int status, String responseBody,
            AtomicReference<String> method, AtomicReference<String> path,
            AtomicReference<String> ifMatch, AtomicReference<Boolean> hadIfMatch) throws Exception {
        return deleteServer(status, responseBody, method, path, ifMatch, hadIfMatch, new AtomicReference<>());
    }

    /** The same server, for the one test that also pins the credential the removal travelled under. */
    private static HttpServer deleteServer(int status, String responseBody,
            AtomicReference<String> method, AtomicReference<String> path,
            AtomicReference<String> ifMatch, AtomicReference<Boolean> hadIfMatch,
            AtomicReference<String> authorization) throws Exception {
        HttpServer server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
        server.createContext("/api/artifacts", exchange -> {
            method.set(exchange.getRequestMethod());
            path.set(exchange.getRequestURI().getRawPath());
            authorization.set(exchange.getRequestHeaders().getFirst("Authorization"));
            hadIfMatch.set(exchange.getRequestHeaders().containsKey("If-Match"));
            ifMatch.set(exchange.getRequestHeaders().getFirst("If-Match"));
            byte[] bytes = responseBody == null ? new byte[0] : responseBody.getBytes(StandardCharsets.UTF_8);
            exchange.sendResponseHeaders(status, bytes.length == 0 ? -1 : bytes.length);
            if (bytes.length > 0) {
                try (OutputStream os = exchange.getResponseBody()) {
                    os.write(bytes);
                }
            }
            exchange.close();
        });
        server.start();
        return server;
    }

    @Test
    void deleteSendsTheMethodTheBearerAndTheQuotedPreconditionAndReportsRemoval() throws Exception {
        AtomicReference<String> method = new AtomicReference<>();
        AtomicReference<String> path = new AtomicReference<>();
        AtomicReference<String> ifMatch = new AtomicReference<>();
        AtomicReference<Boolean> had = new AtomicReference<>();
        AtomicReference<String> authorization = new AtomicReference<>();
        HttpServer server = deleteServer(204, null, method, path, ifMatch, had, authorization);
        try {
            String hash = "a".repeat(64);
            DeleteOutcome outcome = new HttpControlPlaneClient()
                    .delete(baseOf(server), "tok-abc", "src_kfk", hash);

            assertThat(outcome).isEqualTo(new DeleteOutcome.Removed("src_kfk"));
            // A GET to this path would answer 200 with the artifact, which a status-only assertion would
            // read as a successful removal — so the method itself is pinned.
            assertThat(method.get()).isEqualTo("DELETE");
            assertThat(path.get()).isEqualTo("/api/artifacts/src_kfk");
            // A removal that dropped the credential would reach an unauthenticated server just as well,
            // and every other assertion here would still hold.
            assertThat(authorization.get()).isEqualTo("Bearer tok-abc");
            assertThat(ifMatch.get()).isEqualTo("\"" + hash + "\"");
        } finally {
            server.stop(0);
        }
    }

    @Test
    void deleteWithoutAPreconditionOmitsTheHeaderEntirely() throws Exception {
        AtomicReference<String> method = new AtomicReference<>();
        AtomicReference<String> path = new AtomicReference<>();
        AtomicReference<String> ifMatch = new AtomicReference<>();
        AtomicReference<Boolean> had = new AtomicReference<>();
        HttpServer server = deleteServer(428, "{\"code\":\"artifact.precondition-required\","
                + "\"message\":\"A precondition is required.\",\"params\":{\"id\":\"src_kfk\"}}",
                method, path, ifMatch, had);
        try {
            DeleteOutcome outcome = new HttpControlPlaneClient()
                    .delete(baseOf(server), "tok-abc", "src_kfk", null);

            // Sending an empty If-Match would be a malformed precondition, which the server answers as a
            // different refusal than none at all; the header must simply be absent.
            assertThat(had.get()).isFalse();
            assertThat(outcome).isInstanceOfSatisfying(DeleteOutcome.Rejected.class, rejected -> {
                assertThat(rejected.code()).isEqualTo("artifact.precondition-required");
                assertThat(rejected.params()).containsEntry("id", "src_kfk");
            });
        } finally {
            server.stop(0);
        }
    }

    @Test
    void aRefusedRemovalKeepsTheNamedParametersTheCallerHasToActOn() throws Exception {
        AtomicReference<String> method = new AtomicReference<>();
        AtomicReference<String> path = new AtomicReference<>();
        AtomicReference<String> ifMatch = new AtomicReference<>();
        AtomicReference<Boolean> had = new AtomicReference<>();
        HttpServer server = deleteServer(409, "{\"code\":\"artifact.in-use\","
                + "\"message\":\"Still referenced.\",\"params\":{\"id\":\"src_kfk\","
                + "\"referrers\":[\"kfk2my\",\"kfk2pg\"]}}", method, path, ifMatch, had);
        try {
            DeleteOutcome outcome = new HttpControlPlaneClient()
                    .delete(baseOf(server), "tok-abc", "src_kfk", "b".repeat(64));

            assertThat(outcome).isInstanceOfSatisfying(DeleteOutcome.Rejected.class, rejected -> {
                assertThat(rejected.code()).isEqualTo("artifact.in-use");
                // Dropping the parameters would leave the caller a sentence and no next step: which
                // resources to deal with is only in here.
                assertThat(rejected.params().get("referrers")).isEqualTo(List.of("kfk2my", "kfk2pg"));
            });
        } finally {
            server.stop(0);
        }
    }

    @Test
    void anUnreachableServerMakesTheRemovalUnreachableRatherThanASuccess() {
        DeleteOutcome outcome = new HttpControlPlaneClient()
                .delete(URI.create("http://127.0.0.1:1"), "tok", "src_kfk", "c".repeat(64));

        assertThat(outcome).isInstanceOf(DeleteOutcome.Unreachable.class);
    }

    @Test
    void theReadAndTheRemovalOfOneIdTargetTheSameEncodedPath() throws Exception {
        AtomicReference<String> method = new AtomicReference<>();
        AtomicReference<String> path = new AtomicReference<>();
        AtomicReference<String> ifMatch = new AtomicReference<>();
        AtomicReference<Boolean> had = new AtomicReference<>();
        // A space is the cheapest character that tells path encoding apart from form encoding: they
        // agree on everything else an id is likely to hold and disagree on exactly this one.
        String id = "src kfk";
        HttpServer server = deleteServer(404, "{\"code\":\"artifact.not-found\","
                + "\"message\":\"No such resource.\",\"params\":{\"id\":\"src kfk\"}}",
                method, path, ifMatch, had);
        try {
            new HttpControlPlaneClient().get(baseOf(server), "tok", id);
            String afterRead = path.get();
            new HttpControlPlaneClient().delete(baseOf(server), "tok", id, null);
            String afterRemoval = path.get();

            // A removal reads the current version and then deletes the same id, so if these two
            // disagree the halves of one removal address two different resources.
            assertThat(afterRead).isEqualTo("/api/artifacts/src%20kfk");
            assertThat(afterRemoval).isEqualTo(afterRead);
        } finally {
            server.stop(0);
        }
    }

    @Test
    void aRefusedRemovalSurvivesAParameterTheServerSentAsNull() throws Exception {
        AtomicReference<String> method = new AtomicReference<>();
        AtomicReference<String> path = new AtomicReference<>();
        AtomicReference<String> ifMatch = new AtomicReference<>();
        AtomicReference<Boolean> had = new AtomicReference<>();
        HttpServer server = deleteServer(409, "{\"code\":\"artifact.pipeline-not-stopped\","
                + "\"message\":\"Stop it first.\",\"params\":{\"id\":\"kfk2my\","
                + "\"actual\":\"RUNNING\",\"desired\":null}}", method, path, ifMatch, had);
        try {
            DeleteOutcome outcome = new HttpControlPlaneClient()
                    .delete(baseOf(server), "tok", "kfk2my", "d".repeat(64));

            // One null-valued parameter must not cost the caller the code and the message. Those name
            // the refusal and its next step; the null is the least informative part of the body, and
            // losing the whole refusal over it leaves a bare sentence and nothing to act on.
            assertThat(outcome).isInstanceOfSatisfying(DeleteOutcome.Rejected.class, rejected -> {
                assertThat(rejected.code()).isEqualTo("artifact.pipeline-not-stopped");
                assertThat(rejected.params()).containsEntry("actual", "RUNNING");
            });
        } finally {
            server.stop(0);
        }
    }

    @Test
    void readsTheCollectionNamesOutOfWhatTheListingEndpointAnswers() throws Exception {
        // Read against the body the server really sends, entries and all. A decoder that still expected
        // bare names would find none it recognized and report an empty database — a silent wrong answer,
        // not a failure, which is why this is asserted against the wire rather than a fake client.
        AtomicReference<CapturedRequest> seen = new AtomicReference<>();
        HttpServer server = apiServer("/api/sources/views/collections", 200,
                "{\"collections\":["
                        + "{\"name\":\"order_state\",\"kind\":\"view\",\"fields\":[\"id\"],"
                        + "\"description\":\"One row per order\"},"
                        + "{\"name\":\"customers\",\"kind\":\"view\"}]}",
                seen);
        try {
            // This test exercises wire decoding, while timeout behavior has dedicated coverage. Give the
            // in-JVM server a little scheduling headroom so a cold JDK HTTP client cannot hide that contract.
            DataBrowserOutcome.Collections outcome = new HttpControlPlaneClient(
                    Duration.ofSeconds(5), HttpControlPlaneClient.HEAVY_TIMEOUT)
                    .collections(baseOf(server), "tok-abc", "views");

            assertThat(outcome).isInstanceOf(DataBrowserOutcome.Collections.Listed.class);
            assertThat(((DataBrowserOutcome.Collections.Listed) outcome).collections())
                    .containsExactly("order_state", "customers");
            assertThat(seen.get().method()).isEqualTo("GET");
            assertThat(seen.get().authorization()).isEqualTo("Bearer tok-abc");
        } finally {
            server.stop(0);
        }
    }

    @Test
    void applyPostsTheDraftsWithABearerCredentialAndReturnsTheOutcomes() throws Exception {
        AtomicReference<CapturedRequest> seen = new AtomicReference<>();
        HttpServer server = apiServer("/api/artifacts:apply", 200,
                "{\"outcomes\":[{\"id\":\"src_kfk\",\"kind\":\"source\",\"change\":\"CREATED\",\"contentHash\":\"h1\"},"
                        + "{\"id\":\"kfk2my\",\"kind\":\"pipeline\",\"change\":\"UNCHANGED\",\"contentHash\":\"h2\"}]}",
                seen);
        try {
            ApplyOutcome outcome = new HttpControlPlaneClient().apply(baseOf(server), "tok-abc",
                    List.of(new LocalDraft("src_kfk.tap.yml", "kind: source\nid: src_kfk\n")));
            assertThat(outcome).isInstanceOf(ApplyOutcome.Applied.class);
            ApplyOutcome.Applied applied = (ApplyOutcome.Applied) outcome;
            assertThat(applied.items()).containsExactly(
                    new ApplyOutcome.Item("src_kfk", "source", "CREATED"),
                    new ApplyOutcome.Item("kfk2my", "pipeline", "UNCHANGED"));
            assertThat(seen.get().method()).isEqualTo("POST");
            assertThat(seen.get().path()).isEqualTo("/api/artifacts:apply");
            assertThat(seen.get().authorization()).isEqualTo("Bearer tok-abc");
            Map<?, ?> sent = (Map<?, ?>) JsonReader.parse(seen.get().body());
            assertThat(sent.get("drafts")).isInstanceOf(List.class);
            assertThat(seen.get().body()).contains("src_kfk.tap.yml").contains("kind: source");
            assertThat(applied.warnings())
                    .as("a body with no warnings array decodes to none, not to a null the caller must guard")
                    .isEmpty();
        } finally {
            server.stop(0);
        }
    }

    @Test
    void applyDecodesTheServerWarningsApartFromTheOutcomes() throws Exception {
        // The server carries an advisory finding as its code plus named params — the same shape the
        // validation diagnostics travel in — and the CLI renders it from its own bundled catalog.
        AtomicReference<CapturedRequest> seen = new AtomicReference<>();
        HttpServer server = apiServer("/api/artifacts:apply", 200,
                "{\"outcomes\":[{\"id\":\"src_kfk\",\"kind\":\"source\",\"change\":\"CREATED\",\"contentHash\":\"h1\"}],"
                        + "\"warnings\":[{\"code\":\"nest.resident-demand-over-budget\","
                        + "\"params\":{\"path\":\"orders.items\",\"rows\":120000}}]}",
                seen);
        try {
            ApplyOutcome outcome = new HttpControlPlaneClient().apply(baseOf(server), "tok-abc",
                    List.of(new LocalDraft("src_kfk.tap.yml", "kind: source\nid: src_kfk\n")));

            assertThat(outcome).isInstanceOf(ApplyOutcome.Applied.class);
            ApplyOutcome.Applied applied = (ApplyOutcome.Applied) outcome;
            assertThat(applied.items()).extracting(ApplyOutcome.Item::id).containsExactly("src_kfk");
            assertThat(applied.warnings()).singleElement().satisfies(warning -> {
                assertThat(warning.code()).isEqualTo("nest.resident-demand-over-budget");
                assertThat(warning.params()).containsEntry("path", "orders.items");
            });
        } finally {
            server.stop(0);
        }
    }

    @Test
    void applySkipsAWarningEntryWithNoCodeRatherThanFailingTheWholeDecode() throws Exception {
        // Same tolerance the outcome decode already has: one unexpected entry costs its own line, never
        // the applied result — the batch did land on the server, and saying otherwise would be a lie.
        AtomicReference<CapturedRequest> seen = new AtomicReference<>();
        HttpServer server = apiServer("/api/artifacts:apply", 200,
                "{\"outcomes\":[{\"id\":\"src_kfk\",\"kind\":\"source\",\"change\":\"CREATED\",\"contentHash\":\"h1\"}],"
                        + "\"warnings\":[{\"params\":{\"path\":\"orders.items\"}},"
                        + "{\"code\":\"nest.resident-demand-over-budget\"}]}",
                seen);
        try {
            ApplyOutcome outcome = new HttpControlPlaneClient().apply(baseOf(server), "tok-abc",
                    List.of(new LocalDraft("src_kfk.tap.yml", "kind: source\nid: src_kfk\n")));

            ApplyOutcome.Applied applied = (ApplyOutcome.Applied) outcome;
            assertThat(applied.items()).hasSize(1);
            assertThat(applied.warnings()).singleElement().satisfies(warning -> {
                assertThat(warning.code()).isEqualTo("nest.resident-demand-over-budget");
                assertThat(warning.params()).as("a warning with no params decodes to an empty map").isEmpty();
            });
        } finally {
            server.stop(0);
        }
    }

    @Test
    void aDraftWithAPreconditionCarriesItInTheApplyBodyAndOneWithoutOmitsTheKey() throws Exception {
        // Two halves of one contract. Present: the server has to receive it or the check never happens.
        // Absent: the key must not appear at all rather than appear as null — an existing caller's request
        // has to stay byte-identical, and a null would reach a schema that forbids what it does not declare.
        AtomicReference<CapturedRequest> seen = new AtomicReference<>();
        HttpServer server = apiServer("/api/artifacts:apply", 200, "{\"outcomes\":[]}", seen);
        try {
            new HttpControlPlaneClient().apply(baseOf(server), "tok-abc",
                    List.of(new LocalDraft("a.tap.yml", "kind: source\nid: a\n", "f".repeat(64))));
            assertThat(seen.get().body()).contains("\"expectedContentHash\": \"" + "f".repeat(64) + "\"");

            new HttpControlPlaneClient().apply(baseOf(server), "tok-abc",
                    List.of(new LocalDraft("a.tap.yml", "kind: source\nid: a\n")));
            assertThat(seen.get().body()).doesNotContain("expectedContentHash");
        } finally {
            server.stop(0);
        }
    }

    @Test
    void tokenCreatePostsScopeAndReturnsTheOneTimeBearer() throws Exception {
        AtomicReference<CapturedRequest> seen = new AtomicReference<>();
        HttpServer server = apiServer("/api/tokens", 201,
                "{\"tokenId\":\"tok_01\",\"scope\":\"WRITE\",\"token\":\"ts_live_secret\","
                        + "\"createdAt\":\"2026-07-30T01:02:03Z\"}", seen);
        try (HttpControlPlaneClient client = new HttpControlPlaneClient()) {
            TokenCreateOutcome outcome = client.tokenCreate(baseOf(server), "admin-token", "write");
            assertThat(outcome).isEqualTo(new TokenCreateOutcome.Issued(
                    new RemoteCreatedToken("tok_01", "WRITE", "ts_live_secret", "2026-07-30T01:02:03Z")));
            assertThat(seen.get().method()).isEqualTo("POST");
            assertThat(seen.get().path()).isEqualTo("/api/tokens");
            assertThat(seen.get().authorization()).isEqualTo("Bearer admin-token");
            assertThat(JsonReader.parse(seen.get().body())).isEqualTo(Map.of("scope", "write"));
        } finally {
            server.stop(0);
        }
    }

    @Test
    void tokenListDecodesOnlySecretFreeDescriptors() throws Exception {
        AtomicReference<CapturedRequest> seen = new AtomicReference<>();
        HttpServer server = apiServer("/api/tokens", 200,
                "{\"tokens\":[{\"tokenId\":\"tok_01\",\"scope\":\"READ\",\"revoked\":false,"
                        + "\"createdAt\":\"2026-07-30T01:02:03Z\"}]}", seen);
        try (HttpControlPlaneClient client = new HttpControlPlaneClient()) {
            TokenListOutcome outcome = client.tokenList(baseOf(server), "admin-token");
            assertThat(outcome).isEqualTo(new TokenListOutcome.Listed(List.of(
                    new RemoteToken("tok_01", "READ", false, "2026-07-30T01:02:03Z"))));
            assertThat(seen.get().method()).isEqualTo("GET");
            assertThat(seen.get().authorization()).isEqualTo("Bearer admin-token");
        } finally {
            server.stop(0);
        }
    }

    @Test
    void tokenRevokeUsesTheTokenIdAsAnEncodedPathSegment() throws Exception {
        AtomicReference<CapturedRequest> seen = new AtomicReference<>();
        HttpServer server = apiServer("/api/tokens/", 204, null, seen);
        try (HttpControlPlaneClient client = new HttpControlPlaneClient()) {
            assertThat(client.tokenRevoke(baseOf(server), "admin-token", "token/one"))
                    .isEqualTo(new TokenRevokeOutcome.Revoked());
            assertThat(seen.get().method()).isEqualTo("POST");
            assertThat(seen.get().path()).isEqualTo("/api/tokens/token%2Fone:revoke");
            assertThat(seen.get().authorization()).isEqualTo("Bearer admin-token");
        } finally {
            server.stop(0);
        }
    }

    @Test
    void registerPostsTheBase64ArtifactWithABearerCredentialAndReturnsTheRegistration() throws Exception {
        AtomicReference<CapturedRequest> seen = new AtomicReference<>();
        HttpServer server = apiServer("/api/connectors:register", 200,
                "{\"connectorId\":\"orders\",\"contentHash\":\"hash-abc\",\"pdkApiVersion\":\"1.3.5\","
                        + "\"newlyRegistered\":true}",
                seen);
        try {
            ConnectorRegisterOutcome outcome =
                    new HttpControlPlaneClient().register(baseOf(server), "tok-abc", new byte[] {1, 2, 3, 4});
            assertThat(outcome).isEqualTo(new ConnectorRegisterOutcome.Registered(
                    new RegisteredConnector("orders", "hash-abc", "1.3.5", true)));
            assertThat(seen.get().method()).isEqualTo("POST");
            assertThat(seen.get().path()).isEqualTo("/api/connectors:register");
            assertThat(seen.get().authorization()).isEqualTo("Bearer tok-abc");
            // The artifact bytes travel base64-encoded in the JSON body.
            Map<?, ?> sent = (Map<?, ?>) JsonReader.parse(seen.get().body());
            assertThat(sent.get("artifact")).isEqualTo("AQIDBA==");
        } finally {
            server.stop(0);
        }
    }

    @Test
    void registerReturnsRejectedWithTheServerCodeAndMessageOnACodedError() throws Exception {
        HttpServer server = apiServer("/api/connectors:register", 400,
                "{\"code\":\"connector.registration-conflict\",\"params\":{},"
                        + "\"message\":\"A different artifact already holds that id.\"}",
                new AtomicReference<>());
        try {
            ConnectorRegisterOutcome outcome =
                    new HttpControlPlaneClient().register(baseOf(server), "tok", new byte[] {9});
            assertThat(outcome).isEqualTo(new ConnectorRegisterOutcome.Rejected(
                    "connector.registration-conflict", "A different artifact already holds that id."));
        } finally {
            server.stop(0);
        }
    }

    @Test
    void registerReturnsUnreachableWhenTheServerIsDownWithoutThrowing() throws Exception {
        int closedPort;
        try (ServerSocket socket = new ServerSocket(0)) {
            closedPort = socket.getLocalPort();
        }
        ConnectorRegisterOutcome outcome = new HttpControlPlaneClient()
                .register(URI.create("http://127.0.0.1:" + closedPort), "tok", new byte[] {1});
        assertThat(outcome).isInstanceOf(ConnectorRegisterOutcome.Unreachable.class);
    }

    @Test
    void registerWaitsLongerThanTheProbeBudgetAndItsWindowGrowsWithArtifactSize() {
        HttpControlPlaneClient client = new HttpControlPlaneClient();
        // The flat 3s probe budget starved a multi-MB upload; register now waits materially longer,
        assertThat(client.registerTimeout(0)).isGreaterThan(Duration.ofSeconds(3));
        // and its window scales with the number of bytes it must push to the server.
        assertThat(client.registerTimeout(50_000_000)).isGreaterThan(client.registerTimeout(1_000));
    }

    @Test
    void registerReturnsTimedOutNotUnreachableWhenTheServerAnswersTooSlowly() throws Exception {
        HttpServer server = slowServer("/api/connectors:register", 2500);
        try {
            // A tiny injected budget so a slow (but reachable) server trips the read timeout in well under a second.
            ConnectorRegisterOutcome outcome =
                    new HttpControlPlaneClient(Duration.ofMillis(400), Duration.ofMillis(400))
                            .register(baseOf(server), "tok", new byte[] {1, 2, 3, 4});
            assertThat(outcome).isInstanceOf(ConnectorRegisterOutcome.TimedOut.class);
        } finally {
            server.stop(0);
        }
    }

    @Test
    void discoverSchemaReturnsTimedOutWhenTheServerAnswersTooSlowly() throws Exception {
        HttpServer server = slowServer("/api/connections:discover-schema", 2500);
        try {
            ConnectionDiscoverSchemaOutcome outcome =
                    new HttpControlPlaneClient(Duration.ofMillis(400), Duration.ofMillis(400))
                            .discoverSchema(baseOf(server), "tok", "conn1", "mysql", Map.of());
            assertThat(outcome).isInstanceOf(ConnectionDiscoverSchemaOutcome.TimedOut.class);
        } finally {
            server.stop(0);
        }
    }

    @Test
    void testReturnsTimedOutWhenTheServerAnswersTooSlowly() throws Exception {
        HttpServer server = slowServer("/api/connections:test", 2500);
        try {
            ConnectionTestOutcome outcome =
                    new HttpControlPlaneClient(Duration.ofMillis(400), Duration.ofMillis(400))
                            .test(baseOf(server), "tok", "conn1", "mysql", Map.of());
            assertThat(outcome).isInstanceOf(ConnectionTestOutcome.TimedOut.class);
        } finally {
            server.stop(0);
        }
    }

    @Test
    void applyReturnsRejectedWithTheServerCodeAndMessageOnACodedError() throws Exception {
        HttpServer server = apiServer("/api/artifacts:apply", 400,
                "{\"code\":\"dsl.illegal-value\",\"params\":{},\"message\":\"Not a known kind.\"}",
                new AtomicReference<>());
        try {
            ApplyOutcome outcome = new HttpControlPlaneClient()
                    .apply(baseOf(server), "tok", List.of(new LocalDraft("bad.tap.yml", "kind: nope\n")));
            assertThat(outcome).isEqualTo(new ApplyOutcome.Rejected("dsl.illegal-value", "Not a known kind."));
        } finally {
            server.stop(0);
        }
    }

    /**
     * A refusal is a refusal even when one of its named parameters came down as JSON null.
     *
     * <p>Nothing on the way here rejects a null value - the server's error carries its arguments in a
     * map that permits them, and this client decodes them into one that permits them too - so the
     * first thing that refused was the copy taken when the outcome was built, one frame outside the
     * catch that turns a malformed body into a refusal. The result was an uncaught crash out of
     * {@code apply} on a path whose whole posture is that a bad error body is still an error, not a
     * stack trace.
     */
    @Test
    void applyReturnsRejectedEvenWhenAParameterCameDownAsNull() throws Exception {
        HttpServer server = apiServer("/api/artifacts:apply", 400,
                "{\"code\":\"dsl.upsert-needs-key\",\"message\":\"No key.\","
                        + "\"params\":{\"table\":\"events\",\"source\":null}}",
                new AtomicReference<>());
        try {
            ApplyOutcome outcome = new HttpControlPlaneClient()
                    .apply(baseOf(server), "tok", List.of(new LocalDraft("bad.tap.yml", "kind: nope\n")));

            assertThat(outcome).isInstanceOfSatisfying(ApplyOutcome.Rejected.class, rejected -> {
                assertThat(rejected.code()).isEqualTo("dsl.upsert-needs-key");
                assertThat(rejected.params()).containsEntry("table", "events");
                assertThat(rejected.params())
                        .as("the null value is carried, not dropped and not fatal")
                        .containsEntry("source", null);
            });
        } finally {
            server.stop(0);
        }
    }

    @Test
    void applyReturnsUnreachableWhenTheServerIsDownWithoutThrowing() throws Exception {
        int closedPort;
        try (ServerSocket socket = new ServerSocket(0)) {
            closedPort = socket.getLocalPort();
        }
        ApplyOutcome outcome = new HttpControlPlaneClient(Duration.ofMillis(400), Duration.ofMillis(400))
                .apply(URI.create("http://127.0.0.1:" + closedPort),
                        "tok", List.of(new LocalDraft("a.tap.yml", "kind: source\n")));
        assertThat(outcome).isInstanceOf(ApplyOutcome.Unreachable.class);
    }

    // --- lifecycle: POST /api/pipelines/{id}:{verb} under /api, authenticated ----------------------

    @Test
    void lifecyclePostsToTheColonMethodPathWithTheBearerAndReturnsTheNewState() throws Exception {
        AtomicReference<CapturedRequest> seen = new AtomicReference<>();
        HttpServer server = apiServer("/api/pipelines/pl1:start", 200,
                "{\"pipelineId\":\"pl1\",\"targetState\":\"RUNNING\",\"revision\":\"rev-abc\"}", seen);
        try {
            LifecycleOutcome outcome =
                    new HttpControlPlaneClient().lifecycle(baseOf(server), "tok-abc", "pl1", "start");
            assertThat(outcome).isEqualTo(new LifecycleOutcome.Accepted("pl1", "RUNNING", "rev-abc"));
            assertThat(seen.get().method()).isEqualTo("POST");
            assertThat(seen.get().path()).isEqualTo("/api/pipelines/pl1:start");
            assertThat(seen.get().authorization()).isEqualTo("Bearer tok-abc");
        } finally {
            server.stop(0);
        }
    }

    @Test
    void lifecycleReturnsRejectedWithTheServerCodeAndMessageOnAConflict() throws Exception {
        HttpServer server = apiServer("/api/pipelines/pl1:pause", 409,
                "{\"code\":\"lifecycle.illegal-transition\",\"params\":{},\"message\":\"Not running.\"}",
                new AtomicReference<>());
        try {
            LifecycleOutcome outcome =
                    new HttpControlPlaneClient().lifecycle(baseOf(server), "tok", "pl1", "pause");
            assertThat(outcome).isEqualTo(
                    new LifecycleOutcome.Rejected("lifecycle.illegal-transition", "Not running."));
        } finally {
            server.stop(0);
        }
    }

    @Test
    void lifecycleReturnsUnreachableWhenTheServerIsDownWithoutThrowing() throws Exception {
        int closedPort;
        try (ServerSocket socket = new ServerSocket(0)) {
            closedPort = socket.getLocalPort();
        }
        LifecycleOutcome outcome = new HttpControlPlaneClient()
                .lifecycle(URI.create("http://127.0.0.1:" + closedPort), "tok", "pl1", "start");
        assertThat(outcome).isInstanceOf(LifecycleOutcome.Unreachable.class);
    }

    @Test
    void getReturnsFoundWithTheStoredArtifactAndSendsTheCredential() throws Exception {
        AtomicReference<CapturedRequest> seen = new AtomicReference<>();
        HttpServer server = apiServer("/api/artifacts/", 200,
                "{\"id\":\"src_kfk\",\"kind\":\"source\",\"canonicalForm\":\"kind: source\\nid: src_kfk\\n\"}", seen);
        try {
            GetOutcome outcome = new HttpControlPlaneClient().get(baseOf(server), "tok-xyz", "src_kfk");
            assertThat(outcome).isEqualTo(new GetOutcome.Found(
                    new RemoteArtifact("src_kfk", "source", "kind: source\nid: src_kfk\n")));
            assertThat(seen.get().method()).isEqualTo("GET");
            assertThat(seen.get().path()).isEqualTo("/api/artifacts/src_kfk");
            assertThat(seen.get().authorization()).isEqualTo("Bearer tok-xyz");
        } finally {
            server.stop(0);
        }
    }

    @Test
    void getReturnsAbsentOnA404() throws Exception {
        HttpServer server = apiServer("/api/artifacts/", 404, null, new AtomicReference<>());
        try {
            assertThat(new HttpControlPlaneClient().get(baseOf(server), "tok", "missing"))
                    .isInstanceOf(GetOutcome.Absent.class);
        } finally {
            server.stop(0);
        }
    }

    @Test
    void getReturnsRejectedWithTheServerCodeAndMessageOnACodedErrorStatus() throws Exception {
        // a non-404 error status (here 403 control.forbidden) is a coded refusal, distinct from Absent
        HttpServer server = apiServer("/api/artifacts/", 403,
                "{\"code\":\"control.forbidden\",\"params\":{},\"message\":\"You lack the grade.\"}",
                new AtomicReference<>());
        try {
            GetOutcome outcome = new HttpControlPlaneClient(
                    Duration.ofSeconds(5), HttpControlPlaneClient.HEAVY_TIMEOUT)
                    .get(baseOf(server), "tok", "src_kfk");
            assertThat(outcome).isEqualTo(new GetOutcome.Rejected("control.forbidden", "You lack the grade."));
        } finally {
            server.stop(0);
        }
    }

    @Test
    void getReturnsUnreachableWhenTheServerIsDownWithoutThrowing() throws Exception {
        int closedPort;
        try (ServerSocket socket = new ServerSocket(0)) {
            closedPort = socket.getLocalPort();
        }
        assertThat(new HttpControlPlaneClient().get(URI.create("http://127.0.0.1:" + closedPort), "tok", "x"))
                .isInstanceOf(GetOutcome.Unreachable.class);
    }

    @Test
    void listReturnsTheArtifactsAndSendsTheKindFilterAndCredential() throws Exception {
        AtomicReference<CapturedRequest> seen = new AtomicReference<>();
        HttpServer server = apiServer("/api/artifacts", 200,
                "{\"artifacts\":[{\"id\":\"src_kfk\",\"kind\":\"source\",\"canonicalForm\":\"kind: source\\n\"}]}", seen);
        try {
            ListOutcome outcome = new HttpControlPlaneClient().list(baseOf(server), "tok-1", "source");
            assertThat(outcome).isInstanceOf(ListOutcome.Listed.class);
            assertThat(((ListOutcome.Listed) outcome).artifacts())
                    .containsExactly(new RemoteArtifact("src_kfk", "source", "kind: source\n"));
            assertThat(seen.get().path()).isEqualTo("/api/artifacts");
            assertThat(seen.get().query()).isEqualTo("kind=source");
            assertThat(seen.get().authorization()).isEqualTo("Bearer tok-1");
        } finally {
            server.stop(0);
        }
    }

    @Test
    void connectorListReturnsTheConnectorsWithOriginTagsAndSendsTheCredential() throws Exception {
        AtomicReference<CapturedRequest> seen = new AtomicReference<>();
        HttpServer server = apiServer("/api/connectors", 200,
                "{\"connectors\":[{\"id\":\"mysql\",\"name\":\"MySQL\",\"group\":\"database\","
                        + "\"modes\":[\"snapshot\",\"cdc\"],\"sink\":true,\"origin\":\"bundled\"},"
                        + "{\"id\":\"acme\",\"name\":\"Acme\",\"group\":\"database\","
                        + "\"modes\":[\"snapshot\"],\"sink\":false,\"origin\":\"registered\"}]}", seen);
        try {
            ConnectorListOutcome outcome = new HttpControlPlaneClient().connectorList(baseOf(server), "tok-1");
            assertThat(outcome).isInstanceOf(ConnectorListOutcome.Listed.class);
            assertThat(((ConnectorListOutcome.Listed) outcome).connectors()).containsExactly(
                    new CatalogConnector("mysql", "MySQL", "database", List.of("snapshot", "cdc"), true, "bundled"),
                    new CatalogConnector("acme", "Acme", "database", List.of("snapshot"), false, "registered"));
            assertThat(seen.get().path()).isEqualTo("/api/connectors");
            assertThat(seen.get().authorization()).isEqualTo("Bearer tok-1");
        } finally {
            server.stop(0);
        }
    }

    @Test
    void connectorListReturnsRejectedWithTheServerCodeAndMessageOnACodedErrorStatus() throws Exception {
        HttpServer server = apiServer("/api/connectors", 403,
                "{\"code\":\"control.forbidden\",\"params\":{},\"message\":\"You lack the grade.\"}",
                new AtomicReference<>());
        try {
            ConnectorListOutcome outcome = new HttpControlPlaneClient().connectorList(baseOf(server), "tok");
            assertThat(outcome).isEqualTo(new ConnectorListOutcome.Rejected("control.forbidden", "You lack the grade."));
        } finally {
            server.stop(0);
        }
    }

    @Test
    void listWithoutAKindSendsNoQueryFilter() throws Exception {
        AtomicReference<CapturedRequest> seen = new AtomicReference<>();
        HttpServer server = apiServer("/api/artifacts", 200, "{\"artifacts\":[]}", seen);
        try {
            ListOutcome outcome = new HttpControlPlaneClient().list(baseOf(server), "tok", null);
            assertThat(outcome).isInstanceOf(ListOutcome.Listed.class);
            assertThat(((ListOutcome.Listed) outcome).artifacts()).isEmpty();
            assertThat(seen.get().query()).isNull();
        } finally {
            server.stop(0);
        }
    }

    @Test
    void listReturnsRejectedWithTheServerCodeAndMessageOnACodedErrorStatus() throws Exception {
        HttpServer server = apiServer("/api/artifacts", 403,
                "{\"code\":\"control.forbidden\",\"params\":{},\"message\":\"You lack the grade.\"}",
                new AtomicReference<>());
        try {
            ListOutcome outcome = new HttpControlPlaneClient().list(baseOf(server), "tok", null);
            assertThat(outcome).isEqualTo(new ListOutcome.Rejected("control.forbidden", "You lack the grade."));
        } finally {
            server.stop(0);
        }
    }

    @Test
    void listReturnsUnreachableWhenTheServerIsDownWithoutThrowing() throws Exception {
        int closedPort;
        try (ServerSocket socket = new ServerSocket(0)) {
            closedPort = socket.getLocalPort();
        }
        assertThat(new HttpControlPlaneClient().list(URI.create("http://127.0.0.1:" + closedPort), "tok", null))
                .isInstanceOf(ListOutcome.Unreachable.class);
    }

    // --- connection test: POST /api/connections:test, authenticated, decodes the structured report -----

    @Test
    void testPostsTheConnectionAndDecodesTheStructuredReport() throws Exception {
        AtomicReference<CapturedRequest> seen = new AtomicReference<>();
        HttpServer server = apiServer("/api/connections:test", 200,
                "{\"connectionId\":\"conn_ora\",\"connectorId\":\"oracle\",\"outcome\":\"PASSED\",\"checks\":["
                        + "{\"name\":\"ping\",\"status\":\"PASSED\",\"message\":null,\"reason\":null,"
                        + "\"solution\":null,\"connectorErrorCode\":null},"
                        + "{\"name\":\"version\",\"status\":\"WARNING\",\"message\":\"server is old\",\"reason\":null,"
                        + "\"solution\":null,\"connectorErrorCode\":null}],\"testedAt\":1752000000000}",
                seen);
        try {
            ConnectionTestOutcome outcome = new HttpControlPlaneClient()
                    .test(baseOf(server), "tok-abc", "conn_ora", "oracle", Map.of("host", "10.20.0.15"));

            assertThat(outcome).isInstanceOf(ConnectionTestOutcome.Tested.class);
            ConnectionReport report = ((ConnectionTestOutcome.Tested) outcome).report();
            assertThat(report.connectionId()).isEqualTo("conn_ora");
            assertThat(report.connectorId()).isEqualTo("oracle");
            assertThat(report.outcome()).isEqualTo("PASSED");
            assertThat(report.testedAt()).isEqualTo(1752000000000L);
            assertThat(report.checks()).containsExactly(
                    new ConnectionReport.Check("ping", "PASSED", null, null, null, null),
                    new ConnectionReport.Check("version", "WARNING", "server is old", null, null, null));

            // the request carried the connection id, its connector and settings under a bearer credential
            assertThat(seen.get().method()).isEqualTo("POST");
            assertThat(seen.get().path()).isEqualTo("/api/connections:test");
            assertThat(seen.get().authorization()).isEqualTo("Bearer tok-abc");
            Map<?, ?> sent = (Map<?, ?>) JsonReader.parse(seen.get().body());
            assertThat(sent.get("id")).isEqualTo("conn_ora");
            assertThat(sent.get("connectorId")).isEqualTo("oracle");
            assertThat(sent.get("settings")).isEqualTo(Map.of("host", "10.20.0.15"));
        } finally {
            server.stop(0);
        }
    }

    @Test
    void testReturnsRejectedWithTheServerCodeAndMessageOnACodedErrorStatus() throws Exception {
        HttpServer server = apiServer("/api/connections:test", 400,
                "{\"code\":\"control.malformed-request\",\"params\":{},\"message\":\"a connectorId is required\"}",
                new AtomicReference<>());
        try {
            ConnectionTestOutcome outcome = new HttpControlPlaneClient()
                    .test(baseOf(server), "tok", "conn_ora", "oracle", Map.of());
            assertThat(outcome).isEqualTo(
                    new ConnectionTestOutcome.Rejected("control.malformed-request", "a connectorId is required"));
        } finally {
            server.stop(0);
        }
    }

    @Test
    void testReturnsUnreachableWhenTheServerIsDownWithoutThrowing() throws Exception {
        int closedPort;
        try (ServerSocket socket = new ServerSocket(0)) {
            closedPort = socket.getLocalPort();
        }
        assertThat(new HttpControlPlaneClient()
                .test(URI.create("http://127.0.0.1:" + closedPort), "tok", "c", "oracle", Map.of()))
                .isInstanceOf(ConnectionTestOutcome.Unreachable.class);
    }

    // --- connection test result: GET /api/connections/{id}/test-result, decodes the stored report ---------

    @Test
    void testResultReturnsFoundWithTheStoredReportAndSendsTheCredential() throws Exception {
        AtomicReference<CapturedRequest> seen = new AtomicReference<>();
        HttpServer server = apiServer("/api/connections/", 200,
                "{\"connectionId\":\"conn_ora\",\"connectorId\":\"oracle\",\"outcome\":\"FAILED\",\"checks\":["
                        + "{\"name\":\"Login\",\"status\":\"FAILED\",\"message\":\"auth failed\",\"reason\":null,"
                        + "\"solution\":null,\"connectorErrorCode\":\"11000\"}],\"testedAt\":1752000000000}",
                seen);
        try {
            ConnectionTestResultOutcome outcome =
                    new HttpControlPlaneClient().testResult(baseOf(server), "tok-xyz", "conn_ora");

            assertThat(outcome).isInstanceOf(ConnectionTestResultOutcome.Found.class);
            ConnectionReport report = ((ConnectionTestResultOutcome.Found) outcome).report();
            assertThat(report.connectionId()).isEqualTo("conn_ora");
            assertThat(report.connectorId()).isEqualTo("oracle");
            assertThat(report.outcome()).isEqualTo("FAILED");
            assertThat(report.testedAt()).isEqualTo(1752000000000L);
            assertThat(report.checks()).containsExactly(
                    new ConnectionReport.Check("Login", "FAILED", "auth failed", null, null, "11000"));

            // the read is a GET to the connection's result path, under a bearer credential
            assertThat(seen.get().method()).isEqualTo("GET");
            assertThat(seen.get().path()).isEqualTo("/api/connections/conn_ora/test-result");
            assertThat(seen.get().authorization()).isEqualTo("Bearer tok-xyz");
        } finally {
            server.stop(0);
        }
    }

    @Test
    void testResultReturnsAbsentOnA404() throws Exception {
        // a 404 is "never tested", distinct from a coded refusal
        HttpServer server = apiServer("/api/connections/", 404, null, new AtomicReference<>());
        try {
            assertThat(new HttpControlPlaneClient().testResult(baseOf(server), "tok", "never_tested"))
                    .isInstanceOf(ConnectionTestResultOutcome.Absent.class);
        } finally {
            server.stop(0);
        }
    }

    @Test
    void testResultReturnsRejectedWithTheServerCodeAndMessageOnACodedErrorStatus() throws Exception {
        HttpServer server = apiServer("/api/connections/", 403,
                "{\"code\":\"control.forbidden\",\"params\":{},\"message\":\"You lack the grade.\"}",
                new AtomicReference<>());
        try {
            ConnectionTestResultOutcome outcome =
                    new HttpControlPlaneClient().testResult(baseOf(server), "tok", "conn_ora");
            assertThat(outcome).isEqualTo(
                    new ConnectionTestResultOutcome.Rejected("control.forbidden", "You lack the grade."));
        } finally {
            server.stop(0);
        }
    }

    @Test
    void testResultReturnsUnreachableWhenTheServerIsDownWithoutThrowing() throws Exception {
        int closedPort;
        try (ServerSocket socket = new ServerSocket(0)) {
            closedPort = socket.getLocalPort();
        }
        assertThat(new HttpControlPlaneClient()
                .testResult(URI.create("http://127.0.0.1:" + closedPort), "tok", "c"))
                .isInstanceOf(ConnectionTestResultOutcome.Unreachable.class);
    }

    // --- schema discovery: POST /api/connections:discover-schema, decodes the discovered model ---------

    private static final String SCHEMA_BODY =
            "{\"connectionId\":\"conn_ora\",\"connectorId\":\"oracle\",\"tables\":["
                    + "{\"name\":\"orders\",\"fields\":[{\"name\":\"id\",\"type\":\"NUMBER\"},"
                    + "{\"name\":\"note\",\"type\":null}],\"primaryKey\":[\"id\"],"
                    + "\"indexes\":[{\"name\":\"pk_orders\",\"fields\":[\"id\"],\"unique\":true}]}],"
                    + "\"discoveredAt\":1752000000000}";

    private static void assertDecodedSchema(ConnectionSchema schema) {
        assertThat(schema.connectionId()).isEqualTo("conn_ora");
        assertThat(schema.connectorId()).isEqualTo("oracle");
        assertThat(schema.discoveredAt()).isEqualTo(1752000000000L);
        assertThat(schema.tables()).containsExactly(new ConnectionSchema.Table(
                "orders",
                List.of(new ConnectionSchema.Field("id", "NUMBER"), new ConnectionSchema.Field("note", null)),
                List.of("id"),
                List.of(new ConnectionSchema.Index("pk_orders", List.of("id"), true))));
    }

    @Test
    void discoverSchemaPostsTheConnectionAndDecodesTheModel() throws Exception {
        AtomicReference<CapturedRequest> seen = new AtomicReference<>();
        HttpServer server = apiServer("/api/connections:discover-schema", 200, SCHEMA_BODY, seen);
        try {
            ConnectionDiscoverSchemaOutcome outcome = new HttpControlPlaneClient()
                    .discoverSchema(baseOf(server), "tok-abc", "conn_ora", "oracle", Map.of("host", "10.20.0.15"));

            assertThat(outcome).isInstanceOf(ConnectionDiscoverSchemaOutcome.Discovered.class);
            assertDecodedSchema(((ConnectionDiscoverSchemaOutcome.Discovered) outcome).schema());

            // the request carried the connection id, its connector and settings under a bearer credential
            assertThat(seen.get().method()).isEqualTo("POST");
            assertThat(seen.get().path()).isEqualTo("/api/connections:discover-schema");
            assertThat(seen.get().authorization()).isEqualTo("Bearer tok-abc");
            Map<?, ?> sent = (Map<?, ?>) JsonReader.parse(seen.get().body());
            assertThat(sent.get("id")).isEqualTo("conn_ora");
            assertThat(sent.get("connectorId")).isEqualTo("oracle");
            assertThat(sent.get("settings")).isEqualTo(Map.of("host", "10.20.0.15"));
        } finally {
            server.stop(0);
        }
    }

    @Test
    void discoverSchemaReturnsRejectedWithTheServerCodeAndMessageOnACodedErrorStatus() throws Exception {
        HttpServer server = apiServer("/api/connections:discover-schema", 403,
                "{\"code\":\"control.forbidden\",\"params\":{},\"message\":\"You lack the grade.\"}",
                new AtomicReference<>());
        try {
            ConnectionDiscoverSchemaOutcome outcome = new HttpControlPlaneClient()
                    .discoverSchema(baseOf(server), "tok", "conn_ora", "oracle", Map.of());
            assertThat(outcome).isEqualTo(
                    new ConnectionDiscoverSchemaOutcome.Rejected("control.forbidden", "You lack the grade."));
        } finally {
            server.stop(0);
        }
    }

    @Test
    void discoverSchemaReturnsUnreachableWhenTheServerIsDownWithoutThrowing() throws Exception {
        int closedPort;
        try (ServerSocket socket = new ServerSocket(0)) {
            closedPort = socket.getLocalPort();
        }
        assertThat(new HttpControlPlaneClient()
                .discoverSchema(URI.create("http://127.0.0.1:" + closedPort), "tok", "c", "oracle", Map.of()))
                .isInstanceOf(ConnectionDiscoverSchemaOutcome.Unreachable.class);
    }

    // --- schema read-back: GET /api/connections/{id}/schema, decodes the stored model -------------------

    @Test
    void schemaReturnsFoundWithTheStoredModelAndSendsTheCredential() throws Exception {
        AtomicReference<CapturedRequest> seen = new AtomicReference<>();
        HttpServer server = apiServer("/api/connections/", 200, SCHEMA_BODY, seen);
        try {
            ConnectionSchemaOutcome outcome =
                    new HttpControlPlaneClient().schema(baseOf(server), "tok-xyz", "conn_ora");

            assertThat(outcome).isInstanceOf(ConnectionSchemaOutcome.Found.class);
            assertDecodedSchema(((ConnectionSchemaOutcome.Found) outcome).schema());

            // the read is a GET to the connection's schema path, under a bearer credential
            assertThat(seen.get().method()).isEqualTo("GET");
            assertThat(seen.get().path()).isEqualTo("/api/connections/conn_ora/schema");
            assertThat(seen.get().authorization()).isEqualTo("Bearer tok-xyz");
        } finally {
            server.stop(0);
        }
    }

    @Test
    void schemaSkipsAMalformedTableEntryAndKeepsTheRest() throws Exception {
        // one nameless table entry among good ones is skipped — the same lenient policy a malformed
        // check gets in a test report — rather than discarding the whole model
        String body = "{\"connectionId\":\"conn_ora\",\"connectorId\":\"oracle\",\"tables\":["
                + "{\"fields\":[]},"
                + "{\"name\":\"orders\",\"fields\":[],\"primaryKey\":[],\"indexes\":[]}],"
                + "\"discoveredAt\":1}";
        HttpServer server = apiServer("/api/connections/", 200, body, new AtomicReference<>());
        try {
            ConnectionSchemaOutcome outcome = new HttpControlPlaneClient().schema(baseOf(server), "tok", "conn_ora");

            assertThat(outcome).isInstanceOf(ConnectionSchemaOutcome.Found.class);
            ConnectionSchema schema = ((ConnectionSchemaOutcome.Found) outcome).schema();
            assertThat(schema.tables()).extracting(ConnectionSchema.Table::name).containsExactly("orders");
        } finally {
            server.stop(0);
        }
    }

    @Test
    void schemaReturnsAbsentOnA404() throws Exception {
        // a 404 is "never discovered", distinct from a coded refusal
        HttpServer server = apiServer("/api/connections/", 404, null, new AtomicReference<>());
        try {
            assertThat(new HttpControlPlaneClient().schema(baseOf(server), "tok", "never_discovered"))
                    .isInstanceOf(ConnectionSchemaOutcome.Absent.class);
        } finally {
            server.stop(0);
        }
    }

    @Test
    void schemaReturnsRejectedWithTheServerCodeAndMessageOnACodedErrorStatus() throws Exception {
        HttpServer server = apiServer("/api/connections/", 403,
                "{\"code\":\"control.forbidden\",\"params\":{},\"message\":\"You lack the grade.\"}",
                new AtomicReference<>());
        try {
            ConnectionSchemaOutcome outcome = new HttpControlPlaneClient().schema(baseOf(server), "tok", "conn_ora");
            assertThat(outcome).isEqualTo(
                    new ConnectionSchemaOutcome.Rejected("control.forbidden", "You lack the grade."));
        } finally {
            server.stop(0);
        }
    }

    @Test
    void schemaReturnsUnreachableWhenTheServerIsDownWithoutThrowing() throws Exception {
        int closedPort;
        try (ServerSocket socket = new ServerSocket(0)) {
            closedPort = socket.getLocalPort();
        }
        assertThat(new HttpControlPlaneClient()
                .schema(URI.create("http://127.0.0.1:" + closedPort), "tok", "c"))
                .isInstanceOf(ConnectionSchemaOutcome.Unreachable.class);
    }

    // --- observation reads: GET /api/pipelines/{id}/{face} under /api, authenticated ---------------

    @Test
    void statusGetsTheStatusFaceWithTheBearerAndReturnsTheState() throws Exception {
        AtomicReference<CapturedRequest> seen = new AtomicReference<>();
        HttpServer server = apiServer("/api/pipelines/pl1/status", 200,
                "{\"pipelineId\":\"pl1\",\"state\":\"RUNNING\"}", seen);
        try {
            StatusOutcome outcome = new HttpControlPlaneClient().status(baseOf(server), "tok-abc", "pl1");
            assertThat(outcome).isEqualTo(new StatusOutcome.Found("pl1", "RUNNING"));
            assertThat(seen.get().method()).isEqualTo("GET");
            assertThat(seen.get().path()).isEqualTo("/api/pipelines/pl1/status");
            assertThat(seen.get().authorization()).isEqualTo("Bearer tok-abc");
        } finally {
            server.stop(0);
        }
    }

    @Test
    void statusCarriesTheCodedFailureOfAPipelineWhoseJobDied() throws Exception {
        AtomicReference<CapturedRequest> seen = new AtomicReference<>();
        HttpServer server = apiServer("/api/pipelines/pl1/status", 200,
                "{\"pipelineId\":\"pl1\",\"state\":\"FAILED\",\"failure\":{\"code\":\"engine.job-failed\","
                        + "\"params\":{\"cause\":\"sink refused\"},"
                        + "\"message\":\"Pipeline pl1 stopped because its job failed: sink refused.\"}}",
                seen);
        try {
            StatusOutcome outcome = new HttpControlPlaneClient().status(baseOf(server), "tok-abc", "pl1");

            assertThat(outcome).isEqualTo(new StatusOutcome.Found("pl1", "FAILED", "engine.job-failed",
                    "Pipeline pl1 stopped because its job failed: sink refused."));
        } finally {
            server.stop(0);
        }
    }

    @Test
    void statusOfAHealthyPipelineCarriesNoFailure() throws Exception {
        AtomicReference<CapturedRequest> seen = new AtomicReference<>();
        HttpServer server = apiServer("/api/pipelines/pl1/status", 200,
                "{\"pipelineId\":\"pl1\",\"state\":\"RUNNING\"}", seen);
        try {
            StatusOutcome outcome = new HttpControlPlaneClient().status(baseOf(server), "tok-abc", "pl1");

            assertThat(((StatusOutcome.Found) outcome).failureCode()).isNull();
        } finally {
            server.stop(0);
        }
    }

    @Test
    void statusOfAFailedPipelineSurvivesAFailureBlockWithNoMessage() throws Exception {
        // A reply that names the code but carries no rendered sentence must still report the code rather
        // than dropping the whole status to unreachable: the code alone is the diagnosis worth keeping.
        AtomicReference<CapturedRequest> seen = new AtomicReference<>();
        HttpServer server = apiServer("/api/pipelines/pl1/status", 200,
                "{\"pipelineId\":\"pl1\",\"state\":\"FAILED\",\"failure\":{\"code\":\"engine.job-failed\"}}", seen);
        try {
            StatusOutcome outcome = new HttpControlPlaneClient().status(baseOf(server), "tok-abc", "pl1");

            StatusOutcome.Found found = (StatusOutcome.Found) outcome;
            assertThat(found.failureCode()).isEqualTo("engine.job-failed");
            assertThat(found.failureMessage()).isNotNull();
        } finally {
            server.stop(0);
        }
    }

    @Test
    void statusReturnsRejectedWithTheServerCodeAndMessageOnACodedError() throws Exception {
        HttpServer server = apiServer("/api/pipelines/ghost/status", 404,
                "{\"code\":\"monitor.no-observation\",\"params\":{\"pipeline\":\"ghost\"},"
                        + "\"message\":\"No observation is available for pipeline ghost.\"}",
                new AtomicReference<>());
        try {
            StatusOutcome outcome = new HttpControlPlaneClient().status(baseOf(server), "tok", "ghost");
            assertThat(outcome).isEqualTo(new StatusOutcome.Rejected(
                    "monitor.no-observation", "No observation is available for pipeline ghost."));
        } finally {
            server.stop(0);
        }
    }

    @Test
    void statusReturnsUnreachableWhenTheServerIsDownWithoutThrowing() throws Exception {
        int closedPort;
        try (ServerSocket socket = new ServerSocket(0)) {
            closedPort = socket.getLocalPort();
        }
        assertThat(new HttpControlPlaneClient().status(URI.create("http://127.0.0.1:" + closedPort), "tok", "pl1"))
                .isInstanceOf(StatusOutcome.Unreachable.class);
    }

    @Test
    void metricsGetsTheMetricsFaceAndReturnsTheOpenMap() throws Exception {
        AtomicReference<CapturedRequest> seen = new AtomicReference<>();
        HttpServer server = apiServer("/api/pipelines/pl1/metrics", 200,
                "{\"pipelineId\":\"pl1\",\"metrics\":{\"recordCount\":42,\"errorCount\":0}}", seen);
        try {
            MetricsOutcome outcome = new HttpControlPlaneClient().metrics(baseOf(server), "tok-abc", "pl1");
            assertThat(outcome)
                    .isEqualTo(new MetricsOutcome.Found("pl1", Map.of("recordCount", 42L, "errorCount", 0L)));
            assertThat(seen.get().method()).isEqualTo("GET");
            assertThat(seen.get().path()).isEqualTo("/api/pipelines/pl1/metrics");
            assertThat(seen.get().authorization()).isEqualTo("Bearer tok-abc");
        } finally {
            server.stop(0);
        }
    }

    @Test
    void metricsCapturesPerTableOffsetFromTheOpenMap() throws Exception {
        // perTableOffset is a sibling of the metrics map, not a cell inside it: a source position is a
        // string and every metrics cell is a number, so the two never share a container.
        HttpServer server = apiServer("/api/pipelines/pl1/metrics", 200,
                "{\"pipelineId\":\"pl1\",\"metrics\":{\"recordCount\":6},\"perTableOffset\":{\"orders\":\"w7\"}}",
                new AtomicReference<>());
        try {
            MetricsOutcome outcome = new HttpControlPlaneClient().metrics(baseOf(server), "tok", "pl1");
            assertThat(outcome).isEqualTo(new MetricsOutcome.Found(
                    "pl1", Map.of("recordCount", 6L), Map.of("orders", "w7")));
        } finally {
            server.stop(0);
        }
    }

    @Test
    void metricsReturnsAnEmptyMapWhenNoMetricSourceIsWiredYet() throws Exception {
        // Honest-empty: no metric source is wired, so the open map is empty — never faked.
        HttpServer server = apiServer("/api/pipelines/pl1/metrics", 200,
                "{\"pipelineId\":\"pl1\",\"metrics\":{}}", new AtomicReference<>());
        try {
            MetricsOutcome outcome = new HttpControlPlaneClient().metrics(baseOf(server), "tok", "pl1");
            assertThat(outcome).isEqualTo(new MetricsOutcome.Found("pl1", Map.of()));
        } finally {
            server.stop(0);
        }
    }

    @Test
    void metricsReturnsUnreachableWhenTheServerIsDownWithoutThrowing() throws Exception {
        int closedPort;
        try (ServerSocket socket = new ServerSocket(0)) {
            closedPort = socket.getLocalPort();
        }
        assertThat(new HttpControlPlaneClient().metrics(URI.create("http://127.0.0.1:" + closedPort), "tok", "pl1"))
                .isInstanceOf(MetricsOutcome.Unreachable.class);
    }

    @Test
    void snapshotGetsThePerTableProgressIncludingAnUnavailableTotal() throws Exception {
        AtomicReference<CapturedRequest> seen = new AtomicReference<>();
        HttpServer server = apiServer("/api/pipelines/pl1/snapshot", 200,
                "{\"pipelineId\":\"pl1\",\"snapshot\":{"
                        + "\"orders\":{\"rowsDone\":10,\"rowsTotal\":100,\"donePct\":10},"
                        + "\"events\":{\"rowsDone\":5,\"rowsTotal\":null,\"donePct\":null}}}", seen);
        try {
            SnapshotOutcome outcome = new HttpControlPlaneClient().snapshot(baseOf(server), "tok", "pl1");
            assertThat(outcome).isEqualTo(new SnapshotOutcome.Found("pl1", Map.of(
                    "orders", new RemoteTableSnapshot(10, 100L, 10),
                    "events", new RemoteTableSnapshot(5, null, null))));
            assertThat(seen.get().method()).isEqualTo("GET");
            assertThat(seen.get().path()).isEqualTo("/api/pipelines/pl1/snapshot");
        } finally {
            server.stop(0);
        }
    }

    @Test
    void snapshotReturnsRejectedWithTheServerCodeAndMessageOnACodedError() throws Exception {
        HttpServer server = apiServer("/api/pipelines/pl1/snapshot", 403,
                "{\"code\":\"control.forbidden\",\"params\":{},\"message\":\"You lack the grade.\"}",
                new AtomicReference<>());
        try {
            SnapshotOutcome outcome = new HttpControlPlaneClient().snapshot(baseOf(server), "tok", "pl1");
            assertThat(outcome).isEqualTo(new SnapshotOutcome.Rejected("control.forbidden", "You lack the grade."));
        } finally {
            server.stop(0);
        }
    }

    @Test
    void snapshotReturnsUnreachableWhenTheServerIsDownWithoutThrowing() throws Exception {
        int closedPort;
        try (ServerSocket socket = new ServerSocket(0)) {
            closedPort = socket.getLocalPort();
        }
        assertThat(new HttpControlPlaneClient().snapshot(URI.create("http://127.0.0.1:" + closedPort), "tok", "pl1"))
                .isInstanceOf(SnapshotOutcome.Unreachable.class);
    }

    @Test
    void logsGetsThePipelineTailOldestToNewest() throws Exception {
        AtomicReference<CapturedRequest> seen = new AtomicReference<>();
        HttpServer server = apiServer("/api/pipelines/pl1/logs", 200,
                "{\"pipelineId\":\"pl1\",\"lines\":["
                        + "{\"timestampMillis\":1700000000000,\"level\":\"INFO\",\"message\":\"submitted job\"},"
                        + "{\"timestampMillis\":1700000000100,\"level\":\"WARN\",\"message\":\"slow tick\"}]}", seen);
        try {
            LogsOutcome outcome = new HttpControlPlaneClient().logs(baseOf(server), "tok", "pl1");
            assertThat(outcome).isEqualTo(new LogsOutcome.Found("pl1", List.of(
                    new RemoteLogLine(1700000000000L, "INFO", "submitted job"),
                    new RemoteLogLine(1700000000100L, "WARN", "slow tick"))));
            assertThat(seen.get().method()).isEqualTo("GET");
            assertThat(seen.get().path()).isEqualTo("/api/pipelines/pl1/logs");
        } finally {
            server.stop(0);
        }
    }

    @Test
    void logsWithNoLinesIsABenignEmptyFound() throws Exception {
        HttpServer server = apiServer("/api/pipelines/pl1/logs", 200,
                "{\"pipelineId\":\"pl1\",\"lines\":[]}", new AtomicReference<>());
        try {
            assertThat(new HttpControlPlaneClient().logs(baseOf(server), "tok", "pl1"))
                    .isEqualTo(new LogsOutcome.Found("pl1", List.of()));
        } finally {
            server.stop(0);
        }
    }

    @Test
    void logsDropsAMalformedLineButKeepsTheValidOnes() throws Exception {
        // A malformed line entry (missing fields) is dropped rather than crashing the read; valid lines in the
        // same tail are still returned.
        HttpServer server = apiServer("/api/pipelines/pl1/logs", 200,
                "{\"pipelineId\":\"pl1\",\"lines\":["
                        + "{\"timestampMillis\":1,\"level\":\"INFO\",\"message\":\"kept\"},"
                        + "{\"level\":\"WARN\"}]}", new AtomicReference<>());
        try {
            assertThat(new HttpControlPlaneClient().logs(baseOf(server), "tok", "pl1"))
                    .isEqualTo(new LogsOutcome.Found("pl1", List.of(new RemoteLogLine(1L, "INFO", "kept"))));
        } finally {
            server.stop(0);
        }
    }

    @Test
    void logsReturnsRejectedWithTheServerCodeAndMessageOnACodedError() throws Exception {
        HttpServer server = apiServer("/api/pipelines/pl1/logs", 403,
                "{\"code\":\"control.forbidden\",\"params\":{},\"message\":\"You lack the grade.\"}",
                new AtomicReference<>());
        try {
            assertThat(new HttpControlPlaneClient().logs(baseOf(server), "tok", "pl1"))
                    .isEqualTo(new LogsOutcome.Rejected("control.forbidden", "You lack the grade."));
        } finally {
            server.stop(0);
        }
    }

    @Test
    void logsReturnsUnreachableOnAWrongShapeTwoHundred() throws Exception {
        // A 200 whose body is not the expected shape (a proxy splash, a wrong endpoint) is unusable: it maps
        // to unreachable, not a falsely-successful empty tail.
        HttpServer server = apiServer("/api/pipelines/pl1/logs", 200, "{\"unexpected\":true}", new AtomicReference<>());
        try {
            assertThat(new HttpControlPlaneClient().logs(baseOf(server), "tok", "pl1"))
                    .isInstanceOf(LogsOutcome.Unreachable.class);
        } finally {
            server.stop(0);
        }
    }

    @Test
    void logsReturnsUnreachableWhenTheServerIsDownWithoutThrowing() throws Exception {
        int closedPort;
        try (ServerSocket socket = new ServerSocket(0)) {
            closedPort = socket.getLocalPort();
        }
        assertThat(new HttpControlPlaneClient().logs(URI.create("http://127.0.0.1:" + closedPort), "tok", "pl1"))
                .isInstanceOf(LogsOutcome.Unreachable.class);
    }

    @Test
    void snapshotReturnsAnEmptyMapOutsideASnapshotPhase() throws Exception {
        // Honest-empty: outside a snapshot phase the per-table map is empty — a legitimate Found, not a miss.
        HttpServer server = apiServer("/api/pipelines/pl1/snapshot", 200,
                "{\"pipelineId\":\"pl1\",\"snapshot\":{}}", new AtomicReference<>());
        try {
            SnapshotOutcome outcome = new HttpControlPlaneClient().snapshot(baseOf(server), "tok", "pl1");
            assertThat(outcome).isEqualTo(new SnapshotOutcome.Found("pl1", Map.of()));
        } finally {
            server.stop(0);
        }
    }

    // --- a well-formed 200 that is not a usable read reply resolves to unreachable (never a fabricated Found) ---

    @Test
    void statusTreatsAShapeWrong200AsUnreachableNotAFabricatedState() throws Exception {
        // A 200 whose body is valid JSON but not a usable status reply (a reverse proxy / non-Tapstate answer)
        // must never fabricate a state — it resolves to unreachable, upholding the never-throw seam.
        HttpServer server = apiServer("/api/pipelines/pl1/status", 200, "{\"foo\":\"bar\"}", new AtomicReference<>());
        try {
            assertThat(new HttpControlPlaneClient().status(baseOf(server), "tok", "pl1"))
                    .isInstanceOf(StatusOutcome.Unreachable.class);
        } finally {
            server.stop(0);
        }
    }

    @Test
    void statusTreatsAFailureBlockMissingItsCodeAsUnreachableNotAHealthyState() throws Exception {
        // A failure block present but missing its code is a regression of the status contract (the e2e
        // reader over this identical shape throws for exactly this reason) -- it must not silently decode
        // the same way an absent failure block does, which is precisely the encoding of a healthy pipeline.
        HttpServer server = apiServer("/api/pipelines/pl1/status", 200,
                "{\"pipelineId\":\"pl1\",\"state\":\"FAILED\",\"failure\":{\"params\":{\"cause\":\"sink refused\"}}}",
                new AtomicReference<>());
        try {
            assertThat(new HttpControlPlaneClient().status(baseOf(server), "tok", "pl1"))
                    .isInstanceOf(StatusOutcome.Unreachable.class);
        } finally {
            server.stop(0);
        }
    }

    @Test
    void statusTreatsANonObjectFailureBlockAsUnreachableNotAHealthyState() throws Exception {
        HttpServer server = apiServer("/api/pipelines/pl1/status", 200,
                "{\"pipelineId\":\"pl1\",\"state\":\"FAILED\",\"failure\":\"engine.job-failed\"}",
                new AtomicReference<>());
        try {
            assertThat(new HttpControlPlaneClient().status(baseOf(server), "tok", "pl1"))
                    .isInstanceOf(StatusOutcome.Unreachable.class);
        } finally {
            server.stop(0);
        }
    }

    @Test
    void metricsTreatsAShapeWrong200AsUnreachableNotAFabricatedMap() throws Exception {
        // A 200 with a non-object metrics field is not a usable metrics reply; it must not be read as an
        // empty (honest-empty is a real object), so it resolves to unreachable rather than a faked empty map.
        HttpServer server = apiServer("/api/pipelines/pl1/metrics", 200,
                "{\"pipelineId\":\"pl1\",\"metrics\":\"nope\"}", new AtomicReference<>());
        try {
            assertThat(new HttpControlPlaneClient().metrics(baseOf(server), "tok", "pl1"))
                    .isInstanceOf(MetricsOutcome.Unreachable.class);
        } finally {
            server.stop(0);
        }
    }

    @Test
    void snapshotTreatsAShapeWrong200AsUnreachableNotAFabricatedMap() throws Exception {
        HttpServer server = apiServer("/api/pipelines/pl1/snapshot", 200, "{\"pipelineId\":\"pl1\"}",
                new AtomicReference<>());
        try {
            assertThat(new HttpControlPlaneClient().snapshot(baseOf(server), "tok", "pl1"))
                    .isInstanceOf(SnapshotOutcome.Unreachable.class);
        } finally {
            server.stop(0);
        }
    }
}
