package io.tapstate.cli;

import io.tapstate.core.common.JsonReader;
import io.tapstate.control.client.ControlResponse;
import io.tapstate.control.client.HttpControlClient;
import io.tapstate.control.client.RequestBudget;

import java.io.IOException;
import java.net.URI;
import java.net.URLEncoder;
import java.net.http.HttpClient;
import java.net.http.HttpConnectTimeoutException;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.net.http.HttpTimeoutException;
import java.time.Instant;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.net.http.WebSocket;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Base64;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BooleanSupplier;
import java.util.function.Consumer;

/**
 * The production {@link ControlPlaneClient}, backed by the JDK HTTP client (no third-party
 * dependency, so rule R6 holds and the native image needs no extra metadata). Probes and calls are
 * given a short connect and request timeout so an unreachable seed fails fast, and every failure mode —
 * connection refused, timeout, unknown host, or a malformed / unsupported base URL — resolves to a
 * "not healthy" / "unreachable" result rather than throwing, so the caller can walk the seed list and
 * render outcomes without try/catch.
 */
final class HttpControlPlaneClient implements ControlPlaneClient {

    /**
     * The probe / connect budget: short enough that an unreachable seed does not stall the connect walk,
     * and the read budget for the light verbs, whose work is a lookup the server answers at once.
     */
    static final Duration PROBE_TIMEOUT = Duration.ofSeconds(3);

    /**
     * The read budget for the heavy verbs — {@code connections:test} and {@code discover-schema} drive a
     * live round-trip to a remote database, and {@code connectors:register} uploads a multi-MB jar the
     * server then class-loads. The flat probe budget starved them; register scales further with size (see
     * {@link #registerTimeout(int)}). A slow answer within this window means "busy", not "unreachable".
     */
    static final Duration HEAVY_TIMEOUT = Duration.ofSeconds(30);

    /** A conservative floor upload rate (~1 MB/s); below it even a healthy server is presumed too slow. */
    private static final long REGISTER_FLOOR_BYTES_PER_SECOND = 1_000_000;

    private final Duration probeTimeout;
    private final Duration heavyTimeout;
    private HttpClient httpClient;
    private final HttpControlClient sharedClient;

    HttpControlPlaneClient() {
        this(PROBE_TIMEOUT, HEAVY_TIMEOUT);
    }

    /**
     * A seam for tests to shrink the budgets so a deliberately slow server trips the timeout path in
     * milliseconds rather than the production tens of seconds. Production always uses the no-arg form.
     */
    HttpControlPlaneClient(Duration probeTimeout, Duration heavyTimeout) {
        this.probeTimeout = probeTimeout;
        this.heavyTimeout = heavyTimeout;
        this.sharedClient = new HttpControlClient(probeTimeout, heavyTimeout);
    }

    /**
     * The register read budget for an upload of {@code artifactBytes}: the heavy floor plus a second per
     * ~megabyte, so a 47MB connector jar is not judged unreachable while it is still legitimately uploading.
     */
    Duration registerTimeout(int artifactBytes) {
        return heavyTimeout.plusSeconds(Math.max(0, artifactBytes) / REGISTER_FLOOR_BYTES_PER_SECOND);
    }

    /** The client is built lazily so constructing a REPL that never connects costs nothing. */
    private HttpClient client() {
        if (httpClient == null) {
            httpClient = HttpClient.newBuilder().connectTimeout(probeTimeout).build();
        }
        return httpClient;
    }

    @Override
    public boolean isHealthy(URI baseUrl) {
        try {
            HttpRequest request = HttpRequest.newBuilder(endpoint(baseUrl, "/healthz"))
                    .timeout(probeTimeout)
                    .GET()
                    .build();
            HttpResponse<Void> response = send(request, HttpResponse.BodyHandlers.discarding());
            return response.statusCode() == 200;
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            return false;
        } catch (IOException | RuntimeException e) {
            // A malformed / unsupported base URL (e.g. no host) throws IllegalArgumentException when
            // the request is built; that, like an I/O failure, is simply "not healthy", never thrown.
            return false;
        }
    }

    @Override
    public DiscoveryOutcome discover(URI baseUrl) {
        try {
            HttpRequest request = HttpRequest.newBuilder(endpoint(baseUrl, "/.well-known/tapstate"))
                    .timeout(probeTimeout)
                    .GET()
                    .build();
            HttpResponse<String> response = send(request, HttpResponse.BodyHandlers.ofString(StandardCharsets.UTF_8));
            if (response.statusCode() == 200) {
                Object parsed;
                try {
                    parsed = JsonReader.parse(response.body());
                } catch (RuntimeException malformed) {
                    return new DiscoveryOutcome.Invalid("response-body");
                }
                if (!(parsed instanceof Map<?, ?> fields)) {
                    return new DiscoveryOutcome.Invalid("response-shape");
                }
                try {
                    return new DiscoveryOutcome.Discovered(
                            stringOrNull(fields.get("issuer")),
                            stringOrNull(fields.get("clusterId")),
                            stringOrNull(fields.get("apiVersion")),
                            requiredStringList(fields.get("authModes")));
                } catch (IllegalArgumentException invalid) {
                    return new DiscoveryOutcome.Invalid("response-contract");
                }
            }
            Rejection rejected = rejection(response.body(), "Issuer discovery was refused by the server.");
            return new DiscoveryOutcome.Rejected(rejected.code(), rejected.message());
        } catch (InterruptedException interrupted) {
            Thread.currentThread().interrupt();
            return new DiscoveryOutcome.Unreachable();
        } catch (IOException | RuntimeException failure) {
            return new DiscoveryOutcome.Unreachable();
        }
    }

    @Override
    public LoginOutcome login(URI baseUrl, String username, String password) {
        try {
            Map<String, String> payload = new LinkedHashMap<>();
            payload.put("username", username);
            payload.put("password", password);
            HttpRequest request = HttpRequest.newBuilder(endpoint(baseUrl, "/auth/login"))
                    .timeout(probeTimeout)
                    .header("Content-Type", "application/json")
                    .POST(HttpRequest.BodyPublishers.ofString(JsonOut.write(payload), StandardCharsets.UTF_8))
                    .build();
            HttpResponse<String> response =
                    send(request, HttpResponse.BodyHandlers.ofString(StandardCharsets.UTF_8));
            if (response.statusCode() == 200) {
                String token = stringField(response.body(), "token");
                return token == null || token.isBlank()
                        ? new LoginOutcome.Unreachable()   // a 200 with no token is not a usable success
                        : new LoginOutcome.Success(token);
            }
            Rejection r = rejection(response.body(), "Login was refused by the server.");
            return new LoginOutcome.Rejected(r.code(), r.message());
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            return new LoginOutcome.Unreachable();
        } catch (IOException | RuntimeException e) {
            return new LoginOutcome.Unreachable();
        }
    }

    @Override
    public DataBrowserOutcome.Collections collections(URI baseUrl, String credential, String sourceId) {
        ControlResponse response = sharedClient.get(
                baseUrl, credential, "/api/sources/" + urlSegment(sourceId) + "/collections");
        return switch (response) {
            case ControlResponse.Success success -> {
                List<String> names = names(success.body());
                yield names == null
                        ? new DataBrowserOutcome.Collections.Unreachable()
                        : new DataBrowserOutcome.Collections.Listed(names);
            }
            case ControlResponse.Rejected rejected ->
                    new DataBrowserOutcome.Collections.Rejected(rejected.code(), rejected.message());
            case ControlResponse.Unreachable ignored -> new DataBrowserOutcome.Collections.Unreachable();
        };
    }

    @Override
    public DataBrowserOutcome.Stats stats(
            URI baseUrl, String credential, String sourceId, String collection) {
        ControlResponse response = sharedClient.get(baseUrl, credential,
                "/api/sources/" + urlSegment(sourceId) + "/collections/" + urlSegment(collection) + "/stats");
        return switch (response) {
            case ControlResponse.Success success -> {
                if (!(success.body() instanceof Map<?, ?> reported)) {
                    yield new DataBrowserOutcome.Stats.Unreachable();
                }
                yield new DataBrowserOutcome.Stats.Reported(
                        count(reported.get("numOfRows")),
                        count(reported.get("storageSize")),
                        count(reported.get("avgObjSize")));
            }
            case ControlResponse.Rejected rejected ->
                    new DataBrowserOutcome.Stats.Rejected(rejected.code(), rejected.message());
            case ControlResponse.Unreachable ignored -> new DataBrowserOutcome.Stats.Unreachable();
        };
    }

    @Override
    public DataBrowserOutcome.Find find(URI baseUrl, String credential, String sourceId, String collection,
                                        Object filter, DataBrowserCall.Order sort, Integer limit) {
        // Only what was asked for. An absent key is the request — no filter reads every row, no order
        // leaves the order to the database — and sending a null under the key instead says something
        // different to a face that reads present-and-null apart from absent.
        Map<String, Object> body = new LinkedHashMap<>();
        if (filter != null) {
            body.put("filter", filter);
        }
        if (sort != null) {
            Map<String, Object> order = new LinkedHashMap<>();
            order.put("field", sort.field());
            order.put("dir", sort.dir());
            body.put("sort", order);
        }
        if (limit != null) {
            body.put("limit", limit);
        }
        ControlResponse response = sharedClient.post(baseUrl, credential,
                "/api/sources/" + urlSegment(sourceId) + "/collections/" + urlSegment(collection) + ":find",
                body, RequestBudget.HEAVY);
        return switch (response) {
            case ControlResponse.Success success -> {
                DataBrowserOutcome.Find.Read read = read(success.body());
                yield read == null ? new DataBrowserOutcome.Find.Unreachable() : read;
            }
            case ControlResponse.Rejected rejected ->
                    new DataBrowserOutcome.Find.Rejected(rejected.code(), rejected.message());
            case ControlResponse.Unreachable ignored -> new DataBrowserOutcome.Find.Unreachable();
        };
    }

    /**
     * The collection names from a 200 body, or null when the body is not shaped like that answer.
     *
     * <p>Each entry says more than its name — what class of collection it is, the fields discovery
     * found, the text whoever declared it wrote — and this shell reads only the name back out, because
     * that is all it prints. What the rest is for is a caller that has to decide what to read next
     * without a person looking at the screen.
     */
    private static List<String> names(Object body) {
        if (!(body instanceof Map<?, ?> m && m.get("collections") instanceof List<?> listed)) {
            return null;
        }
        List<String> names = new ArrayList<>(listed.size());
        for (Object entry : listed) {
            if (entry instanceof Map<?, ?> described && described.get("name") instanceof String text) {
                names.add(text);
            }
        }
        return names;
    }

    /**
     * The preview from a 200 body, or null when the body is not shaped like one. {@code moreAvailable}
     * has no fallback on purpose: read as false when it is missing, a truncated preview would render as
     * a whole collection, which is the one thing this face carries it to prevent.
     */
    private static DataBrowserOutcome.Find.Read read(Object body) {
        if (!(body instanceof Map<?, ?> m
                && m.get("rows") instanceof List<?> listed
                && m.get("moreAvailable") instanceof Boolean more)) {
            return null;
        }
        List<Map<String, Object>> rows = new ArrayList<>(listed.size());
        for (Object row : listed) {
            if (row instanceof Map<?, ?> fields) {
                Map<String, Object> copy = new LinkedHashMap<>();
                fields.forEach((name, value) -> copy.put(String.valueOf(name), value));
                rows.add(copy);
            }
        }
        return new DataBrowserOutcome.Find.Read(rows, count(m.get("approximateTotal")), more);
    }

    /** One reported count, or null when the server reported none — never zero standing in for absent. */
    private static Long count(Object reported) {
        return reported instanceof Number number ? number.longValue() : null;
    }

    @Override
    public TokenCreateOutcome tokenCreate(URI baseUrl, String credential, String scope) {
        ControlResponse response = sharedClient.post(
                baseUrl, credential, "/api/tokens", Map.of("scope", scope), RequestBudget.LIGHT);
        return switch (response) {
            case ControlResponse.Success success -> {
                RemoteCreatedToken created = createdToken(success.body());
                yield created == null
                        ? new TokenCreateOutcome.Unreachable()
                        : new TokenCreateOutcome.Issued(created);
            }
            case ControlResponse.Rejected rejected ->
                    new TokenCreateOutcome.Rejected(rejected.code(), rejected.message());
            case ControlResponse.Unreachable ignored -> new TokenCreateOutcome.Unreachable();
        };
    }

    @Override
    public TokenListOutcome tokenList(URI baseUrl, String credential) {
        ControlResponse response = sharedClient.get(baseUrl, credential, "/api/tokens");
        return switch (response) {
            case ControlResponse.Success success -> new TokenListOutcome.Listed(tokens(success.body()));
            case ControlResponse.Rejected rejected ->
                    new TokenListOutcome.Rejected(rejected.code(), rejected.message());
            case ControlResponse.Unreachable ignored -> new TokenListOutcome.Unreachable();
        };
    }

    @Override
    public TokenRevokeOutcome tokenRevoke(URI baseUrl, String credential, String tokenId) {
        ControlResponse response = sharedClient.post(
                baseUrl, credential, "/api/tokens/" + urlSegment(tokenId) + ":revoke", null, RequestBudget.LIGHT);
        return switch (response) {
            case ControlResponse.Success ignored -> new TokenRevokeOutcome.Revoked();
            case ControlResponse.Rejected rejected ->
                    new TokenRevokeOutcome.Rejected(rejected.code(), rejected.message());
            case ControlResponse.Unreachable ignored -> new TokenRevokeOutcome.Unreachable();
        };
    }

    @Override
    public void close() {
        sharedClient.close();
    }

    @Override
    public ApplyOutcome apply(URI baseUrl, String credential, List<LocalDraft> drafts) {
        try {
            HttpRequest request = authed(baseUrl, "/api/artifacts:apply", credential)
                    .header("Content-Type", "application/json")
                    .POST(HttpRequest.BodyPublishers.ofString(applyBody(drafts), StandardCharsets.UTF_8))
                    .build();
            HttpResponse<String> response =
                    send(request, HttpResponse.BodyHandlers.ofString(StandardCharsets.UTF_8));
            if (response.statusCode() == 200) {
                return applied(response.body());
            }
            Rejection r = rejection(response.body(), "The server refused the apply.");
            return new ApplyOutcome.Rejected(r.code(), r.message(), r.params());
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            return new ApplyOutcome.Unreachable();
        } catch (IOException | RuntimeException e) {
            return new ApplyOutcome.Unreachable();
        }
    }

    @Override
    public GetOutcome get(URI baseUrl, String credential, String id) {
        try {
            HttpRequest request = authed(baseUrl, "/api/artifacts/" + urlSegment(id), credential).GET().build();
            HttpResponse<String> response =
                    send(request, HttpResponse.BodyHandlers.ofString(StandardCharsets.UTF_8));
            int status = response.statusCode();
            if (status == 200) {
                RemoteArtifact artifact = remoteArtifact(response.body());
                // a 200 that is not a usable artifact (a proxy / non-Tapstate reply) is treated as unreachable
                return artifact == null ? new GetOutcome.Unreachable() : new GetOutcome.Found(artifact);
            }
            if (status == 404) {
                return new GetOutcome.Absent();
            }
            Rejection r = rejection(response.body(), "The server refused the read.");
            return new GetOutcome.Rejected(r.code(), r.message());
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            return new GetOutcome.Unreachable();
        } catch (IOException | RuntimeException e) {
            return new GetOutcome.Unreachable();
        }
    }

    @Override
    public DeleteOutcome delete(URI baseUrl, String credential, String id, String expectedContentHash) {
        try {
            HttpRequest.Builder builder = authed(
                    baseUrl, "/api/artifacts/" + urlSegment(id), credential);
            // Send no If-Match at all rather than an empty one when the caller supplied no precondition:
            // the server tells "you sent none" (428) apart from "yours is stale" (412), and an empty
            // header would turn the first into the second.
            if (expectedContentHash != null) {
                builder.header("If-Match", "\"" + expectedContentHash + "\"");
            }
            HttpResponse<String> response = send(
                    builder.DELETE().build(), HttpResponse.BodyHandlers.ofString(StandardCharsets.UTF_8));
            if (response.statusCode() / 100 == 2) {
                return new DeleteOutcome.Removed(id);
            }
            return rejectedDelete(response.body());
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            return new DeleteOutcome.Unreachable();
        } catch (IOException | RuntimeException e) {
            return new DeleteOutcome.Unreachable();
        }
    }

    /**
     * Decodes a refused removal, keeping the named parameters as well as the message. The two refusal
     * grounds each carry what the caller has to act on — who still references the resource, and what state
     * the pipeline is really in — so dropping the parameters would leave a sentence and no next step.
     */
    private static DeleteOutcome rejectedDelete(String body) {
        Map<String, Object> params = new LinkedHashMap<>();
        try {
            if (JsonReader.parse(body) instanceof Map<?, ?> map && map.get("code") instanceof String code) {
                String message = map.get("message") instanceof String m ? m : code;
                if (map.get("params") instanceof Map<?, ?> raw) {
                    raw.forEach((key, value) -> params.put(String.valueOf(key), value));
                }
                // An unmodifiable view rather than Map.copyOf: copyOf rejects a null value, and the
                // throw would be caught below and cost the caller the code and the message over the
                // least informative part of the body. A null-valued parameter is kept as sent.
                return new DeleteOutcome.Rejected(code, message, Collections.unmodifiableMap(params));
            }
        } catch (RuntimeException malformed) {
            // fall through: a non-coded / unparseable error body is still a refusal, not a crash
        }
        return new DeleteOutcome.Rejected("", "The server refused the removal.", Map.of());
    }

    @Override
    public ListOutcome list(URI baseUrl, String credential, String kind) {
        try {
            String path = kind == null || kind.isBlank()
                    ? "/api/artifacts"
                    : "/api/artifacts?kind=" + URLEncoder.encode(kind, StandardCharsets.UTF_8);
            HttpRequest request = authed(baseUrl, path, credential).GET().build();
            HttpResponse<String> response =
                    send(request, HttpResponse.BodyHandlers.ofString(StandardCharsets.UTF_8));
            if (response.statusCode() == 200) {
                return new ListOutcome.Listed(remoteArtifacts(response.body()));
            }
            Rejection r = rejection(response.body(), "The server refused the read.");
            return new ListOutcome.Rejected(r.code(), r.message());
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            return new ListOutcome.Unreachable();
        } catch (IOException | RuntimeException e) {
            return new ListOutcome.Unreachable();
        }
    }

    @Override
    public ConnectionTestOutcome test(
            URI baseUrl, String credential, String id, String connectorId, Map<String, Object> settings) {
        try {
            HttpRequest request = authed(baseUrl, "/api/connections:test", credential, heavyTimeout)
                    .header("Content-Type", "application/json")
                    .POST(HttpRequest.BodyPublishers.ofString(
                            connectionBody(id, connectorId, settings), StandardCharsets.UTF_8))
                    .build();
            HttpResponse<String> response =
                    send(request, HttpResponse.BodyHandlers.ofString(StandardCharsets.UTF_8));
            if (response.statusCode() == 200) {
                ConnectionReport report = connectionReport(response.body());
                // a 200 that is not a usable report (a proxy / non-Tapstate reply) is treated as unreachable
                return report == null
                        ? new ConnectionTestOutcome.Unreachable()
                        : new ConnectionTestOutcome.Tested(report);
            }
            Rejection r = rejection(response.body(), "The server refused the connection test.");
            return new ConnectionTestOutcome.Rejected(r.code(), r.message());
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            return new ConnectionTestOutcome.Unreachable();
        } catch (HttpConnectTimeoutException e) {
            return new ConnectionTestOutcome.Unreachable();   // never connected: not reachable
        } catch (HttpTimeoutException e) {
            return new ConnectionTestOutcome.TimedOut();       // reached the server, but no answer in time
        } catch (IOException | RuntimeException e) {
            return new ConnectionTestOutcome.Unreachable();
        }
    }

    @Override
    public ConnectionTestResultOutcome testResult(URI baseUrl, String credential, String id) {
        try {
            HttpRequest request =
                    authed(baseUrl, "/api/connections/" + id + "/test-result", credential).GET().build();
            HttpResponse<String> response =
                    send(request, HttpResponse.BodyHandlers.ofString(StandardCharsets.UTF_8));
            int status = response.statusCode();
            if (status == 200) {
                ConnectionReport report = connectionReport(response.body());
                // a 200 that is not a usable report (a proxy / non-Tapstate reply) is treated as unreachable
                return report == null
                        ? new ConnectionTestResultOutcome.Unreachable()
                        : new ConnectionTestResultOutcome.Found(report);
            }
            if (status == 404) {
                return new ConnectionTestResultOutcome.Absent();
            }
            Rejection r = rejection(response.body(), "The server refused the read.");
            return new ConnectionTestResultOutcome.Rejected(r.code(), r.message());
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            return new ConnectionTestResultOutcome.Unreachable();
        } catch (IOException | RuntimeException e) {
            return new ConnectionTestResultOutcome.Unreachable();
        }
    }

    @Override
    public ConnectionDiscoverSchemaOutcome discoverSchema(
            URI baseUrl, String credential, String id, String connectorId, Map<String, Object> settings) {
        try {
            HttpRequest request = authed(baseUrl, "/api/connections:discover-schema", credential, heavyTimeout)
                    .header("Content-Type", "application/json")
                    .POST(HttpRequest.BodyPublishers.ofString(
                            connectionBody(id, connectorId, settings), StandardCharsets.UTF_8))
                    .build();
            HttpResponse<String> response =
                    send(request, HttpResponse.BodyHandlers.ofString(StandardCharsets.UTF_8));
            if (response.statusCode() == 200) {
                ConnectionSchema schema = connectionSchema(response.body());
                // a 200 that is not a usable model (a proxy / non-Tapstate reply) is treated as unreachable
                return schema == null
                        ? new ConnectionDiscoverSchemaOutcome.Unreachable()
                        : new ConnectionDiscoverSchemaOutcome.Discovered(schema);
            }
            Rejection r = rejection(response.body(), "The server refused the schema discovery.");
            return new ConnectionDiscoverSchemaOutcome.Rejected(r.code(), r.message());
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            return new ConnectionDiscoverSchemaOutcome.Unreachable();
        } catch (HttpConnectTimeoutException e) {
            return new ConnectionDiscoverSchemaOutcome.Unreachable();   // never connected: not reachable
        } catch (HttpTimeoutException e) {
            return new ConnectionDiscoverSchemaOutcome.TimedOut();       // reached the server, but no answer in time
        } catch (IOException | RuntimeException e) {
            return new ConnectionDiscoverSchemaOutcome.Unreachable();
        }
    }

    @Override
    public ConnectionSchemaOutcome schema(URI baseUrl, String credential, String id) {
        try {
            HttpRequest request = authed(baseUrl, "/api/connections/" + id + "/schema", credential).GET().build();
            HttpResponse<String> response =
                    send(request, HttpResponse.BodyHandlers.ofString(StandardCharsets.UTF_8));
            int status = response.statusCode();
            if (status == 200) {
                ConnectionSchema schema = connectionSchema(response.body());
                // a 200 that is not a usable model (a proxy / non-Tapstate reply) is treated as unreachable
                return schema == null
                        ? new ConnectionSchemaOutcome.Unreachable()
                        : new ConnectionSchemaOutcome.Found(schema);
            }
            if (status == 404) {
                return new ConnectionSchemaOutcome.Absent();
            }
            Rejection r = rejection(response.body(), "The server refused the read.");
            return new ConnectionSchemaOutcome.Rejected(r.code(), r.message());
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            return new ConnectionSchemaOutcome.Unreachable();
        } catch (IOException | RuntimeException e) {
            return new ConnectionSchemaOutcome.Unreachable();
        }
    }

    @Override
    public ConnectorRegisterOutcome register(URI baseUrl, String credential, byte[] artifact) {
        try {
            HttpRequest request = authed(baseUrl, "/api/connectors:register", credential, registerTimeout(artifact.length))
                    .header("Content-Type", "application/json")
                    .POST(HttpRequest.BodyPublishers.ofString(registerBody(artifact), StandardCharsets.UTF_8))
                    .build();
            HttpResponse<String> response =
                    send(request, HttpResponse.BodyHandlers.ofString(StandardCharsets.UTF_8));
            if (response.statusCode() == 200) {
                RegisteredConnector registered = registeredConnector(response.body());
                // a 200 that is not a usable registration (a proxy / non-Tapstate reply) is treated as unreachable
                return registered == null
                        ? new ConnectorRegisterOutcome.Unreachable()
                        : new ConnectorRegisterOutcome.Registered(registered);
            }
            Rejection r = rejection(response.body(), "The server refused the connector registration.");
            return new ConnectorRegisterOutcome.Rejected(r.code(), r.message());
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            return new ConnectorRegisterOutcome.Unreachable();
        } catch (HttpConnectTimeoutException e) {
            return new ConnectorRegisterOutcome.Unreachable();   // never connected: the server is not reachable
        } catch (HttpTimeoutException e) {
            return new ConnectorRegisterOutcome.TimedOut();       // reached the server, but no answer in time
        } catch (IOException | RuntimeException e) {
            return new ConnectorRegisterOutcome.Unreachable();
        }
    }

    @Override
    public ConnectorListOutcome connectorList(URI baseUrl, String credential) {
        try {
            HttpRequest request = authed(baseUrl, "/api/connectors", credential).GET().build();
            HttpResponse<String> response =
                    send(request, HttpResponse.BodyHandlers.ofString(StandardCharsets.UTF_8));
            if (response.statusCode() == 200) {
                return new ConnectorListOutcome.Listed(catalogConnectors(response.body()));
            }
            Rejection r = rejection(response.body(), "The server refused the read.");
            return new ConnectorListOutcome.Rejected(r.code(), r.message());
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            return new ConnectorListOutcome.Unreachable();
        } catch (IOException | RuntimeException e) {
            return new ConnectorListOutcome.Unreachable();
        }
    }

    /** The connectors decoded from a 200 body's {@code connectors} array; empty if the shape is unexpected. */
    private static List<CatalogConnector> catalogConnectors(String body) {
        List<CatalogConnector> connectors = new ArrayList<>();
        if (JsonReader.parse(body) instanceof Map<?, ?> map && map.get("connectors") instanceof List<?> list) {
            for (Object o : list) {
                if (o instanceof Map<?, ?> m && m.get("id") instanceof String id) {
                    connectors.add(new CatalogConnector(
                            id,
                            stringOrNull(m.get("name")),
                            stringOrNull(m.get("group")),
                            stringList(m.get("modes")),
                            m.get("sink") instanceof Boolean sink && sink,
                            stringOrNull(m.get("origin"))));
                }
            }
        }
        return connectors;
    }

    /** The register request body: {@code {"artifact":"<base64 of the jar bytes>"}}. */
    private static String registerBody(byte[] artifact) {
        Map<String, Object> body = new LinkedHashMap<>();
        body.put("artifact", Base64.getEncoder().encodeToString(artifact));
        return JsonOut.write(body);
    }

    /** The registration decoded from a 200 body, or {@code null} if the body is not a usable registration. */
    private static RegisteredConnector registeredConnector(String body) {
        if (JsonReader.parse(body) instanceof Map<?, ?> m
                && m.get("connectorId") instanceof String connectorId
                && m.get("contentHash") instanceof String contentHash) {
            boolean newlyRegistered = m.get("newlyRegistered") instanceof Boolean b && b;
            return new RegisteredConnector(connectorId, contentHash, stringOrNull(m.get("pdkApiVersion")), newlyRegistered);
        }
        return null;
    }

    /** The discovered model decoded from a 200 body, or {@code null} if the body is not a usable model. */
    private static ConnectionSchema connectionSchema(String body) {
        if (JsonReader.parse(body) instanceof Map<?, ?> m
                && m.get("connectionId") instanceof String connectionId
                && m.get("connectorId") instanceof String connectorId
                && m.get("tables") instanceof List<?> rawTables) {
            long discoveredAt = m.get("discoveredAt") instanceof Number n ? n.longValue() : 0L;
            List<ConnectionSchema.Table> tables = new ArrayList<>();
            for (Object o : rawTables) {
                // a malformed table entry is skipped, same policy as a malformed check in a test report
                if (!(o instanceof Map<?, ?> t) || !(t.get("name") instanceof String name)) {
                    continue;
                }
                List<ConnectionSchema.Field> fields = new ArrayList<>();
                if (t.get("fields") instanceof List<?> rawFields) {
                    for (Object f : rawFields) {
                        if (f instanceof Map<?, ?> fm && fm.get("name") instanceof String fieldName) {
                            fields.add(new ConnectionSchema.Field(fieldName, stringOrNull(fm.get("type"))));
                        }
                    }
                }
                List<ConnectionSchema.Index> indexes = new ArrayList<>();
                if (t.get("indexes") instanceof List<?> rawIndexes) {
                    for (Object i : rawIndexes) {
                        if (i instanceof Map<?, ?> im && im.get("name") instanceof String indexName) {
                            indexes.add(new ConnectionSchema.Index(indexName,
                                    stringList(im.get("fields")),
                                    im.get("unique") instanceof Boolean unique && unique));
                        }
                    }
                }
                tables.add(new ConnectionSchema.Table(name, fields, stringList(t.get("primaryKey")), indexes));
            }
            return new ConnectionSchema(connectionId, connectorId, tables, discoveredAt);
        }
        return null;
    }

    /** The value's string elements when it is a list, else empty (an absent / malformed optional field). */
    private static List<String> stringList(Object value) {
        List<String> strings = new ArrayList<>();
        if (value instanceof List<?> list) {
            for (Object element : list) {
                if (element instanceof String string) {
                    strings.add(string);
                }
            }
        }
        return strings;
    }

    /** A discovery auth-mode array must contain strings only; malformed entries cannot be ignored. */
    private static List<String> requiredStringList(Object value) {
        if (!(value instanceof List<?> list)) {
            throw new IllegalArgumentException("authModes must be an array");
        }
        List<String> strings = new ArrayList<>(list.size());
        for (Object element : list) {
            if (!(element instanceof String string)) {
                throw new IllegalArgumentException("authModes must contain strings only");
            }
            strings.add(string);
        }
        return strings;
    }

    /** The connection-verb request body: {@code {"id":..,"connectorId":..,"settings":{..}}}. */
    private static String connectionBody(String id, String connectorId, Map<String, Object> settings) {
        Map<String, Object> body = new LinkedHashMap<>();
        body.put("id", id);
        body.put("connectorId", connectorId);
        body.put("settings", settings == null ? Map.of() : settings);
        return JsonOut.write(body);
    }

    /** The connection report decoded from a 200 body, or {@code null} if the body is not a usable report. */
    private static ConnectionReport connectionReport(String body) {
        if (JsonReader.parse(body) instanceof Map<?, ?> m
                && m.get("connectionId") instanceof String connectionId
                && m.get("connectorId") instanceof String connectorId
                && m.get("outcome") instanceof String outcome) {
            long testedAt = m.get("testedAt") instanceof Number n ? n.longValue() : 0L;
            List<ConnectionReport.Check> checks = new ArrayList<>();
            if (m.get("checks") instanceof List<?> items) {
                for (Object o : items) {
                    if (o instanceof Map<?, ?> c
                            && c.get("name") instanceof String name
                            && c.get("status") instanceof String status) {
                        checks.add(new ConnectionReport.Check(name, status,
                                stringOrNull(c.get("message")), stringOrNull(c.get("reason")),
                                stringOrNull(c.get("solution")), stringOrNull(c.get("connectorErrorCode"))));
                    }
                }
            }
            return new ConnectionReport(connectionId, connectorId, outcome, checks, testedAt);
        }
        return null;
    }

    /** The value as a string when it is one, else {@code null} (an absent / JSON-null optional field). */
    private static String stringOrNull(Object value) {
        return value instanceof String s ? s : null;
    }

    @Override
    public LifecycleOutcome lifecycle(URI baseUrl, String credential, String pipelineId, String verb) {
        try {
            HttpRequest request = authed(baseUrl, "/api/pipelines/" + pipelineId + ":" + verb, credential)
                    .POST(HttpRequest.BodyPublishers.noBody())
                    .build();
            HttpResponse<String> response =
                    send(request, HttpResponse.BodyHandlers.ofString(StandardCharsets.UTF_8));
            if (response.statusCode() == 200) {
                LifecycleOutcome.Accepted accepted = desiredState(response.body());
                // a 200 that is not a usable desired-state reply (a proxy / non-Tapstate answer) is unreachable
                return accepted == null ? new LifecycleOutcome.Unreachable() : accepted;
            }
            Rejection r = rejection(response.body(), "The server refused the lifecycle verb.");
            return new LifecycleOutcome.Rejected(r.code(), r.message());
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            return new LifecycleOutcome.Unreachable();
        } catch (IOException | RuntimeException e) {
            return new LifecycleOutcome.Unreachable();
        }
    }

    /** The new desired state decoded from a 200 body, or {@code null} unless it carries all three string fields. */
    private static LifecycleOutcome.Accepted desiredState(String body) {
        if (JsonReader.parse(body) instanceof Map<?, ?> m
                && m.get("pipelineId") instanceof String id
                && m.get("targetState") instanceof String state
                && m.get("revision") instanceof String revision) {
            return new LifecycleOutcome.Accepted(id, state, revision);
        }
        return null;
    }

    @Override
    public StatusOutcome status(URI baseUrl, String credential, String pipelineId) {
        try {
            HttpRequest request =
                    authed(baseUrl, "/api/pipelines/" + pipelineId + "/status", credential).GET().build();
            HttpResponse<String> response =
                    send(request, HttpResponse.BodyHandlers.ofString(StandardCharsets.UTF_8));
            if (response.statusCode() == 200) {
                StatusOutcome.Found found = statusFound(response.body());
                // a 200 that is not a usable status reply (a proxy / non-Tapstate answer) is unreachable
                return found == null ? new StatusOutcome.Unreachable() : found;
            }
            Rejection r = rejection(response.body(), "The server refused the read.");
            return new StatusOutcome.Rejected(r.code(), r.message());
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            return new StatusOutcome.Unreachable();
        } catch (IOException | RuntimeException e) {
            return new StatusOutcome.Unreachable();
        }
    }

    @Override
    public MetricsOutcome metrics(URI baseUrl, String credential, String pipelineId) {
        try {
            HttpRequest request =
                    authed(baseUrl, "/api/pipelines/" + pipelineId + "/metrics", credential).GET().build();
            HttpResponse<String> response =
                    send(request, HttpResponse.BodyHandlers.ofString(StandardCharsets.UTF_8));
            if (response.statusCode() == 200) {
                MetricsOutcome.Found found = metricsFound(response.body());
                return found == null ? new MetricsOutcome.Unreachable() : found;
            }
            Rejection r = rejection(response.body(), "The server refused the read.");
            return new MetricsOutcome.Rejected(r.code(), r.message());
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            return new MetricsOutcome.Unreachable();
        } catch (IOException | RuntimeException e) {
            return new MetricsOutcome.Unreachable();
        }
    }

    @Override
    public SnapshotOutcome snapshot(URI baseUrl, String credential, String pipelineId) {
        try {
            HttpRequest request =
                    authed(baseUrl, "/api/pipelines/" + pipelineId + "/snapshot", credential).GET().build();
            HttpResponse<String> response =
                    send(request, HttpResponse.BodyHandlers.ofString(StandardCharsets.UTF_8));
            if (response.statusCode() == 200) {
                SnapshotOutcome.Found found = snapshotFound(response.body());
                return found == null ? new SnapshotOutcome.Unreachable() : found;
            }
            Rejection r = rejection(response.body(), "The server refused the read.");
            return new SnapshotOutcome.Rejected(r.code(), r.message());
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            return new SnapshotOutcome.Unreachable();
        } catch (IOException | RuntimeException e) {
            return new SnapshotOutcome.Unreachable();
        }
    }

    /**
     * The status decoded from a 200 body, or {@code null} unless it carries a string id and a string
     * state -- and, when it carries a {@code failure} block at all, one shaped like a real one. A present
     * but malformed failure block (not an object, or missing/wrong-typed code) is a regression of the
     * status contract, not a healthy pipeline: it must not silently decode the same way an absent failure
     * block does, so the whole body is treated as shape-wrong here rather than a fabricated healthy read.
     */
    private static StatusOutcome.Found statusFound(String body) {
        if (JsonReader.parse(body) instanceof Map<?, ?> m
                && m.get("pipelineId") instanceof String id
                && m.get("state") instanceof String state) {
            Object rawFailure = m.get("failure");
            if (rawFailure == null) {
                return new StatusOutcome.Found(id, state);
            }
            if (!(rawFailure instanceof Map<?, ?> failure) || !(failure.get("code") instanceof String code)) {
                return null;
            }
            // The message is the server's rendering of that code; when it is absent the code still names
            // the diagnosis, so it stands in rather than the whole read degrading to unreachable.
            String message = failure.get("message") instanceof String rendered ? rendered : code;
            return new StatusOutcome.Found(id, state, code, message);
        }
        return null;
    }

    /**
     * The metrics decoded from a 200 body's {@code metrics} object, or {@code null} unless the body carries a
     * string id and a metrics object. Each numeric cell is read as a long; the sibling {@code perTableOffset}
     * object carries the per-table positions ({@code table -> srcpos}) and is absent until one is acked. Any
     * non-numeric metrics cell is dropped, so a malformed entry never crashes the read. An empty object is a
     * legitimate empty (no source wired yet).
     */
    private static MetricsOutcome.Found metricsFound(String body) {
        if (JsonReader.parse(body) instanceof Map<?, ?> m
                && m.get("pipelineId") instanceof String id
                && m.get("metrics") instanceof Map<?, ?> metrics) {
            Map<String, Long> stats = new LinkedHashMap<>();
            for (Map.Entry<?, ?> e : metrics.entrySet()) {
                if (e.getKey() instanceof String name && e.getValue() instanceof Number value) {
                    stats.put(name, value.longValue());
                }
            }
            Map<String, String> perTableOffset = new LinkedHashMap<>();
            if (m.get("perTableOffset") instanceof Map<?, ?> offsets) {
                for (Map.Entry<?, ?> e : offsets.entrySet()) {
                    if (e.getKey() instanceof String table && e.getValue() instanceof String position) {
                        perTableOffset.put(table, position);
                    }
                }
            }
            return new MetricsOutcome.Found(id, stats, perTableOffset);
        }
        return null;
    }

    /**
     * The per-table progress decoded from a 200 body's {@code snapshot} object, or {@code null} unless the body
     * carries a string id and a snapshot object. A table needs a numeric {@code rowsDone}; {@code rowsTotal} and
     * {@code donePct} are kept null when absent or null (unavailable), never faked. An empty object is a
     * legitimate empty (outside a snapshot phase).
     */
    private static SnapshotOutcome.Found snapshotFound(String body) {
        if (JsonReader.parse(body) instanceof Map<?, ?> m
                && m.get("pipelineId") instanceof String id
                && m.get("snapshot") instanceof Map<?, ?> snapshot) {
            Map<String, RemoteTableSnapshot> tables = new LinkedHashMap<>();
            for (Map.Entry<?, ?> e : snapshot.entrySet()) {
                if (e.getKey() instanceof String table && e.getValue() instanceof Map<?, ?> t
                        && t.get("rowsDone") instanceof Number rowsDone) {
                    Long rowsTotal = t.get("rowsTotal") instanceof Number n ? n.longValue() : null;
                    Integer donePct = t.get("donePct") instanceof Number n ? n.intValue() : null;
                    tables.put(table, new RemoteTableSnapshot(rowsDone.longValue(), rowsTotal, donePct));
                }
            }
            return new SnapshotOutcome.Found(id, tables);
        }
        return null;
    }

    @Override
    public LogsOutcome logs(URI baseUrl, String credential, String pipelineId) {
        try {
            HttpRequest request =
                    authed(baseUrl, "/api/pipelines/" + pipelineId + "/logs", credential).GET().build();
            HttpResponse<String> response =
                    send(request, HttpResponse.BodyHandlers.ofString(StandardCharsets.UTF_8));
            if (response.statusCode() == 200) {
                LogsOutcome.Found found = logsFound(response.body());
                return found == null ? new LogsOutcome.Unreachable() : found;
            }
            Rejection r = rejection(response.body(), "The server refused the read.");
            return new LogsOutcome.Rejected(r.code(), r.message());
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            return new LogsOutcome.Unreachable();
        } catch (IOException | RuntimeException e) {
            return new LogsOutcome.Unreachable();
        }
    }

    private static LogsOutcome.Found logsFound(String body) {
        if (JsonReader.parse(body) instanceof Map<?, ?> m
                && m.get("pipelineId") instanceof String id
                && m.get("lines") instanceof List<?> lines) {
            List<RemoteLogLine> parsed = new ArrayList<>();
            for (Object element : lines) {
                if (element instanceof Map<?, ?> line
                        && line.get("timestampMillis") instanceof Number ts
                        && line.get("level") instanceof String level
                        && line.get("message") instanceof String message) {
                    parsed.add(new RemoteLogLine(ts.longValue(), level, message));
                }
            }
            return new LogsOutcome.Found(id, parsed);
        }
        return null;
    }

    // --- streaming reads over a websocket (status --watch / logs --follow) -----------------------

    /** How long to wait after a live connection drops before re-attaching (same landing node in L1). */
    private static final Duration RECONNECT_BACKOFF = Duration.ofSeconds(1);

    /** How often the blocking stream loop wakes to check the stop signal while waiting. */
    private static final Duration STOP_POLL = Duration.ofMillis(200);

    @Override
    public String watchStatus(URI baseUrl, String credential, String pipelineId,
            StatusStream sink, BooleanSupplier stop) {
        return stream(wsUri(baseUrl, "/api/pipelines/" + pipelineId + "/status/watch"), credential, stop, frame -> {
            StatusOutcome.Found found = statusFound(frame);
            if (found != null) {
                sink.state(found.pipelineId(), found.state(), found.failureCode(), found.failureMessage());
            }
        });
    }

    @Override
    public String followLogs(URI baseUrl, String credential, String pipelineId,
            LogStream sink, BooleanSupplier stop) {
        return stream(wsUri(baseUrl, "/api/pipelines/" + pipelineId + "/logs/follow"), credential, stop, frame -> {
            LogsOutcome.Found found = logsFound(frame);
            if (found != null && !found.lines().isEmpty()) {
                sink.lines(found.pipelineId(), found.lines());
            }
        });
    }


    @Override
    public String tail(URI baseUrl, String credential, String sourceId, String collection, Object filter,
            TailStream sink, BooleanSupplier stop) {
        String path = "/api/data-browser/" + urlSegment(sourceId) + "/" + urlSegment(collection) + "/tail";
        // The filter travels in the handshake query because a handshake has no body. It goes as the same
        // JSON a read would have sent, so both faces meet the identical reading on the far side.
        String query = filter == null ? "" : "?filter=" + encode(JsonOut.compact(filter));
        return stream(wsUri(baseUrl, path + query), credential, stop, frame -> {
            TailChange change = tailChange(frame);
            if (change != null) {
                sink.change(change);
            }
        });
    }

    /** One streamed change frame, or null when the frame is not one. */
    private static TailChange tailChange(String frame) {
        Object parsed = JsonReader.parse(frame);
        if (!(parsed instanceof Map<?, ?> map) || !(map.get("kind") instanceof String kind)) {
            return null;
        }
        String at = TIME.format(Instant.ofEpochMilli(
                map.get("at") instanceof Number ms ? ms.longValue() : 0L).atZone(ZoneId.systemDefault()));
        // A row the frame did not carry stays absent here. Decoding it into an empty map would turn
        // "the connector did not say" into "the connector said there was nothing".
        return new TailChange(TailChange.Kind.valueOf(kind), at, row(map, "before"), row(map, "after"));
    }

    @SuppressWarnings("unchecked")
    private static Map<String, Object> row(Map<?, ?> frame, String side) {
        return frame.get(side) instanceof Map<?, ?> written ? (Map<String, Object>) written : null;
    }

    /** How a streamed change's time is shown: the clock, because a follow is read as it happens. */
    private static final DateTimeFormatter TIME = DateTimeFormatter.ofPattern("HH:mm:ss");

    private static String encode(String segment) {
        return URLEncoder.encode(segment, StandardCharsets.UTF_8);
    }

    /** The websocket policy-violation close code, which the server sends carrying a coded refusal. */

    private static final int WS_POLICY_VIOLATION = 1008;

    /**
     * Opens a websocket to {@code wsUri}, delivering each decoded text frame to {@code onFrame}, and blocks
     * until {@code stop} signals. A refused or unreachable handshake ends the stream; a live connection that
     * later drops is re-attached after a short backoff until stopped. One close is terminal rather than a
     * drop: the server closing with {@code 1008} whose reason names a coded refusal — the stream can never
     * be served (e.g. a pipeline id that will never resolve), so re-attaching would be refused identically
     * forever, churning the connection while the caller waits on something that cannot come. That refusal's
     * code is returned; every other ending returns {@code null}. Never throws.
     */
    private String stream(URI wsUri, String credential, BooleanSupplier stop, Consumer<String> onFrame) {
        while (!stop.getAsBoolean()) {
            CountDownLatch closed = new CountDownLatch(1);
            AtomicReference<String> refusal = new AtomicReference<>();
            WebSocket ws;
            try {
                ws = client().newWebSocketBuilder()
                        .header("Authorization", "Bearer " + credential)
                        .buildAsync(wsUri, new StreamListener(onFrame, closed, refusal))
                        .join();
            } catch (RuntimeException handshakeFailed) {
                // join() wraps a refused (401/403) or unreachable handshake in a CompletionException (a
                // RuntimeException); either way it cannot be streamed, so end the stream.
                return null;
            }
            awaitClosedOrStop(closed, stop);
            ws.abort();
            if (refusal.get() != null) {
                return refusal.get();
            }
            if (stop.getAsBoolean()) {
                return null;
            }
            // A live connection dropped (not a stop): re-attach after a short backoff.
            if (!sleepUnlessStopped(RECONNECT_BACKOFF, stop)) {
                return null;
            }
        }
        return null;
    }

    /** The {@code ws(s)} endpoint for {@code path} against an {@code http(s)} base, tolerating a trailing slash. */
    static URI wsUri(URI baseUrl, String path) {
        String base = baseUrl.toString();
        if (base.endsWith("/")) {
            base = base.substring(0, base.length() - 1);
        }
        String wsBase;
        if (base.startsWith("https://")) {
            wsBase = "wss://" + base.substring("https://".length());
        } else if (base.startsWith("http://")) {
            wsBase = "ws://" + base.substring("http://".length());
        } else {
            wsBase = base;   // already a ws / wss base
        }
        return URI.create(wsBase + path);
    }

    /** Waits until the connection closes or {@code stop} is signalled, whichever comes first. */
    private static void awaitClosedOrStop(CountDownLatch closed, BooleanSupplier stop) {
        try {
            while (!stop.getAsBoolean()) {
                if (closed.await(STOP_POLL.toMillis(), TimeUnit.MILLISECONDS)) {
                    return;
                }
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    /** Sleeps {@code duration} in short chunks; returns {@code false} the moment {@code stop} is signalled. */
    private static boolean sleepUnlessStopped(Duration duration, BooleanSupplier stop) {
        long remaining = duration.toMillis();
        try {
            while (remaining > 0) {
                if (stop.getAsBoolean()) {
                    return false;
                }
                long chunk = Math.min(remaining, STOP_POLL.toMillis());
                Thread.sleep(chunk);
                remaining -= chunk;
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            return false;
        }
        return !stop.getAsBoolean();
    }

    /** A websocket listener that reassembles text frames and hands each complete one to a decoder. */
    private static final class StreamListener implements WebSocket.Listener {
        private final Consumer<String> onFrame;
        private final CountDownLatch closed;
        private final AtomicReference<String> refusal;
        private final StringBuilder partial = new StringBuilder();

        StreamListener(Consumer<String> onFrame, CountDownLatch closed, AtomicReference<String> refusal) {
            this.onFrame = onFrame;
            this.closed = closed;
            this.refusal = refusal;
        }

        @Override
        public CompletionStage<?> onText(WebSocket webSocket, CharSequence data, boolean last) {
            partial.append(data);
            if (last) {
                String frame = partial.toString();
                partial.setLength(0);
                try {
                    onFrame.accept(frame);
                } catch (RuntimeException malformed) {
                    // A malformed frame is dropped, never crashes the stream.
                }
            }
            webSocket.request(1);
            return null;
        }

        @Override
        public void onError(WebSocket webSocket, Throwable error) {
            closed.countDown();
        }

        @Override
        public CompletionStage<?> onClose(WebSocket webSocket, int statusCode, String reason) {
            // A policy-violation close carries the server's coded refusal as its reason: the stream is
            // being ended deliberately, not dropped, so the reconnect loop must stop and surface it.
            if (statusCode == WS_POLICY_VIOLATION && reason != null && !reason.isBlank()) {
                refusal.set(reason);
            }
            closed.countDown();
            return null;
        }
    }

    /** A request builder for a light verb: the short probe read budget and the bearer credential. */
    private HttpRequest.Builder authed(URI baseUrl, String path, String credential) {
        return authed(baseUrl, path, credential, probeTimeout);
    }

    /** A request builder carrying an explicit read {@code timeout} — the heavy verbs pass a larger one. */
    private HttpRequest.Builder authed(URI baseUrl, String path, String credential, Duration timeout) {
        return HttpRequest.newBuilder(endpoint(baseUrl, path))
                .timeout(timeout)
                .header("Authorization", "Bearer " + credential);
    }

    /** Enforces the request budget outside the JDK client's transport state machine as well. */
    private <T> HttpResponse<T> send(
            HttpRequest request, HttpResponse.BodyHandler<T> handler)
            throws IOException, InterruptedException {
        CompletableFuture<HttpResponse<T>> future = client().sendAsync(request, handler);
        Duration timeout = request.timeout().orElse(probeTimeout);
        try {
            return future.get(Math.max(1, timeout.toMillis()), TimeUnit.MILLISECONDS);
        } catch (InterruptedException error) {
            future.cancel(true);
            throw error;
        } catch (TimeoutException error) {
            future.cancel(true);
            throw new HttpTimeoutException("HTTP request exceeded " + timeout);
        } catch (ExecutionException error) {
            Throwable cause = error.getCause();
            if (cause instanceof IOException io) {
                throw io;
            }
            if (cause instanceof RuntimeException runtime) {
                throw runtime;
            }
            if (cause instanceof Error fatal) {
                throw fatal;
            }
            throw new IOException("HTTP request failed", cause);
        }
    }

    /** The absolute request URI for {@code path} against a base, tolerating a trailing slash on the base. */
    private static URI endpoint(URI baseUrl, String path) {
        String base = baseUrl.toString();
        if (base.endsWith("/")) {
            base = base.substring(0, base.length() - 1);
        }
        return URI.create(base + path);
    }

    /**
     * The apply request body: {@code {"drafts":[{"source":..,"content":..}]}} in submission order, each
     * draft carrying {@code expectedContentHash} only when one was given. A draft with no precondition
     * omits the key rather than sending null, so a request that asked for no check stays exactly the shape
     * it has always been — and the published schema refuses properties it does not declare, which a null
     * would still be one of.
     */
    private static String applyBody(List<LocalDraft> drafts) {
        List<Object> array = new ArrayList<>();
        for (LocalDraft draft : drafts) {
            Map<String, Object> d = new LinkedHashMap<>();
            d.put("source", draft.source());
            d.put("content", draft.content());
            if (draft.expectedContentHash() != null) {
                d.put("expectedContentHash", draft.expectedContentHash());
            }
            array.add(d);
        }
        Map<String, Object> body = new LinkedHashMap<>();
        body.put("drafts", array);
        return JsonOut.write(body);
    }

    /**
     * A 200 apply body decoded into its two arrays: the per-artifact outcomes and the advisory warnings.
     * The body is parsed once and each array read from the same map, so the two never disagree about
     * which response they came from.
     */
    private static ApplyOutcome.Applied applied(String body) {
        if (JsonReader.parse(body) instanceof Map<?, ?> map) {
            return new ApplyOutcome.Applied(applyItems(map), applyWarnings(map));
        }
        return new ApplyOutcome.Applied(List.of(), List.of());
    }

    /** The apply outcomes decoded from a 200 body's {@code outcomes} array; empty if the shape is unexpected. */
    private static List<ApplyOutcome.Item> applyItems(Map<?, ?> map) {
        List<ApplyOutcome.Item> items = new ArrayList<>();
        if (map.get("outcomes") instanceof List<?> outcomes) {
            for (Object o : outcomes) {
                if (o instanceof Map<?, ?> m
                        && m.get("id") instanceof String id
                        && m.get("kind") instanceof String kind
                        && m.get("change") instanceof String change) {
                    items.add(new ApplyOutcome.Item(id, kind, change));
                }
            }
        }
        return items;
    }

    /**
     * The advisory findings decoded from a 200 body's {@code warnings} array. A server that sends none —
     * or one old enough not to send the array at all — decodes to no warnings rather than to a null the
     * caller has to guard. An entry with no code is skipped on its own: it costs its own line, never the
     * applied result, because the batch did land whatever the server said about it afterwards.
     */
    private static List<ApplyOutcome.Warning> applyWarnings(Map<?, ?> map) {
        List<ApplyOutcome.Warning> warnings = new ArrayList<>();
        if (map.get("warnings") instanceof List<?> entries) {
            for (Object o : entries) {
                if (o instanceof Map<?, ?> m && m.get("code") instanceof String code) {
                    warnings.add(new ApplyOutcome.Warning(code, warningParams(m.get("params"))));
                }
            }
        }
        return warnings;
    }

    /** One warning's named params, keyed by name; anything but a JSON object reads as none. */
    private static Map<String, Object> warningParams(Object params) {
        if (!(params instanceof Map<?, ?> map)) {
            return Map.of();
        }
        Map<String, Object> named = new LinkedHashMap<>();
        map.forEach((key, value) -> named.put(String.valueOf(key), value));
        return named;
    }

    /** One stored artifact decoded from a 200 body, or {@code null} if the body is not a usable artifact. */
    private static RemoteArtifact remoteArtifact(String body) {
        if (JsonReader.parse(body) instanceof Map<?, ?> m) {
            return artifactOf(m);
        }
        return null;
    }

    /** The stored artifacts decoded from a 200 body's {@code artifacts} array; empty if the shape is unexpected. */
    private static List<RemoteArtifact> remoteArtifacts(String body) {
        List<RemoteArtifact> artifacts = new ArrayList<>();
        if (JsonReader.parse(body) instanceof Map<?, ?> map && map.get("artifacts") instanceof List<?> list) {
            for (Object o : list) {
                if (o instanceof Map<?, ?> m) {
                    RemoteArtifact artifact = artifactOf(m);
                    if (artifact != null) {
                        artifacts.add(artifact);
                    }
                }
            }
        }
        return artifacts;
    }

    /** One artifact from a decoded JSON object, or {@code null} unless it carries all three string fields. */
    private static RemoteArtifact artifactOf(Map<?, ?> m) {
        if (m.get("id") instanceof String id
                && m.get("kind") instanceof String kind
                && m.get("canonicalForm") instanceof String canonical) {
            return new RemoteArtifact(id, kind, canonical);
        }
        return null;
    }

    /** The string value of {@code key} in a JSON object body, or {@code null} if absent / not a string. */
    private static String stringField(String body, String key) {
        try {
            if (JsonReader.parse(body) instanceof Map<?, ?> map && map.get(key) instanceof String s) {
                return s;
            }
        } catch (RuntimeException malformed) {
            // a malformed body has no usable field
        }
        return null;
    }

    /** A parsed coded refusal: the server's code and message, or a fixed generic message if not coded. */
    private record Rejection(String code, String message, Map<String, Object> params) {
    }

    /**
     * Turns a non-2xx response body into a coded rejection ({@code {code, message}}), or a generic one
     * carrying {@code genericMessage} when the body is not coded / not parseable — a non-coded error body
     * (e.g. a container 500 page) is still a refusal, never a crash and never leaked raw to the user.
     */
    private static Rejection rejection(String body, String genericMessage) {
        try {
            if (JsonReader.parse(body) instanceof Map<?, ?> map && map.get("code") instanceof String code) {
                String message = map.get("message") instanceof String m ? m : code;
                // The parameters come across too. The message arrives rendered, but the catalog holds a
                // remedy for the same code, and rendering that on this side needs the values it names.
                Map<String, Object> params = new LinkedHashMap<>();
                if (map.get("params") instanceof Map<?, ?> raw) {
                    raw.forEach((key, value) -> params.put(String.valueOf(key), value));
                }
                return new Rejection(code, message, Collections.unmodifiableMap(params));
            }
        } catch (RuntimeException malformed) {
            // fall through: a non-coded / unparseable error body is still a refusal, not a crash
        }
        return new Rejection("", genericMessage, Map.of());
    }

    private static RemoteCreatedToken createdToken(Object body) {
        if (body instanceof Map<?, ?> map
                && map.get("tokenId") instanceof String tokenId
                && map.get("scope") instanceof String scope
                && map.get("token") instanceof String token
                && map.get("createdAt") instanceof String createdAt) {
            return new RemoteCreatedToken(tokenId, scope, token, createdAt);
        }
        return null;
    }

    private static List<RemoteToken> tokens(Object body) {
        List<RemoteToken> out = new ArrayList<>();
        if (body instanceof Map<?, ?> map && map.get("tokens") instanceof List<?> rows) {
            for (Object row : rows) {
                if (row instanceof Map<?, ?> token
                        && token.get("tokenId") instanceof String tokenId
                        && token.get("scope") instanceof String scope
                        && token.get("revoked") instanceof Boolean revoked
                        && token.get("createdAt") instanceof String createdAt) {
                    out.add(new RemoteToken(tokenId, scope, revoked, createdAt));
                }
            }
        }
        return List.copyOf(out);
    }

    private static String urlSegment(String value) {
        return URLEncoder.encode(value, StandardCharsets.UTF_8).replace("+", "%20");
    }
}
