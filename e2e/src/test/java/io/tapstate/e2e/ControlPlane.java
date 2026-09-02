package io.tapstate.e2e;

import io.tapstate.control.core.MonitorError;
import io.tapstate.core.common.JsonReader;
import io.tapstate.core.common.JsonWriter;
import io.tapstate.core.lifecycle.LifecycleVerb;
import io.tapstate.core.lifecycle.PipelineState;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.net.URI;
import java.net.URLEncoder;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.net.http.WebSocket;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Base64;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletionException;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Predicate;

/**
 * The product's HTTP surface, as a caller sees it.
 *
 * <p>The harness speaks this wire itself rather than borrowing the CLI's client: the CLI's own
 * testing is unit tests, a corpus and a native smoke, and pulling it in here would make every
 * specification a test of two things at once. What is shared with the product on purpose is the JSON
 * codec and the DSL parser - the places where a second implementation would be a second truth.
 *
 * <p>Failures are surfaced, never absorbed: a refused verb fails the specification carrying the
 * server's own status and body, because a harness that turns a 4xx into a quiet nothing would let a
 * broken product pass.
 */
final class ControlPlane {

    private static final Duration TIMEOUT = Duration.ofSeconds(20);

    /** How often a caller waiting on a pushed change looks again; a follow is told, never asked. */
    private static final Duration POLL = Duration.ofMillis(100);

    /**
     * The bound for uploading a connector, which is the one request whose body is tens of megabytes -
     * base64 of a shaded jar. The ordinary bound is about a server that has stopped answering; this one
     * is about how long bytes take to move, and the largest connector this harness registers takes
     * longer than that on a busy machine.
     */
    private static final Duration UPLOAD_TIMEOUT = Duration.ofMinutes(3);

    /**
     * What every per-namespace count of unassemblable changes is named with, before the namespace itself.
     * Written here rather than shared with the runtime that publishes it: this side is a reader of the
     * product's metrics face, and a name it imported from the publisher would agree with the publisher by
     * construction rather than by contract - so a rename would move both ends at once and assert nothing.
     */
    private static final String DEAD_LETTERED_PREFIX = "nestDeadLettered.";

    private final URI baseUrl;
    private final HttpClient http = HttpClient.newBuilder().connectTimeout(TIMEOUT).build();

    private String credential;

    ControlPlane(URI baseUrl) {
        this.baseUrl = baseUrl;
    }

    /** Whether the product answers its health probe; the readiness signal a launcher waits on. */
    boolean healthy() {
        try {
            HttpResponse<String> response = send(get("/healthz"));
            return response.statusCode() == 200 && "ok".equals(response.body());
        } catch (UncheckedIOException e) {
            return false;
        }
    }

    /**
     * Creates the first admin and holds its token for every later call. Bootstrap is refused off
     * loopback, which is why both tiers run the server on this machine rather than in a container.
     */
    void bootstrapAndLogin(String username, String password) {
        String body = JsonWriter.write(Map.of("username", username, "password", password));
        expect(send(post("/auth/bootstrap", body)), 204, "bootstrap the first admin");
        login(username, password);
    }

    /**
     * Logs an existing admin in and holds its token for every later call. Separate from the bootstrap
     * above because an admin outlives the server that created it: a witness that restarts the server
     * against the same store meets an account that already exists, and bootstrapping again is refused.
     */
    void login(String username, String password) {
        String body = JsonWriter.write(Map.of("username", username, "password", password));
        HttpResponse<String> login = send(post("/auth/login", body));
        expect(login, 200, "log in");
        if (!(JsonReader.parse(login.body()) instanceof Map<?, ?> map)
                || !(map.get("token") instanceof String token)) {
            throw new AssertionError("login returned no token: " + login.body());
        }
        credential = token;
    }

    /**
     * Applies resource documents as one batch, each named by the file it came from. One call, not one
     * per file: the product resolves references within the submitted set, so resources that name each
     * other have to be submitted together.
     */
    void apply(Map<String, String> contentBySource) {
        List<Map<String, String>> drafts = contentBySource.entrySet().stream()
                .map(entry -> Map.of("source", entry.getKey(), "content", entry.getValue()))
                .toList();
        String body = JsonWriter.write(Map.of("drafts", drafts));
        expect(send(authed("/api/artifacts:apply", body)), 200, "apply " + contentBySource.keySet());
    }

    /**
     * Applies a batch the product is expected to refuse, and returns the refusal it answered with.
     *
     * <p>A separate verb rather than a flag on {@link #apply}, for the same reason registration has one:
     * a caller that meant to apply and was refused has failed, and a caller that meant to witness a
     * refusal and got an apply has failed too. One return value cannot mean both.
     */
    Refusal applyExpectingRefusal(Map<String, String> contentBySource) {
        List<Map<String, String>> drafts = contentBySource.entrySet().stream()
                .map(entry -> Map.of("source", entry.getKey(), "content", entry.getValue()))
                .toList();
        String body = JsonWriter.write(Map.of("drafts", drafts));
        HttpResponse<String> response = send(authed("/api/artifacts:apply", body));
        return interpretRefusal(response.statusCode(), response.body(), "applying " + contentBySource.keySet());
    }

    /**
     * One document submitted for apply, optionally carrying the version it was written against. A null
     * {@code expectedContentHash} submits no precondition, which is the unconditional apply every caller
     * above sends.
     */
    record Draft(String source, String content, String expectedContentHash) {}

    /** Applies one document written against the version {@code expectedContentHash} names. */
    void applyExpecting(String source, String content, String expectedContentHash) {
        expect(send(authed("/api/artifacts:apply", applyBody(List.of(
                        new Draft(source, content, expectedContentHash))))),
                200, "apply " + source + " against version " + expectedContentHash);
    }

    /**
     * Attempts an apply carrying a precondition the product is expected to refuse, and returns the refusal.
     * The peer of {@link #applyExpecting}, kept apart from it for the reason every other refusal verb here
     * is kept apart from its success: one return value cannot mean both.
     */
    Refusal applyExpectingRefusal(String source, String content, String expectedContentHash) {
        HttpResponse<String> response = send(authed("/api/artifacts:apply",
                applyBody(List.of(new Draft(source, content, expectedContentHash)))));
        return interpretRefusal(response.statusCode(), response.body(), "applying " + source);
    }

    /**
     * The apply request body. A draft carrying no precondition omits the field entirely rather than sending
     * an explicit null: the field is defined as optional, and a caller that never asked for the check has to
     * travel the wire indistinguishably from one written before the field existed.
     */
    private static String applyBody(List<Draft> drafts) {
        List<Map<String, String>> encoded = drafts.stream()
                .map(draft -> draft.expectedContentHash() == null
                        ? Map.of("source", draft.source(), "content", draft.content())
                        : Map.of("source", draft.source(), "content", draft.content(),
                                "expectedContentHash", draft.expectedContentHash()))
                .toList();
        return JsonWriter.write(Map.of("drafts", encoded));
    }

    /** The ids the server holds - read back from the server, which is the truth, not from the files sent. */
    List<String> artifactIds() {
        HttpResponse<String> response = send(authedGet("/api/artifacts"));
        expect(response, 200, "list artifacts");
        if (!(JsonReader.parse(response.body()) instanceof Map<?, ?> map)
                || !(map.get("artifacts") instanceof List<?> artifacts)) {
            throw new AssertionError("artifact list was not a list: " + response.body());
        }
        return artifacts.stream()
                .map(each -> each instanceof Map<?, ?> m ? m.get("id") : null)
                .map(String::valueOf)
                .toList();
    }

    /**
     * Mints a standing credential at the given scope and answers with it.
     *
     * <p>What a machine is given, as opposed to the session token a person's login returns. A
     * specification that launches something outside this JVM and points it at the product needs one of
     * these: handing over the session token instead would work, and would quietly make that
     * specification a test of a credential nothing in production issues to a peer.
     */
    String mintToken(String scope) {
        HttpResponse<String> response = send(authed("/api/tokens", JsonWriter.write(Map.of("scope", scope))));
        expect(response, 201, "mint a " + scope + " token");
        if (!(JsonReader.parse(response.body()) instanceof Map<?, ?> map)
                || !(map.get("token") instanceof String token)) {
            throw new AssertionError("minting a token returned no token: " + response.body());
        }
        return token;
    }

    /**
     * Removes a declared source, so what is derived from the registry can be watched losing it.
     *
     * <p>Deleting the source rather than the artifact, which is the verb this branch has. The property
     * a caller is watching is that a derived answer follows the registry rather than being snapshotted,
     * and either verb takes the same thing out of it.
     */
    void deleteSource(String sourceId) {
        // Read it first for its ETag: the delete is refused without one, so that a caller working from
        // a stale view of a source cannot remove the version somebody else has since replaced.
        HttpResponse<String> current = send(authedGet("/api/sources/" + sourceId));
        expect(current, 200, "read the source " + sourceId + " before deleting it");
        String tag = current.headers().firstValue("ETag")
                .orElseThrow(() -> new AssertionError("the source " + sourceId + " came back with no ETag"));
        HttpRequest request = HttpRequest.newBuilder(baseUrl.resolve("/api/sources/" + sourceId))
                .timeout(TIMEOUT)
                .header("Authorization", "Bearer " + requireCredential())
                .header("If-Match", tag)
                .DELETE()
                .build();
        HttpResponse<String> response = send(request);
        if (response.statusCode() != 204 && response.statusCode() != 200) {
            throw new AssertionError("could not delete the source " + sourceId + ": HTTP "
                    + response.statusCode() + " - " + response.body());
        }
    }

    /**
     * One stored artifact as the server hands it back, hash included. The hash is the precondition an edit
     * or a removal has to supply, so reading it here is what makes read-then-remove a closed loop over the
     * wire rather than something the caller computes locally off bytes it hopes are the same.
     */
    record StoredArtifact(String id, String kind, String canonicalForm, String contentHash) {}

    /**
     * The artifact stored under {@code id}, or empty when the server holds none.
     *
     * <p>Empty is a reading, not a failure: "it is gone" is the assertion a removal witness makes, and a
     * caller that could not distinguish an absent artifact from a broken read could not make it. Every
     * other non-200 stays loud.
     */
    Optional<StoredArtifact> artifact(String id) {
        HttpResponse<String> response = send(authedGet("/api/artifacts/" + urlSegment(id)));
        if (response.statusCode() == 404) {
            return Optional.empty();
        }
        expect(response, 200, "read the artifact " + id);
        if (!(JsonReader.parse(response.body()) instanceof Map<?, ?> map)) {
            throw new AssertionError("the artifact read was not an object: " + response.body());
        }
        return Optional.of(new StoredArtifact(
                string(map, "id", response.body()),
                string(map, "kind", response.body()),
                string(map, "canonicalForm", response.body()),
                string(map, "contentHash", response.body())));
    }

    /**
     * The content hash of the stored artifact {@code id}, failing when the server holds none.
     *
     * <p>Separate from {@link #artifact} because a caller reading a hash in order to spend it on a removal
     * has already assumed the artifact is there; getting an empty back would fail it one call later, on a
     * line that says nothing about what actually went wrong.
     */
    String contentHash(String id) {
        return artifact(id)
                .orElseThrow(() -> new AssertionError(
                        "no artifact " + id + " to read a content hash from"))
                .contentHash();
    }

    /** Removes the artifact {@code id}, offering {@code expectedContentHash} as the version the caller read. */
    void deleteArtifact(String id, String expectedContentHash) {
        expect(send(authedDelete(id, expectedContentHash)), 204, "delete " + id);
    }

    /**
     * Attempts a removal the product is expected to refuse, and returns the refusal it answered with. A
     * null {@code expectedContentHash} sends no {@code If-Match} at all, which is the unconditional
     * removal the product answers with its own missing-precondition code.
     *
     * <p>A separate verb rather than a flag on {@link #deleteArtifact}, for the reason the apply and
     * register pairs are separate: a caller that meant to remove and was refused has failed, and a caller
     * that meant to witness a refusal and got a removal has failed too - and the second is the regression
     * these callers exist to catch, so it can never be allowed to read as success.
     */
    Refusal deleteArtifactExpectingRefusal(String id, String expectedContentHash) {
        HttpResponse<String> response = send(authedDelete(id, expectedContentHash));
        return interpretRefusal(response.statusCode(), response.body(), "deleting " + id);
    }

    /** Registers a connector's runtime jar; the product makes this idempotent by content hash. */
    void registerConnector(String connectorId, byte[] jar) {
        String body = JsonWriter.write(Map.of("artifact", Base64.getEncoder().encodeToString(jar)));
        expect(send(authed("/api/connectors:register", body, UPLOAD_TIMEOUT)), 200,
                "register the " + connectorId + " connector");
    }

    /**
     * The refusal a rejected verb answered with: the HTTP status, the code the product named, and the
     * named arguments it sent with it.
     *
     * <p>The arguments are here because several refusals are only actionable through them - who is still
     * referencing the resource, what state the pipeline is actually in - and a caller that could read the
     * code but not the arguments would have to assert that a refusal happened without ever checking it
     * named the right thing. They are empty, never null, for a body that carried none.
     */
    record Refusal(int status, String code, Map<String, Object> params) {

        Refusal(int status, String code) {
            this(status, code, Map.of());
        }
    }

    /**
     * Posts an artifact the product is expected to refuse, and returns the refusal it answered with.
     *
     * <p>A separate verb rather than a flag on {@link #registerConnector}: a caller that meant to
     * register and was refused has failed, and a caller that meant to witness a refusal and got a
     * registration has failed too. One return value cannot mean both.
     */
    Refusal registerConnectorExpectingRefusal(byte[] jar) {
        String body = JsonWriter.write(Map.of("artifact", Base64.getEncoder().encodeToString(jar)));
        HttpResponse<String> response = send(authed("/api/connectors:register", body));
        return interpretRefusal(response.statusCode(), response.body(), "registering the artifact");
    }

    /**
     * What an answer to a verb the caller expected to be refused is allowed to mean. Only a client error
     * is a refusal: it is the product having judged the request and declined it, which is the outcome
     * such a caller is witnessing.
     *
     * <p>Every other answer is this specification's own failure and stays loud. A success is the refusal
     * not happening at all - the regression these callers exist to catch. A server failure is the product
     * unable to answer, which says nothing about whether the request was acceptable; read as the expected
     * refusal it would let a server that fell over stand in for a gate that held, and the assertion on
     * the code would then be the only thing between a green run and a hole, on a body that carries no
     * code at all. A redirect is not a judgement either.
     */
    static Refusal interpretRefusal(int status, String body, String what) {
        if (status < 400) {
            throw new AssertionError(
                    "expected " + what + " to be refused, but the product did not refuse it: HTTP " + status
                            + " - " + body);
        }
        if (status >= 500) {
            throw new AssertionError(
                    "expected " + what + " to be refused, but the server failed instead: HTTP " + status
                            + " - " + body);
        }
        return new Refusal(status, codeOf(body), paramsOf(body));
    }

    /** Every connector id the online catalog answers with, registered rows and bundled ones alike. */
    List<String> connectorIds() {
        HttpResponse<String> response = send(authedGet("/api/connectors"));
        expect(response, 200, "list the online connector catalog");
        if (!(JsonReader.parse(response.body()) instanceof Map<?, ?> map)
                || !(map.get("connectors") instanceof List<?> connectors)) {
            throw new AssertionError("the catalog listing carried no connectors: " + response.body());
        }
        return connectors.stream()
                .map(each -> each instanceof Map<?, ?> row ? row.get("id") : null)
                .map(String::valueOf)
                .toList();
    }

    /** Discovers a source's model, which is what a target table is later derived from. */
    void discoverSchema(String resourceId, String connectorId, Map<String, Object> settings) {
        String body = JsonWriter.write(
                Map.of("id", resourceId, "connectorId", connectorId, "settings", settings));
        expect(send(authed("/api/connections:discover-schema", body)), 200, "discover the model of " + resourceId);
    }

    /**
     * Drives a connection test and returns the report body verbatim. The verb probes the connection
     * for real - it inits the connector, discovers, and reads a small sample - so it exercises paths
     * no other verb reaches, which is why a witness that only applies and discovers cannot stand in
     * for it.
     *
     * <p>The body rather than the parsed form of {@link #testConnection}, because what a caller wants
     * here is what the report does <em>not</em> say: a connector fault laundered into a message is
     * invisible once the report is reduced to a status per check.
     */
    String testConnectionBody(String resourceId, String connectorId, Map<String, Object> settings) {
        String body = JsonWriter.write(
                Map.of("id", resourceId, "connectorId", connectorId, "settings", settings));
        HttpResponse<String> response = send(authed("/api/connections:test", body));
        expect(response, 200, "test the connection of " + resourceId);
        return response.body();
    }

    /**
     * The collections the read face lists for a declared source, each as the object it came down as.
     *
     * <p>Entries stay maps rather than becoming a record, and that is the point rather than laziness: what
     * this face promises about a thing nobody could answer is that the key is <em>absent</em>, not that it
     * carries an empty value. Decoding into a typed shape would give every absent key a null and erase
     * exactly the distinction a caller reads.
     */
    List<Map<String, Object>> collections(String sourceId) {
        HttpResponse<String> response = send(authedGet("/api/sources/" + sourceId + "/collections"));
        expect(response, 200, "list the collections of " + sourceId);
        return entriesOf(response.body(), "collections");
    }

    /** The refusal a listing was expected to be met with, read on the same terms every other one is. */
    Refusal collectionsExpectingRefusal(String sourceId) {
        HttpResponse<String> response = send(authedGet("/api/sources/" + sourceId + "/collections"));
        return interpretRefusal(response.statusCode(), response.body(), "listing the collections of " + sourceId);
    }

    /**
     * One preview read, answered whole: the rows, and whatever the face says around them.
     *
     * <p>The request travels as the caller wrote it rather than through named parameters, because several
     * of these specifications exist to send a field the shape does not have and watch it change nothing.
     * A typed argument list could not express that request at all.
     */
    Map<String, Object> find(String sourceId, String collection, Map<String, Object> request) {
        HttpResponse<String> response = send(authed(findPath(sourceId, collection), JsonWriter.write(request)));
        expect(response, 200, "read " + collection + " of " + sourceId);
        if (!(JsonReader.parse(response.body()) instanceof Map<?, ?> map)) {
            throw new AssertionError("a preview answer was not an object: " + response.body());
        }
        return asObject(map);
    }

    /** The refusal a read was expected to be met with, carrying the code the product named. */
    Refusal findExpectingRefusal(String sourceId, String collection, Map<String, Object> request) {
        HttpResponse<String> response = send(authed(findPath(sourceId, collection), JsonWriter.write(request)));
        return interpretRefusal(
                response.statusCode(), response.body(), "reading " + collection + " of " + sourceId);
    }

    /** What the face reports about one collection's size. */
    Map<String, Object> stats(String sourceId, String collection) {
        HttpResponse<String> response =
                send(authedGet("/api/sources/" + sourceId + "/collections/" + collection + "/stats"));
        expect(response, 200, "read the stats of " + collection + " of " + sourceId);
        if (!(JsonReader.parse(response.body()) instanceof Map<?, ?> map)) {
            throw new AssertionError("a stats answer was not an object: " + response.body());
        }
        return asObject(map);
    }

    /** The refusal a stats read was expected to be met with. */
    Refusal statsExpectingRefusal(String sourceId, String collection) {
        HttpResponse<String> response =
                send(authedGet("/api/sources/" + sourceId + "/collections/" + collection + "/stats"));
        return interpretRefusal(
                response.statusCode(), response.body(), "reading the stats of " + collection + " of " + sourceId);
    }

    private static String findPath(String sourceId, String collection) {
        return "/api/sources/" + sourceId + "/collections/" + collection + ":find";
    }

    /**
     * Opens a follow of a collection and collects the changes the product pushes down it.
     *
     * <p>Driven over the websocket rather than through the CLI, for the same reason every other read verb
     * here is: what is under test is the face, and a client in the middle would put its own reading of a
     * request between the caller and the answer. The filter travels in the handshake query as the very
     * JSON a one-shot read sends in its body, so "the same filter reached both faces" is a literal claim
     * rather than an approximate one.
     *
     * <p>The caller closes it. A follow holds a connector instance for as long as it is open, so one left
     * behind counts against the host's ceiling for the rest of the JVM.
     */
    Follow follow(String sourceId, String collection, Map<String, Object> filter) {
        String path = "/api/data-browser/" + sourceId + "/" + collection + "/tail";
        String query = filter == null
                ? ""
                : "?filter=" + URLEncoder.encode(JsonWriter.write(filter), StandardCharsets.UTF_8);
        URI address = URI.create(
                baseUrl.toString().replaceFirst("^http", "ws") + path + query);
        Follow follow = new Follow(address);
        try {
            http.newWebSocketBuilder()
                    .connectTimeout(TIMEOUT)
                    .header("Authorization", "Bearer " + requireCredential())
                    .buildAsync(address, follow)
                    .join();
        } catch (CompletionException refused) {
            throw new AssertionError("could not follow " + collection + " of " + sourceId, refused.getCause());
        }
        return follow;
    }

    /**
     * One open follow, and every change it has been sent.
     *
     * <p>Frames are kept whole and in arrival order. Order is what lets a caller assert that something was
     * <em>not</em> sent: within one follow the store's stream is ordered and skips nothing, so a change
     * the caller knows came last arriving is proof that every earlier one has already been delivered or
     * filtered out. Without that a "it never arrived" assertion is only ever "it had not arrived yet".
     */
    static final class Follow implements AutoCloseable, WebSocket.Listener {

        private final URI address;
        private final List<Map<String, Object>> frames = Collections.synchronizedList(new ArrayList<>());
        private final StringBuilder partial = new StringBuilder();
        private final AtomicReference<String> ended = new AtomicReference<>();

        private volatile WebSocket socket;

        private Follow(URI address) {
            this.address = address;
        }

        /** Every change delivered so far, oldest first. */
        List<Map<String, Object>> frames() {
            synchronized (frames) {
                return List.copyOf(frames);
            }
        }

        /**
         * Waits for the product to end this follow, and answers with what it said when it did.
         *
         * <p>A follow can be refused after its handshake has already succeeded: the upgrade completes,
         * then the read is attempted and may not be servable. The refusal therefore arrives as a close
         * carrying the code, not as a failure to connect - so a caller witnessing one has to wait for
         * the close rather than expect the open to throw.
         */
        String awaitClose(Duration within, String what) {
            long deadline = System.nanoTime() + within.toNanos();
            while (System.nanoTime() - deadline < 0) {
                String closed = ended.get();
                if (closed != null) {
                    return closed;
                }
                sleep();
            }
            throw new AssertionError("waited " + within + " for " + what + " on " + address
                    + ", and the follow was still open, having been sent " + frames());
        }

        /**
         * Waits until a change satisfying the predicate has arrived, and answers with everything delivered
         * up to and including it.
         *
         * <p>Fails rather than returns short on timeout, and says what did arrive: a follow that is not
         * running at all and one that is running and filtering everything out look identical from here,
         * and only the frames that did come tell them apart.
         */
        List<Map<String, Object>> awaitFrame(Predicate<Map<String, Object>> wanted, Duration within, String what) {
            long deadline = System.nanoTime() + within.toNanos();
            while (System.nanoTime() - deadline < 0) {
                List<Map<String, Object>> delivered = frames();
                if (delivered.stream().anyMatch(wanted)) {
                    return delivered;
                }
                String closed = ended.get();
                if (closed != null) {
                    throw new AssertionError("the follow of " + address + " ended (" + closed
                            + ") before " + what + "; it had been sent " + delivered);
                }
                sleep();
            }
            throw new AssertionError("waited " + within + " for " + what + " on " + address
                    + ", and was sent " + frames());
        }

        @Override
        public void onOpen(WebSocket webSocket) {
            socket = webSocket;
            webSocket.request(1);
        }

        @Override
        public CompletionStage<?> onText(WebSocket webSocket, CharSequence data, boolean last) {
            partial.append(data);
            if (last) {
                String text = partial.toString();
                partial.setLength(0);
                if (!(JsonReader.parse(text) instanceof Map<?, ?> frame)) {
                    throw new AssertionError("a followed change was not an object: " + text);
                }
                frames.add(asObject(frame));
            }
            webSocket.request(1);
            return null;
        }

        @Override
        public CompletionStage<?> onClose(WebSocket webSocket, int statusCode, String reason) {
            ended.compareAndSet(null, statusCode + " " + reason);
            return null;
        }

        @Override
        public void onError(WebSocket webSocket, Throwable error) {
            ended.compareAndSet(null, String.valueOf(error));
        }

        /**
         * Closes the follow, and never throws doing it.
         *
         * <p>This runs from a try-with-resources, so anything it threw would be added to whatever the
         * body was already failing with - and a peer that has gone away is the ordinary case here, not
         * a finding. The interesting failure is the assertion; this must not stand in front of it.
         */
        @Override
        public void close() {
            WebSocket open = socket;
            if (open == null) {
                return;
            }
            try {
                open.sendClose(WebSocket.NORMAL_CLOSURE, "done").join();
            } catch (RuntimeException alreadyGone) {
                open.abort();
            }
        }

        private static void sleep() {
            try {
                Thread.sleep(POLL.toMillis());
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new AssertionError("interrupted while following", e);
            }
        }
    }

    /** The list under one key of an answer, each element kept as the object it arrived as. */
    private static List<Map<String, Object>> entriesOf(String body, String key) {
        if (!(JsonReader.parse(body) instanceof Map<?, ?> map) || !(map.get(key) instanceof List<?> entries)) {
            throw new AssertionError("an answer carried no " + key + ": " + body);
        }
        List<Map<String, Object>> out = new ArrayList<>(entries.size());
        for (Object entry : entries) {
            if (!(entry instanceof Map<?, ?> row)) {
                throw new AssertionError("a " + key + " entry was not an object: " + body);
            }
            out.add(asObject(row));
        }
        return out;
    }

    /**
     * A decoded object as a map keyed by string. Keys absent on the wire stay absent here - nothing is
     * filled in - so a caller may assert that the product left a key out.
     */
    private static Map<String, Object> asObject(Map<?, ?> decoded) {
        Map<String, Object> out = new LinkedHashMap<>();
        decoded.forEach((key, value) -> out.put(String.valueOf(key), value));
        return out;
    }

    /**
     * Records a lifecycle intent. The verb's own spelling comes from the product's enum, so the wire
     * word cannot drift from the word the product accepts.
     *
     * <p>A stop does not come through here. The product refuses one that does not say what becomes of
     * the pipeline's state, and a harness that picked an answer on the caller's behalf would be the one
     * place in the system where that question has a default -- so {@link #stop} takes it and every
     * caller has to say. The refusal below is for a caller who reached for the wrong one.
     */
    void lifecycle(String pipelineId, LifecycleVerb verb) {
        if (verb == LifecycleVerb.STOP) {
            throw new IllegalArgumentException(
                    "a stop says what becomes of the pipeline's state: call stop(pipelineId, purgeState)");
        }
        expect(send(authed("/api/pipelines/" + pipelineId + ":" + verb.id(), "")),
                200, verb.id() + " " + pipelineId);
    }

    /**
     * Stops the pipeline, saying whether stopping also clears what it has accumulated -- its resume
     * position and its operators' state.
     */
    void stop(String pipelineId, boolean purgeState) {
        expect(send(authed("/api/pipelines/" + pipelineId + ":" + LifecycleVerb.STOP.id(),
                        "{\"purgeState\":" + purgeState + "}")),
                200, LifecycleVerb.STOP.id() + " " + pipelineId);
    }

    /**
     * The published lifecycle state, or empty when the pipeline has published no observation yet.
     *
     * <p>Empty is a reading, not a failure. Between recording a start intent and the first convergence
     * pass there is no observation at all and the product says so with a coded refusal - so a caller that
     * took that for fatal could never wait for a pipeline to come up, which is the one thing waiting is
     * for. Answering with some state instead would be worse still: it would invent a reading.
     *
     * <p>Empty is weaker than it looks: this read is served from the published observations alone, so a
     * pipeline that was never applied answers exactly as one that is applied and not yet converged. A
     * caller cannot tell "still coming" from "never existed" here, and a wait for the second will spend
     * its whole bound before saying so.
     */
    Optional<PipelineState> state(String pipelineId) {
        HttpResponse<String> response = send(authedGet("/api/pipelines/" + pipelineId + "/status"));
        return interpretState(response.statusCode(), response.body(), pipelineId);
    }

    /**
     * Attempts a status read the product is expected to refuse, and returns the refusal it answered with.
     *
     * <p>Kept apart from {@link #state} because the two emptinesses are not the same thing. That reader
     * treats "no observation published yet" as a reading and keeps every other refusal loud, which is what
     * lets a caller wait for a pipeline to come up. A pipeline that no longer exists is a different answer
     * with a different code, and a caller witnessing that has to see the code rather than an absence it
     * cannot tell from "not converged yet".
     */
    Refusal stateExpectingRefusal(String pipelineId) {
        HttpResponse<String> response = send(authedGet("/api/pipelines/" + urlSegment(pipelineId) + "/status"));
        return interpretRefusal(response.statusCode(), response.body(), "reading the status of " + pipelineId);
    }

    /**
     * The published error count, or empty when the pipeline has published no observation yet.
     *
     * <p>Empty is a reading and not a failure, on the same terms {@link #state} is: the metrics face answers
     * the product's own {@code monitor.no-observation} code for a pipeline no convergence pass has reached,
     * and a wait exists to sit through exactly that window.
     */
    Optional<Long> errorCount(String pipelineId) {
        HttpResponse<String> response = send(authedGet("/api/pipelines/" + pipelineId + "/metrics"));
        return interpretErrorCount(response.statusCode(), response.body(), pipelineId);
    }

    /**
     * The pipeline's node-local log as the product serves it, or the refusal body when there is none.
     *
     * <p>A diagnostic read, never an assertion: it is what a pipeline that reports itself healthy while
     * moving nothing has left to say. Every answer is handed back verbatim, refusals included, because a
     * diagnosis that throws while diagnosing tells the reader less than the refusal would have.
     */
    String logs(String pipelineId) {
        HttpResponse<String> response = send(authedGet("/api/pipelines/" + pipelineId + "/logs"));
        return response.statusCode() + " " + response.body();
    }

    /**
     * Runs the product's own connection test and answers the overall outcome with each check's status.
     *
     * <p>Both halves are returned because the interesting question about this verb is the relationship
     * between them: a check can report a warning while the overall outcome still passes, and a caller
     * that saw only one of the two could not tell that had happened.
     */
    ConnectionTest testConnection(String connectionId, String connectorId, Map<String, Object> settings) {
        String body = JsonWriter.write(
                Map.of("id", connectionId, "connectorId", connectorId, "settings", settings));
        HttpResponse<String> response = send(authed("/api/connections:test", body));
        expect(response, 200, "test the connection " + connectionId);
        if (!(JsonReader.parse(response.body()) instanceof Map<?, ?> report)) {
            throw new AssertionError("the connection test answered no report: " + response.body());
        }
        Map<String, String> statusByCheck = new LinkedHashMap<>();
        if (report.get("checks") instanceof List<?> checks) {
            for (Object each : checks) {
                if (each instanceof Map<?, ?> check) {
                    statusByCheck.put(String.valueOf(check.get("name")), String.valueOf(check.get("status")));
                }
            }
        }
        return new ConnectionTest(String.valueOf(report.get("outcome")), statusByCheck);
    }

    /** A connection test's overall outcome, and the status each individual check reported. */
    record ConnectionTest(String outcome, Map<String, String> statusByCheck) {
    }

    /** The published metrics body verbatim, for the same diagnostic use and on the same terms as {@link #logs}. */
    String metrics(String pipelineId) {
        HttpResponse<String> response = send(authedGet("/api/pipelines/" + pipelineId + "/metrics"));
        return response.statusCode() + " " + response.body();
    }

    /**
     * The canonical code of the published failure, or empty when the pipeline has published none — it is
     * healthy, or no convergence pass has reached it yet.
     *
     * <p>Empty is a reading and not a failure, on the same terms {@link #state} is. The two emptinesses are
     * deliberately one here: a specification asserting a failure code is waiting for a run to die, and
     * "not dead yet" and "no observation yet" are both answered by waiting.
     */
    Optional<String> failureCode(String pipelineId) {
        HttpResponse<String> response = send(authedGet("/api/pipelines/" + pipelineId + "/status"));
        return interpretFailureCode(response.statusCode(), response.body(), pipelineId);
    }

    /**
     * What a status answer is allowed to say about a failure, read the way the two above are: only the
     * product's own {@code monitor.no-observation} code reads as "nothing published yet", every other refusal
     * stays loud. A healthy pipeline carries no failure field at all, which is the empty reading; a failure
     * present but missing its code is a regression of the status contract and is surfaced rather than waited
     * out.
     */
    static Optional<String> interpretFailureCode(int status, String body, String pipelineId) {
        if (status == 404 && MonitorError.NO_OBSERVATION.code().equals(codeOf(body))) {
            return Optional.empty();
        }
        if (status != 200) {
            throw new AssertionError(
                    "could not read the status of " + pipelineId + ": expected HTTP 200, got " + status
                            + " - " + body);
        }
        if (!(JsonReader.parse(body) instanceof Map<?, ?> map)) {
            throw new AssertionError("status answer was not an object: " + body);
        }
        if (!(map.get("failure") instanceof Map<?, ?> failure)) {
            return Optional.empty();
        }
        if (!(failure.get("code") instanceof String code)) {
            throw new AssertionError("status carried a failure with no code: " + body);
        }
        return Optional.of(code);
    }

    /**
     * What a status answer is allowed to mean. Only the product's own {@code monitor.no-observation} code
     * reads as "nothing published yet"; every other refusal stays loud, another code's 404 included. A rule
     * written on the status alone would let a route that 404s for its own reasons pass for a pipeline that
     * is merely slow to converge, and the specification would sit out its whole bound and then blame the
     * data. The code is the product's contract for exactly this distinction, so the code is what is read.
     */
    static Optional<PipelineState> interpretState(int status, String body, String pipelineId) {
        if (status == 404 && MonitorError.NO_OBSERVATION.code().equals(codeOf(body))) {
            return Optional.empty();
        }
        if (status != 200) {
            throw new AssertionError(
                    "could not read the status of " + pipelineId + ": expected HTTP 200, got " + status
                            + " - " + body);
        }
        if (!(JsonReader.parse(body) instanceof Map<?, ?> map) || !(map.get("state") instanceof String state)) {
            throw new AssertionError("status carried no state: " + body);
        }
        return Optional.of(PipelineState.valueOf(state));
    }

    /**
     * What a metrics answer is allowed to mean, read exactly the way a status answer is: only the product's
     * own {@code monitor.no-observation} code reads as "nothing published yet", and every other refusal stays
     * loud. A published observation always carries the errorCount metric - the runtime derives it from the
     * actual state - so a 200 that omits it is a regression of that contract, surfaced rather than waited out
     * as though the pipeline were merely slow to converge.
     */
    static Optional<Long> interpretErrorCount(int status, String body, String pipelineId) {
        if (status == 404 && MonitorError.NO_OBSERVATION.code().equals(codeOf(body))) {
            return Optional.empty();
        }
        if (status != 200) {
            throw new AssertionError(
                    "could not read the metrics of " + pipelineId + ": expected HTTP 200, got " + status
                            + " - " + body);
        }
        if (!(JsonReader.parse(body) instanceof Map<?, ?> map) || !(map.get("metrics") instanceof Map<?, ?> metrics)) {
            throw new AssertionError("metrics answer carried no metrics: " + body);
        }
        if (!(metrics.get("errorCount") instanceof Number errorCount)) {
            throw new AssertionError("metrics carried no errorCount: " + body);
        }
        return Optional.of(errorCount.longValue());
    }

    /**
     * How many changes this pipeline's nests could never place in a document, added up over every namespace
     * that reported any, or empty when the pipeline has published no observation yet.
     *
     * <p>Summed rather than keyed by namespace on purpose. A namespace name is derived from the pipeline and
     * the embed path inside it, so asking a specification to name one would be asking an author to copy an
     * internal name by hand - and to rewrite their assertion whenever a step is renamed. What a specification
     * is asking is whether this pipeline threw anything away, which is one number.
     *
     * <p>Absent namespaces read as nothing discarded rather than as nothing measured, which is the opposite
     * of how the readings around it are treated and is right here: this metric is published only where rows
     * were lost, so no entry is the healthy answer rather than an unwired one.
     */
    Optional<Long> deadLettered(String pipelineId) {
        HttpResponse<String> response = send(authedGet("/api/pipelines/" + pipelineId + "/metrics"));
        return interpretDeadLettered(response.statusCode(), response.body(), pipelineId);
    }

    /** What a metrics answer says about discarded changes, read exactly the way the error count is. */
    static Optional<Long> interpretDeadLettered(int status, String body, String pipelineId) {
        return interpretMetricTotal(status, body, pipelineId, DEAD_LETTERED_PREFIX);
    }

    /**
     * Every metric this pipeline publishes under {@code prefix}, added up, on the same terms as the reading
     * above: summed rather than keyed by namespace, because a namespace name is derived from the pipeline
     * and the embed path inside it and no specification should be copying one by hand.
     *
     * <p>Absent names read as zero rather than as unmeasured, which is right for a metric published only
     * where there is something to say. It leaves the caller with the discriminating half to do: a witness
     * resting on "zero" alone would pass on a pipeline that published nothing at all.
     */
    Optional<Long> metricTotal(String pipelineId, String prefix) {
        HttpResponse<String> response = send(authedGet("/api/pipelines/" + pipelineId + "/metrics"));
        return interpretMetricTotal(response.statusCode(), response.body(), pipelineId, prefix);
    }

    static Optional<Long> interpretMetricTotal(int status, String body, String pipelineId, String prefix) {
        if (status == 404 && MonitorError.NO_OBSERVATION.code().equals(codeOf(body))) {
            return Optional.empty();
        }
        if (status != 200) {
            throw new AssertionError(
                    "could not read the metrics of " + pipelineId + ": expected HTTP 200, got " + status
                            + " - " + body);
        }
        if (!(JsonReader.parse(body) instanceof Map<?, ?> map) || !(map.get("metrics") instanceof Map<?, ?> metrics)) {
            throw new AssertionError("metrics answer carried no metrics: " + body);
        }
        long total = 0L;
        for (Map.Entry<?, ?> entry : metrics.entrySet()) {
            if (entry.getKey() instanceof String name && name.startsWith(prefix)
                    && entry.getValue() instanceof Number count) {
                total += count.longValue();
            }
        }
        return Optional.of(total);
    }

    /**
     * How many records this pipeline's live job has driven to its sinks, or empty when it has no live job
     * or has published no observation yet.
     *
     * <p>This is a count of writes, not of source changes, which is what makes it the reading a witness
     * of coalescing rests on: a node that folds several changes into one send costs several changes here
     * and one record. Cumulative over a run, so what a witness compares is two readings of it.
     */
    Optional<Long> recordCount(String pipelineId) {
        HttpResponse<String> response = send(authedGet("/api/pipelines/" + pipelineId + "/metrics"));
        return interpretRecordCount(response.statusCode(), response.body(), pipelineId);
    }

    /**
     * How many rows each selected table's full load has read <em>on the run that is live now</em>, keyed by
     * table. Empty when the pipeline has no live run.
     *
     * <p>The per-run scope is the whole reason a witness reads this rather than the target: a resumed run
     * that skips a table reports zero for it, while the target still holds every row the earlier run put
     * there, so the target cannot tell a skipped table from a re-read one. Every selected table appears --
     * one that was not read reports zero rather than going absent -- so a missing key is a broken reading
     * and not a table that was skipped.
     */
    Map<String, Long> snapshotRowsRead(String pipelineId) {
        HttpResponse<String> response = send(authedGet("/api/pipelines/" + pipelineId + "/snapshot"));
        if (response.statusCode() == 404 && MonitorError.NO_OBSERVATION.code().equals(codeOf(response.body()))) {
            return Map.of();
        }
        if (response.statusCode() != 200) {
            throw new AssertionError("could not read the snapshot progress of " + pipelineId
                    + ": expected HTTP 200, got " + response.statusCode() + " - " + response.body());
        }
        if (!(JsonReader.parse(response.body()) instanceof Map<?, ?> map)
                || !(map.get("snapshot") instanceof Map<?, ?> snapshot)) {
            throw new AssertionError("snapshot answer carried no snapshot: " + response.body());
        }
        Map<String, Long> read = new LinkedHashMap<>();
        for (Map.Entry<?, ?> entry : snapshot.entrySet()) {
            if (entry.getValue() instanceof Map<?, ?> table && table.get("rowsDone") instanceof Number done) {
                read.put(String.valueOf(entry.getKey()), done.longValue());
            }
        }
        return read;
    }

    static Optional<Long> interpretRecordCount(int status, String body, String pipelineId) {
        if (status == 404 && MonitorError.NO_OBSERVATION.code().equals(codeOf(body))) {
            return Optional.empty();
        }
        if (status != 200) {
            throw new AssertionError(
                    "could not read the metrics of " + pipelineId + ": expected HTTP 200, got " + status
                            + " - " + body);
        }
        if (!(JsonReader.parse(body) instanceof Map<?, ?> map)
                || !(map.get("metrics") instanceof Map<?, ?> metrics)) {
            throw new AssertionError("metrics answer carried no metrics: " + body);
        }
        // Absent while no job is live, which is a real reading and not a broken face: the count comes from
        // the run itself, so a pipeline between runs has none rather than zero.
        return metrics.get("recordCount") instanceof Number count
                ? Optional.of(count.longValue())
                : Optional.empty();
    }

    /**
     * The durable source position this pipeline has acked for one table, or empty when it has acked none
     * there yet. This is the frontier as a reader sees it: below it, every change is either at a sink or
     * held somewhere it survives a restart from.
     *
     * <p>Returned as the opaque string the product publishes, never parsed. A position's shape belongs to
     * the connector that issued it, so a witness may ask whether this reading differs from an earlier one -
     * that is what "the frontier moved" means here - but never whether one is greater than another.
     */
    Optional<String> durablePosition(String pipelineId, String table) {
        HttpResponse<String> response = send(authedGet("/api/pipelines/" + pipelineId + "/metrics"));
        return interpretDurablePosition(response.statusCode(), response.body(), pipelineId, table);
    }

    static Optional<String> interpretDurablePosition(
            int status, String body, String pipelineId, String table) {
        if (status == 404 && MonitorError.NO_OBSERVATION.code().equals(codeOf(body))) {
            return Optional.empty();
        }
        if (status != 200) {
            throw new AssertionError(
                    "could not read the metrics of " + pipelineId + ": expected HTTP 200, got " + status
                            + " - " + body);
        }
        if (!(JsonReader.parse(body) instanceof Map<?, ?> map)) {
            throw new AssertionError("metrics answer did not parse: " + body);
        }
        // Absent until a position is acked, and absent is a real reading here rather than a broken face.
        if (!(map.get("perTableOffset") instanceof Map<?, ?> offsets)) {
            return Optional.empty();
        }
        return offsets.get(table) instanceof String position ? Optional.of(position) : Optional.empty();
    }

    /**
     * The code a structured error body carries, or null for a body that is not one - a body that does not
     * parse included. A refusal can come from something that is not the product at all (an empty body, a
     * proxy's HTML), and the caller's job is to report that loudly with the pipeline and status named; it
     * cannot do that if reading the body for a code throws a parse error over the top of it.
     */
    private static String codeOf(String body) {
        try {
            return JsonReader.parse(body) instanceof Map<?, ?> map && map.get("code") instanceof String code
                    ? code
                    : null;
        } catch (RuntimeException e) {
            return null;
        }
    }

    /**
     * The named arguments a structured error body carries, or none for a body that is not one. Read on the
     * same terms as {@link #codeOf}: a refusal that came from something other than the product must still be
     * reportable, so an unparseable body answers empty rather than throwing over the top of the report.
     */
    private static Map<String, Object> paramsOf(String body) {
        try {
            if (!(JsonReader.parse(body) instanceof Map<?, ?> map)
                    || !(map.get("params") instanceof Map<?, ?> params)) {
                return Map.of();
            }
            // Copied through a map that tolerates a null value rather than Map.copyOf, which rejects one. A
            // single null argument would otherwise throw here and lose the whole refusal - code, message and
            // all - which is the failure this harness exists to report, not to suffer.
            Map<String, Object> copy = new LinkedHashMap<>();
            params.forEach((key, value) -> copy.put(String.valueOf(key), value));
            return Collections.unmodifiableMap(copy);
        } catch (RuntimeException e) {
            return Map.of();
        }
    }

    /** A required string field of a structured answer, named in the failure when the answer omits it. */
    private static String string(Map<?, ?> map, String field, String body) {
        if (!(map.get(field) instanceof String value)) {
            throw new AssertionError("the artifact read carried no " + field + ": " + body);
        }
        return value;
    }

    /**
     * One path segment, encoded the way the product's own clients encode it - form encoding with the plus
     * put back as {@code %20}, because a literal plus in a path is a plus and not a space.
     *
     * <p>Written out here rather than borrowed: the harness travels the public wire, so what it needs is an
     * id that survives the trip, not the product's private helper. Ids that need any of this are pinned
     * where the client builds the request; over this wire it keeps a specification honest about the address
     * it asked for.
     */
    private static String urlSegment(String value) {
        return URLEncoder.encode(value, StandardCharsets.UTF_8).replace("+", "%20");
    }

    private HttpRequest get(String path) {
        return HttpRequest.newBuilder(baseUrl.resolve(path)).timeout(TIMEOUT).GET().build();
    }

    /**
     * A conditional removal. A null hash sends no {@code If-Match} header at all rather than an empty one:
     * an empty header is a malformed precondition, and the caller that passes null is witnessing the
     * refusal of a removal that carried no precondition in the first place.
     */
    private HttpRequest authedDelete(String id, String expectedContentHash) {
        HttpRequest.Builder builder = HttpRequest.newBuilder(baseUrl.resolve("/api/artifacts/" + urlSegment(id)))
                .timeout(TIMEOUT)
                .header("Authorization", "Bearer " + requireCredential())
                .DELETE();
        if (expectedContentHash != null) {
            builder.header("If-Match", "\"" + expectedContentHash + "\"");
        }
        return builder.build();
    }

    private HttpRequest authedGet(String path) {
        return HttpRequest.newBuilder(baseUrl.resolve(path))
                .timeout(TIMEOUT)
                .header("Authorization", "Bearer " + requireCredential())
                .GET()
                .build();
    }

    private HttpRequest post(String path, String body) {
        return HttpRequest.newBuilder(baseUrl.resolve(path))
                .timeout(TIMEOUT)
                .header("Content-Type", "application/json")
                .POST(HttpRequest.BodyPublishers.ofString(body, StandardCharsets.UTF_8))
                .build();
    }

    private HttpRequest authed(String path, String body) {
        return authed(path, body, TIMEOUT);
    }

    /** The same request with a bound of the caller's own, for a body large enough to need one. */
    private HttpRequest authed(String path, String body, Duration timeout) {
        return HttpRequest.newBuilder(baseUrl.resolve(path))
                .timeout(timeout)
                .header("Content-Type", "application/json")
                .header("Authorization", "Bearer " + requireCredential())
                .POST(HttpRequest.BodyPublishers.ofString(body, StandardCharsets.UTF_8))
                .build();
    }

    /**
     * The bearer credential this harness logged in with, for handing to a second client that has to reach
     * the same server as the same principal - the shipped MCP sidecar, which is configured with a token
     * rather than a login.
     */
    String credential() {
        return requireCredential();
    }

    private String requireCredential() {
        if (credential == null) {
            throw new IllegalStateException("no credential: log in before driving an authenticated verb");
        }
        return credential;
    }

    private HttpResponse<String> send(HttpRequest request) {
        try {
            return http.send(request, HttpResponse.BodyHandlers.ofString(StandardCharsets.UTF_8));
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new AssertionError("interrupted while calling " + request.uri(), e);
        }
    }

    private static void expect(HttpResponse<String> response, int status, String what) {
        if (response.statusCode() != status) {
            throw new AssertionError(
                    "could not " + what + ": expected HTTP " + status + ", got " + response.statusCode()
                            + " - " + response.body());
        }
    }
}
