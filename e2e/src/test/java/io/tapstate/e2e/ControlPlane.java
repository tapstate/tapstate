package io.tapstate.e2e;

import io.tapstate.control.core.MonitorError;
import io.tapstate.core.common.JsonReader;
import io.tapstate.core.common.JsonWriter;
import io.tapstate.core.lifecycle.LifecycleVerb;
import io.tapstate.core.lifecycle.PipelineState;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.Base64;
import java.util.List;
import java.util.Map;
import java.util.Optional;

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

    /** Registers a connector's runtime jar; the product makes this idempotent by content hash. */
    void registerConnector(String connectorId, byte[] jar) {
        String body = JsonWriter.write(Map.of("artifact", Base64.getEncoder().encodeToString(jar)));
        expect(send(authed("/api/connectors:register", body)), 200, "register the " + connectorId + " connector");
    }

    /** The refusal a rejected verb answered with: the HTTP status, and the code the product named. */
    record Refusal(int status, String code) {}

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
        return new Refusal(status, codeOf(body));
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
     * Records a lifecycle intent. The verb's own spelling comes from the product's enum, so the wire
     * word cannot drift from the word the product accepts.
     */
    void lifecycle(String pipelineId, LifecycleVerb verb) {
        expect(send(authed("/api/pipelines/" + pipelineId + ":" + verb.id(), "")),
                200, verb.id() + " " + pipelineId);
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

    private HttpRequest get(String path) {
        return HttpRequest.newBuilder(baseUrl.resolve(path)).timeout(TIMEOUT).GET().build();
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
        return HttpRequest.newBuilder(baseUrl.resolve(path))
                .timeout(TIMEOUT)
                .header("Content-Type", "application/json")
                .header("Authorization", "Bearer " + requireCredential())
                .POST(HttpRequest.BodyPublishers.ofString(body, StandardCharsets.UTF_8))
                .build();
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
