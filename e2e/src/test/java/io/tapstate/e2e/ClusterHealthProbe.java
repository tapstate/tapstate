package io.tapstate.e2e;

import io.tapstate.core.common.JsonWriter;
import io.tapstate.core.common.Domain;
import io.tapstate.core.lifecycle.PipelineState;
import java.io.IOException;
import java.net.URI;
import java.net.http.HttpRequest;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.LinkOption;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.nio.file.attribute.PosixFilePermissions;
import java.security.MessageDigest;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collections;
import java.util.IdentityHashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.Optional;
import java.util.function.Supplier;
import java.util.UUID;

/** Optional buffered observations of actual health and status responses; never alternate requests. */
final class ClusterHealthProbe {
    private static final ThreadLocal<Scope> ACTIVE = new ThreadLocal<>();
    private static final int MAX_SAMPLES = 4096;
    private ClusterHealthProbe() { }

    static Scope install(String caseHash, String run, String attempt) {
        if (!caseHash.matches("[0-9a-f]{64}") || !(run.equals("local") || run.matches("[1-9][0-9]*"))
                || !attempt.matches("[1-9][0-9]*")) throw new IllegalArgumentException("invalid health capture identity");
        Scope scope = new Scope(ACTIVE.get(), caseHash, run, attempt);
        ACTIVE.set(scope);
        return scope;
    }
    static Scope activeScope() { return ACTIVE.get(); }
    static long started() { return ACTIVE.get() == null ? 0 : System.nanoTime(); }
    static void bind(long pid, URI base) {
        Scope scope = ACTIVE.get();
        if (scope == null) return;
        try { scope.bind(pid, base.resolve("/healthz")); }
        catch (RuntimeException | Error failure) { scope.complete = false; }
    }
    static void response(HttpRequest request, int status, String body, long started) {
        observe(request, status, body, null, started);
    }
    static void failure(HttpRequest request, Throwable failure, long started) {
        observe(request, 0, null, failure, started);
    }
    static void pipelineStatus(HttpRequest request, int status, String body, String pipeline, Optional<PipelineState> state,
            Supplier<Optional<String>> failureCode) {
        Scope scope = ACTIVE.get();
        if (scope == null) return;
        long begin = System.nanoTime();
        try { scope.pipelineStatus(request.uri(), status, body, pipeline, state, failureCode, begin); }
        catch (RuntimeException | Error observationFailure) { scope.complete = false; }
        finally { scope.overhead += System.nanoTime() - begin; }
    }
    private static void observe(HttpRequest request, int status, String body, Throwable failure, long started) {
        Scope scope = ACTIVE.get();
        if (scope == null) return;
        long begin = System.nanoTime();
        try { scope.observe(request.uri(), status, body, failure, started, begin); }
        catch (RuntimeException | Error observationFailure) { scope.complete = false; }
        finally { scope.overhead += System.nanoTime() - begin; }
    }
    static String hash(String value) {
        try { return java.util.HexFormat.of().formatHex(MessageDigest.getInstance("SHA-256")
                .digest(value.getBytes(StandardCharsets.UTF_8))); }
        catch (java.security.NoSuchAlgorithmException failure) { throw new AssertionError(failure); }
    }
    private record Launch(String id, long pid, int port) { }

    static final class Scope implements AutoCloseable {
        private final Scope previous;
        private final Thread owner = Thread.currentThread();
        private final String caseHash;
        private final String run;
        private final String attempt;
        private final String startedAt = Instant.now().toString();
        private final long startNanos = System.nanoTime();
        private final Map<URI, Launch> launches = new LinkedHashMap<>();
        private final List<Map<String, Object>> samples = new ArrayList<>();
        private final List<Map<String, Object>> pipelineStatus = new ArrayList<>();
        private volatile boolean complete = true;
        private long overhead;
        private boolean closed;
        private boolean requestedClose;
        private boolean written;
        private Scope(Scope previous, String caseHash, String run, String attempt) {
            this.previous = previous; this.caseHash = caseHash; this.run = run; this.attempt = attempt;
        }
        private void bind(long pid, URI uri) {
            if (closed || pid <= 0 || uri.getPort() < 1 || launches.containsKey(uri)) {
                complete = false; return;
            }
            launches.put(uri, new Launch(UUID.randomUUID().toString(), pid, uri.getPort()));
        }
        private void observe(URI uri, int status, String body, Throwable failure, long started, long finished) {
            if (closed || samples.size() + pipelineStatus.size() >= MAX_SAMPLES) { complete = false; return; }
            Launch launch = launches.get(uri);
            if (launch == null) complete = false;
            boolean recognized = "http".equals(uri.getScheme())
                    && Set.of("127.0.0.1", "localhost", "[::1]", "::1").contains(uri.getHost())
                    && "/healthz".equals(uri.getPath()) && uri.getUserInfo() == null
                    && uri.getQuery() == null && uri.getFragment() == null;
            if (!recognized || finished < started) complete = false;
            List<String> causes = new ArrayList<>();
            Set<Throwable> seen = Collections.newSetFromMap(new IdentityHashMap<>());
            Throwable cause = failure;
            while (cause != null && causes.size() < 8 && seen.add(cause)) {
                causes.add(cause.getClass().getName()); cause = cause.getCause();
            }
            if (cause != null) complete = false;
            Map<String, Object> sample = new LinkedHashMap<>();
            sample.put("launch", launch == null ? "unknown" : launch.id());
            sample.put("pid", launch == null ? -1L : launch.pid());
            sample.put("http_port", uri.getPort());
            sample.put("request_sha256", hash(uri.toString()));
            sample.put("endpoint", recognized ? "loopback-healthz" : "unrecognized");
            sample.put("started_monotonic_ns", started); sample.put("finished_monotonic_ns", finished);
            sample.put("http_status", status);
            sample.put("body_kind", body == null ? "no-response" : "ok".equals(body) ? "ok" : "broken".equals(body) ? "broken" : "other");
            sample.put("body_sha256", body == null ? "unknown" : hash(body));
            sample.put("healthy", status == 200 && "ok".equals(body));
            sample.put("exception_classes", List.copyOf(causes));
            samples.add(sample);
        }
        private void pipelineStatus(URI uri, int status, String body, String pipeline, Optional<PipelineState> state,
                Supplier<Optional<String>> failureCode, long nanos) {
            if (closed || samples.size() + pipelineStatus.size() >= MAX_SAMPLES) { complete = false; return; }
            Launch launch = launches.get(uri.resolve("/healthz"));
            if (launch == null) complete = false;
            String code = "unpublished";
            try {
                Optional<String> published = failureCode.get();
                if (published.isPresent()) {
                    String candidate = published.get();
                    int dot = candidate.indexOf('.');
                    String segment = "[a-z][a-z0-9]*(-[a-z0-9]+)*";
                    if (candidate.length() <= 1024 && (candidate.startsWith("pdk.")
                            || candidate.startsWith("connector.") && candidate.indexOf('.', "connector.".length()) >= 0)) {
                        // Connector-specific code namespaces remain an explicit projection gap.
                        complete = false; code = "unsupported-namespace";
                    } else if (candidate.length() <= 1024 && dot > 0
                            && candidate.substring(0, dot).matches(segment)
                            && candidate.substring(dot + 1).matches(segment)
                            && Domain.isRegistered(candidate.substring(0, dot))) code = candidate;
                    else { complete = false; code = "noncanonical"; }
                }
            } catch (RuntimeException | Error failure) { complete = false; code = "unavailable"; }
            Map<String, Object> sample = new LinkedHashMap<>();
            sample.put("launch", launch == null ? "unknown" : launch.id());
            sample.put("pid", launch == null ? -1L : launch.pid());
            sample.put("http_port", uri.getPort()); sample.put("request_sha256", hash(uri.toString()));
            sample.put("observed_monotonic_ns", nanos); sample.put("http_status", status);
            sample.put("pipeline_sha256", hash(pipeline)); sample.put("state", state.map(PipelineState::name).orElse("unpublished"));
            sample.put("failure_code", code); sample.put("body_sha256", hash(body));
            pipelineStatus.add(sample);
        }
        boolean complete() { return complete; }
        void incomplete() { complete = false; }
        Path write(Path directory, boolean testFailed) throws IOException {
            if (!closed || written || Thread.currentThread() != owner) throw new IOException("health capture is not closed by its owner");
            Path absolute = directory.toAbsolutePath().normalize();
            for (Path part = absolute; part != null; part = part.getParent())
                if (Files.isSymbolicLink(part)) throw new IOException("health capture directory contains a link");
            if (!Files.isDirectory(absolute, LinkOption.NOFOLLOW_LINKS)) throw new IOException("owned health capture directory unavailable");
            Map<String, Object> value = new LinkedHashMap<>();
            value.put("schema", 1); value.put("type", "actual-cluster-health-requests");
            value.put("case_sha256", caseHash); value.put("run_id", run); value.put("run_attempt", attempt);
            value.put("started_at", startedAt); value.put("scope_start_monotonic_ns", startNanos);
            value.put("test_outcome", testFailed ? "failed" : "returned");
            value.put("capture_complete", complete); value.put("samples", List.copyOf(samples));
            value.put("pipeline_status", List.copyOf(pipelineStatus));
            value.put("collection_overhead_ns", overhead); value.put("write_cost_complete", false);
            Path file = absolute.resolve("cluster-health-probe-" + UUID.randomUUID() + ".json");
            byte[] bytes = (JsonWriter.write(value) + "\n").getBytes(StandardCharsets.UTF_8);
            try (var channel = Files.newByteChannel(file, Set.of(StandardOpenOption.CREATE_NEW, StandardOpenOption.WRITE,
                    LinkOption.NOFOLLOW_LINKS), PosixFilePermissions.asFileAttribute(PosixFilePermissions.fromString("rw-------")))) {
                java.nio.ByteBuffer buffer = java.nio.ByteBuffer.wrap(bytes);
                while (buffer.hasRemaining()) channel.write(buffer);
            }
            written = true;
            return file;
        }
        @Override public void close() {
            if (Thread.currentThread() != owner || closed) { complete = false; return; }
            requestedClose = true;
            if (ACTIVE.get() != this) { complete = false; return; }
            closed = true;
            Scope restore = previous;
            while (restore != null && restore.owner == owner && restore.requestedClose) {
                restore.closed = true; restore = restore.previous;
            }
            if (restore == null) ACTIVE.remove(); else ACTIVE.set(restore);
        }
    }
}
