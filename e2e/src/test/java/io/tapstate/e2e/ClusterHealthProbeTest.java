package io.tapstate.e2e;

import com.sun.net.httpserver.HttpServer;
import io.tapstate.core.common.JsonReader;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.net.InetSocketAddress;
import java.net.URI;
import java.net.http.HttpRequest;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.attribute.PosixFilePermissions;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.io.TempDir;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Real HTTP controls discriminate probe outcomes from optional diagnostic collection. */
class ClusterHealthProbeTest {
    @TempDir Path scratch;
    @BeforeEach void useTheCanonicalOwnedTemporaryDirectory() throws IOException { scratch = scratch.toRealPath(); }
    private static final String CASE = ClusterHealthProbe.hash("fixture invocation");

    @Test void recordsTheActualStatusAndExactBodyWithoutChangingTheHealthDecision() throws Exception {
        List<Integer> statuses = List.of(200, 503, 200);
        List<String> bodies = List.of("ok", "broken", "health-private-response-sentinel");
        AtomicInteger next = new AtomicInteger();
        HttpServer server = server(exchange -> {
            int index = next.getAndIncrement();
            byte[] body = bodies.get(index).getBytes(StandardCharsets.UTF_8);
            exchange.sendResponseHeaders(statuses.get(index), body.length);
            try (var output = exchange.getResponseBody()) { output.write(body); }
        });
        URI base = base(server);
        var scope = ClusterHealthProbe.install(CASE, "local", "1");
        try {
            ClusterHealthProbe.bind(123, base);
            ControlPlane control = new ControlPlane(base);
            assertThat(control.healthy()).isTrue();
            assertThat(control.healthy()).isFalse();
            assertThat(control.healthy()).isFalse();
        } finally { scope.close(); server.stop(0); }
        Path file = scope.write(scratch, false);
        Map<?, ?> capture = read(file);
        List<?> samples = (List<?>) capture.get("samples");
        assertThat(samples).hasSize(3);
        assertThat(samples.stream().map(row -> ((Map<?, ?>) row).get("http_status")).toList()).isEqualTo(List.of(200L, 503L, 200L));
        assertThat(samples.stream().map(row -> ((Map<?, ?>) row).get("body_kind")).toList()).isEqualTo(List.of("ok", "broken", "other"));
        assertThat(samples.stream().map(row -> ((Map<?, ?>) row).get("healthy")).toList()).isEqualTo(List.of(true, false, false));
        assertThat(samples.stream().map(row -> ((Map<?, ?>) row).get("pid")).toList()).isEqualTo(List.of(123L, 123L, 123L));
        assertThat(capture.get("capture_complete")).isEqualTo(true);
        assertThat(Files.readString(file)).doesNotContain(base.toString(), bodies.get(2));
        assertThat(PosixFilePermissions.toString(Files.getPosixFilePermissions(file))).isEqualTo("rw-------");
        assertThat(capture.get("write_cost_complete")).isEqualTo(false);
    }

    @Test void retainsConnectionFailureClassesAndStillReturnsTheOriginalFalse() throws Exception {
        HttpServer server = server(exchange -> exchange.close());
        URI base = base(server); server.stop(0);
        var scope = ClusterHealthProbe.install(CASE, "local", "1");
        try {
            ClusterHealthProbe.bind(234, base);
            assertThat(new ControlPlane(base).healthy()).isFalse();
        } finally { scope.close(); }
        Map<?, ?> sample = (Map<?, ?>) ((List<?>) read(scope.write(scratch, true)).get("samples")).getFirst();
        assertThat(sample.get("http_status")).isEqualTo(0L);
        assertThat(sample.get("body_kind")).isEqualTo("no-response");
        assertThat(sample.get("exception_classes")).asList().contains("java.io.UncheckedIOException", "java.net.ConnectException");
    }

    @Test void anUnboundOrOverflowedCollectorCannotChangeARealSuccessfulProbe() throws Exception {
        HttpServer server = server(exchange -> {
            byte[] body = "ok".getBytes(StandardCharsets.UTF_8);
            exchange.sendResponseHeaders(200, body.length);
            try (var output = exchange.getResponseBody()) { output.write(body); }
        });
        URI base = base(server);
        var scope = ClusterHealthProbe.install(CASE, "local", "1");
        try {
            assertThat(new ControlPlane(base).healthy()).isTrue();
            ClusterHealthProbe.bind(345, base);
            HttpRequest request = HttpRequest.newBuilder(base.resolve("/healthz")).build();
            for (int i = 0; i < 4100; i++) ClusterHealthProbe.response(request, 200, "ok", System.nanoTime());
            assertThat(new ControlPlane(base).healthy()).isTrue();
        } finally { scope.close(); server.stop(0); }
        Map<?, ?> capture = read(scope.write(scratch, false));
        assertThat(capture.get("capture_complete")).isEqualTo(false);
        assertThat(((List<?>) capture.get("samples")).size()).isLessThan(4100);
    }

    @Test void exceptionMessagesAndUnexpectedUriComponentsNeverLeaveTheBuffer() throws Exception {
        URI base = URI.create("http://private-user:private-password@127.0.0.1:12345/private-path?private-query#private-fragment");
        var scope = ClusterHealthProbe.install(CASE, "local", "1");
        try {
            HttpRequest request = HttpRequest.newBuilder(base).build();
            ClusterHealthProbe.failure(request, new UncheckedIOException(new IOException("private-error " + base)), System.nanoTime());
        } finally { scope.close(); }
        Path file = scope.write(scratch, false);
        assertThat(Files.readString(file)).doesNotContain("private-", "http://", "127.0.0.1");
        assertThat(read(file).get("capture_complete")).isEqualTo(false);
    }

    @Test void nestedAndForeignClosesRestoreTheOwningInvocationWithoutMixingLaunches() throws Exception {
        var before = ClusterHealthProbe.activeScope();
        var outer = ClusterHealthProbe.install(CASE, "42", "1");
        var inner = ClusterHealthProbe.install(ClusterHealthProbe.hash("different invocation"), "42", "1");
        URI first = URI.create("http://127.0.0.1:12345"), second = URI.create("http://127.0.0.1:12346");
        try {
            ClusterHealthProbe.bind(456, first); ClusterHealthProbe.bind(567, second);
            ClusterHealthProbe.response(HttpRequest.newBuilder(first.resolve("/healthz")).build(), 200, "ok", System.nanoTime());
            ClusterHealthProbe.response(HttpRequest.newBuilder(second.resolve("/healthz")).build(), 503, "broken", System.nanoTime());
            Thread foreign = new Thread(inner::close); foreign.start(); foreign.join();
            assertThat(ClusterHealthProbe.activeScope()).isSameAs(inner);
            outer.close();
        } finally { inner.close(); outer.close(); }
        assertThat(ClusterHealthProbe.activeScope()).isSameAs(before);
        Map<?, ?> capture = read(inner.write(scratch, false));
        List<?> samples = (List<?>) capture.get("samples");
        assertThat(samples.stream().map(row -> ((Map<?, ?>) row).get("pid")).toList()).isEqualTo(List.of(456L, 567L));
        assertThat(samples.stream().map(row -> ((Map<?, ?>) row).get("launch"))).doesNotHaveDuplicates();
        assertThat(capture.get("case_sha256")).isEqualTo(ClusterHealthProbe.hash("different invocation"));
        assertThat(read(outer.write(scratch, false)).get("samples")).isEqualTo(List.of());
    }

    @Test void disabledCollectionAndUnsafeOutputDoNotConsumeForeignInputs() throws Exception {
        assertThatCode(() -> ClusterHealthProbe.response(null, -1, null, 0)).doesNotThrowAnyException();
        var scope = ClusterHealthProbe.install(CASE, "local", "1"); scope.close();
        Path ordinary = scratch.resolve("ordinary"); Files.writeString(ordinary, "retained");
        assertThatThrownBy(() -> scope.write(ordinary, false)).isInstanceOf(IOException.class);
        Path link = scratch.resolve("link"); Files.createSymbolicLink(link, scratch);
        assertThatThrownBy(() -> scope.write(link, false)).isInstanceOf(IOException.class);
        assertThat(Files.readString(ordinary)).isEqualTo("retained");
    }

    private static HttpServer server(com.sun.net.httpserver.HttpHandler handler) throws IOException {
        HttpServer server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
        server.createContext("/", handler); server.start(); return server;
    }
    private static URI base(HttpServer server) { return URI.create("http://127.0.0.1:" + server.getAddress().getPort()); }
    private static Map<?, ?> read(Path file) throws IOException { return (Map<?, ?>) JsonReader.parse(Files.readString(file)); }
}
