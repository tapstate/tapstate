package io.tapstate.e2e;

import com.sun.net.httpserver.HttpServer;
import io.tapstate.control.core.MonitorError;
import io.tapstate.core.common.JsonWriter;
import org.junit.jupiter.api.Test;

import java.net.InetSocketAddress;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class ActuationHandoffSnapshotWaitTest {
    @Test
    void waitsThroughAbsentAndUnlandedSnapshotProgress() throws Exception {
        try (Fixture fixture = new Fixture(List.of(
                new Reply(404, Map.of("code", MonitorError.NO_OBSERVATION.code())),
                new Reply(200, Map.of("pipelineId", "handoff_pipe", "snapshot", Map.of())),
                new Reply(200, snapshot(1, false)),
                new Reply(200, snapshot(2, true))))) {
            AMemberLosingActuationLetsGoOfItsCaptureIT.awaitOriginalSnapshot(fixture.control);
            assertThat(fixture.reads.get()).isEqualTo(4);
        }
    }

    @Test
    void aServerFailureStillAbortsTheInitialWait() throws Exception {
        try (Fixture fixture = new Fixture(List.of(new Reply(500, Map.of("code", "controlled-refusal"))))) {
            assertThatThrownBy(() ->
                    AMemberLosingActuationLetsGoOfItsCaptureIT.awaitOriginalSnapshot(fixture.control))
                    .isInstanceOf(AssertionError.class).hasMessageContaining("got 500");
            assertThat(fixture.reads.get()).isEqualTo(1);
        }
    }

    private static Map<String, ?> snapshot(long rows, boolean landed) {
        return Map.of("pipelineId", "handoff_pipe", "snapshot", Map.of("orders", Map.of(
                "rowsDone", rows, "rowsTotal", 2L, "donePct", rows * 50, "landed", landed)));
    }

    private record Reply(int status, Map<String, ?> body) {}

    private static final class Fixture implements AutoCloseable {
        private final HttpServer server;
        private final AtomicInteger reads = new AtomicInteger();
        private final ControlPlane control;

        private Fixture(List<Reply> replies) throws Exception {
            server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
            server.createContext("/", exchange -> {
                Reply reply = exchange.getRequestURI().getPath().equals("/auth/login")
                        ? new Reply(200, Map.of("token", "controlled-token"))
                        : replies.get(Math.min(reads.getAndIncrement(), replies.size() - 1));
                byte[] bytes = JsonWriter.write(reply.body()).getBytes(StandardCharsets.UTF_8);
                exchange.sendResponseHeaders(reply.status(), bytes.length);
                try (var output = exchange.getResponseBody()) {
                    output.write(bytes);
                }
            });
            server.start();
            control = new ControlPlane(URI.create("http://127.0.0.1:" + server.getAddress().getPort()));
            control.login("test", "local-password");
        }

        @Override
        public void close() {
            server.stop(0);
        }
    }
}
