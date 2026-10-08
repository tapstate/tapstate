package io.tapstate.e2e;

import com.sun.net.httpserver.HttpServer;
import io.tapstate.core.common.JsonWriter;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

import java.net.InetSocketAddress;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** A job's sampled counters lag durable writes; neither zero forever nor a new snapshot is acceptable. */
class AtlasRuntimeMetricsTest {

    @Test
    void theRestartGuardWaitsForAnActualPositiveSampleInsteadOfReadingTheInitialZero() throws Exception {
        try (Fixture fixture = new Fixture(List.of(0L, 0L, 3L), 0)) {
            AtlasRuntime.assertResumedRun(fixture.control, "pipeline", "probe", 32, Duration.ofSeconds(2));
            assertThat(fixture.reads.get()).isGreaterThanOrEqualTo(3);
        }
    }

    @ParameterizedTest
    @CsvSource({"0,0,sampled record count", "32,0,resumed job record count", "3,1,resumed job snapshot reads"})
    void zeroForeverAnOldRunOrAnotherSnapshotCannotSatisfyTheRestartGuard(
            long count, long snapshot, String reason) throws Exception {
        try (Fixture fixture = new Fixture(List.of(count), snapshot)) {
            assertThatThrownBy(() -> AtlasRuntime.assertResumedRun(
                    fixture.control, "pipeline", "probe", 32, Duration.ofMillis(200)))
                    .isInstanceOf(AssertionError.class).hasMessageContaining(reason);
        }
    }

    private static final class Fixture implements AutoCloseable {
        final HttpServer server;
        final AtomicInteger reads = new AtomicInteger();
        final ControlPlane control;

        Fixture(List<Long> counts, long snapshot) throws Exception {
            server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
            server.createContext("/", exchange -> {
                String path = exchange.getRequestURI().getPath();
                Map<String, ?> body = path.equals("/auth/login") ? Map.of("token", "controlled-token")
                        : path.endsWith("/status") ? Map.of("state", "RUNNING")
                        : Map.of("metrics", Map.of("recordCount", counts.get(Math.min(reads.getAndIncrement(), counts.size() - 1)),
                                "snapshot.rows.read.probe", snapshot));
                byte[] bytes = JsonWriter.write(body).getBytes(StandardCharsets.UTF_8);
                exchange.sendResponseHeaders(200, bytes.length);
                try (var output = exchange.getResponseBody()) { output.write(bytes); }
            });
            server.start();
            control = new ControlPlane(URI.create("http://127.0.0.1:" + server.getAddress().getPort()));
            control.login("test", "local-password");
        }

        @Override public void close() { server.stop(0); }
    }
}
