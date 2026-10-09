package io.tapstate.e2e;

import com.sun.net.httpserver.HttpServer;
import io.tapstate.core.common.JsonReader;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.net.InetSocketAddress;
import java.net.URI;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Controlled children exercise the existing native readiness, cleanup and fresh routing boundaries. */
class FreshNativeServerRoutingTest {
    @Test
    void theExplicitFreshTierRetainsAnEndedBindFailureThenReturnsItsSecondOwnedHealthyChild(@TempDir Path scratch)
            throws IOException {
        try (Fixture fixture = new Fixture(scratch)) {
            ServerHandle result = Tiers.REAL_PROCESS.launchFresh("mongodb://unused/fresh", "owned_operator", fixture.setup());
            assertThat(result).isSameAs(fixture.second);
            assertThat(result.baseUrl()).isEqualTo(fixture.second.baseUrl());
            assertThat(fixture.first.terminated()).isTrue();
            assertThat(fixture.first.stagingDirectory()).doesNotExist();
            assertThat(fixture.events).containsExactly("guard", "launch-1", "guard", "launch-2");
            assertThat(fixture.requests).hasSize(2);
            assertThat(fixture.requests.getLast()).isSameAs(fixture.requests.getFirst());
            assertThat(fixture.requests.getFirst().storeUri()).isEqualTo("mongodb://unused/fresh");
            assertThat(fixture.requests.getFirst().operatorStateDatabase()).isEqualTo("owned_operator");
            List<Path> retained;
            try (var files = Files.list(fixture.receipts)) { retained = files.sorted().toList(); }
            assertThat(retained).hasSize(2);
            Path first = retained.stream().filter(path -> path.getFileName().toString().startsWith("attempt-1-")).findFirst().orElseThrow();
            Path second = retained.stream().filter(path -> path.getFileName().toString().startsWith("attempt-2-")).findFirst().orElseThrow();
            assertThat(Files.readString(first.resolve("server.raw.log"))).isEqualTo(Files.readString(fixture.first.output()));
            Object parsed = JsonReader.parse(Files.readString(first.resolve("receipt.json")));
            assertThat(parsed).isInstanceOf(Map.class);
            Map<?, ?> receipt = (Map<?, ?>) parsed;
            assertThat(receipt.get("pid")).isEqualTo(101L);
            assertThat(receipt.get("baseUri")).isEqualTo(fixture.first.baseUrl().toString());
            assertThat(receipt.get("argv")).isEqualTo(fixture.first.launchCommand());
            assertThat(receipt.get("ended")).isEqualTo(true); assertThat(receipt.get("exit")).isEqualTo(1L);
            assertThat(receipt.get("rawLogState")).isEqualTo("ENDED_CHILD_COMPLETE");
            Map<?, ?> success = (Map<?, ?>) JsonReader.parse(Files.readString(second.resolve("receipt.json")));
            assertThat(success.get("rawLogState")).isEqualTo("LIVE_STARTUP_PREFIX");
        }
    }

    @Test
    void theExistingOrdinaryHealthyPathStillStopsAtItsFirstFailedChild(@TempDir Path scratch) throws IOException {
        try (Fixture fixture = new Fixture(scratch)) {
            var setup = fixture.setup();
            RealProcessServer child = setup.launch().apply(new FreshNativeServer.Request("mongodb://unused/fresh",
                    "owned_operator", setup.jar(), setup.applicationArguments()));
            assertThatThrownBy(() -> RealProcessServer.healthy(child)).isInstanceOf(AssertionError.class)
                    .hasMessageContaining("exited with status 1");
            assertThat(fixture.requests).hasSize(1); assertThat(child.terminated()).isTrue();
        }
    }

    @Test
    void theFreshTierDoesNotLaunchAnAlreadyConfiguredWorkload(@TempDir Path scratch) throws IOException {
        try (Fixture fixture = new Fixture(scratch)) {
            var ordinary = fixture.setup();
            var nonfresh = new FreshNativeServer.Setup(ordinary.jar(), ordinary.applicationArguments(),
                    () -> new FreshMemberStartup.Freshness(false, true, true, true), ordinary.launch(),
                    ordinary.receipts(), ordinary.clock());
            assertThatThrownBy(() -> Tiers.REAL_PROCESS.launchFresh("mongodb://unused/fresh", "owned_operator", nonfresh))
                    .isInstanceOf(AssertionError.class).hasMessageContaining("empty pipeline workload");
            assertThat(fixture.requests).isEmpty();
        }
    }

    private static final class Fixture implements AutoCloseable {
        private final Path scratch, receipts;
        private final HttpServer refused, healthy;
        private final AtomicInteger launches = new AtomicInteger();
        private final List<String> events = new ArrayList<>();
        private final List<FreshNativeServer.Request> requests = new ArrayList<>();
        private RealProcessServer first, second;
        Fixture(Path scratch) throws IOException {
            this.scratch = scratch; receipts = scratch.resolve("receipts");
            refused = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
            refused.createContext("/healthz", exchange -> { exchange.sendResponseHeaders(503, -1); exchange.close(); });
            refused.start();
            healthy = RealProcessServerReadinessTest.healthyEndpoint(new AtomicInteger());
        }
        FreshNativeServer.Setup setup() throws IOException {
            Path jar = Files.writeString(scratch.resolve("frozen.jar"), "controlled launch boundary");
            return new FreshNativeServer.Setup(jar, List.of(), () -> {
                events.add("guard"); return new FreshMemberStartup.Freshness(true, true, true, true);
            }, request -> {
                int ordinal = launches.incrementAndGet(); events.add("launch-" + ordinal); requests.add(request);
                try {
                    if (ordinal == 1) { first = child(request, ordinal, refused, false); return first; }
                    assertThat(first.terminated()).isTrue();
                    assertThat(first.stagingDirectory()).doesNotExist();
                    try (var retained = Files.list(receipts)) {
                        assertThat(retained.anyMatch(path -> path.getFileName().toString().startsWith("attempt-1-"))).isTrue();
                    }
                    second = child(request, ordinal, healthy, true); return second;
                } catch (IOException failure) { throw new UncheckedIOException(failure); }
            }, receipts, System::nanoTime);
        }
        private RealProcessServer child(FreshNativeServer.Request request, int ordinal, HttpServer endpoint, boolean alive)
                throws IOException {
            int port = endpoint.getAddress().getPort();
            Path directory = Files.createDirectory(scratch.resolve("child-" + ordinal));
            Path staging = Files.createDirectory(directory.resolve("staging"));
            String log = alive ? "Tomcat started on port " + port + " (http)\n" :
                    "APPLICATION FAILED TO START\nWeb server failed to start. Port " + port + " was already in use.\n";
            Path output = Files.writeString(directory.resolve("server.out"), log);
            List<String> argv = List.of("controlled-java", "-jar", request.jar().toString(), "--server.port=" + port,
                    "--tapstate.store.mongo.uri=" + request.storeUri(),
                    "--tapstate.store.mongo.operator-state-database=" + request.operatorStateDatabase());
            return new RealProcessServer(new RealProcessServerReadinessTest.ControlledProcess(alive, 100L + ordinal),
                    URI.create("http://127.0.0.1:" + port), output, staging, argv);
        }
        @Override public void close() {
            if (first != null) { first.close(); } if (second != null) { second.close(); }
            refused.stop(0); healthy.stop(0);
        }
    }
}
