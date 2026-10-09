package io.tapstate.e2e;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.sun.net.httpserver.HttpServer;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class RealProcessServerReadinessTest {

    @Test
    void aForeignHealthyEndpointCannotConfirmAnExitedChild(@TempDir Path scratch) throws IOException {
        AtomicInteger requests = new AtomicInteger();
        HttpServer endpoint = healthyEndpoint(requests);
        try (RealProcessServer child = child(scratch, endpoint, false, "owned child exited before HTTP binding")) {
            assertThatThrownBy(() -> child.awaitHealthy(Duration.ofSeconds(1), true))
                    .isInstanceOf(AssertionError.class).hasMessageContaining("exited with status 1");
            assertThat(requests.get()).as("the foreign endpoint never confirms this child").isZero();
        } finally {
            endpoint.stop(0);
        }
    }

    @Test
    void aLiveChildWithoutItsOwnBindingCannotUseAForeignHealthResponse(@TempDir Path scratch) throws IOException {
        AtomicInteger requests = new AtomicInteger();
        HttpServer endpoint = healthyEndpoint(requests);
        try (RealProcessServer child = child(scratch, endpoint, true, "Tomcat initialized before binding")) {
            assertThatThrownBy(() -> child.awaitHealthy(Duration.ofMillis(250), true))
                    .isInstanceOf(AssertionError.class).hasMessageContaining("did not answer");
            assertThat(requests.get()).isZero();
        } finally {
            endpoint.stop(0);
        }
    }

    @Test
    void aChildExitingDuringTheHealthRequestCannotBeConfirmed(@TempDir Path scratch) throws IOException {
        AtomicInteger requests = new AtomicInteger();
        ControlledProcess process = new ControlledProcess(true);
        HttpServer endpoint = healthyEndpoint(requests, process::destroy);
        int port = endpoint.getAddress().getPort();
        Path staging = Files.createDirectory(scratch.resolve("staging"));
        Path log = Files.writeString(scratch.resolve("server.out"), "Tomcat started on port " + port + " (http)");
        try (RealProcessServer child = new RealProcessServer(process,
                URI.create("http://127.0.0.1:" + port), log, staging)) {
            assertThatThrownBy(() -> child.awaitHealthy(Duration.ofSeconds(1), true))
                    .isInstanceOf(AssertionError.class).hasMessageContaining("exited with status 1");
            assertThat(requests.get()).isEqualTo(1);
        } finally {
            endpoint.stop(0);
        }
    }

    @Test
    void aLiveChildWithItsMatchingBindingStillRequiresHealth(@TempDir Path scratch) throws IOException {
        AtomicInteger requests = new AtomicInteger();
        HttpServer endpoint = healthyEndpoint(requests);
        int port = endpoint.getAddress().getPort();
        try (RealProcessServer child = child(scratch, endpoint, true, "Tomcat started on port " + port + " (http)")) {
            child.awaitHealthy(Duration.ofSeconds(1), true);
            assertThat(requests.get()).isEqualTo(1);
        } finally {
            endpoint.stop(0);
        }
    }

    static HttpServer healthyEndpoint(AtomicInteger requests) throws IOException {
        return healthyEndpoint(requests, () -> { });
    }

    private static HttpServer healthyEndpoint(AtomicInteger requests, Runnable beforeResponse) throws IOException {
        HttpServer server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
        server.createContext("/healthz", exchange -> {
            requests.incrementAndGet();
            beforeResponse.run();
            byte[] body = "ok".getBytes(StandardCharsets.UTF_8);
            exchange.sendResponseHeaders(200, body.length);
            exchange.getResponseBody().write(body);
            exchange.close();
        });
        server.start();
        return server;
    }

    private static RealProcessServer child(Path scratch, HttpServer endpoint, boolean alive, String output)
            throws IOException {
        Path staging = Files.createDirectory(scratch.resolve("staging"));
        Path log = Files.writeString(scratch.resolve("server.out"), output);
        return new RealProcessServer(new ControlledProcess(alive),
                URI.create("http://127.0.0.1:" + endpoint.getAddress().getPort()), log, staging);
    }

    static final class ControlledProcess extends Process {
        private volatile boolean alive;
        private final long pid;
        ControlledProcess(boolean alive) { this(alive, 1L); }
        ControlledProcess(boolean alive, long pid) { this.alive = alive; this.pid = pid; }
        @Override public long pid() { return pid; }
        @Override public java.io.OutputStream getOutputStream() { return java.io.OutputStream.nullOutputStream(); }
        @Override public java.io.InputStream getInputStream() { return java.io.InputStream.nullInputStream(); }
        @Override public java.io.InputStream getErrorStream() { return java.io.InputStream.nullInputStream(); }
        @Override public int waitFor() { alive = false; return 1; }
        @Override public boolean waitFor(long timeout, TimeUnit unit) { return !alive; }
        @Override public int exitValue() {
            if (alive) { throw new IllegalThreadStateException("the controlled child is alive"); }
            return 1;
        }
        @Override public boolean isAlive() { return alive; }
        @Override public void destroy() { alive = false; }
        @Override public Process destroyForcibly() { alive = false; return this; }
    }
}
