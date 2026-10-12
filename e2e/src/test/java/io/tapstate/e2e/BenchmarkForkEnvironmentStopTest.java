package io.tapstate.e2e;

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpServer;
import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * A fork stops its pipelines before the owned server shuts down. The product refuses a stop that does not say
 * what becomes of the pipeline's state, so the harness must send one that does, and must keep the state: the
 * stop exists only to close sink writers ahead of shutdown.
 */
class BenchmarkForkEnvironmentStopTest {

    @Test
    void everyPipelineIsStoppedKeepingItsStateAndAwaitedUntilStopped() throws Exception {
        var stops = new CopyOnWriteArrayList<String>();
        HttpServer server = HttpServer.create(new InetSocketAddress(InetAddress.getLoopbackAddress(), 0), 0);
        server.createContext("/auth/login", exchange -> respond(exchange, "{\"token\":\"token\"}"));
        server.createContext("/api/pipelines/", exchange -> {
            String path = exchange.getRequestURI().getPath().substring("/api/pipelines/".length());
            if (path.endsWith(":stop")) {
                String pipeline = path.substring(0, path.length() - ":stop".length());
                stops.add(pipeline + " " + new String(exchange.getRequestBody().readAllBytes(), StandardCharsets.UTF_8));
                respond(exchange, "{}");
            } else {
                String pipeline = path.substring(0, path.length() - "/status".length());
                String state = stops.stream().anyMatch(stop -> stop.startsWith(pipeline + " ")) ? "STOPPED" : "RUNNING";
                respond(exchange, "{\"pipelineId\":\"" + pipeline + "\",\"state\":\"" + state + "\"}");
            }
        });
        server.start();
        try {
            var control = new ControlPlane(URI.create("http://127.0.0.1:" + server.getAddress().getPort()), System::nanoTime);
            control.login("admin", "secret");

            BenchmarkForkEnvironment.stopAll(control, List.of("first", "second"), Duration.ofSeconds(10));

            assertThat(stops).containsExactly("first {\"purgeState\":false}", "second {\"purgeState\":false}");
            assertThat(control.state("first")).contains(io.tapstate.core.lifecycle.PipelineState.STOPPED);
        } finally {
            server.stop(0);
        }
    }

    private static void respond(HttpExchange exchange, String body) throws IOException {
        byte[] bytes = body.getBytes(StandardCharsets.UTF_8);
        exchange.getResponseHeaders().add("Content-Type", "application/json");
        exchange.sendResponseHeaders(200, bytes.length);
        try (var output = exchange.getResponseBody()) { output.write(bytes); }
    }
}
