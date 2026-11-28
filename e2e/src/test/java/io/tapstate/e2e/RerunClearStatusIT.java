package io.tapstate.e2e;

import com.sun.net.httpserver.HttpServer;
import io.tapstate.core.common.IoErrorDefinitions;
import io.tapstate.core.common.JsonWriter;
import io.tapstate.core.common.LifecycleErrorDefinitions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CopyOnWriteArrayList;

import static org.assertj.core.api.Assertions.assertThat;

/** The declarative vocabulary does not assert the CLI output after a refused Start. */
class RerunClearStatusIT {

    private static final String RETRY = "is still clearing. After clearing completes, run: tapstate start pl1";
    private static final String START_REFUSAL = LifecycleErrorDefinitions.PIPELINE_NOT_RUNNABLE.code();

    @Test
    void aPreviousRunFailureDoesNotHideThePendingClear(@TempDir Path home) throws Exception {
        String previousFailure = IoErrorDefinitions.DOCUMENT_UNREADABLE.code();
        var run = rerunWithStatus(home, "FAILED", Map.of("code", previousFailure,
                "message", "The previous run could not read its revision.",
                "params", Map.of("id", "pl1", "field", "revision")));

        assertThat(run.stderr()).contains(RETRY).doesNotContain(previousFailure);
    }

    @Test
    void anUnprovenClearReportsItsDiagnostic(@TempDir Path home) throws Exception {
        String clearFailure = IoErrorDefinitions.PIPELINE_CLEAR_UNPROVEN.code();
        var run = rerunWithStatus(home, null, Map.of("code", clearFailure,
                "message", "The clearing Stop for pipeline 'pl1' could not be confirmed.",
                "params", Map.of("pipeline", "pl1")));

        assertThat(run.stderr()).contains(clearFailure, "could not be confirmed.").doesNotContain(RETRY);
    }

    private static CliOnce.Run rerunWithStatus(Path home, String observedState, Map<String, Object> failure)
            throws Exception {
        var status = new java.util.LinkedHashMap<String, Object>();
        status.put("pipelineId", "pl1");
        status.put("state", "FAILED");
        status.put("observedState", observedState);
        status.put("desiredState", "STOPPED");
        status.put("stopState", "CLEARING");
        status.put("failure", failure);
        var calls = new CopyOnWriteArrayList<String>();
        HttpServer server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
        server.createContext("/", exchange -> {
            try (exchange) {
                String request = exchange.getRequestMethod() + " " + exchange.getRequestURI();
                int code = 200;
                String body;
                switch (request) {
                    case "GET /healthz" -> body = "ok";
                    case "GET /version" -> body = "{\"version\":\"0.8.0\"}";
                    case "GET /.well-known/tapstate" -> body = """
                            {"issuer":"urn:tapstate:cluster:fixture","clusterId":"fixture",
                             "apiVersion":"tapstate/v1","authModes":["password","machine_token"]}
                            """;
                    case "GET /api/pipelines/pl1/start-checks?intent=rerun" -> {
                        calls.add(request);
                        body = """
                                {"pipelineId":"pl1","intent":"RERUN","contentHash":"abc",
                                 "outcome":"READY","findings":[]}
                                """;
                    }
                    case "POST /api/pipelines/pl1:stop" -> {
                        calls.add(request + " " + new String(exchange.getRequestBody().readAllBytes(),
                                StandardCharsets.UTF_8));
                        body = """
                                {"pipelineId":"pl1","targetState":"STOPPED","revision":"abc"}
                                """;
                    }
                    case "POST /api/pipelines/pl1:start" -> {
                        calls.add(request);
                        code = 409;
                        body = JsonWriter.write(Map.of("code", START_REFUSAL, "message", "pl1 is not runnable.",
                                "params", Map.of("pipeline", "pl1")));
                    }
                    case "GET /api/pipelines/pl1/status" -> {
                        calls.add(request);
                        body = JsonWriter.write(status);
                    }
                    default -> {
                        code = 404;
                        body = "unexpected request: " + request;
                    }
                }
                byte[] bytes = body.getBytes(StandardCharsets.UTF_8);
                exchange.getResponseHeaders().set("Content-Type", "application/json");
                exchange.sendResponseHeaders(code, bytes.length);
                exchange.getResponseBody().write(bytes);
            }
        });
        server.start();
        try {
            var run = CliOnce.runWithPassword(null, List.of("-Duser.home=" + home),
                    "-c", "http://127.0.0.1:" + server.getAddress().getPort(), "--token", "fixture-token",
                    "restart", "pl1", "--rerun", "-y");
            assertThat(run.exitCode()).as("stdout:%n%s%nstderr:%n%s", run.stdout(), run.stderr()).isEqualTo(1);
            assertThat(calls).containsExactly("GET /api/pipelines/pl1/start-checks?intent=rerun",
                    "POST /api/pipelines/pl1:stop {\"purgeState\":true}", "POST /api/pipelines/pl1:start",
                    "GET /api/pipelines/pl1/status");
            assertThat(run.stderr()).contains(START_REFUSAL);
            return run;
        } finally {
            server.stop(0);
        }
    }
}
