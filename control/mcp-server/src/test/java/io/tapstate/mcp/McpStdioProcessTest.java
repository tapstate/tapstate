package io.tapstate.mcp;

import io.tapstate.core.common.JsonReader;
import org.junit.jupiter.api.Test;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.io.Writer;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;

class McpStdioProcessTest {

    @Test
    void stdioNegotiatesListsReadToolsAndExitsAfterEofWithoutStdoutNoise() throws Exception {
        Path stderr = Files.createTempFile("tapstate-mcp-", ".stderr");
        Process process = start(stderr, unusedLoopbackPort());
        try (Writer input = new OutputStreamWriter(process.getOutputStream(), StandardCharsets.UTF_8);
                BufferedReader output = process.inputReader(StandardCharsets.UTF_8)) {
            send(input, """
                    {"jsonrpc":"2.0","id":1,"method":"initialize","params":{
                      "protocolVersion":"2025-06-18","capabilities":{},
                      "clientInfo":{"name":"tapstate-test","version":"1"}}}
                    """);
            Map<?, ?> initialize = readResponse(output, Duration.ofSeconds(10));
            assertThat(initialize.get("id")).isEqualTo(1L);
            assertThat(((Map<?, ?>) initialize.get("result")).get("protocolVersion"))
                    .isEqualTo("2025-06-18");
            // The version this sidecar announces to whatever drives it. It is configured, not compiled,
            // so nothing but this could notice it being left behind by a release -- and the number is
            // read by a model that will report it. Note whose version it is: the sidecar ships with the
            // CLI, so this is that half of the pair, never the server's.
            String projectVersion = System.getProperty("tapstate.project.version");
            assertThat(projectVersion)
                    .as("the build must pass -Dtapstate.project.version so this guard can run at all")
                    .isNotBlank();
            assertThat(((Map<?, ?>) ((Map<?, ?>) initialize.get("result")).get("serverInfo")).get("version"))
                    .isEqualTo(projectVersion);

            send(input, """
                    {"jsonrpc":"2.0","method":"notifications/initialized"}
                    """);
            send(input, """
                    {"jsonrpc":"2.0","id":2,"method":"tools/list","params":{}}
                    """);
            Map<?, ?> listed = readResponse(output, Duration.ofSeconds(10));
            assertThat(listed.get("id")).isEqualTo(2L);
            List<?> tools = (List<?>) ((Map<?, ?>) listed.get("result")).get("tools");
            assertThat(tools).hasSize(15);
            assertThat(tools.stream()
                    .map(tool -> String.valueOf(((Map<?, ?>) tool).get("name")))
                    .toList())
                    // artifact_get is asserted here, in a session negotiated by a real subprocess with no
                    // write access, because that is the session a caller is in when it decides whether to
                    // remove something: the read that supplies the precondition must not itself require
                    // the privilege the precondition guards.
                    .contains("connector_get", "source_draft", "artifact_validate", "artifact_get",
                            "pipeline_logs")
                    // The read face, over the real protocol: a client that spoke to this process would
                    // be offered these three by name, which is the only place that is true end to end.
                    .contains("data_browser_collections", "data_browser_find", "data_browser_stats")
                    .doesNotContain("source_create", "source_list", "source_get", "source_update", "source_delete",
                            "artifact_apply", "pipeline_start", "artifact_delete");

            send(input, """
                    {"jsonrpc":"2.0","id":3,"method":"tools/call","params":{
                      "name":"pipeline_status","arguments":{"id":"orders"}}}
                    """);
            Map<?, ?> called = readResponse(output, Duration.ofSeconds(10));
            Map<?, ?> callResult = (Map<?, ?>) called.get("result");
            assertThat(callResult.get("isError")).isEqualTo(true);
            assertThat(((Map<?, ?>) callResult.get("structuredContent")).get("code"))
                    .isEqualTo("control.unreachable");
        } finally {
            process.getOutputStream().close();
            if (!process.waitFor(5, TimeUnit.SECONDS)) {
                process.destroyForcibly();
                assertThat(process.waitFor(5, TimeUnit.SECONDS)).isTrue();
            }
        }
        assertThat(process.exitValue()).isZero();
        assertThat(Files.readString(stderr)).doesNotContain("process-test-token");
        Files.deleteIfExists(stderr);
    }

    private static Process start(Path stderr, int serverPort) throws IOException {
        String java = Path.of(System.getProperty("java.home"), "bin", "java").toString();
        String classpath = System.getProperty("surefire.test.class.path", System.getProperty("java.class.path"));
        ProcessBuilder builder = new ProcessBuilder(
                java, "-cp", classpath, TapstateMcpApplication.class.getName());
        builder.environment().put("TAPSTATE_TOKEN", "process-test-token");
        builder.environment().put("TAPSTATE_SERVER_URL", "http://127.0.0.1:" + serverPort);
        builder.redirectError(stderr.toFile());
        return builder.start();
    }

    private static int unusedLoopbackPort() throws IOException {
        try (ServerSocket socket = new ServerSocket(0, 1, InetAddress.getLoopbackAddress())) {
            return socket.getLocalPort();
        }
    }

    private static void send(Writer input, String message) throws IOException {
        input.write(message.strip().replace("\n", ""));
        input.write('\n');
        input.flush();
    }

    private static Map<?, ?> readResponse(BufferedReader output, Duration timeout) throws Exception {
        CompletableFuture<String> line = CompletableFuture.supplyAsync(() -> {
            try {
                return output.readLine();
            } catch (IOException error) {
                throw new IllegalStateException(error);
            }
        });
        String response = line.get(timeout.toMillis(), TimeUnit.MILLISECONDS);
        assertThat(response).isNotBlank();
        return (Map<?, ?>) JsonReader.parse(response);
    }
}
