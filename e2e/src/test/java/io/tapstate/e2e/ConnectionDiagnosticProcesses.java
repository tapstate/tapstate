package io.tapstate.e2e;

import io.tapstate.core.common.JsonReader;
import io.tapstate.core.common.JsonWriter;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.Writer;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;

/** Optional local process acceptance for the pinned native CLI and packaged MCP sidecar. */
final class ConnectionDiagnosticProcesses {
    private ConnectionDiagnosticProcesses() {
    }

    static void verify(Path home, PreparedNativeStore storage, URI server, ControlPlane control,
            String id, Map<?, ?> expected) throws Exception {
        String nativePath = System.getProperty("tapstate.e2e.native-cli");
        String mcpJar = System.getProperty("tapstate.e2e.mcp-jar");
        if (nativePath == null && mcpJar == null) return;
        assertThat(nativePath).isNotBlank();
        assertThat(mcpJar).isNotBlank();

        var cli = CliOnce.runNative(Path.of(nativePath), home, storage.password(),
                "-c", server.toString(), "-u", storage.username(), "test-result", id, "-o", "json");
        assertThat(cli.exitCode()).isEqualTo(1);
        var report = (Map<?, ?>) JsonReader.parse(cli.stdout());
        assertSameDiagnostics(expected, report);
        var text = CliOnce.runNative(Path.of(nativePath), home, storage.password(),
                "-c", server.toString(), "-u", storage.username(), "test-result", id);
        assertThat(text.exitCode()).isEqualTo(1);
        assertThat(text.stdout()).contains("FAILED", "28P01", "password authentication failed");

        String java = Path.of(System.getProperty("java.home"), "bin", "java").toString();
        var builder = new ProcessBuilder(java, "-jar", mcpJar, "--server", server.toString());
        builder.environment().put("TAPSTATE_TOKEN", control.mintToken("read"));
        builder.redirectError(ProcessBuilder.Redirect.DISCARD);
        Process mcp = builder.start();
        try (var in = mcp.outputWriter(StandardCharsets.UTF_8); var out = mcp.inputReader(StandardCharsets.UTF_8)) {
            send(in, Map.of("jsonrpc", "2.0", "id", 1, "method", "initialize", "params",
                    Map.of("protocolVersion", "2025-06-18", "capabilities", Map.of(),
                            "clientInfo", Map.of("name", "connection-witness", "version", "1"))));
            assertThat(receive(out).get("id")).isEqualTo(1L);
            send(in, Map.of("jsonrpc", "2.0", "method", "notifications/initialized"));
            send(in, Map.of("jsonrpc", "2.0", "id", 2, "method", "tools/call", "params",
                    Map.of("name", "connection_test_result", "arguments", Map.of("id", id))));
            var reply = (Map<?, ?>) receive(out).get("result");
            assertThat(reply.get("isError")).isEqualTo(false);
            assertSameDiagnostics(expected, (Map<?, ?>) reply.get("structuredContent"));
        } finally {
            if (!mcp.waitFor(5, TimeUnit.SECONDS)) mcp.destroyForcibly();
        }
        assertThat(mcp.exitValue()).isZero();
    }

    private static void assertSameDiagnostics(Map<?, ?> expected, Map<?, ?> report) {
        assertThat(report.get("outcome")).isEqualTo(expected.get("outcome"));
        assertThat(report.get("testedAt")).isEqualTo(expected.get("testedAt"));
        assertThat(checkDiagnostics(report)).isEqualTo(checkDiagnostics(expected));
    }

    private static List<?> checkDiagnostics(Map<?, ?> report) {
        return ((List<?>) report.get("checks")).stream().map(item -> ((Map<?, ?>) item).get("diagnostic")).toList();
    }

    private static void send(Writer input, Map<String, Object> message) throws IOException {
        input.write(JsonWriter.write(message));
        input.write('\n');
        input.flush();
    }

    private static Map<?, ?> receive(BufferedReader output) throws Exception {
        return CompletableFuture.supplyAsync(() -> {
            try {
                return (Map<?, ?>) JsonReader.parse(output.readLine());
            } catch (IOException failure) {
                throw new java.io.UncheckedIOException(failure);
            }
        }).get(20, TimeUnit.SECONDS);
    }
}
