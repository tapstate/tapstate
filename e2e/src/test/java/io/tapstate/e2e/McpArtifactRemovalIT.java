package io.tapstate.e2e;

import io.tapstate.core.common.JsonReader;
import io.tapstate.testsupport.DockerGate;

import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.io.UncheckedIOException;
import java.io.Writer;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * The witness that a model driving the tool surface can remove a resource it did not create - read its
 * version through the tools it actually has, then spend that version on the removal.
 *
 * <p>The removal verb's own description promises the caller supplies "the hash of the version just read".
 * Making that true on the tool surface took two changes that only work together: the read verb had to be
 * published there at all, and the read's answer had to start carrying the hash. Either one shipped alone
 * leaves the promise false, and neither is visible from the other's tests - so this case is written to fail
 * on each of them separately. Without the verb published, it is missing from the session's tool list;
 * without the hash in the answer, the read returns and there is nothing to spend.
 *
 * <p>The seeded resource is a <b>pipeline</b>, and that is the load-bearing choice. A source could already
 * be read with its hash through the Source-specific verb, so a case seeded with one would be satisfied by
 * either half-implementation - the general read verb could be absent entirely and the case would still pass
 * by going through the Source door. A pipeline has no such door. It is also applied over the HTTP face
 * before the session starts, so the model meets a resource it has no memory of, which is the situation the
 * whole read-then-remove loop exists for: everything applied before today.
 *
 * <p>The removal is confirmed over the HTTP face rather than by the tool's own answer. A tool that reports
 * success while removing nothing is exactly the sort of thing a case reading only its own return value
 * cannot see.
 *
 * <p>Driven against the embedded server. The fidelity axis the other cases sweep is about how the server
 * was launched, and what varies here is the sidecar - which is a separate process reached over the same
 * public wire either way.
 */
class McpArtifactRemovalIT {

    private static final String MCP_BOOT_JAR_PROPERTY = "tapstate.e2e.mcp-boot-jar";
    private static final String PIPELINE_ID = "mcp_removed_pipeline";
    private static final String SOURCE_ID = "mcp_src";
    private static final String TARGET_ID = "mcp_tgt";

    @TempDir
    private Path directory;

    @BeforeAll
    static void requireDocker() {
        DockerGate.require();
    }

    @Test
    void aModelReadsTheVersionOfAPipelineItNeverAppliedAndRemovesIt() throws Exception {
        try (var storage = PreparedNativeStore.create("e2e", "e2e-password"); ServerHandle server = Tiers.IN_PROCESS.launch(storage)) {
            ControlPlane control = new ControlPlane(server.baseUrl());
            storage.loginAndAwaitBusiness(control);
            control.registerConnector(
                    E2eConnectorJar.CONNECTOR_ID, Files.readAllBytes(E2eConnectorJar.buildInto(directory)));
            control.apply(workspace());
            assertThat(control.artifactIds())
                    .as("the pipeline exists before any session opens, so the model meets a resource it "
                            + "has no memory of applying")
                    .contains(PIPELINE_ID);

            Process process = startMcp(server, control.credential(), directory.resolve("mcp.stderr"));
            try (Writer input = new OutputStreamWriter(process.getOutputStream(), StandardCharsets.UTF_8);
                    BufferedReader output = process.inputReader(StandardCharsets.UTF_8)) {
                send(input, """
                        {"jsonrpc":"2.0","id":1,"method":"initialize","params":{
                          "protocolVersion":"2025-06-18","capabilities":{},
                          "clientInfo":{"name":"tapstate-e2e","version":"1"}}}
                        """);
                receive(output);
                send(input, """
                        {"jsonrpc":"2.0","method":"notifications/initialized"}
                        """);

                send(input, """
                        {"jsonrpc":"2.0","id":2,"method":"tools/list","params":{}}
                        """);
                List<?> tools = (List<?>) ((Map<?, ?>) receive(output).get("result")).get("tools");
                assertThat(tools.stream()
                        .map(tool -> String.valueOf(((Map<?, ?>) tool).get("name")))
                        .toList())
                        .as("the read verb has to be on the surface, or the removal verb's promise that "
                                + "the caller read the version first names a door that is not there")
                        .contains("artifact_get", "artifact_delete");

                send(input, """
                        {"jsonrpc":"2.0","id":3,"method":"tools/call","params":{
                          "name":"artifact_get","arguments":{"id":"%s"}}}
                        """.formatted(PIPELINE_ID));
                Map<?, ?> read = (Map<?, ?>) receive(output).get("result");
                assertThat(read.get("isError")).as("reading the pipeline through the tool surface").isEqualTo(false);
                Map<?, ?> artifact = (Map<?, ?>) read.get("structuredContent");
                assertThat(artifact.get("id")).isEqualTo(PIPELINE_ID);
                Object contentHash = artifact.get("contentHash");
                assertThat(contentHash)
                        .as("the version travels back with the read - a model cannot take a SHA-256 of the "
                                + "text it was just handed, so a read without this leaves it nothing to spend")
                        .isInstanceOf(String.class);

                send(input, """
                        {"jsonrpc":"2.0","id":4,"method":"tools/call","params":{
                          "name":"artifact_delete","arguments":{"id":"%s","expectedContentHash":"%s"}}}
                        """.formatted(PIPELINE_ID, contentHash));
                Map<?, ?> removed = (Map<?, ?>) receive(output).get("result");
                assertThat(removed.get("isError"))
                        .as("spending the version that was just read: %s", removed)
                        .isEqualTo(false);
            } finally {
                process.getOutputStream().close();
                if (!process.waitFor(5, TimeUnit.SECONDS)) {
                    process.destroyForcibly();
                    assertThat(process.waitFor(5, TimeUnit.SECONDS)).isTrue();
                }
            }

            assertThat(control.artifact(PIPELINE_ID))
                    .as("the server after the session - read through the HTTP face, because a tool that "
                            + "answers success while removing nothing cannot be caught by its own answer")
                    .isEmpty();
        }
    }

    private static Map<String, String> workspace() throws Exception {
        Path sourceDirectory = Files.createTempDirectory("mcp-src");
        Path targetDirectory = Files.createTempDirectory("mcp-tgt");
        Map<String, String> resources = new LinkedHashMap<>();
        resources.put(SOURCE_ID + ".tap.yml", Workspaces.cdcSourceYaml(SOURCE_ID, sourceDirectory));
        resources.put(TARGET_ID + ".tap.yml", Workspaces.targetYaml(TARGET_ID, targetDirectory));
        resources.put(PIPELINE_ID + ".tap.yml",
                Workspaces.pipelineYaml(PIPELINE_ID, SOURCE_ID, TARGET_ID, "orders"));
        return resources;
    }

    /** The shipped sidecar, pointed at the real server and given write access, as a session with one would be. */
    private static Process startMcp(ServerHandle server, String token, Path stderr) throws IOException {
        ProcessBuilder builder = new ProcessBuilder(
                Path.of(System.getProperty("java.home"), "bin", "java").toString(),
                "-jar", mcpBootJar().toString(),
                "--allow-write");
        builder.environment().put("TAPSTATE_SERVER_URL", server.baseUrl().toString());
        builder.environment().put("TAPSTATE_TOKEN", token);
        builder.redirectError(stderr.toFile());
        return builder.start();
    }

    private static Path mcpBootJar() {
        String file = System.getProperty(MCP_BOOT_JAR_PROPERTY);
        assertThat(file)
                .as("the build must set %s so the MCP process can be launched", MCP_BOOT_JAR_PROPERTY)
                .isNotBlank();
        Path jar = Path.of(file);
        assertThat(jar).isRegularFile();
        return jar;
    }

    private static void send(Writer input, String message) throws IOException {
        input.write(message.strip().replace("\n", ""));
        input.write('\n');
        input.flush();
    }

    private static Map<?, ?> receive(BufferedReader output) throws Exception {
        CompletableFuture<String> line = CompletableFuture.supplyAsync(() -> {
            try {
                return output.readLine();
            } catch (IOException error) {
                throw new UncheckedIOException(error);
            }
        });
        String response = line.get(Duration.ofSeconds(30).toMillis(), TimeUnit.MILLISECONDS);
        assertThat(response).isNotBlank();
        return (Map<?, ?>) JsonReader.parse(response);
    }
}
