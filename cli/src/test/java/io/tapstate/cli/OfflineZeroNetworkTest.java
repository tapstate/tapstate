package io.tapstate.cli;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import picocli.CommandLine;

import java.net.URI;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;

class OfflineZeroNetworkTest {

    @Test
    void offlineVerbsFinishBeforeContextResolutionOrTransport(@TempDir Path home) throws Exception {
        Path workspace = Files.createDirectory(home.resolve("orders"));
        ContextDefinition definition = new ContextDefinition(
                UUID.fromString("018f0d7a-7b2e-7e30-a8dd-6f78fc0d8ff2"),
                List.of(URI.create("http://127.0.0.1:1")),
                new ContextTls(true),
                UUID.fromString("5c199643-04da-4f72-9831-3a77e3590eed"));
        ContextConfig config = new ContextConfig(1, "dev", Map.of("dev", definition),
                Map.of(workspace.toRealPath().toString(), "dev"));
        AtomicInteger configReads = new AtomicInteger();
        ContextResolver resolver = new ContextResolver(() -> {
            configReads.incrementAndGet();
            return config;
        }, name -> "dev");
        CommandLine commandLine = Cli.newCommandLine();
        StringWriter sink = new StringWriter();
        commandLine.setOut(new PrintWriter(sink));
        commandLine.setErr(new PrintWriter(sink));
        try (HttpControlPlaneClient client = new HttpControlPlaneClient()) {
            Repl repl = new Repl(commandLine, workspace, client, new ScriptedPrompter(), name -> null,
                    resolver, null);

            repl.dispatch(List.of("validate"));
            repl.dispatch(List.of("new", "--kind", "source", "--id", "src", "--connector", "mysql",
                    "--dry-run"));
            repl.dispatch(List.of("explain", "source.id"));

            assertThat(configReads).hasValue(0);
            assertThat(repl.session().isConnected()).isFalse();
        }
    }
}
