package io.tapstate.cli;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import picocli.CommandLine;

import java.lang.reflect.Proxy;
import java.net.URI;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Clock;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;

class OfflineZeroNetworkTest {

    @Test
    void offlineVerbsFinishBeforeContextResolutionOrTransport(@TempDir Path home) throws Exception {
        Path workspace = Files.createDirectory(home.resolve("orders"));
        Path sourceDir = Files.createDirectory(workspace.resolve("source"));
        Files.writeString(sourceDir.resolve("existing.tap.yml"), """
                version: tapstate/v1
                kind: source
                id: existing
                connector: mongodb
                config: { uri: "${MONGO_URI}" }
                """);
        URI seed = URI.create("http://127.0.0.1:17900");
        UUID authRef = UUID.fromString("5c199643-04da-4f72-9831-3a77e3590eed");
        UUID contextId = UUID.fromString("018f0d7a-7b2e-7e30-a8dd-6f78fc0d8ff2");
        ContextDefinition definition = new ContextDefinition(
                contextId,
                List.of(seed),
                new ContextTls(true),
                authRef);
        ContextConfig config = new ContextConfig(1, "dev", Map.of("dev", definition),
                Map.of(workspace.toRealPath().toString(), "dev"));
        ContextConfigStore configStore = ContextConfigStore.underHome(home);
        configStore.save(config);
        Instant createdAt = Instant.parse("2026-08-17T10:00:00Z");
        AuthSessionRecord cached = new AuthSessionRecord(
                1, authRef, contextId, "urn:tapstate:cluster:01J5FIXTURE", "admin", List.of("read"),
                "tss_s01.session-secret", createdAt, createdAt.plusSeconds(30L * 24 * 60 * 60),
                createdAt.plusSeconds(90L * 24 * 60 * 60));
        AuthFileStore authStore = AuthFileStore.underHome(home);
        assertThat(authStore.save(cached, false)).isEqualTo(AuthFileStore.SaveResult.PERSISTED);
        assertThat(home.resolve(".tapstate/config.yaml")).isRegularFile();
        assertThat(home.resolve(".tapstate/auth/" + authRef + ".json")).isRegularFile();

        AtomicInteger networkCalls = new AtomicInteger();
        ControlPlaneClient client = (ControlPlaneClient) Proxy.newProxyInstance(
                ControlPlaneClient.class.getClassLoader(),
                new Class<?>[]{ControlPlaneClient.class},
                (proxy, method, arguments) -> {
                    networkCalls.incrementAndGet();
                    return method.getReturnType() == boolean.class ? false : null;
                });
        CountingPrompter prompter = new CountingPrompter();
        ContextResolver resolver = new ContextResolver(configStore, name -> "dev");
        CommandLine commandLine = Cli.newCommandLine();
        StringWriter stdout = new StringWriter();
        StringWriter stderr = new StringWriter();
        commandLine.setOut(new PrintWriter(stdout));
        commandLine.setErr(new PrintWriter(stderr));
        AuthService authService = new AuthService(client, authStore,
                Clock.fixed(createdAt.plusSeconds(1), ZoneOffset.UTC));
        Repl repl = new Repl(commandLine, workspace, client, prompter, name -> null,
                resolver, null, authService);
        repl.terminalCheck(() -> false);

        assertThat(repl.dispatch(List.of("validate"), true)).isTrue();
        assertThat(repl.lastExitCode()).isZero();
        assertThat(repl.dispatch(List.of("new", "--kind", "source", "--id", "src", "--connector", "mysql",
                "--dry-run"), true)).isTrue();
        assertThat(repl.lastExitCode()).isZero();
        assertThat(repl.dispatch(List.of("explain", "source.id"), true)).isTrue();
        assertThat(repl.lastExitCode()).isZero();

        assertThat(networkCalls).hasValue(0);
        assertThat(prompter.calls).hasValue(0);
        assertThat(repl.session().isConnected()).isFalse();
        assertThat(stderr.toString()).isEmpty();
        assertThat(stdout.toString())
                .contains("valid:", "version: tapstate/v1", "source.id")
                .doesNotContain("Password", "Sign in", "tapstate(", cached.sessionToken());
    }

    private static final class CountingPrompter implements Prompter {

        private final AtomicInteger calls = new AtomicInteger();

        @Override
        public String ask(String question, String defaultValue) {
            calls.incrementAndGet();
            return "";
        }

        @Override
        public String secret(String question) {
            calls.incrementAndGet();
            return "";
        }

        @Override
        public String choose(String question, List<String> options) {
            calls.incrementAndGet();
            return options.get(options.size() - 1);
        }

        @Override
        public String lines(String question) {
            calls.incrementAndGet();
            return "";
        }
    }
}
