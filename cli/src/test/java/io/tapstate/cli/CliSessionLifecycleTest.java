package io.tapstate.cli;

import org.junit.jupiter.api.Test;

import java.lang.reflect.Proxy;
import java.net.URI;
import java.util.ArrayList;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

class CliSessionLifecycleTest {

    @Test
    void passwordPromptClosesBeforeTheWorkbenchTakesTerminalOwnership() {
        List<String> lifecycle = new ArrayList<>();
        RecordingPrompter prompt = new RecordingPrompter(lifecycle);
        List<String> logins = new ArrayList<>();
        ControlPlaneClient client = connectedClient(lifecycle, logins);
        LaunchOptions launch = LaunchOptions.parse("--connect", "localhost:7900", "--user", "alice")
                .withEnv(name -> null);

        int status = Cli.runSession(launch, client, () -> prompt, repl -> {
            lifecycle.add("workbench-open");
            assertThat(prompt.closed).as("the password terminal owner must be closed before workbench")
                    .isTrue();
            return Cli.EXIT_OK;
        });

        assertThat(status).isZero();
        assertThat(logins).containsExactly("alice:secret@http://localhost:7900");
        assertThat(lifecycle).containsExactly(
                "password-read", "prompt-close", "workbench-open", "control-close");
    }

    private static ControlPlaneClient connectedClient(List<String> lifecycle, List<String> logins) {
        return (ControlPlaneClient) Proxy.newProxyInstance(
                ControlPlaneClient.class.getClassLoader(),
                new Class<?>[] {ControlPlaneClient.class},
                (proxy, method, arguments) -> switch (method.getName()) {
                    case "isHealthy" -> true;
                    case "serverVersion" -> Cli.VERSION_NUMBER;
                    case "discover" -> new DiscoveryOutcome.Discovered(
                            "urn:tapstate:cluster:test-cluster", "test-cluster", "tapstate/v1",
                            List.of("password", "machine_token"));
                    case "login" -> {
                        logins.add(arguments[1] + ":" + arguments[2] + "@" + arguments[0]);
                        yield new LoginOutcome.Success("test-token");
                    }
                    case "close" -> {
                        lifecycle.add("control-close");
                        yield null;
                    }
                    case "toString" -> "connected-test-control-plane";
                    case "hashCode" -> System.identityHashCode(proxy);
                    case "equals" -> proxy == arguments[0];
                    default -> throw new AssertionError("unexpected control-plane call: " + method);
                });
    }

    private static final class RecordingPrompter implements Prompter, AutoCloseable {
        private final List<String> lifecycle;
        private boolean closed;

        private RecordingPrompter(List<String> lifecycle) {
            this.lifecycle = lifecycle;
        }

        @Override
        public String ask(String question, String defaultValue) {
            throw new AssertionError("unexpected free-text prompt");
        }

        @Override
        public String secret(String question) {
            lifecycle.add("password-read");
            return "secret";
        }

        @Override
        public String choose(String question, List<String> options) {
            throw new AssertionError("unexpected choice prompt");
        }

        @Override
        public String lines(String question) {
            throw new AssertionError("unexpected block prompt");
        }

        @Override
        public void close() {
            if (!closed) {
                closed = true;
                lifecycle.add("prompt-close");
            }
        }
    }
}
