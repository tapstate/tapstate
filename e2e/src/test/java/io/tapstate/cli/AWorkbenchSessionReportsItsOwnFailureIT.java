package io.tapstate.cli;

import io.tapstate.core.common.TapstateException;
import org.jline.terminal.impl.DumbTerminal;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.PrintWriter;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;

class AWorkbenchSessionReportsItsOwnFailureIT {
    @Test
    void theCliProcessReportsTheDiagnosticAfterRestoringItsTerminal(@TempDir Path directory) throws Exception {
        Path errorOutput = directory.resolve("stderr.txt");
        String classpath = System.getProperty("surefire.test.class.path", System.getProperty("java.class.path"));
        Process cli = new ProcessBuilder(Path.of(System.getProperty("java.home"), "bin", "java").toString(),
                "-cp", classpath, FailureProbe.class.getName())
                .redirectError(errorOutput.toFile())
                .redirectOutput(ProcessBuilder.Redirect.DISCARD)
                .start();
        try {
            assertThat(cli.waitFor(20, TimeUnit.SECONDS)).as("workbench process exits").isTrue();
            assertThat(cli.exitValue()).isEqualTo(Cli.EXIT_DIAGNOSTIC);
            assertThat(Files.readString(errorOutput))
                    .contains(CliError.CONTEXT_SOURCE_CONFLICT.code(), "terminal restored")
                    .doesNotContain(CliError.WORKBENCH_UNAVAILABLE.code(), "private diagnostic cause");
        } finally {
            if (cli.isAlive()) cli.destroyForcibly();
        }
    }

    /** Runs the real workbench runner while deterministically injecting the session boundary failure. */
    public static final class FailureProbe {
        public static void main(String[] args) {
            int result = Workbench.run(new PrintWriter(System.err, true), () -> new DumbTerminal(
                    "workbench-test", "xterm", new ByteArrayInputStream(new byte[0]), new ByteArrayOutputStream(),
                    StandardCharsets.UTF_8) {
                @Override
                protected void doClose() throws IOException {
                    super.doClose();
                    System.err.println("terminal restored");
                }
            }, runner -> {
                throw new TapstateException(CliError.CONTEXT_SOURCE_CONFLICT, Map.of(),
                        new IOException("private diagnostic cause"));
            });
            System.exit(result);
        }
    }
}
