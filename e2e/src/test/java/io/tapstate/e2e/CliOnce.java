package io.tapstate.e2e;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * The CLI run once as its own process, waited for, and read.
 *
 * <p>The one-shot half of what {@link CliProcess} does for a command that never exits on its own. The
 * two are separate because what they observe is different: a command that ends has an exit status and
 * complete output, and one that does not has neither, so a caller of the first would have to invent a
 * moment to stop looking.
 *
 * <p>Its own process rather than a call into the front end, everywhere it is used. The options a
 * launch line carries are parsed before the command table exists, and the status the process returns
 * is what a script reads; calling a method would exercise neither.
 */
final class CliOnce {

    /** Where the build wrote the classpath the CLI is launched from. */
    private static final String CLI_CLASSPATH_PROPERTY = "tapstate.e2e.cli-classpath";

    private CliOnce() {
    }

    /** What one CLI process produced. */
    record Run(int exitCode, String stdout, String stderr) {
    }

    /** Launches the CLI on the classpath the build recorded, and waits for it. */
    static Run run(String... args) {
        return runWithPassword(null, args);
    }

    /** Launches the CLI with its password supplied outside the command line. */
    static Run runWithPassword(String password, String... args) {
        List<String> command = new ArrayList<>(List.of(
                Path.of(System.getProperty("java.home"), "bin", "java").toString(),
                "-cp", classpath(),
                "io.tapstate.cli.Cli"));
        command.addAll(List.of(args));
        try {
            Path outFile = Files.createTempFile("cli-out", ".txt");
            Path errFile = Files.createTempFile("cli-err", ".txt");
            outFile.toFile().deleteOnExit();
            errFile.toFile().deleteOnExit();
            // Redirected to files rather than drained one after the other. Reading stdout to EOF
            // blocks until the process exits, so a CLI that fills the stderr pipe buffer deadlocks
            // before the timeout below is ever reached -- on exactly the noisy failures it is for.
            ProcessBuilder builder = new ProcessBuilder(command)
                    .redirectOutput(outFile.toFile())
                    .redirectError(errFile.toFile());
            if (password != null) {
                builder.environment().put("TAPSTATE_PASSWORD", password);
            }
            Process process = builder.start();
            if (!process.waitFor(2, TimeUnit.MINUTES)) {
                process.destroyForcibly();
                throw new AssertionError("the CLI did not exit; output so far:\n"
                        + Files.readString(outFile, StandardCharsets.UTF_8)
                        + Files.readString(errFile, StandardCharsets.UTF_8));
            }
            return new Run(process.exitValue(),
                    Files.readString(outFile, StandardCharsets.UTF_8),
                    Files.readString(errFile, StandardCharsets.UTF_8));
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new AssertionError("interrupted waiting for the CLI", e);
        }
    }

    private static String classpath() {
        String file = System.getProperty(CLI_CLASSPATH_PROPERTY);
        assertThat(file)
                .as("the build must set %s so the CLI can be launched", CLI_CLASSPATH_PROPERTY)
                .isNotBlank();
        try {
            return Files.readString(Path.of(file)).trim();
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }
}
