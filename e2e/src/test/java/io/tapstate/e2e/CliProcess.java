package io.tapstate.e2e;

import org.junit.jupiter.api.Assumptions;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.UncheckedIOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.function.Predicate;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * A CLI command running for as long as the caller watches it, with its output readable as it arrives.
 *
 * <p>The one-shot runner beside this waits for the process to exit and reads what it produced. That
 * cannot drive the two commands that never exit on their own: what they are for is showing changes as
 * they happen, so what has to be observed is the output growing while the process is still running,
 * and the process has to be stopped by the observer.
 *
 * <p>Output is drained on its own thread from the moment the process starts. Reading it only when the
 * caller asks would leave it in a pipe buffer, and a command that fills that buffer blocks writing
 * into it - so a specification that waited for the next line could deadlock against a product that
 * was working perfectly.
 */
final class CliProcess implements AutoCloseable {

    /** Where the build wrote the classpath the CLI is launched from. */
    private static final String CLI_CLASSPATH_PROPERTY = "tapstate.e2e.cli-classpath";

    private static final Duration POLL = Duration.ofMillis(100);

    private final Process process;
    private final StringBuilder output = new StringBuilder();
    private final Thread drain;

    private CliProcess(Process process) {
        this.process = process;
        this.drain = new Thread(this::drainOutput, "cli-output");
        this.drain.setDaemon(true);
        this.drain.start();
    }

    /** Starts the CLI with its output on a pipe - what a command in a script or a redirect gets. */
    static CliProcess onAPipe(String... args) {
        return onAPipe(Map.of(), args);
    }

    /** Starts the CLI with its output on a pipe and the supplied process environment. */
    static CliProcess onAPipe(Map<String, String> environment, String... args) {
        return new CliProcess(start(command(args), environment));
    }

    /**
     * Starts the CLI with its output on a pseudo-terminal - what a person at a shell gets.
     *
     * <p>Through {@code script(1)}, which allocates one and runs the command against it. That is the
     * whole apparatus: no library, no dependency added to this module. Its two implementations take
     * their arguments differently and neither accepts the other's spelling, so both are spelled out
     * here - util-linux wants the command as one string after {@code -c}, BSD wants it as arguments
     * after the file. Where the tool is missing entirely the specification aborts rather than falling
     * back to a pipe: a pipe is the condition these commands refuse, so falling back would turn a
     * witness into a witness of something else.
     */
    static CliProcess onATerminal(String... args) {
        return onATerminal(Map.of(), args);
    }

    /** Starts the CLI with a pseudo-terminal and the supplied process environment. */
    static CliProcess onATerminal(Map<String, String> environment, String... args) {
        requireScript();
        List<String> command = command(args);
        boolean mac = System.getProperty("os.name", "").toLowerCase(Locale.ROOT).contains("mac");
        List<String> wrapped = new ArrayList<>();
        wrapped.add("script");
        if (mac) {
            wrapped.add("-q");
            wrapped.add("/dev/null");
            wrapped.addAll(command);
        } else {
            wrapped.add("-qec");
            wrapped.add(shellWord(command));
            wrapped.add("/dev/null");
        }
        return new CliProcess(start(wrapped, environment));
    }

    /** Everything the command has written so far. */
    String output() {
        synchronized (output) {
            return output.toString();
        }
    }

    /**
     * Waits until the output satisfies the predicate, and answers with it.
     *
     * <p>Fails with everything seen so far, because for these commands "it has not happened" and "it
     * happened and was not shown" are the same thing from out here until the output is read out loud.
     */
    String awaitOutput(Predicate<String> wanted, Duration within, String what) {
        long deadline = System.nanoTime() + within.toNanos();
        while (System.nanoTime() - deadline < 0) {
            String seen = output();
            if (wanted.test(seen)) {
                return seen;
            }
            if (!process.isAlive() && !wanted.test(output())) {
                throw new AssertionError("the CLI exited before " + what + "; it had written:\n" + output());
            }
            sleep();
        }
        throw new AssertionError("waited " + within + " for " + what + "; the CLI had written:\n" + output());
    }

    /** Whether the command is still running - what "it did not refuse and did not finish" looks like. */
    boolean alive() {
        return process.isAlive();
    }

    @Override
    public void close() {
        process.destroy();
        try {
            if (!process.waitFor(10, TimeUnit.SECONDS)) {
                process.destroyForcibly();
                process.waitFor(10, TimeUnit.SECONDS);
            }
            drain.join(TimeUnit.SECONDS.toMillis(5));
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    private void drainOutput() {
        try (BufferedReader reader = new BufferedReader(
                new InputStreamReader(process.getInputStream(), StandardCharsets.UTF_8))) {
            int c;
            while ((c = reader.read()) != -1) {
                synchronized (output) {
                    output.append((char) c);
                }
            }
        } catch (IOException closed) {
            // The process went away mid-read; whatever arrived before that is what there is.
        }
    }

    /**
     * Aborts unless a terminal can actually be allocated, and says which half is missing.
     *
     * <p>Aborting rather than failing, on the same reasoning the Docker gate uses: a developer machine
     * without the tool is an ordinary condition, and a specification that cannot be set up has not
     * found anything. What it must never do is quietly run on a pipe instead - these commands behave
     * differently there by design, so that would be a green run of a different specification.
     */
    private static void requireScript() {
        List<String> probe = System.getProperty("os.name", "").toLowerCase(Locale.ROOT).contains("mac")
                ? List.of("script", "-q", "/dev/null", "true")
                : List.of("script", "-qec", "true", "/dev/null");
        try {
            Process check = new ProcessBuilder(probe).redirectErrorStream(true).start();
            check.getInputStream().readAllBytes();
            Assumptions.assumeTrue(check.waitFor(30, TimeUnit.SECONDS) && check.exitValue() == 0,
                    "script(1) cannot allocate a terminal here: skipping a witness that needs one");
        } catch (IOException absent) {
            Assumptions.abort("no script(1): skipping a witness that needs a terminal");
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new AssertionError("interrupted probing for script(1)", e);
        }
    }

    private static List<String> command(String... args) {
        List<String> command = new ArrayList<>(List.of(
                Path.of(System.getProperty("java.home"), "bin", "java").toString(),
                "-cp", cliClasspath(),
                "io.tapstate.cli.Cli"));
        command.addAll(List.of(args));
        return command;
    }

    private static Process start(List<String> command, Map<String, String> environment) {
        try {
            ProcessBuilder builder = new ProcessBuilder(command).redirectErrorStream(true);
            builder.environment().putAll(environment);
            return builder.start();
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    /** One shell word per argument, so a path holding a space survives the round trip through -c. */
    private static String shellWord(List<String> command) {
        StringBuilder line = new StringBuilder();
        for (String argument : command) {
            if (!line.isEmpty()) {
                line.append(' ');
            }
            line.append('\'').append(argument.replace("'", "'\\''")).append('\'');
        }
        return line.toString();
    }

    private static String cliClasspath() {
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

    private static void sleep() {
        try {
            Thread.sleep(POLL.toMillis());
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new AssertionError("interrupted while watching the CLI", e);
        }
    }
}
