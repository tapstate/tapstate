package io.tapstate.testsupport;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.io.UncheckedIOException;
import java.io.Writer;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

/**
 * A member of a kill test: a separate JVM on this test's own classpath, which the test can kill with
 * SIGKILL, freeze with SIGSTOP and thaw with SIGCONT.
 *
 * <p>A process, not a thread, because what a kill test asks is what the store does with a client that
 * vanishes or stops answering mid-transaction: its connections and server sessions stay as they were, and
 * nothing in the JVM gets to run a finally block or close a socket. Interrupting a thread cannot show that.
 *
 * <p>The member talks to the test by lines: it prints a marker line when it reaches a point the test waits
 * for ({@link #awaitLine}), and reads commands from its standard input ({@link #send}). Standard error is
 * kept so a failed member can be diagnosed from the test report.
 */
public final class MemberProcess implements AutoCloseable {

    private static final String END_OF_OUTPUT = "\u0000end";

    private final String name;
    private final Process process;
    private final BlockingQueue<String> lines = new LinkedBlockingQueue<>();
    private final List<String> seen = java.util.Collections.synchronizedList(new ArrayList<>());
    private final StringBuffer errors = new StringBuffer();
    private final Writer input;

    private MemberProcess(String name, Process process) {
        this.name = name;
        this.process = process;
        this.input = new OutputStreamWriter(process.getOutputStream(), StandardCharsets.UTF_8);
        pump(process.getInputStream(), line -> {
            seen.add(line);
            lines.add(line);
        }, () -> lines.add(END_OF_OUTPUT));
        pump(process.getErrorStream(), line -> errors.append(line).append('\n'), () -> { });
    }

    /**
     * Starts {@code main} in a new JVM on this JVM's classpath, with {@code properties} as system properties
     * and {@code arguments} as its arguments.
     */
    public static MemberProcess start(String name, Class<?> main, Map<String, String> properties, String... arguments) {
        Objects.requireNonNull(main, "main");
        List<String> command = new ArrayList<>();
        command.add(Path.of(System.getProperty("java.home"), "bin", "java").toString());
        command.add("-Xmx256m");
        command.add("-cp");
        command.add(System.getProperty("java.class.path"));
        properties.forEach((key, value) -> command.add("-D" + key + "=" + value));
        command.add(main.getName());
        command.addAll(List.of(arguments));
        try {
            return new MemberProcess(name, new ProcessBuilder(command).start());
        } catch (IOException failed) {
            throw new UncheckedIOException("could not start member " + name, failed);
        }
    }

    /** The operating system's id of the member. */
    public long pid() {
        return process.pid();
    }

    /**
     * Waits for the next line that starts with {@code prefix} and answers it; lines before it are skipped.
     * Fails, with what the member wrote to standard error, if the member exits or {@code timeout} passes.
     */
    public String awaitLine(String prefix, Duration timeout) {
        long deadline = System.nanoTime() + timeout.toNanos();
        try {
            while (true) {
                long left = deadline - System.nanoTime();
                String line = left <= 0 ? null : lines.poll(left, TimeUnit.NANOSECONDS);
                if (line == null) {
                    throw new AssertionError(name + " printed no line starting with '" + prefix + "' within "
                            + timeout + "; stderr:\n" + errors);
                }
                if (line.equals(END_OF_OUTPUT)) {
                    lines.add(END_OF_OUTPUT);
                    throw new AssertionError(name + " ended before printing '" + prefix + "'; stderr:\n" + errors);
                }
                if (line.startsWith(prefix)) {
                    return line;
                }
            }
        } catch (InterruptedException interrupted) {
            Thread.currentThread().interrupt();
            throw new AssertionError("interrupted waiting for " + name, interrupted);
        }
    }

    /** Every line the member has printed so far. */
    public List<String> output() {
        synchronized (seen) {
            return List.copyOf(seen);
        }
    }

    /** Writes one command line to the member's standard input. */
    public void send(String line) {
        try {
            input.write(line);
            input.write('\n');
            input.flush();
        } catch (IOException failed) {
            throw new UncheckedIOException("could not write to member " + name, failed);
        }
    }

    /** SIGKILL: the member stops at once, and nothing in it runs again. */
    public void kill() {
        process.destroyForcibly();
        awaitExit(Duration.ofSeconds(30));
    }

    /** SIGSTOP: the member stops running but keeps its sockets and server sessions, as a long pause would. */
    public void freeze() {
        signal("STOP");
    }

    /** SIGCONT: a frozen member carries on from where it stopped. */
    public void thaw() {
        signal("CONT");
    }

    /** Waits for the member to exit and answers its exit code. */
    public int awaitExit(Duration timeout) {
        try {
            if (!process.waitFor(timeout.toMillis(), TimeUnit.MILLISECONDS)) {
                throw new AssertionError(name + " did not exit within " + timeout);
            }
            return process.exitValue();
        } catch (InterruptedException interrupted) {
            Thread.currentThread().interrupt();
            throw new AssertionError("interrupted waiting for " + name, interrupted);
        }
    }

    /** What the member has written to standard error so far. */
    public String errors() {
        return errors.toString();
    }

    private void signal(String signal) {
        try {
            Process kill = new ProcessBuilder("kill", "-" + signal, Long.toString(process.pid())).inheritIO().start();
            if (!kill.waitFor(10, TimeUnit.SECONDS) || kill.exitValue() != 0) {
                throw new AssertionError("could not send SIG" + signal + " to " + name);
            }
        } catch (IOException failed) {
            throw new UncheckedIOException("could not signal member " + name, failed);
        } catch (InterruptedException interrupted) {
            Thread.currentThread().interrupt();
            throw new AssertionError("interrupted signalling " + name, interrupted);
        }
    }

    private void pump(java.io.InputStream stream, java.util.function.Consumer<String> sink, Runnable ended) {
        Thread thread = new Thread(() -> {
            try (BufferedReader reader = new BufferedReader(new InputStreamReader(stream, StandardCharsets.UTF_8))) {
                for (String line = reader.readLine(); line != null; line = reader.readLine()) {
                    sink.accept(line);
                }
            } catch (IOException closed) {
                // the member is gone; what it printed before that has been kept
            } finally {
                ended.run();
            }
        }, "member-" + name + "-output");
        thread.setDaemon(true);
        thread.start();
    }

    /** Kills the member if it is still running: a test never leaves one behind, frozen or not. */
    @Override
    public void close() {
        if (process.isAlive()) {
            try {
                thaw();
            } catch (AssertionError | UncheckedIOException alreadyGone) {
                // killed below either way
            }
            process.destroyForcibly();
        }
    }
}
