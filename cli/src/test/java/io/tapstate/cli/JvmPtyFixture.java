package io.tapstate.cli;

import org.junit.jupiter.api.Assumptions;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Runs a JVM child inside the host's real pseudo-terminal implementation.
 *
 * <p>The shell around the JVM records terminal attributes before and after the child. Tests retain
 * the PTY transcript so they can also verify cursor and alternate-screen control sequences.
 */
final class JvmPtyFixture implements AutoCloseable {

    private static final String READY_MARKER = "__TAPSTATE_PTY_READY__";
    private static final String PID_MARKER = "__TAPSTATE_PTY_JVM_PID__";
    private static final String EXIT_MARKER = "__TAPSTATE_PTY_EXIT__";
    private static final Duration START_TIMEOUT = Duration.ofSeconds(10);
    private static final Duration STOP_TIMEOUT = Duration.ofSeconds(2);
    private static final Pattern READY_PATTERN = Pattern.compile(
            Pattern.quote(READY_MARKER) + "([^|\\r\\n]+)\\|([^\\r\\n]+)");
    private static final Pattern PID_PATTERN = Pattern.compile(Pattern.quote(PID_MARKER) + "(\\d+)");
    private static final Pattern EXIT_PATTERN = Pattern.compile(
            Pattern.quote(EXIT_MARKER) + "(-?\\d+)\\|([^\\r\\n]+)");
    private static final String HARNESS = """
            set +e
            stty rows 53 cols 83
            before_state=$(stty -g)
            terminal_device=$(tty)
            printf '\n%s%%s|%%s\n' "$terminal_device" "$before_state"
            "$TAPSTATE_PTY_JAVA" -Duser.home="$TAPSTATE_PTY_HOME" -cp "$TAPSTATE_PTY_CLASSPATH" \
              io.tapstate.cli.WorkbenchPtyProbe "$TAPSTATE_PTY_MODE"
            child_status=$?
            after_state=$(stty -g)
            printf '\n%s%%s|%%s\n' "$child_status" "$after_state"
            exit 0
            """.formatted(READY_MARKER, EXIT_MARKER);

    enum Mode {
        BARE("bare"),
        BACKEND_SELECTION("backend-selection"),
        INJECTED_EXCEPTION("injected-exception"),
        EXCEPTIONAL_UNWIND("exceptional-unwind"),
        SETUP_FAILURE("setup-failure");

        private final String argument;

        Mode(String argument) {
            this.argument = argument;
        }
    }

    private final Process process;
    private final OutputStream input;
    private final InputStream output;
    private final ByteArrayOutputStream transcript = new ByteArrayOutputStream();
    private final Object transcriptChanged = new Object();
    private final Thread outputReader;
    private final boolean macOs;
    private String terminalDevice;
    private String initialTerminalState;
    private long jvmPid;

    private JvmPtyFixture(Process process, boolean macOs) {
        this.process = process;
        this.input = process.getOutputStream();
        this.output = process.getInputStream();
        this.macOs = macOs;
        this.outputReader = Thread.ofVirtual().name("tapstate-pty-output").start(this::readOutput);
    }

    static JvmPtyFixture start(Path home, Mode mode) throws Exception {
        String osName = System.getProperty("os.name", "").toLowerCase(Locale.ROOT);
        boolean macOs = osName.contains("mac");
        boolean linux = osName.contains("linux");
        Assumptions.assumeTrue(macOs || linux, "requires a POSIX pseudo-terminal host");

        Path script = Path.of("/usr/bin/script");
        Assumptions.assumeTrue(Files.isExecutable(script), "requires /usr/bin/script");
        Files.createDirectories(home);

        List<String> command = macOs
                ? List.of(script.toString(), "-q", "/dev/null", "/bin/sh", "-c", HARNESS)
                : List.of(script.toString(), "-qfec", HARNESS, "/dev/null");
        ProcessBuilder builder = new ProcessBuilder(command).redirectErrorStream(true);
        builder.directory(home.toFile());
        Map<String, String> environment = builder.environment();
        environment.remove("TAPSTATE_CONTEXT");
        environment.remove("TAPSTATE_PASSWORD");
        environment.remove("TAPSTATE_TOKEN");
        environment.put("TERM", "xterm-256color");
        environment.put("TAPSTATE_WORKDIR", home.toString());
        environment.put("TAPSTATE_PTY_JAVA", Path.of(System.getProperty("java.home"), "bin", "java").toString());
        environment.put("TAPSTATE_PTY_HOME", home.toString());
        environment.put("TAPSTATE_PTY_CLASSPATH", testClasspath());
        environment.put("TAPSTATE_PTY_MODE", mode.argument);

        JvmPtyFixture fixture = new JvmPtyFixture(builder.start(), macOs);
        try {
            fixture.awaitText(READY_MARKER, START_TIMEOUT);
            fixture.captureStartupMarkers();
            fixture.awaitText(PID_MARKER, START_TIMEOUT);
            fixture.captureJvmPid();
            return fixture;
        } catch (Throwable failure) {
            fixture.close();
            throw failure;
        }
    }

    void awaitText(String expected, Duration timeout) throws InterruptedException {
        long deadline = System.nanoTime() + timeout.toNanos();
        synchronized (transcriptChanged) {
            while (!transcript().contains(expected) && System.nanoTime() < deadline && process.isAlive()) {
                long remainingMillis = Math.max(1,
                        TimeUnit.NANOSECONDS.toMillis(deadline - System.nanoTime()));
                transcriptChanged.wait(Math.min(remainingMillis, 50));
            }
        }
        if (!transcript().contains(expected)) {
            throw new AssertionError("PTY transcript did not contain '" + expected + "': " + visibleTranscript());
        }
    }

    int transcriptLength() {
        return transcript().length();
    }

    void awaitTextAfter(String expected, int offset, Duration timeout) throws InterruptedException {
        long deadline = System.nanoTime() + timeout.toNanos();
        synchronized (transcriptChanged) {
            while (!transcriptAfter(offset).contains(expected)
                    && System.nanoTime() < deadline
                    && process.isAlive()) {
                long remainingMillis = Math.max(1,
                        TimeUnit.NANOSECONDS.toMillis(deadline - System.nanoTime()));
                transcriptChanged.wait(Math.min(remainingMillis, 50));
            }
        }
        if (!transcriptAfter(offset).contains(expected)) {
            throw new AssertionError(
                    "PTY transcript after offset " + offset + " did not contain '" + expected + "': "
                            + visibleTranscript());
        }
    }

    void send(String value) throws IOException {
        input.write(value.getBytes(StandardCharsets.UTF_8));
        input.flush();
    }

    void closeInput() throws IOException {
        input.write(4);
        input.flush();
        input.close();
    }

    void resize(int columns, int rows) throws Exception {
        List<String> command = new ArrayList<>();
        command.add("/bin/stty");
        command.add(macOs ? "-f" : "-F");
        command.add(terminalDevice);
        command.add("rows");
        command.add(Integer.toString(rows));
        command.add("cols");
        command.add(Integer.toString(columns));
        Process resize = new ProcessBuilder(command).redirectErrorStream(true).start();
        String failure = new String(resize.getInputStream().readAllBytes(), StandardCharsets.UTF_8);
        if (!resize.waitFor(STOP_TIMEOUT.toMillis(), TimeUnit.MILLISECONDS) || resize.exitValue() != 0) {
            resize.destroyForcibly();
            throw new AssertionError("stty could not resize " + terminalDevice + ": " + failure);
        }
    }

    void signal(String signal) throws Exception {
        Process sender = new ProcessBuilder("/bin/kill", "-" + signal, Long.toString(jvmPid))
                .redirectErrorStream(true)
                .start();
        String failure = new String(sender.getInputStream().readAllBytes(), StandardCharsets.UTF_8);
        if (!sender.waitFor(STOP_TIMEOUT.toMillis(), TimeUnit.MILLISECONDS) || sender.exitValue() != 0) {
            sender.destroyForcibly();
            throw new AssertionError("could not send SIG" + signal + " to JVM " + jvmPid + ": " + failure);
        }
    }

    boolean awaitExit(Duration timeout) throws InterruptedException {
        if (!process.waitFor(timeout.toMillis(), TimeUnit.MILLISECONDS)) {
            return false;
        }
        outputReader.join(STOP_TIMEOUT.toMillis());
        return transcript().contains(EXIT_MARKER);
    }

    String transcript() {
        synchronized (transcript) {
            return transcript.toString(StandardCharsets.UTF_8);
        }
    }

    private String transcriptAfter(int offset) {
        String current = transcript();
        return current.substring(Math.min(offset, current.length()));
    }

    String visibleTranscript() {
        return transcript().replace("\u001b", "<ESC>");
    }

    String initialTerminalState() {
        return normalizeTerminalState(initialTerminalState);
    }

    String finalTerminalState() {
        Matcher matcher = EXIT_PATTERN.matcher(transcript());
        if (!matcher.find()) {
            throw new AssertionError("PTY exit marker is absent: " + visibleTranscript());
        }
        return normalizeTerminalState(matcher.group(2));
    }

    int childExitStatus() {
        Matcher matcher = EXIT_PATTERN.matcher(transcript());
        if (!matcher.find()) {
            throw new AssertionError("PTY exit marker is absent: " + visibleTranscript());
        }
        return Integer.parseInt(matcher.group(1));
    }

    long jvmPid() {
        return jvmPid;
    }

    @Override
    public void close() {
        if (process.isAlive()) {
            try {
                List<ProcessHandle> tree = new ArrayList<>(process.descendants().toList());
                tree.add(process.toHandle());
                tree.reversed().forEach(ProcessHandle::destroy);
                awaitTermination(tree);
                tree.reversed().stream().filter(ProcessHandle::isAlive).forEach(ProcessHandle::destroyForcibly);
                awaitTermination(tree);
            } catch (RuntimeException unavailableProcessInventory) {
                process.destroy();
                awaitTermination(List.of(process.toHandle()));
                if (process.isAlive()) {
                    process.destroyForcibly();
                }
            }
        }
        try {
            input.close();
        } catch (IOException ignored) {
            // The process may already have closed its PTY input.
        }
        try {
            output.close();
        } catch (IOException ignored) {
            // The process may already have closed its PTY output.
        }
    }

    private static String testClasspath() {
        String classpath = System.getProperty("surefire.test.class.path", System.getProperty("java.class.path"));
        return Pattern.compile(Pattern.quote(System.getProperty("path.separator")))
                .splitAsStream(classpath)
                .map(entry -> Path.of(entry).toAbsolutePath().normalize().toString())
                .collect(java.util.stream.Collectors.joining(System.getProperty("path.separator")));
    }

    private void captureStartupMarkers() {
        Matcher matcher = READY_PATTERN.matcher(transcript());
        if (!matcher.find()) {
            throw new AssertionError("PTY ready marker is malformed: " + visibleTranscript());
        }
        terminalDevice = matcher.group(1);
        initialTerminalState = matcher.group(2);
    }

    private void captureJvmPid() {
        Matcher matcher = PID_PATTERN.matcher(transcript());
        if (!matcher.find()) {
            throw new AssertionError("PTY JVM PID marker is malformed: " + visibleTranscript());
        }
        jvmPid = Long.parseLong(matcher.group(1));
    }

    private void readOutput() {
        byte[] buffer = new byte[4096];
        try {
            int count;
            while ((count = output.read(buffer)) >= 0) {
                synchronized (transcript) {
                    transcript.write(buffer, 0, count);
                }
                synchronized (transcriptChanged) {
                    transcriptChanged.notifyAll();
                }
            }
        } catch (IOException ignored) {
            // Closing a timed-out fixture interrupts the transcript reader.
        } finally {
            synchronized (transcriptChanged) {
                transcriptChanged.notifyAll();
            }
        }
    }

    private static String normalizeTerminalState(String terminalState) {
        String normalized = terminalState.replaceAll(":ispeed=[^:]+", "").replaceAll(":ospeed=[^:]+", "");
        Matcher localFlags = Pattern.compile(":lflag=([0-9a-fA-F]+):").matcher(normalized);
        if (!localFlags.find()) {
            return normalized;
        }
        long configuredFlags = Long.parseUnsignedLong(localFlags.group(1), 16) & ~0x20000000L;
        return localFlags.replaceFirst(":lflag=" + Long.toHexString(configuredFlags) + ":");
    }

    private static void awaitTermination(List<ProcessHandle> processes) {
        long deadline = System.nanoTime() + STOP_TIMEOUT.toNanos();
        while (processes.stream().anyMatch(ProcessHandle::isAlive) && System.nanoTime() < deadline) {
            try {
                Thread.sleep(20);
            } catch (InterruptedException interrupted) {
                Thread.currentThread().interrupt();
                return;
            }
        }
    }
}
