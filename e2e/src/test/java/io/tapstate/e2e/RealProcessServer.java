package io.tapstate.e2e;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.URI;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.List;
import java.util.concurrent.TimeUnit;

/**
 * The shipped jar, launched as a user launches it.
 *
 * <p>This tier exists to catch what the in-process tier structurally cannot: the boot jar's nested
 * layout, a resource that only resolves from an exploded classpath, a config the launcher supplies.
 * The product runs in its own JVM, on loopback - bootstrap is refused off loopback, so a container
 * would be a worse test, not a better one.
 */
final class RealProcessServer implements ServerHandle {

    /** Set by the failsafe binding, which knows where the reactor put the deliverable. */
    private static final String BOOT_JAR_PROPERTY = "tapstate.e2e.boot-jar";

    private static final Duration STARTUP_BUDGET = Duration.ofSeconds(120);
    private static final Duration POLL_INTERVAL = Duration.ofMillis(250);
    private static final Duration SHUTDOWN_BUDGET = Duration.ofSeconds(20);

    private final Process process;
    private final URI baseUrl;
    private final Path output;

    private RealProcessServer(Process process, URI baseUrl, Path output) {
        this.process = process;
        this.baseUrl = baseUrl;
        this.output = output;
    }

    /** Launches the deliverable and returns once its health probe answers. */
    static RealProcessServer start(String storeUri) {
        Path jar = bootJar();
        int port = freePort();
        // The literal address, not the name: "localhost" resolves to both 127.0.0.1 and ::1, and the
        // launch below binds only the first.
        URI baseUrl = URI.create("http://127.0.0.1:" + port);
        Path workingDirectory = workingDirectory();
        Path output = workingDirectory.resolve("server.out");
        Process process = launch(jar, port, storeUri, workingDirectory, output);
        try {
            awaitHealthy(process, baseUrl, output);
        } catch (RuntimeException | AssertionError e) {
            process.destroyForcibly();
            throw e;
        }
        return new RealProcessServer(process, baseUrl, output);
    }

    @Override
    public URI baseUrl() {
        return baseUrl;
    }

    @Override
    public void close() {
        process.destroy();
        try {
            if (!process.waitFor(SHUTDOWN_BUDGET.toMillis(), TimeUnit.MILLISECONDS)) {
                process.destroyForcibly();
                process.waitFor(SHUTDOWN_BUDGET.toMillis(), TimeUnit.MILLISECONDS);
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            process.destroyForcibly();
        }
    }

    private static Process launch(Path jar, int port, String storeUri, Path workingDirectory, Path output) {
        List<String> command = List.of(
                javaBinary(),
                "-jar",
                jar.toString(),
                // The role the deliverable is documented to take; parsed by the product before Spring starts.
                "--role=all",
                // Bound to the loopback, which is where freePort() reserved it and where the probe and
                // every case dial it. Left to itself the product binds the wildcard, and a wildcard bind
                // does not own 127.0.0.1:<port> -- a process already holding that port on the loopback
                // keeps receiving the requests, and this server answers none of them.
                "--server.address=127.0.0.1",
                "--server.port=" + port,
                "--tapstate.store.mongo.enabled=true",
                "--tapstate.store.mongo.uri=" + storeUri,
                "--tapstate.store.mongo.server-selection-timeout=5s",
                // A staging directory of this launch's own, for the same reason the other tier gets one:
                // the cache is content-addressed and reused, so a shared one serves a stale connector.
                "--" + ServerHandle.PLUGINS_DIRECTORY_SETTING + "=" + ServerHandle.privateStagingDirectory(),
                "--" + ServerHandle.ALSO_ACCEPT_IDS_SETTING + "=" + E2eConnectorJar.CONNECTOR_ID);
        try {
            return new ProcessBuilder(command)
                    .directory(workingDirectory.toFile())
                    .redirectErrorStream(true)
                    .redirectOutput(output.toFile())
                    .start();
        } catch (IOException e) {
            throw new UncheckedIOException("could not launch " + jar, e);
        }
    }

    /**
     * Polls the product's own readiness signal until it answers. A fixed sleep here would either be
     * too short on a loaded machine or waste its whole length on every green run.
     */
    private static void awaitHealthy(Process process, URI baseUrl, Path output) {
        ControlPlane probe = new ControlPlane(baseUrl);
        long start = System.nanoTime();
        long deadline = start + STARTUP_BUDGET.toNanos();
        while (true) {
            if (probe.healthy()) {
                return;
            }
            if (!process.isAlive()) {
                throw new AssertionError(
                        "the server exited with status " + process.exitValue() + " before answering "
                                + baseUrl + "/healthz; its output was:\n" + tail(output));
            }
            if (System.nanoTime() - deadline >= 0) {
                process.destroyForcibly();
                throw new AssertionError(
                        "the server did not answer " + baseUrl + "/healthz after "
                                + Duration.ofNanos(System.nanoTime() - start) + " (budget " + STARTUP_BUDGET
                                + "); its output was:\n" + tail(output));
            }
            sleep(POLL_INTERVAL);
        }
    }

    private static Path bootJar() {
        String configured = System.getProperty(BOOT_JAR_PROPERTY);
        if (configured == null || configured.isBlank()) {
            throw new AssertionError(
                    "no " + BOOT_JAR_PROPERTY + " system property: this tier drives the deliverable, so "
                            + "the build must say where it is");
        }
        Path jar = Path.of(configured);
        if (!Files.isRegularFile(jar)) {
            throw new AssertionError(
                    "no deliverable at " + jar + ": package the app module before running this tier");
        }
        return jar;
    }

    private static String javaBinary() {
        return Path.of(System.getProperty("java.home"), "bin", "java").toString();
    }

    private static int freePort() {
        // The product cannot be asked for the port it chose from outside its JVM, so the port is chosen
        // here and handed to it. The socket is closed before the launch, which leaves a small window -
        // the alternative, a fixed port, turns any busy machine into a permanent failure instead.
        //
        // Reserved on the loopback specifically, because that is the address the server is launched on
        // and the address every case dials. A wildcard reservation proves only that the port is free on
        // some address: the allocator hands one out even when another local process already holds it on
        // 127.0.0.1, and that holder is the one a loopback connection reaches. Asking on the same address
        // is also what narrows the window above to a racer binding this exact address, rather than any
        // holder that was already there before the reservation was made.
        try (ServerSocket socket = new ServerSocket(0, 0, InetAddress.getLoopbackAddress())) {
            return socket.getLocalPort();
        } catch (IOException e) {
            throw new UncheckedIOException("could not reserve a port for the server", e);
        }
    }

    private static Path workingDirectory() {
        try {
            // Its own directory, so the log the server writes under its working directory is this run's.
            return Files.createTempDirectory("tapstate-e2e-server");
        } catch (IOException e) {
            throw new UncheckedIOException("could not create a working directory for the server", e);
        }
    }

    /** The server's own words, so a failure here says what the product said rather than only that it failed. */
    private static String tail(Path output) {
        try {
            List<String> lines = Files.readAllLines(output);
            return String.join("\n", lines.subList(Math.max(0, lines.size() - 40), lines.size()));
        } catch (IOException e) {
            return "(its output at " + output + " could not be read: " + e.getMessage() + ")";
        }
    }

    private static void sleep(Duration interval) {
        try {
            Thread.sleep(interval.toMillis());
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new AssertionError("interrupted while waiting for the server to start", e);
        }
    }
}
