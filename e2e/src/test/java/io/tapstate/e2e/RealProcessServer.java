package io.tapstate.e2e;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.URI;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.function.IntFunction;
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

    /**
     * Where a launch listens and where every case dials it.
     *
     * <p>A case whose subject is a member of a cluster widens the listen to every address with a later
     * argument of its own - such a member is refused unless it advertises somewhere another member can
     * reach it. It is still dialled here, and it has to be: the first admin is created over a channel
     * the server accepts only from this address. {@link #freePort()} reserves on this address too, so
     * widening the listen cannot hand a case somebody else's server on it.
     */
    private static final String LOOPBACK = "127.0.0.1";

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
        return start(storeUri, SharedMongo.OPERATOR_STATE_DATABASE);
    }

    /** Launches this build with additional command-line settings owned by one focused witness. */
    static RealProcessServer start(String storeUri, List<String> additionalArguments) {
        return start(storeUri, SharedMongo.OPERATOR_STATE_DATABASE, bootJar(), additionalArguments);
    }

    /** Launches the deliverable with an explicit operator-state database. */
    static RealProcessServer start(String storeUri, String operatorStateDatabase) {
        return start(storeUri, operatorStateDatabase, bootJar());
    }

    /**
     * The same, with further settings on the launch.
     *
     * <p>For a witness whose subject is something the deliverable only does when it is configured to -
     * joining another member of its own, say. They are the command line arguments an operator would
     * pass, appended after the ones every launch carries, so a case about a setting exercises the way
     * that setting really arrives rather than a fixture's idea of it.
     *
     * <p>Built from the port rather than handed over ready-made, because a member has to advertise the
     * address it is reachable at and that port is chosen here. A caller that had to know it in advance
     * would have to choose it too, and then this would be launching something at an address it does not
     * own.
     */
    static RealProcessServer start(String storeUri, IntFunction<List<String>> extraArguments) {
        return start(storeUri, LOOPBACK, extraArguments);
    }

    /**
     * The same, listening on {@code listenAddress} rather than only where it is dialled.
     *
     * <p>For a witness whose subject is a member of a cluster. Such a member is refused at startup
     * unless it advertises an address another member can reach, and advertising one while listening
     * only on the loopback would advertise an address nothing answers at. It is still dialled on the
     * loopback, and has to be: the first admin may only be created from there.
     *
     * <p>A parameter rather than something a caller appends, because a repeated command line option
     * does not replace the earlier one here - the values are joined with a comma and the product is
     * handed the pair, which fails to parse as one address.
     */
    static RealProcessServer start(String storeUri, String listenAddress,
            IntFunction<List<String>> extraArguments) {
        RealProcessServer server = launching(storeUri, bootJar(), listenAddress, extraArguments);
        try {
            awaitHealthy(server.process, server.baseUrl, server.output);
        } catch (RuntimeException | AssertionError e) {
            server.process.destroyForcibly();
            throw e;
        }
        return server;
    }

    /**
     * The same, for a build of the product that is not the one this reactor made.
     *
     * <p>Only one witness needs this, and it needs it structurally: the reactor builds this build and
     * nothing else, so a case whose subject is what an <em>older</em> binary does when handed a store
     * this build has migrated cannot get its subject from here. The jar is built beside the run and
     * named to it.
     */
    static RealProcessServer start(String storeUri, Path jar) {
        return start(storeUri, SharedMongo.OPERATOR_STATE_DATABASE, jar);
    }

    private static RealProcessServer start(String storeUri, String operatorStateDatabase, Path jar) {
        return start(storeUri, operatorStateDatabase, jar, List.of());
    }

    private static RealProcessServer start(String storeUri, String operatorStateDatabase, Path jar,
            List<String> additionalArguments) {
        RealProcessServer server = launching(storeUri, operatorStateDatabase, jar, additionalArguments);
        try {
            awaitHealthy(server.process, server.baseUrl, server.output);
        } catch (RuntimeException | AssertionError e) {
            server.process.destroyForcibly();
            throw e;
        }
        return server;
    }

    /**
     * Launches the deliverable and returns straight away, without waiting for it to serve.
     *
     * <p>For a witness whose subject is something the server does on the way up. Waiting for health
     * would mean waiting for the very thing such a witness means to interrupt, and the interruption
     * would then always land after the work rather than inside it.
     */
    static RealProcessServer launching(String storeUri) {
        return launching(storeUri, SharedMongo.OPERATOR_STATE_DATABASE, bootJar());
    }

    /** The same, launching the jar named rather than the one this reactor built. See {@link #start(String, Path)}. */
    static RealProcessServer launching(String storeUri, Path jar) {
        return launching(storeUri, SharedMongo.OPERATOR_STATE_DATABASE, jar);
    }

    private static RealProcessServer launching(String storeUri, String operatorStateDatabase, Path jar) {
        return launching(storeUri, operatorStateDatabase, jar, List.of());
    }

    private static RealProcessServer launching(String storeUri, String operatorStateDatabase, Path jar,
            List<String> additionalArguments) {
        return launching(storeUri, operatorStateDatabase, jar, LOOPBACK, port -> additionalArguments);
    }

    /** The same, with further settings on the launch. See {@link #start(String, IntFunction)}. */
    static RealProcessServer launching(String storeUri, Path jar, IntFunction<List<String>> extraArguments) {
        return launching(storeUri, jar, LOOPBACK, extraArguments);
    }

    /**
     * The same, listening on {@code listenAddress} rather than the loopback, with the jar this reactor
     * built -- what a member of a cluster needs, since one that binds the loopback is refused.
     */
    static RealProcessServer launching(String storeUri, String listenAddress,
            IntFunction<List<String>> extraArguments) {
        return launching(storeUri, bootJar(), listenAddress, extraArguments);
    }

    /** The same, listening on {@code listenAddress}. See {@link #start(String, String, IntFunction)}. */
    static RealProcessServer launching(String storeUri, Path jar, String listenAddress,
            IntFunction<List<String>> extraArguments) {
        return launching(storeUri, SharedMongo.OPERATOR_STATE_DATABASE, jar, listenAddress, extraArguments);
    }

    private static RealProcessServer launching(String storeUri, String operatorStateDatabase, Path jar,
            String listenAddress, IntFunction<List<String>> extraArguments) {
        int port = freePort();
        // The literal address, not the name: "localhost" resolves to both 127.0.0.1 and ::1, and the
        // launch below binds only the first.
        URI baseUrl = URI.create("http://" + LOOPBACK + ":" + port);
        Path workingDirectory = workingDirectory();
        Path output = workingDirectory.resolve("server.out");
        Process process = launch(jar, port, listenAddress, storeUri, operatorStateDatabase,
                workingDirectory, output, extraArguments.apply(port));
        return new RealProcessServer(process, baseUrl, output);
    }

    /**
     * Ends the process the way losing power ends it: no signal it can handle, nothing of its own run
     * on the way out.
     *
     * <p>{@link #close()} asks the process to stop and gives it time to, which is what a witness of
     * ordinary shutdown wants. A witness of a crash wants the opposite, and the difference is not
     * cosmetic: a process shut down politely gets to finish what it was doing, which is precisely the
     * state a crash witness needs it never to reach.
     */
    void kill() {
        process.destroyForcibly();
        try {
            process.waitFor(SHUTDOWN_BUDGET.toMillis(), TimeUnit.MILLISECONDS);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new AssertionError("interrupted while waiting for the killed server to go away", e);
        }
    }

    @Override
    public URI baseUrl() {
        return baseUrl;
    }

    /**
     * Where this launch wrote what it said. Some claims are only about a member's own start - which of
     * two members did a piece of one-time work, and how often - and the store records how far that work
     * got rather than who did it, so the account the server gave of itself is the only place to look.
     */
    Path output() {
        return output;
    }

    /** Whether it is still running, so a witness waiting on it can tell waiting from waiting forever. */
    boolean isAlive() {
        return process.isAlive();
    }

    /** The child JVM identity used by external benchmark resource sampling. */
    long pid() {
        return process.pid();
    }

    /**
     * What it exited with, for a witness whose subject is the server declining to start.
     *
     * <p>"Stopped" and "failed" are not the same outcome to anything supervising the process, so a
     * witness of a refusal has to be able to tell them apart.
     */
    int exitValue() {
        return process.exitValue();
    }

    /** The end of what it said, for a failure message that carries the server's own words. */
    String tail() {
        return tail(output);
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

    private static Process launch(Path jar, int port, String listenAddress, String storeUri,
            String operatorStateDatabase, Path workingDirectory, Path output, List<String> extraArguments) {
        List<String> command = new ArrayList<>(List.of(
                javaBinary(),
                "-jar",
                jar.toString(),
                // The role the deliverable is documented to take; parsed by the product before Spring starts.
                "--role=all",
                // Bound to the loopback, which is where freePort() reserved it and where the probe and
                // every case dial it. Left to itself the product binds the wildcard, and a wildcard bind
                // does not own 127.0.0.1:<port> -- a process already holding that port on the loopback
                // keeps receiving the requests, and this server answers none of them.
                "--server.address=" + listenAddress,
                "--server.port=" + port,
                "--tapstate.store.mongo.enabled=true",
                "--tapstate.store.mongo.uri=" + storeUri,
                "--" + ServerHandle.OPERATOR_STATE_DATABASE_SETTING + "=" + operatorStateDatabase,
                "--tapstate.store.mongo.server-selection-timeout=5s",
                // A staging directory of this launch's own, for the same reason the other tier gets one:
                // the cache is content-addressed and reused, so a shared one serves a stale connector.
                "--" + ServerHandle.PLUGINS_DIRECTORY_SETTING + "=" + ServerHandle.privateStagingDirectory(),
                "--" + ServerHandle.ALSO_ACCEPT_IDS_SETTING + "=" + E2eConnectorJar.CONNECTOR_ID));
        // After the standing ones, and additional to them rather than replacing any: a repeated option
        // is joined with the earlier one by comma rather than winning over it, so anything a case needs
        // to set differently is a parameter above instead of an argument here.
        command.addAll(extraArguments);
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

    /**
     * A port nothing is listening on, for a caller that has to know one before anything is launched -
     * two members pointed at each other cannot both be told the other's address afterwards.
     *
     * <p>Reserved by binding and releasing, so it is free at the moment it is answered and not promised
     * beyond that. Two calls never answer the same port while both are outstanding, which is what a
     * caller lining up a pair needs.
     */
    static int reservePort() {
        return freePort();
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
