package io.tapstate.e2e;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.net.URI;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
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
     * the server accepts only from this address. {@link #freePort()} reserves across local addresses,
     * so widening the listen cannot hand a case somebody else's server on it.
     */
    private static final String LOOPBACK = "127.0.0.1";

    private static final Duration STARTUP_BUDGET = Duration.ofSeconds(120);
    private static final Duration POLL_INTERVAL = Duration.ofMillis(250);
    private static final Duration SHUTDOWN_BUDGET = Duration.ofSeconds(20);

    // Keep closed reservations too: a delayed launch or cleanup must never refer to a port handed
    // to a later launch in this JVM. Open reservations protect members not yet launched from reuse.
    private static final Map<Integer, Socket> PORT_RESERVATIONS = new LinkedHashMap<>();

    private final Process process;
    private final URI baseUrl;
    private final Path output;
    private final Path stagingDirectory;
    // Whether the launch made the directory itself, and so deletes it as it ends; one a witness named is the
    // witness's to keep or clear.
    private final boolean ownsStagingDirectory;
    private boolean closeRequested;
    private long closeRequestedAt;
    private ClusterLifecycleProbe.Facts lifecycle;

    private RealProcessServer(Process process, URI baseUrl, Path output, Path stagingDirectory,
            boolean ownsStagingDirectory) {
        this.process = process;
        this.baseUrl = baseUrl;
        this.output = output;
        this.stagingDirectory = stagingDirectory;
        this.ownsStagingDirectory = ownsStagingDirectory;
    }

    /** Launches the deliverable and returns once its health probe answers. */
    static RealProcessServer start(String storeUri) {
        return start(storeUri, SharedMongo.OPERATOR_STATE_DATABASE);
    }

    /** Launches this build with additional command-line settings owned by one focused witness. */
    static RealProcessServer start(String storeUri, List<String> additionalArguments) {
        return start(storeUri, SharedMongo.OPERATOR_STATE_DATABASE, bootJar(), additionalArguments);
    }

    /** Keeps a focused witness's durable operator state in its own deployment database. */
    static RealProcessServer start(String storeUri, String operatorStateDatabase, List<String> additionalArguments) {
        return start(storeUri, operatorStateDatabase, bootJar(), additionalArguments);
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
        return healthy(launching(storeUri, bootJar(), listenAddress, extraArguments));
    }

    /**
     * The same, staging the connectors it resolves into {@code stagingDirectory} rather than a directory of this
     * launch's own, which it then leaves where it is when it ends; null for a directory of the launch's own.
     *
     * <p>For a witness whose subject is a member that cannot stage what it is asked to load. A parameter rather than
     * something a caller appends, for the reason the listen address is one: the standing setting would be joined
     * with the caller's by a comma rather than replaced by it.
     */
    static RealProcessServer start(String storeUri, String listenAddress, Path stagingDirectory,
            IntFunction<List<String>> extraArguments) {
        return healthy(launching(storeUri, listenAddress, stagingDirectory, extraArguments));
    }

    /**
     * The same, for a build of the product that is not the one this reactor made.
     *
     * <p>Only one witness needs this, and it needs it structurally: the reactor builds this build and
     * nothing else, so a case whose subject is what an <em>older</em> binary does when handed a store
     * this build has migrated cannot get its subject from here. The lane supplies the published jar
     * and names it to this run.
     */
    static RealProcessServer start(String storeUri, Path jar) {
        return start(storeUri, SharedMongo.OPERATOR_STATE_DATABASE, jar);
    }

    private static RealProcessServer start(String storeUri, String operatorStateDatabase, Path jar) {
        return start(storeUri, operatorStateDatabase, jar, List.of());
    }

    private static RealProcessServer start(String storeUri, String operatorStateDatabase, Path jar,
            List<String> additionalArguments) {
        return healthy(launching(storeUri, operatorStateDatabase, jar, additionalArguments));
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

    /** The same, staging into the directory a witness names, which the launch leaves where it is as it ends. */
    static RealProcessServer launching(String storeUri, Path jar, Path stagingDirectory) {
        return launching(storeUri, SharedMongo.OPERATOR_STATE_DATABASE, jar, LOOPBACK, List.of(), stagingDirectory,
                port -> List.of());
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

    /** The same, staging into the directory a witness names. See {@link #start(String, String, Path, IntFunction)}. */
    static RealProcessServer launching(String storeUri, String listenAddress, Path stagingDirectory,
            IntFunction<List<String>> extraArguments) {
        return launching(storeUri, SharedMongo.OPERATOR_STATE_DATABASE, bootJar(), listenAddress,
                List.of(), stagingDirectory, extraArguments);
    }

    /** Prepares one initial member's owned paths, ports and command without creating a process. */
    static PreparedLaunch prepare(String storeUri, String listenAddress,
            IntFunction<List<String>> extraArguments) {
        return prepare(storeUri, listenAddress, null, extraArguments);
    }

    /** The same preparation, retaining a staging directory explicitly owned by the witness. */
    static PreparedLaunch prepare(String storeUri, String listenAddress, Path stagingDirectory,
            IntFunction<List<String>> extraArguments) {
        return prepare(storeUri, SharedMongo.OPERATOR_STATE_DATABASE, bootJar(), listenAddress,
                List.of(), stagingDirectory, extraArguments);
    }

    /** The same, listening on {@code listenAddress}. See {@link #start(String, String, IntFunction)}. */
    static RealProcessServer launching(String storeUri, Path jar, String listenAddress,
            IntFunction<List<String>> extraArguments) {
        return launching(storeUri, SharedMongo.OPERATOR_STATE_DATABASE, jar, listenAddress, extraArguments);
    }

    private static RealProcessServer launching(String storeUri, String operatorStateDatabase, Path jar,
            String listenAddress, IntFunction<List<String>> extraArguments) {
        return launching(storeUri, operatorStateDatabase, jar, listenAddress, List.of(), null, extraArguments);
    }

    /**
     * Launches this build in a JVM given {@code jvmOptions}, and returns once its health probe answers.
     *
     * <p>For a witness whose subject is how the deliverable behaves inside a runtime of a particular shape -- a
     * heap too small to hold a load whole, say. The options go before {@code -jar}, where the JVM reads them;
     * handed to the product instead they would be settings nobody reads.
     */
    static RealProcessServer startInJvm(String storeUri, List<String> jvmOptions) {
        return healthy(launching(storeUri, SharedMongo.OPERATOR_STATE_DATABASE, bootJar(), LOOPBACK,
                jvmOptions, null, port -> List.of()));
    }

    /**
     * Returns the launch once its health probe answers, and ends it otherwise.
     *
     * <p>A launch that never came up is not handed to anybody, so nobody else will ever end it or clear
     * up after it. Whatever goes wrong doing that is attached to the failure rather than replacing it:
     * why the server did not come up is the thing a reader needs.
     */
    private static RealProcessServer healthy(RealProcessServer server) {
        server.observe(ClusterLifecycleProbe.Phase.HEALTH_STARTED);
        try {
            awaitHealthy(server.process, server.baseUrl, server.output);
        } catch (RuntimeException | AssertionError e) {
            try {
                server.kill();
            } catch (RuntimeException | AssertionError cleanup) {
                e.addSuppressed(cleanup);
            }
            throw e;
        }
        server.observe(ClusterLifecycleProbe.Phase.HEALTH_PASSED);
        return server;
    }

    /** Waits on the same health predicate and budget used by a single-process start. */
    void awaitHealthy() {
        healthy(this);
    }

    private static RealProcessServer launching(String storeUri, String operatorStateDatabase, Path jar,
            String listenAddress, List<String> jvmOptions, Path stagingDirectory,
            IntFunction<List<String>> extraArguments) {
        try (PreparedLaunch prepared = prepare(storeUri, operatorStateDatabase, jar, listenAddress,
                jvmOptions, stagingDirectory, extraArguments)) {
            return prepared.launch();
        }
    }

    private static PreparedLaunch prepare(String storeUri, String operatorStateDatabase, Path jar,
            String listenAddress, List<String> jvmOptions, Path stagingDirectory,
            IntFunction<List<String>> extraArguments) {
        int port = freePort();
        // The literal address, not the name: "localhost" resolves to both 127.0.0.1 and ::1, and the
        // launch below binds only the first.
        URI baseUrl = URI.create("http://" + LOOPBACK + ":" + port);
        boolean owned = stagingDirectory == null;
        Path staging = stagingDirectory;
        try {
            Path workingDirectory = workingDirectory();
            Path output = workingDirectory.resolve("server.out");
            try { Files.createFile(output); }
            catch (IOException failure) {
                throw new UncheckedIOException("could not prepare the server output at " + output, failure);
            }
            if (owned) staging = ServerHandle.privateStagingDirectory();
            return new PreparedLaunch(jar, jvmOptions, port, listenAddress, storeUri, operatorStateDatabase,
                    workingDirectory, baseUrl, output, staging, owned, extraArguments.apply(port));
        } catch (RuntimeException | Error failure) {
            try { releasePort(port); }
            catch (RuntimeException | Error cleanup) { failure.addSuppressed(cleanup); }
            if (owned && staging != null) {
                try { ServerHandle.discardStagingDirectory(staging); }
                catch (RuntimeException | Error cleanup) { failure.addSuppressed(cleanup); }
            }
            // The working directory is diagnostic evidence, not a connector cache to discard.
            throw failure;
        }
    }

    /** One owned launch plan; handed-off processes and caller-owned staging are never cancelled here. */
    static final class PreparedLaunch implements AutoCloseable {
        private final Path jar;
        private final List<String> jvmOptions;
        private final int httpPort;
        private final String listenAddress;
        private final String storeUri;
        private final String operatorStateDatabase;
        private final Path workingDirectory;
        private final URI baseUrl;
        private final Path output;
        private final Path stagingDirectory;
        private final boolean ownsStagingDirectory;
        private final List<String> extraArguments;
        private final ClusterLifecycleProbe.Facts lifecycle;
        private boolean launchAttempted;
        private boolean handedOff;
        private boolean released;
        private Process unhandedProcess;
        private long forcedAt;

        private PreparedLaunch(Path jar, List<String> jvmOptions, int httpPort, String listenAddress,
                String storeUri, String operatorStateDatabase, Path workingDirectory, URI baseUrl,
                Path output, Path stagingDirectory, boolean ownsStagingDirectory, List<String> extraArguments) {
            this.jar = jar;
            this.jvmOptions = List.copyOf(jvmOptions);
            this.httpPort = httpPort;
            this.listenAddress = listenAddress;
            this.storeUri = storeUri;
            this.operatorStateDatabase = operatorStateDatabase;
            this.workingDirectory = workingDirectory;
            this.baseUrl = baseUrl;
            this.output = output;
            this.stagingDirectory = stagingDirectory;
            this.ownsStagingDirectory = ownsStagingDirectory;
            this.extraArguments = List.copyOf(extraArguments);
            this.lifecycle = ClusterLifecycleProbe.facts(httpPort, workingDirectory, output, stagingDirectory,
                    storeUri, operatorStateDatabase, listenAddress, this.jvmOptions, this.extraArguments);
            ClusterLifecycleProbe.record(ClusterLifecycleProbe.Phase.PREPARED, lifecycle, -1);
        }

        synchronized RealProcessServer launch() {
            if (released || launchAttempted) {
                throw new IllegalStateException("the prepared server launch is no longer available");
            }
            launchAttempted = true;
            try {
                unhandedProcess = RealProcessServer.launch(jar, jvmOptions, httpPort, listenAddress,
                        storeUri, operatorStateDatabase, workingDirectory, output, stagingDirectory, extraArguments);
                RealProcessServer server = new RealProcessServer(
                        unhandedProcess, baseUrl, output, stagingDirectory, ownsStagingDirectory);
                server.lifecycle = lifecycle;
                server.observe(ClusterLifecycleProbe.Phase.LAUNCHED);
                handedOff = true;
                unhandedProcess = null;
                return server;
            } catch (RuntimeException | Error failure) {
                try { close(); }
                catch (RuntimeException | Error cleanup) {
                    if (cleanup != failure) failure.addSuppressed(cleanup);
                }
                throw failure;
            }
        }

        int httpPort() {
            return httpPort;
        }

        Path workingDirectory() {
            return workingDirectory;
        }

        Path output() {
            return output;
        }

        Path stagingDirectory() {
            return stagingDirectory;
        }

        @Override
        public synchronized void close() {
            if (handedOff || released) return;
            List<Throwable> failures = new ArrayList<>();
            InterruptedException interrupted = null;
            boolean exited = unhandedProcess == null || !unhandedProcess.isAlive();
            try {
                if (!exited) {
                    try {
                        unhandedProcess.destroyForcibly();
                        if (forcedAt == 0) forcedAt = System.nanoTime();
                        while (!exited) {
                            long remaining = SHUTDOWN_BUDGET.toNanos() - (System.nanoTime() - forcedAt);
                            if (remaining <= 0) break;
                            try {
                                exited = unhandedProcess.waitFor(remaining, TimeUnit.NANOSECONDS);
                            } catch (InterruptedException failure) {
                                if (interrupted == null) interrupted = failure;
                                else interrupted.addSuppressed(failure);
                            }
                        }
                    } catch (RuntimeException | Error failure) {
                        failures.add(failure);
                    }
                }
                boolean exitConfirmed = exited && (unhandedProcess == null || !unhandedProcess.isAlive());
                if (!exitConfirmed) {
                    failures.add(new AssertionError("the unhanded server did not exit within "
                            + SHUTDOWN_BUDGET + "; its output was:\n" + tail(output), interrupted));
                }
                try { releasePort(httpPort); }
                catch (RuntimeException | Error failure) { failures.add(failure); }
                if (exitConfirmed && ownsStagingDirectory) {
                    try { ServerHandle.discardStagingDirectory(stagingDirectory); }
                    catch (RuntimeException | Error failure) { failures.add(failure); }
                }
                if (interrupted != null) {
                    failures.add(new AssertionError("interrupted while cancelling the prepared server", interrupted));
                }
                if (!failures.isEmpty()) {
                    Throwable primary = failures.getFirst();
                    failures.stream().skip(1).filter(failure -> failure != primary).forEach(primary::addSuppressed);
                    if (primary instanceof RuntimeException failure) throw failure;
                    throw (Error) primary;
                }
                released = true;
                // Logs and the private working directory are retained even when no JVM could start.
            } finally {
                if (interrupted != null) Thread.currentThread().interrupt();
            }
        }
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
            if (!process.waitFor(SHUTDOWN_BUDGET.toMillis(), TimeUnit.MILLISECONDS)) {
                throw new AssertionError("the killed server did not exit within " + SHUTDOWN_BUDGET
                        + "; its output was:\n" + tail());
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new AssertionError("interrupted while waiting for the killed server to go away", e);
        }
        // The harness's own leftovers, not the server's: a crash witness may never close what it killed.
        discardOwnStagingDirectory();
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

    /**
     * The operating system's id for this process: what a connector running inside it names itself by, and so how
     * a case finds the member carrying a write the connector reports.
     */
    long pid() {
        return process.pid();
    }

    private void observe(ClusterLifecycleProbe.Phase phase) {
        if (lifecycle != null) ClusterLifecycleProbe.record(phase, lifecycle, pid());
    }

    /** Where this launch staged its connectors, for a witness that the directory goes when the launch does. */
    Path stagingDirectory() {
        return stagingDirectory;
    }

    /** Whether it is still running, so a witness waiting on it can tell waiting from waiting forever. */
    boolean isAlive() {
        return process.isAlive();
    }

    /** Waits for all startup runners, not merely the earlier Tomcat health response. */
    void awaitReady() {
        observe(ClusterLifecycleProbe.Phase.READY_STARTED);
        ControlPlane control = new ControlPlane(baseUrl);
        Await.until("the process to complete application startup", STARTUP_BUDGET, () -> {
            if (!process.isAlive()) {
                throw new AssertionError("the server exited with status " + process.exitValue()
                        + " before application readiness; its output was:\n" + tail());
            }
            try {
                return Files.readString(output).contains("Tapstate application is ready") && control.healthy();
            } catch (IOException failure) {
                throw new UncheckedIOException("could not read application readiness", failure);
            }
        }, this::tail);
        observe(ClusterLifecycleProbe.Phase.READY_PASSED);
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
        requestClose();
        awaitClosed();
    }

    /** Requests this owned process's polite exit without waiting for another member. */
    synchronized void requestClose() {
        if (closeRequested) return;
        closeRequestedAt = System.nanoTime();
        closeRequested = true;
        process.destroy();
        observe(ClusterLifecycleProbe.Phase.QUIT_REQUESTED);
    }

    /** Confirms exit under the original grace and forced-exit budgets before discarding staging. */
    void awaitClosed() {
        long requestedAt;
        synchronized (this) {
            if (!closeRequested) {
                throw new IllegalStateException("the owned server has not been asked to close");
            }
            requestedAt = closeRequestedAt;
        }
        InterruptedException interrupted = null;
        try {
            boolean exited = !process.isAlive();
            long remainingGrace = SHUTDOWN_BUDGET.toNanos() - (System.nanoTime() - requestedAt);
            if (!exited && remainingGrace > 0) {
                try {
                    exited = process.waitFor(remainingGrace, TimeUnit.NANOSECONDS);
                } catch (InterruptedException failure) {
                    interrupted = failure;
                }
            }
            if (!exited) {
                process.destroyForcibly();
                observe(ClusterLifecycleProbe.Phase.FORCE_REQUESTED);
                long forcedAt = System.nanoTime();
                while (!exited) {
                    long remaining = SHUTDOWN_BUDGET.toNanos() - (System.nanoTime() - forcedAt);
                    if (remaining <= 0) break;
                    try {
                        exited = process.waitFor(remaining, TimeUnit.NANOSECONDS);
                    } catch (InterruptedException failure) {
                        if (interrupted == null) interrupted = failure;
                        else interrupted.addSuppressed(failure);
                    }
                }
            }
            if (!exited || process.isAlive()) {
                throw new AssertionError("the server did not exit after its " + SHUTDOWN_BUDGET
                        + " grace and " + SHUTDOWN_BUDGET + " forced-exit budgets; its output was:\n"
                        + tail(), interrupted);
            }
            observe(ClusterLifecycleProbe.Phase.EXIT_CONFIRMED);
            AssertionError interruptedFailure = interrupted == null ? null : new AssertionError(
                    "interrupted while waiting for the server to close; its exit was confirmed", interrupted);
            try {
                discardOwnStagingDirectory();
            } catch (RuntimeException | Error cleanup) {
                if (interruptedFailure == null) throw cleanup;
                interruptedFailure.addSuppressed(cleanup);
            }
            if (interruptedFailure != null) throw interruptedFailure;
        } finally {
            if (interrupted != null) Thread.currentThread().interrupt();
        }
    }

    private void discardOwnStagingDirectory() {
        if (ownsStagingDirectory) {
            ServerHandle.discardStagingDirectory(stagingDirectory);
        }
    }

    private static Process launch(Path jar, List<String> jvmOptions, int port, String listenAddress,
            String storeUri, String operatorStateDatabase, Path workingDirectory, Path output,
            Path stagingDirectory, List<String> extraArguments) {
        List<String> command = new ArrayList<>();
        command.add(javaBinary());
        command.addAll(jvmOptions);
        command.addAll(List.of(
                "-jar",
                jar.toString(),
                // The role the deliverable is documented to take; parsed by the product before Spring starts.
                "--role=all",
                // Bound to the loopback, which is where the probe and
                // every case dial it. Left to itself the product binds the wildcard, and a wildcard bind
                // does not own 127.0.0.1:<port> -- a process already holding that port on the loopback
                // keeps receiving the requests, and this server answers none of them.
                "--server.address=" + listenAddress,
                "--server.port=" + port,
                "--tapstate.store.mongo.enabled=true",
                "--tapstate.store.mongo.uri=" + storeUri,
                "--" + ServerHandle.OPERATOR_STATE_DATABASE_SETTING + "=" + operatorStateDatabase,
                "--tapstate.store.mongo.server-selection-timeout=5s",
                // A staging directory of this launch's own unless a witness names one, for the same reason
                // the other tier gets one: the cache is content-addressed and reused, so a shared one serves
                // a stale connector.
                "--" + ServerHandle.PLUGINS_DIRECTORY_SETTING + "=" + stagingDirectory,
                "--" + ServerHandle.ALSO_ACCEPT_IDS_SETTING + "=" + E2eConnectorJar.CONNECTOR_ID));
        // After the standing ones, and additional to them rather than replacing any: a repeated option
        // is joined with the earlier one by comma rather than winning over it, so anything a case needs
        // to set differently is a parameter above instead of an argument here.
        command.addAll(extraArguments);
        try {
            releaseLaunchPorts(port, extraArguments);
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
     * <p>The reservation stays bound until the process or link takes it. Two calls never answer the
     * same port, and a member waiting for its turn to launch cannot lose its port to an earlier
     * member's outbound connection.
     */
    static int reservePort() {
        return freePort();
    }

    private static synchronized int freePort() {
        try {
            while (true) {
                Socket socket = new Socket();
                try {
                    // Members bind a routable address, links bind loopback, and HTTP can bind both.
                    // Disallow address reuse so the reservation owns the port on every local address.
                    // A bound client socket reserves without listening to a member's discovery dials.
                    socket.setReuseAddress(false);
                    socket.bind(new InetSocketAddress(InetAddress.getByName("0.0.0.0"), 0));
                    int port = socket.getLocalPort();
                    if (!PORT_RESERVATIONS.containsKey(port)) {
                        PORT_RESERVATIONS.put(port, socket);
                        return port;
                    }
                } catch (IOException failure) {
                    socket.close();
                    throw failure;
                }
                socket.close();
            }
        } catch (IOException e) {
            throw new UncheckedIOException("could not reserve a port for the server", e);
        }
    }

    /** Releases only a reservation made here, immediately before its listener takes the port. */
    static synchronized void releasePort(int port) {
        Socket socket = PORT_RESERVATIONS.get(port);
        if (socket != null) {
            try {
                socket.close();
            } catch (IOException e) {
                throw new UncheckedIOException("could not release reserved port " + port, e);
            }
        }
    }

    private static synchronized void releaseLaunchPorts(int httpPort, List<String> extraArguments) {
        releasePort(httpPort);
        for (int reserved : PORT_RESERVATIONS.keySet()) {
            if (extraArguments.contains("--tapstate.hz.member-port=" + reserved)) {
                releasePort(reserved);
            }
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
