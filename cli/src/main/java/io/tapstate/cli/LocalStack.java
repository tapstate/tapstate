package io.tapstate.cli;

import io.tapstate.core.common.TapstateException;

import java.io.IOException;
import java.io.InputStream;
import java.io.PrintWriter;
import java.io.UncheckedIOException;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.nio.file.attribute.PosixFileAttributeView;
import java.nio.file.attribute.PosixFilePermissions;
import java.security.SecureRandom;
import java.util.Base64;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.BooleanSupplier;
import java.util.function.LongConsumer;
import java.util.function.Predicate;
import java.util.function.Supplier;
import java.util.function.UnaryOperator;

/**
 * The local development stack the guided first run starts when nothing is listening on the default
 * server ({@code docs/first-run/README.md}, "Step 1 - which server"): the server and its managed store,
 * in Docker, on the loopback port the default names, with the official connectors staged for the
 * server to register at boot and a first administrator created by the stack itself.
 *
 * <p>It lives under the user's home, beside the context store, and it is started only through the
 * {@link ProcessRunner} seam - {@code docker compose up -d} in that directory, which is idempotent, so
 * a second start over a running stack changes nothing. The {@code .env} holding the generated admin
 * password is written once and kept: regenerating it would lock out the admin the first run created.
 *
 * <p>There is no verb to stop, inspect or upgrade it, and there will not be: the one line printed
 * after a start - the {@code docker compose ... down} to run - is the whole interface. What this
 * class does not do is sign in or bind anything; it hands back the admin credentials and the guided
 * flow takes it from there, through the same services every other sign-in goes through.
 */
final class LocalStack {

    /** Where the stack lives, under the home directory. */
    static final String DIR = ".tapstate/local-stack";
    static final String COMPOSE_FILE = "docker-compose.yml";
    private static final String ENV_FILE = ".env";
    private static final String CONNECTORS_DIR = "connectors";
    private static final String COMPOSE_RESOURCE = "/local-stack/" + COMPOSE_FILE;
    /** The token the bundled compose file carries where the server's version goes. */
    private static final String VERSION_TOKEN = "@TAPSTATE_VERSION@";

    static final String ADMIN_USER = "admin";
    private static final String ENV_ADMIN_USER = "TAPSTATE_ADMIN_USER";
    private static final String ENV_ADMIN_PASSWORD = "TAPSTATE_ADMIN_PASSWORD";

    /**
     * Where the connector jars are published, and the two environment names that redirect it - the
     * same location and the same names the quickstart script reads, so a mirror configured for one
     * serves the other.
     */
    private static final String RELEASES = "https://github.com/tapstate/tapstate/releases";
    private static final String CONNECTORS_PATH = "/download/connectors-preview";
    private static final String ENV_CONNECTORS_URL = "TAPSTATE_CONNECTORS_URL";
    private static final String ENV_BASE_URL = "TAPSTATE_BASE_URL";

    /**
     * The connector jars staged for the server to register at boot. Three, not the whole official
     * list: the release publishes one jar per engine, and the managed variants of each engine
     * (aliyun-rds-mysql, mongodb-atlas and the rest) have no published artifact of their own to fetch.
     * The set grows here when a jar is published, never ahead of one.
     */
    static final List<String> CONNECTOR_JARS = List.of("mysql", "mongodb", "postgres");

    /** How long the stack is given to answer its health probe: this many polls, this far apart. */
    static final int HEALTH_POLLS = 120;
    static final long POLL_MILLIS = 1_000;

    /** How a jar reaches the connector directory; the production one is an HTTP GET, tests inject a fake. */
    interface Downloader {
        void download(URI from, Path to) throws IOException;

        /** Fetches over HTTP, following the redirect a release download answers with. */
        static Downloader http() {
            return (from, to) -> {
                HttpClient client = HttpClient.newBuilder().followRedirects(HttpClient.Redirect.NORMAL).build();
                // to a sibling first, then moved: a jar the server finds half-written would be registered
                // half-written, and a re-run must be able to tell "fetched" from "started fetching"
                Path part = to.resolveSibling(to.getFileName() + ".part");
                try {
                    HttpResponse<Path> response = client.send(
                            HttpRequest.newBuilder(from).GET().build(), HttpResponse.BodyHandlers.ofFile(part));
                    if (response.statusCode() != 200) {
                        throw new IOException("HTTP " + response.statusCode() + " from " + from);
                    }
                    Files.move(part, to, StandardCopyOption.REPLACE_EXISTING, StandardCopyOption.ATOMIC_MOVE);
                } catch (InterruptedException interrupted) {
                    Thread.currentThread().interrupt();
                    throw new IOException("interrupted while fetching " + from, interrupted);
                } finally {
                    Files.deleteIfExists(part);
                }
            };
        }
    }

    /** The administrator the stack's bootstrap service creates, as written to its {@code .env}. */
    record Admin(String user, String password) {
        @Override
        public String toString() {
            return "Admin[user=" + user + ", password=<redacted>]";
        }
    }

    private final Path dir;
    private final ProcessRunner runner;
    private final Downloader downloader;
    private final BooleanSupplier dockerOnPath;
    private final ControlPlaneClient probe;
    private final UnaryOperator<String> env;
    private final LongConsumer sleeper;
    private final int healthPolls;

    /**
     * @param home         the home directory the stack lives under - the context store's home
     * @param dockerOnPath whether {@code docker} is on the PATH, asked before anything is written
     * @param probe        what answers the health question once the stack is up
     * @param env          the process environment, for the connector mirror settings
     * @param sleeper      what waits between two health polls; a test injects one that does not
     * @param healthPolls  how many polls before the stack is given up on
     */
    LocalStack(Path home, ProcessRunner runner, Downloader downloader, BooleanSupplier dockerOnPath,
               ControlPlaneClient probe, UnaryOperator<String> env, LongConsumer sleeper, int healthPolls) {
        this.dir = home.resolve(DIR);
        this.runner = runner;
        this.downloader = downloader;
        this.dockerOnPath = dockerOnPath;
        this.probe = probe;
        this.env = env;
        this.sleeper = sleeper;
        this.healthPolls = healthPolls;
    }

    /** The production stack under {@code home}: real processes, real downloads, real waiting. */
    static LocalStack under(Path home, ControlPlaneClient probe, UnaryOperator<String> env) {
        return new LocalStack(home, ProcessRunner.system(), Downloader.http(), DockerBinary::isOnThePath, probe,
                env, LocalStack::pause, HEALTH_POLLS);
    }

    /** Where the stack lives on disk. */
    Path dir() {
        return dir;
    }

    /** The one command that stops it - the whole of the stack's interface after a start. */
    String stopCommand() {
        return "docker compose -f " + dir.resolve(COMPOSE_FILE) + " down";
    }

    /**
     * The admin an earlier start wrote to the stack's {@code .env}, for a run that finds the stack
     * already listening; empty when no stack was ever started here.
     */
    Optional<Admin> savedAdmin() throws IOException {
        Map<String, String> values = DotEnv.read(dir.resolve(ENV_FILE));
        String password = values.get(ENV_ADMIN_PASSWORD);
        if (password == null || password.isEmpty()) {
            return Optional.empty();
        }
        return Optional.of(new Admin(values.getOrDefault(ENV_ADMIN_USER, ADMIN_USER), password));
    }

    /**
     * Brings the stack up and waits until the server answers, then returns the admin to sign in as.
     * Preflight first, so a machine without Docker is refused before anything is written - which is why
     * the preflight runs from wherever the CLI was started, not from the stack directory: on a first run
     * that directory does not exist yet, and no program can be started in a directory that is not there.
     * The compose file is rewritten every time (it is the CLI's, and carries the CLI's version), the
     * {@code .env} and the jars are written only when absent.
     *
     * @param prose where to say that the wait has started, or null when nobody is reading
     * @throws TapstateException {@code cli.docker-unavailable}, with what went wrong in {@code reason}
     */
    Admin start(PrintWriter prose) throws IOException {
        if (!dockerOnPath.getAsBoolean()) {
            throw unavailable("no docker command is on the PATH");
        }
        ProcessRunner.Result compose = run(null, "docker", "compose", "version");
        if (!compose.succeeded() || compose.stdout().isBlank()) {
            throw unavailable("docker compose is not available"
                    + (compose.stderr().isBlank() ? "" : ": " + compose.stderr().strip()));
        }
        createDirectories();
        Files.writeString(dir.resolve(COMPOSE_FILE), composeFile(Cli.VERSION_NUMBER));
        Admin admin = savedAdmin().orElseGet(this::writeAdmin);
        stageConnectors();
        if (prose != null) {
            prose.println("Starting the local development stack in Docker; the first start pulls images and can take a few minutes.");
            prose.flush();
        }
        ProcessRunner.Result up = run(dir, "docker", "compose", "up", "-d");
        if (!up.succeeded()) {
            throw unavailable("docker compose up failed" + (up.stderr().isBlank() ? "" : ": " + up.stderr().strip()));
        }
        Boolean healthy = retryWhile(() -> probe.isHealthy(GuidedNew.DEFAULT_SERVER), answered -> !answered);
        if (!healthy) {
            throw unavailable("the stack in " + dir + " was started but did not answer on " + GuidedNew.DEFAULT_SERVER_TEXT
                    + " within " + (healthPolls * POLL_MILLIS / 1_000) + " s; look at its logs with"
                    + " docker compose -f " + dir.resolve(COMPOSE_FILE) + " logs, or stop it with " + stopCommand());
        }
        return admin;
    }

    /**
     * Calls {@code attempt} until its answer stops satisfying {@code notYet}, waiting the poll interval
     * between calls, for at most the health bound; the last answer is handed back either way, so the
     * caller reports what it actually got. The same bound as the health wait because it is the same
     * wait: the stack's bootstrap service creates the admin a moment after the server answers, and a
     * sign-in that lands in that moment is refused, not failed.
     */
    <T> T retryWhile(Supplier<T> attempt, Predicate<T> notYet) {
        T answer = attempt.get();
        for (int poll = 1; poll < healthPolls && notYet.test(answer); poll++) {
            sleeper.accept(POLL_MILLIS);
            answer = attempt.get();
        }
        return answer;
    }

    /** Runs {@code command} in {@code where}, or from the CLI's own directory when that is null. */
    private ProcessRunner.Result run(Path where, String... command) {
        try {
            return runner.run(where, List.of(command));
        } catch (IOException notStarted) {
            throw unavailable(String.join(" ", command) + " could not be run: " + notStarted.getMessage());
        }
    }

    private static TapstateException unavailable(String reason) {
        return new TapstateException(CliError.DOCKER_UNAVAILABLE, Map.of("reason", reason), null);
    }

    /**
     * The stack directory and its connector directory, and above them the {@code .tapstate} root the
     * context store and the session store share. That root is theirs: they refuse it unless it is
     * owner-only, so when this is the first thing to create it, it is created the way they would.
     * The stack's own directories stay at the default mode - the server in the container reads the
     * connector directory, and it is not the user.
     */
    private void createDirectories() throws IOException {
        Path root = dir.getParent();
        if (!Files.exists(root)) {
            if (Files.getFileAttributeView(root.getParent(), PosixFileAttributeView.class) != null) {
                Files.createDirectories(root, PosixFilePermissions.asFileAttribute(
                        PosixFilePermissions.fromString("rwx------")));
            } else {
                Files.createDirectories(root);
            }
        }
        Files.createDirectories(dir.resolve(CONNECTORS_DIR));
    }

    /** Writes the {@code .env} with a fresh random password, readable by this user alone where the filesystem can say so. */
    private Admin writeAdmin() {
        byte[] random = new byte[24];
        new SecureRandom().nextBytes(random);
        // url-safe base64: no '$', which Compose would read as interpolation, and no quoting needed
        String password = Base64.getUrlEncoder().withoutPadding().encodeToString(random);
        Path file = dir.resolve(ENV_FILE);
        try {
            Files.writeString(file, ENV_ADMIN_USER + "=" + ADMIN_USER + "\n" + ENV_ADMIN_PASSWORD + "=" + password + "\n");
            try {
                Files.setPosixFilePermissions(file, PosixFilePermissions.fromString("rw-------"));
            } catch (UnsupportedOperationException notPosix) {
                // a filesystem with no POSIX bits (Windows) has no owner-only mode to set
            }
        } catch (IOException unwritable) {
            throw new UncheckedIOException("cannot write " + file, unwritable);
        }
        return new Admin(ADMIN_USER, password);
    }

    /** Fetches each published connector jar that is not already staged. */
    private void stageConnectors() throws IOException {
        String base = connectorsBase();
        for (String id : CONNECTOR_JARS) {
            String jar = id + "-connector.jar";
            Path target = dir.resolve(CONNECTORS_DIR).resolve(jar);
            if (Files.exists(target)) {
                continue;
            }
            URI from = URI.create(base + jar);
            try {
                downloader.download(from, target);
            } catch (IOException failed) {
                throw unavailable("the connector jar " + from + " could not be fetched: " + failed.getMessage());
            }
        }
    }

    private String connectorsBase() {
        String explicit = env.apply(ENV_CONNECTORS_URL);
        String base;
        if (explicit != null && !explicit.isBlank()) {
            base = explicit;
        } else {
            String releases = env.apply(ENV_BASE_URL);
            base = (releases == null || releases.isBlank() ? RELEASES : releases) + CONNECTORS_PATH;
        }
        return base.endsWith("/") ? base : base + "/";
    }

    /** The compose file as written: the bundled one, with the CLI's own version in the server image. */
    static String composeFile(String version) {
        String bundled = bundledCompose();
        if (!bundled.contains(VERSION_TOKEN)) {
            throw new IllegalStateException("the bundled compose file carries no " + VERSION_TOKEN + " to pin");
        }
        return bundled.replace(VERSION_TOKEN, version);
    }

    /** The bundled compose file, verbatim. Absent means a broken build, not a user error, so it crashes bare. */
    static String bundledCompose() {
        try (InputStream in = LocalStack.class.getResourceAsStream(COMPOSE_RESOURCE)) {
            if (in == null) {
                throw new IllegalStateException("the resource " + COMPOSE_RESOURCE + " is not on the classpath");
            }
            return new String(in.readAllBytes(), StandardCharsets.UTF_8);
        } catch (IOException unreadable) {
            throw new UncheckedIOException("cannot read the bundled resource " + COMPOSE_RESOURCE, unreadable);
        }
    }

    private static void pause(long millis) {
        try {
            Thread.sleep(millis);
        } catch (InterruptedException interrupted) {
            Thread.currentThread().interrupt();
        }
    }
}
