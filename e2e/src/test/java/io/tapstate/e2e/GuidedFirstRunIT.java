package io.tapstate.e2e;

import io.tapstate.testsupport.DockerGate;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.MethodOrderer;
import org.junit.jupiter.api.Order;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestMethodOrder;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.Statement;
import java.time.Duration;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.regex.Pattern;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * The guided first run, end to end: {@code tapstate new} writes a workspace bound to a real server
 * and signed in to it, {@code tapstate up} brings that workspace to running with nothing on its
 * command line, and the rows of a real table end up in the view store where the recipe said they
 * would.
 *
 * <p>The CLI runs as its own process, twice over, and that is the claim: what {@code new} saved
 * under the home directory is all a later process has, so a session that was only ever held in
 * memory, or a binding written to the wrong place, is the failure this catches and no in-JVM test
 * can. The JVM is handed a home directory of its own, so neither process reads or writes the home of
 * whoever runs the build.
 *
 * <p>The document count is the assertion that discriminates. The state line says a pipeline was
 * started; only the rows arriving in the view say the source was discovered and read - a run that
 * skipped discovery starts a pipeline that stays at running with nothing in its view. The second
 * run's count is what says convergence changed nothing: a re-applied pipeline or a second snapshot
 * would show up there before anywhere else.
 *
 * <p>Gated the way every real-connector witness is: on Docker for the databases, and on a directory
 * of real connector jars - the source is a real MySQL connector, and the view store the server seeds
 * at start is reached through the real MongoDB one.
 */
@DisplayName("the guided first run: new, up, up again, and a preflight that names its stage")
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
class GuidedFirstRunIT {

    private static final String USER = "e2e";
    private static final String PASSWORD = "e2e-password";
    private static final String DATABASE = "e2e_first_run";
    private static final String TABLE = "orders";
    /** Unique on the shared view store: every run in this JVM materializes into the one views database. */
    private static final String VIEW = "first_run_orders";
    private static final String SOURCE_ID = TABLE + "_src";
    private static final String PIPELINE_ID = TABLE + "_sync";
    private static final long SEEDED_ROWS = 5;
    private static final Duration TIMEOUT = Duration.ofSeconds(120);
    private static final Duration POLL = Duration.ofMillis(250);

    /** The stage, the resource and then a code: the shape a failure of {@code up} is promised to take. */
    private static final Pattern PREFLIGHT_FAILURE =
            Pattern.compile("up: preflight failed on " + SOURCE_ID + ": (\\S+) ");

    private static ServerHandle server;
    private static MongoEndpoints mongo;
    private static EndpointAddress viewStore;
    private static Map<String, Object> mysql;
    private static Path home;
    private static Path workspace;
    @TempDir
    static Path temporaryRoot;

    @BeforeAll
    static void bringUpTheWorld() throws Exception {
        DockerGate.require();
        RealConnectorGate.require("mysql", "mongodb");
        mysql = SharedMySql.settings(DATABASE);
        seed(mysql, SEEDED_ROWS);
        server = Tiers.IN_PROCESS.launch(SharedMongo.replicaSetUrl(DATABASE));
        ControlPlane control = new ControlPlane(server.baseUrl());
        control.bootstrapAndLogin(USER, PASSWORD);
        // The connectors a first run needs, registered the way every real-connector witness registers
        // them: the source's own, and the one the server's view store is reached through.
        control.registerConnector("mysql", ConnectorJars.bytesFor("mysql"));
        control.registerConnector("mongodb", ConnectorJars.bytesFor("mongodb"));
        mongo = new MongoEndpoints();
        // Views land in the "views" database of the server's own replica set, under the view's id.
        viewStore = EndpointAddress.uri(SharedMongo.replicaSetUrl("views"));
        home = Files.createDirectories(temporaryRoot.resolve("home"));
        workspace = Files.createDirectories(temporaryRoot.resolve("workspace"));
    }

    @AfterAll
    static void bringItDown() {
        if (mongo != null) {
            mongo.close();
        }
        if (server != null) {
            server.close();
        }
    }

    @Test
    @Order(1)
    void newWritesAWorkspaceOfflineAndUpBindsItAndBringsItToRunning() throws IOException {
        // `new` is given no server, no user and no password: it writes files and reaches nothing.
        CliOnce.Run created = cli(null, "new", "mirrored-table", "--yes",
                "--connector", "mysql",
                "--set", "host=" + mysql.get("host"),
                "--set", "port=" + mysql.get("port"),
                "--set", "database=" + DATABASE,
                "--set", "username=" + mysql.get("username"),
                "--set", "password=" + mysql.get("password"),
                "--table", TABLE, "--view", VIEW,
                "-w", workspace.toString());
        assertThat(created.exitCode()).as(report("new", created)).isZero();

        Path source = workspace.resolve("source/" + SOURCE_ID + ".tap.yml");
        assertThat(source).exists();
        assertThat(workspace.resolve("pipeline/" + PIPELINE_ID + ".tap.yml")).exists();
        assertThat(workspace.resolve(".env")).exists();
        assertThat(workspace.resolve(".gitignore")).exists();
        // The secret is a reference in the artifact and a value in .env, never a value in the artifact.
        String reference = "${" + SOURCE_ID.toUpperCase(Locale.ROOT) + "_PASSWORD}";
        String sourceText = Files.readString(source, StandardCharsets.UTF_8);
        assertThat(sourceText).contains(reference);
        assertThat(sourceText)
                .as("the password may not be written into the source artifact")
                .doesNotContainPattern("(?m)^\\s*password:\\s*\"?" + Pattern.quote(String.valueOf(mysql.get("password"))));

        // The workspace is unbound, because `new` binds nothing: this first `up` is where the server is
        // named, signed in to and bound, and the second one below runs with none of it on its line.
        CliOnce.Run up = cli(PASSWORD, "up",
                "--server", server.baseUrl().toString(), "--user", USER,
                "-w", workspace.toString());
        assertThat(up.exitCode()).as(report("up", up)).isZero();
        assertThat(up.stdout()).contains("State: running");
        awaitCount(VIEW, SEEDED_ROWS);
    }

    @Test
    @Order(2)
    void upAgainConvergesOnAWorkspaceThatIsAlreadyUpAndChangesNothing() {
        // nothing on this line and nothing in the environment: what the first up saved is the whole credential
        CliOnce.Run again = cli(null, "up", "-w", workspace.toString());
        assertThat(again.exitCode()).as(report("up (again)", again)).isZero();
        assertThat(again.stdout()).contains("State: running (nothing to do)");
        assertThat(mongo.count(viewStore, VIEW))
                .as("a second up neither re-snapshots nor starts a second pipeline into the view")
                .isEqualTo(SEEDED_ROWS);
    }

    @Test
    @Order(3)
    void aSourceThatDoesNotAnswerFailsPreflightByStageAndCode() throws IOException {
        Path unreachable = Files.createDirectories(temporaryRoot.resolve("unreachable"));
        copy(workspace, unreachable);
        Path source = unreachable.resolve("source/" + SOURCE_ID + ".tap.yml");
        String pointedAtAClosedPort = Files.readString(source, StandardCharsets.UTF_8)
                .replaceAll("(?m)^(\\s*host:\\s*\"?)[^\"\\s]+", "$1" + "127.0.0.1")
                .replaceAll("(?m)^(\\s*port:\\s*\"?)\\d+", "$1" + closedPort());
        Files.writeString(source, pointedAtAClosedPort, StandardCharsets.UTF_8);

        // The copy is bound to nothing, so the server and the sign-in travel on the launch line; what
        // is under test is the stage the run stops at, not how it reached the server.
        CliOnce.Run up = cli(PASSWORD, "-c", server.baseUrl().toString(), "-u", USER,
                "up", "-w", unreachable.toString());

        assertThat(up.exitCode()).as(report("up (unreachable source)", up)).isEqualTo(1);
        assertThat(up.stderr())
                .as("the failure names the stage and the resource, then a code - never the command that ran")
                .containsPattern(PREFLIGHT_FAILURE);
    }

    /** One CLI process with a home directory of its own; the password goes in the environment, when given. */
    private static CliOnce.Run cli(String password, String... args) {
        return CliOnce.runWithPassword(password, List.of("-Duser.home=" + home), args);
    }

    private static String report(String what, CliOnce.Run run) {
        return what + " exited " + run.exitCode() + "\nstdout:\n" + run.stdout() + "\nstderr:\n" + run.stderr();
    }

    /** Reads the view the way a user would, from outside the product, until the rows are all there. */
    private static void awaitCount(String collection, long expected) {
        long deadline = System.nanoTime() + TIMEOUT.toNanos();
        long last = -1;
        while (System.nanoTime() - deadline < 0) {
            last = mongo.count(viewStore, collection);
            if (last == expected) {
                return;
            }
            sleep();
        }
        assertThat(last)
                .as("documents in the view %s after a first run over %d real MySQL rows", collection, expected)
                .isEqualTo(expected);
    }

    private static void seed(Map<String, Object> settings, long rows) throws Exception {
        try (Connection connection = SharedMySql.connect(settings)) {
            try (Statement statement = connection.createStatement()) {
                statement.execute("CREATE TABLE " + TABLE + " (id INT PRIMARY KEY, name VARCHAR(64))");
            }
            try (PreparedStatement insert =
                    connection.prepareStatement("INSERT INTO " + TABLE + " (id, name) VALUES (?, ?)")) {
                for (long id = 1; id <= rows; id++) {
                    insert.setLong(1, id);
                    insert.setString(2, "order-" + id);
                    insert.addBatch();
                }
                insert.executeBatch();
            }
        }
    }

    /** A loopback port nothing listens on: bound to learn the number, then released. */
    private static int closedPort() throws IOException {
        try (ServerSocket socket = new ServerSocket(0, 1, InetAddress.getLoopbackAddress())) {
            return socket.getLocalPort();
        }
    }

    /** The workspace as new wrote it, dotfiles included, at a directory bound to nothing. */
    private static void copy(Path from, Path to) throws IOException {
        try (Stream<Path> files = Files.walk(from)) {
            for (Path file : files.toList()) {
                Path target = to.resolve(from.relativize(file).toString());
                if (Files.isDirectory(file)) {
                    Files.createDirectories(target);
                } else {
                    Files.copy(file, target);
                }
            }
        }
    }

    private static void sleep() {
        try {
            Thread.sleep(POLL.toMillis());
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new AssertionError("interrupted while waiting for the first run's rows to reach the view", e);
        }
    }
}
