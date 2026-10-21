package io.tapstate.e2e;

import io.tapstate.testsupport.DockerGate;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.utility.DockerImageName;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.LinkedHashMap;
import java.util.Map;

/**
 * One PostgreSQL server for every specification in the JVM, the arrangement {@link SharedMySql} and
 * {@link SharedMongo} already make and for the same reason: a container per test class costs a start-up
 * each time and buys nothing, because runs stay independent by taking a database of their own rather
 * than a daemon of their own. Ryuk reaps the container when the JVM exits, so there is no stop to forget.
 *
 * <p>Started with logical decoding on, which is the one setting that cannot be added later. The image
 * ships {@code wal_level=replica}: a write-ahead log physical replication can follow and logical decoding
 * cannot. A change-data-capture read against that server does not fail where it is configured - it fails
 * much later, when the connector asks for a replication slot, and only the streaming half fails, so the
 * snapshot half of the same specification passes. Turning it on here means a case never meets that
 * half-working state.
 *
 * <p>Two settings differ from MySQL's arrangement and are worth naming, because both are the kind of
 * thing that reads as a connector defect when it is really a server that was never asked for it:
 * replication needs a sender slot per stream, and this is one server per JVM shared by every postgres
 * case, whose slots are not dropped when a stream stops. Both settings boot at 10 on this image, which a
 * full pass exhausts; the ceiling is raised rather than merely restated, and exhaustion surfaces as
 * {@code all replication slots are in use} rather than as anything naming the server.
 *
 * <p>Unlike MySQL, no privilege grant is needed. The image's own superuser is the account the harness
 * connects as, and it already carries the replication attribute; the MySQL equivalent exists only
 * because that image's default test user does not.
 */
final class SharedPostgres {

    private static final DockerImageName IMAGE = DockerImageName.parse("postgres:16");

    private static PostgreSQLContainer<?> container;

    private SharedPostgres() {
    }

    /**
     * The settings addressing a database of the caller's own on the shared server, spelled the way a
     * resource spells them - host, port, database, username, password, no one of which is the address.
     */
    static synchronized Map<String, Object> settings(String database) {
        PostgreSQLContainer<?> server = server();
        provision(server, database);
        Map<String, Object> settings = new LinkedHashMap<>();
        settings.put("host", server.getHost());
        settings.put("port", server.getMappedPort(PostgreSQLContainer.POSTGRESQL_PORT));
        settings.put("database", database);
        settings.put("username", server.getUsername());
        settings.put("password", server.getPassword());
        return settings;
    }

    /**
     * A connection to the database those settings address, for a test laying down a fixture of its own.
     *
     * <p>Deliberately not the path {@link PostgresEndpoints} takes, for the reason its MySQL counterpart
     * gives: that driver builds its address out of the resource, because what it dials has to be what the
     * product was given. This one is plumbing for a test that already holds the settings.
     */
    static Connection connect(Map<String, Object> settings) throws SQLException {
        String url = "jdbc:postgresql://" + settings.get("host") + ":" + settings.get("port") + "/"
                + settings.get("database");
        return DriverManager.getConnection(
                url, String.valueOf(settings.get("username")), String.valueOf(settings.get("password")));
    }

    private static PostgreSQLContainer<?> server() {
        if (container == null) {
            DockerGate.require();
            PostgreSQLContainer<?> starting = new PostgreSQLContainer<>(IMAGE)
                    .withCommand("postgres",
                            // withCommand replaces the command wholesale, so the image default this
                            // wrapper otherwise supplies has to be restated here or every write on this
                            // server waits on a real disk flush.
                            "-c", "fsync=off",
                            "-c", "wal_level=logical",
                            "-c", "max_wal_senders=32",
                            // Stopped streams keep their slots, so this ceiling is sized for every slot a
                            // complete pass leaves behind; one complete pass already needs more than 32.
                            "-c", "max_replication_slots=64");
            starting.start();
            container = starting;
        }
        return container;
    }

    /**
     * Creates the database if it is not already there.
     *
     * <p>Two queries rather than one, because PostgreSQL has no {@code CREATE DATABASE IF NOT EXISTS}.
     * Nothing guards the gap between them and nothing needs to: the container is one per JVM and the
     * only caller is synchronized, so there is no second writer to lose a race with. A tolerance for
     * "already exists" was written here first and taken out again - it could not be reached, and
     * untested code for an unreachable case is worse than no code, because the next reader has to work
     * out whether the case is real.
     */
    private static void provision(PostgreSQLContainer<?> server, String database) {
        try (Connection admin = asAdmin(server); Statement statement = admin.createStatement()) {
            try (var existing = statement.executeQuery(
                    "SELECT 1 FROM pg_database WHERE datname = '" + database + "'")) {
                if (existing.next()) {
                    return;
                }
            }
            statement.execute("CREATE DATABASE \"" + database + "\"");
        } catch (SQLException e) {
            throw new IllegalStateException("cannot provision the database " + database, e);
        }
    }

    private static Connection asAdmin(PostgreSQLContainer<?> server) throws SQLException {
        return DriverManager.getConnection(
                server.getJdbcUrl(), server.getUsername(), server.getPassword());
    }
}
