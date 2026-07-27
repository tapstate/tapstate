package io.tapstate.e2e;

import io.tapstate.testsupport.DockerGate;
import org.testcontainers.containers.MySQLContainer;
import org.testcontainers.utility.DockerImageName;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.LinkedHashMap;
import java.util.Map;

/**
 * One MySQL server for every specification in the JVM, the arrangement {@link SharedMongo} already makes
 * and for the same reason: a container per test class costs a start-up each time - MySQL's is the slowest
 * here - and buys nothing, because runs stay independent by taking a database of their own rather than a
 * daemon of their own. Ryuk reaps the container when the JVM exits, so there is no stop to forget.
 *
 * <p>One difference from Mongo had to be settled by running it rather than by argument: the binary log a
 * CDC read tails belongs to the server, not to a database, so two runs sharing a server share a log where
 * on Mongo they would share nothing. Isolation survives because the connector is told which database it
 * reads and filters the log to it - the two tiers of the CDC witness now run against one server, each
 * seeing only its own rows, where they used to get a container each.
 *
 * <p>Replication privileges are granted once, when the server starts. They are server-wide and the
 * default test user lacks them, so granting them per database would be both wrong and repeated; a CDC
 * read cannot open the binary log without them.
 */
final class SharedMySql {

    private static final DockerImageName IMAGE = DockerImageName.parse("mysql:8.0");

    private static MySQLContainer<?> container;

    private SharedMySql() {
    }

    /**
     * The settings addressing a database of the caller's own on the shared server, spelled the way a
     * resource spells them - host, port, database, username, password, no one of which is the address.
     */
    static synchronized Map<String, Object> settings(String database) {
        MySQLContainer<?> server = server();
        provision(server, database);
        Map<String, Object> settings = new LinkedHashMap<>();
        settings.put("host", server.getHost());
        settings.put("port", server.getMappedPort(MySQLContainer.MYSQL_PORT));
        settings.put("database", database);
        settings.put("username", server.getUsername());
        settings.put("password", server.getPassword());
        return settings;
    }

    /**
     * A connection to the database those settings address, for a test laying down a fixture of its own.
     *
     * <p>Deliberately not the path {@link MySqlEndpoints} takes: that driver builds its address out of the
     * resource, because what it dials has to be what the product was given. This one is plumbing for a
     * test that already holds the settings.
     */
    static Connection connect(Map<String, Object> settings) throws SQLException {
        String url = "jdbc:mysql://" + settings.get("host") + ":" + settings.get("port") + "/"
                + settings.get("database") + "?sslMode=DISABLED&allowPublicKeyRetrieval=true";
        return DriverManager.getConnection(
                url, String.valueOf(settings.get("username")), String.valueOf(settings.get("password")));
    }

    private static MySQLContainer<?> server() {
        if (container == null) {
            DockerGate.require();
            MySQLContainer<?> starting = new MySQLContainer<>(IMAGE);
            starting.start();
            grantReplication(starting);
            container = starting;
        }
        return container;
    }

    /** Creates the database and lets the test user work in it; both are idempotent, so a re-ask is free. */
    private static void provision(MySQLContainer<?> server, String database) {
        try (Connection root = asRoot(server); Statement statement = root.createStatement()) {
            statement.execute("CREATE DATABASE IF NOT EXISTS `" + database + "`");
            statement.execute(
                    "GRANT ALL PRIVILEGES ON `" + database + "`.* TO '" + server.getUsername() + "'@'%'");
            statement.execute("FLUSH PRIVILEGES");
        } catch (SQLException e) {
            throw new IllegalStateException("cannot provision the database " + database, e);
        }
    }

    /** Binlog CDC needs replication privileges the default test user lacks; they are granted as root. */
    private static void grantReplication(MySQLContainer<?> server) {
        try (Connection root = asRoot(server); Statement statement = root.createStatement()) {
            statement.execute("GRANT REPLICATION SLAVE, REPLICATION CLIENT, RELOAD, SELECT ON *.* TO '"
                    + server.getUsername() + "'@'%'");
            statement.execute("FLUSH PRIVILEGES");
        } catch (SQLException e) {
            throw new IllegalStateException("cannot grant replication privileges", e);
        }
    }

    private static Connection asRoot(MySQLContainer<?> server) throws SQLException {
        return DriverManager.getConnection(server.getJdbcUrl(), "root", server.getPassword());
    }
}
