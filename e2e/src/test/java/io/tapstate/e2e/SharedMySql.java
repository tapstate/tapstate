package io.tapstate.e2e;

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
    private static final DockerImageName IMAGE = image();
    private static MySQLContainer<?> container;
    private SharedMySql() { }

    @FunctionalInterface interface Connections { Connection open(String uri, String user, String password) throws SQLException; }
    @FunctionalInterface private interface SqlBody { void accept(Statement statement) throws SQLException; }
    @FunctionalInterface private interface SqlClose { void close() throws SQLException; }

    private static DockerImageName image() {
        return BenchmarkSharedSupportSourceWork.entered(BenchmarkReturnWorkBudget.Activity.SHARED_SUPPORT,
                BenchmarkSharedSupportSourceWork.Kind.MY_IMAGE, () -> BenchmarkReturnWorkBudget.opaqueRequest(
                        BenchmarkReturnWorkBudget.current(), BenchmarkReturnWorkBudget.Activity.SHARED_SUPPORT,
                        () -> DockerImageName.parse("mysql:8.0")));
    }

    /**
     * The settings addressing a database of the caller's own on the shared server, spelled the way a
     * resource spells them - host, port, database, username, password, no one of which is the address.
     */
    static synchronized Map<String, Object> settings(String database) {
        return BenchmarkSharedSupportSourceWork.entered(BenchmarkReturnWorkBudget.Activity.SHARED_SUPPORT,
                BenchmarkSharedSupportSourceWork.Kind.MY_SETTINGS, () -> {
                    BenchmarkSharedSupportSourceWork.scalar(database, BenchmarkSharedSupportSourceWork.TEXT);
                    MySQLContainer<?> server = server();
                    provision(server, database, DriverManager::getConnection);
                    var scope = BenchmarkReturnWorkBudget.current();
                    Object host = BenchmarkReturnWorkBudget.opaqueRequest(scope, BenchmarkReturnWorkBudget.Activity.SHARED_SUPPORT, server::getHost);
                    BenchmarkSharedSupportSourceWork.local(host);
                    Object port = BenchmarkReturnWorkBudget.opaqueRequest(scope, BenchmarkReturnWorkBudget.Activity.SHARED_SUPPORT,
                            () -> server.getMappedPort(MySQLContainer.MYSQL_PORT));
                    Object username = BenchmarkReturnWorkBudget.opaqueRequest(scope, BenchmarkReturnWorkBudget.Activity.SHARED_SUPPORT, server::getUsername);
                    BenchmarkSharedSupportSourceWork.local(username);
                    Object password = BenchmarkReturnWorkBudget.opaqueRequest(scope, BenchmarkReturnWorkBudget.Activity.SHARED_SUPPORT, server::getPassword);
                    BenchmarkSharedSupportSourceWork.local(password);
                    BenchmarkSharedSupportSourceWork.local(port);
                    Map<String, Object> settings = new LinkedHashMap<>();
                    settings.put("host", host); settings.put("port", port); settings.put("database", database);
                    settings.put("username", username); settings.put("password", password);
                    return settings;
                });
    }

    /**
     * A connection to the database those settings address, for a test laying down a fixture of its own.
     *
     * <p>Deliberately not the path {@link MySqlEndpoints} takes: that driver builds its address out of the
     * resource, because what it dials has to be what the product was given. This one is plumbing for a
     * test that already holds the settings.
     */
    static Connection connect(Map<String, Object> settings) throws SQLException {
        return connect(settings, DriverManager::getConnection);
    }

    static Connection connect(Map<String, Object> settings, Connections connections) throws SQLException {
        if (BenchmarkReturnWorkBudget.current() == null) {
            String url = "jdbc:mysql://" + settings.get("host") + ":" + settings.get("port") + "/"
                    + settings.get("database") + "?sslMode=DISABLED&allowPublicKeyRetrieval=true";
            return connections.open(url, String.valueOf(settings.get("username")), String.valueOf(settings.get("password")));
        }
        return BenchmarkSharedSupportSourceWork.entered(BenchmarkReturnWorkBudget.Activity.SHARED_SUPPORT,
                BenchmarkSharedSupportSourceWork.Kind.MY_CONNECT, () -> {
                    Object host = settings.get("host"); BenchmarkSharedSupportSourceWork.local(host);
                    String hostText = String.valueOf(host);
                    Object port = settings.get("port"); BenchmarkSharedSupportSourceWork.local(port);
                    String portText = String.valueOf(port);
                    Object database = settings.get("database"); BenchmarkSharedSupportSourceWork.local(database);
                    String url = "jdbc:mysql://" + hostText + ":" + portText + "/" + String.valueOf(database)
                            + "?sslMode=DISABLED&allowPublicKeyRetrieval=true";
                    Object username = settings.get("username"); BenchmarkSharedSupportSourceWork.local(username);
                    String user = String.valueOf(username);
                    Object password = settings.get("password"); BenchmarkSharedSupportSourceWork.local(password);
                    String secret = String.valueOf(password);
                    return BenchmarkReturnWorkBudget.external(BenchmarkReturnWorkBudget.current(),
                            BenchmarkReturnWorkBudget.Activity.SHARED_SUPPORT, () -> connections.open(url, user, secret));
                });
    }

    private static MySQLContainer<?> server() {
        return BenchmarkSharedSupportSourceWork.entered(BenchmarkReturnWorkBudget.Activity.SHARED_SUPPORT,
                BenchmarkSharedSupportSourceWork.Kind.MY_SERVER, () -> {
                    if (container == null) {
                        BenchmarkSharedSupportSourceWork.dockerGate();
                        var scope = BenchmarkReturnWorkBudget.current();
                        MySQLContainer<?> starting = BenchmarkReturnWorkBudget.opaqueRequest(scope,
                                BenchmarkReturnWorkBudget.Activity.SHARED_SUPPORT, () -> new MySQLContainer<>(IMAGE));
                        BenchmarkReturnWorkBudget.opaqueRequest(scope, BenchmarkReturnWorkBudget.Activity.SHARED_SUPPORT,
                                () -> { starting.start(); return null; });
                        grantReplication(starting);
                        container = starting;
                    }
                    return container;
                });
    }

    /** The SQL order and SQLException wrapper are the original fixture contract. */
    static void provision(MySQLContainer<?> server, String database, Connections connections) {
        BenchmarkSharedSupportSourceWork.entered(BenchmarkReturnWorkBudget.Activity.SHARED_SUPPORT,
                BenchmarkSharedSupportSourceWork.Kind.MY_PROVISION, () -> {
                    BenchmarkSharedSupportSourceWork.scalar(database, BenchmarkSharedSupportSourceWork.TEXT);
                    try {
                        rootStatements(server, connections, statement -> {
                            command(statement, "CREATE DATABASE IF NOT EXISTS `" + database + "`");
                            String username = BenchmarkReturnWorkBudget.opaqueRequest(BenchmarkReturnWorkBudget.current(),
                                    BenchmarkReturnWorkBudget.Activity.SHARED_SUPPORT, server::getUsername);
                            BenchmarkSharedSupportSourceWork.local(username);
                            command(statement, "GRANT ALL PRIVILEGES ON `" + database + "`.* TO '" + username + "'@'%'");
                            command(statement, "FLUSH PRIVILEGES");
                        });
                    } catch (SQLException e) { throw new IllegalStateException("cannot provision the database " + database, e); }
                    return null;
                });
    }

    /** Binlog CDC needs replication privileges the default test user lacks; they are granted as root. */
    static void grantReplication(MySQLContainer<?> server) {
        grantReplication(server, DriverManager::getConnection);
    }

    static void grantReplication(MySQLContainer<?> server, Connections connections) {
        BenchmarkSharedSupportSourceWork.entered(BenchmarkReturnWorkBudget.Activity.SHARED_SUPPORT,
                BenchmarkSharedSupportSourceWork.Kind.MY_GRANT, () -> {
                    try {
                        rootStatements(server, connections, statement -> {
                            String username = BenchmarkReturnWorkBudget.opaqueRequest(BenchmarkReturnWorkBudget.current(),
                                    BenchmarkReturnWorkBudget.Activity.SHARED_SUPPORT, server::getUsername);
                            BenchmarkSharedSupportSourceWork.local(username);
                            command(statement, "GRANT REPLICATION SLAVE, REPLICATION CLIENT, RELOAD, SELECT ON *.* TO '" + username + "'@'%'");
                            command(statement, "FLUSH PRIVILEGES");
                        });
                    } catch (SQLException e) { throw new IllegalStateException("cannot grant replication privileges", e); }
                    return null;
                });
    }

    private static Connection asRoot(MySQLContainer<?> server, Connections connections) throws SQLException {
        return BenchmarkSharedSupportSourceWork.entered(BenchmarkReturnWorkBudget.Activity.SHARED_SUPPORT,
                BenchmarkSharedSupportSourceWork.Kind.MY_ROOT_CONNECT, () -> {
                    var scope = BenchmarkReturnWorkBudget.current();
                    String uri = BenchmarkReturnWorkBudget.opaqueRequest(scope, BenchmarkReturnWorkBudget.Activity.SHARED_SUPPORT, server::getJdbcUrl);
                    BenchmarkSharedSupportSourceWork.local(uri);
                    String password = BenchmarkReturnWorkBudget.opaqueRequest(scope, BenchmarkReturnWorkBudget.Activity.SHARED_SUPPORT, server::getPassword);
                    BenchmarkSharedSupportSourceWork.local(password);
                    return BenchmarkReturnWorkBudget.external(scope, BenchmarkReturnWorkBudget.Activity.SHARED_SUPPORT,
                            () -> connections.open(uri, "root", password));
                });
    }

    private static void command(Statement statement, String sql) throws SQLException {
        BenchmarkSharedSupportSourceWork.entered(BenchmarkReturnWorkBudget.Activity.SHARED_SUPPORT,
                BenchmarkSharedSupportSourceWork.Kind.MY_COMMAND, () -> {
                    // This is derived concatenation of two prior-bounded TEXT scalars and fixed SQL.
                    BenchmarkSharedSupportSourceWork.scalar(sql, BenchmarkSharedSupportSourceWork.MY_COMMAND_TEXT);
                    BenchmarkReturnWorkBudget.external(BenchmarkReturnWorkBudget.current(), BenchmarkReturnWorkBudget.Activity.SHARED_SUPPORT,
                            () -> statement.execute(sql));
                    return null;
                });
    }

    /** Reproduce TWR close order, including the exact primary and all original close suppression. */
    private static void rootStatements(MySQLContainer<?> server, Connections connections, SqlBody body) throws SQLException {
        if (BenchmarkReturnWorkBudget.current() == null) {
            // Keep Java's original TWR semantics, including pathological self-suppression.
            try (Connection root = asRoot(server, connections); Statement statement = root.createStatement()) {
                body.accept(statement);
            }
            return;
        }
        BenchmarkSharedSupportSourceWork.entered(BenchmarkReturnWorkBudget.Activity.SHARED_SUPPORT,
                BenchmarkSharedSupportSourceWork.Kind.MY_STATEMENTS, () -> {
                    Connection root = null; Statement statement = null; Throwable primary = null;
                    try {
                        root = asRoot(server, connections);
                        Connection actual = root;
                        statement = BenchmarkReturnWorkBudget.external(BenchmarkReturnWorkBudget.current(),
                                BenchmarkReturnWorkBudget.Activity.SHARED_SUPPORT, actual::createStatement);
                        body.accept(statement);
                    } catch (SQLException | RuntimeException | Error failure) { primary = failure; throw failure; }
                    finally {
                        Throwable ending = primary;
                        if (statement != null) { Statement actual = statement; ending = closeSql(actual::close, ending); }
                        if (root != null) { Connection actual = root; ending = closeSql(actual::close, ending); }
                        if (ending != null && ending != primary) {
                            if (ending instanceof SQLException failure) throw failure;
                            if (ending instanceof RuntimeException failure) throw failure;
                            if (ending instanceof Error failure) throw failure;
                            throw new AssertionError("owned SQL close returned an unsupported failure", ending);
                        }
                    }
                    return null;
                });
    }

    private static Throwable closeSql(SqlClose close, Throwable primary) {
        try {
            BenchmarkSharedSupportSourceWork.entered(BenchmarkReturnWorkBudget.Activity.CLEANUP,
                    BenchmarkSharedSupportSourceWork.Kind.MY_CLOSE, () -> {
                        BenchmarkReturnWorkBudget.external(BenchmarkReturnWorkBudget.current(), BenchmarkReturnWorkBudget.Activity.CLEANUP,
                                () -> { close.close(); return null; }); return null;
                    });
        } catch (SQLException | RuntimeException | Error failure) {
            if (primary == null) return failure;
            try { primary.addSuppressed(failure); }
            catch (IllegalArgumentException selfSuppression) { return selfSuppression; }
        }
        return primary;
    }
}
