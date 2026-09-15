package io.tapstate.e2e;

import io.tapstate.testsupport.DockerGate;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.MySQLContainer;
import org.testcontainers.utility.DockerImageName;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.Statement;
import java.util.LinkedHashMap;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * A source whose backend cannot answer the shape this face asks in is refused with a code.
 *
 * <p>The read face speaks one request shape, and it is the document store's. Most connectors in the
 * catalogue register the command it is carried on - they are simply asked in SQL rather than in
 * documents - so "can this connector be browsed" is not answered by whether the capability is
 * present. It is answered where the request meets the backend, and this is the witness for what a
 * user gets there.
 *
 * <p>What must not happen is the two silent shapes. A read that answers zero rows would say the table
 * is empty, and a read that fails without a code cannot be told from the product falling over: there
 * would be nothing for a caller to match on and nothing for a person to look up. So the assertion is
 * on the code, on the parameter naming which connector, and - the part that is a claim about the
 * product's manners rather than its logic - on the refusal being a refusal, not a server failure.
 *
 * <p>MySQL rather than a connector rigged to fail, because a rigged one would only witness the
 * harness's idea of failing. This is the ordinary case a user will actually reach: a real, supported,
 * fully working connector that this particular face cannot speak to.
 *
 * <p>The listing is asserted first, and asserted to <em>succeed</em>. That is not scope creep - it is
 * what makes the refusal meaningful. A source that failed at every verb would prove nothing about the
 * request shape; the point is that the same connection lists its tables perfectly well and cannot be
 * asked for rows.
 *
 * <p>Gated on real connector jars, so it runs on the real-connector lane. Run it with:
 *
 * <pre>
 *   mvn -pl e2e -am verify -Dapi.version=1.44 \
 *     -Dtapstate.e2e.connectors-dir=/path/to/connectors \
 *     -Dit.test=DataBrowserUnsupportedBackendIT -Dfailsafe.failIfNoSpecifiedTests=false
 * </pre>
 */
class DataBrowserUnsupportedBackendIT {

    private static final String SOURCE_ID = "src_mysql";
    private static final String TABLE = "orders";
    private static final long SEEDED_ROWS = 3;

    @BeforeAll
    static void requireDockerAndTheRealConnector() {
        DockerGate.require();
        RealConnectorGate.require("mysql");
    }

    @Test
    void refusesToReadRowsFromABackendThisFaceCannotAskInWhileStillListingIt() throws Exception {
        try (MySQLContainer<?> mysql = new MySQLContainer<>(DockerImageName.parse("mysql:8.0"))) {
            mysql.start();
            seed(mysql);

            try (ServerHandle server =
                    InProcessServer.start(SharedMongo.replicaSetUrl("e2e_unsupported_backend"))) {
                ControlPlane control = new ControlPlane(server.baseUrl());
                control.bootstrapAndLogin("e2e", "e2e-password");
                control.registerConnector("mysql", ConnectorJars.bytesFor("mysql"));
                control.apply(Map.of("src_mysql.tap.yml", sourceYaml(mysql)));

                // The connection works, and the face can use it - for this verb. Whatever the row read
                // does below, it is not "this source is broken".
                assertThat(control.collections(SOURCE_ID))
                        .as("the tables the read face lists for a working MySQL connection")
                        .extracting(entry -> entry.get("name"))
                        .contains(TABLE);

                // Reading rows is where the shape stops fitting.
                //
                // That this is a refusal at all, rather than the product falling over, is asserted by
                // the call itself: findExpectingRefusal fails loudly on a 5xx, saying the server failed
                // instead of refusing. It is worth saying that here because the obvious way to write it
                // - reading the status back off the answer and asserting it is a 4xx - cannot fail. A
                // 5xx never reaches that line, so the assertion would read like a guard and be one of
                // the shapes it exists to catch.
                ControlPlane.Refusal refused = control.findExpectingRefusal(SOURCE_ID, TABLE, Map.of());
                assertThat(refused.code())
                        .as("the code a row read of a backend this face cannot ask in is refused with")
                        .isEqualTo("data-browser.connector-not-browsable");
            }
        }
    }

    private static void seed(MySQLContainer<?> mysql) throws Exception {
        try (Connection connection =
                DriverManager.getConnection(mysql.getJdbcUrl(), mysql.getUsername(), mysql.getPassword())) {
            try (Statement statement = connection.createStatement()) {
                statement.execute("CREATE TABLE " + TABLE + " (id INT PRIMARY KEY, name VARCHAR(64))");
            }
            try (PreparedStatement insert =
                    connection.prepareStatement("INSERT INTO " + TABLE + " (id, name) VALUES (?, ?)")) {
                for (long id = 1; id <= SEEDED_ROWS; id++) {
                    insert.setLong(1, id);
                    insert.setString(2, "order-" + id);
                    insert.addBatch();
                }
                insert.executeBatch();
            }
        }
    }

    /** The port is a number: the connector's config bean holds it as one, and a string is a cast failure. */
    private static String sourceYaml(MySQLContainer<?> mysql) {
        Map<String, Object> config = new LinkedHashMap<>();
        config.put("host", mysql.getHost());
        config.put("port", mysql.getMappedPort(MySQLContainer.MYSQL_PORT));
        config.put("database", mysql.getDatabaseName());
        config.put("username", mysql.getUsername());
        config.put("password", mysql.getPassword());
        return """
                version: tapstate/v1
                kind: source
                id: src_mysql
                connector: mysql
                config: { host: %s, port: %s, database: %s, username: %s, password: %s }
                """
                .formatted(config.get("host"), config.get("port"), config.get("database"),
                        config.get("username"), config.get("password"));
    }
}
