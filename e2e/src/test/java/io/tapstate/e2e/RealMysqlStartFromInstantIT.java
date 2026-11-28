package io.tapstate.e2e;

import static org.assertj.core.api.Assertions.assertThat;

import io.tapstate.core.lifecycle.LifecycleVerb;
import io.tapstate.testsupport.DockerGate;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;

class RealMysqlStartFromInstantIT {

    @Test
    void directCdcOnlyDeliversChangesAtOrAfterTheStartInstant() throws SQLException {
        DockerGate.require();
        RealConnectorGate.require("mysql", "mongodb");
        var mysql = SharedMySql.settings("start_from_instant");
        Instant cutoff;
        try (var connection = SharedMySql.connect(mysql); var statement = connection.createStatement()) {
            statement.executeUpdate("CREATE TABLE orders (id INT PRIMARY KEY)");
            statement.executeUpdate("INSERT INTO orders VALUES (101)");
            cutoff = mysqlTime(statement).plusSeconds(1);
            Await.until("MySQL's clock to pass the start instant", () -> mysqlTime(statement).isAfter(cutoff),
                    () -> "clock=" + mysqlTime(statement) + ", cutoff=" + cutoff);
            statement.executeUpdate("INSERT INTO orders VALUES (102)");
        }

        String targetUri = SharedMongo.replicaSetUrl("start_from_instant_target");
        var target = EndpointAddress.uri(targetUri);
        try (var storage = PreparedNativeStore.create(); var server = Tiers.IN_PROCESS.launch(storage);
                var mongo = new MongoEndpoints()) {
            var control = new ControlPlane(server.baseUrl());
            storage.loginAndAwaitBusiness(control);
            control.registerConnector("mysql", ConnectorJars.bytesFor("mysql"));
            control.registerConnector("mongodb", ConnectorJars.bytesFor("mongodb"));
            control.apply(Map.of("source.tap.yml", """
                    version: tapstate/v1
                    kind: source
                    id: mysql_source
                    connector: mysql
                    config: { host: %s, port: %s, database: %s, username: %s, password: %s }
                    mode: cdc
                    tables: [orders]
                    srs: {enabled: false}
                    """.formatted(mysql.get("host"), mysql.get("port"), mysql.get("database"),
                    mysql.get("username"), mysql.get("password")), "target.tap.yml", """
                    version: tapstate/v1
                    kind: source
                    id: mongo_target
                    connector: mongodb
                    config: { uri: "%s" }
                    """.formatted(targetUri), "pipeline.tap.yml", """
                    version: tapstate/v1
                    kind: pipeline
                    id: start_from_instant
                    source: mysql_source
                    settings: { read_mode: cdc_only, start_from: "%s" }
                    serve:
                      from: orders
                      sync:
                        - source: mongo_target
                    """.formatted(cutoff)));
            control.discoverSchema("mysql_source", "mysql", mysql);
            control.lifecycle("start_from_instant", LifecycleVerb.START);

            Await.until("the row written after the start instant to arrive in MongoDB",
                    () -> targetIds(mongo, target).contains(102),
                    () -> "rows=" + targetIds(mongo, target) + ", logs=" + control.logs("start_from_instant"));
            assertThat(targetIds(mongo, target)).as("MySQL CDC rows delivered from %s", cutoff)
                    .containsExactly(102);
        }
    }

    private static Instant mysqlTime(Statement statement) {
        try (var result = statement.executeQuery("SELECT UNIX_TIMESTAMP()")) {
            result.next();
            return Instant.ofEpochSecond(result.getLong(1));
        } catch (SQLException e) {
            throw new IllegalStateException("Cannot read the MySQL server clock", e);
        }
    }

    private static List<Integer> targetIds(MongoEndpoints mongo, EndpointAddress target) {
        return mongo.documents(target, "orders").stream()
                .map(row -> ((Number) row.get("id")).intValue()).sorted().toList();
    }
}
