package io.tapstate.e2e;

import io.tapstate.core.lifecycle.LifecycleVerb;
import io.tapstate.testsupport.DockerGate;

import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.sql.Connection;
import java.sql.Statement;
import java.time.Duration;
import java.util.LinkedHashMap;
import java.util.Map;

/** Proves that each pipeline consuming one physical source connection receives its snapshot. */
class BothPipelinesOnOneSourceConnectionDeliverIT {

    private static final String TABLE = "orders";
    private static final long SEEDED_ROWS = 5;

    @BeforeAll
    static void requireDockerAndRealConnectors() {
        DockerGate.require();
        RealConnectorGate.require("mysql", "mongodb");
    }

    @Test
    void bothPipelinesOnOneSourceConnectionDeliverTheirSnapshot() throws Exception {
        Map<String, Object> mysql = SharedMySql.settings("two_pipeline_source");
        seed(mysql);

        String firstTargetUri = SharedMongo.replicaSetUrl("two_pipeline_first_target");
        String secondTargetUri = SharedMongo.replicaSetUrl("two_pipeline_second_target");
        try (ServerHandle server = Tiers.IN_PROCESS.launch(
                        SharedMongo.replicaSetUrl("two_pipeline_store"));
                MongoEndpoints mongo = new MongoEndpoints()) {
            ControlPlane control = new ControlPlane(server.baseUrl());
            control.bootstrapAndLogin("e2e", "e2e-password");
            control.registerConnector("mysql", ConnectorJars.bytesFor("mysql"));
            control.registerConnector("mongodb", ConnectorJars.bytesFor("mongodb"));

            control.apply(workspace(mysql, firstTargetUri, secondTargetUri));
            control.discoverSchema("first_source", "mysql", mysql);
            control.discoverSchema("second_source", "mysql", mysql);

            control.lifecycle("first_pipeline", LifecycleVerb.START);
            control.lifecycle("second_pipeline", LifecycleVerb.START);

            EndpointAddress firstTarget = EndpointAddress.uri(firstTargetUri);
            EndpointAddress secondTarget = EndpointAddress.uri(secondTargetUri);
            Await.until(
                    "the first pipeline to deliver all source rows",
                    () -> count(mongo, firstTarget) == SEEDED_ROWS,
                    () -> reading(control, mongo, firstTarget, secondTarget));
            Await.until(
                    "the second pipeline on the same source connection to deliver all source rows",
                    Duration.ofSeconds(30),
                    () -> count(mongo, secondTarget) == SEEDED_ROWS,
                    () -> reading(control, mongo, firstTarget, secondTarget));
        }
    }

    private static Map<String, String> workspace(
            Map<String, Object> mysql, String firstTargetUri, String secondTargetUri) {
        Map<String, String> resources = new LinkedHashMap<>();
        resources.put("first_source.tap.yml", source("first_source", mysql));
        resources.put("second_source.tap.yml", source("second_source", mysql));
        resources.put("first_target.tap.yml", target("first_target", firstTargetUri));
        resources.put("second_target.tap.yml", target("second_target", secondTargetUri));
        resources.put("first_pipeline.tap.yml", pipeline("first_pipeline", "first_source", "first_target"));
        resources.put("second_pipeline.tap.yml", pipeline("second_pipeline", "second_source", "second_target"));
        return resources;
    }

    private static String source(String id, Map<String, Object> mysql) {
        return """
                version: tapstate/v1
                kind: source
                id: %s
                connector: mysql
                config: { host: %s, port: %s, database: %s, username: %s, password: %s }
                mode: cdc
                tables: [ orders ]
                """.formatted(id, mysql.get("host"), mysql.get("port"), mysql.get("database"),
                        mysql.get("username"), mysql.get("password"));
    }

    private static String target(String id, String uri) {
        return """
                version: tapstate/v1
                kind: source
                id: %s
                connector: mongodb
                config: { uri: "%s" }
                """.formatted(id, uri);
    }

    private static String pipeline(String id, String source, String target) {
        return """
                version: tapstate/v1
                kind: pipeline
                id: %s
                source: %s
                settings: { read_mode: snapshot_and_cdc }
                serve:
                  from: orders
                  sync:
                    - source: %s
                """.formatted(id, source, target);
    }

    private static void seed(Map<String, Object> mysql) throws Exception {
        try (Connection connection = SharedMySql.connect(mysql); Statement statement = connection.createStatement()) {
            statement.execute("CREATE TABLE orders (id INT PRIMARY KEY, name VARCHAR(64))");
            statement.execute("INSERT INTO orders VALUES "
                    + "(1, 'one'), (2, 'two'), (3, 'three'), (4, 'four'), (5, 'five')");
        }
    }

    private static long count(MongoEndpoints mongo, EndpointAddress target) {
        return mongo.documents(target, TABLE).size();
    }

    private static String reading(
            ControlPlane control,
            MongoEndpoints mongo,
            EndpointAddress firstTarget,
            EndpointAddress secondTarget) {
        return "first target rows=" + count(mongo, firstTarget)
                + ", second target rows=" + count(mongo, secondTarget)
                + ", first state=" + control.state("first_pipeline")
                + ", second state=" + control.state("second_pipeline")
                + ", first logs=" + control.logs("first_pipeline")
                + ", second logs=" + control.logs("second_pipeline");
    }
}
