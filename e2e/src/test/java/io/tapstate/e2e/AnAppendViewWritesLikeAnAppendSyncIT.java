package io.tapstate.e2e;

import io.tapstate.core.lifecycle.LifecycleVerb;
import io.tapstate.core.lifecycle.PipelineState;
import io.tapstate.testsupport.DockerGate;
import org.bson.Document;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.sql.Connection;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * A view told to append writes the way a sync target told to append does, change for change.
 *
 * <p>{@code write_mode: append} means on a view what it means on a {@code serve.sync} element: one source
 * table feeds both, a row is updated and then deleted, and every key holds the same documents on both sides.
 * The case does not say what appending should produce - that is the sync target's existing behavior, which
 * the documentation states as measured here - only that the two are one behavior. A view still writing as an
 * upsert, the only way it could write before it was given the setting, leaves the deleted row's key empty
 * where the sync target keeps a document under it.
 *
 * <pre>
 *   mvn -pl e2e -am verify -Dapi.version=1.44 \
 *     -Dtapstate.e2e.connectors-dir=/path/to/connectors \
 *     -Dit.test=AnAppendViewWritesLikeAnAppendSyncIT -Dfailsafe.failIfNoSpecifiedTests=false
 * </pre>
 *
 * <p>Java rather than a declarative example, and the reason is a missing word: the claim compares two
 * targets with each other, and the vocabulary compares one reading with a value written down - which here
 * would mean writing down what appending produces, the one thing this case declines to presume.
 */
class AnAppendViewWritesLikeAnAppendSyncIT {

    private static final String DATABASE = "append_parity_db";
    private static final String TABLE = "orders";
    private static final String VIEW = "order_state";
    private static final String PIPELINE = "append_parity";

    @BeforeAll
    static void requireDockerAndRealConnectors() {
        DockerGate.require();
        RealConnectorGate.require("mysql", "mongodb");
    }

    @Test
    void anUpdateAndADeleteLeaveTheSameDocumentsUnderEveryKeyOnBothSides() throws Exception {
        Map<String, Object> mysql = SharedMySql.settings(DATABASE);
        execute(mysql, "DROP TABLE IF EXISTS " + TABLE,
                "CREATE TABLE " + TABLE + " (id INT PRIMARY KEY, name VARCHAR(64))",
                "INSERT INTO " + TABLE + " (id, name) VALUES (1, 'one'), (2, 'two'), (3, 'three')");
        String storeUri = SharedMongo.replicaSetUrl("append_parity_store");
        String targetUri = SharedMongo.replicaSetUrl("append_parity_target");
        String viewUri = SharedMongo.replicaSetUrl("append_parity_views");
        EndpointAddress target = EndpointAddress.uri(targetUri);
        EndpointAddress views = EndpointAddress.uri(viewUri);

        try (ServerHandle server = InProcessServer.start(storeUri);
                MongoEndpoints mongo = new MongoEndpoints()) {
            ControlPlane control = new ControlPlane(server.baseUrl());
            control.bootstrapAndLogin("e2e", "e2e-password");
            control.registerConnector("mysql", ConnectorJars.bytesFor("mysql"));
            control.registerConnector("mongodb", ConnectorJars.bytesFor("mongodb"));
            Map<String, String> resources = new LinkedHashMap<>();
            resources.put("src_mysql.tap.yml", """
                    version: tapstate/v1
                    kind: source
                    id: src_mysql
                    connector: mysql
                    config: { host: %s, port: %s, database: %s, username: %s, password: %s }
                    mode: cdc
                    tables: [ %s ]
                    """.formatted(mysql.get("host"), mysql.get("port"), mysql.get("database"),
                    mysql.get("username"), mysql.get("password"), TABLE));
            resources.put("tgt_mongo.tap.yml", mongoYaml("tgt_mongo", targetUri));
            resources.put("views.tap.yml", mongoYaml("views", viewUri));
            resources.put(PIPELINE + ".tap.yml", """
                    version: tapstate/v1
                    kind: pipeline
                    id: %s
                    source: src_mysql
                    settings: { read_mode: snapshot_and_cdc }
                    view:
                      id: %s
                      from: %s
                      primary_key: id
                      write_mode: append
                      storage: { warm: { collection: %s } }
                    serve:
                      from: %s
                      sync:
                        - source: tgt_mongo
                          write_mode: append
                    """.formatted(PIPELINE, VIEW, TABLE, VIEW, TABLE));
            control.apply(resources);
            control.discoverSchema("src_mysql", "mysql", mysql);
            control.lifecycle(PIPELINE, LifecycleVerb.START);

            awaitBothSides(mongo, target, views, "the full load on both sides",
                    side -> side.containsKeys("1", "2", "3"));
            execute(mysql, "UPDATE " + TABLE + " SET name = 'uno' WHERE id = 1");
            awaitBothSides(mongo, target, views, "the update on both sides", side -> side.holds("1", "uno"));
            execute(mysql, "DELETE FROM " + TABLE + " WHERE id = 2");
            // A row inserted after the delete, waited for on both sides: changes to one table arrive in order,
            // so once it is there the delete before it has been written too, whatever it wrote.
            execute(mysql, "INSERT INTO " + TABLE + " (id, name) VALUES (4, 'four')");
            awaitBothSides(mongo, target, views, "the row inserted after the delete", side -> side.holds("4", "four"));

            Keyed synced = Keyed.of(mongo.documents(target, TABLE));
            Keyed viewed = Keyed.of(mongo.documents(views, VIEW));
            assertThat(viewed.byKey())
                    .as("the view's documents under each key, against the sync target's")
                    .isEqualTo(synced.byKey());
            assertThat(synced.byKey().get("2"))
                    .as("documents under the deleted row's key: appending writes a delete, it removes nothing")
                    .isNotEmpty();
            assertThat(control.state(PIPELINE))
                    .as("the pipeline after both changes - a write refused on a key would have failed it")
                    .contains(PipelineState.RUNNING);
        }
    }

    /** One side's documents: under each key, every name written there, in order. */
    private record Keyed(Map<String, List<String>> byKey) {

        static Keyed of(List<Document> documents) {
            Map<String, List<String>> byKey = new TreeMap<>();
            for (Document document : documents) {
                byKey.computeIfAbsent(String.valueOf(document.get("id")), key -> new ArrayList<>())
                        .add(String.valueOf(document.get("name")));
            }
            byKey.values().forEach(names -> names.sort(String::compareTo));
            return new Keyed(byKey);
        }

        boolean containsKeys(String... keys) {
            return byKey.keySet().containsAll(List.of(keys));
        }

        boolean holds(String key, String name) {
            return byKey.getOrDefault(key, List.of()).contains(name);
        }
    }

    private interface SideCheck {
        boolean test(Keyed side);
    }

    private static void awaitBothSides(MongoEndpoints mongo, EndpointAddress target, EndpointAddress views,
            String what, SideCheck check) {
        Await.until(what,
                () -> check.test(Keyed.of(mongo.documents(target, TABLE)))
                        && check.test(Keyed.of(mongo.documents(views, VIEW))),
                () -> "sync=" + Keyed.of(mongo.documents(target, TABLE)).byKey()
                        + " view=" + Keyed.of(mongo.documents(views, VIEW)).byKey());
    }

    private static String mongoYaml(String id, String uri) {
        return """
                version: tapstate/v1
                kind: source
                id: %s
                connector: mongodb
                config: { uri: "%s" }
                """.formatted(id, uri);
    }

    private static void execute(Map<String, Object> settings, String... statements) throws Exception {
        try (Connection connection = SharedMySql.connect(settings);
                Statement statement = connection.createStatement()) {
            for (String sql : statements) {
                statement.execute(sql);
            }
        }
    }
}
