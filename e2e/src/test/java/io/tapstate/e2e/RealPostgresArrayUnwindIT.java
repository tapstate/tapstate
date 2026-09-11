package io.tapstate.e2e;

import io.tapstate.core.lifecycle.LifecycleVerb;
import io.tapstate.testsupport.DockerGate;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

import java.sql.Connection;
import java.sql.Statement;
import java.util.LinkedHashMap;
import java.util.Locale;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * A native PostgreSQL array reaches unwind through the real connector's array codec. The declarative
 * seed vocabulary only accepts scalar columns, so this witness creates its SQL array directly.
 */
class RealPostgresArrayUnwindIT {

    @BeforeAll
    static void requireConnectors() {
        DockerGate.require();
        RealConnectorGate.require("postgres", "mongodb");
    }

    @ParameterizedTest
    @EnumSource(Tiers.class)
    void nativeArraysExpandAndEmptyArraysDisappearBeforeParentDeletionRemovesTheRows(Tiers tier) throws Exception {
        String suffix = tier.name().toLowerCase(Locale.ROOT);
        // Connector state is namespaced by pipeline/node in the shared operator-state database.
        // A separate application database alone does not isolate a second tier's replication slot.
        String pipelineId = "array_unwind_" + suffix;
        String sourceId = "array_source_" + suffix;
        String targetId = "array_target_" + suffix;
        Map<String, Object> postgres = SharedPostgres.settings("unwind_arrays_" + suffix);
        sql(postgres, "CREATE TABLE orders (id BIGINT PRIMARY KEY, customer TEXT, items TEXT[])",
                "ALTER TABLE orders REPLICA IDENTITY FULL",
                "INSERT INTO orders VALUES (1, 'ada', ARRAY['a','b']), (2, 'lin', ARRAY[]::TEXT[])");
        // Confirm the fixture exercises the JDBC carrier used by the connector's registered codec.
        try (Connection connection = SharedPostgres.connect(postgres);
                Statement statement = connection.createStatement();
                var rows = statement.executeQuery("SELECT items FROM orders WHERE id = 1")) {
            assertThat(rows.next()).isTrue();
            assertThat(rows.getObject(1).getClass().getName()).isEqualTo("org.postgresql.jdbc.PgArray");
        }
        String targetUri = SharedMongo.replicaSetUrl("unwind_arrays_target_" + suffix);
        EndpointAddress target = EndpointAddress.uri(targetUri);
        try (ServerHandle server = tier.launch(SharedMongo.replicaSetUrl("unwind_arrays_store_" + suffix));
                MongoEndpoints mongo = new MongoEndpoints()) {
            ControlPlane control = new ControlPlane(server.baseUrl());
            control.bootstrapAndLogin("e2e", "e2e-password");
            control.registerConnector("postgres", ConnectorJars.bytesFor("postgres"));
            control.registerConnector("mongodb", ConnectorJars.bytesFor("mongodb"));
            Map<String, Object> config = new LinkedHashMap<>(postgres);
            config.put("user", config.remove("username"));
            config.put("schema", "public");
            Map<String, String> resources = new LinkedHashMap<>();
            resources.put("source.tap.yml", """
                    version: tapstate/v1
                    kind: source
                    id: %s
                    connector: postgres
                    config: { host: %s, port: %s, database: %s, schema: public, user: %s, password: %s }
                    mode: cdc
                    tables: [orders]
                    """.formatted(sourceId, config.get("host"), config.get("port"), config.get("database"),
                            config.get("user"), config.get("password")));
            resources.put("target.tap.yml", """
                    version: tapstate/v1
                    kind: source
                    id: %s
                    connector: mongodb
                    config: { uri: "%s" }
                    """.formatted(targetId, targetUri));
            control.apply(resources);
            control.discoverSchema(sourceId, "postgres", config);
            resources.put("pipeline.tap.yml", """
                    version: tapstate/v1
                    kind: pipeline
                    id: %s
                    source: %s
                    settings: { read_mode: snapshot_and_cdc }
                    transforms:
                      - { id: expanded, from: [orders], type: unwind, path: items, include_array_index: item_index }
                    serve:
                      from: expanded
                      sync:
                        - source: %s
                    """.formatted(pipelineId, sourceId, targetId));
            control.apply(resources);
            control.lifecycle(pipelineId, LifecycleVerb.START);
            Await.until("two rows from the nonempty native array",
                    () -> mongo.count(target, "orders") == 2,
                    () -> control.logs(pipelineId));
            var expanded = mongo.documents(target, "orders");
            assertThat(expanded).allSatisfy(row -> {
                assertThat(((Number) row.get("id")).longValue()).isEqualTo(1L);
                assertThat(row.get("customer")).isEqualTo("ada");
            });
            assertThat(expanded.stream().map(row ->
                    ((Number) row.get("item_index")).longValue() + ":" + row.get("items")).toList())
                    .containsExactlyInAnyOrder("0:a", "1:b");

            sql(postgres, "DELETE FROM orders WHERE id = 1");
            Await.until("the expanded rows disappear after deleting their parent",
                    () -> mongo.count(target, "orders") == 0,
                    () -> control.logs(pipelineId));
            assertThat(mongo.documents(target, "orders")).isEmpty();
            assertThat(control.errorCount(pipelineId)).contains(0L);
        }
    }

    private static void sql(Map<String, Object> settings, String... statements) throws Exception {
        try (Connection connection = SharedPostgres.connect(settings);
                Statement statement = connection.createStatement()) {
            for (String sql : statements) {
                statement.execute(sql);
            }
        }
    }
}
