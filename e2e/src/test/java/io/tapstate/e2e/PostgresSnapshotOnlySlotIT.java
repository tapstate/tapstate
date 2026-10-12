package io.tapstate.e2e;

import io.tapstate.core.lifecycle.LifecycleVerb;
import io.tapstate.core.lifecycle.PipelineState;
import io.tapstate.testsupport.DockerGate;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.sql.Connection;
import java.sql.Statement;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.SoftAssertions.assertSoftly;

/** A PostgreSQL load with no change tail must not retain a logical replication slot. */
class PostgresSnapshotOnlySlotIT {

    private static final String PIPELINE = "snapshot_only_slot";

    @BeforeAll
    static void requireDockerAndConnectors() {
        DockerGate.require();
        RealConnectorGate.require("postgres", "mongodb");
    }

    @Test
    void aSnapshotOnlyLoadAndItsClearingStopLeaveNoReplicationSlot() throws Exception {
        Map<String, Object> postgres = SharedPostgres.settings("snapshot_only_slot_source_632");
        try (Connection connection = SharedPostgres.connect(postgres);
                Statement statement = connection.createStatement()) {
            statement.execute("CREATE TABLE items (id INT PRIMARY KEY, value TEXT)");
            statement.execute("INSERT INTO items VALUES (1, 'snapshot row')");
        }

        String targetUri = SharedMongo.replicaSetUrl("snapshot_only_slot_target_632");
        try (ServerHandle server = Tiers.IN_PROCESS.launch(
                SharedMongo.replicaSetUrl("snapshot_only_slot_store_632"), "snapshot_only_slot_state_632");
                MongoEndpoints mongo = new MongoEndpoints()) {
            ControlPlane control = new ControlPlane(server.baseUrl());
            control.bootstrapAndLogin("e2e", "e2e-password");
            control.registerConnector("postgres", ConnectorJars.bytesFor("postgres"));
            control.registerConnector("mongodb", ConnectorJars.bytesFor("mongodb"));

            Map<String, String> resources = new LinkedHashMap<>();
            resources.put("source.tap.yml", """
                    version: tapstate/v1
                    kind: source
                    id: src_pg
                    connector: postgres
                    config: { host: %s, port: %s, database: %s, schema: public, user: %s, password: %s }
                    mode: cdc
                    tables: [ items ]
                    """.formatted(postgres.get("host"), postgres.get("port"), postgres.get("database"),
                    postgres.get("username"), postgres.get("password")));
            resources.put("target.tap.yml", """
                    version: tapstate/v1
                    kind: source
                    id: tgt_mongo
                    connector: mongodb
                    config: { uri: "%s" }
                    """.formatted(targetUri));
            resources.put("pipeline.tap.yml", """
                    version: tapstate/v1
                    kind: pipeline
                    id: %s
                    source: src_pg
                    settings: { read_mode: snapshot_only }
                    transforms:
                      - { id: loaded_items, from: items, type: filter, expr: "true" }
                    serve:
                      from: loaded_items
                      sync:
                        - source: tgt_mongo
                    """.formatted(PIPELINE));
            control.apply(resources);
            Map<String, Object> discovery = new LinkedHashMap<>(postgres);
            discovery.put("user", discovery.remove("username"));
            discovery.put("schema", "public");
            control.discoverSchema("src_pg", "postgres", discovery);
            assertThat(PostgresSlots.of(postgres))
                    .as("the source has no replication slot before the snapshot-only pipeline starts")
                    .isEmpty();

            control.lifecycle(PIPELINE, LifecycleVerb.START);
            EndpointAddress target = EndpointAddress.uri(targetUri);
            Await.until("the snapshot row to reach MongoDB",
                    () -> mongo.count(target, "items") == 1,
                    () -> control.logs(PIPELINE));
            Await.until("the snapshot-only pipeline to report RUNNING",
                    () -> control.state(PIPELINE).filter(PipelineState.RUNNING::equals).isPresent(),
                    () -> control.state(PIPELINE) + "; pipeline logs: " + control.logs(PIPELINE));
            List<PostgresSlots.Slot> afterLoad = PostgresSlots.of(postgres);

            control.stop(PIPELINE, true);
            Await.until("the clearing stop to report STOPPED",
                    () -> control.state(PIPELINE).filter(PipelineState.STOPPED::equals).isPresent(),
                    () -> control.state(PIPELINE) + "; pipeline logs: " + control.logs(PIPELINE));
            List<PostgresSlots.Slot> afterClear = PostgresSlots.of(postgres);

            assertSoftly(softly -> {
                softly.assertThat(afterLoad)
                        .as("snapshot_only has no change tail to consume or confirm a slot after its load")
                        .isEmpty();
                softly.assertThat(afterClear)
                        .as("purgeState=true must leave no slot retaining WAL after snapshot_only is STOPPED; "
                                + "slots after the load: %s", afterLoad)
                        .isEmpty();
            });
        }
    }
}
