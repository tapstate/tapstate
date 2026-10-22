package io.tapstate.e2e;

import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.model.Filters;
import com.mongodb.client.model.Updates;
import io.tapstate.core.lifecycle.LifecycleVerb;
import io.tapstate.testsupport.RequiresDocker;
import java.time.Duration;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import org.bson.Document;
import org.bson.types.ObjectId;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;
import static org.assertj.core.api.Assertions.assertThat;

/** Real source capture, target reconciliation and persisted replay of expanded MongoDB documents. */
@RequiresDocker
class RealMongoUnwindIdentityIT {
    private static final Duration TIMEOUT = Duration.ofSeconds(120);
    private static final int PARENTS = 200;
    private static final String TABLE = "orders";

    @BeforeAll
    static void requireConnector() { RealConnectorGate.require("mongodb"); }

    @ParameterizedTest
    @EnumSource(Tiers.class)
    void fullLoadChangesAndWholeRuntimeReplayKeepEveryExpandedRow(Tiers tier) throws Exception {
        String suffix = UUID.randomUUID().toString().replace("-", "");
        String sourceDb = "unwind_src_" + suffix;
        String targetDb = "unwind_tgt_" + suffix;
        String metadataDb = "unwind_meta_" + suffix;
        String operatorDb = "unwind_ops_" + suffix;
        String sourceUri = SharedMongo.replicaSetUrl(sourceDb);
        String targetUri = SharedMongo.replicaSetUrl(targetDb);
        String storeUri = SharedMongo.replicaSetUrl(metadataDb);
        String pipeline = "unwind_" + suffix;
        String preserved = tier == Tiers.IN_PROCESS ? "_unwind_source_id" : "original_order_id";
        boolean objectIds = tier == Tiers.REAL_PROCESS;
        try (var source = MongoClients.create(sourceUri); var target = MongoClients.create(targetUri);
                var metadata = MongoClients.create(storeUri)) {
            var sourceDatabase = source.getDatabase(sourceDb);
            var targetDatabase = target.getDatabase(targetDb);
            try {
                sourceDatabase.createCollection(TABLE);
                sourceDatabase.runCommand(new Document("collMod", TABLE)
                        .append("changeStreamPreAndPostImages", new Document("enabled", true)));
                MongoCollection<Document> parents = sourceDatabase.getCollection(TABLE);
                MongoCollection<Document> children = targetDatabase.getCollection(TABLE);
                List<Document> seeded = new ArrayList<>();
                for (int i = 0; i < PARENTS; i++) seeded.add(parent(i, objectIds, List.of("t0", "t1")));
                parents.insertMany(seeded);
                String acknowledged;
                Object stableId;
                try (ServerHandle server = tier.launch(storeUri, operatorDb)) {
                    ControlPlane control = new ControlPlane(server.baseUrl());
                    control.bootstrapAndLogin("e2e", "e2e-password");
                    control.registerConnector("mongodb", ConnectorJars.bytesFor("mongodb"));
                    Map<String, String> resources = new LinkedHashMap<>();
                    resources.put("source.tap.yml", """
                            version: tapstate/v1
                            kind: source
                            id: src_orders
                            connector: mongodb
                            config: { uri: "%s", preImage: true }
                            mode: cdc
                            tables: [orders]
                            """.formatted(sourceUri));
                    resources.put("target.tap.yml", """
                            version: tapstate/v1
                            kind: source
                            id: tgt_orders
                            connector: mongodb
                            config: { uri: "%s" }
                            """.formatted(targetUri));
                    control.apply(resources);
                    control.discoverSchema("src_orders", "mongodb", Map.of("uri", sourceUri, "preImage", true));
                    resources.put("pipeline.tap.yml", """
                            version: tapstate/v1
                            kind: pipeline
                            id: %s
                            source: src_orders
                            settings: { read_mode: snapshot_and_cdc }
                            transforms:
                              - id: expanded
                                type: unwind
                                from: [orders]
                                path: TAGS
                                include_array_index: TAG_NO
                                element_type: string
                            %sserve:
                              from: expanded
                              sync: [{ id: output, source: tgt_orders, write_mode: upsert, on_full_load: clear }]
                            """.formatted(pipeline, objectIds ? "    source_id_field: original_order_id\n" : ""));
                    control.apply(resources);
                    control.lifecycle(pipeline, LifecycleVerb.START);
                    Await.until("all 400 expanded snapshot rows", TIMEOUT,
                            () -> children.countDocuments() == PARENTS * 2, () -> control.logs(pipeline));
                    assertRows(parents, children, preserved);
                    stableId = children.find(new Document(preserved, id(0, objectIds)).append("TAG_NO", 0L))
                            .first().get("_id");
                    String snapshotAck = Await.answered("snapshot target acknowledgement", TIMEOUT,
                            () -> control.durablePosition(pipeline, TABLE));
                    parents.updateOne(Filters.eq("_id", id(0, objectIds)), Updates.set("TAGS", List.of("changed", "added", "last")));
                    parents.deleteOne(Filters.eq("_id", id(1, objectIds)));
                    parents.updateOne(Filters.eq("_id", id(2, objectIds)), Updates.set("TAGS", List.of("t1", "t0")));
                    Await.until("live updates, deletion and array reordering", TIMEOUT,
                            () -> actual(children, preserved).equals(expected(parents)), () -> control.logs(pipeline));
                    acknowledged = Await.answered("live changes acknowledged", TIMEOUT,
                            () -> control.durablePosition(pipeline, TABLE).filter(position -> !position.equals(snapshotAck)));
                    assertThat(children.find(new Document(preserved, id(0, objectIds)).append("TAG_NO", 0L)).first())
                            .containsEntry("_id", stableId);
                }
                Map<List<Object>, String> beforeDowntime = actual(children, preserved);
                parents.updateOne(Filters.eq("_id", id(0, objectIds)), Updates.set("TAGS", List.of("offline")));
                parents.deleteOne(Filters.eq("_id", id(2, objectIds)));
                parents.insertOne(parent(PARENTS, objectIds, List.of("new-a", "new-b")));
                assertThat(actual(children, preserved)).isEqualTo(beforeDowntime);
                // No reapply, register or START: the persisted desired state and source acknowledgement are reused.
                try (ServerHandle server = tier.launch(storeUri, operatorDb)) {
                    ControlPlane control = new ControlPlane(server.baseUrl());
                    control.login("e2e", "e2e-password");
                    Await.until("downtime changes replayed after runtime restart", TIMEOUT,
                            () -> actual(children, preserved).equals(expected(parents)), () -> control.logs(pipeline));
                    String replayAck = Await.answered("replayed changes acknowledged", TIMEOUT,
                            () -> control.durablePosition(pipeline, TABLE).filter(position -> !position.equals(acknowledged)));
                    parents.insertOne(parent(PARENTS + 1, objectIds, List.of("live-after-restart")));
                    Await.until("new CDC after replay", TIMEOUT,
                            () -> actual(children, preserved).equals(expected(parents))
                                    && control.durablePosition(pipeline, TABLE).filter(position -> !position.equals(replayAck)).isPresent(),
                            () -> control.logs(pipeline));
                    assertRows(parents, children, preserved);
                    assertThat(children.find(new Document(preserved, id(0, objectIds)).append("TAG_NO", 0L)).first())
                            .containsEntry("_id", stableId);
                    AtlasRuntime.assertResumedRun(control, pipeline, TABLE, PARENTS, TIMEOUT);
                }
            } finally {
                sourceDatabase.drop();
                targetDatabase.drop();
                metadata.getDatabase(metadataDb).drop();
                metadata.getDatabase(operatorDb).drop();
            }
        }
    }

    private static Object id(int value, boolean objectIds) {
        return objectIds ? new ObjectId(String.format("%024x", value + 1)) : "order-" + value;
    }

    private static Document parent(int value, boolean objectIds, List<String> tags) {
        return new Document("_id", id(value, objectIds)).append("owner", "customer-" + value).append("TAGS", tags);
    }

    private static Map<List<Object>, String> expected(MongoCollection<Document> parents) {
        Map<List<Object>, String> rows = new LinkedHashMap<>();
        for (Document parent : parents.find()) {
            List<String> tags = parent.getList("TAGS", String.class);
            for (int index = 0; index < tags.size(); index++) rows.put(List.of(parent.get("_id"), (long) index), tags.get(index));
        }
        return rows;
    }

    private static Map<List<Object>, String> actual(MongoCollection<Document> children, String preserved) {
        Map<List<Object>, String> rows = new LinkedHashMap<>();
        for (Document row : children.find()) {
            if (!row.containsKey(preserved) || !(row.get("TAG_NO") instanceof Number ordinal)) return Map.of();
            rows.put(List.of(row.get(preserved), ordinal.longValue()), row.getString("TAGS"));
        }
        return rows;
    }

    private static void assertRows(MongoCollection<Document> parents, MongoCollection<Document> children, String preserved) {
        Map<List<Object>, String> expected = expected(parents);
        assertThat(actual(children, preserved)).isEqualTo(expected);
        assertThat(children.countDocuments()).isEqualTo(expected.size());
    }
}
