package io.tapstate.e2e;

import com.mongodb.client.MongoClients;
import io.tapstate.adapters.pdk.ConnectorIntrospector;
import io.tapstate.adapters.pdk.ConnectorRef;
import io.tapstate.adapters.pdk.PdkSinkPort;
import io.tapstate.core.common.TapstateType;
import io.tapstate.core.event.Envelope;
import io.tapstate.core.model.PipelineNode;
import io.tapstate.spi.sink.*;
import io.tapstate.testsupport.RequiresDocker;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import org.bson.Document;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import static org.assertj.core.api.Assertions.assertThat;

/** Exercises expanded parent identifiers against the real target's built-in unique index. */
@RequiresDocker
class MongoUnwindIdentityIT {
    private static ConnectorRef connector;

    @BeforeAll
    static void loadConnector() throws Exception {
        RealConnectorGate.require("mongodb");
        Path jar = ConnectorJars.pathFor("mongodb");
        var inspected = new ConnectorIntrospector().introspect(List.of(jar));
        connector = new ConnectorRef(List.of(jar), inspected.className(), inspected.pdkApiVersion(),
                null, inspected.spec());
    }

    @Test
    void expandedRowsKeepIndependentIdentityThroughCdcAndReplay() throws Exception {
        String database = "unwind_identity_" + UUID.randomUUID().toString().replace("-", "");
        String uri = SharedMongo.replicaSetUrl(database);
        TargetTable target = new TargetTable("orders", List.of(
                new TargetField("_id", "string", true, TapstateType.STRING),
                new TargetField("TAG_NO", "long", true, TapstateType.INT64),
                new TargetField("TAGS", "string", false, TapstateType.STRING)),
                List.of(new TargetIndex(List.of("_id", "TAG_NO"), true)),
                new UnwindIdentity(List.of("_id"), "_unwind_source_id"));
        PipelineNode node = new PipelineNode("unwind_identity", "sink");
        PdkSinkPort sink = new PdkSinkPort(id -> connector);
        SinkConfig config = new SinkConfig("mongodb", Map.of("uri", uri, "database", database),
                WriteMode.UPSERT, DdlPolicy.FAIL, target, node, OnFullLoad.APPEND, true);
        try (var client = MongoClients.create(uri)) {
            var mongo = client.getDatabase(database);
            try {
                var collection = mongo.getCollection("orders");
                List<Envelope> snapshot = new ArrayList<>();
                for (int parent = 0; parent < 200; parent++) {
                    for (int tag = 0; tag < 2; tag++) {
                        snapshot.add(Envelope.insert(1L, "orders", row(parent, tag, "t" + tag), null));
                    }
                }
                List<Envelope> changes = List.of(
                        Envelope.update(2L, "orders", row(0, 0, "t0"), row(0, 0, "changed"), null),
                        Envelope.delete(3L, "orders", row(0, 1, "t1"), null),
                        Envelope.insert(4L, "orders", row(0, 2, "added"), null),
                        Envelope.delete(5L, "orders", row(1, 0, "t0"), null),
                        Envelope.delete(5L, "orders", row(1, 1, "t1"), null));
                Object stableId;
                try (SinkWriter writer = sink.open(config)) {
                    writer.write(snapshot).toCompletableFuture().get(30, TimeUnit.SECONDS);
                    assertThat(collection.countDocuments()).isEqualTo(400);
                    assertThat(collection.distinct("_id", org.bson.types.ObjectId.class).into(new ArrayList<>())).hasSize(400);
                    assertThat(collection.countDocuments(new Document("_unwind_source_id", "0"))).isEqualTo(2);
                    stableId = collection.find(new Document("_unwind_source_id", "0").append("TAG_NO", 0L))
                            .first().get("_id");
                    writer.write(changes).toCompletableFuture().get(30, TimeUnit.SECONDS);
                }
                SinkConfig resumed = new SinkConfig("mongodb", config.settings(), WriteMode.UPSERT,
                        DdlPolicy.FAIL, target, node, OnFullLoad.APPEND, false);
                try (SinkWriter writer = sink.open(resumed)) {
                    writer.write(changes).toCompletableFuture().get(30, TimeUnit.SECONDS);
                    writer.write(changes).toCompletableFuture().get(30, TimeUnit.SECONDS);
                }
                assertThat(collection.countDocuments()).isEqualTo(398);
                assertThat(collection.countDocuments(new Document("_unwind_source_id", "1"))).isZero();
                assertThat(collection.countDocuments(new Document("_unwind_source_id", "0").append("TAG_NO", 1L))).isZero();
                Document changed = collection.find(new Document("_unwind_source_id", "0").append("TAG_NO", 0L)).first();
                assertThat(changed).containsEntry("_id", stableId).containsEntry("TAGS", "changed");
                assertThat(collection.countDocuments(new Document("_unwind_source_id", "0").append("TAG_NO", 2L))).isEqualTo(1);
            } finally {
                mongo.drop();
            }
        }
    }

    @Test
    void specialUniqueIndexesDoNotMasqueradeAsCompleteLogicalKeyConstraints() {
        String database = "unwind_indexes_" + UUID.randomUUID().toString().replace("-", "");
        String uri = SharedMongo.replicaSetUrl(database);
        TargetTable target = new TargetTable("orders", List.of(
                new TargetField("_id", "string", true, TapstateType.STRING),
                new TargetField("TAG_NO", "long", true, TapstateType.INT64)),
                List.of(new TargetIndex(List.of("_id", "TAG_NO"), true)),
                new UnwindIdentity(List.of("_id"), "_unwind_source_id"));
        SinkConfig config = new SinkConfig("mongodb", Map.of("uri", uri, "database", database),
                WriteMode.UPSERT, DdlPolicy.FAIL, target, null, OnFullLoad.APPEND, false);
        try (var client = MongoClients.create(uri)) {
            var mongo = client.getDatabase(database);
            try {
                for (var options : List.of(
                        new com.mongodb.client.model.IndexOptions().unique(true).sparse(true),
                        new com.mongodb.client.model.IndexOptions().unique(true).partialFilterExpression(new Document("TAG_NO", new Document("$gte", 0))),
                        new com.mongodb.client.model.IndexOptions().unique(true).collation(
                                com.mongodb.client.model.Collation.builder().locale("en").collationStrength(
                                        com.mongodb.client.model.CollationStrength.SECONDARY).build()))) {
                    mongo.createCollection("orders");
                    mongo.getCollection("orders").createIndex(new Document("_unwind_source_id", 1).append("TAG_NO", 1), options);
                    var plan = new io.tapstate.adapters.pdk.PdkTargetIdentityPlanner(id -> connector)
                            .assess(config, Map.of("orders", target)).get("orders");
                    assertThat(plan.status()).isEqualTo("action-required");
                    assertThat(plan.reason()).isEqualTo("other-unique-constraint");
                    mongo.getCollection("orders").drop();
                }
            } finally {
                mongo.drop();
            }
        }
    }

    private static Map<String, Object> row(int parent, int tag, String value) {
        return Map.of("_id", Integer.toString(parent), "TAG_NO", (long) tag, "TAGS", value);
    }
}
