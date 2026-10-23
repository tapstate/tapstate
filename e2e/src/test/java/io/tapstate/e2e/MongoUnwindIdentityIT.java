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
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import io.tapstate.core.event.ConvertedValue;
import io.tapstate.core.event.Bytes;
import io.tapstate.core.common.TapstateException;
import io.tapstate.adapters.pdk.ConnectorError;

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

    @Test
    void arrayScalarElementKeysAreRejectedBeforeSnapshotCdcOrReplayCanWriteAPrefix() throws Exception {
        String database = "unwind_key_guard_" + UUID.randomUUID().toString().replace("-", "");
        String uri = SharedMongo.replicaSetUrl(database);
        var sink = new PdkSinkPort(id -> connector);
        try (var client = MongoClients.create(uri)) {
            var mongo = client.getDatabase(database);
            try (var writer = sink.open(keyConfig(uri, database))) {
                var collection = mongo.getCollection("orders");
                for (var keys : List.of(List.of(List.of("a"), "a"), List.of("a", List.of("a")))) {
                    var expanded = expand(Envelope.insert(1L, "orders", parent(keys), null));
                    assertThat(expanded).hasSize(2);
                    rejected(writer, expanded);
                    assertThat(collection.countDocuments()).isZero();
                }
                var initial = parent(List.of("a", "b"));
                writer.write(expand(Envelope.read(2L, "orders", initial, null))).toCompletableFuture().get(30, TimeUnit.SECONDS);
                var before = collection.find().into(new ArrayList<>());
                var changed = parent(List.of("a", List.of("b")));
                var cdc = expand(Envelope.update(3L, "orders", initial, changed, null));
                rejected(writer, cdc);
                rejected(writer, cdc);
                assertThat(collection.find().into(new ArrayList<>())).containsExactlyInAnyOrderElementsOf(before);
                var deleted = expand(Envelope.delete(4L, "orders", initial, null));
                writer.write(deleted).toCompletableFuture().get(30, TimeUnit.SECONDS);
                writer.write(deleted).toCompletableFuture().get(30, TimeUnit.SECONDS);
                assertThat(collection.countDocuments()).isZero();
            } finally {
                mongo.drop();
            }
        }
    }

    @Test
    void existingMultikeyDocumentsCannotBeOverwrittenOrDeletedByScalarMatches() throws Exception {
        String database = "unwind_existing_keys_" + UUID.randomUUID().toString().replace("-", "");
        String uri = SharedMongo.replicaSetUrl(database);
        var oid = new org.bson.types.ObjectId();
        var instant = java.time.Instant.ofEpochMilli(1_700_000_000_123L);
        var binary = new org.bson.types.Binary((byte) 0x80, new byte[] {1, 2, 3});
        var nativeKeys = List.of("a", 42L, oid, java.util.Date.from(instant), binary);
        var portableKeys = List.of("a", 42L, new ConvertedValue(oid.toHexString(), "OBJECT_ID"), instant,
                new ConvertedValue(new Bytes(binary.getType(), binary.getData()), "BINARY"));
        try (var client = MongoClients.create(uri)) {
            var mongo = client.getDatabase(database);
            try {
                for (int i = 0; i < nativeKeys.size(); i++) {
                    mongo.getCollection("orders").drop();
                    try (var writer = new PdkSinkPort(id -> connector).open(keyConfig(uri, database))) {
                        var collection = mongo.getCollection("orders");
                        collection.insertOne(new Document("_unwind_source_id", "p").append("k", List.of(nativeKeys.get(i))));
                        var saved = collection.find().first();
                        var scalar = Map.<String, Object>of("_id", "p", "k", portableKeys.get(i));
                        var prefix = Envelope.insert(1L, "orders", Map.<String, Object>of("_id", "other", "k", "safe"), null);
                        for (var event : List.of(Envelope.insert(2L, "orders", scalar, null),
                                Envelope.update(3L, "orders", scalar, scalar, null), Envelope.delete(4L, "orders", scalar, null))) {
                            rejected(writer, List.of(prefix, event));
                            rejected(writer, List.of(event));
                            assertThat(collection.find().into(new ArrayList<>())).containsExactly(saved);
                        }
                    }
                }
            } finally {
                mongo.drop();
            }
        }
    }

    @Test
    void canonicalNativeScalarKeysKeepTheirTypesAndIdentityThroughUpdatesAndReplay() throws Exception {
        String database = "unwind_scalar_keys_" + UUID.randomUUID().toString().replace("-", "");
        String uri = SharedMongo.replicaSetUrl(database);
        var oid = new org.bson.types.ObjectId();
        var instant = java.time.Instant.ofEpochMilli(1_700_000_000_123L);
        var keys = List.of("a", Long.MAX_VALUE, true, new ConvertedValue(oid.toHexString(), "OBJECT_ID"), instant,
                new ConvertedValue(new Bytes((byte) 0x80, new byte[] {1, 2, 3}), "BINARY"));
        try (var client = MongoClients.create(uri)) {
            var mongo = client.getDatabase(database);
            try {
                List<Document> snapshot;
                try (var writer = new PdkSinkPort(id -> connector).open(keyConfig(uri, database))) {
                    var events = expand(Envelope.read(1L, "orders", parent(keys), null));
                    writer.write(events).toCompletableFuture().get(30, TimeUnit.SECONDS);
                    snapshot = mongo.getCollection("orders").find().into(new ArrayList<>());
                    assertThat(snapshot).hasSize(keys.size());
                    writer.write(events).toCompletableFuture().get(30, TimeUnit.SECONDS);
                    assertThat(mongo.getCollection("orders").find().into(new ArrayList<>())).containsExactlyInAnyOrderElementsOf(snapshot);
                }
                try (var writer = new PdkSinkPort(id -> connector).open(keyConfig(uri, database))) {
                    var changes = expand(Envelope.update(2L, "orders", parent(keys), parent(keys.subList(1, keys.size())), null));
                    writer.write(changes).toCompletableFuture().get(30, TimeUnit.SECONDS);
                    writer.write(changes).toCompletableFuture().get(30, TimeUnit.SECONDS);
                    assertThat(mongo.getCollection("orders").countDocuments()).isEqualTo(keys.size() - 1);
                    assertThat(mongo.getCollection("orders").find().into(new ArrayList<>())).allSatisfy(doc ->
                            assertThat(snapshot).anySatisfy(old -> assertThat(old).containsEntry("_id", doc.get("_id"))));
                    var deletes = expand(Envelope.delete(3L, "orders", parent(keys.subList(1, keys.size())), null));
                    writer.write(deletes).toCompletableFuture().get(30, TimeUnit.SECONDS);
                    writer.write(deletes).toCompletableFuture().get(30, TimeUnit.SECONDS);
                    assertThat(mongo.getCollection("orders").countDocuments()).isZero();
                }
            } finally {
                mongo.drop();
            }
        }
    }

    @Test
    void aTypeOnlyElementKeyChangeCannotCreateANonReplayableUpdate() throws Exception {
        String database = "unwind_type_change_" + UUID.randomUUID().toString().replace("-", "");
        String uri = SharedMongo.replicaSetUrl(database);
        String hex = new org.bson.types.ObjectId().toHexString();
        Object objectId = new ConvertedValue(hex, "OBJECT_ID");
        try (var client = MongoClients.create(uri)) {
            var mongo = client.getDatabase(database);
            try {
                for (var pair : List.of(List.of(objectId, hex), List.of(hex, objectId))) {
                    mongo.getCollection("orders").drop();
                    try (var writer = new PdkSinkPort(id -> connector).open(keyConfig(uri, database))) {
                        var before = parent(List.of(pair.get(0)));
                        var after = parent(List.of(pair.get(1)));
                        writer.write(expand(Envelope.read(1L, "orders", before, null))).toCompletableFuture().get(30, TimeUnit.SECONDS);
                        var saved = mongo.getCollection("orders").find().first();
                        var change = expand(Envelope.update(2L, "orders", before, after, null));
                        assertThat(change).hasSize(1);
                        rejected(writer, change);
                        rejected(writer, change);
                        assertThat(mongo.getCollection("orders").find().into(new ArrayList<>())).containsExactly(saved);
                    }
                }
            } finally {
                mongo.drop();
            }
        }
    }

    private static SinkConfig keyConfig(String uri, String database) {
        var target = new TargetTable("orders", List.of(new TargetField("_id", "string", true, TapstateType.STRING),
                new TargetField("k", "json", true, TapstateType.JSON), new TargetField("items", "json", false, TapstateType.JSON)),
                List.of(new TargetIndex(List.of("_id", "k"), true)), new UnwindIdentity(List.of("_id"), "_unwind_source_id"));
        return new SinkConfig("mongodb", Map.of("uri", uri, "database", database), WriteMode.UPSERT, DdlPolicy.FAIL,
                target, null, OnFullLoad.APPEND, false);
    }

    private static Map<String, Object> parent(List<?> keys) {
        return Map.of("_id", "p", "items", keys.stream().map(key -> Map.of("k", key, "value", "kept")).toList());
    }

    private static List<Envelope> expand(Envelope event) {
        var spec = io.tapstate.adapters.transform.UnwindSpec.from(
                new io.tapstate.core.model.TransformBody.Unwind("items", null, false, "k", "json"), List.of("_id"));
        return io.tapstate.adapters.transform.StatelessTransforms.unwind(spec).transform(event);
    }

    private static void rejected(SinkWriter writer, List<Envelope> events) {
        assertThatThrownBy(() -> writer.write(events).toCompletableFuture().get(30, TimeUnit.SECONDS))
                .hasRootCauseInstanceOf(TapstateException.class)
                .satisfies(error -> {
                    Throwable root = error;
                    while (root.getCause() != null) root = root.getCause();
                    assertThat(((TapstateException) root).code()).isEqualTo(ConnectorError.TARGET_IDENTITY_CONFLICT);
                    assertThat(((TapstateException) root).args()).containsEntry("reason", "key-representation-unsupported");
                });
    }

    private static Map<String, Object> row(int parent, int tag, String value) {
        return Map.of("_id", Integer.toString(parent), "TAG_NO", (long) tag, "TAGS", value);
    }
}
