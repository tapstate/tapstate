package io.tapstate.e2e;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.mongodb.ReadConcern;
import com.mongodb.WriteConcern;
import com.mongodb.client.MongoClients;
import com.mongodb.client.model.Sorts;
import io.tapstate.core.lifecycle.LifecycleVerb;
import io.tapstate.core.lifecycle.PipelineState;
import io.tapstate.testsupport.RequiresDocker;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.IntStream;
import org.bson.BsonDecimal128;
import org.bson.BsonDocument;
import org.bson.BsonInt32;
import org.bson.BsonInt64;
import org.bson.BsonString;
import org.bson.BsonType;
import org.bson.Document;
import org.bson.types.Decimal128;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

@RequiresDocker
class MongoScriptDecimalIT {
    private static final String TABLE = "records";
    private static final Duration LIMIT = Duration.ofSeconds(55);
    private static final BsonDecimal128 EXACT = new BsonDecimal128(
            Decimal128.parse("1234567890.123456789012345678901234"));

    @BeforeAll
    static void requireMongoConnector() {
        RealConnectorGate.require("mongodb");
    }

    @Test
    void decimalDescriptionsAreUsedOnlyForUnchangedScriptValues() {
        String suffix = UUID.randomUUID().toString().replace("-", "");
        String sourceName = "script_decimal_src_" + suffix;
        String sourceUri = SharedMongo.replicaSetUrl(sourceName);
        List<String> targetNames = new ArrayList<>();
        try (var mongo = MongoClients.create(sourceUri);
                var storage = PreparedNativeStore.create();
                var server = RealProcessServer.startPrepared(storage.configurationUri(), "127.0.0.1", ignored -> List.of())) {
            var sourceDb = mongo.getDatabase(sourceName).withWriteConcern(WriteConcern.MAJORITY);
            try {
                sourceDb.getCollection(TABLE, BsonDocument.class).insertMany(List.of(
                        row(1, "first", EXACT), row(2, "second", new BsonDecimal128(Decimal128.parse("-4.5000")))));
                ControlPlane control = new ControlPlane(server.baseUrl());
                storage.loginAndAwaitBusiness(control);
                control.registerConnector("mongodb", ConnectorJars.bytesFor("mongodb"));
                String source = connection("src_script", sourceUri, sourceName, true);
                control.apply(Map.of("source.tap.yml", source));
                control.discoverSchema("src_script", "mongodb", Map.of("uri", sourceUri, "database", sourceName));
                assertTrue(control.sourceSchemaFields("src_script", TABLE).contains("d0"));

                List<Variant> variants = List.of(
                        new Variant("copy", "", PipelineState.COMPLETED),
                        new Variant("identity", "return record;", PipelineState.COMPLETED),
                        new Variant("preserved", """
                                record.after.value = Number(record.after.value) + 7;
                                record.after.label = 'js-' + record.after.label;
                                record.after.extra = 'new-column';
                                return record;
                                """, PipelineState.COMPLETED),
                        new Variant("mixed", """
                                if (record.after._id === 2) record.after.d0 = 'decimal-as-text';
                                return record;
                                """, PipelineState.COMPLETED),
                        new Variant("empty", "return record;", PipelineState.COMPLETED),
                        new Variant("rejected", "throw new Error('invalid script output');", PipelineState.FAILED));
                for (Variant variant : variants) {
                    String targetName = "script_" + variant.name() + "_" + suffix.substring(0, 16);
                    targetNames.add(targetName);
                    String targetUri = SharedMongo.replicaSetUrl(targetName);
                    var target = mongo.getDatabase(targetName).withReadConcern(ReadConcern.MAJORITY)
                            .getCollection(TABLE, BsonDocument.class);
                    var original = new BsonDocument("_id", new BsonInt64(99)).append("sentinel", new BsonString("existing"));
                    target.insertOne(original);
                    String pipe = "script_decimal_" + variant.name();
                    control.apply(Map.of("source.tap.yml", source,
                            "target.tap.yml", connection("tgt_" + variant.name(), targetUri, targetName, false),
                            "pipeline.tap.yml", pipeline(pipe, "tgt_" + variant.name(), variant.body(), variant.name().equals("empty"))));
                    control.lifecycle(pipe, LifecycleVerb.START);
                    Await.until("terminal state for " + pipe, LIMIT,
                            () -> control.state(pipe).filter(state -> state == PipelineState.FAILED
                                    || state == PipelineState.COMPLETED).isPresent(),
                            () -> control.state(pipe) + " " + control.metrics(pipe) + " " + control.logs(pipe));
                    assertEquals(variant.terminal(), control.state(pipe).orElseThrow(),
                            () -> control.metrics(pipe) + " " + control.logs(pipe));
                    var landed = target.find().sort(Sorts.ascending("_id")).into(new ArrayList<>());
                    if (variant.terminal() == PipelineState.FAILED) {
                        assertEquals(List.of(original), landed);
                        continue;
                    }
                    if (variant.name().equals("empty")) {
                        assertTrue(landed.isEmpty());
                        continue;
                    }
                    assertEquals(2, landed.size());
                    var first = landed.getFirst();
                    var second = landed.getLast();
                    assertEquals(BsonType.DECIMAL128, first.get("d0").getBsonType());
                    assertEquals(EXACT, first.get("d0"));
                    if (variant.name().equals("mixed")) {
                        assertEquals("decimal-as-text", second.getString("d0").getValue());
                    } else {
                        assertEquals(new BsonDecimal128(Decimal128.parse("-4.5000")), second.get("d0"));
                    }
                    if (variant.name().equals("preserved")) {
                        assertEquals(14, first.getNumber("value").intValue());
                        assertEquals("js-first", first.getString("label").getValue());
                        assertEquals("new-column", first.getString("extra").getValue());
                    }
                }
            } finally {
                targetNames.forEach(name -> mongo.getDatabase(name).drop());
                sourceDb.drop();
            }
        }
    }

    @Test
    void parallelWritersWaitForDeferredClearPastTenSeconds() throws Exception {
        String suffix = UUID.randomUUID().toString().replace("-", "");
        String sourceName = "script_slow_src_" + suffix;
        String targetName = "script_slow_tgt_" + suffix;
        String targetApp = "script-deferred-clear";
        String pipe = "script_deferred_clear";
        int rows = 32;
        try (var replica = BenchmarkTargetLimit.newReplicaSet()) {
            replica.start();
            String sourceUri = replica.getReplicaSetUrl(sourceName);
            String uri = replica.getReplicaSetUrl(targetName);
            String targetUri = uri + (uri.contains("?") ? "&" : "?") + "appName=" + targetApp;
            try (var mongo = MongoClients.create(sourceUri);
                    var storage = PreparedNativeStore.create();
                    var server = RealProcessServer.startPrepared(storage.configurationUri(), "127.0.0.1",
                            ignored -> List.of())) {
                mongo.getDatabase(sourceName).withWriteConcern(WriteConcern.MAJORITY)
                        .getCollection(TABLE, BsonDocument.class).insertMany(IntStream.rangeClosed(1, rows)
                                .mapToObj(key -> row(key, "source-" + key, EXACT)).toList());
                var target = mongo.getDatabase(targetName).withReadConcern(ReadConcern.MAJORITY)
                        .getCollection(TABLE, BsonDocument.class);
                target.insertOne(row(99, "pre-existing", EXACT));
                var control = new ControlPlane(server.baseUrl());
                storage.loginAndAwaitBusiness(control);
                control.registerConnector("mongodb", ConnectorJars.bytesFor("mongodb"));
                String source = connection("src_script", sourceUri, sourceName, true);
                control.apply(Map.of("source.tap.yml", source));
                control.discoverSchema("src_script", "mongodb", Map.of("uri", sourceUri, "database", sourceName));
                control.apply(Map.of("source.tap.yml", source,
                        "target.tap.yml", connection("tgt_script", targetUri, targetName, false),
                        "pipeline.tap.yml", pipeline(pipe, "tgt_script", "return record;", false,
                                ", execution: {parallelism: 2, batch: {max_records: 1}}")));
                var admin = mongo.getDatabase("admin").withTimeout(20, TimeUnit.SECONDS);
                var hold = new Document("failCommands", List.of("delete")).append("appName", targetApp)
                        .append("namespace", targetName + "." + TABLE)
                        .append("blockConnection", true).append("blockTimeMS", 13_000);
                long before = admin.runCommand(new Document("configureFailPoint", "failCommand")
                        .append("mode", "alwaysOn").append("data", hold)).get("count", Number.class).longValue();
                var after = new AtomicLong();
                try (AutoCloseable disarm = () -> after.set(admin.runCommand(
                        new Document("configureFailPoint", "failCommand").append("mode", "off"))
                        .get("count", Number.class).longValue())) {
                    control.lifecycle(pipe, LifecycleVerb.START);
                    admin.withTimeout(LIMIT.toSeconds(), TimeUnit.SECONDS).runCommand(new Document(
                            "waitForFailPoint", "failCommand").append("timesEntered", before + 1));
                    Await.until("terminal state after the deferred target clear", LIMIT,
                            () -> control.state(pipe).filter(state -> state == PipelineState.COMPLETED
                                    || state == PipelineState.FAILED).isPresent(), () -> control.logs(pipe));
                    assertEquals(PipelineState.COMPLETED, control.state(pipe).orElseThrow(), () -> control.logs(pipe));
                    var landed = target.find().sort(Sorts.ascending("_id")).into(new ArrayList<>());
                    assertEquals(rows, landed.size());
                    for (int i = 0; i < rows; i++) {
                        assertEquals(i + 1L, landed.get(i).getNumber("_id").longValue());
                        assertEquals(EXACT, landed.get(i).get("d0"));
                    }
                }
                assertEquals(1, after.get() - before, "one target clear");
            }
        }
    }

    private static BsonDocument row(long key, String label, BsonDecimal128 amount) {
        return new BsonDocument("_id", new BsonInt64(key)).append("value", new BsonInt32(7))
                .append("label", new BsonString(label)).append("d0", amount);
    }

    private static String connection(String id, String uri, String database, boolean input) {
        return """
                version: tapstate/v1
                kind: source
                id: %s
                connector: mongodb
                config: {uri: "%s", database: "%s"}
                %s
                """.formatted(id, uri, database, input ? "mode: cdc\ntables: [records]" : "");
    }

    private static String pipeline(String id, String target, String body, boolean empty) {
        return pipeline(id, target, body, empty, "");
    }

    private static String pipeline(String id, String target, String body, boolean empty, String sinkOptions) {
        String script = body.isEmpty() ? "" : """
                transforms:
                  - id: script
                    from: [records]
                    type: js
                    script: |
                      function process(record) {
                        if (record.after == null) return record;
                %s
                      }
                """.formatted(body.strip().indent(8).stripTrailing());
        return """
                version: tapstate/v1
                kind: pipeline
                id: %s
                source: src_script
                settings: {read_mode: snapshot_only}
                %s%sserve:
                  from: %s
                  sync: [{id: output, source: %s, write_mode: upsert, on_full_load: clear%s}]
                """.formatted(id, script,
                        empty ? "  - {id: no_rows, from: [script], type: filter, expr: \"false\"}\n" : "",
                        empty ? "no_rows" : body.isEmpty() ? "/.*/" : "script", target, sinkOptions);
    }

    private record Variant(String name, String body, PipelineState terminal) {
    }
}
