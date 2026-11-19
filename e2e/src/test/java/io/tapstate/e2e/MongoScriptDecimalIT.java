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
import org.bson.BsonDecimal128;
import org.bson.BsonDocument;
import org.bson.BsonInt32;
import org.bson.BsonInt64;
import org.bson.BsonString;
import org.bson.BsonType;
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
                  sync: [{id: output, source: %s, write_mode: upsert, on_full_load: clear}]
                """.formatted(id, script,
                        empty ? "  - {id: no_rows, from: [script], type: filter, expr: \"false\"}\n" : "",
                        empty ? "no_rows" : body.isEmpty() ? "/.*/" : "script", target);
    }

    private record Variant(String name, String body, PipelineState terminal) {
    }
}
