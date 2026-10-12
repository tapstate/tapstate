package io.tapstate.e2e;

import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.model.Filters;
import com.mongodb.client.model.Updates;
import io.tapstate.core.lifecycle.LifecycleVerb;
import io.tapstate.core.common.JsonWriter;
import io.tapstate.adapters.mongostore.SystemCollections;
import io.tapstate.testsupport.RequiresDocker;
import org.bson.Document;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.time.Duration;
import java.time.Instant;
import java.util.LinkedHashMap;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.TreeMap;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;

/** Real Atlas source and target in both modes, including downtime and a whole runtime restart. */
@RequiresDocker
class RealAtlasToAtlasPipelineIT {

    private static final Duration TIMEOUT = Duration.ofSeconds(120);
    private static final String CONNECTOR = "mongodb-atlas";
    private static final String COLLECTION = "probe";
    private static final String SOURCE_ID = "src_atlas";
    private static final String TARGET_ID = "tgt_atlas";
    private static final String PIPELINE_ID = "atlas_to_atlas";
    private static final int SEEDED_ROWS = 32;

    @BeforeAll
    static void requireRealAtlas() {
        Assumptions.assumeTrue(System.getenv("TAPSTATE_ATLAS_TEST_URI") != null,
                "a controlled Atlas URI is required for this live witness");
        RealConnectorGate.require(CONNECTOR);
    }

    static Stream<Arguments> modesAndTiers() {
        return Arrays.stream(AtlasRuntime.Mode.values()).flatMap(mode ->
                Arrays.stream(Tiers.values()).map(tier -> Arguments.of(mode, tier)));
    }

    @ParameterizedTest(name = "{0}/{1}")
    @MethodSource("modesAndTiers")
    void pipelineReadsAndWritesAtlasAcrossAWholeRuntimeRestart(AtlasRuntime.Mode mode, Tiers tier) throws Exception {
        String baseUri = System.getenv("TAPSTATE_ATLAS_TEST_URI");
        String suffix = (mode == AtlasRuntime.Mode.CLOUD ? "cl" : "op") + "_"
                + (tier == Tiers.REAL_PROCESS ? "jvm" : "ip") + "_"
                + UUID.randomUUID().toString().substring(0, 8);
        String sourceDatabase = "ts_plan_as_" + suffix;
        String targetDatabase = "ts_plan_at_" + suffix;
        String metadataDatabase = "ts_plan_am_" + suffix;
        String sourceUri = inDatabase(baseUri, sourceDatabase);
        String targetUri = inDatabase(baseUri, targetDatabase);
        String storeUri = SharedMongo.replicaSetUrl(metadataDatabase);

        try (MongoClient source = MongoClients.create(sourceUri);
             MongoClient target = MongoClients.create(targetUri);
             AutoCloseable sourceCleanup = () -> source.getDatabase(sourceDatabase).drop();
             AutoCloseable targetCleanup = () -> target.getDatabase(targetDatabase).drop();
             MongoClient rawMetadata = MongoClients.create(storeUri);
             AtlasRuntime runtime = new AtlasRuntime(mode, storeUri)) {
            MongoCollection<Document> sourceRows = source.getDatabase(sourceDatabase).getCollection(COLLECTION);
            MongoCollection<Document> targetRows = target.getDatabase(targetDatabase).getCollection(COLLECTION);
            List<Document> seeded = new java.util.ArrayList<>(List.of(
                    new Document("_id", "first").append("value", 11),
                    new Document("_id", "second").append("value", 22)));
            for (int index = 2; index < SEEDED_ROWS; index++) {
                seeded.add(new Document("_id", "seed-" + index).append("value", index));
            }
            sourceRows.insertMany(seeded);

            String acknowledged;
            String keyId;
            Map<String, Long> beforeShutdown;
            try (ServerHandle server = runtime.launch(tier)) {
                ControlPlane control = runtime.control(server, true);
                runtime.register(control, CONNECTOR);
                Map<String, String> resources = new LinkedHashMap<>();
                resources.put("source.tap.yml", sourceYaml(sourceUri));
                resources.put("target.tap.yml", targetYaml(targetUri));
                resources.put("pipeline.tap.yml", pipelineYaml());
                control.apply(resources);
                control.discoverSchema(SOURCE_ID, CONNECTOR,
                        Map.of("isUri", true, "uri", sourceUri));
                control.lifecycle(PIPELINE_ID, LifecycleVerb.START);

                Await.until("Atlas snapshot rows to reach the target", TIMEOUT,
                        () -> targetRows.countDocuments() == SEEDED_ROWS,
                        () -> "target count=" + targetRows.countDocuments());
                assertThat(value(targetRows, "first")).isEqualTo(11L);
                assertThat(value(targetRows, "second")).isEqualTo(22L);
                assertThat(rows(targetRows)).isEqualTo(rows(sourceRows));
                String snapshotAck = Await.answered("Atlas snapshot target ACK", TIMEOUT,
                        () -> control.durablePosition(PIPELINE_ID, COLLECTION));

                sourceRows.insertOne(new Document("_id", "third").append("value", 33));
                Await.until("Atlas insert to reach the target", TIMEOUT,
                        () -> value(targetRows, "third") == 33L, () -> "the inserted row is absent");
                String insertedAck = Await.answered("Atlas insert target ACK", TIMEOUT,
                        () -> control.durablePosition(PIPELINE_ID, COLLECTION)
                                .filter(position -> !position.equals(snapshotAck)));
                sourceRows.updateOne(Filters.eq("_id", "first"), Updates.set("value", 111));
                Await.until("Atlas update to reach the target", TIMEOUT,
                        () -> value(targetRows, "first") == 111L, () -> "the updated row is absent");
                String updatedAck = Await.answered("Atlas update target ACK", TIMEOUT,
                        () -> control.durablePosition(PIPELINE_ID, COLLECTION)
                                .filter(position -> !position.equals(insertedAck)));
                sourceRows.deleteOne(Filters.eq("_id", "second"));
                Await.until("Atlas CDC insert, update, and delete to reach the target", TIMEOUT,
                        () -> targetRows.countDocuments() == SEEDED_ROWS
                                && value(targetRows, "first") == 111L
                                && value(targetRows, "third") == 33L
                                && targetRows.find(Filters.eq("_id", "second")).first() == null,
                        () -> "target count=" + targetRows.countDocuments()
                                + ", first=" + value(targetRows, "first")
                                + ", third=" + value(targetRows, "third"));
                Await.until("Atlas target to durably acknowledge the CDC writes", TIMEOUT,
                        () -> control.durablePosition(PIPELINE_ID, COLLECTION)
                                .filter(position -> !position.equals(updatedAck)).isPresent(),
                        () -> "the target ACK has not covered the final live delete");
                acknowledged = control.durablePosition(PIPELINE_ID, COLLECTION).orElseThrow();
                assertThat(rows(targetRows)).isEqualTo(rows(sourceRows));
                beforeShutdown = rows(targetRows);
                keyId = keyId(rawMetadata, metadataDatabase);
                assertThat(control.snapshotRowsRead(PIPELINE_ID)).containsEntry(COLLECTION, (long) SEEDED_ROWS);
            }

            Instant stoppedAt = Instant.now();
            sourceRows.insertOne(new Document("_id", "fourth").append("value", 44));
            sourceRows.updateOne(Filters.eq("_id", "first"), Updates.set("value", 222));
            sourceRows.deleteOne(Filters.eq("_id", "third"));
            assertThat(rows(targetRows)).as("nothing carries the downtime changes while the runtime is gone")
                    .isEqualTo(beforeShutdown);

            // No apply/register/START: desired state, keyring, schemas and ACK must survive the JVM.
            try (ServerHandle server = runtime.launch(tier)) {
                ControlPlane control = runtime.control(server, false);
                Await.until("Atlas downtime insert, update and delete to be replayed", TIMEOUT,
                        () -> rows(targetRows).equals(rows(sourceRows)),
                        () -> "target count=" + targetRows.countDocuments());
                assertThat(keyId(rawMetadata, metadataDatabase)).isEqualTo(keyId);
                String replayAck = Await.answered("Atlas downtime target ACK", TIMEOUT,
                        () -> control.durablePosition(PIPELINE_ID, COLLECTION)
                                .filter(position -> !position.equals(acknowledged)));
                sourceRows.insertOne(new Document("_id", "fifth").append("value", 55));
                Await.until("a new Atlas CDC row to cross after the runtime restart", TIMEOUT,
                        () -> targetRows.countDocuments() == SEEDED_ROWS + 1 && value(targetRows, "fifth") == 55L,
                        () -> "target count=" + targetRows.countDocuments()
                                + ", fifth=" + value(targetRows, "fifth"));
                Await.until("Atlas target ACK and current observation to advance after restart", TIMEOUT,
                        () -> control.durablePosition(PIPELINE_ID, COLLECTION)
                                .filter(position -> !position.equals(replayAck)).isPresent()
                                && control.observedAt(PIPELINE_ID).filter(time -> time.isAfter(stoppedAt)).isPresent(),
                        () -> "target ACK did not advance after the resumed CDC write");
                AtlasRuntime.assertResumedRun(control, PIPELINE_ID, COLLECTION, SEEDED_ROWS, TIMEOUT);
                assertThat(rows(targetRows)).isEqualTo(rows(sourceRows));
                assertThat(targetRows.find(Filters.eq("_id", "second")).first()).isNull();
                assertThat(targetRows.find(Filters.eq("_id", "third")).first()).isNull();
                runtime.assertAuthenticationBoundary();
            }
        }
    }

    private static Map<String, Long> rows(MongoCollection<Document> collection) {
        Map<String, Long> rows = new TreeMap<>();
        for (Document row : collection.find()) rows.put(row.getString("_id"), ((Number) row.get("value")).longValue());
        return rows;
    }

    private static String keyId(MongoClient raw, String database) {
        Document keyring = SystemCollections.SYSTEM_META.on(raw.getDatabase(database))
                .find(new Document("_id", "source-config-keyring"))
                .projection(new Document("activeKeyId", 1).append("_id", 0)).first();
        assertThat(keyring != null && keyring.getString("activeKeyId") != null)
                .as("the runtime persisted its shared config key identity").isTrue();
        return keyring.getString("activeKeyId");
    }

    private static long value(MongoCollection<Document> collection, String id) {
        Document found = collection.find(Filters.eq("_id", id)).first();
        return found == null ? Long.MIN_VALUE : ((Number) found.get("value")).longValue();
    }

    private static String sourceYaml(String uri) {
        return """
                version: tapstate/v1
                kind: source
                id: %s
                connector: %s
                config: %s
                mode: cdc
                tables: [ %s ]
                """.formatted(SOURCE_ID, CONNECTOR, JsonWriter.write(Map.of("isUri", true, "uri", uri)), COLLECTION);
    }

    private static String targetYaml(String uri) {
        return """
                version: tapstate/v1
                kind: source
                id: %s
                connector: %s
                config: %s
                """.formatted(TARGET_ID, CONNECTOR, JsonWriter.write(Map.of("isUri", true, "uri", uri)));
    }

    private static String pipelineYaml() {
        return """
                version: tapstate/v1
                kind: pipeline
                id: %s
                source: %s
                settings: { read_mode: snapshot_and_cdc }
                transforms:
                  - { id: all_rows, from: [%s], type: filter, expr: "true" }
                serve:
                  from: all_rows
                  sync:
                    - source: %s
                """.formatted(PIPELINE_ID, SOURCE_ID, COLLECTION, TARGET_ID);
    }

    private static String inDatabase(String uri, String database) {
        int schemeEnd = uri.indexOf("://");
        int slash = schemeEnd < 0 ? -1 : uri.indexOf('/', schemeEnd + 3);
        if (slash < 0) {
            throw new IllegalArgumentException("Atlas test URI must include a database path");
        }
        int options = uri.indexOf('?', slash);
        return uri.substring(0, slash + 1) + database
                + (options < 0 ? "" : uri.substring(options));
    }
}
