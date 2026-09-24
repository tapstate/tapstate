package io.tapstate.e2e;

import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.model.Filters;
import com.mongodb.client.model.Updates;
import io.tapstate.core.lifecycle.LifecycleVerb;
import io.tapstate.core.lifecycle.PipelineState;
import io.tapstate.testsupport.RequiresDocker;
import org.bson.Document;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

import java.time.Duration;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;

/** Real Atlas source and target through the on-prem control and Pipeline runtime. */
@RequiresDocker
class RealAtlasToAtlasPipelineIT {

    private static final Duration TIMEOUT = Duration.ofSeconds(120);
    private static final String CONNECTOR = "mongodb-atlas";
    private static final String COLLECTION = "probe";
    private static final String SOURCE_ID = "src_atlas";
    private static final String TARGET_ID = "tgt_atlas";
    private static final String PIPELINE_ID = "atlas_to_atlas";

    @BeforeAll
    static void requireRealAtlas() {
        Assumptions.assumeTrue(System.getenv("TAPSTATE_ATLAS_TEST_URI") != null,
                "a controlled Atlas URI is required for this live witness");
        RealConnectorGate.require(CONNECTOR);
    }

    @ParameterizedTest
    @EnumSource(Tiers.class)
    void onPremPipelineReadsAndWritesAtlasAcrossARestart(Tiers tier) {
        String baseUri = System.getenv("TAPSTATE_ATLAS_TEST_URI");
        String suffix = tier.name().toLowerCase(Locale.ROOT) + "_"
                + UUID.randomUUID().toString().substring(0, 8);
        String sourceDatabase = "ts_plan_e2e_src_" + suffix;
        String targetDatabase = "ts_plan_e2e_tgt_" + suffix;
        String sourceUri = inDatabase(baseUri, sourceDatabase);
        String targetUri = inDatabase(baseUri, targetDatabase);
        String storeUri = SharedMongo.replicaSetUrl("ts_plan_e2e_store_" + suffix);

        try (MongoClient source = MongoClients.create(sourceUri);
             MongoClient target = MongoClients.create(targetUri)) {
            try {
                MongoCollection<Document> sourceRows = source.getDatabase(sourceDatabase).getCollection(COLLECTION);
                MongoCollection<Document> targetRows = target.getDatabase(targetDatabase).getCollection(COLLECTION);
                sourceRows.insertMany(List.of(
                        new Document("_id", "first").append("value", 11),
                        new Document("_id", "second").append("value", 22)));

                try (ServerHandle server = tier.launch(storeUri)) {
                    ControlPlane control = new ControlPlane(server.baseUrl());
                    control.bootstrapAndLogin("e2e", "e2e-password");
                    control.registerConnector(CONNECTOR, ConnectorJars.bytesFor(CONNECTOR));
                    Map<String, String> resources = new LinkedHashMap<>();
                    resources.put("source.tap.yml", sourceYaml(sourceUri));
                    resources.put("target.tap.yml", targetYaml(targetUri));
                    resources.put("pipeline.tap.yml", pipelineYaml());
                    control.apply(resources);
                    control.discoverSchema(SOURCE_ID, CONNECTOR,
                            Map.of("isUri", true, "uri", sourceUri));
                    control.lifecycle(PIPELINE_ID, LifecycleVerb.START);

                    Await.until("Atlas snapshot rows to reach the target", TIMEOUT,
                            () -> targetRows.countDocuments() == 2,
                            () -> "target count=" + targetRows.countDocuments());
                    assertThat(value(targetRows, "first")).isEqualTo(11L);
                    assertThat(value(targetRows, "second")).isEqualTo(22L);

                    sourceRows.insertOne(new Document("_id", "third").append("value", 33));
                    sourceRows.updateOne(Filters.eq("_id", "first"), Updates.set("value", 111));
                    sourceRows.deleteOne(Filters.eq("_id", "second"));
                    Await.until("Atlas CDC insert, update, and delete to reach the target", TIMEOUT,
                            () -> targetRows.countDocuments() == 2
                                    && value(targetRows, "first") == 111L
                                    && value(targetRows, "third") == 33L
                                    && targetRows.find(Filters.eq("_id", "second")).first() == null,
                            () -> "target count=" + targetRows.countDocuments()
                                    + ", first=" + value(targetRows, "first")
                                    + ", third=" + value(targetRows, "third"));

                    control.stop(PIPELINE_ID, false);
                    Await.until("Atlas Pipeline to stop", TIMEOUT,
                            () -> control.state(PIPELINE_ID).filter(PipelineState.STOPPED::equals).isPresent(),
                            () -> String.valueOf(control.state(PIPELINE_ID)));
                    control.lifecycle(PIPELINE_ID, LifecycleVerb.START);
                    sourceRows.insertOne(new Document("_id", "fourth").append("value", 44));
                    Await.until("Atlas Pipeline to resume CDC after restart", TIMEOUT,
                            () -> targetRows.countDocuments() == 3 && value(targetRows, "fourth") == 44L,
                            () -> "target count=" + targetRows.countDocuments()
                                    + ", fourth=" + value(targetRows, "fourth"));
                    assertThat(targetRows.find(Filters.eq("_id", "second")).first()).isNull();
                    assertThat(value(targetRows, "first")).isEqualTo(111L);
                    assertThat(value(targetRows, "third")).isEqualTo(33L);
                }
            } finally {
                source.getDatabase(sourceDatabase).drop();
                target.getDatabase(targetDatabase).drop();
            }
        }
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
                config: { isUri: true, uri: "%s" }
                mode: cdc
                tables: [ %s ]
                """.formatted(SOURCE_ID, CONNECTOR, uri, COLLECTION);
    }

    private static String targetYaml(String uri) {
        return """
                version: tapstate/v1
                kind: source
                id: %s
                connector: %s
                config: { isUri: true, uri: "%s" }
                """.formatted(TARGET_ID, CONNECTOR, uri);
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
