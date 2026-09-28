package io.tapstate.e2e;

import static org.assertj.core.api.Assertions.assertThat;

import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import io.tapstate.core.lifecycle.LifecycleVerb;
import io.tapstate.core.lifecycle.PipelineState;
import io.tapstate.testsupport.DockerGate;
import java.time.Duration;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import org.bson.Document;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

/**
 * A cdc-only pipeline started from the present, over a source another pipeline is already tailing,
 * receives the changes made after it started and none of the ones made before.
 *
 * <p>The source is read once and its changes are kept in a buffer every pipeline over it reads from. That
 * buffer outlives any one reader, so when this pipeline arrives it already holds the changes the first
 * pipeline was sent. Read from its beginning, the newcomer is handed all of them -- history from before it
 * existed, which a read "from the present" is exactly the request not to receive.
 *
 * <p><b>What it arrives into is made certain first.</b> The first pipeline has written every earlier change
 * to its own target before the second one starts, so each of them is in the buffer by then: an absence of
 * them at the second target is then the second pipeline's behaviour, not the buffer's. And the second
 * pipeline has to be seen carrying something, or an empty target would pass the absence check too -- so
 * changes keep being written after it starts until one of them arrives. Writing more than one is fine: a
 * pipeline marked RUNNING is not necessarily on the buffer yet, and a change made before it is on it is not
 * one it was asked for.
 */
class ACdcOnlyPipelineJoiningALiveTailReceivesOnlyWhatComesAfterItIT {

    private static final Duration TIMEOUT = Duration.ofSeconds(90);
    private static final String SOURCE = "live_tail_source";
    private static final String FIRST_TARGET = "live_tail_first_target";
    private static final String SECOND_TARGET = "live_tail_second_target";
    private static final String FIRST_PIPELINE = "live_tail_first";
    private static final String SECOND_PIPELINE = "live_tail_cdc_only";
    private static final String COLLECTION = "orders";
    private static final String BEFORE = "before-the-second-pipeline";
    private static final String AFTER = "after-the-second-pipeline";
    private static final int WRITTEN_BEFORE = 3;
    /** How often a further change is written while the second pipeline has yet to carry one. */
    private static final Duration WRITE_EVERY = Duration.ofSeconds(2);

    @BeforeAll
    static void requireDockerAndTheRealConnector() {
        DockerGate.require();
        RealConnectorGate.require("mongodb");
    }

    @ParameterizedTest
    @EnumSource(Tiers.class)
    void itReceivesWhatIsWrittenAfterItStartsAndNothingFromBefore(Tiers tier) throws Exception {
        String suffix = tier.name().toLowerCase(Locale.ROOT);
        String sourceDatabase = "live_tail_source_" + suffix;
        String sourceUri = SharedMongo.replicaSetUrl(sourceDatabase);
        String firstTargetUri = SharedMongo.replicaSetUrl("live_tail_first_target_" + suffix);
        String secondTargetUri = SharedMongo.replicaSetUrl("live_tail_second_target_" + suffix);

        try (MongoClient source = MongoClients.create(sourceUri);
                MongoEndpoints mongo = new MongoEndpoints();
                ServerHandle server = tier.launch(SharedMongo.replicaSetUrl("live_tail_store_" + suffix))) {
            source.getDatabase(sourceDatabase).getCollection(COLLECTION).drop();
            insert(source, sourceDatabase, 1, "seeded");
            ControlPlane control = new ControlPlane(server.baseUrl());
            control.bootstrapAndLogin("e2e", "e2e-password");
            control.registerConnector("mongodb", ConnectorJars.bytesFor("mongodb"));
            control.apply(resources(sourceUri, firstTargetUri, secondTargetUri));
            control.discoverSchema(SOURCE, "mongodb", Map.of("uri", sourceUri, "database", sourceDatabase));

            EndpointAddress firstTarget = EndpointAddress.uri(firstTargetUri);
            EndpointAddress secondTarget = EndpointAddress.uri(secondTargetUri);
            control.lifecycle(FIRST_PIPELINE, LifecycleVerb.START);
            awaitState(control, FIRST_PIPELINE, PipelineState.RUNNING);
            Await.until("the first pipeline to land the seeded row", TIMEOUT,
                    () -> names(mongo, firstTarget).contains("seeded"),
                    () -> names(mongo, firstTarget).toString());
            for (int i = 1; i <= WRITTEN_BEFORE; i++) {
                insert(source, sourceDatabase, 1 + i, BEFORE);
            }
            Await.until("every change made before the second pipeline to reach the first one's target", TIMEOUT,
                    () -> count(names(mongo, firstTarget), BEFORE) == WRITTEN_BEFORE,
                    () -> names(mongo, firstTarget).toString());

            control.lifecycle(SECOND_PIPELINE, LifecycleVerb.START);
            awaitState(control, SECOND_PIPELINE, PipelineState.RUNNING);
            int[] nextId = {100};
            long[] lastWrite = {System.nanoTime() - WRITE_EVERY.toNanos()};
            Await.until("a change made after the second pipeline started to reach its target", TIMEOUT,
                    () -> {
                        if (count(names(mongo, secondTarget), AFTER) > 0) {
                            return true;
                        }
                        if (System.nanoTime() - lastWrite[0] >= WRITE_EVERY.toNanos()) {
                            insert(source, sourceDatabase, nextId[0]++, AFTER);
                            lastWrite[0] = System.nanoTime();
                        }
                        return false;
                    },
                    () -> "rows=" + names(mongo, secondTarget) + ", state=" + control.state(SECOND_PIPELINE)
                            + ", logs=" + control.logs(SECOND_PIPELINE));

            assertThat(names(mongo, secondTarget))
                    .as("the cdc-only pipeline carries what was written after it started and nothing the "
                            + "buffer held from before it existed")
                    .doesNotContain(BEFORE, "seeded")
                    .contains(AFTER);
            assertThat(control.errorCount(SECOND_PIPELINE)).contains(0L);
        }
    }

    private static Map<String, String> resources(String sourceUri, String firstTargetUri, String secondTargetUri) {
        Map<String, String> resources = new LinkedHashMap<>();
        resources.put(SOURCE + ".tap.yml", """
                version: tapstate/v1
                kind: source
                id: %s
                connector: mongodb
                config: { uri: "%s" }
                mode: cdc
                tables: [ %s ]
                """.formatted(SOURCE, sourceUri, COLLECTION));
        resources.put(FIRST_TARGET + ".tap.yml", target(FIRST_TARGET, firstTargetUri));
        resources.put(SECOND_TARGET + ".tap.yml", target(SECOND_TARGET, secondTargetUri));
        resources.put(FIRST_PIPELINE + ".tap.yml", pipeline(FIRST_PIPELINE, "snapshot_and_cdc", FIRST_TARGET));
        resources.put(SECOND_PIPELINE + ".tap.yml", pipeline(SECOND_PIPELINE, "cdc_only", SECOND_TARGET));
        return resources;
    }

    private static String target(String targetId, String targetUri) {
        return """
                version: tapstate/v1
                kind: source
                id: %s
                connector: mongodb
                config: { uri: "%s" }
                """.formatted(targetId, targetUri);
    }

    private static String pipeline(String pipelineId, String readMode, String targetId) {
        return """
                version: tapstate/v1
                kind: pipeline
                id: %s
                source: %s
                settings: { read_mode: %s }
                transforms:
                  - { id: all_rows, from: [%s], type: filter, expr: "true" }
                serve:
                  from: all_rows
                  sync:
                    - source: %s
                """.formatted(pipelineId, SOURCE, readMode, COLLECTION, targetId);
    }

    private static void insert(MongoClient source, String database, int id, String name) {
        source.getDatabase(database).getCollection(COLLECTION)
                .insertOne(new Document("_id", id).append("oid", id).append("name", name));
    }

    private static List<String> names(MongoEndpoints mongo, EndpointAddress target) {
        return mongo.documents(target, COLLECTION).stream()
                .map(document -> document.getString("name"))
                .sorted()
                .toList();
    }

    private static long count(List<String> names, String name) {
        return names.stream().filter(name::equals).count();
    }

    private static void awaitState(ControlPlane control, String pipelineId, PipelineState expected) {
        Await.until(pipelineId + " to reach " + expected, TIMEOUT,
                () -> control.state(pipelineId).filter(expected::equals).isPresent(),
                () -> String.valueOf(control.state(pipelineId)));
    }
}
