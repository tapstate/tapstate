package io.tapstate.e2e;

import static org.assertj.core.api.Assertions.assertThat;

import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import io.tapstate.core.lifecycle.LifecycleVerb;
import io.tapstate.core.lifecycle.PipelineState;
import io.tapstate.testsupport.DockerGate;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import org.bson.Document;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

/**
 * A CDC-only pipeline starts at its own present instead of another pipeline's snapshot seam.
 *
 * <p>The first pipeline creates the shared mining-chain record and records a snapshot seam, then stops
 * before its tail sees a change. A row written after that stream has returned and before the CDC-only
 * pipeline starts is the discriminator: replaying the first pipeline's seam delivers it, while starting
 * at the CDC-only pipeline's own present does not. A second row written after the new stream opens must
 * arrive, so an empty target or a tail that never started cannot satisfy the absence assertion.
 *
 * <p>The two sources deliberately have different artifact ids and table selections but identical physical
 * Mongo coordinates. Mining-chain identity excludes the table subset, so they meet in the one persisted
 * chain whose pipeline-scoped seam is under test. The observed real connector is used only to place both
 * writes on known sides of stream teardown and startup; the result is read independently from the target.
 */
class ACdcOnlyPipelineDoesNotBorrowAnotherPipelinesSnapshotSeamIT {

    private static final Duration TIMEOUT = Duration.ofSeconds(90);
    private static final String FIRST_SOURCE = "seam_owner_source";
    private static final String SECOND_SOURCE = "cdc_only_source";
    private static final String FIRST_TARGET = "seam_owner_target";
    private static final String SECOND_TARGET = "cdc_only_target";
    private static final String FIRST_PIPELINE = "seam_owner_pipeline";
    private static final String SECOND_PIPELINE = "cdc_only_pipeline";
    private static final String FIRST_COLLECTION = "seam_owner_orders";
    private static final String SECOND_COLLECTION = "cdc_only_orders";
    private static final String BEFORE_START = "written-before-cdc-only-start";
    private static final String AFTER_START = "written-after-cdc-only-start";

    @BeforeAll
    static void requireDockerAndTheRealConnector() {
        DockerGate.require();
        RealConnectorGate.require("mongodb");
    }

    @ParameterizedTest
    @EnumSource(Tiers.class)
    void aCdcOnlyPipelineDoesNotReplayAChangeFromBeforeItsOwnStart(
            Tiers tier, @TempDir Path temporary) throws Exception {
        String suffix = tier.name().toLowerCase(Locale.ROOT);
        String sourceDatabase = "cdc_own_start_source_" + suffix;
        String sourceUri = SharedMongo.replicaSetUrl(sourceDatabase);
        String targetUri = SharedMongo.replicaSetUrl("cdc_own_start_target_" + suffix);
        Path witness = temporary.resolve("mongo-writes");
        Path tail = temporary.resolve("mongo-writes.tail");
        byte[] connector = ObservedMongoConnectorJar.build(ConnectorJars.bytesFor("mongodb"), witness);

        try (MongoClient source = MongoClients.create(sourceUri);
                MongoEndpoints mongo = new MongoEndpoints();
                ServerHandle server = tier.launch(
                        SharedMongo.replicaSetUrl("cdc_own_start_store_" + suffix))) {
            seed(source, sourceDatabase, FIRST_COLLECTION, "snapshot-row");
            ControlPlane control = connected(server, connector);

            control.apply(workspace(
                    FIRST_SOURCE, FIRST_TARGET, FIRST_PIPELINE, FIRST_COLLECTION,
                    "snapshot_and_cdc", sourceUri, targetUri));
            control.discoverSchema(
                    FIRST_SOURCE, "mongodb", Map.of("uri", sourceUri, "database", sourceDatabase));
            control.lifecycle(FIRST_PIPELINE, LifecycleVerb.START);

            EndpointAddress target = EndpointAddress.uri(targetUri);
            Await.until("the seam-owning pipeline to land its snapshot", TIMEOUT,
                    () -> names(mongo, target, FIRST_COLLECTION).contains("snapshot-row"),
                    () -> names(mongo, target, FIRST_COLLECTION).toString());
            Await.until("the seam-owning pipeline's tail to open", TIMEOUT,
                    () -> tailLines(tail).size() == 1 && tailLines(tail).getFirst().startsWith("START "),
                    () -> tailLines(tail).toString());

            control.stop(FIRST_PIPELINE, false);
            awaitState(control, FIRST_PIPELINE, PipelineState.STOPPED);
            Await.until("the seam-owning pipeline's tail to return", TIMEOUT,
                    () -> tailLines(tail).size() == 2 && tailLines(tail).getLast().equals("END"),
                    () -> tailLines(tail).toString());

            // No stream is open here. This row belongs before the second pipeline's own present, but
            // after the older seam that pipeline must not borrow.
            seed(source, sourceDatabase, SECOND_COLLECTION, BEFORE_START);
            control.apply(workspace(
                    SECOND_SOURCE, SECOND_TARGET, SECOND_PIPELINE, SECOND_COLLECTION,
                    "cdc_only", sourceUri, targetUri));
            control.discoverSchema(
                    SECOND_SOURCE, "mongodb", Map.of("uri", sourceUri, "database", sourceDatabase));
            control.lifecycle(SECOND_PIPELINE, LifecycleVerb.START);
            awaitState(control, SECOND_PIPELINE, PipelineState.RUNNING);
            Await.until("the CDC-only pipeline's tail to open", TIMEOUT,
                    () -> tailLines(tail).size() == 3 && tailLines(tail).getLast().startsWith("START "),
                    () -> tailLines(tail).toString());

            insert(source, sourceDatabase, SECOND_COLLECTION, 2, AFTER_START);
            Await.until("the change written after the CDC-only start to reach the target", TIMEOUT,
                    () -> names(mongo, target, SECOND_COLLECTION).contains(AFTER_START),
                    () -> "rows=" + names(mongo, target, SECOND_COLLECTION)
                            + ", state=" + control.state(SECOND_PIPELINE)
                            + ", logs=" + control.logs(SECOND_PIPELINE));

            assertThat(names(mongo, target, SECOND_COLLECTION))
                    .as("the live CDC-only tail carries its post-start row without replaying the row "
                            + "written before that pipeline existed")
                    .containsExactly(AFTER_START);
            assertThat(control.errorCount(SECOND_PIPELINE)).contains(0L);
        }
    }

    private static ControlPlane connected(ServerHandle server, byte[] connector) {
        ControlPlane control = new ControlPlane(server.baseUrl());
        control.bootstrapAndLogin("e2e", "e2e-password");
        control.registerConnector("mongodb", connector);
        return control;
    }

    private static Map<String, String> workspace(
            String sourceId,
            String targetId,
            String pipelineId,
            String collection,
            String readMode,
            String sourceUri,
            String targetUri) {
        Map<String, String> resources = new LinkedHashMap<>();
        resources.put(sourceId + ".tap.yml", sourceYaml(sourceId, collection, sourceUri));
        resources.put(targetId + ".tap.yml", targetYaml(targetId, targetUri));
        resources.put(pipelineId + ".tap.yml",
                pipelineYaml(pipelineId, sourceId, targetId, collection, readMode));
        return resources;
    }

    private static String sourceYaml(String sourceId, String collection, String sourceUri) {
        return """
                version: tapstate/v1
                kind: source
                id: %s
                connector: mongodb
                config: { uri: "%s" }
                mode: cdc
                tables: [ %s ]
                """.formatted(sourceId, sourceUri, collection);
    }

    private static String targetYaml(String targetId, String targetUri) {
        return """
                version: tapstate/v1
                kind: source
                id: %s
                connector: mongodb
                config: { uri: "%s" }
                """.formatted(targetId, targetUri);
    }

    private static String pipelineYaml(
            String pipelineId, String sourceId, String targetId, String collection, String readMode) {
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
                """.formatted(pipelineId, sourceId, readMode, collection, targetId);
    }

    private static void seed(
            MongoClient source, String database, String collection, String name) {
        source.getDatabase(database).getCollection(collection).drop();
        insert(source, database, collection, 1, name);
    }

    private static void insert(
            MongoClient source, String database, String collection, int id, String name) {
        source.getDatabase(database).getCollection(collection)
                .insertOne(new Document("_id", id).append("oid", id).append("name", name));
    }

    private static List<String> names(
            MongoEndpoints mongo, EndpointAddress target, String collection) {
        return mongo.documents(target, collection).stream()
                .map(document -> document.getString("name"))
                .sorted()
                .toList();
    }

    private static List<String> tailLines(Path tail) {
        try {
            return Files.exists(tail) ? Files.readAllLines(tail) : List.of();
        } catch (java.io.IOException failure) {
            throw new java.io.UncheckedIOException(failure);
        }
    }

    private static void awaitState(
            ControlPlane control, String pipelineId, PipelineState expected) {
        Await.until(pipelineId + " to reach " + expected, TIMEOUT,
                () -> control.state(pipelineId).filter(expected::equals).isPresent(),
                () -> String.valueOf(control.state(pipelineId)));
    }
}
