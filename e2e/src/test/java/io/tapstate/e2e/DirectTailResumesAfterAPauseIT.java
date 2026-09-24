package io.tapstate.e2e;

import com.mongodb.ConnectionString;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import io.tapstate.core.lifecycle.LifecycleVerb;
import io.tapstate.core.lifecycle.PipelineState;
import io.tapstate.testsupport.DockerGate;

import org.bson.Document;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Direct Mongo tails retain changes across a pause and across a completed stop/start, with the shared
 * replay buffer disabled. A delete distinguishes CDC replay from a repeated snapshot: an upsert-only
 * load cannot remove a row that is already in the target.
 *
 * <p>A stop response acknowledges intent. The stop-window case waits for STOPPED and observes the
 * connector's stream returning before deleting, so the old stream cannot capture the change. It also
 * observes the start actually handed to the next stream: Mongo must receive a resume token, not a new
 * timestamp. The pause case covers delivery across a hold, without claiming that a hold ends a stream.
 *
 * <p>Both cases run in-process and against the packaged server, using real Mongo and its unchanged
 * connector jar. Java is needed because the declarative vocabulary cannot observe connector starts
 * and teardown. Requires Docker, {@code -Dtapstate.e2e.connectors-dir}, and the app module packaged.
 */
class DirectTailResumesAfterAPauseIT {

    @ParameterizedTest
    @EnumSource(Tiers.class)
    void aDirectTailResumesADeleteMadeAfterItsPreviousStreamHasEnded(
            Tiers tier, @TempDir Path temporary) throws Exception {
        String suffix = "direct_tail_cycled_" + tier.name().toLowerCase(Locale.ROOT);
        String database = suffix + "_src";
        String sourceUri = SharedMongo.replicaSetUrl(database);
        String targetUri = SharedMongo.replicaSetUrl(suffix + "_tgt");
        Path witness = temporary.resolve("mongo-writes");
        Path tail = temporary.resolve("mongo-writes.tail");
        byte[] connector = ObservedMongoConnectorJar.build(ConnectorJars.bytesFor("mongodb"), witness);
        EndpointAddress target = EndpointAddress.uri(targetUri);
        try (MongoClient source = MongoClients.create(sourceUri);
                MongoEndpoints mongo = new MongoEndpoints();
                ServerHandle server = tier.launch(SharedMongo.replicaSetUrl(suffix + "_state"))) {
            seed(source, database);
            ControlPlane control = start(server, suffix, sourceUri, targetUri, connector);
            awaitCount(mongo, target, "the seeded load");
            rename(source, database, CHANGED_BEFORE, BEFORE_PAUSE);
            awaitName(mongo, target, CHANGED_BEFORE, BEFORE_PAUSE, "the running tail");
            control.stop(suffix, false);
            // The stop response acknowledges intent; only the observed state confirms teardown.
            awaitState(control, suffix, PipelineState.STOPPED);
            Await.until("the first connector stream to return", TIMEOUT,
                    () -> tailLines(tail).contains("END"), () -> tailLines(tail).toString());
            assertThat(tailLines(tail)).as("one connector stream must end before the stopped-window delete")
                    .hasSize(2);
            assertThat(tailLines(tail).getLast()).isEqualTo("END");
            assertThat(namesOf(mongo, target, DELETED_WHILE_PAUSED)).containsExactly("order-2");
            delete(source, database, DELETED_WHILE_PAUSED);
            control.lifecycle(suffix, LifecycleVerb.START);
            awaitState(control, suffix, PipelineState.RUNNING);
            Await.until("a second connector stream start after stop/start", TIMEOUT,
                    () -> tailLines(tail).stream().filter(line -> line.startsWith("START ")).count() == 2,
                    () -> tailLines(tail).toString());
            List<String> observations = tailLines(tail);
            System.out.println("Direct tail restart observations (" + tier + "): " + observations);
            assertThat(observations).as("the restarted stream must follow the first stream's return")
                    .hasSize(3);
            assertThat(observations.get(1)).isEqualTo("END");
            // Golden shape of this real connector's opaque resume token. A freshly sampled timestamp
            // can overlap a recent delete, so target contents alone do not prove a recorded start.
            assertThat(observations.get(2))
                    .as("the restarted connector must receive a recorded Mongo resume token")
                    .startsWith("START {\"_data\":");
            rename(source, database, CHANGED_BEFORE, "changed-after-restart");
            awaitName(mongo, target, CHANGED_BEFORE, "changed-after-restart", "the restarted tail");
            assertThat(namesOf(mongo, target, DELETED_WHILE_PAUSED))
                    .as("the stopped-window delete must precede the confirmed post-restart change")
                    .isEmpty();
        }
    }

    private static List<String> tailLines(Path path) {
        try {
            return Files.exists(path) ? Files.readAllLines(path) : List.of();
        } catch (java.io.IOException failure) {
            throw new java.io.UncheckedIOException(failure);
        }
    }

    private static final Duration TIMEOUT = Duration.ofSeconds(90);

    private static final long SEEDED_ROWS = 5;
    private static final String COLLECTION = "orders";
    private static final String SOURCE_ID = "src_mongo";
    private static final String TARGET_ID = "tgt_mongo";
    private static final String BEFORE_PAUSE = "changed-before-pause";
    private static final int CHANGED_BEFORE = 1;
    private static final int DELETED_WHILE_PAUSED = 2;

    @BeforeAll
    static void requireDockerAndTheRealConnector() {
        DockerGate.require();
        RealConnectorGate.require("mongodb");
    }

    @ParameterizedTest
    @EnumSource(Tiers.class)
    void aDirectTailPicksUpTheDeleteMadeWhileItWasPaused(Tiers tier) {
        String suffix = "direct_tail_" + tier.name().toLowerCase(Locale.ROOT);
        String database = suffix + "_src";
        String sourceUri = SharedMongo.replicaSetUrl(database);
        String targetUri = SharedMongo.replicaSetUrl(suffix + "_tgt");
        String storeUri = SharedMongo.replicaSetUrl(suffix + "_state");
        EndpointAddress target = EndpointAddress.uri(targetUri);

        try (MongoClient source = MongoClients.create(new ConnectionString(sourceUri));
                MongoEndpoints mongo = new MongoEndpoints();
                ServerHandle server = tier.launch(storeUri)) {

            seed(source, database);
            ControlPlane control = start(server, suffix, sourceUri, targetUri);

            awaitCount(mongo, target, "the full load of the seeded documents");
            // A change while it is running, so the run ends holding a position it confirmed rather than
            // only a finished load: carrying on has to have something to carry on from.
            rename(source, database, CHANGED_BEFORE, BEFORE_PAUSE);
            awaitName(mongo, target, CHANGED_BEFORE, BEFORE_PAUSE, "the change the running tail captured");

            control.lifecycle(suffix, LifecycleVerb.PAUSE);
            awaitState(control, suffix, PipelineState.PAUSED);

            // The change this case is about, and it is a delete for the reason the class comment gives.
            // A hold can retain its stream; only the separate stop-window case witnesses a new start.
            delete(source, database, DELETED_WHILE_PAUSED);

            control.lifecycle(suffix, LifecycleVerb.RESUME);
            awaitState(control, suffix, PipelineState.RUNNING);

            awaitGone(mongo, target, DELETED_WHILE_PAUSED);
        }
    }

    /** Everything up to and including the start. */
    private static ControlPlane start(
            ServerHandle server, String pipelineId, String sourceUri, String targetUri) {
        return start(server, pipelineId, sourceUri, targetUri, ConnectorJars.bytesFor("mongodb"));
    }

    private static ControlPlane start(
            ServerHandle server, String pipelineId, String sourceUri, String targetUri, byte[] connector) {
        ControlPlane control = new ControlPlane(server.baseUrl());
        control.bootstrapAndLogin("e2e", "e2e-password");
        control.registerConnector("mongodb", connector);

        Map<String, String> resources = new LinkedHashMap<>();
        resources.put(SOURCE_ID + ".tap.yml", sourceYaml(sourceUri, pipelineId + "_src"));
        resources.put(TARGET_ID + ".tap.yml", targetYaml(targetUri, pipelineId + "_tgt"));
        resources.put("pipeline.tap.yml", pipelineYaml(pipelineId));
        control.apply(resources);
        control.discoverSchema(SOURCE_ID, "mongodb",
                Map.of("uri", sourceUri, "database", pipelineId + "_src"));
        control.lifecycle(pipelineId, LifecycleVerb.START);
        return control;
    }

    /** The pipeline that does not buffer: the switch is off, and that is the only thing unusual here. */
    private static String pipelineYaml(String pipelineId) {
        return """
                version: tapstate/v1
                kind: pipeline
                id: %s
                source:
                  - { id: %s, srs: false }
                settings: { read_mode: snapshot_and_cdc }
                transforms:
                  - { id: all_rows, from: [%s], type: filter, expr: "true" }
                serve:
                  from: all_rows
                  sync:
                    - source: %s
                """
                .formatted(pipelineId, SOURCE_ID, COLLECTION, TARGET_ID);
    }

    private static String sourceYaml(String uri, String database) {
        return """
                version: tapstate/v1
                kind: source
                id: %s
                connector: mongodb
                config: { uri: "%s", database: %s }
                mode: cdc
                tables: [ %s ]
                """
                .formatted(SOURCE_ID, uri, database, COLLECTION);
    }

    private static String targetYaml(String uri, String database) {
        return """
                version: tapstate/v1
                kind: source
                id: %s
                connector: mongodb
                config: { uri: "%s", database: %s }
                """
                .formatted(TARGET_ID, uri, database);
    }

    /** Written by a driver of the database rather than through any face of the product. */
    private static void seed(MongoClient client, String database) {
        client.getDatabase(database).getCollection(COLLECTION).drop();
        for (int id = 1; id <= SEEDED_ROWS; id++) {
            client.getDatabase(database).getCollection(COLLECTION)
                    .insertOne(new Document("_id", id).append("oid", id).append("name", "order-" + id));
        }
    }

    private static void rename(MongoClient client, String database, int id, String name) {
        client.getDatabase(database).getCollection(COLLECTION)
                .updateOne(new Document("_id", id), new Document("$set", new Document("name", name)));
    }

    private static void delete(MongoClient client, String database, int id) {
        client.getDatabase(database).getCollection(COLLECTION).deleteOne(new Document("_id", id));
    }

    private static void awaitState(ControlPlane control, String pipelineId, PipelineState expected) {
        Await.until("%s to reach %s".formatted(pipelineId, expected), TIMEOUT,
                () -> control.state(pipelineId).filter(expected::equals).isPresent(),
                () -> String.valueOf(control.state(pipelineId)));
    }

    private static void awaitCount(MongoEndpoints mongo, EndpointAddress target, String what) {
        Await.until("the target to hold %d documents after %s".formatted(SEEDED_ROWS, what), TIMEOUT,
                () -> mongo.count(target, COLLECTION) == SEEDED_ROWS,
                () -> "%d documents".formatted(mongo.count(target, COLLECTION)));
    }

    private static void awaitName(
            MongoEndpoints mongo, EndpointAddress target, int id, String expected, String what) {
        Await.until("%s, read back from the target".formatted(what), TIMEOUT,
                () -> namesOf(mongo, target, id).contains(expected),
                () -> namesOf(mongo, target, id).toString());
    }

    private static void awaitGone(MongoEndpoints mongo, EndpointAddress target, int id) {
        Await.until(
                "the document deleted while the pipeline was paused to be gone from the target -- a load "
                        + "of the whole collection cannot express a delete, and a tail begun at the present "
                        + "moment never saw one made before it started",
                TIMEOUT,
                () -> namesOf(mongo, target, id).isEmpty(),
                () -> "still holding " + namesOf(mongo, target, id));
    }

    private static List<String> namesOf(MongoEndpoints mongo, EndpointAddress target, int id) {
        return mongo.documents(target, COLLECTION).stream()
                .filter(document -> document.get("oid") instanceof Number found && found.intValue() == id)
                .map(document -> String.valueOf(document.get("name")))
                .toList();
    }
}
