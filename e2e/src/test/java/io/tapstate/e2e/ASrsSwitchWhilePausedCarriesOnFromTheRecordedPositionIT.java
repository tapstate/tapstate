package io.tapstate.e2e;

import com.mongodb.ConnectionString;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import io.tapstate.core.lifecycle.LifecycleVerb;
import io.tapstate.core.lifecycle.PipelineState;
import io.tapstate.testsupport.DockerGate;

import org.bson.Document;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

import java.time.Duration;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;

/**
 * Turning the shared replay store off while a pipeline is held does not lose the window it was held for.
 *
 * <p>The switch decides whether changes are staged for replay. It does not decide where a run begins --
 * but the two used to be the same thing, because the position a run resumed from lived on the shared
 * chain and a pipeline reading its source directly wrote nothing there. Flipping the switch mid-life is
 * the shape that puts weight on them being separate: the run that stops was buffering and recorded its
 * position on the chain, and the run that follows does not buffer and has to find that position anyway.
 *
 * <p><strong>The changes made during the hold are a delete and an insert, and the delete is the one that
 * discriminates.</strong> An insert arriving proves the target moved; it does not prove how. Two things
 * could carry it: carrying on from the recorded position, and loading the collection again from the
 * start -- and the second gets the right answer for the wrong reason. A delete separates them outright.
 * A full load cannot express one (the document is simply absent from what it reads, and the row already
 * in the target stays), and a tail begun at the present moment never sees one made before it started.
 * A document that goes away is therefore something only a run that resumed where the last one stopped
 * can have done.
 *
 * <p><strong>Why this does not count the resumed run's load instead, stated because it was measured.</strong>
 * The obvious reading -- "the resuming run read zero rows" -- is not available after a hold. Measured on
 * the sibling case that holds a direct tail: after a resume that face answers the seeded count, and the
 * figure has two readings, a run that really did read the collection again and the previous run's own
 * count still being published because a resume did not begin a new one. Nothing separates those, so
 * nothing here rests on that number. The delete does the same work and is unambiguous.
 *
 * <p>Mongo on both ends. The artifact is re-applied while the pipeline is held, changing one word in it,
 * so what the resume picks up is a pipeline that no longer buffers.
 *
 * <p>Gated on Docker and on a directory of real connector jars
 * ({@code -Dtapstate.e2e.connectors-dir}); the real-process tier additionally needs the app module
 * packaged. Run it with:
 *
 * <pre>
 *   mvn -pl e2e -am verify -Dapi.version=1.44 \
 *     -Dtapstate.e2e.connectors-dir=/path/to/connectors \
 *     -Dit.test=ASrsSwitchWhilePausedCarriesOnFromTheRecordedPositionIT -Dtest=NoSuchUnitTestOnPurpose
 * </pre>
 */
class ASrsSwitchWhilePausedCarriesOnFromTheRecordedPositionIT {

    private static final Duration TIMEOUT = Duration.ofSeconds(90);

    private static final long SEEDED_ROWS = 5;
    private static final String COLLECTION = "orders";
    private static final String SOURCE_ID = "src_mongo";
    private static final String TARGET_ID = "tgt_mongo";
    private static final String BEFORE_PAUSE = "changed-while-buffering";
    private static final int CHANGED_BEFORE = 1;
    private static final int DELETED_WHILE_PAUSED = 2;
    private static final int ADDED_WHILE_PAUSED = 6;
    private static final String ADDED_NAME = "added-while-paused";

    @BeforeAll
    static void requireDockerAndTheRealConnector() {
        DockerGate.require();
        RealConnectorGate.require("mongodb");
    }

    @ParameterizedTest
    @EnumSource(Tiers.class)
    void turningTheBufferOffWhileHeldStillCarriesTheWindowItWasHeldFor(Tiers tier) {
        String suffix = "srs_switch_paused_" + tier.name().toLowerCase(Locale.ROOT);
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
            // A change while it is running and buffering, so the run ends holding a position it confirmed
            // rather than only a finished load: carrying on has to have something to carry on from.
            rename(source, database, CHANGED_BEFORE, BEFORE_PAUSE);
            awaitName(mongo, target, CHANGED_BEFORE, BEFORE_PAUSE, "the change the buffered tail captured");

            control.lifecycle(suffix, LifecycleVerb.PAUSE);
            awaitState(control, suffix, PipelineState.PAUSED);

            // Nothing is reading the source now. Both changes land in the source's own log and nowhere
            // else -- the ring is not being written either, because the run that fed it has stopped.
            delete(source, database, DELETED_WHILE_PAUSED);
            insert(source, database, ADDED_WHILE_PAUSED, ADDED_NAME);

            // The one word this case turns. Applied while the pipeline is held, so the resume that
            // follows starts a pipeline that no longer buffers, against a position written by one that did.
            // The whole workspace goes back, not just the pipeline: an apply replaces what is there rather
            // than patching it, so sending the pipeline alone leaves its source and target referring to
            // nothing and is refused outright.
            control.apply(workspace(suffix, sourceUri, targetUri, false));

            control.lifecycle(suffix, LifecycleVerb.RESUME);
            awaitState(control, suffix, PipelineState.RUNNING);

            awaitPresent(mongo, target, ADDED_WHILE_PAUSED, ADDED_NAME);
            awaitGone(mongo, target, DELETED_WHILE_PAUSED);
        }
    }

    /** Everything up to and including the start. The pipeline begins with the buffer on. */
    private static ControlPlane start(
            ServerHandle server, String pipelineId, String sourceUri, String targetUri) {
        ControlPlane control = new ControlPlane(server.baseUrl());
        control.bootstrapAndLogin("e2e", "e2e-password");
        control.registerConnector("mongodb", ConnectorJars.bytesFor("mongodb"));

        control.apply(workspace(pipelineId, sourceUri, targetUri, true));
        control.discoverSchema(SOURCE_ID, "mongodb",
                Map.of("uri", sourceUri, "database", pipelineId + "_src"));
        control.lifecycle(pipelineId, LifecycleVerb.START);
        return control;
    }

    /**
     * The whole workspace, with the buffering switch set as asked.
     *
     * <p>Built as a whole both times it is sent. An apply is a replacement of what the workspace holds,
     * so the second one has to carry the source and the target it does not change, or they stop existing
     * and the pipeline that refers to them is refused.
     */
    private static Map<String, String> workspace(
            String pipelineId, String sourceUri, String targetUri, boolean srs) {
        Map<String, String> resources = new LinkedHashMap<>();
        resources.put(SOURCE_ID + ".tap.yml", sourceYaml(sourceUri, pipelineId + "_src"));
        resources.put(TARGET_ID + ".tap.yml", targetYaml(targetUri, pipelineId + "_tgt"));
        resources.put("pipeline.tap.yml", pipelineYaml(pipelineId, srs));
        return resources;
    }

    /** The same pipeline either way; {@code srs} is the only thing that differs between the two runs. */
    private static String pipelineYaml(String pipelineId, boolean srs) {
        return """
                version: tapstate/v1
                kind: pipeline
                id: %s
                source:
                  - { id: %s, srs: %s }
                settings: { read_mode: snapshot_and_cdc }
                transforms:
                  - { id: all_rows, from: [%s], type: filter, expr: "true" }
                serve:
                  from: all_rows
                  sync:
                    - source: %s
                """
                .formatted(pipelineId, SOURCE_ID, srs, COLLECTION, TARGET_ID);
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
            insert(client, database, id, "order-" + id);
        }
    }

    private static void insert(MongoClient client, String database, int id, String name) {
        client.getDatabase(database).getCollection(COLLECTION)
                .insertOne(new Document("_id", id).append("oid", id).append("name", name));
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

    private static void awaitPresent(
            MongoEndpoints mongo, EndpointAddress target, int id, String expected) {
        Await.until(
                "the document inserted while the pipeline was held to reach the target after the switch "
                        + "was turned off -- the half that says the target moved at all",
                TIMEOUT,
                () -> namesOf(mongo, target, id).contains(expected),
                () -> "the target held " + namesOf(mongo, target, id));
    }

    private static void awaitGone(MongoEndpoints mongo, EndpointAddress target, int id) {
        Await.until(
                "the document deleted while the pipeline was held to be gone from the target -- a load of "
                        + "the whole collection cannot express a delete, and a tail begun at the present "
                        + "moment never saw one made before it started, so only a run that carried on from "
                        + "the recorded position can have done this",
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
