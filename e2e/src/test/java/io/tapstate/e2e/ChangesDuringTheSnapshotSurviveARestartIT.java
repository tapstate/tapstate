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
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Changes made while the full load is still running survive a restart: none is lost, and none is undone
 * by a load row arriving after it.
 *
 * <p>The seam between a load and the tail that follows it is the one place a resume can lose data without
 * anything looking wrong. A load reads a table as it was; the tail carries what happened since; the point
 * they meet has to be recorded so a later run picks the tail up there and not somewhere else. When that
 * point is overwritten each round instead of kept, a restarted run's tail begins at the new seam and every
 * change made during the first load falls between the two -- delivered by neither, reported by nothing.
 *
 * <p>So the changes here are made <em>while the load is still going</em>, and the pipeline is taken down
 * before it finishes. The window is found by watching the target fill: the case acts once some rows have
 * landed and the rest have not, which is the only externally visible sign that a load is in flight.
 *
 * <p>Three changes, because they fail differently:
 *
 * <ul>
 *   <li>an <strong>insert</strong> -- lost if the seam moved, and a load of the original table cannot
 *       produce it, so its presence at the end can only come from the tail;</li>
 *   <li>a <strong>delete</strong> -- the one a load can never express: an upsert leaves the row it already
 *       wrote, so a document that goes away can only be a change that was carried;</li>
 *   <li>an <strong>update</strong> -- which catches the opposite fault. If the load row for that document
 *       arrives after the change, the target ends holding the value from before it. Every count is right
 *       in that world and the value is wrong, which is why this one is asserted on the value.</li>
 * </ul>
 *
 * <p><strong>What this does not witness, measured rather than assumed.</strong> It does not show that the
 * restarted run's tail began at the recorded seam. Neutering the resolution that turns a stored offset
 * into a tail start -- so that every run begins at the source's present moment -- leaves this case green
 * on both tiers. The same edit reddens three unit cases, so the site is real and the edit reaches it;
 * this run does not arrive there. That is the third case in this suite to find the same thing, and it is
 * written up separately: whatever positions a tail in a deployed run, the end-to-end path does not go
 * through the place the unit cases assert about.
 *
 * <p><strong>What this does not witness, measured rather than assumed.</strong> It does not show that the
 * restarted run's tail began at the recorded seam. Two edits were made to find out, and both left this
 * case green on both tiers: making every run begin at the source's present moment rather than at a stored
 * offset, and making the seam be overwritten each round rather than kept -- which is precisely the fault
 * this case was written against. Neither edit is inert. The first reddens three unit cases; the second
 * reddens the unit case that asserts a snapshot which never drained resumes from the seam it recorded
 * rather than the one sampled now. So both sites are real and both edits reach them, and this run arrives
 * at neither.
 *
 * <p>That is the same finding two other cases in this suite reached independently, and it is written up
 * separately rather than guessed at here: whatever positions a tail in a deployed run, the end-to-end path
 * does not go through the places the unit cases assert about. What this case does witness is the outcome
 * -- three changes made during a load, all correct after a restart, with the delete shown to have been in
 * the target beforehand so that its absence cannot be vacuous -- which nothing covered end to end before.
 *
 * <p>Gated on Docker and on a directory of real connector jars
 * ({@code -Dtapstate.e2e.connectors-dir}); the real-process tier additionally needs the app module
 * packaged. Run it with:
 *
 * <pre>
 *   mvn -pl e2e -am verify -Dapi.version=1.44 \
 *     -Dtapstate.e2e.connectors-dir=/path/to/connectors \
 *     -Dit.test=ChangesDuringTheSnapshotSurviveARestartIT -Dtest=NoSuchUnitTestOnPurpose
 * </pre>
 */
class ChangesDuringTheSnapshotSurviveARestartIT {

    private static final Duration TIMEOUT = Duration.ofSeconds(180);

    /**
     * Enough rows that the load takes long enough to be caught in the middle of. The window this case
     * needs is "some landed, not all", and a load of a handful of documents does not have one.
     */
    private static final int SEEDED_ROWS = 20000;

    private static final String COLLECTION = "orders";
    private static final String SOURCE_ID = "src_mongo";
    private static final String TARGET_ID = "tgt_mongo";

    /** The three documents the case touches, chosen well inside the seeded range. */
    private static final int UPDATED = 7;
    private static final int DELETED = 11;
    private static final int INSERTED = SEEDED_ROWS + 1;

    private static final String AFTER_THE_LOAD_BEGAN = "changed-during-the-load";
    private static final String INSERTED_NAME = "inserted-during-the-load";

    @BeforeAll
    static void requireDockerAndTheRealConnector() {
        DockerGate.require();
        RealConnectorGate.require("mongodb");
    }

    @ParameterizedTest
    @EnumSource(Tiers.class)
    void aChangeMadeDuringTheLoadIsStillThereAfterARestart(Tiers tier) {
        String suffix = "during_load_" + tier.name().toLowerCase(Locale.ROOT);
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

            awaitTheLoadIsUnderWay(mongo, target);

            // The document that is about to be deleted has to be in the target before it is deleted,
            // or its absence at the end says nothing: a run that simply loaded the table again would
            // never have written it either, and the assertion would pass on a case that never happened.
            Await.until("the document about to be deleted to have reached the target first", TIMEOUT,
                    () -> !namesOf(mongo, target, DELETED).isEmpty(),
                    () -> "the target held " + namesOf(mongo, target, DELETED));

            // The three changes, all made while the load is still writing.
            update(source, database, UPDATED, AFTER_THE_LOAD_BEGAN);
            delete(source, database, DELETED);
            insert(source, database, INSERTED, INSERTED_NAME);

            // Down before the load finishes. A stop that keeps: what the run recorded is what the next one
            // is meant to carry on from, and clearing it would make this a different case.
            control.stop(suffix, false);
            awaitState(control, suffix, PipelineState.STOPPED);
            control.lifecycle(suffix, LifecycleVerb.START);
            awaitState(control, suffix, PipelineState.RUNNING);

            // Each of the three waited for on its own, and the count only afterwards. A count is the wrong
            // barrier here and was measured to be: the total reaches its final value the moment the delete
            // lands, whether or not the insert has arrived, so an assertion taken behind it races the one
            // change it is about. Each wait carries its own bound and reports its own reading.
            awaitName(mongo, target, INSERTED, INSERTED_NAME,
                    "the document inserted while the load was running -- a load of the table as it was "
                            + "cannot produce it, so its arrival is the tail having carried it");
            awaitGone(mongo, target, DELETED);
            awaitName(mongo, target, UPDATED, AFTER_THE_LOAD_BEGAN,
                    "the document changed while the load was running -- every count is right even when its "
                            + "load row lands after the change and puts the old value back, so this one is "
                            + "read on the value");

            // And nothing else moved: the seeded rows, plus the one added, less the one removed.
            assertThat(mongo.count(target, COLLECTION))
                    .as("documents in the target once all three changes have arrived")
                    .isEqualTo(SEEDED_ROWS);
        }
    }

    /**
     * Waits for the load to be visibly in flight -- some rows in the target, not all of them.
     *
     * <p>Counted at the target rather than read off the product's own progress face, and the seeded table
     * is large for the same reason. Measured while writing this: the rows-read figure is not a usable
     * signal at all -- a bounded read appends the whole table into a buffer before anything downstream
     * runs, so that face answers the full count on the very first poll and the "part way through" state it
     * would report never exists. What does take time is the writing, so what has reached the target is the
     * progress there is to see. At three thousand rows even that was over between two polls on one tier;
     * the size here is what makes the window wide enough to land in on both.
     */
    private static void awaitTheLoadIsUnderWay(MongoEndpoints mongo, EndpointAddress target) {
        Await.until("the full load to be under way but not finished", TIMEOUT,
                () -> {
                    long landed = mongo.count(target, COLLECTION);
                    return landed > 0 && landed < SEEDED_ROWS;
                },
                () -> "%d of %d documents had landed".formatted(mongo.count(target, COLLECTION), SEEDED_ROWS));
    }

    /** Everything up to and including the start. */
    private static ControlPlane start(
            ServerHandle server, String pipelineId, String sourceUri, String targetUri) {
        ControlPlane control = new ControlPlane(server.baseUrl());
        control.bootstrapAndLogin("e2e", "e2e-password");
        control.registerConnector("mongodb", ConnectorJars.bytesFor("mongodb"));

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

    private static String pipelineYaml(String pipelineId) {
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
        List<Document> documents = new ArrayList<>(SEEDED_ROWS);
        for (int id = 1; id <= SEEDED_ROWS; id++) {
            documents.add(new Document("_id", id).append("oid", id).append("name", "order-" + id));
        }
        client.getDatabase(database).getCollection(COLLECTION).insertMany(documents);
    }

    private static void update(MongoClient client, String database, int id, String name) {
        client.getDatabase(database).getCollection(COLLECTION)
                .updateOne(new Document("_id", id), new Document("$set", new Document("name", name)));
    }

    private static void delete(MongoClient client, String database, int id) {
        client.getDatabase(database).getCollection(COLLECTION).deleteOne(new Document("_id", id));
    }

    private static void insert(MongoClient client, String database, int id, String name) {
        client.getDatabase(database).getCollection(COLLECTION)
                .insertOne(new Document("_id", id).append("oid", id).append("name", name));
    }

    private static void awaitState(ControlPlane control, String pipelineId, PipelineState expected) {
        Await.until("%s to reach %s".formatted(pipelineId, expected), TIMEOUT,
                () -> control.state(pipelineId).filter(expected::equals).isPresent(),
                () -> String.valueOf(control.state(pipelineId)));
    }

    private static void awaitName(
            MongoEndpoints mongo, EndpointAddress target, int id, String expected, String what) {
        Await.until("%s, read back from the target".formatted(what), TIMEOUT,
                () -> namesOf(mongo, target, id).contains(expected),
                () -> namesOf(mongo, target, id).toString());
    }

    private static void awaitGone(MongoEndpoints mongo, EndpointAddress target, int id) {
        Await.until(
                "the document deleted while the load was running -- and shown above to have reached the "
                        + "target before that -- to be gone from it",
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
