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
 * A pipeline that does not buffer through the shared replay store still carries on from where it was.
 *
 * <p>Turning that buffering off used to take the position accounting with it: the record a run resumed
 * from lived on the shared chain, and a pipeline reading its source directly wrote nothing there. A
 * direct tail therefore had no position to come back to and began at the source's present moment, so
 * every change made while it was paused fell in the gap between the two and was never delivered --
 * not late, not duplicated, simply absent, with nothing in any face saying so.
 *
 * <p><strong>The change made while it is paused is a delete, and that is the whole of the design.</strong>
 * Two things could carry a paused-window <em>edit</em> to the target, and only one of them is what this
 * case is about: carrying on from the recorded position, and loading the whole collection again. The
 * second gets the value right for the wrong reason, and an upsert absorbs the duplicates on the way, so
 * an edit witnesses nothing here. A delete separates them outright -- a full load cannot express one (the
 * document is simply absent from what it reads, and the row already in the target stays), and a tail
 * begun at the present moment never sees one made before it started. A document that goes away is
 * therefore something only a run that resumed where the last one stopped can have done.
 *
 * <p><strong>What these two cases do not witness, stated because it was measured and not guessed.</strong>
 * They do not show that the tail began where the record said. The resolution that turns a stored offset
 * into a tail start was neutered -- made to answer "the present moment" for every run -- and both cases
 * below stayed green, delete and all, on both tiers. That mutation is not inert: under it three unit
 * cases redden, among them the one asserting a direct tail begins where the record says. So the site is
 * real, the mutation reaches it, and neither of these two arrives there. By what other route a delete
 * made before a run started reaches the target is not known, and is written up rather than guessed at.
 *
 * <p>What they do witness is worth having and was not covered anywhere at this level: a pipeline reading
 * its source directly loses nothing across a hold, and nothing across a stop and a start. Both are claims
 * about the product a user would make, and both were unasserted end to end.
 *
 * <p>The reading these cases also do <em>not</em> take is the resumed run's own load count. Measured:
 * after a resume that face answers {@code {orders=5}} on both tiers, and the figure has two readings --
 * a run that really did read the collection again, and the previous run's own count still being published
 * because a resume did not begin a new one. Nothing here separates those, so nothing here rests on it.
 *
 * <p>Mongo on both ends, and the buffering switch is the only thing this case turns. What that switch is
 * meant to decide is whether changes are staged for replay, never where a run begins -- so a source that
 * keeps no resume material of its own is what keeps the reading about the switch rather than about a
 * connector.
 *
 * <p>Gated on Docker and on a directory of real connector jars
 * ({@code -Dtapstate.e2e.connectors-dir}); the real-process tier additionally needs the app module
 * packaged. Run it with:
 *
 * <pre>
 *   mvn -pl e2e -am verify -Dapi.version=1.44 \
 *     -Dtapstate.e2e.connectors-dir=/path/to/connectors \
 *     -Dit.test=DirectTailResumesAfterAPauseIT -Dtest=NoSuchUnitTestOnPurpose
 * </pre>
 */
class DirectTailResumesAfterAPauseIT {

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
            // Nothing is reading the source now, and this pipeline does not buffer, so no copy of it
            // exists anywhere but in the source's own log.
            delete(source, database, DELETED_WHILE_PAUSED);

            control.lifecycle(suffix, LifecycleVerb.RESUME);
            awaitState(control, suffix, PipelineState.RUNNING);

            awaitGone(mongo, target, DELETED_WHILE_PAUSED);
        }
    }

    /**
     * The same claim over a pipeline that really went down and came back, rather than one that was held.
     *
     * <p>Both are here because they are different situations, not because one is stronger: a hold is not
     * a run ending, and a stop is. It was written expecting the stop to be the one that asks where to
     * begin and so the one a mutation at that question could redden. It is not -- the same neutering that
     * leaves the held case green leaves this one green too. That result is what the class comment records;
     * the pair stands as two situations covered, not as a witness of the position.
     */
    @ParameterizedTest
    @EnumSource(Tiers.class)
    void aDirectTailPicksUpTheDeleteMadeWhileItWasStopped(Tiers tier) {
        String suffix = "direct_tail_cycled_" + tier.name().toLowerCase(Locale.ROOT);
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
            rename(source, database, CHANGED_BEFORE, BEFORE_PAUSE);
            awaitName(mongo, target, CHANGED_BEFORE, BEFORE_PAUSE, "the change the running tail captured");

            // A stop that keeps, then a start: the run really ends, so the one that follows has to ask
            // where to begin. That question is what this case is about.
            control.stop(suffix, false);
            delete(source, database, DELETED_WHILE_PAUSED);
            control.lifecycle(suffix, LifecycleVerb.START);
            awaitState(control, suffix, PipelineState.RUNNING);

            awaitGone(mongo, target, DELETED_WHILE_PAUSED);
        }
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
