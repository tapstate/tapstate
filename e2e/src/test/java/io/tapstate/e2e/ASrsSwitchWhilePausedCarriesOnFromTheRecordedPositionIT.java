package io.tapstate.e2e;

import static org.assertj.core.api.Assertions.assertThat;

import com.mongodb.ConnectionString;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import io.tapstate.adapters.mongostore.MongoStorePort;
import io.tapstate.core.lifecycle.LifecycleVerb;
import io.tapstate.core.lifecycle.PipelineState;
import io.tapstate.runtime.srs.CaptureError;
import io.tapstate.spi.store.SrsConsumerId;
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
import java.util.Set;

/**
 * Turning the shared replay store off while a pipeline is held, then resuming it, is refused with a code,
 * and the refusal leaves everything the held run recorded as it was.
 *
 * <p>The switch decides which record a pipeline recovers from. With the buffer on, it is the pipeline's
 * cursor on the shared capture of its source; with it off, it is the record of an independent channel that
 * reads the source directly. The held run wrote the first, and the resume asks for the second, of which
 * there is none. The first cannot stand in for it: what the held run confirmed is a place in the shared
 * capture's log, and the capture's position in the source may run ahead of that place, keeping the changes
 * in between only in the log. An independent channel reads the source and not the log, so nothing the held
 * run recorded proves where it may safely begin. A record that cannot prove a safe start is refused with a
 * code that says so, and the retained state is kept: clearing it, by a full reload or by accepting a new
 * starting point and the gap that comes with it, is the user's decision and never a side effect of the
 * refusal.
 *
 * <p><strong>What the refusal has to leave alone, and why each part is read.</strong>
 * <ul>
 *   <li>The cursor the held run recorded on the shared capture, read just before the resume and again after
 *       the refusal, and compared whole. A refusal that reset part of it would still pass a check that it is
 *       there.</li>
 *   <li>The pipeline's recorded intent: still to run, and not to clear its state on the way. A refusal that
 *       turned itself into a stop that clears would take the cursor with it, but possibly only after the
 *       cursor had been read.</li>
 *   <li>The target. A change made while the pipeline ran reaches it first, so this pipeline is seen carrying
 *       changes when it runs. A delete and an insert are then made at the source while it is held. After the
 *       refusal the deleted document is still in the target and the inserted one is not: nothing ran before
 *       the refusal and moved them.</li>
 * </ul>
 *
 * <p>Mongo on both ends. The artifact is re-applied while the pipeline is held, changing one word in it, so
 * what the resume asks for is a pipeline that no longer buffers.
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
    void turningTheBufferOffWhileHeldIsRefusedAndKeepsWhatTheHeldRunRecorded(Tiers tier) {
        String suffix = "srs_switch_paused_" + tier.name().toLowerCase(Locale.ROOT);
        String database = suffix + "_src";
        String sourceUri = SharedMongo.replicaSetUrl(database);
        String targetUri = SharedMongo.replicaSetUrl(suffix + "_tgt");
        String storeUri = SharedMongo.replicaSetUrl(suffix + "_state");
        EndpointAddress target = EndpointAddress.uri(targetUri);

        try (MongoClient source = MongoClients.create(new ConnectionString(sourceUri));
                MongoClient store = MongoClients.create(new ConnectionString(storeUri));
                StoreDocuments documents = StoreDocuments.at(storeUri);
                MongoEndpoints mongo = new MongoEndpoints();
                ServerHandle server = tier.launch(storeUri)) {

            seed(source, database);
            ControlPlane control = start(server, suffix, sourceUri, targetUri);

            awaitCount(mongo, target, "the full load of the seeded documents");
            // A change while it is running and buffering, so the held run has confirmed something past its
            // load, and so this pipeline is seen carrying a change when it runs.
            rename(source, database, CHANGED_BEFORE, BEFORE_PAUSE);
            awaitName(mongo, target, CHANGED_BEFORE, BEFORE_PAUSE, "the change the buffered tail captured");

            control.lifecycle(suffix, LifecycleVerb.PAUSE);
            awaitState(control, suffix, PipelineState.PAUSED);

            // Changes a run would carry if one ran. Nothing is reading the source now, so both land in the
            // source's own log and nowhere else.
            delete(source, database, DELETED_WHILE_PAUSED);
            insert(source, database, ADDED_WHILE_PAUSED, ADDED_NAME);

            // The one word this case turns. Applied while the pipeline is held, so the resume that follows
            // asks for a pipeline that no longer buffers, against a record written by one that did.
            // The whole workspace goes back, not just the pipeline: an apply replaces what is there rather
            // than patching it, so sending the pipeline alone leaves its source and target referring to
            // nothing and is refused outright.
            control.apply(workspace(suffix, sourceUri, targetUri, false));

            String chain = sharedCapture(documents);
            String consumer = SrsConsumerId.of(suffix, SOURCE_ID).value();
            Document recorded = documents.consumerOffset(chain, consumer);
            assertThat(recorded)
                    .as("the cursor the held run recorded on the shared capture, read before the resume")
                    .isNotNull();

            control.lifecycle(suffix, LifecycleVerb.RESUME);
            awaitState(control, suffix, PipelineState.FAILED);

            assertThat(control.failureCode(suffix))
                    .as("the refusal names a record that cannot prove a safe start")
                    .contains(CaptureError.RECOVERY_PROGRESS_UNPROVEN.code());
            assertThat(documents.consumerOffset(chain, consumer))
                    .as("the refusal leaves the held run's cursor exactly as it was")
                    .isEqualTo(recorded);
            Document intent = intent(store, storeUri, suffix);
            assertThat(intent).as("the pipeline's recorded intent").isNotNull();
            assertThat(intent.getString("targetState"))
                    .as("the intent is still to run")
                    .isEqualTo(PipelineState.RUNNING.name());
            assertThat(intent.getBoolean("purgeState"))
                    .as("and not to clear the pipeline's state")
                    .isFalse();
            assertThat(namesOf(mongo, target, DELETED_WHILE_PAUSED))
                    .as("the document deleted while held is still in the target: no run carried the delete")
                    .containsExactly("order-" + DELETED_WHILE_PAUSED);
            assertThat(namesOf(mongo, target, ADDED_WHILE_PAUSED))
                    .as("the document inserted while held is not in the target: no run carried the insert")
                    .isEmpty();
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

    /**
     * The one capture this case's store holds: the shared one the buffered run read through. The resume is
     * refused before an independent channel is opened, so it stays the only one.
     */
    private static String sharedCapture(StoreDocuments documents) {
        Set<String> chains = documents.miningChainIds();
        assertThat(chains).as("the captures this case's store holds").hasSize(1);
        return chains.iterator().next();
    }

    /**
     * The pipeline's recorded intent, read from the store's own document: no face of the product serves it,
     * and whether a refusal rewrote it is the question.
     */
    private static Document intent(MongoClient store, String storeUri, String pipelineId) {
        return store.getDatabase(new ConnectionString(storeUri).getDatabase())
                .getCollection(MongoStorePort.PIPELINE_DESIRED)
                .find(new Document("_id", pipelineId))
                .first();
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

    /** Checked, because a delete that matched nothing would leave the target agreeing for no reason. */
    private static void delete(MongoClient client, String database, int id) {
        assertThat(client.getDatabase(database).getCollection(COLLECTION)
                        .deleteOne(new Document("_id", id)).getDeletedCount())
                .as("the source to have deleted document %d", id)
                .isEqualTo(1L);
    }

    private static void awaitState(ControlPlane control, String pipelineId, PipelineState expected) {
        Await.until("%s to reach %s".formatted(pipelineId, expected), TIMEOUT,
                () -> control.state(pipelineId).filter(expected::equals).isPresent(),
                () -> control.state(pipelineId) + ", failure " + control.failureCode(pipelineId));
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

    private static List<String> namesOf(MongoEndpoints mongo, EndpointAddress target, int id) {
        return mongo.documents(target, COLLECTION).stream()
                .filter(document -> document.get("oid") instanceof Number found && found.intValue() == id)
                .map(document -> String.valueOf(document.get("name")))
                .toList();
    }
}
