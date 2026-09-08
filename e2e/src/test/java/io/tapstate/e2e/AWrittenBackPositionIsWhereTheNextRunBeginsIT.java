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
import java.util.Locale;
import java.util.Map;
import java.util.function.LongSupplier;
import java.util.concurrent.atomic.AtomicLong;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * A resume point read out of the product and handed straight back to it takes the pipeline to where it
 * was read from -- so the changes made since are delivered a second time.
 *
 * <p>Two things have to hold for that, and only one of them is obvious. The obvious one is that a
 * written-back point is used at all rather than ignored. The other is that what the product <em>renders</em>
 * and what it <em>reads back</em> are the same coordinate system: a position that renders into a shape the
 * write-back cannot resolve leaves the pipeline somewhere else entirely, and every face still answers
 * normally. So the token this case hands back is not one it composed -- it is the one the product printed,
 * carried through untouched.
 *
 * <p>The envelope around it is composed, and has to be. Handing the rendered document straight back is
 * refused, by name: it also carries readings, when the point was recorded among them, and a write-back
 * that names one is answered {@code position.field-not-editable}. That is the right refusal -- setting a
 * derived field is meaningless -- so what a caller round-trips is the token, inside the smallest document
 * that names a point.
 *
 * <p>The count is the assertion, for the same reason it is elsewhere in this family: the target cannot
 * tell. Every change here is an upsert of a document that is already there, so a second delivery leaves
 * the target byte for byte as it was -- correct, and silent about whether anything was replayed.
 *
 * <p>Which puts a second reading beside it, because on its own a count of records driven does not say
 * <em>what</em> was driven: a run that read the whole collection again would drive records too, and "it
 * went back" and "it started over" would be the same number. So the run after the write-back is also
 * asserted to have read nothing from the collection. With no load in it, every record it drove is a
 * change it was handed a second time.
 *
 * <p>This was first written the other way, with changes only and no load at all, which would have made
 * the count mean one thing by construction. That mode does not run: a pipeline declared to read changes
 * only, over a source read directly, fails its job on an invariant about acknowledging load rows against
 * a chain that has no record -- nothing reaches the target at all. It is written up separately; this case
 * takes the mode that works and buys the same discrimination with the extra reading instead.
 *
 * <p>Gated on Docker and on a directory of real connector jars
 * ({@code -Dtapstate.e2e.connectors-dir}); the real-process tier additionally needs the app module
 * packaged. Run it with:
 *
 * <pre>
 *   mvn -pl e2e -am verify -Dapi.version=1.44 \
 *     -Dtapstate.e2e.connectors-dir=/path/to/connectors \
 *     -Dit.test=CdcOnlyResumesFromAWrittenBackPositionIT -Dtest=NoSuchUnitTestOnPurpose
 * </pre>
 */
class AWrittenBackPositionIsWhereTheNextRunBeginsIT {

    private static final Duration TIMEOUT = Duration.ofSeconds(90);
    private static final Duration SETTLE = Duration.ofSeconds(8);

    private static final long SEEDED_ROWS = 5;
    private static final String COLLECTION = "orders";
    private static final String SOURCE_ID = "src_mongo";
    private static final String TARGET_ID = "tgt_mongo";

    /** The changes made after the point is read. Both are replayed if the write-back landed. */
    private static final long CHANGES_AFTER_THE_POINT = 2;


    @BeforeAll
    static void requireDockerAndTheRealConnector() {
        DockerGate.require();
        RealConnectorGate.require("mongodb");
    }

    @ParameterizedTest
    @EnumSource(Tiers.class)
    void aPointHandedBackTakesThePipelineToWhereItWasRead(Tiers tier) {
        String suffix = "written_back_" + tier.name().toLowerCase(Locale.ROOT);
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

            // The load's completion is the barrier: it says the source is being read, so a change made
            // after it is one the tail is in a position to see.
            awaitCount(mongo, target, "the full load of the seeded documents");

            // One change first, so there is a real point to read rather than the empty one a run that has
            // confirmed nothing would report.
            rename(source, database, 1, "v1");
            awaitName(mongo, target, 1, "v1", "the first change, which gives the point something to be");

            // The point, taken as the product renders it, reduced to the parts a caller may set.
            Map<String, String> point = control.resumePoint(suffix);
            assertThat(point)
                    .as("the rendered resume point, which is what gets handed back later -- an empty "
                            + "reading would make the write-back below a no-op that still passes")
                    .containsKeys("chainId", "token");

            // The changes this case is about: made after the point was read, and confirmed, so that
            // without a write-back the next run has nothing left to deliver.
            rename(source, database, 2, "v2");
            awaitName(mongo, target, 2, "v2", "the second change");
            rename(source, database, 3, "v3");
            awaitName(mongo, target, 3, "v3", "the third change");

            // Down, then back where it was. The write-back is refused while the pipeline is up, which is
            // a different case; here it is the ordinary path.
            control.stop(suffix, false);
            // Waited for, not assumed: the write-back is refused while the pipeline is still up, and a
            // stop is answered before the pipeline has finished coming down. Without this the case fails
            // on that refusal, which is a different case's subject.
            awaitState(control, suffix, PipelineState.STOPPED);
            control.writeBackPosition(suffix, point.get("chainId"), point.get("token"));
            control.lifecycle(suffix, LifecycleVerb.START);
            awaitState(control, suffix, PipelineState.RUNNING);

            // First that the run read nothing from the collection, so the count below can only be changes.
            assertThat(control.snapshotRowsRead(suffix))
                    .as("the snapshot face of the run after the write-back: with no live run it answers "
                            + "nothing at all, which the reading taken from it would report as a "
                            + "collection that was never read")
                    .containsKey(COLLECTION);
            assertThat(control.snapshotRowsRead(suffix).get(COLLECTION))
                    .as("documents that run read from %s: the stop kept what was recorded, so the load is "
                            + "not done again and every record it drives is a change", COLLECTION)
                    .isZero();

            assertThat(settledRecordCount(control, suffix))
                    .as("records the run after the write-back drove: it was put back before the two "
                            + "changes that followed the point, so it is given both again. Left where it "
                            + "was, or handed a point it could not resolve, it would have nothing to "
                            + "deliver and this reading would be nought")
                    .isGreaterThanOrEqualTo(CHANGES_AFTER_THE_POINT);

            // And the replay did not corrupt what was already right. Said because a rewind is the one
            // operation in this family that can make the target worse rather than merely slower.
            awaitName(mongo, target, 3, "v3", "the third change, still standing after the replay");
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

    /** Changes only, and read directly: neither a load nor a shared buffer is in this case's way. */
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

    private static void awaitCount(MongoEndpoints mongo, EndpointAddress target, String what) {
        Await.until("the target to hold %d documents after %s".formatted(SEEDED_ROWS, what), TIMEOUT,
                () -> mongo.count(target, COLLECTION) == SEEDED_ROWS,
                () -> "%d documents".formatted(mongo.count(target, COLLECTION)));
    }

    private static void awaitState(ControlPlane control, String pipelineId, PipelineState expected) {
        Await.until("%s to reach %s".formatted(pipelineId, expected), TIMEOUT,
                () -> control.state(pipelineId).filter(expected::equals).isPresent(),
                () -> String.valueOf(control.state(pipelineId)));
    }

    private static void awaitName(
            MongoEndpoints mongo, EndpointAddress target, int id, String expected, String what) {
        Await.until("%s, read back from the target".formatted(what), TIMEOUT,
                () -> mongo.documents(target, COLLECTION).stream().anyMatch(document ->
                        document.get("oid") instanceof Number found && found.intValue() == id
                                && expected.equals(document.getString("name"))),
                () -> mongo.documents(target, COLLECTION).toString());
    }

    /** What the live run has driven to its sinks, once that count settles; -1 while none is live. */
    private static long settledRecordCount(ControlPlane control, String pipelineId) {
        return settled(() -> control.recordCount(pipelineId).orElse(-1L));
    }

    /**
     * A reading once it stops moving. The count is the job's last collection rather than its current
     * total, so a sample taken the moment an await returns can be short by whatever that collection
     * missed -- and short is the direction that would make this case fail for the wrong reason.
     */
    private static long settled(LongSupplier reading) {
        AtomicLong last = new AtomicLong(Long.MIN_VALUE);
        AtomicLong unchangedSince = new AtomicLong(System.nanoTime());
        Await.until("a reading of the live run to stop moving", TIMEOUT,
                () -> {
                    long now = reading.getAsLong();
                    if (now != last.get()) {
                        last.set(now);
                        unchangedSince.set(System.nanoTime());
                        return false;
                    }
                    return System.nanoTime() - unchangedSince.get() > SETTLE.toNanos();
                },
                () -> String.valueOf(last.get()));
        return last.get();
    }
}
