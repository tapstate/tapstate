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
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.LongSupplier;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * The word on its own carries on from the position the run recorded; the same word told to rerun reads
 * the collection from the start again.
 *
 * <p>Two cases rather than one, and neither stands without the other. What the terminal composes for
 * either word is a stop and a start, differing only in whether the stop clears what was recorded -- so
 * an implementation that ignored the difference entirely, and simply cycled the pipeline, would satisfy
 * whichever of the two happens to match what cycling does. Read together they pin both ends: one asserts
 * the restarted run reads nothing, the other asserts it reads everything.
 *
 * <p>Neither claim can be read off the target. Re-reading the whole collection also leaves the target
 * correct -- that is precisely why a value assertion witnesses nothing here -- and an idempotent upsert
 * absorbs the duplicates on the way. What separates carrying on from starting over is how much the run
 * after the word had to read, so both cases assert the restarted run's own full-load reading, at the
 * value each claim names: nought for one, every seeded row for the other.
 *
 * <p>The source is Mongo, and that is the subject rather than a convenience. The claim under test is
 * that the engine records a position and hands it back to a source it rebuilds, which is a statement
 * about the engine and not about any one connector. A source connector that keeps its own resume
 * material -- a schema history, a server identity -- in a map it is handed per run brings a second,
 * independent way to fail back to the same reading, and a red case would then not say which of the two
 * was the cause. This connector keeps nothing of its own: what it resumes from is exactly what it was
 * handed, so the reading is a reading of the engine.
 *
 * <p>Liveness before the reading, in the case that asserts nought. A run that never started has read
 * nothing either, and the snapshot face of a pipeline with no live run answers nothing at all -- which a
 * reading defaulted to nought reports as a collection that was never read. So the restarted run is first
 * made to carry a document written after the restart, and beside the claim stands a guard reading the
 * state and the run's own record count. Both have to hold, and the claim is asserted first on purpose --
 * the guard's bound is on volume, so an implementation that read the collection again would trip it on
 * the way past and the reading in question would never be looked at. The case that asserts the whole
 * collection needs no such gate: nought there would mean the run never started, so the count it asserts
 * is its own liveness.
 *
 * <p>Gated on Docker and on a directory of real connector jars
 * ({@code -Dtapstate.e2e.connectors-dir}); the real-process tier additionally needs the app module
 * packaged. Run it with:
 *
 * <pre>
 *   mvn -pl e2e -am verify -Dapi.version=1.44 \
 *     -Dtapstate.e2e.connectors-dir=/path/to/connectors \
 *     -Dit.test=RestartKeepsThePositionIT -Dtest=NoSuchUnitTestOnPurpose
 * </pre>
 */
class RestartKeepsThePositionIT {

    private static final Duration TIMEOUT = Duration.ofSeconds(90);
    private static final Duration SETTLE = Duration.ofSeconds(8);

    private static final long SEEDED_ROWS = 5;
    private static final String COLLECTION = "orders";
    private static final String SOURCE_ID = "src_mongo";
    private static final String TARGET_ID = "tgt_mongo";
    private static final String BEFORE_RESTART = "changed-before-restart";
    private static final String LIVENESS_ROW = "after-restart-liveness";
    private static final int CHANGED_ID = 1;
    private static final int LIVENESS_ID = 99;

    @BeforeAll
    static void requireDockerAndTheRealConnector() {
        DockerGate.require();
        RealConnectorGate.require("mongodb");
    }

    @ParameterizedTest
    @EnumSource(Tiers.class)
    void thePlainRestartCarriesOnWithoutReadingTheCollectionAgain(Tiers tier) throws Exception {
        Fixture fixture = Fixture.forCase("restart_keep", tier);

        try (MongoClient source = fixture.openSource();
                MongoEndpoints mongo = new MongoEndpoints();
                ServerHandle server = tier.launch(fixture.storeUri())) {

            fixture.seed(source);
            ControlPlane control = fixture.start(server);

            awaitCount(mongo, fixture.target(), SEEDED_ROWS, "the full load of the seeded documents");
            // A change after the full load, so the run ends holding a recorded stream position rather
            // than only a finished load: carrying on has to have something to carry on from.
            fixture.rename(source, CHANGED_ID, BEFORE_RESTART);
            awaitName(mongo, fixture.target(), CHANGED_ID, BEFORE_RESTART,
                    "the change the first run captured");

            // What this run reached, so the run after it can be told from it by its own count.
            long droveBefore = settledRecordCount(control, fixture.pipelineId());

            // The pair the terminal composes for the plain word: a stop that keeps, then a start.
            control.stop(fixture.pipelineId(), false);
            control.lifecycle(fixture.pipelineId(), LifecycleVerb.START);

            // Liveness before the reading. A run that has not begun its full load has read nought too,
            // so without a document that actually crosses after the restart the assertion is vacuous.
            fixture.add(source, LIVENESS_ID, LIVENESS_ROW);
            awaitName(mongo, fixture.target(), LIVENESS_ID, LIVENESS_ROW,
                    "a document written after the restart");

            // The claim first, the guard on it second. Both run before this passes, so a green here
            // still means what it did; what the order buys is which line a red one lands on. Asserted
            // the other way round, every implementation that reads the collection again trips the
            // guard's volume bound on the way past -- a re-reading run drives what it re-read -- and
            // the reading this case is about is never reached at all.
            long read = settledRowsRead(control, fixture.pipelineId());
            assertThat(read)
                    .as("documents the restarted run read from %s: the plain word carries on from the "
                            + "recorded position, so the collection is not read again", COLLECTION)
                    .isZero();
            assertTheReadingCameFromTheRunThatReplacedTheOneBefore(
                    control, fixture.pipelineId(), droveBefore, read);
        }
    }

    @ParameterizedTest
    @EnumSource(Tiers.class)
    void aRerunReadsTheWholeCollectionAgain(Tiers tier) throws Exception {
        Fixture fixture = Fixture.forCase("restart_rerun", tier);

        try (MongoClient source = fixture.openSource();
                MongoEndpoints mongo = new MongoEndpoints();
                ServerHandle server = tier.launch(fixture.storeUri())) {

            fixture.seed(source);
            ControlPlane control = fixture.start(server);

            awaitCount(mongo, fixture.target(), SEEDED_ROWS, "the full load of the seeded documents");
            // The same recorded position the other case carries on from. It is seeded here as well, so
            // that what this case asserts is a rerun overriding a position that was there to be used --
            // not a rerun that had nothing to carry on from in the first place.
            fixture.rename(source, CHANGED_ID, BEFORE_RESTART);
            awaitName(mongo, fixture.target(), CHANGED_ID, BEFORE_RESTART,
                    "the change the first run captured");

            // The pair the terminal composes for the rerun: the stop clears, so there is nothing left
            // to carry on from.
            control.stop(fixture.pipelineId(), true);
            control.lifecycle(fixture.pipelineId(), LifecycleVerb.START);

            // The count is its own liveness here: nought would mean the run never started, and every
            // seeded document is the only reading that says it really read the collection again.
            assertThat(settledRowsRead(control, fixture.pipelineId()))
                    .as("documents the rerun read from %s: it was told to read everything, so it reads "
                            + "every seeded document", COLLECTION)
                    .isEqualTo(SEEDED_ROWS);
        }
    }

    /**
     * One case's databases, pipeline and the writes into its source.
     *
     * <p>Everything here is named per case <em>and</em> per tier. The recorded position lives in the
     * store under the pipeline's id, so two runs sharing either would let one case pass for the other's
     * reason -- which is the failure this file would be least able to see, both readings being what the
     * case expects.
     */
    private record Fixture(String pipelineId, String sourceUri, String targetUri, String storeUri) {

        static Fixture forCase(String name, Tiers tier) {
            String suffix = name + "_" + tier.name().toLowerCase(Locale.ROOT);
            return new Fixture(
                    suffix,
                    SharedMongo.replicaSetUrl(suffix + "_src"),
                    SharedMongo.replicaSetUrl(suffix + "_tgt"),
                    SharedMongo.replicaSetUrl(suffix + "_state"));
        }

        String sourceDatabase() {
            return pipelineId + "_src";
        }

        EndpointAddress target() {
            return EndpointAddress.uri(targetUri);
        }

        MongoClient openSource() {
            return MongoClients.create(new ConnectionString(sourceUri));
        }

        /** Written by a driver of the database rather than through any face of the product. */
        void seed(MongoClient client) {
            client.getDatabase(sourceDatabase()).getCollection(COLLECTION).drop();
            for (int id = 1; id <= SEEDED_ROWS; id++) {
                add(client, id, "order-" + id);
            }
        }

        void add(MongoClient client, int id, String name) {
            client.getDatabase(sourceDatabase()).getCollection(COLLECTION)
                    .insertOne(new Document("_id", id).append("oid", id).append("name", name));
        }

        void rename(MongoClient client, int id, String name) {
            client.getDatabase(sourceDatabase()).getCollection(COLLECTION)
                    .updateOne(new Document("_id", id), new Document("$set", new Document("name", name)));
        }

        /** Everything up to and including the start, which both cases here do identically. */
        ControlPlane start(ServerHandle server) {
            ControlPlane control = new ControlPlane(server.baseUrl());
            control.bootstrapAndLogin("e2e", "e2e-password");
            control.registerConnector("mongodb", ConnectorJars.bytesFor("mongodb"));

            Map<String, String> resources = new LinkedHashMap<>();
            resources.put(SOURCE_ID + ".tap.yml", sourceYaml());
            resources.put(TARGET_ID + ".tap.yml", targetYaml());
            resources.put("pipeline.tap.yml", pipelineYaml());
            control.apply(resources);
            // The source model is discovered because the key it reports is what the sink writes under.
            // Without it the sink has no key, and the change below would land as a second document
            // rather than as a change to the one already there.
            control.discoverSchema(SOURCE_ID, "mongodb", Map.of("uri", sourceUri, "database", sourceDatabase()));
            control.lifecycle(pipelineId, LifecycleVerb.START);
            return control;
        }

        String sourceYaml() {
            return """
                    version: tapstate/v1
                    kind: source
                    id: %s
                    connector: mongodb
                    config: { uri: "%s", database: %s }
                    mode: cdc
                    tables: [ %s ]
                    """
                    .formatted(SOURCE_ID, sourceUri, sourceDatabase(), COLLECTION);
        }

        String targetYaml() {
            return """
                    version: tapstate/v1
                    kind: source
                    id: %s
                    connector: mongodb
                    config: { uri: "%s", database: %s }
                    """
                    .formatted(TARGET_ID, targetUri, pipelineId + "_tgt");
        }

        String pipelineYaml() {
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
    }

    /**
     * Says that the reading just taken is a reading of a run that is there and is a new one. Two
     * situations answer "it read nought" while the claim resting on it is false: a pipeline with no live
     * run answers the snapshot face with nothing, and the default that reading takes for a missing
     * collection turns that into the very value the claim asserts; and a restart that did not restart
     * leaves the run before it delivering, so the documents the case waited for arrive on time while
     * saying nothing about a run that was never built.
     *
     * <p>The record count separates all three, because it counts only what the live job drove: it is
     * absent when there is no job, it is the old figure and still climbing when the old job is the one
     * still running, and a job that replaced another begins again at nought -- so the one reading that
     * means "a new run drove these documents" is one above nought and below what the run before it
     * reached.
     *
     * <p>That upper bound is the reason this runs after the claim rather than before it. It is a bound
     * on volume, and a new run that read the whole collection again drives what it re-read -- so it
     * exceeds the bound just as an old run still going does. Run first, it would therefore fail on
     * every implementation the claim is there to catch, and the claim's own line would never be
     * reached: the case would still be red, and the reading it is about would be untested.
     */
    private static void assertTheReadingCameFromTheRunThatReplacedTheOneBefore(
            ControlPlane control, String pipelineId, long droveBefore, long read) {
        assertThat(control.state(pipelineId))
                .as("the state of %s, whose run this reading is a reading of -- and the reading itself "
                        + "was %d, which the claim already accepted and which a run that is not there "
                        + "would have answered just the same", pipelineId, read)
                .contains(PipelineState.RUNNING);
        assertThat(control.snapshotRowsRead(pipelineId))
                .as("the snapshot face of %s: with no live run it answers nothing at all, which the "
                        + "reading taken from it would report as a collection that was never read",
                        pipelineId)
                .containsKey(COLLECTION);
        assertThat(settledRecordCount(control, pipelineId))
                .as("records the live run of %s has driven: the run before it reached %d, so a count at "
                        + "or above that is that same run still going rather than the new one, and a "
                        + "count of nought is documents that reached the target by some other route",
                        pipelineId, droveBefore)
                .isStrictlyBetween(0L, droveBefore);
    }

    /**
     * A reading once it stops moving. Each of these is the job's last collection rather than its current
     * total, so a sample taken the moment an await returns can be short by whatever that collection
     * missed -- and short is the direction that would make these cases pass.
     */
    private static long settled(String what, LongSupplier reading) {
        AtomicLong last = new AtomicLong(Long.MIN_VALUE);
        AtomicLong unchangedSince = new AtomicLong(System.nanoTime());
        Await.until("%s to stop moving".formatted(what), TIMEOUT,
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

    /** What the run has driven to its sinks, once that count settles; -1 for as long as none is live. */
    private static long settledRecordCount(ControlPlane control, String pipelineId) {
        return settled("the records the live run of %s has driven".formatted(pipelineId),
                () -> control.recordCount(pipelineId).orElse(-1L));
    }

    /** What the run has read from the collection, once that reading settles. */
    private static long settledRowsRead(ControlPlane control, String pipelineId) {
        return settled("the documents the live run of %s has read".formatted(pipelineId),
                () -> control.snapshotRowsRead(pipelineId).getOrDefault(COLLECTION, 0L));
    }

    private static void awaitCount(
            MongoEndpoints mongo, EndpointAddress target, long expected, String what) {
        Await.until("the target to hold %d documents after %s".formatted(expected, what), TIMEOUT,
                () -> mongo.count(target, COLLECTION) == expected,
                () -> "%d documents".formatted(mongo.count(target, COLLECTION)));
    }

    /**
     * Waits for some document carrying the given identity to carry the given name.
     *
     * <p>Phrased over the documents the target holds rather than over one the target is expected to
     * hold under a particular key: what the sink writes a document under is its own affair, and a
     * witness that asked for a key would be asserting that as well as what it means to.
     */
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
