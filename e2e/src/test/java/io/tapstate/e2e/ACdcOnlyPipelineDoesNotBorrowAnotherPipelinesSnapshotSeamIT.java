package io.tapstate.e2e;

import static org.assertj.core.api.Assertions.assertThat;

import com.mongodb.ConnectionString;
import com.mongodb.client.ClientSession;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoCollection;
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
import java.util.Objects;
import java.util.concurrent.atomic.AtomicReference;
import org.bson.BsonTimestamp;
import org.bson.Document;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

/**
 * A CDC-only pipeline is never handed another pipeline's snapshot seam. On a source nobody holds it starts
 * at its own present; on a source another pipeline holds, it carries on from where the source's mining chain
 * stands, because the chain has one reader and one position to resume from.
 *
 * <p>Both cases begin the same way: a first pipeline loads one collection, which records its seam on the
 * chain, and opens its tail. The two sources deliberately have different artifact ids and table selections
 * but identical physical Mongo coordinates. Mining-chain identity excludes the table subset, so they meet in
 * one persisted chain. The observed real connector is used only to place writes on known sides of stream
 * teardown and startup; every result is read independently from the target.
 *
 * <p><b>Nobody holds the source.</b> The first pipeline is cleared, and the chain with it. A row written
 * after that and before the CDC-only pipeline starts is not delivered: there is nothing to carry on from, so
 * the new pipeline begins at its own present. A row written after its stream opens must arrive, so an empty
 * target or a tail that never started cannot satisfy the absence. The source names its present as a second
 * of its cluster time, so the row before the start is written a second ahead of it, not merely before it.
 *
 * <p><b>Another pipeline holds the source.</b> The first pipeline is stopped with its state kept, so it still
 * owes every change on its collection after where it stopped, and the chain's reader has to carry on from
 * there for it -- otherwise those changes are gone for it. The CDC-only pipeline reads through the same
 * reader, so it is handed the changes on its own collection from that point: one written before it started
 * among them. What it is not handed is a row written after the first pipeline's seam that the chain had
 * already moved past -- that row reaches it only if it was given the seam rather than the chain's position.
 * And the first pipeline, started again, receives the change made on its collection while it was stopped.
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
    private static final String PASSED_BY_THE_CHAIN = "written-before-the-chain-moved-on";
    private static final String MOVES_THE_CHAIN_ON = "moves-the-chain-on";
    private static final String WHILE_STOPPED = "written-while-the-seam-owner-was-stopped";
    private static final String BEFORE_START = "written-before-cdc-only-start";
    private static final String AFTER_START = "written-after-cdc-only-start";
    /** Where the case writes the marks that move the source's cluster time on; neither pipeline reads it. */
    private static final String CLOCK_DATABASE = "cdc_own_start_clock";

    @BeforeAll
    static void requireDockerAndTheRealConnector() {
        DockerGate.require();
        RealConnectorGate.require("mongodb");
    }

    @ParameterizedTest
    @EnumSource(Tiers.class)
    void aCdcOnlyPipelineOnASourceNobodyHoldsStartsAtItsOwnPresent(
            Tiers tier, @TempDir Path temporary) throws Exception {
        try (Case run = Case.open(tier, "free", temporary)) {
            run.startTheSeamOwner();
            String chain = ChainNotes.chainOf(run.storeUri, FIRST_PIPELINE);

            run.control.stop(FIRST_PIPELINE, true);
            run.awaitState(FIRST_PIPELINE, PipelineState.STOPPED);
            run.awaitTailLines(2, "END");
            Await.until("clearing the only pipeline on the chain to clear the chain", TIMEOUT,
                    () -> chainRecord(run.storeUri, chain) == null,
                    () -> String.valueOf(chainRecord(run.storeUri, chain)));

            run.insertAndWaitOutItsSecond(SECOND_COLLECTION, 1, BEFORE_START);
            run.startTheCdcOnlyPipeline();
            run.insert(SECOND_COLLECTION, 2, AFTER_START);
            run.awaitRow(SECOND_COLLECTION, AFTER_START);

            assertThat(run.names(SECOND_COLLECTION))
                    .as("the live CDC-only tail carries its post-start row without replaying the row "
                            + "written before that pipeline existed")
                    .containsExactly(AFTER_START);
            assertThat(run.control.errorCount(SECOND_PIPELINE)).contains(0L);
        }
    }

    @ParameterizedTest
    @EnumSource(Tiers.class)
    void aCdcOnlyPipelineOnAHeldSourceCarriesOnFromWhereTheChainStandsAndNotFromTheSeam(
            Tiers tier, @TempDir Path temporary) throws Exception {
        try (Case run = Case.open(tier, "held", temporary)) {
            run.startTheSeamOwner();
            String chain = ChainNotes.chainOf(run.storeUri, FIRST_PIPELINE);

            // After the first pipeline's seam, on a collection its tail does not read; then a change it does
            // read, which it lands. Once the chain has released that change, its position is past both.
            run.insert(SECOND_COLLECTION, 1, PASSED_BY_THE_CHAIN);
            run.insert(FIRST_COLLECTION, 2, MOVES_THE_CHAIN_ON);
            run.awaitRow(FIRST_COLLECTION, MOVES_THE_CHAIN_ON);
            Await.until("the seam owner to confirm its change on the chain", TIMEOUT,
                    () -> confirmedOn(run.storeUri, chain, FIRST_PIPELINE, FIRST_COLLECTION),
                    () -> String.valueOf(chainRecord(run.storeUri, chain)));
            awaitTwoReleasesFromNow(run.storeUri, chain);

            run.control.stop(FIRST_PIPELINE, false);
            run.awaitState(FIRST_PIPELINE, PipelineState.STOPPED);
            run.awaitTailLines(2, "END");

            // Nothing reads the source here. The first pipeline still owes this change on its collection.
            run.insert(FIRST_COLLECTION, 3, WHILE_STOPPED);
            run.insert(SECOND_COLLECTION, 2, BEFORE_START);
            run.startTheCdcOnlyPipeline();
            run.insert(SECOND_COLLECTION, 3, AFTER_START);
            run.awaitRow(SECOND_COLLECTION, AFTER_START);

            assertThat(run.names(SECOND_COLLECTION))
                    .as("the CDC-only pipeline carries on from the chain's position, which the held pipeline "
                            + "pinned before the row written ahead of its start -- and past the row only "
                            + "the first pipeline's seam would hand it")
                    .containsExactly(AFTER_START, BEFORE_START);
            assertThat(run.control.errorCount(SECOND_PIPELINE)).contains(0L);

            run.control.lifecycle(FIRST_PIPELINE, LifecycleVerb.START);
            run.awaitState(FIRST_PIPELINE, PipelineState.RUNNING);
            run.awaitRow(FIRST_COLLECTION, WHILE_STOPPED);
        }
    }

    /**
     * Waits until the chain's reader has released two more runs of the source log. Two, because the first
     * release after a confirmation landed may have been worked out just before it did; the reader releases
     * one run at a time, so the second was worked out once the first had been written. A quiet MongoDB
     * source keeps handing over runs that carry only its position, so the releases keep coming.
     */
    private static void awaitTwoReleasesFromNow(String storeUri, String chain) {
        Object seen = releasedThrough(storeUri, chain);
        for (int release = 1; release <= 2; release++) {
            Object before = seen;
            Await.until("the chain's reader to release another run of the source log", TIMEOUT,
                    () -> !Objects.equals(releasedThrough(storeUri, chain), before),
                    () -> "released through " + releasedThrough(storeUri, chain));
            seen = releasedThrough(storeUri, chain);
        }
    }

    /** The order of the last run of the source log the chain's reader released. */
    private static Object releasedThrough(String storeUri, String chain) {
        Document record = chainRecord(storeUri, chain);
        return record == null ? null : record.get("sourceReadSeq");
    }

    /** The chain's record, read straight out of the store, or null where the chain has none. */
    private static Document chainRecord(String storeUri, String chain) {
        try (MongoClient client = MongoClients.create(storeUri)) {
            return client.getDatabase(new ConnectionString(storeUri).getDatabase())
                    .getCollection("srs_meta")
                    .find(new Document("_id", chain))
                    .first();
        }
    }

    /** Whether {@code pipeline}'s record on the chain holds a confirmation for {@code table}. */
    private static boolean confirmedOn(String storeUri, String chain, String pipeline, String table) {
        try (MongoClient client = MongoClients.create(storeUri)) {
            Document consumer = client.getDatabase(new ConnectionString(storeUri).getDatabase())
                    .getCollection("srs_consumer_offsets")
                    .find(new Document("_id.chain", chain).append("_id.pipeline", pipeline))
                    .first();
            return consumer != null && consumer.get("sinkAckedByTable") instanceof Document confirmations
                    && confirmations.containsKey(table);
        }
    }

    /** One case's server, source, target and observed tail. */
    private static final class Case implements AutoCloseable {

        private final MongoClient source;
        private final MongoEndpoints mongo;
        private final ServerHandle server;
        private final ControlPlane control;
        private final String sourceDatabase;
        private final String sourceUri;
        private final String targetUri;
        private final String storeUri;
        private final Path tail;

        private Case(MongoClient source, MongoEndpoints mongo, ServerHandle server, ControlPlane control,
                String sourceDatabase, String sourceUri, String targetUri, String storeUri, Path tail) {
            this.source = source;
            this.mongo = mongo;
            this.server = server;
            this.control = control;
            this.sourceDatabase = sourceDatabase;
            this.sourceUri = sourceUri;
            this.targetUri = targetUri;
            this.storeUri = storeUri;
            this.tail = tail;
        }

        static Case open(Tiers tier, String scenario, Path temporary) throws Exception {
            String suffix = scenario + "_" + tier.name().toLowerCase(Locale.ROOT);
            String sourceDatabase = "cdc_own_start_source_" + suffix;
            String storeUri = SharedMongo.replicaSetUrl("cdc_own_start_store_" + suffix);
            Path witness = temporary.resolve("mongo-writes");
            byte[] connector = ObservedMongoConnectorJar.build(ConnectorJars.bytesFor("mongodb"), witness);
            MongoClient source = MongoClients.create(SharedMongo.replicaSetUrl(sourceDatabase));
            MongoEndpoints mongo = new MongoEndpoints();
            ServerHandle server = null;
            try {
                server = tier.launch(storeUri);
                ControlPlane control = new ControlPlane(server.baseUrl());
                control.bootstrapAndLogin("e2e", "e2e-password");
                control.registerConnector("mongodb", connector);
                return new Case(source, mongo, server, control, sourceDatabase,
                        SharedMongo.replicaSetUrl(sourceDatabase),
                        SharedMongo.replicaSetUrl("cdc_own_start_target_" + suffix),
                        storeUri, temporary.resolve("mongo-writes.tail"));
            } catch (Exception | Error failure) {
                closeAll(server, mongo, source);
                throw failure;
            }
        }

        /** Starts the pipeline whose load records the seam, and waits for its load and its open tail. */
        void startTheSeamOwner() {
            source.getDatabase(sourceDatabase).getCollection(FIRST_COLLECTION).drop();
            insert(FIRST_COLLECTION, 1, "snapshot-row");
            control.apply(workspace(FIRST_SOURCE, FIRST_TARGET, FIRST_PIPELINE, FIRST_COLLECTION,
                    "snapshot_and_cdc", sourceUri, targetUri));
            control.discoverSchema(
                    FIRST_SOURCE, "mongodb", Map.of("uri", sourceUri, "database", sourceDatabase));
            control.lifecycle(FIRST_PIPELINE, LifecycleVerb.START);
            awaitRow(FIRST_COLLECTION, "snapshot-row");
            awaitTailLines(1, "START ");
        }

        /** Starts the CDC-only pipeline over the second collection, and waits for its tail to open. */
        void startTheCdcOnlyPipeline() {
            control.apply(workspace(SECOND_SOURCE, SECOND_TARGET, SECOND_PIPELINE, SECOND_COLLECTION,
                    "cdc_only", sourceUri, targetUri));
            control.discoverSchema(
                    SECOND_SOURCE, "mongodb", Map.of("uri", sourceUri, "database", sourceDatabase));
            control.lifecycle(SECOND_PIPELINE, LifecycleVerb.START);
            awaitState(SECOND_PIPELINE, PipelineState.RUNNING);
            awaitTailLines(3, "START ");
        }

        void insert(String collection, int id, String name) {
            source.getDatabase(sourceDatabase).getCollection(collection)
                    .insertOne(new Document("_id", id).append("oid", id).append("name", name));
        }

        /**
         * Writes a row, then waits until the source's cluster time has moved into a later second than the write.
         *
         * <p>A MongoDB source names its present as a second of its cluster time, and a stream started there is
         * handed everything written in that second. A row written moments before a pipeline starts can share
         * the second, and is then handed to it although it was written first. Each wait writes a mark to a
         * database neither pipeline reads, which is what moves the cluster time on.
         */
        void insertAndWaitOutItsSecond(String collection, int id, String name) {
            BsonTimestamp written;
            try (ClientSession session = source.startSession()) {
                source.getDatabase(sourceDatabase).getCollection(collection).insertOne(session,
                        new Document("_id", id).append("oid", id).append("name", name));
                written = session.getOperationTime();
            }
            MongoCollection<Document> marks = source.getDatabase(CLOCK_DATABASE).getCollection("marks");
            AtomicReference<BsonTimestamp> now = new AtomicReference<>(written);
            Await.until("the source's cluster time to leave the second " + name + " was written in", TIMEOUT,
                    () -> {
                        try (ClientSession session = source.startSession()) {
                            marks.insertOne(session, new Document());
                            now.set(session.getOperationTime());
                        }
                        return now.get().getTime() > written.getTime();
                    },
                    () -> "written at " + written + ", cluster time now " + now.get());
        }

        void awaitRow(String collection, String name) {
            Await.until(name + " to reach the target's " + collection, TIMEOUT,
                    () -> names(collection).contains(name),
                    () -> "rows=" + names(collection) + ", " + FIRST_PIPELINE + "=" + control.state(FIRST_PIPELINE)
                            + ", " + SECOND_PIPELINE + "=" + control.state(SECOND_PIPELINE));
        }

        List<String> names(String collection) {
            return mongo.documents(EndpointAddress.uri(targetUri), collection).stream()
                    .map(document -> document.getString("name"))
                    .sorted()
                    .toList();
        }

        /** Waits until the observed tail has written {@code lines} lines, the last beginning {@code with}. */
        void awaitTailLines(int lines, String with) {
            Await.until("the observed tail to reach line " + lines, TIMEOUT,
                    () -> tailLines().size() == lines && tailLines().getLast().startsWith(with),
                    () -> tailLines().toString());
        }

        void awaitState(String pipelineId, PipelineState expected) {
            Await.until(pipelineId + " to reach " + expected, TIMEOUT,
                    () -> control.state(pipelineId).filter(expected::equals).isPresent(),
                    () -> String.valueOf(control.state(pipelineId)));
        }

        private List<String> tailLines() {
            try {
                return Files.exists(tail) ? Files.readAllLines(tail) : List.of();
            } catch (java.io.IOException failure) {
                throw new java.io.UncheckedIOException(failure);
            }
        }

        @Override
        public void close() throws Exception {
            closeAll(server, mongo, source);
        }

        /** Closes the server first, then the clients it was observed by; a null is one never opened. */
        private static void closeAll(AutoCloseable... opened) throws Exception {
            Exception first = null;
            for (AutoCloseable resource : opened) {
                try {
                    if (resource != null) {
                        resource.close();
                    }
                } catch (Exception failure) {
                    if (first == null) {
                        first = failure;
                    } else {
                        first.addSuppressed(failure);
                    }
                }
            }
            if (first != null) {
                throw first;
            }
        }
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
}
