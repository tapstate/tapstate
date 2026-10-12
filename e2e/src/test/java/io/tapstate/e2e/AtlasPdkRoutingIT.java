package io.tapstate.e2e;

import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import io.tapstate.adapters.mongostore.SystemCollections;
import io.tapstate.core.common.JsonWriter;
import io.tapstate.core.event.Op;
import io.tapstate.core.event.SourceOrder;
import io.tapstate.core.lifecycle.LifecycleVerb;
import io.tapstate.core.lifecycle.PipelineState;
import io.tapstate.runtime.srs.SrsRingbuffer;
import io.tapstate.spi.store.SrsConsumerId;
import io.tapstate.testsupport.RequiresDocker;
import org.bson.Document;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;
import org.testcontainers.containers.MongoDBContainer;
import org.testcontainers.utility.DockerImageName;

import java.time.Duration;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.TreeMap;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * The checked Atlas connector's real PDK functions recover a refused DELETE in both runtime modes.
 *
 * <p>Both data endpoints are owned local replica sets. Only the target client's delete command is
 * refused, after snapshot and an update have crossed. The capture may checkpoint the durable SRS
 * batch, but the consumer cannot confirm the refused change. The same stored pipeline then resumes
 * across a context replacement without another apply or registration, and removes the old target row.
 * This is a controlled consumer fault witness, not real Atlas permissions or Cloud provisioning.
 */
@RequiresDocker
class AtlasPdkRoutingIT {

    private static final Duration TIMEOUT = Duration.ofSeconds(120);
    private static final String CONNECTOR = "mongodb-atlas";
    private static final String SOURCE = "ack_source";
    private static final String TARGET = "ack_target";
    private static final String PIPELINE = "atlas_delete_recovery";
    private static final String TABLE = "probe";
    private static final String DELETED_ID = "delete-me";

    @BeforeAll
    static void requireCheckedConnector() {
        RealConnectorGate.require(CONNECTOR);
    }

    @ParameterizedTest(name = "{0}/REFUSED_DELETE")
    @EnumSource(AtlasRuntime.Mode.class)
    void aRefusedDeleteDoesNotAdvanceTheConsumerAndIsReplayedAfterRestart(AtlasRuntime.Mode mode)
            throws Exception {
        String suffix = mode.name().toLowerCase(java.util.Locale.ROOT) + "_"
                + UUID.randomUUID().toString().substring(0, 8);
        String sourceDatabase = "ts_plan_afs_" + suffix;
        String targetDatabase = "ts_plan_aft_" + suffix;
        String metadataDatabase = "ts_plan_afm_" + suffix;
        String sourceUri = SharedMongo.replicaSetUrl(sourceDatabase);
        String metadataUri = SharedMongo.replicaSetUrl(metadataDatabase);

        try (MongoClient source = MongoClients.create(sourceUri);
             DeleteFailureTarget target = DeleteFailureTarget.start(targetDatabase, "delete-ack-" + suffix);
             MongoClient metadata = MongoClients.create(metadataUri);
             AutoCloseable sourceCleanup = () -> source.getDatabase(sourceDatabase).drop();
             AutoCloseable metadataCleanup = () -> {
                 for (String database : List.of(metadataDatabase, metadataDatabase + "_operator",
                         metadataDatabase + "_views")) metadata.getDatabase(database).drop();
             };
             AtlasRuntime runtime = new AtlasRuntime(mode, metadataUri)) {
            MongoCollection<Document> sourceRows = source.getDatabase(sourceDatabase).getCollection(TABLE);
            MongoCollection<Document> targetRows = target.database().getCollection(TABLE);
            MongoDatabase raw = metadata.getDatabase(metadataDatabase);
            sourceRows.insertMany(List.of(row(DELETED_ID, 10), row("keep-1", 20), row("keep-2", 30),
                    row("keep-3", 40), row("keep-4", 50)));

            SourceOrder refused;
            Map<String, Long> beforeFailure;
            try (ServerHandle server = runtime.launch(Tiers.IN_PROCESS)) {
                ControlPlane control = runtime.control(server, true);
                runtime.register(control, CONNECTOR);
                control.apply(workspace(sourceUri, target.uri()));
                control.discoverSchema(SOURCE, CONNECTOR, Map.of("isUri", true, "uri", sourceUri));
                control.lifecycle(PIPELINE, LifecycleVerb.START);
                awaitRows(sourceRows, targetRows, "the snapshot");
                Await.until("the initial snapshot's sampled row counter", TIMEOUT,
                        () -> Long.valueOf(5).equals(control.snapshotRowsRead(PIPELINE).get(TABLE)),
                        () -> String.valueOf(control.snapshotRowsRead(PIPELINE)));
                assertThat(control.snapshotRowsRead(PIPELINE)).containsEntry(TABLE, 5L);
                String firstAck = Await.answered("the snapshot's target confirmation", TIMEOUT,
                        () -> control.durablePosition(PIPELINE, TABLE));

                sourceRows.updateOne(new Document("_id", "keep-1"),
                        new Document("$set", new Document("value", 21)));
                awaitRows(sourceRows, targetRows, "a live update before the fault");
                Await.answered("the live update's target confirmation", TIMEOUT,
                        () -> control.durablePosition(PIPELINE, TABLE).filter(value -> !value.equals(firstAck)));
                beforeFailure = rows(targetRows);
                target.rejectDeletes();
                sourceRows.deleteOne(new Document("_id", DELETED_ID));

                awaitState(control, PipelineState.FAILED);
                assertThat(target.allowDeletes())
                        .as("the exact target client's real delete reached the armed server fault")
                        .isPositive();
                Document deleted = Await.answered("the refused DELETE retained in the durable SRS log", TIMEOUT,
                        () -> deletedRecord(raw));
                refused = new SourceOrder(number(deleted, "epoch"),
                        number(deleted.get("_id", Document.class), "seq"));
                Document consumer = consumer(raw);
                assertThat(confirmed(consumer)).as("the consumer cannot confirm the refused DELETE")
                        .isLessThan(refused);
                assertThat(tableDone(consumer)).isLessThan(refused.seq());
                assertThat(rows(targetRows)).as("a failed write leaves the target's old row in place")
                        .isEqualTo(beforeFailure).containsKey(DELETED_ID);

                Document capture = SystemCollections.SRS_META.on(raw)
                        .find(new Document("_id", consumer.getString("miningChainId"))).first();
                assertThat(capture).containsEntry("sourceReadDurable", true);
                assertThat(new SourceOrder(number(capture, "sourceReadEpoch"), number(capture, "sourceReadSeq")))
                        .as("SRS capture can checkpoint the durable batch independently of the target")
                        .isGreaterThanOrEqualTo(refused);

                control.stop(PIPELINE, false);
                awaitState(control, PipelineState.STOPPED);
                assertThat(confirmed(consumer(raw))).as("the keeping stop preserves the confirmation floor")
                        .isLessThan(refused);
            }

            sourceRows.insertOne(row("downtime", 60));
            assertThat(rows(targetRows)).as("a stopped runtime cannot perform the pending DELETE")
                    .isEqualTo(beforeFailure);
            try (ServerHandle server = runtime.launch(Tiers.IN_PROCESS)) {
                ControlPlane control = runtime.control(server, false);
                assertThat(control.state(PIPELINE)).contains(PipelineState.STOPPED);
                assertThat(confirmed(consumer(raw))).isLessThan(refused);
                control.lifecycle(PIPELINE, LifecycleVerb.START);
                awaitRows(sourceRows, targetRows, "the pending DELETE and downtime insert after restart");
                Await.until("the replayed DELETE to become durably confirmed", TIMEOUT,
                        () -> confirmed(consumer(raw)).compareTo(refused) >= 0
                                && tableDone(consumer(raw)) >= refused.seq(),
                        () -> "consumer=" + consumer(raw));
                assertThat(rows(targetRows)).doesNotContainKey(DELETED_ID);
                String replayAck = Await.answered("the recovered target confirmation", TIMEOUT,
                        () -> control.durablePosition(PIPELINE, TABLE));

                sourceRows.updateOne(new Document("_id", "keep-2"),
                        new Document("$set", new Document("value", 75)));
                awaitRows(sourceRows, targetRows, "a fresh write after recovery");
                Await.answered("a new target confirmation after recovery", TIMEOUT,
                        () -> control.durablePosition(PIPELINE, TABLE).filter(value -> !value.equals(replayAck)));
                AtlasRuntime.assertResumedRun(control, PIPELINE, TABLE, 5L, TIMEOUT);
                runtime.assertAuthenticationBoundary();
                control.stop(PIPELINE, false);
                awaitState(control, PipelineState.STOPPED);
            }
        }
    }

    private static void awaitState(ControlPlane control, PipelineState state) {
        Await.until(PIPELINE + " to reach " + state, TIMEOUT,
                () -> control.state(PIPELINE).filter(state::equals).isPresent(),
                () -> String.valueOf(control.state(PIPELINE)) + ", logs=" + control.logs(PIPELINE));
    }

    private static void awaitRows(MongoCollection<Document> source, MongoCollection<Document> target, String phase) {
        Await.until(phase + " to reach the target", TIMEOUT, () -> rows(target).equals(rows(source)),
                () -> "source=" + rows(source) + ", target=" + rows(target));
    }

    private static Map<String, Long> rows(MongoCollection<Document> collection) {
        Map<String, Long> result = new TreeMap<>();
        for (Document row : collection.find()) result.put(row.getString("_id"), number(row, "value"));
        return result;
    }

    private static Document row(String id, int value) {
        return new Document("_id", id).append("value", value);
    }

    private static Document consumer(MongoDatabase raw) {
        Document consumer = SystemCollections.SRS_CONSUMER_OFFSETS.on(raw)
                .find(new Document("pipelineId", SrsConsumerId.of(PIPELINE, SOURCE).value())).first();
        assertThat(consumer).as("the pipeline has an actual persistent consumer checkpoint").isNotNull();
        return consumer;
    }

    private static Optional<Document> deletedRecord(MongoDatabase raw) {
        String chain = consumer(raw).getString("miningChainId");
        for (Document record : SystemCollections.SRS_LOG.on(raw).find(new Document("op", Op.DELETE.symbol()))) {
            Document key = record.get("_id", Document.class);
            if (SrsRingbuffer.ringName(chain, TABLE).equals(key.getString("ring"))) return Optional.of(record);
        }
        return Optional.empty();
    }

    private static SourceOrder confirmed(Document consumer) {
        return new SourceOrder(number(consumer, "sinkAckedEpoch"), number(consumer, "sinkAckedSeq"));
    }

    private static long tableDone(Document consumer) {
        return number(consumer.get("perTableRingDone", Document.class), TABLE);
    }

    private static long number(Document document, String field) {
        assertThat(document).as("document carrying " + field).isNotNull();
        assertThat(document.get(field)).as(field).isInstanceOf(Number.class);
        return ((Number) document.get(field)).longValue();
    }

    private static Map<String, String> workspace(String sourceUri, String targetUri) {
        Map<String, String> resources = new LinkedHashMap<>();
        resources.put(SOURCE + ".tap.yml", """
                version: tapstate/v1
                kind: source
                id: %s
                connector: mongodb-atlas
                config: %s
                mode: cdc
                tables: [ %s ]
                """.formatted(SOURCE, JsonWriter.write(Map.of("isUri", true, "uri", sourceUri)), TABLE));
        resources.put(TARGET + ".tap.yml", """
                version: tapstate/v1
                kind: source
                id: %s
                connector: mongodb-atlas
                config: %s
                """.formatted(TARGET, JsonWriter.write(Map.of("isUri", true, "uri", targetUri))));
        resources.put(PIPELINE + ".tap.yml", Workspaces.pipelineYaml(PIPELINE, SOURCE, TARGET, TABLE));
        return resources;
    }

    /** Only this owned target daemon and this target URI's application name can match the fault. */
    private static final class DeleteFailureTarget implements AutoCloseable {
        private final MongoDBContainer container;
        private final MongoClient client;
        private final String database;
        private final String application;
        private boolean armed;
        private long previousCount;

        private DeleteFailureTarget(MongoDBContainer container, String database, String application) {
            this.container = container;
            this.database = database;
            this.application = application;
            client = MongoClients.create(container.getReplicaSetUrl());
        }

        static DeleteFailureTarget start(String database, String application) {
            MongoDBContainer container = new MongoDBContainer(DockerImageName.parse("mongo:7.0"))
                    .withCommand("--replSet", "docker-rs", "--bind_ip_all",
                            "--setParameter", "enableTestCommands=1");
            try {
                container.start();
                return new DeleteFailureTarget(container, database, application);
            } catch (RuntimeException | Error failure) {
                closeOrSuppress(container::close, failure);
                throw failure;
            }
        }

        String uri() {
            String uri = container.getReplicaSetUrl(database);
            return uri + (uri.contains("?") ? "&" : "?") + "appName=" + application;
        }

        MongoDatabase database() {
            return client.getDatabase(database);
        }

        void rejectDeletes() {
            Document result = client.getDatabase("admin").runCommand(new Document("configureFailPoint", "failCommand")
                    .append("mode", "alwaysOn")
                    .append("data", new Document("failCommands", List.of("delete"))
                            .append("errorCode", 13).append("appName", application)));
            armed = true;
            previousCount = number(result, "count");
        }

        long allowDeletes() {
            Document result = client.getDatabase("admin").runCommand(
                    new Document("configureFailPoint", "failCommand").append("mode", "off"));
            armed = false;
            return number(result, "count") - previousCount;
        }

        @Override
        public void close() {
            Throwable failure = closeOrSuppress(() -> {
                if (armed) allowDeletes();
            }, null);
            failure = closeOrSuppress(client::close, failure);
            failure = closeOrSuppress(container::close, failure);
            if (failure instanceof RuntimeException runtime) throw runtime;
            if (failure instanceof Error error) throw error;
        }

        private static Throwable closeOrSuppress(Runnable action, Throwable first) {
            try {
                action.run();
            } catch (RuntimeException | Error failure) {
                if (first == null) return failure;
                first.addSuppressed(failure);
            }
            return first;
        }
    }
}
