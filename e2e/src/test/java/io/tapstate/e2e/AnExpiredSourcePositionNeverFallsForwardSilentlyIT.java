package io.tapstate.e2e;

import com.mongodb.ConnectionString;
import com.mongodb.MongoCommandException;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoDatabase;
import io.tapstate.adapters.mongostore.MongoStorePort;
import io.tapstate.adapters.pdk.ConnectorError;
import io.tapstate.control.core.ClusterRecoveryItemView;
import io.tapstate.core.lifecycle.LifecycleVerb;
import io.tapstate.core.lifecycle.PipelineState;
import io.tapstate.spi.store.ClusterRecoveryStatus;
import io.tapstate.spi.store.SrsConsumerId;
import io.tapstate.testsupport.DockerGate;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicLong;
import org.bson.BsonDocument;
import org.bson.BsonTimestamp;
import org.bson.Document;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.testcontainers.containers.MongoDBContainer;
import org.testcontainers.utility.DockerImageName;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * A genuine source-log rollover is refused through the unchanged Mongo connector and packaged
 * cluster. An independent later queue item still recovers. These local processes do not substitute
 * for an acceptance run on independent hosts.
 */
class AnExpiredSourcePositionNeverFallsForwardSilentlyIT {
    private static final String CONNECTOR = "mongodb";
    private static final String EXPIRED = "a_expired_position";
    private static final String FOLLOWING = "z_following_source";
    private static final String EXPIRED_SOURCE = "expired_source";
    private static final String LIVE_SOURCE = "live_source";
    private static final String EXPIRED_TABLE = "expired_orders";
    private static final String LIVE_TABLE = "live_orders";
    private static final List<String> NODES = List.of("expiry-a", "expiry-b", "expiry-c");
    private static final Duration BOUND = Duration.ofMinutes(3);
    private static final long MAX_CHURN_BYTES = 64L * 1024 * 1024;

    @BeforeAll
    static void requireDockerAndTheRealConnector() {
        DockerGate.require();
        RealConnectorGate.require(CONNECTOR);
    }

    @Test
    void aRealExpiredResumeIsTerminalWithoutBlockingTheFollowingSource(@TempDir Path temporary) throws Exception {
        String storeUri = SharedMongo.replicaSetUrl("e2e_expired_position_state");
        String targetUri = SharedMongo.replicaSetUrl("e2e_expired_position_target");
        String liveUri = SharedMongo.replicaSetUrl("e2e_expired_position_live");
        Path readWitness = temporary.resolve("mongo-reader-events");
        byte[] connector = ObservedMongoConnectorJar.build(ConnectorJars.bytesFor(CONNECTOR),
                temporary.resolve("mongo-writes"), readWitness);
        // Only this source has a deliberately bounded log. The shared state/target/source replica
        // set retains its original configuration for every other specification in the JVM.
        try (MongoDBContainer sourceSet = new MongoDBContainer(DockerImageName.parse("mongo:7.0"))
                .withCommand("--replSet", "docker-rs", "--bind_ip_all", "--oplogSize", "1",
                        "--oplogMinRetentionHours", "0")) {
            sourceSet.start();
            String expiredUri = sourceSet.getReplicaSetUrl("e2e_expired_position_source");
            try (MongoClient source = MongoClients.create(expiredUri);
                    MongoClient live = MongoClients.create(liveUri);
                    MongoClient targets = MongoClients.create(targetUri);
                    MongoClient state = MongoClients.create(storeUri)) {
                MongoDatabase sourceDb = database(source, expiredUri);
                MongoDatabase liveDb = database(live, liveUri);
                MongoDatabase targetDb = database(targets, targetUri);
                MongoDatabase store = database(state, storeUri);
                seed(sourceDb, EXPIRED_TABLE);
                seed(liveDb, LIVE_TABLE);
                try (PartitionableCluster cluster = PartitionableCluster.start(storeUri,
                        "real-expired-position", NODES, Duration.ofSeconds(9))) {
                    NODES.forEach(node -> cluster.awaitExactly(node, NODES.size(), BOUND));
                    ControlPlane first = cluster.member(NODES.getFirst());
                    first.registerConnector(CONNECTOR, connector);
                    first.apply(resources(expiredUri, liveUri, targetUri));
                    first.discoverSchema(EXPIRED_SOURCE, CONNECTOR, Map.of("uri", expiredUri));
                    first.discoverSchema(LIVE_SOURCE, CONNECTOR, Map.of("uri", liveUri));
                    first.lifecycle(EXPIRED, LifecycleVerb.START);
                    first.lifecycle(FOLLOWING, LifecycleVerb.START);
                    awaitState(first, EXPIRED, PipelineState.RUNNING);
                    awaitState(first, FOLLOWING, PipelineState.RUNNING);
                    awaitValue(targetDb, EXPIRED_TABLE, "seed");
                    awaitValue(targetDb, LIVE_TABLE, "seed");
                    change(sourceDb, EXPIRED_TABLE, "confirmed-before-expiry");
                    change(liveDb, LIVE_TABLE, "confirmed-before-restart");
                    awaitValue(targetDb, EXPIRED_TABLE, "confirmed-before-expiry");
                    awaitValue(targetDb, LIVE_TABLE, "confirmed-before-restart");
                    Await.until("the expired source's confirmed native token to be durable", BOUND,
                            () -> {
                                Retained position = confirmed(store);
                                return position != null && delivery(readWitness, position.token()).isPresent();
                            },
                            () -> String.valueOf(consumer(store)) + "; " + lines(readWitness));
                    long originalGeneration = first.executionGenerationOf(EXPIRED).orElseThrow();
                    assertThat(first.captureOwnersOf(EXPIRED)).hasSize(1);
                    String originalCapture = first.captureOwnersOf(EXPIRED).keySet().iterator().next();
                    cluster.killAll();
                    assertThat(NODES).allSatisfy(node -> assertThat(cluster.isAlive(node)).isFalse());
                    Retained retained = confirmed(store);
                    assertThat(retained).isNotNull();
                    String nativeToken = delivery(readWitness, retained.token()).orElseThrow();
                    BsonDocument resumeToken = BsonDocument.parse(nativeToken);
                    assertThat(resumeToken).containsKey("_data");
                    int beforeRestart = lines(readWitness).size();
                    // No old reader process is alive when this server-time upper bound is taken.
                    BsonTimestamp upper = source.getDatabase("admin").runCommand(new Document("hello", 1))
                            .get("operationTime", BsonTimestamp.class);
                    assertThat(upper).isNotNull();
                    rollPast(source, sourceDb, upper);
                    assertThat(oldest(source).compareTo(upper)).isPositive();
                    assertThatThrownBy(() -> {
                        try (var cursor = sourceDb.watch().startAfter(resumeToken).cursor()) {
                            cursor.tryNext();
                        }
                    }).isInstanceOf(MongoCommandException.class)
                            .satisfies(failure -> assertThat(((MongoCommandException) failure).getErrorCode()).isEqualTo(286));
                    Await.until("the old profile authorization horizon to expire on Mongo time", BOUND,
                            () -> store.getCollection(MongoStorePort.CLUSTER_EXECUTION_PROFILES).find(
                                    new Document("_id", cluster.clusterId()).append("$expr",
                                            new Document("$lte", List.of("$authorizationUntil", "$$NOW"))))
                                    .first() != null,
                            () -> String.valueOf(store.getCollection(MongoStorePort.CLUSTER_EXECUTION_PROFILES)
                                    .find(new Document("_id", cluster.clusterId())).first()));
                    cluster.relaunchAll();
                    ControlPlane resumed = cluster.member(NODES.getFirst());
                    Await.until("the real connector to retry the exact expired request", BOUND,
                            () -> after(readWitness, beforeRestart).stream()
                                    .anyMatch(line -> line.startsWith("START\t" + EXPIRED_TABLE + "\t")),
                            () -> after(readWitness, beforeRestart).toString());
                    change(sourceDb, EXPIRED_TABLE, "after-expiry-sentinel");
                    change(liveDb, LIVE_TABLE, "after-restart-sentinel");
                    ClusterRecoveryItemView failed = Await.answered("the expired queue item to be terminal", BOUND,
                            () -> item(resumed, EXPIRED)
                                    .filter(item -> ClusterRecoveryStatus.REBUILD_FAILED.name().equals(item.persistedStatus())));
                    ClusterRecoveryItemView recovered = Await.answered("the following source to recover", BOUND,
                            () -> item(resumed, FOLLOWING).filter(item -> "RECOVERED".equals(item.persistedStatus())));
                    assertThat(failed.originalExecutionGeneration()).isEqualTo(originalGeneration);
                    assertThat(failed.attempt()).isEqualTo(1);
                    assertThat(failed.diagnostic()).isNotNull();
                    assertThat(failed.diagnostic().code()).isEqualTo(ConnectorError.RESUME_POSITION_REJECTED.code());
                    assertThat(failed.diagnostic().params()).containsEntry("pdkCode", "10003")
                            .containsEntry("requested", retained.token());
                    assertThat(failed.originalPositions().get(EXPIRED_SOURCE)).satisfies(position -> {
                        assertThat(position.captureId()).isEqualTo(originalCapture);
                        assertThat(position.token()).isEqualTo(retained.token());
                        assertThat(position.epoch()).isEqualTo(retained.epoch());
                        assertThat(position.sequence()).isEqualTo(retained.sequence());
                    });
                    assertThat(failed.successor().requestedPositions().get(EXPIRED_SOURCE).token()).isEqualTo(retained.token());
                    List<String> observed = after(readWitness, beforeRestart);
                    assertThat(observed.stream().filter(line -> line.startsWith("START\t" + EXPIRED_TABLE + "\t")))
                            .allSatisfy(line -> assertThat(line.split("\t", 4)[2]).isEqualTo(retained.token()));
                    assertThat(observed).anyMatch(line -> line.startsWith("REFUSED\t" + EXPIRED_TABLE + "\t")
                            && line.split("\t", 6)[4].equals("10003"));
                    assertThat(observed).noneMatch(line -> line.startsWith("DELIVERY\t" + EXPIRED_TABLE + "\t"));
                    assertThat(targetDb.getCollection(EXPIRED_TABLE).find(new Document("_id", 1)).first().getString("value"))
                            .isEqualTo("confirmed-before-expiry");
                    assertThat(recovered.enqueueSequence()).isGreaterThan(failed.enqueueSequence());
                    awaitValue(targetDb, LIVE_TABLE, "after-restart-sentinel");
                    awaitState(resumed, FOLLOWING, PipelineState.RUNNING);
                }
            }
        }
    }

    private static void rollPast(MongoClient source, MongoDatabase database, BsonTimestamp upper) {
        String payload = "x".repeat(64 * 1024);
        AtomicLong bytes = new AtomicLong();
        AtomicLong ids = new AtomicLong();
        Await.until("the owned source oplog to move beyond the old reader's upper bound", BOUND, () -> {
            if (oldest(source).compareTo(upper) > 0) return true;
            if (bytes.get() < MAX_CHURN_BYTES) {
                List<Document> batch = new ArrayList<>();
                for (int count = 0; count < 16; count++) {
                    batch.add(new Document("_id", ids.incrementAndGet()).append("payload", payload));
                }
                database.getCollection("owned_oplog_churn").insertMany(batch);
                bytes.addAndGet((long) payload.length() * batch.size());
            }
            return false;
        }, () -> "oldest=" + oldest(source) + ", upper=" + upper + ", churnBytes=" + bytes.get());
    }

    private static BsonTimestamp oldest(MongoClient source) {
        Document entry = source.getDatabase("local").getCollection("oplog.rs")
                .find().sort(new Document("$natural", 1)).first();
        if (entry == null) throw new AssertionError("the owned replica set has no oplog entry");
        return entry.get("ts", BsonTimestamp.class);
    }

    private static Document consumer(MongoDatabase store) {
        return store.getCollection(MongoStorePort.SRS_CONSUMER_OFFSETS)
                .find(new Document("pipelineId", SrsConsumerId.of(EXPIRED, EXPIRED_SOURCE).value())).first();
    }

    private static Retained confirmed(MongoDatabase store) {
        Document consumer = consumer(store);
        if (consumer == null || !(consumer.get("sinkAckedByTable") instanceof Document tables)
                || !(tables.get(EXPIRED_TABLE) instanceof Document point)
                || point.getString("sinkAckedSrcpos") == null
                || !(point.get("sinkAckedEpoch") instanceof Number epoch)
                || !(point.get("sinkAckedSeq") instanceof Number sequence)) return null;
        return new Retained(point.getString("sinkAckedSrcpos"), epoch.longValue(), sequence.longValue());
    }

    private static Optional<String> delivery(Path path, String token) {
        return lines(path).stream().filter(line -> line.startsWith("DELIVERY\t" + EXPIRED_TABLE + "\t"))
                .map(line -> line.split("\t", 4)).filter(fields -> fields.length == 4 && fields[2].equals(token))
                .map(fields -> fields[3]).findFirst();
    }

    private static List<String> lines(Path path) {
        try { return Files.exists(path) ? Files.readAllLines(path) : List.of(); }
        catch (IOException failure) { throw new UncheckedIOException(failure); }
    }

    private static List<String> after(Path path, int before) {
        List<String> all = lines(path);
        return all.subList(Math.min(before, all.size()), all.size());
    }

    private static Optional<ClusterRecoveryItemView> item(ControlPlane control, String pipeline) {
        return control.pipelineRecovery(pipeline).items().stream()
                .max(java.util.Comparator.comparingLong(ClusterRecoveryItemView::enqueueSequence));
    }

    private static MongoDatabase database(MongoClient client, String uri) {
        return client.getDatabase(new ConnectionString(uri).getDatabase());
    }

    private static void seed(MongoDatabase database, String table) {
        database.getCollection(table).insertOne(new Document("_id", 1).append("value", "seed"));
    }

    private static void change(MongoDatabase database, String table, String value) {
        database.getCollection(table).updateOne(new Document("_id", 1),
                new Document("$set", new Document("value", value)));
    }

    private static void awaitValue(MongoDatabase database, String table, String value) {
        Await.until(table + " to hold " + value, BOUND,
                () -> database.getCollection(table).find(new Document("_id", 1).append("value", value)).first() != null,
                () -> String.valueOf(database.getCollection(table).find(new Document("_id", 1)).first()));
    }

    private static void awaitState(ControlPlane control, String pipeline, PipelineState expected) {
        Await.until(pipeline + " to reach " + expected, BOUND,
                () -> control.state(pipeline).filter(expected::equals).isPresent(),
                () -> String.valueOf(control.state(pipeline)));
    }

    private static Map<String, String> resources(String expired, String live, String target) {
        Map<String, String> resources = new LinkedHashMap<>();
        add(resources, EXPIRED, EXPIRED_SOURCE, EXPIRED_TABLE, expired, target);
        add(resources, FOLLOWING, LIVE_SOURCE, LIVE_TABLE, live, target);
        return resources;
    }

    private static void add(Map<String, String> resources, String pipeline, String source, String table,
            String uri, String targetUri) {
        String target = source + "_target";
        resources.put(source + ".tap.yml", """
                version: tapstate/v1
                kind: source
                id: %s
                connector: mongodb
                config: { uri: "%s" }
                mode: cdc
                tables: [ %s ]
                """.formatted(source, uri, table));
        resources.put(target + ".tap.yml", """
                version: tapstate/v1
                kind: source
                id: %s
                connector: mongodb
                config: { uri: "%s" }
                """.formatted(target, targetUri));
        resources.put(pipeline + ".tap.yml", """
                version: tapstate/v1
                kind: pipeline
                id: %s
                source:
                  - { id: %s, srs: false }
                settings: { read_mode: snapshot_and_cdc }
                transforms:
                  - { id: rows_through, from: [%s], type: filter, expr: "true" }
                serve:
                  from: rows_through
                  sync:
                    - source: %s
                """.formatted(pipeline, source, table, target));
    }

    private record Retained(String token, long epoch, long sequence) {}
}
