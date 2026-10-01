package io.tapstate.adapters.mongostore;

import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import io.tapstate.core.common.TapstateException;
import io.tapstate.core.dsl.DslParser;
import io.tapstate.core.model.Resource;
import io.tapstate.core.model.canonical.CanonicalHash;
import io.tapstate.spi.store.WorkloadClaim;
import io.tapstate.spi.store.WorkloadClaimKey;
import io.tapstate.spi.store.WorkloadClaimType;
import io.tapstate.spi.store.WorkloadOwner;
import io.tapstate.testsupport.RequiresDocker;
import org.bson.Document;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.testcontainers.containers.MongoDBContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.utility.DockerImageName;

import java.io.PrintWriter;
import java.io.StringWriter;
import java.security.SecureRandom;
import java.time.Duration;
import java.util.Arrays;
import java.util.Date;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Real replica-set witnesses for automatic, shared Source keyring bootstrap and strict reload. */
@RequiresDocker
class SourceConfigKeyringStoreIT {

    private static final DockerImageName MONGO_IMAGE = DockerImageName.parse("mongo:7.0");

    @Container
    private static final MongoDBContainer REPLICA_SET = new MongoDBContainer(MONGO_IMAGE);

    private static MongoClient client;

    @AfterAll
    static void closeClient() {
        if (client != null) client.close();
    }

    @Test
    void concurrentFirstNodesConvergeOnOneKeyringAndClearBothCandidates() {
        MongoDatabase database = freshDatabase("keyring_race");
        MongoCollection<Document> systemMeta = SystemCollections.SYSTEM_META.on(database);
        RecordingRandom firstRandom = new RecordingRandom((byte) 1);
        RecordingRandom secondRandom = new RecordingRandom((byte) 2);
        SourceConfigKeyringStore first = new SourceConfigKeyringStore(database, firstRandom);
        SourceConfigKeyringStore second = new SourceConfigKeyringStore(database, secondRandom);
        CountDownLatch start = new CountDownLatch(1);

        CompletableFuture<SourceConfigCipher> firstLoad = CompletableFuture.supplyAsync(() -> load(first, start));
        CompletableFuture<SourceConfigCipher> secondLoad = CompletableFuture.supplyAsync(() -> load(second, start));
        start.countDown();
        SourceConfigCipher firstCipher = firstLoad.join();
        SourceConfigCipher secondCipher = secondLoad.join();

        assertThat(firstCipher.activeKeyId()).isEqualTo(secondCipher.activeKeyId());
        assertThat(systemMeta.countDocuments(new Document("_id", "source-config-keyring"))).isEqualTo(1);
        assertThat(firstRandom.generated()).containsOnly((byte) 0);
        assertThat(secondRandom.generated()).containsOnly((byte) 0);

        String encrypted = firstCipher.encrypt("orders", "mysql", "{\"password\":\"secret\"}");
        assertThat(secondCipher.decrypt("orders", "mysql", encrypted))
                .isEqualTo("{\"password\":\"secret\"}");
    }

    @Test
    void restartLoadsTheSameKeyAndAbsenceNeverGeneratesAReplacement() {
        MongoDatabase database = freshDatabase("keyring_restart");
        SourceConfigKeyringStore store = new SourceConfigKeyringStore(database);

        assertThatThrownBy(store::loadExistingCipher)
                .isInstanceOfSatisfying(TapstateException.class,
                        error -> assertThat(error.code()).isEqualTo(StoreError.SOURCE_CONFIG_KEYRING_INVALID));
        assertThat(SystemCollections.SYSTEM_META.on(database).countDocuments()).isZero();

        SourceConfigCipher created = store.loadOrCreateCipher();
        String envelope = created.encrypt("orders", "mysql", "{\"password\":\"secret\"}");
        SourceConfigCipher restarted = new SourceConfigKeyringStore(database).loadExistingCipher();

        assertThat(restarted.activeKeyId()).isEqualTo(created.activeKeyId());
        assertThat(restarted.decrypt("orders", "mysql", envelope))
                .isEqualTo("{\"password\":\"secret\"}");
    }

    @Test
    void concurrentRotationPreparationKeepsOneCandidateAndClearsBothLocalCopies() {
        MongoDatabase database = freshDatabase("keyring_prepare_race");
        new SourceConfigKeyringStore(database).loadOrCreateCipher();
        RecordingRandom firstRandom = new RecordingRandom((byte) 3);
        RecordingRandom secondRandom = new RecordingRandom((byte) 4);
        SourceConfigKeyringStore first = new SourceConfigKeyringStore(database, firstRandom);
        SourceConfigKeyringStore second = new SourceConfigKeyringStore(database, secondRandom);
        CountDownLatch start = new CountDownLatch(1);

        CompletableFuture<SourceConfigKeyringStore.Loaded> firstPrepare =
                CompletableFuture.supplyAsync(() -> prepare(first, start));
        CompletableFuture<SourceConfigKeyringStore.Loaded> secondPrepare =
                CompletableFuture.supplyAsync(() -> prepare(second, start));
        start.countDown();
        SourceConfigKeyringStore.Loaded firstResult = firstPrepare.join();
        SourceConfigKeyringStore.Loaded secondResult = secondPrepare.join();

        assertThat(firstResult.epoch()).isEqualTo(2);
        assertThat(secondResult.epoch()).isEqualTo(2);
        assertThat(firstResult.preparedKeyId()).isEqualTo(secondResult.preparedKeyId());
        assertThat(firstRandom.generated()).containsOnly((byte) 0);
        assertThat(secondRandom.generated()).containsOnly((byte) 0);
        assertThat(SystemCollections.SYSTEM_META.on(database)
                .find(new Document("_id", "source-config-keyring")).first()
                .getList("keys", Document.class)).hasSize(2);
    }

    @Test
    void malformedKeyringFailsClosedWithoutMaterialInDiagnostics() {
        MongoDatabase database = freshDatabase("keyring_corrupt");
        MongoCollection<Document> systemMeta = SystemCollections.SYSTEM_META.on(database);
        SourceConfigKeyringStore store = new SourceConfigKeyringStore(database);
        store.loadOrCreateCipher();
        Document keyring = systemMeta.find(new Document("_id", "source-config-keyring")).first();
        String material = keyring.getList("keys", Document.class).getFirst().getString("material");
        systemMeta.updateOne(new Document("_id", "source-config-keyring"),
                new Document("$set", new Document("keys.0.material", "not-base64@")));

        assertThatThrownBy(store::loadExistingCipher)
                .isInstanceOfSatisfying(TapstateException.class, error -> {
                    assertThat(error.code()).isEqualTo(StoreError.SOURCE_CONFIG_KEYRING_INVALID);
                    assertThat(error.getCause()).isNull();
                    StringWriter printed = new StringWriter();
                    error.printStackTrace(new PrintWriter(printed));
                    assertThat(printed.toString()).doesNotContain(material, "not-base64@");
                });
    }

    @ParameterizedTest(name = "malformed persisted {0} is rejected safely")
    @MethodSource("malformedKeyringValues")
    void malformedPersistedKeyringTypesAreCodedRatherThanTruncatedOrBareThrown(String field, Object value) {
        MongoDatabase database = freshDatabase("keyring_invalid_shape");
        MongoCollection<Document> systemMeta = SystemCollections.SYSTEM_META.on(database);
        SourceConfigKeyringStore store = new SourceConfigKeyringStore(database);
        store.loadOrCreateCipher();
        Document original = systemMeta.find(new Document("_id", "source-config-keyring")).first();
        String material = original.getList("keys", Document.class).getFirst().getString("material");
        systemMeta.updateOne(new Document("_id", "source-config-keyring"),
                new Document("$set", new Document(field, value)));

        assertThatThrownBy(store::loadExistingCipher).isInstanceOfSatisfying(TapstateException.class, error -> {
            assertThat(error.code()).isEqualTo(StoreError.SOURCE_CONFIG_KEYRING_INVALID);
            assertThat(error.getCause()).isNull();
            StringWriter printed = new StringWriter();
            error.printStackTrace(new PrintWriter(printed));
            assertThat(printed.toString()).doesNotContain(material, "malformed-keyring-input-sentinel");
        });
        assertThat(systemMeta.countDocuments(new Document("_id", "source-config-keyring"))).isEqualTo(1);
    }

    private static Stream<Arguments> malformedKeyringValues() {
        String sentinel = "malformed-keyring-input-sentinel";
        return Stream.of(
                Arguments.of("formatVersion", sentinel),
                Arguments.of("formatVersion", 1.5d),
                Arguments.of("formatVersion", 4_294_967_297L),
                Arguments.of("epoch", sentinel),
                Arguments.of("epoch", 1.5d),
                Arguments.of("activeKeyId", new Document("value", sentinel)),
                Arguments.of("preparedKeyId", 17),
                Arguments.of("preparedKeyId", (Object) null),
                Arguments.of("keys", new Document("value", sentinel)),
                Arguments.of("keys", List.of(sentinel)),
                Arguments.of("keys.0", (Object) null),
                Arguments.of("keys.0.id", 17),
                Arguments.of("keys.0.state", new Document("value", sentinel)),
                Arguments.of("keys.0.material", List.of(sentinel)));
    }

    @Test
    void rotationWaitsForEveryLiveBootThenReencryptsWithoutChangingLogicalIdentity() {
        MongoDatabase database = freshDatabase("keyring_rotation");
        SourceConfigKeyringStore keyrings = new SourceConfigKeyringStore(database);
        keyrings.loadOrCreateCipher();
        SourceConfigKeyringHandle firstHandle = new SourceConfigKeyringHandle(keyrings);
        SourceConfigKeyringHandle secondHandle = new SourceConfigKeyringHandle(keyrings);
        SourceConfigKeyringHandle staleReader = new SourceConfigKeyringHandle(keyrings);
        SourceConfigKeyringHandle staleWriter = new SourceConfigKeyringHandle(keyrings);
        MongoWorkloadClaimStore claims = new MongoWorkloadClaimStore(SystemCollections.WORKLOAD_CLAIMS.on(database));
        Duration ttl = Duration.ofSeconds(30);
        WorkloadClaim first = nodeSession(claims, "node-a", "boot-a1", ttl);
        WorkloadClaim second = nodeSession(claims, "node-b", "boot-b1", ttl);
        firstHandle.acknowledge(first, ttl);
        secondHandle.acknowledge(second, ttl);

        MongoCollection<Document> artifacts = SystemCollections.ARTIFACTS.on(database);
        MongoArtifactStore artifactStore = new MongoArtifactStore(client, artifacts, staleReader);
        Resource source = new DslParser().parse("""
                version: tapstate/v1
                kind: source
                id: orders
                connector: mysql
                config:
                  password: rotation-secret
                """);
        artifactStore.save(source);
        Document before = artifacts.find(new Document("_id", "orders")).first();
        String firstEnvelope = before.get("body", Document.class).getString("config");
        String firstKey = SourceConfigCipher.envelopeKeyId(firstEnvelope);

        assertThat(firstHandle.prepareRotation()).isEqualTo(2);
        firstHandle.acknowledge(first, ttl);
        assertThatThrownBy(firstHandle::activatePrepared)
                .isInstanceOfSatisfying(TapstateException.class,
                        error -> assertThat(error.code())
                                .isEqualTo(StoreError.SOURCE_CONFIG_KEYRING_ROTATION_BLOCKED));

        secondHandle.acknowledge(second, ttl);
        assertThat(firstHandle.activatePrepared()).isEqualTo(3);

        Document after = artifacts.find(new Document("_id", "orders")).first();
        String secondEnvelope = after.get("body", Document.class).getString("config");
        assertThat(SourceConfigCipher.envelopeKeyId(secondEnvelope)).isNotEqualTo(firstKey);
        assertThat(secondEnvelope).doesNotContain("rotation-secret");
        assertThat(after.getString("contentHash")).isEqualTo(CanonicalHash.of(source));
        assertThat(artifactStore.get("orders")).contains(source);
        assertThat(SystemCollections.SYSTEM_META.on(database)
                .find(new Document("_id", "source-config-keyring")).first()
                .getList("keys", Document.class))
                .extracting(key -> key.getString("state"))
                .containsExactlyInAnyOrder("read-only", "active");

        Resource changed = new DslParser().parse("""
                version: tapstate/v1
                kind: source
                id: orders
                connector: mysql
                config:
                  password: changed-after-rotation
                """);
        new MongoArtifactStore(client, artifacts, staleWriter).save(changed);
        String changedEnvelope = artifacts.find(new Document("_id", "orders")).first()
                .get("body", Document.class).getString("config");
        assertThat(SourceConfigCipher.envelopeKeyId(changedEnvelope))
                .isEqualTo(SourceConfigCipher.envelopeKeyId(secondEnvelope));
        assertThat(changedEnvelope).doesNotContain("changed-after-rotation");

        claims.release(first);
        WorkloadClaim replacement = nodeSession(claims, "node-a", "boot-a2", ttl);
        firstHandle.acknowledge(replacement, ttl);
        assertThatThrownBy(() -> firstHandle.acknowledge(first, ttl))
                .isInstanceOfSatisfying(TapstateException.class,
                        error -> assertThat(error.code()).isEqualTo(StoreError.SOURCE_CONFIG_KEYRING_NOT_READY));
        Document currentAck = acknowledgement(database, "node-a");
        assertThat(currentAck.getString("bootId")).isEqualTo("boot-a2");
        assertThat(currentAck.get("claimGeneration", Number.class).longValue())
                .isEqualTo(replacement.claimGeneration());

        secondHandle.release(second);
        assertThat(acknowledgement(database, "node-b").getDate("leaseUntil"))
                .isBeforeOrEqualTo(new Date(System.currentTimeMillis() + 1_000));
    }

    @Test
    void crashedNodeStopsBlockingAfterItsNodeSessionAndAcknowledgementExpire() throws Exception {
        MongoDatabase database = freshDatabase("keyring_expired_node");
        SourceConfigKeyringStore keyrings = new SourceConfigKeyringStore(database);
        keyrings.loadOrCreateCipher();
        SourceConfigKeyringHandle handle = new SourceConfigKeyringHandle(keyrings);
        MongoWorkloadClaimStore claims = new MongoWorkloadClaimStore(SystemCollections.WORKLOAD_CLAIMS.on(database));
        Duration ttl = Duration.ofMillis(250);
        WorkloadClaim crashed = nodeSession(claims, "node-crashed", "boot-crashed", ttl);
        handle.acknowledge(crashed, ttl);
        handle.prepareRotation();

        assertThatThrownBy(handle::activatePrepared)
                .isInstanceOfSatisfying(TapstateException.class,
                        error -> assertThat(error.code())
                                .isEqualTo(StoreError.SOURCE_CONFIG_KEYRING_ROTATION_BLOCKED));

        Thread.sleep(500);
        assertThat(handle.activatePrepared()).isEqualTo(3);
    }

    private static SourceConfigCipher load(SourceConfigKeyringStore store, CountDownLatch start) {
        try {
            start.await();
            return store.loadOrCreateCipher();
        } catch (InterruptedException interrupted) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException("interrupted before keyring race", interrupted);
        }
    }

    private static SourceConfigKeyringStore.Loaded prepare(
            SourceConfigKeyringStore store, CountDownLatch start) {
        try {
            start.await();
            return store.prepareRotation();
        } catch (InterruptedException interrupted) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException("interrupted before keyring preparation race", interrupted);
        }
    }

    private static WorkloadClaim nodeSession(
            MongoWorkloadClaimStore claims, String nodeId, String bootId, Duration ttl) {
        return claims.acquire(new WorkloadClaimKey("cluster-a", WorkloadClaimType.NODE_SESSION, nodeId),
                new WorkloadOwner(nodeId, bootId), 0, ttl).claim();
    }

    private static Document acknowledgement(MongoDatabase database, String nodeId) {
        return SystemCollections.SYSTEM_META.on(database)
                .find(new Document("kind", "source-config-keyring-node").append("nodeId", nodeId))
                .first();
    }

    private static MongoDatabase freshDatabase(String name) {
        if (client == null) client = MongoClients.create(REPLICA_SET.getReplicaSetUrl());
        MongoDatabase database = client.getDatabase(name);
        database.drop();
        return database;
    }

    private static final class RecordingRandom extends SecureRandom {
        private final byte fill;
        private byte[] generated;

        private RecordingRandom(byte fill) {
            this.fill = fill;
        }

        @Override
        public void nextBytes(byte[] bytes) {
            Arrays.fill(bytes, fill);
            generated = bytes;
        }

        private byte[] generated() {
            return generated;
        }
    }
}
