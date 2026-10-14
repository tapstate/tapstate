package io.tapstate.adapters.mongostore;

import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import com.mongodb.ConnectionString;
import com.mongodb.MongoClientSettings;
import com.mongodb.MongoException;
import com.mongodb.event.CommandListener;
import com.mongodb.event.CommandFailedEvent;
import com.mongodb.MongoWriteException;
import com.mongodb.event.CommandStartedEvent;
import com.mongodb.event.CommandSucceededEvent;
import io.tapstate.core.common.TapstateException;
import io.tapstate.core.dsl.DslParser;
import io.tapstate.core.model.Resource;
import io.tapstate.core.model.canonical.CanonicalHash;
import io.tapstate.spi.store.IoError;
import io.tapstate.spi.store.WorkloadClaim;
import io.tapstate.spi.store.ArtifactMutation;
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
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
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
    void concurrentSourceCreatesWaitForTheKeyringFenceWithoutReplacingTheWinner() throws Exception {
        MongoDatabase database = freshDatabase("keyring_source_create_race");
        new SourceConfigKeyringStore(client, database).loadOrCreateCipher();
        CountDownLatch fenced = new CountDownLatch(1);
        CountDownLatch release = new CountDownLatch(1);
        CountDownLatch conflicts = new CountDownLatch(3);
        AtomicInteger fenceRequest = new AtomicInteger(-1);
        AtomicReference<Throwable> hookFailure = new AtomicReference<>();
        CommandListener holdFence = new CommandListener() {
            @Override public void commandStarted(CommandStartedEvent event) {
                if (!"update".equals(event.getCommandName())) return;
                var update = event.getCommand().getArray("updates").get(0).asDocument().getDocument("u");
                if (update.containsKey("$inc") && update.getDocument("$inc").containsKey("sourceWriteFence")) {
                    fenceRequest.set(event.getRequestId());
                }
            }

            @Override public void commandSucceeded(CommandSucceededEvent event) {
                if (event.getRequestId() != fenceRequest.get()) return;
                fenced.countDown();
                try {
                    if (!release.await(10, TimeUnit.SECONDS)) throw new AssertionError("Source fence was not released");
                } catch (Throwable failure) {
                    hookFailure.set(failure);
                    if (failure instanceof InterruptedException) Thread.currentThread().interrupt();
                }
            }
        };
        CommandListener observeConflicts = new CommandListener() {
            @Override public void commandFailed(CommandFailedEvent event) {
                if (event.getThrowable() instanceof MongoException error && error.getCode() == 112
                        && error.hasErrorLabel("TransientTransactionError")) {
                    conflicts.countDown();
                }
            }
        };
        String uri = REPLICA_SET.getReplicaSetUrl(database.getName());
        MongoClientSettings holdingSettings = MongoClientSettings.builder()
                .applyConnectionString(new ConnectionString(uri)).addCommandListener(holdFence).build();
        MongoClientSettings waitingSettings = MongoClientSettings.builder()
                .applyConnectionString(new ConnectionString(uri)).addCommandListener(observeConflicts).build();
        Resource winnerSource = new DslParser().parse("""
                version: tapstate/v1
                kind: source
                id: managed_views
                connector: mongodb
                config: { uri: winner-config-sentinel }
                """);
        Resource losingSource = new DslParser().parse("""
                version: tapstate/v1
                kind: source
                id: managed_views
                connector: mongodb
                config: { uri: losing-config-sentinel }
                """);
        try (MongoClient holding = MongoClients.create(holdingSettings);
                MongoClient waiting = MongoClients.create(waitingSettings)) {
            MongoDatabase holdingDatabase = holding.getDatabase(database.getName());
            MongoDatabase waitingDatabase = waiting.getDatabase(database.getName());
            MongoArtifactStore winner = new MongoArtifactStore(holding, SystemCollections.ARTIFACTS.on(holdingDatabase),
                    new SourceConfigKeyringHandle(new SourceConfigKeyringStore(holding, holdingDatabase)));
            MongoArtifactStore loser = new MongoArtifactStore(waiting, SystemCollections.ARTIFACTS.on(waitingDatabase),
                    new SourceConfigKeyringHandle(new SourceConfigKeyringStore(waiting, waitingDatabase)));
            CompletableFuture<ArtifactMutation> creating = CompletableFuture.supplyAsync(() -> winner.create(winnerSource));
            CompletableFuture<ArtifactMutation> contending = null;
            try {
                assertThat(fenced.await(5, TimeUnit.SECONDS)).as("the first Source transaction owns the fence").isTrue();
                contending = CompletableFuture.supplyAsync(() -> loser.create(losingSource));
                // Keep the real write conflict alive across the former retry limit, then let the holder commit.
                assertThat(conflicts.await(5, TimeUnit.SECONDS)).as("three real transient write conflicts occurred").isTrue();
                release.countDown();
                assertThat(creating.get(5, TimeUnit.SECONDS)).isEqualTo(ArtifactMutation.CREATED);
                assertThat(contending.get(5, TimeUnit.SECONDS)).isEqualTo(ArtifactMutation.ALREADY_EXISTS);
                assertThat(loser.get(winnerSource.id())).contains(winnerSource);
                assertThat(SystemCollections.ARTIFACTS.on(database).countDocuments()).isEqualTo(1);
                assertThat(hookFailure.get()).isNull();
            } finally {
                release.countDown();
                creating.handle((result, failure) -> null).get(5, TimeUnit.SECONDS);
                if (contending != null) contending.handle((result, failure) -> null).get(5, TimeUnit.SECONDS);
            }
        }
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
        SourceConfigKeyringStore keyrings = new SourceConfigKeyringStore(client, database);
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
        artifacts.updateOne(new Document("_id", "orders"), new Document("$set",
                new Document("storageProvenance", new Document("marker", "preserved-storage-field"))));
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
        assertThat(after.get("storageProvenance")).isEqualTo(before.get("storageProvenance"));
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
        SourceConfigKeyringStore keyrings = new SourceConfigKeyringStore(client, database);
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

    @Test
    void aSourceWriteWhoseKeyWasLoadedBeforeActivationCannotPersistWithTheRetiredWriterKey() {
        MongoDatabase database = freshDatabase("keyring_late_writer");
        SourceConfigKeyringStore keyrings = new SourceConfigKeyringStore(client, database);
        keyrings.loadOrCreateCipher();
        SourceConfigKeyringHandle writer = new SourceConfigKeyringHandle(keyrings);
        SourceConfigKeyringHandle rotation = new SourceConfigKeyringHandle(keyrings);
        AtomicBoolean activated = new AtomicBoolean();
        SourceConfigCipherProvider delayed = new SourceConfigCipherProvider() {
            @Override public SourceConfigCipher current() { return writer.current(); }
            @Override public boolean requiresWriteFence() { return writer.requiresWriteFence(); }
            @Override public boolean fenceWrite(SourceConfigWriteScope scope, String keyId) {
                return writer.fenceWrite(scope, keyId);
            }

            @Override public SourceConfigCipher refresh() {
                SourceConfigCipher readBeforeActivation = writer.refresh();
                if (activated.compareAndSet(false, true)) {
                    rotation.prepareRotation();
                    rotation.activatePrepared();
                }
                // A real writer can be descheduled after its keyring read and before its Mongo write.
                return readBeforeActivation;
            }
        };
        MongoCollection<Document> artifacts = SystemCollections.ARTIFACTS.on(database);
        MongoArtifactStore store = new MongoArtifactStore(client, artifacts, delayed);
        Resource source = new DslParser().parse("""
                version: tapstate/v1
                kind: source
                id: delayed_writer
                connector: mysql
                config: { password: delayed-config-sentinel }
                """);
        store.save(source);
        assertThat(activated).isTrue();
        String active = keyrings.loadExistingCipher().activeKeyId();
        Document stored = artifacts.find(new Document("_id", "delayed_writer")).first();
        assertThat(stored.get("body", Document.class).getString("config"))
                .as("activation must fence or retry the in-flight old-key write")
                .startsWith("tscfg:1:" + active + ":");
        assertThat(stored.getString("contentHash")).isEqualTo(CanonicalHash.of(source));
    }

    @Test
    void overlappingRotatorsCannotRewriteANewerEnvelopeWithAnEarlierActiveKey() {
        MongoDatabase database = freshDatabase("keyring_overlapping_rotation");
        SourceConfigKeyringStore baseline = new SourceConfigKeyringStore(client, database);
        baseline.loadOrCreateCipher();
        SourceConfigKeyringHandle later = new SourceConfigKeyringHandle(baseline);
        MongoCollection<Document> artifacts = SystemCollections.ARTIFACTS.on(database);
        Resource source = new DslParser().parse("""
                version: tapstate/v1
                kind: source
                id: overlapping_rotation
                connector: mysql
                config: { password: overlapping-rotation-sentinel }
                """);
        new MongoArtifactStore(client, artifacts, baseline.loadExistingCipher()).save(source);
        AtomicInteger activationRequest = new AtomicInteger(-1);
        AtomicInteger phase = new AtomicInteger();
        AtomicReference<Throwable> hookFailure = new AtomicReference<>();
        CommandListener ordering = new CommandListener() {
            @Override public void commandStarted(CommandStartedEvent event) {
                if (!"update".equals(event.getCommandName())) return;
                var updates = event.getCommand().getArray("updates");
                var replacement = updates.get(0).asDocument().getDocument("u");
                if (replacement.containsKey("activeKeyId") && replacement.getNumber("epoch").longValue() == 3) {
                    activationRequest.set(event.getRequestId());
                }
            }

            @Override public void commandSucceeded(CommandSucceededEvent event) {
                try {
                    if (event.getRequestId() == activationRequest.get() && phase.compareAndSet(0, 1)) {
                        // Persist K3 before the first rotator loads its post-activation K2 view.
                        later.prepareRotation();
                        return;
                    }
                    if (!"find".equals(event.getCommandName()) || phase.get() != 1) return;
                    var cursor = event.getResponse().getDocument("cursor", null);
                    if (cursor == null || !cursor.containsKey("firstBatch")) return;
                    var rows = cursor.getArray("firstBatch");
                    if (rows.isEmpty() || !rows.get(0).isDocument()) return;
                    var row = rows.get(0).asDocument();
                    if (!row.containsKey("preparedKeyId") || !row.containsKey("epoch")
                            || row.getNumber("epoch").longValue() != 4) return;
                    if (phase.compareAndSet(1, 2)) later.activatePrepared();
                } catch (Throwable failure) {
                    hookFailure.compareAndSet(null, failure);
                }
            }
        };
        MongoClientSettings settings = MongoClientSettings.builder()
                .applyConnectionString(new ConnectionString(REPLICA_SET.getReplicaSetUrl(database.getName())))
                .addCommandListener(ordering).build();
        try (MongoClient firstClient = MongoClients.create(settings)) {
            SourceConfigKeyringHandle first = new SourceConfigKeyringHandle(
                    new SourceConfigKeyringStore(firstClient, firstClient.getDatabase(database.getName())));
            first.prepareRotation();
            first.activatePrepared();
        }
        assertThat(hookFailure.get()).as("both controlled rotation transitions executed successfully").isNull();
        assertThat(phase.get()).isEqualTo(2);
        String active = baseline.loadExistingCipher().activeKeyId();
        Document stored = artifacts.find(new Document("_id", source.id())).first();
        assertThat(stored.get("body", Document.class).getString("config"))
                .as("an earlier rotator must not downgrade a completed newer rotation")
                .startsWith("tscfg:1:" + active + ":");
        assertThat(stored.getString("contentHash")).isEqualTo(CanonicalHash.of(source));
    }

    @Test
    void aLiveNodeAckSurvivesThreeActualKeyringConflictsWhenThePeerCommits() throws Exception {
        heldPeerAcknowledgement(true, false);
    }

    @Test
    void aPersistentlyHeldPeerRefusesAckWithinABoundWithoutPublishingPartialProof() throws Exception {
        heldPeerAcknowledgement(false, false);
    }

    @Test
    void aLiveNodeAckSurvivesThreeActualNodeTouchConflictsWhenThePeerCommits() throws Exception {
        heldPeerAcknowledgement(true, true);
    }

    @Test
    void aPersistentlyHeldNodeRefusesAckWithinABoundWithoutPublishingPartialProof() throws Exception {
        heldPeerAcknowledgement(false, true);
    }

    private void heldPeerAcknowledgement(boolean releaseAfterThirdConflict, boolean holdNode) throws Exception {
        String name = "keyring_ack_" + (holdNode ? "node_" : "ring_")
                + (releaseAfterThirdConflict ? "peer_commits" : "peer_remains_held");
        MongoDatabase database = freshDatabase(name);
        SourceConfigKeyringStore setup = new SourceConfigKeyringStore(client, database);
        setup.loadOrCreateCipher();
        MongoWorkloadClaimStore claims = new MongoWorkloadClaimStore(SystemCollections.WORKLOAD_CLAIMS.on(database));
        Duration ttl = Duration.ofSeconds(30);
        WorkloadClaim first = nodeSession(claims, "node-a", "boot-a", ttl);
        WorkloadClaim second = nodeSession(claims, "node-b", "boot-b", ttl);
        WorkloadClaim heldNode = holdNode ? first : second;
        long epoch = setup.loadExisting().epoch();
        CountDownLatch peerOwnsRing = new CountDownLatch(1);
        CountDownLatch releasePeer = new CountDownLatch(1);
        CountDownLatch threeRealConflicts = new CountDownLatch(3);
        AtomicInteger heldRingRequest = new AtomicInteger(-1);
        AtomicInteger labeledConflicts = new AtomicInteger();
        List<String> actualErrors = new java.util.concurrent.CopyOnWriteArrayList<>();
        Set<Integer> contenderRequests = java.util.concurrent.ConcurrentHashMap.newKeySet();
        CommandListener heldPeer = new CommandListener() {
            @Override public void commandStarted(CommandStartedEvent event) {
                if (ackContendedWrite(event, name, holdNode)) heldRingRequest.set(event.getRequestId());
            }
            @Override public void commandSucceeded(CommandSucceededEvent event) {
                if (event.getRequestId() != heldRingRequest.get()) return;
                if (holdNode) {
                    assertThat(event.getResponse().get("value")).isInstanceOf(org.bson.BsonDocument.class);
                } else {
                    assertThat(event.getResponse().getNumber("nModified").intValue()).isEqualTo(1);
                }
                peerOwnsRing.countDown();
                try {
                    if (!releasePeer.await(20, TimeUnit.SECONDS)) throw new AssertionError("the peer ACK transaction was not released");
                } catch (InterruptedException interrupted) {
                    Thread.currentThread().interrupt();
                    throw new AssertionError("the peer ACK transaction was interrupted", interrupted);
                }
            }
        };
        CommandListener observeContender = new CommandListener() {
            @Override public void commandStarted(CommandStartedEvent event) {
                if (ackContendedWrite(event, name, holdNode)) contenderRequests.add(event.getRequestId());
            }
            @Override public void commandFailed(CommandFailedEvent event) {
                if (!contenderRequests.remove(event.getRequestId()) || !(event.getThrowable() instanceof MongoException failure)) return;
                int code = failure instanceof MongoWriteException write ? write.getError().getCode() : failure.getCode();
                observe(code, failure.hasErrorLabel("TransientTransactionError"),
                        failure.hasErrorLabel("UnknownTransactionCommitResult"));
            }
            @Override public void commandSucceeded(CommandSucceededEvent event) {
                if (!contenderRequests.remove(event.getRequestId())) return;
                var reply = event.getResponse();
                boolean transientError = reply.containsKey("errorLabels") && reply.getArray("errorLabels").stream()
                        .anyMatch(label -> label.isString() && "TransientTransactionError".equals(label.asString().getValue()));
                boolean ambiguous = reply.containsKey("errorLabels") && reply.getArray("errorLabels").stream()
                        .anyMatch(label -> label.isString() && "UnknownTransactionCommitResult".equals(label.asString().getValue()));
                if (reply.containsKey("writeErrors")) {
                    reply.getArray("writeErrors").forEach(error -> observe(error.asDocument().getNumber("code").intValue(),
                            transientError, ambiguous));
                }
            }
            private void observe(int code, boolean transientError, boolean ambiguous) {
                actualErrors.add("code=" + code + ",transient=" + transientError + ",ambiguous=" + ambiguous);
                if (code == 112 && transientError && !ambiguous) {
                    labeledConflicts.incrementAndGet();
                    threeRealConflicts.countDown();
                }
            }
        };
        MongoClientSettings peerSettings = MongoClientSettings.builder()
                .applyConnectionString(new ConnectionString(REPLICA_SET.getReplicaSetUrl()))
                .addCommandListener(heldPeer).build();
        MongoClientSettings contenderSettings = MongoClientSettings.builder()
                .applyConnectionString(new ConnectionString(REPLICA_SET.getReplicaSetUrl()))
                .addCommandListener(observeContender).build();
        try (MongoClient peerClient = MongoClients.create(peerSettings);
                MongoClient contenderClient = MongoClients.create(contenderSettings)) {
            SourceConfigKeyringStore peer = new SourceConfigKeyringStore(peerClient, peerClient.getDatabase(name));
            SourceConfigKeyringStore contender = new SourceConfigKeyringStore(contenderClient, contenderClient.getDatabase(name));
            var workers = Executors.newFixedThreadPool(2);
            try {
                var winning = workers.submit(() -> peer.acknowledge(heldNode, epoch, ttl));
                assertThat(peerOwnsRing.await(10, TimeUnit.SECONDS))
                        .as("the peer actually owns the uncommitted %s write", holdNode ? "NODE" : "KEYRING").isTrue();
                var losing = workers.submit(() -> {
                    try {
                        return new AckOutcome(contender.acknowledge(first, epoch, ttl), null);
                    } catch (TapstateException coded) {
                        return new AckOutcome(false, coded);
                    }
                });
                assertThat(threeRealConflicts.await(10, TimeUnit.SECONDS))
                        .as("three actual code112 transient responses must precede the release; observed=%s", actualErrors).isTrue();
                if (releaseAfterThirdConflict) {
                    releasePeer.countDown();
                    assertThat(winning.get(5, TimeUnit.SECONDS)).isTrue();
                    AckOutcome result = losing.get(5, TimeUnit.SECONDS);
                    assertThat(result.failure()).as("bounded peer contention must not fail an otherwise live exact ACK").isNull();
                    assertThat(result.applied()).isTrue();
                    assertExactAck(database, first, epoch);
                    if (!holdNode) assertExactAck(database, second, epoch);
                } else {
                    AckOutcome result = losing.get(5, TimeUnit.SECONDS);
                    assertThat(result.failure()).isNotNull();
                    assertThat(result.failure().code()).isEqualTo(IoError.STORE_UNAVAILABLE);
                    assertThat(result.applied()).isFalse();
                    assertThat(acknowledgement(database, first.key().resourceId())).isNull();
                    assertThat(acknowledgement(database, second.key().resourceId())).isNull();
                    releasePeer.countDown();
                    assertThat(winning.get(5, TimeUnit.SECONDS)).isTrue();
                    assertThat(contender.acknowledge(first, epoch, ttl)).isTrue();
                    assertExactAck(database, first, epoch);
                }
                assertThat(labeledConflicts.get()).isGreaterThanOrEqualTo(3);
                assertThat(setup.loadExisting().epoch()).isEqualTo(epoch);
            } finally {
                releasePeer.countDown();
                workers.shutdownNow();
                assertThat(workers.awaitTermination(10, TimeUnit.SECONDS)).isTrue();
            }
        }
    }

    private static boolean ackContendedWrite(CommandStartedEvent event, String database, boolean node) {
        if (!database.equals(event.getDatabaseName())) return false;
        var command = event.getCommand();
        if (node) {
            if (!"findAndModify".equals(event.getCommandName())
                    || !SystemCollections.WORKLOAD_CLAIMS.collectionName()
                            .equals(command.getString("findAndModify").getValue())) return false;
            var update = command.getDocument("update");
            return update.containsKey("$inc") && update.getDocument("$inc").containsKey("sourceConfigAckFence");
        }
        if (!"update".equals(event.getCommandName())
                || !SystemCollections.SYSTEM_META.collectionName().equals(command.getString("update").getValue())) return false;
        var update = command.getArray("updates").get(0).asDocument().getDocument("u");
        return update.containsKey("$inc") && update.getDocument("$inc").containsKey("nodeAckFence");
    }

    private static void assertExactAck(MongoDatabase database, WorkloadClaim node, long epoch) {
        Document ack = acknowledgement(database, node.key().resourceId());
        assertThat(ack).isNotNull();
        assertThat(ack.getString("bootId")).isEqualTo(node.owner().bootId());
        assertThat(ack.get("claimGeneration", Number.class).longValue()).isEqualTo(node.claimGeneration());
        assertThat(ack.get("epoch", Number.class).longValue()).isEqualTo(epoch);
        Document current = SystemCollections.WORKLOAD_CLAIMS.on(database).find(new Document("_id",
                new Document("clusterId", node.key().clusterId()).append("resourceType", node.key().type().name())
                        .append("resourceId", node.key().resourceId()))).first();
        assertThat(ack.getDate("leaseUntil")).isBeforeOrEqualTo(current.getDate("leaseUntil"));
    }

    private record AckOutcome(boolean applied, TapstateException failure) {}

    @Test
    void aRetryingAckCannotOverwriteAReplacementBootProof() throws Exception {
        retryingAcknowledgementRequalifiesAfterConflict(true);
    }

    @Test
    void aRetryingAckCannotPublishAnEpochItDidNotLoad() throws Exception {
        retryingAcknowledgementRequalifiesAfterConflict(false);
    }

    private void retryingAcknowledgementRequalifiesAfterConflict(boolean replaceBoot) throws Exception {
        String name = replaceBoot ? "keyring_ack_retry_replaced_boot" : "keyring_ack_retry_new_epoch";
        MongoDatabase database = freshDatabase(name);
        SourceConfigKeyringStore setup = new SourceConfigKeyringStore(client, database);
        setup.loadOrCreateCipher();
        MongoWorkloadClaimStore claims = new MongoWorkloadClaimStore(SystemCollections.WORKLOAD_CLAIMS.on(database));
        Duration ttl = Duration.ofSeconds(30);
        WorkloadClaim original = nodeSession(claims, "node-a", "boot-a", ttl);
        long originalEpoch = setup.loadExisting().epoch();
        CountDownLatch peerOwnsNode = new CountDownLatch(1);
        CountDownLatch releasePeer = new CountDownLatch(1);
        CountDownLatch threeRealConflicts = new CountDownLatch(3);
        CountDownLatch nextNodeCommand = new CountDownLatch(1);
        CountDownLatch resumeStaleRequest = new CountDownLatch(1);
        AtomicInteger heldRequest = new AtomicInteger(-1);
        AtomicInteger nodeRequests = new AtomicInteger();
        AtomicInteger pausedRequest = new AtomicInteger(-1);
        AtomicInteger staleEpochRequest = new AtomicInteger(-1);
        AtomicBoolean staleNodeTouched = new AtomicBoolean();
        AtomicBoolean staleEpochRefused = new AtomicBoolean();
        Set<Integer> contenderRequests = java.util.concurrent.ConcurrentHashMap.newKeySet();
        List<String> actualErrors = new java.util.concurrent.CopyOnWriteArrayList<>();
        CommandListener heldPeer = new CommandListener() {
            @Override public void commandStarted(CommandStartedEvent event) {
                if (ackContendedWrite(event, name, true)) heldRequest.set(event.getRequestId());
            }
            @Override public void commandSucceeded(CommandSucceededEvent event) {
                if (event.getRequestId() != heldRequest.get()) return;
                assertThat(event.getResponse().get("value")).isInstanceOf(org.bson.BsonDocument.class);
                peerOwnsNode.countDown();
                awaitAckGate(releasePeer, "the original peer ACK transaction");
            }
        };
        CommandListener observeAndPause = new CommandListener() {
            @Override public void commandStarted(CommandStartedEvent event) {
                if (ackContendedWrite(event, name, true)) {
                    contenderRequests.add(event.getRequestId());
                    if (nodeRequests.incrementAndGet() == 4) {
                        pausedRequest.set(event.getRequestId());
                        nextNodeCommand.countDown();
                        awaitAckGate(resumeStaleRequest, "the stale retry NODE command");
                    }
                } else if (ackContendedWrite(event, name, false)) {
                    staleEpochRequest.set(event.getRequestId());
                }
            }
            @Override public void commandFailed(CommandFailedEvent event) {
                if (!contenderRequests.remove(event.getRequestId()) || !(event.getThrowable() instanceof MongoException failure)) return;
                int code = failure instanceof MongoWriteException write ? write.getError().getCode() : failure.getCode();
                observe(code, failure.hasErrorLabel("TransientTransactionError"),
                        failure.hasErrorLabel("UnknownTransactionCommitResult"));
            }
            @Override public void commandSucceeded(CommandSucceededEvent event) {
                var reply = event.getResponse();
                if (event.getRequestId() == pausedRequest.get()) {
                    staleNodeTouched.set(reply.get("value") instanceof org.bson.BsonDocument);
                }
                if (event.getRequestId() == staleEpochRequest.get()) {
                    staleEpochRefused.set(reply.getNumber("n").intValue() == 0);
                }
                if (!contenderRequests.remove(event.getRequestId())) return;
                boolean transientError = reply.containsKey("errorLabels") && reply.getArray("errorLabels").stream()
                        .anyMatch(label -> label.isString() && "TransientTransactionError".equals(label.asString().getValue()));
                boolean ambiguous = reply.containsKey("errorLabels") && reply.getArray("errorLabels").stream()
                        .anyMatch(label -> label.isString() && "UnknownTransactionCommitResult".equals(label.asString().getValue()));
                if (reply.containsKey("writeErrors")) {
                    reply.getArray("writeErrors").forEach(error -> observe(error.asDocument().getNumber("code").intValue(),
                            transientError, ambiguous));
                }
            }
            private void observe(int code, boolean transientError, boolean ambiguous) {
                actualErrors.add("code=" + code + ",transient=" + transientError + ",ambiguous=" + ambiguous);
                if (code == 112 && transientError && !ambiguous) threeRealConflicts.countDown();
            }
        };
        MongoClientSettings peerSettings = MongoClientSettings.builder()
                .applyConnectionString(new ConnectionString(REPLICA_SET.getReplicaSetUrl()))
                .addCommandListener(heldPeer).build();
        MongoClientSettings contenderSettings = MongoClientSettings.builder()
                .applyConnectionString(new ConnectionString(REPLICA_SET.getReplicaSetUrl()))
                .addCommandListener(observeAndPause).build();
        try (MongoClient peerClient = MongoClients.create(peerSettings);
                MongoClient contenderClient = MongoClients.create(contenderSettings)) {
            SourceConfigKeyringStore peer = new SourceConfigKeyringStore(peerClient, peerClient.getDatabase(name));
            SourceConfigKeyringStore contender = new SourceConfigKeyringStore(contenderClient, contenderClient.getDatabase(name));
            var workers = Executors.newFixedThreadPool(2);
            try {
                var winning = workers.submit(() -> peer.acknowledge(original, originalEpoch, ttl));
                assertThat(peerOwnsNode.await(10, TimeUnit.SECONDS)).isTrue();
                var stale = workers.submit(() -> contender.acknowledge(original, originalEpoch, ttl));
                assertThat(threeRealConflicts.await(10, TimeUnit.SECONDS))
                        .as("the real retry history must precede the change; observed=%s", actualErrors).isTrue();
                releasePeer.countDown();
                assertThat(winning.get(5, TimeUnit.SECONDS)).isTrue();
                assertThat(nextNodeCommand.await(5, TimeUnit.SECONDS))
                        .as("this is a post-fix control requiring a fourth actual attempt").isTrue();
                SourceConfigKeyringHandle current = new SourceConfigKeyringHandle(setup);
                if (replaceBoot) {
                    assertThat(claims.release(original)).isTrue();
                    WorkloadClaim replacement = nodeSession(claims, "node-a", "boot-a2", ttl);
                    assertThat(replacement.claimGeneration()).isGreaterThan(original.claimGeneration());
                    current.acknowledge(replacement, ttl);
                    assertExactAck(database, replacement, originalEpoch);
                } else {
                    assertThat(current.prepareRotation()).isGreaterThan(originalEpoch);
                    current.acknowledge(original, ttl);
                    assertExactAck(database, original, setup.loadExisting().epoch());
                }
                Document expectedAck = acknowledgement(database, "node-a");
                Document expectedNode = SystemCollections.WORKLOAD_CLAIMS.on(database).find(new Document("_id",
                        new Document("clusterId", original.key().clusterId())
                                .append("resourceType", original.key().type().name())
                                .append("resourceId", original.key().resourceId()))).first();
                long expectedEpoch = setup.loadExisting().epoch();
                resumeStaleRequest.countDown();

                assertThat(stale.get(5, TimeUnit.SECONDS)).isFalse();
                assertThat(acknowledgement(database, "node-a")).isEqualTo(expectedAck);
                assertThat(SystemCollections.WORKLOAD_CLAIMS.on(database).find(new Document("_id", expectedNode.get("_id"))).first())
                        .as("a refused stale ACK must leave no partial NODE fence increment").isEqualTo(expectedNode);
                assertThat(setup.loadExisting().epoch()).isEqualTo(expectedEpoch);
                if (replaceBoot) {
                    assertThat(staleNodeTouched).isFalse();
                    assertThat(staleEpochRequest.get()).isEqualTo(-1);
                } else {
                    assertThat(staleNodeTouched).as("the exact original NODE was touched inside the refused transaction").isTrue();
                    assertThat(staleEpochRefused).as("the actual new epoch rejects the stale loaded epoch").isTrue();
                }
            } finally {
                releasePeer.countDown();
                resumeStaleRequest.countDown();
                workers.shutdownNow();
                assertThat(workers.awaitTermination(10, TimeUnit.SECONDS)).isTrue();
            }
        }
    }

    private static void awaitAckGate(CountDownLatch gate, String operation) {
        try {
            if (!gate.await(20, TimeUnit.SECONDS)) throw new AssertionError(operation + " was not released");
        } catch (InterruptedException interrupted) {
            Thread.currentThread().interrupt();
            throw new AssertionError(operation + " was interrupted", interrupted);
        }
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
