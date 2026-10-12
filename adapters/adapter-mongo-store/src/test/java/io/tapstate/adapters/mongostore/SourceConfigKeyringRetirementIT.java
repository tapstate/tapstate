package io.tapstate.adapters.mongostore;

import com.mongodb.ConnectionString;
import com.mongodb.MongoClientSettings;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoDatabase;
import com.mongodb.event.CommandListener;
import com.mongodb.event.CommandStartedEvent;
import com.mongodb.event.CommandSucceededEvent;
import io.tapstate.core.common.TapstateException;
import io.tapstate.core.dsl.DslParser;
import io.tapstate.core.model.Resource;
import io.tapstate.core.model.canonical.CanonicalHash;
import io.tapstate.spi.store.IoError;
import io.tapstate.spi.store.WorkloadClaim;
import io.tapstate.spi.store.WorkloadClaimKey;
import io.tapstate.spi.store.WorkloadClaimType;
import io.tapstate.spi.store.WorkloadOwner;
import io.tapstate.testsupport.RequiresDocker;
import org.bson.Document;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.testcontainers.containers.MongoDBContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.utility.DockerImageName;

import java.time.Duration;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Online retirement in isolated metadata, not deletion of a real deployment or historical backup. */
@RequiresDocker
class SourceConfigKeyringRetirementIT {
    @Container
    private static final MongoDBContainer MONGO = new MongoDBContainer(DockerImageName.parse("mongo:7.0"));
    private static final Duration TTL = Duration.ofSeconds(30);
    private static final String SECRET = "retirement-whole-config-sentinel";

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    void oldOnlineKeysCanRetireWithoutChangingSourcesOrBreakingACompletePairedBackup(boolean cloud) {
        try (Fixture fixture = new Fixture()) {
            Resource source = source("orders", cloud);
            fixture.artifacts().save(source);
            Document backedUpSource = fixture.source("orders");
            Document backedUpRing = fixture.ring();
            String oldKey = fixture.handle.current().activeKeyId();
            fixture.rotate();
            Document before = fixture.source("orders");
            assertThat(fixture.handle.retireReadOnlyKeys()).isEqualTo(4);
            Document retired = fixture.ring();
            assertThat(retired.getList("keys", Document.class)).hasSize(1);
            assertThat(retired.getList("keys", Document.class).getFirst().getString("id")).isNotEqualTo(oldKey);
            assertThat(before.equals(fixture.source("orders"))).as("retirement does not rewrite a Source").isTrue();
            assertThat(fixture.artifacts().get("orders")).contains(source);
            assertThat(fixture.source("orders").getString("contentHash")).isEqualTo(CanonicalHash.of(source));
            assertThat(fixture.source("orders").toJson()).doesNotContain(SECRET);
            assertThat(fixture.handle.retireReadOnlyKeys()).as("no historical keys is an idempotent no-op").isEqualTo(4);
            assertThat(retired.equals(fixture.ring())).isTrue();

            // An independently retained consistent pair has its own K1. Removing online K1 does not
            // require a backup-inventory service and must not silently rewrite another database.
            MongoDatabase restored = fixture.client.getDatabase(fixture.database.getName() + "_paired");
            SystemCollections.SYSTEM_META.on(restored).insertOne(backedUpRing);
            SystemCollections.ARTIFACTS.on(restored).insertOne(backedUpSource);
            SourceConfigKeyringHandle restoredKeys = new SourceConfigKeyringHandle(
                    new SourceConfigKeyringStore(fixture.client, restored));
            assertThat(new MongoArtifactStore(fixture.client, SystemCollections.ARTIFACTS.on(restored), restoredKeys)
                    .get("orders")).contains(source);
            assertThat(restoredKeys.current().activeKeyId()).isEqualTo(oldKey);
        }
    }

    @Test
    void evenOneValidHistoricalEnvelopeBlocksRetirementWithoutPartialChanges() {
        try (Fixture fixture = new Fixture()) {
            fixture.artifacts().save(source("still_referenced", false));
            Document oldSource = fixture.source("still_referenced");
            fixture.rotate();
            // Represent a remaining older persisted reference, not a supported writer bypass.
            SystemCollections.ARTIFACTS.on(fixture.database)
                    .replaceOne(new Document("_id", "still_referenced"), oldSource);
            fixture.refusesRetirement(StoreError.SOURCE_CONFIG_KEYRING_ROTATION_BLOCKED);
            assertThat(oldSource.equals(fixture.source("still_referenced"))).isTrue();
            assertThat(fixture.artifacts().get("still_referenced")).contains(source("still_referenced", false));
        }
    }

    @Test
    void damagedKindMarkersCannotHideAHistoricalEnvelopeFromRetirement() {
        try (Fixture fixture = new Fixture()) {
            fixture.artifacts().save(source("hidden_old_reference", false));
            Document historical = fixture.source("hidden_old_reference");
            fixture.rotate();
            historical.put("kind", "damaged-stored-kind");
            historical.get("body", Document.class).put("kind", "damaged-stored-kind");
            SystemCollections.ARTIFACTS.on(fixture.database)
                    .replaceOne(new Document("_id", "hidden_old_reference"), historical);
            fixture.refusesRetirement(IoError.DOCUMENT_UNREADABLE);
            assertThat(historical.equals(fixture.source("hidden_old_reference"))).isTrue();
        }
    }

    @ParameterizedTest
    @ValueSource(strings = {"cipher", "hash", "kind"})
    void unreadableSourcesCannotBeExcludedFromTheReferenceScan(String damage) {
        try (Fixture fixture = new Fixture()) {
            fixture.artifacts().save(source("unreadable", false));
            fixture.rotate();
            String field = switch (damage) {
                case "cipher" -> "body.config";
                case "hash" -> "contentHash";
                case "kind" -> "kind";
                default -> throw new IllegalStateException("unrecognized fixture damage");
            };
            SystemCollections.ARTIFACTS.on(fixture.database).updateOne(new Document("_id", "unreadable"),
                    new Document("$set", new Document(field, "damaged-stored-value-sentinel")));
            fixture.refusesRetirement(IoError.DOCUMENT_UNREADABLE);
        }
    }

    @Test
    void preparedKeysCannotBeRemovedByAnOverlappingRetirement() {
        try (Fixture fixture = new Fixture()) {
            fixture.rotate();
            assertThat(fixture.handle.prepareRotation()).isEqualTo(4);
            fixture.refusesRetirement(StoreError.SOURCE_CONFIG_KEYRING_ROTATION_BLOCKED);
            assertThat(fixture.ring().getList("keys", Document.class)).hasSize(3);
        }
    }

    @Test
    void everyLiveBootMustAcknowledgeTheActiveEpochAndAReplacementCannotReuseAnOldAck() {
        try (Fixture fixture = new Fixture()) {
            fixture.rotate();
            WorkloadClaim first = fixture.node("node-a", "boot-a", TTL);
            WorkloadClaim second = fixture.node("node-b", "boot-b", TTL);
            fixture.handle.acknowledge(first, TTL);
            fixture.refusesRetirement(StoreError.SOURCE_CONFIG_KEYRING_ROTATION_BLOCKED);
            fixture.handle.acknowledge(second, TTL);
            assertThat(fixture.claims.release(second)).isTrue();
            WorkloadClaim replacement = fixture.node("node-b", "boot-b2", TTL);
            assertThat(replacement.claimGeneration()).isGreaterThan(second.claimGeneration());
            fixture.refusesRetirement(StoreError.SOURCE_CONFIG_KEYRING_ROTATION_BLOCKED);
            fixture.handle.acknowledge(replacement, TTL);
            assertThat(fixture.handle.retireReadOnlyKeys()).isEqualTo(4);
            fixture.handle.acknowledge(first, TTL);
            fixture.handle.acknowledge(replacement, TTL);
            assertThat(fixture.handle.epoch()).isEqualTo(4);
        }
    }

    @Test
    void exitedAndExpiredBootsDoNotCreateAPermanentRetirementWait() throws Exception {
        try (Fixture fixture = new Fixture()) {
            fixture.rotate();
            WorkloadClaim exited = fixture.node("departed", "boot-exited", TTL);
            WorkloadClaim crashed = fixture.node("crashed", "boot-crashed", Duration.ofMillis(300));
            assertThat(fixture.claims.release(exited)).isTrue();
            long deadline = System.nanoTime() + Duration.ofSeconds(3).toNanos();
            while (fixture.claims.read(crashed.key()).orElseThrow().leased() && System.nanoTime() < deadline) {
                Thread.sleep(10);
            }
            assertThat(fixture.claims.read(crashed.key()).orElseThrow().leased()).isFalse();
            assertThat(fixture.handle.retireReadOnlyKeys()).isEqualTo(4);
            WorkloadClaim joined = fixture.node("joined", "boot-joined", TTL);
            fixture.handle.acknowledge(joined, TTL);
            assertThat(fixture.handle.epoch()).isEqualTo(4);
            fixture.artifacts().save(source("after_retirement", false));
            assertThat(fixture.artifacts().get("after_retirement")).contains(source("after_retirement", false));
        }
    }

    @Test
    void anUncommittedSupportedWriterFencesRetirementBeforeTheReferenceScan() throws Exception {
        try (Fixture fixture = new Fixture()) {
            fixture.rotate();
            Document before = fixture.ring();
            fixture.client.getDatabase("admin").runCommand(new Document("setParameter", 1)
                    .append("maxTransactionLockRequestTimeoutMillis", 100));
            CountDownLatch entered = new CountDownLatch(1);
            CountDownLatch release = new CountDownLatch(1);
            AtomicInteger fenceRequest = new AtomicInteger(-1);
            CommandListener holdWriter = new CommandListener() {
                @Override public void commandStarted(CommandStartedEvent event) {
                    if (isRingFence(event)) fenceRequest.set(event.getRequestId());
                }
                @Override public void commandSucceeded(CommandSucceededEvent event) {
                    if (event.getRequestId() != fenceRequest.get()) return;
                    entered.countDown();
                    try {
                        if (!release.await(15, TimeUnit.SECONDS)) throw new AssertionError("writer fence was not released");
                    } catch (InterruptedException interrupted) {
                        Thread.currentThread().interrupt();
                        throw new AssertionError("writer fence was interrupted", interrupted);
                    }
                }
            };
            AtomicInteger sourceScans = new AtomicInteger();
            CommandListener observeRetirer = new CommandListener() {
                @Override public void commandStarted(CommandStartedEvent event) {
                    if ("find".equals(event.getCommandName()) && event.getCommand().containsKey("find")
                            && "artifacts".equals(event.getCommand().getString("find").getValue())) sourceScans.incrementAndGet();
                }
            };
            try (MongoClient writerClient = fixture.client(holdWriter);
                    MongoClient retireClient = fixture.client(observeRetirer)) {
                SourceConfigKeyringHandle writer = new SourceConfigKeyringHandle(new SourceConfigKeyringStore(
                        writerClient, writerClient.getDatabase(fixture.database.getName())));
                SourceConfigKeyringHandle retirer = new SourceConfigKeyringHandle(new SourceConfigKeyringStore(
                        retireClient, retireClient.getDatabase(fixture.database.getName())));
                Resource source = source("held_writer", false);
                CompletableFuture<Void> writing = CompletableFuture.runAsync(() ->
                        new MongoArtifactStore(writerClient, SystemCollections.ARTIFACTS.on(
                                writerClient.getDatabase(fixture.database.getName())), writer).save(source));
                try {
                    assertThat(entered.await(10, TimeUnit.SECONDS)).as("the real writer acquired the ring fence").isTrue();
                    assertThatThrownBy(retirer::retireReadOnlyKeys).isInstanceOfSatisfying(TapstateException.class,
                            error -> assertThat(error.code()).isEqualTo(IoError.STORE_UNAVAILABLE));
                    assertThat(sourceScans.get()).as("retirement cannot scan past an uncommitted writer").isZero();
                    assertThat(before.equals(fixture.ring())).isTrue();
                } finally {
                    release.countDown();
                    writing.get(15, TimeUnit.SECONDS);
                }
                assertThat(fixture.handle.retireReadOnlyKeys()).isEqualTo(4);
                assertThat(fixture.artifacts().get(source.id())).contains(source);
            } finally {
                release.countDown();
                fixture.client.getDatabase("admin").runCommand(new Document("setParameter", 1)
                        .append("maxTransactionLockRequestTimeoutMillis", 5));
            }
        }
    }

    private static boolean isRingFence(CommandStartedEvent event) {
        if (!"update".equals(event.getCommandName())) return false;
        var update = event.getCommand().getArray("updates").getFirst().asDocument().getDocument("u");
        return update.containsKey("$inc") && update.getDocument("$inc").containsKey("sourceWriteFence");
    }

    private static Resource source(String id, boolean cloud) {
        return new DslParser().parse("""
                version: tapstate/v1
                kind: source
                id: %s
                connector: mongodb-atlas
                %s
                config:
                  uri: mongodb://fake-user:fake-password@localhost/data
                  nested: { unmarkedValue: %s }
                """.formatted(id, cloud ? "metadata: { cloud: true, user_id: stable-test-user }" : "", SECRET));
    }

    private static final class Fixture implements AutoCloseable {
        private final MongoClient client = MongoClients.create(MONGO.getReplicaSetUrl());
        private final MongoDatabase database = client.getDatabase("key_retirement_" + Long.toUnsignedString(System.nanoTime(), 16));
        private final SourceConfigKeyringStore rings = new SourceConfigKeyringStore(client, database);
        private final SourceConfigKeyringHandle handle;
        private final MongoWorkloadClaimStore claims = new MongoWorkloadClaimStore(SystemCollections.WORKLOAD_CLAIMS.on(database));

        private Fixture() { rings.loadOrCreateCipher(); handle = new SourceConfigKeyringHandle(rings); }
        private MongoArtifactStore artifacts() { return new MongoArtifactStore(client, SystemCollections.ARTIFACTS.on(database), handle); }
        private Document ring() { return SystemCollections.SYSTEM_META.on(database).find(new Document("_id", "source-config-keyring")).first(); }
        private Document source(String id) { return SystemCollections.ARTIFACTS.on(database).find(new Document("_id", id)).first(); }
        private void rotate() { assertThat(handle.prepareRotation()).isEqualTo(2); assertThat(handle.activatePrepared()).isEqualTo(3); }
        private WorkloadClaim node(String id, String boot, Duration ttl) {
            return claims.acquire(new WorkloadClaimKey("retirement-cluster", WorkloadClaimType.NODE_SESSION, id),
                    new WorkloadOwner(id, boot), 0, ttl).claim();
        }
        private MongoClient client(CommandListener listener) {
            return MongoClients.create(MongoClientSettings.builder().applyConnectionString(
                    new ConnectionString(MONGO.getReplicaSetUrl(database.getName()))).addCommandListener(listener).build());
        }
        private void refusesRetirement(io.tapstate.core.common.TapstateErrorCode code) {
            Document before = ring();
            assertThatThrownBy(handle::retireReadOnlyKeys).isInstanceOfSatisfying(TapstateException.class, error -> {
                assertThat(error.code()).isEqualTo(code);
                assertThat(error.getCause()).isNull();
                assertThat(error.getMessage()).doesNotContain(SECRET, "fake-password");
            });
            assertThat(before.equals(ring())).as("rejected retirement commits no partial fence or key removal").isTrue();
        }
        @Override public void close() { client.close(); }
    }
}
