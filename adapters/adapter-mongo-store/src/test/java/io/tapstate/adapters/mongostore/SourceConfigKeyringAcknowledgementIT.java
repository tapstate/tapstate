package io.tapstate.adapters.mongostore;

import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoDatabase;
import com.mongodb.ConnectionString;
import com.mongodb.MongoClientSettings;
import com.mongodb.event.CommandListener;
import com.mongodb.event.CommandStartedEvent;
import com.mongodb.event.CommandSucceededEvent;
import com.mongodb.MongoException;
import com.mongodb.client.model.FindOneAndUpdateOptions;
import io.tapstate.core.common.TapstateException;
import io.tapstate.spi.store.WorkloadClaim;
import io.tapstate.spi.store.WorkloadClaimKey;
import io.tapstate.spi.store.WorkloadClaimType;
import io.tapstate.spi.store.WorkloadOwner;
import io.tapstate.testsupport.RequiresDocker;
import org.bson.Document;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;
import org.testcontainers.containers.MongoDBContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.utility.DockerImageName;

import java.time.Duration;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Real ownership and epoch checks; an acknowledgement cannot outlive the lease it proves. */
@RequiresDocker
class SourceConfigKeyringAcknowledgementIT {

    @Container
    private static final MongoDBContainer MONGO = new MongoDBContainer(DockerImageName.parse("mongo:7.0"));

    private static final Duration TTL = Duration.ofSeconds(10);

    @ParameterizedTest
    @EnumSource(LostOwnership.class)
    void lostBootCannotRefreshItsAcknowledgementEvenBeforeTheReplacementAcknowledges(
            LostOwnership loss) throws Exception {
        try (MongoClient client = MongoClients.create(MONGO.getReplicaSetUrl())) {
            MongoDatabase database = freshDatabase(client);
            SourceConfigKeyringStore rings = new SourceConfigKeyringStore(client, database);
            rings.loadOrCreateCipher();
            SourceConfigKeyringHandle handle = new SourceConfigKeyringHandle(rings);
            MongoWorkloadClaimStore claims = claims(database);
            WorkloadClaim old = acquire(claims, "boot-old", loss == LostOwnership.EXPIRED
                    ? Duration.ofSeconds(1) : TTL);
            handle.acknowledge(old, TTL);
            Document before = acknowledgement(database);
            WorkloadClaim replacement = null;
            if (loss == LostOwnership.EXPIRED) {
                long deadline = System.nanoTime() + Duration.ofSeconds(3).toNanos();
                while (claims.read(old.key()).orElseThrow().leased() && System.nanoTime() < deadline) {
                    Thread.sleep(10);
                }
                assertThat(claims.read(old.key()).orElseThrow().leased()).isFalse();
            } else {
                assertThat(claims.release(old)).isTrue();
                if (loss == LostOwnership.REPLACED) {
                    replacement = acquire(claims, "boot-new", TTL);
                    assertThat(replacement.claimGeneration()).isGreaterThan(old.claimGeneration());
                    // The new boot has not published an ACK; an ACK-only generation filter is insufficient.
                    assertThat(acknowledgement(database).getString("bootId")).isEqualTo("boot-old");
                }
            }

            assertThatThrownBy(() -> handle.acknowledge(old, TTL))
                    .isInstanceOfSatisfying(TapstateException.class, error -> {
                        assertThat(error.code()).isEqualTo(StoreError.SOURCE_CONFIG_KEYRING_NOT_READY);
                        assertThat(error.args()).isEmpty();
                        assertThat(error.getCause()).isNull();
                    });
            assertThat(acknowledgement(database)).isEqualTo(before);
            if (replacement != null) {
                handle.acknowledge(replacement, TTL);
                assertThat(acknowledgement(database)).containsEntry("bootId", "boot-new")
                        .containsEntry("claimGeneration", replacement.claimGeneration());
                // Releasing the old ACK cannot delete or expire the replacement's proof.
                Document current = acknowledgement(database);
                handle.release(old);
                assertThat(acknowledgement(database)).isEqualTo(current);
            }
        }
    }

    @Test
    void aDelayedEpochCannotOverwriteTheProofOfANewerLoadedKeyring() {
        try (MongoClient client = MongoClients.create(MONGO.getReplicaSetUrl())) {
            MongoDatabase database = freshDatabase(client);
            SourceConfigKeyringStore rings = new SourceConfigKeyringStore(client, database);
            rings.loadOrCreateCipher();
            SourceConfigKeyringHandle handle = new SourceConfigKeyringHandle(rings);
            WorkloadClaim node = acquire(claims(database), "boot-live", TTL);
            handle.acknowledge(node, TTL);
            long oldEpoch = handle.epoch();
            assertThat(handle.prepareRotation()).isEqualTo(oldEpoch + 1);
            handle.acknowledge(node, TTL);
            Document current = acknowledgement(database);

            assertThat(rings.acknowledge(node, oldEpoch, TTL)).isFalse();
            assertThat(acknowledgement(database)).isEqualTo(current);
            assertThat(handle.activatePrepared()).isEqualTo(oldEpoch + 2);
            assertThat(rings.acknowledge(node, oldEpoch + 1, TTL)).isFalse();
            handle.acknowledge(node, TTL);
            assertThat(acknowledgement(database).get("epoch", Number.class).longValue())
                    .isEqualTo(handle.epoch());
        }
    }

    @Test
    void acknowledgementDeadlineIsBoundedByTheActualNodeLeaseAndAdvancesOnlyAfterRenewal() {
        try (MongoClient client = MongoClients.create(MONGO.getReplicaSetUrl())) {
            MongoDatabase database = freshDatabase(client);
            SourceConfigKeyringStore rings = new SourceConfigKeyringStore(client, database);
            rings.loadOrCreateCipher();
            SourceConfigKeyringHandle handle = new SourceConfigKeyringHandle(rings);
            MongoWorkloadClaimStore claims = claims(database);
            WorkloadClaim node = acquire(claims, "boot-live", TTL);
            handle.acknowledge(node, Duration.ofMinutes(1));
            assertThat(acknowledgement(database).getDate("leaseUntil").toInstant())
                    .isEqualTo(claims.read(node.key()).orElseThrow().claim().leaseUntil());

            WorkloadClaim renewed = claims.renew(node, Duration.ofSeconds(20)).orElseThrow();
            handle.acknowledge(renewed, Duration.ofMinutes(1));
            assertThat(acknowledgement(database).getDate("leaseUntil").toInstant())
                    .isEqualTo(renewed.leaseUntil()).isAfter(node.leaseUntil());
            handle.acknowledge(renewed, Duration.ofSeconds(1));
            assertThat(acknowledgement(database).getDate("leaseUntil").toInstant())
                    .isBefore(renewed.leaseUntil());
        }
    }

    @Test
    void anEpochSwitchAfterLoadingRetriesWithTheNewKeysBeforeAcknowledging() {
        try (MongoClient client = MongoClients.create(MONGO.getReplicaSetUrl())) {
            MongoDatabase database = freshDatabase(client);
            SourceConfigKeyringStore rotation = new SourceConfigKeyringStore(client, database);
            rotation.loadOrCreateCipher();
            WorkloadClaim node = acquire(claims(database), "boot-live", TTL);
            AtomicBoolean armed = new AtomicBoolean();
            AtomicBoolean switched = new AtomicBoolean();
            AtomicReference<Throwable> transitionFailure = new AtomicReference<>();
            CommandListener switchAfterRead = new CommandListener() {
                @Override
                public void commandSucceeded(CommandSucceededEvent event) {
                    if (!armed.get() || !"find".equals(event.getCommandName())) return;
                    var cursor = event.getResponse().getDocument("cursor", null);
                    if (cursor == null || !cursor.containsKey("firstBatch")) return;
                    var rows = cursor.getArray("firstBatch");
                    if (rows.isEmpty() || !rows.get(0).isDocument()
                            || !rows.get(0).asDocument().containsKey("activeKeyId")) return;
                    if (armed.compareAndSet(true, false)) {
                        try {
                            assertThat(rotation.prepareRotation().epoch()).isEqualTo(2);
                            switched.set(true);
                        } catch (Throwable failure) {
                            transitionFailure.set(failure);
                        }
                    }
                }
            };
            MongoClientSettings settings = MongoClientSettings.builder()
                    .applyConnectionString(new ConnectionString(MONGO.getReplicaSetUrl()))
                    .addCommandListener(switchAfterRead).build();
            try (MongoClient joining = MongoClients.create(settings)) {
                SourceConfigKeyringHandle handle = new SourceConfigKeyringHandle(
                        new SourceConfigKeyringStore(joining, joining.getDatabase(database.getName())));
                assertThat(handle.epoch()).isEqualTo(1);
                armed.set(true);
                handle.acknowledge(node, TTL);
                assertThat(transitionFailure.get()).isNull();
                assertThat(switched).as("the real keyring changed after this caller's first read").isTrue();
                assertThat(handle.epoch()).isEqualTo(2);
                assertThat(acknowledgement(database).get("epoch", Number.class).longValue()).isEqualTo(2);
                assertThat(rotation.activatePrepared().epoch()).isEqualTo(3);
            }
        }
    }

    @ParameterizedTest
    @EnumSource(FencedRecord.class)
    void ownershipAndEpochRemainFencedUntilTheAcknowledgementCommits(FencedRecord record) throws Exception {
        try (MongoClient client = MongoClients.create(MONGO.getReplicaSetUrl())) {
            MongoDatabase database = freshDatabase(client);
            SourceConfigKeyringStore rings = new SourceConfigKeyringStore(client, database);
            rings.loadOrCreateCipher();
            MongoWorkloadClaimStore claims = claims(database);
            WorkloadClaim node = acquire(claims, "boot-live", TTL);
            CountDownLatch fenced = new CountDownLatch(1);
            CountDownLatch resume = new CountDownLatch(1);
            AtomicInteger fenceRequest = new AtomicInteger(-1);
            AtomicReference<Throwable> hookFailure = new AtomicReference<>();
            CommandListener holdAfterFence = new CommandListener() {
                @Override
                public void commandStarted(CommandStartedEvent event) {
                    var command = event.getCommand();
                    var update = "findAndModify".equals(event.getCommandName())
                            ? command.getDocument("update", null)
                            : "update".equals(event.getCommandName())
                                    ? command.getArray("updates").get(0).asDocument().getDocument("u") : null;
                    if (update != null && update.containsKey("$inc")
                            && update.getDocument("$inc").containsKey(record.counter)) {
                        fenceRequest.set(event.getRequestId());
                    }
                }

                @Override
                public void commandSucceeded(CommandSucceededEvent event) {
                    if (event.getRequestId() != fenceRequest.get()) return;
                    fenced.countDown();
                    try {
                        if (!resume.await(5, TimeUnit.SECONDS)) throw new AssertionError("ACK fence was not released");
                    } catch (Throwable failure) {
                        hookFailure.set(failure);
                        if (failure instanceof InterruptedException) Thread.currentThread().interrupt();
                    }
                }
            };
            MongoClientSettings settings = MongoClientSettings.builder()
                    .applyConnectionString(new ConnectionString(MONGO.getReplicaSetUrl()))
                    .addCommandListener(holdAfterFence).build();
            try (MongoClient acknowledging = MongoClients.create(settings)) {
                SourceConfigKeyringHandle handle = new SourceConfigKeyringHandle(new SourceConfigKeyringStore(
                        acknowledging, acknowledging.getDatabase(database.getName())));
                CompletableFuture<Void> ack = CompletableFuture.runAsync(() -> handle.acknowledge(node, TTL));
                try {
                    assertThat(fenced.await(5, TimeUnit.SECONDS)).as("the actual server accepted the in-transaction fence")
                            .isTrue();
                    assertThat(acknowledgement(database)).as("the ACK has not been published yet").isNull();
                    var collection = record == FencedRecord.NODE_SESSION
                            ? SystemCollections.WORKLOAD_CLAIMS.on(database) : SystemCollections.SYSTEM_META.on(database);
                    Document filter = record == FencedRecord.NODE_SESSION
                            ? new Document("ownerBootId", "boot-live").append("claimGeneration", node.claimGeneration())
                            : new Document("_id", "source-config-keyring");
                    Document update = record == FencedRecord.NODE_SESSION
                            ? new Document("$set", new Document("leaseUntil", new java.util.Date(0)))
                            : new Document("$inc", new Document("epoch", 1L));
                    assertThatThrownBy(() -> collection.findOneAndUpdate(filter, update,
                            new FindOneAndUpdateOptions().maxTime(200, TimeUnit.MILLISECONDS)))
                            .as("another Mongo writer cannot change ownership or epoch before the ACK commits")
                            .isInstanceOfSatisfying(MongoException.class,
                                    error -> assertThat(error.getCode()).isEqualTo(50));
                } finally {
                    resume.countDown();
                    ack.get(10, TimeUnit.SECONDS);
                }
                assertThat(hookFailure.get()).isNull();
                assertThat(acknowledgement(database)).containsEntry("epoch", 1L).containsEntry("bootId", "boot-live");
                assertThat(claims.read(node.key()).orElseThrow().leased()).isTrue();
                assertThat(rings.prepareRotation().epoch()).isEqualTo(2);
                assertThat(claims.release(node)).isTrue();
                assertThat(rings.acknowledge(node, 2, TTL)).isFalse();
            }
        }
    }

    private static MongoDatabase freshDatabase(MongoClient client) {
        return client.getDatabase("keyring_ack_" + Long.toUnsignedString(System.nanoTime(), 16));
    }

    private static MongoWorkloadClaimStore claims(MongoDatabase database) {
        return new MongoWorkloadClaimStore(SystemCollections.WORKLOAD_CLAIMS.on(database));
    }

    private static WorkloadClaim acquire(MongoWorkloadClaimStore claims, String boot, Duration ttl) {
        return claims.acquire(new WorkloadClaimKey("cluster-ack", WorkloadClaimType.NODE_SESSION, "node-a"),
                new WorkloadOwner("node-a", boot), 0, ttl).claim();
    }

    private static Document acknowledgement(MongoDatabase database) {
        return SystemCollections.SYSTEM_META.on(database)
                .find(new Document("kind", "source-config-keyring-node").append("nodeId", "node-a"))
                .first();
    }

    private enum LostOwnership { RELEASED, EXPIRED, REPLACED }

    private enum FencedRecord {
        NODE_SESSION("sourceConfigAckFence"), KEYRING("nodeAckFence");
        private final String counter;
        FencedRecord(String counter) { this.counter = counter; }
    }
}
