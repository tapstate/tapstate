package io.tapstate.adapters.mongostore;

import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.ClientSession;
import com.mongodb.ConnectionString;
import com.mongodb.MongoClientSettings;
import com.mongodb.event.CommandListener;
import com.mongodb.event.CommandStartedEvent;
import com.mongodb.event.CommandSucceededEvent;
import com.mongodb.event.CommandFailedEvent;
import com.mongodb.MongoException;
import com.mongodb.client.model.FindOneAndUpdateOptions;
import io.tapstate.core.common.TapstateException;
import io.tapstate.spi.store.WorkloadClaim;
import io.tapstate.spi.store.WorkloadClaimKey;
import io.tapstate.spi.store.WorkloadClaimType;
import io.tapstate.spi.store.WorkloadOwner;
import io.tapstate.spi.store.IoError;
import io.tapstate.testsupport.RequiresDocker;
import org.bson.Document;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.MongoDBContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.utility.DockerImageName;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

import java.time.Duration;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
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

    @Test
    void anUnlabelledWriteConflictDoesNotAuthorizeReplayingAnAcknowledgement() {
        rejectedSessionIsNotRetried(new MongoException(112, "unlabelled conflict"), false);
    }

    @Test
    void interruptionStopsConflictRetriesAndPreservesTheInterrupt() {
        MongoException conflict = new MongoException(112, "transaction conflict");
        conflict.addLabel("TransientTransactionError");
        try {
            rejectedSessionIsNotRetried(conflict, true);
        } finally {
            Thread.interrupted();
        }
    }

    private static void rejectedSessionIsNotRetried(MongoException failure, boolean interrupt) {
        try (MongoClient actual = MongoClients.create(MONGO.getReplicaSetUrl())) {
            MongoDatabase database = freshDatabase(actual);
            SourceConfigKeyringStore ring = new SourceConfigKeyringStore(actual, database);
            ring.loadOrCreateCipher();
            WorkloadClaim node = acquire(claims(database), "boot-live", TTL);
            AtomicInteger attempts = new AtomicInteger();
            MongoClient rejecting = (MongoClient) Proxy.newProxyInstance(MongoClient.class.getClassLoader(),
                    new Class<?>[] {MongoClient.class}, (proxy, method, arguments) -> {
                        if (method.getName().equals("startSession")) {
                            attempts.incrementAndGet();
                            if (interrupt) Thread.currentThread().interrupt();
                            throw failure;
                        }
                        return invoke(method, actual, arguments);
                    });
            SourceConfigKeyringStore acknowledgements = new SourceConfigKeyringStore(rejecting, database);
            try {
                assertThatThrownBy(() -> acknowledgements.acknowledge(node, 1L, TTL))
                        .isInstanceOfSatisfying(TapstateException.class,
                                error -> assertThat(error.code()).isEqualTo(IoError.STORE_UNAVAILABLE));
                assertThat(attempts.get()).isEqualTo(1);
                if (interrupt) assertThat(Thread.currentThread().isInterrupted()).isTrue();
            } finally {
                // Check the production method's flag before clearing this test thread for DB reads
                // and cleanup; the driver itself refuses those operations while interrupted.
                if (interrupt) Thread.interrupted();
            }
            assertThat(acknowledgement(database)).isNull();
        }
    }

    @Test
    void anUnknownCommitResultDoesNotReplayAnAlreadyCommittedAcknowledgement() {
        try (MongoClient actual = MongoClients.create(MONGO.getReplicaSetUrl())) {
            MongoDatabase database = freshDatabase(actual);
            new SourceConfigKeyringStore(actual, database).loadOrCreateCipher();
            WorkloadClaim node = acquire(claims(database), "boot-live", TTL);
            AtomicInteger transactions = new AtomicInteger();
            AtomicInteger commits = new AtomicInteger();
            MongoException ambiguous = new MongoException(91, "commit response unavailable");
            ambiguous.addLabel("TransientTransactionError");
            ambiguous.addLabel("UnknownTransactionCommitResult");
            MongoClient uncertain = (MongoClient) Proxy.newProxyInstance(MongoClient.class.getClassLoader(),
                    new Class<?>[] {MongoClient.class}, (proxy, method, arguments) -> {
                        if (!method.getName().equals("startSession")) return invoke(method, actual, arguments);
                        ClientSession session = (ClientSession) invoke(method, actual, arguments);
                        transactions.incrementAndGet();
                        return Proxy.newProxyInstance(ClientSession.class.getClassLoader(),
                                new Class<?>[] {ClientSession.class}, (wrapped, operation, parameters) -> {
                                    Object result = invoke(operation, session, parameters);
                                    if (operation.getName().equals("commitTransaction")) {
                                        commits.incrementAndGet();
                                        // The server committed, but its answer was lost. Replaying the
                                        // transaction would publish a second ownership fence.
                                        throw ambiguous;
                                    }
                                    return result;
                                });
                    });
            SourceConfigKeyringStore acknowledgements = new SourceConfigKeyringStore(uncertain, database);
            assertThatThrownBy(() -> acknowledgements.acknowledge(node, 1L, TTL))
                    .isInstanceOfSatisfying(TapstateException.class,
                            error -> assertThat(error.code()).isEqualTo(IoError.STORE_UNAVAILABLE));
            assertThat(transactions.get()).isEqualTo(1);
            assertThat(commits.get()).isEqualTo(1);
            assertThat(acknowledgement(database)).containsEntry("epoch", 1L).containsEntry("bootId", "boot-live");
            assertThat(SystemCollections.WORKLOAD_CLAIMS.on(database)
                    .find(new Document("ownerBootId", "boot-live")).first())
                    .containsEntry("sourceConfigAckFence", 1L);
        }
    }

    private static Object invoke(Method method, Object target, Object[] arguments) throws Throwable {
        try {
            return method.invoke(target, arguments);
        } catch (InvocationTargetException failure) {
            throw failure.getCause();
        }
    }

    @Test
    void persistentConflictsExhaustTheRetryWindowWithoutPublishingAnAcknowledgement() {
        try (MongoClient actual = MongoClients.create(MONGO.getReplicaSetUrl())) {
            MongoDatabase database = freshDatabase(actual);
            new SourceConfigKeyringStore(actual, database).loadOrCreateCipher();
            WorkloadClaim node = acquire(claims(database), "boot-live", TTL);
            AtomicInteger attempts = new AtomicInteger();
            MongoException conflict = new MongoException(112, "persistent transaction conflict");
            conflict.addLabel("TransientTransactionError");
            MongoClient refusing = (MongoClient) Proxy.newProxyInstance(MongoClient.class.getClassLoader(),
                    new Class<?>[] {MongoClient.class}, (proxy, method, arguments) -> {
                        if (method.getName().equals("startSession")) {
                            attempts.incrementAndGet();
                            throw conflict;
                        }
                        return invoke(method, actual, arguments);
                    });
            SourceConfigKeyringStore ring = new SourceConfigKeyringStore(refusing, database);
            org.junit.jupiter.api.Assertions.assertTimeoutPreemptively(Duration.ofSeconds(10), () -> {
                long started = System.nanoTime();
                assertThatThrownBy(() -> ring.acknowledge(node, 1L, TTL))
                        .isInstanceOfSatisfying(TapstateException.class,
                                error -> assertThat(error.code()).isEqualTo(IoError.STORE_UNAVAILABLE));
                assertThat(System.nanoTime() - started).isGreaterThanOrEqualTo(Duration.ofSeconds(2).toNanos());
            });
            assertThat(attempts.get()).isGreaterThan(3);
            assertThat(acknowledgement(database)).isNull();
        }
    }

    @Test
    void interruptionDuringBackoffStopsRetryingAndPreservesTheInterrupt() throws Exception {
        try (MongoClient actual = MongoClients.create(MONGO.getReplicaSetUrl())) {
            MongoDatabase database = freshDatabase(actual);
            new SourceConfigKeyringStore(actual, database).loadOrCreateCipher();
            WorkloadClaim node = acquire(claims(database), "boot-live", TTL);
            CountDownLatch backingOff = new CountDownLatch(1);
            AtomicInteger attempts = new AtomicInteger();
            MongoException conflict = new MongoException(112, "transaction conflict while backing off");
            conflict.addLabel("TransientTransactionError");
            MongoClient refusing = (MongoClient) Proxy.newProxyInstance(MongoClient.class.getClassLoader(),
                    new Class<?>[] {MongoClient.class}, (proxy, method, arguments) -> {
                        if (method.getName().equals("startSession")) {
                            if (attempts.incrementAndGet() >= 4) backingOff.countDown();
                            throw conflict;
                        }
                        return invoke(method, actual, arguments);
                    });
            SourceConfigKeyringStore ring = new SourceConfigKeyringStore(refusing, database);
            AtomicReference<Throwable> failure = new AtomicReference<>();
            AtomicBoolean interrupted = new AtomicBoolean();
            Thread worker = new Thread(() -> {
                try {
                    ring.acknowledge(node, 1L, TTL);
                } catch (Throwable error) {
                    failure.set(error);
                } finally {
                    interrupted.set(Thread.currentThread().isInterrupted());
                }
            }, "keyring-ack-backoff-test");
            worker.setDaemon(true);
            worker.start();
            try {
                assertThat(backingOff.await(5, TimeUnit.SECONDS)).isTrue();
                long deadline = System.nanoTime() + Duration.ofSeconds(5).toNanos();
                boolean sleeping = false;
                while (worker.isAlive() && System.nanoTime() < deadline && !sleeping) {
                    sleeping = java.util.Arrays.stream(worker.getStackTrace()).anyMatch(frame ->
                            frame.getClassName().equals(TimeUnit.class.getName())
                                    && frame.getMethodName().equals("sleep"));
                    if (!sleeping) Thread.onSpinWait();
                }
                assertThat(sleeping).as("interrupt the actual retry backoff, not a database call").isTrue();
                worker.interrupt();
                worker.join(5_000);
                assertThat(worker.isAlive()).isFalse();
                assertThat(failure.get()).isInstanceOfSatisfying(TapstateException.class,
                        error -> assertThat(error.code()).isEqualTo(IoError.STORE_UNAVAILABLE));
                assertThat(interrupted.get()).isTrue();
                assertThat(acknowledgement(database)).isNull();
            } finally {
                worker.interrupt();
                worker.join(5_000);
            }
        }
    }

    @Test
    void acknowledgementSurvivesThreeCompetingKeyringWritesWithoutLosingItsNodeSession() {
        try (MongoClient competing = MongoClients.create(MONGO.getReplicaSetUrl())) {
            MongoDatabase database = freshDatabase(competing);
            SourceConfigKeyringStore rings = new SourceConfigKeyringStore(competing, database);
            rings.loadOrCreateCipher();
            MongoWorkloadClaimStore claims = claims(database);
            WorkloadClaim node = acquire(claims, "boot-live", TTL);
            AtomicInteger writes = new AtomicInteger();
            AtomicInteger conflicts = new AtomicInteger();
            AtomicReference<Throwable> hookFailure = new AtomicReference<>();
            CommandListener contender = new CommandListener() {
                @Override
                public void commandStarted(CommandStartedEvent event) {
                    if (!"update".equals(event.getCommandName())) return;
                    var update = event.getCommand().getArray("updates").get(0).asDocument().getDocument("u");
                    if (!update.containsKey("$inc")
                            || !update.getDocument("$inc").containsKey("nodeAckFence")) return;
                    if (writes.getAndIncrement() < 3) {
                        try {
                            // The transaction already read its node claim. A committed competing write
                            // makes its keyring snapshot stale before it attempts the shared fence.
                            assertThat(SystemCollections.SYSTEM_META.on(database).updateOne(
                                    new Document("_id", "source-config-keyring"),
                                    new Document("$inc", new Document("nodeAckFence", 1L)))
                                    .getModifiedCount()).isEqualTo(1);
                        } catch (Throwable error) {
                            hookFailure.set(error);
                        }
                    }
                }

                @Override
                public void commandFailed(CommandFailedEvent event) {
                    if (event.getThrowable() instanceof MongoException error && error.getCode() == 112) {
                        assertThat(error.hasErrorLabel("TransientTransactionError")).isTrue();
                        conflicts.incrementAndGet();
                    }
                }
            };
            MongoClientSettings settings = MongoClientSettings.builder()
                    .applyConnectionString(new ConnectionString(MONGO.getReplicaSetUrl()))
                    .addCommandListener(contender).build();
            try (MongoClient acknowledging = MongoClients.create(settings)) {
                SourceConfigKeyringHandle handle = new SourceConfigKeyringHandle(new SourceConfigKeyringStore(
                        acknowledging, acknowledging.getDatabase(database.getName())));
                handle.acknowledge(node, TTL);
                assertThat(hookFailure.get()).isNull();
                assertThat(conflicts.get()).as("three real snapshot write conflicts were observed").isEqualTo(3);
                assertThat(writes.get()).isEqualTo(4);
                assertThat(acknowledgement(database)).containsEntry("bootId", "boot-live")
                        .containsEntry("claimGeneration", node.claimGeneration()).containsEntry("epoch", 1L);
                assertThat(claims.read(node.key()).orElseThrow().leased()).isTrue();
                assertThat(SystemCollections.WORKLOAD_CLAIMS.on(database)
                        .find(new Document("ownerBootId", "boot-live")).first())
                        .containsEntry("sourceConfigAckFence", 1L);
            }
        }
    }

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
