package io.tapstate.adapters.mongostore;

import com.mongodb.ConnectionString;
import com.mongodb.MongoClientSettings;
import com.mongodb.MongoException;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoDatabase;
import com.mongodb.event.CommandFailedEvent;
import com.mongodb.event.CommandListener;
import com.mongodb.event.CommandStartedEvent;
import com.mongodb.event.CommandSucceededEvent;
import io.tapstate.core.common.TapstateException;
import io.tapstate.spi.store.IoError;
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
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Real Mongo conflicts with controlled scheduling outside the failed transaction. */
@RequiresDocker
class SourceConfigKeyringAcknowledgementRetryIT {
    @Container
    private static final MongoDBContainer MONGO = new MongoDBContainer(DockerImageName.parse("mongo:7.0"))
            .withCommand("--replSet", "docker-rs", "--bind_ip_all", "--setParameter", "enableTestCommands=1");
    private static final Duration TTL = Duration.ofSeconds(20);

    @Test
    void aLiveAcknowledgementWaitsForTheHoldingTransactionThenCommits() throws Exception {
        try (Fixture fixture = new Fixture()) {
            Pause pause = new Pause();
            Trace trace = new Trace();
            try (MongoClient contender = fixture.client(trace, "contender")) {
                SourceConfigKeyringStore rings = new SourceConfigKeyringStore(contender,
                        contender.getDatabase(fixture.database.getName()), pause);
                SourceConfigKeyringHandle handle = new SourceConfigKeyringHandle(rings);
                fixture.holdRing();
                WorkloadClaim node = fixture.node("node-b", "boot-b", TTL);
                CompletableFuture<Void> ack = CompletableFuture.runAsync(() -> handle.acknowledge(node, TTL), fixture.pool);
                try {
                    awaitPause(pause, ack);
                    trace.assertConflict();
                    assertThat(fixture.ack("node-b")).isNull();
                    assertThat(fixture.claim("node-b")).doesNotContainKey("sourceConfigAckFence");
                    fixture.commitHolder();
                    pause.resume.countDown();
                    ack.get(5, TimeUnit.SECONDS);
                    assertThat(pause.calls).hasValue(1);
                    assertThat(trace.attempts).hasValue(2);
                    assertThat(fixture.ring().get("nodeAckFence", Number.class).longValue()).isEqualTo(2);
                    fixture.assertAck(node, handle.epoch());
                    assertThat(fixture.claim("node-b").get("sourceConfigAckFence", Number.class).longValue()).isEqualTo(1);
                } finally {
                    pause.resume.countDown();
                }
            }
        }
    }

    @Test
    void theDefaultPauseSeparatesRealConflictingTransactions() throws Exception {
        try (Fixture fixture = new Fixture()) {
            Trace trace = new Trace();
            try (MongoClient contender = fixture.client(trace, "default-pause")) {
                SourceConfigKeyringStore rings = new SourceConfigKeyringStore(contender,
                        contender.getDatabase(fixture.database.getName()));
                rings.loadExistingCipher();
                fixture.holdRing();
                WorkloadClaim node = fixture.node("node-b", "boot-b", TTL);
                CompletableFuture<Boolean> ack = CompletableFuture.supplyAsync(() -> rings.acknowledge(node, 1, TTL), fixture.pool);
                trace.firstConflict.get(5, TimeUnit.SECONDS);
                trace.assertConflict();
                fixture.commitHolder();
                assertThat(ack.get(5, TimeUnit.SECONDS)).isTrue();
                assertThat(trace.attempts.get()).isBetween(2, 3);
                assertThat(Duration.ofNanos(trace.starts.get(1) - trace.failures.getFirst()))
                        .as("the default policy waits after the real conflict before starting a new transaction")
                        .isGreaterThanOrEqualTo(Duration.ofMillis(50));
                fixture.assertAck(node, 1);
            }
        }
    }

    @Test
    void aPersistentRealConflictExhaustsExactlyThreeAttemptsWithoutPublishingAnAck() throws Exception {
        try (Fixture fixture = new Fixture()) {
            AtomicInteger pauses = new AtomicInteger();
            Trace trace = new Trace();
            try (MongoClient contender = fixture.client(trace, "persistent")) {
                SourceConfigKeyringStore rings = new SourceConfigKeyringStore(contender,
                        contender.getDatabase(fixture.database.getName()), attempt -> pauses.incrementAndGet());
                fixture.holdRing();
                WorkloadClaim node = fixture.node("node-b", "boot-b", TTL);
                assertThatThrownBy(() -> rings.acknowledge(node, 1, TTL))
                        .isInstanceOfSatisfying(TapstateException.class,
                                error -> assertUnavailable(error, 112));
                trace.assertConflict();
                assertThat(trace.attempts).hasValue(3);
                assertThat(trace.conflicts).hasValue(3);
                assertThat(pauses).hasValue(2);
                assertThat(fixture.ack("node-b")).isNull();
                assertThat(fixture.claim("node-b")).doesNotContainKey("sourceConfigAckFence");
            }
        }
    }

    @ParameterizedTest
    @EnumSource(Change.class)
    void aRetryRechecksTheOwnerEpochAndServerLeaseAfterItsWait(Change change) throws Exception {
        try (Fixture fixture = new Fixture()) {
            Pause pause = new Pause();
            Trace trace = new Trace();
            try (MongoClient contender = fixture.client(trace, "changed")) {
                SourceConfigKeyringStore rings = new SourceConfigKeyringStore(contender,
                        contender.getDatabase(fixture.database.getName()), pause);
                rings.loadExistingCipher();
                fixture.holdRing();
                WorkloadClaim node = fixture.node("node-b", "boot-b",
                        change == Change.EXPIRED ? Duration.ofSeconds(1) : TTL);
                CompletableFuture<Boolean> ack = CompletableFuture.supplyAsync(() -> rings.acknowledge(node, 1, TTL), fixture.pool);
                try {
                    awaitPause(pause, ack);
                    trace.assertConflict();
                    fixture.commitHolder();
                    WorkloadClaim replacement = null;
                    if (change == Change.REPLACED) {
                        assertThat(fixture.claims.release(node)).isTrue();
                        replacement = fixture.node("node-b", "boot-new", TTL);
                        assertThat(replacement.claimGeneration()).isGreaterThan(node.claimGeneration());
                    } else if (change == Change.EPOCH) {
                        assertThat(fixture.rings.prepareRotation().epoch()).isEqualTo(2);
                    } else {
                        long deadline = System.nanoTime() + Duration.ofSeconds(5).toNanos();
                        while (fixture.claims.read(node.key()).orElseThrow().leased() && System.nanoTime() < deadline) {
                            Thread.sleep(10);
                        }
                        assertThat(fixture.claims.read(node.key()).orElseThrow().leased()).isFalse();
                    }
                    pause.resume.countDown();
                    assertThat(ack.get(5, TimeUnit.SECONDS)).isFalse();
                    assertThat(fixture.ack("node-b")).isNull();
                    assertThat(fixture.claim("node-b")).doesNotContainKey("sourceConfigAckFence");
                    SourceConfigKeyringHandle handle = new SourceConfigKeyringHandle(rings);
                    if (change == Change.EPOCH) {
                        handle.acknowledge(node, TTL);
                        fixture.assertAck(node, 2);
                        assertThat(handle.epoch()).isEqualTo(2);
                    } else {
                        assertThatThrownBy(() -> handle.acknowledge(node, TTL))
                                .isInstanceOfSatisfying(TapstateException.class, error -> {
                                    assertThat(error.code()).isEqualTo(StoreError.SOURCE_CONFIG_KEYRING_NOT_READY);
                                    assertThat(error.getCause()).isNull();
                                });
                        if (replacement != null) {
                            handle.acknowledge(replacement, TTL);
                            fixture.assertAck(replacement, 1);
                        }
                    }
                } finally {
                    pause.resume.countDown();
                }
            }
        }
    }

    @Test
    void interruptingTheWaitPreservesTheFlagAndDoesNotStartAnotherTransaction() throws Exception {
        try (Fixture fixture = new Fixture()) {
            AtomicInteger pauses = new AtomicInteger();
            Trace trace = new Trace();
            try (MongoClient contender = fixture.client(trace, "interrupted")) {
                SourceConfigKeyringStore rings = new SourceConfigKeyringStore(contender,
                        contender.getDatabase(fixture.database.getName()), attempt -> {
                            pauses.incrementAndGet();
                            Thread.currentThread().interrupt();
                            Thread.sleep(1);
                        });
                fixture.holdRing();
                WorkloadClaim node = fixture.node("node-b", "boot-b", TTL);
                Outcome result = CompletableFuture.supplyAsync(() -> {
                    try {
                        rings.acknowledge(node, 1, TTL);
                        throw new AssertionError("interrupted retry unexpectedly acknowledged");
                    } catch (TapstateException failure) {
                        return new Outcome(failure, Thread.currentThread().isInterrupted());
                    } finally {
                        Thread.interrupted();
                    }
                }, fixture.pool).get(5, TimeUnit.SECONDS);
                assertUnavailable(result.failure, 112);
                assertThat(result.interrupted).isTrue();
                assertThat(pauses).hasValue(1);
                assertThat(trace.attempts).hasValue(1);
                trace.assertConflict();
                assertThat(fixture.ack("node-b")).isNull();
                assertThat(fixture.claim("node-b")).doesNotContainKey("sourceConfigAckFence");
            }
        }
    }

    @Test
    void anUnknownCommitResultDoesNotReplayTheAcknowledgementTransaction() throws Exception {
        try (Fixture fixture = new Fixture()) {
            AtomicInteger pauses = new AtomicInteger();
            Trace trace = new Trace();
            String applicationName = "ack-commit-" + UUID.randomUUID();
            try (MongoClient contender = fixture.client(trace, applicationName)) {
                SourceConfigKeyringStore rings = new SourceConfigKeyringStore(contender,
                        contender.getDatabase(fixture.database.getName()), attempt -> pauses.incrementAndGet());
                WorkloadClaim node = fixture.node("node-b", "boot-b", TTL);
                fixture.admin.runCommand(new Document("configureFailPoint", "failCommand")
                        .append("mode", "alwaysOn").append("data", new Document("appName", applicationName)
                                .append("failCommands", List.of("commitTransaction"))
                                .append("errorCode", 64)
                                .append("errorLabels", List.of("TransientTransactionError", "UnknownTransactionCommitResult"))));
                try {
                    assertThatThrownBy(() -> rings.acknowledge(node, 1, TTL))
                            .isInstanceOfSatisfying(TapstateException.class,
                                    error -> assertUnavailable(error, 64));
                    assertThat(trace.unknownCommits).hasValueGreaterThanOrEqualTo(1);
                    assertThat(trace.attempts).hasValue(1);
                    assertThat(trace.ringWrites).hasValue(1);
                    assertThat(pauses).hasValue(0);
                    assertThat(fixture.ack("node-b")).isNull();
                } finally {
                    fixture.admin.runCommand(new Document("configureFailPoint", "failCommand").append("mode", "off"));
                }
            }
        }
    }

    private static void awaitPause(Pause pause, CompletableFuture<?> acknowledgement) throws Exception {
        try {
            CompletableFuture.anyOf(pause.entered, acknowledgement).get(5, TimeUnit.SECONDS);
        } catch (ExecutionException exhausted) {
            throw new AssertionError("A live ACK exhausted its retries before the holding transaction could commit", exhausted.getCause());
        }
        assertThat(pause.entered.isDone()).as("a real conflict reached the bounded retry wait").isTrue();
    }

    private static void assertUnavailable(TapstateException error, int code) {
        assertThat(error.code()).isEqualTo(IoError.STORE_UNAVAILABLE);
        assertThat(error.args().get("detail").toString()).endsWith(" code=" + code);
        assertThat(error.getCause()).isNull();
    }

    private enum Change { REPLACED, EPOCH, EXPIRED }
    private record Outcome(TapstateException failure, boolean interrupted) {}

    private static final class Pause implements SourceConfigKeyringStore.AcknowledgementRetryPause {
        private final CompletableFuture<Void> entered = new CompletableFuture<>();
        private final CountDownLatch resume = new CountDownLatch(1);
        private final AtomicInteger calls = new AtomicInteger();

        @Override
        public void pause(int failedAttempt) throws InterruptedException {
            calls.incrementAndGet();
            entered.complete(null);
            if (!resume.await(10, TimeUnit.SECONDS)) throw new AssertionError("owned ACK retry was not resumed");
        }
    }

    private static final class Trace implements CommandListener {
        private final AtomicInteger attempts = new AtomicInteger();
        private final AtomicInteger ringWrites = new AtomicInteger();
        private final AtomicInteger conflicts = new AtomicInteger();
        private final AtomicInteger unknownCommits = new AtomicInteger();
        private final AtomicReference<MongoException> conflict = new AtomicReference<>();
        private final CompletableFuture<Void> firstConflict = new CompletableFuture<>();
        private final List<Long> starts = new CopyOnWriteArrayList<>();
        private final List<Long> failures = new CopyOnWriteArrayList<>();

        @Override
        public void commandStarted(CommandStartedEvent event) {
            if (event.getCommand().getBoolean("startTransaction", org.bson.BsonBoolean.FALSE).getValue()) {
                starts.add(System.nanoTime());
                attempts.incrementAndGet();
            }
            if (isRingFence(event)) ringWrites.incrementAndGet();
        }

        @Override
        public void commandFailed(CommandFailedEvent event) {
            if (!(event.getThrowable() instanceof MongoException error)) return;
            if (error.getCode() == 112) {
                conflict.set(error);
                conflicts.incrementAndGet();
                failures.add(System.nanoTime());
                firstConflict.complete(null);
            }
            if (error.hasErrorLabel("UnknownTransactionCommitResult")) unknownCommits.incrementAndGet();
        }

        private void assertConflict() {
            assertThat(conflict.get()).isNotNull();
            assertThat(conflict.get().getCode()).isEqualTo(112);
            assertThat(conflict.get().hasErrorLabel("TransientTransactionError")).isTrue();
            assertThat(conflict.get().hasErrorLabel("UnknownTransactionCommitResult")).isFalse();
        }
    }

    private static boolean isRingFence(CommandStartedEvent event) {
        if (!"update".equals(event.getCommandName())) return false;
        var update = event.getCommand().getArray("updates").getFirst().asDocument().getDocument("u");
        return update.containsKey("$inc") && update.getDocument("$inc").containsKey("nodeAckFence");
    }

    private static final class Fixture implements AutoCloseable {
        private final MongoClient control = MongoClients.create(MONGO.getReplicaSetUrl());
        private final MongoDatabase database = control.getDatabase("keyring_ack_retry_" + UUID.randomUUID().toString().replace("-", ""));
        private final MongoDatabase admin = control.getDatabase("admin");
        private final MongoWorkloadClaimStore claims = new MongoWorkloadClaimStore(SystemCollections.WORKLOAD_CLAIMS.on(database));
        private final SourceConfigKeyringStore rings = new SourceConfigKeyringStore(control, database);
        private final ExecutorService pool = Executors.newCachedThreadPool(Thread.ofPlatform().daemon().factory());
        private final CountDownLatch holderEntered = new CountDownLatch(1);
        private final CountDownLatch holderResume = new CountDownLatch(1);
        private final AtomicInteger holderRequest = new AtomicInteger(-1);
        private final AtomicReference<Throwable> hookFailure = new AtomicReference<>();
        private MongoClient holderClient;
        private CompletableFuture<Boolean> holder;

        private Fixture() { rings.loadOrCreateCipher(); }

        private MongoClient client(CommandListener listener, String applicationName) {
            return MongoClients.create(MongoClientSettings.builder()
                    .applyConnectionString(new ConnectionString(MONGO.getReplicaSetUrl()))
                    .applicationName(applicationName).addCommandListener(listener).build());
        }

        private WorkloadClaim node(String id, String boot, Duration ttl) {
            return claims.acquire(new WorkloadClaimKey("cluster-retry", WorkloadClaimType.NODE_SESSION, id),
                    new WorkloadOwner(id, boot), 0, ttl).claim();
        }

        private void holdRing() throws Exception {
            holderClient = client(new CommandListener() {
                @Override
                public void commandStarted(CommandStartedEvent event) {
                    if (isRingFence(event)) holderRequest.set(event.getRequestId());
                }
                @Override
                public void commandSucceeded(CommandSucceededEvent event) {
                    if (event.getRequestId() != holderRequest.get()) return;
                    holderEntered.countDown();
                    try {
                        if (!holderResume.await(15, TimeUnit.SECONDS)) throw new AssertionError("owned ring holder was not resumed");
                    } catch (Throwable failure) {
                        hookFailure.set(failure);
                        if (failure instanceof InterruptedException) Thread.currentThread().interrupt();
                    }
                }
            }, "holder");
            SourceConfigKeyringStore holding = new SourceConfigKeyringStore(holderClient, holderClient.getDatabase(database.getName()));
            WorkloadClaim node = node("node-a", "boot-a", TTL);
            holder = CompletableFuture.supplyAsync(() -> holding.acknowledge(node, 1, TTL), pool);
            assertThat(holderEntered.await(5, TimeUnit.SECONDS)).as("the server accepted the holding transaction's ring write").isTrue();
        }

        private void commitHolder() throws Exception {
            holderResume.countDown();
            assertThat(holder.get(5, TimeUnit.SECONDS)).isTrue();
            assertThat(hookFailure.get()).isNull();
        }

        private Document ring() { return SystemCollections.SYSTEM_META.on(database).find(new Document("_id", "source-config-keyring")).first(); }
        private Document claim(String nodeId) { return SystemCollections.WORKLOAD_CLAIMS.on(database).find(new Document("ownerNodeId", nodeId)).first(); }
        private Document ack(String nodeId) { return SystemCollections.SYSTEM_META.on(database).find(new Document("kind", "source-config-keyring-node").append("nodeId", nodeId)).first(); }

        private void assertAck(WorkloadClaim node, long epoch) {
            Document acknowledged = ack(node.key().resourceId());
            assertThat(acknowledged).containsEntry("bootId", node.owner().bootId())
                    .containsEntry("claimGeneration", node.claimGeneration()).containsEntry("epoch", epoch);
            assertThat(acknowledged.getDate("leaseUntil")).isBeforeOrEqualTo(claim(node.owner().nodeId()).getDate("leaseUntil"));
            assertThat(claims.read(node.key()).orElseThrow().leased()).isTrue();
        }

        @Override
        public void close() throws Exception {
            try {
                holderResume.countDown();
                if (holder != null) assertThat(holder.get(5, TimeUnit.SECONDS)).isTrue();
                assertThat(hookFailure.get()).isNull();
            } finally {
                pool.shutdownNow();
                boolean stopped;
                try {
                    stopped = pool.awaitTermination(5, TimeUnit.SECONDS);
                } finally {
                    try {
                        if (holderClient != null) holderClient.close();
                    } finally {
                        control.close();
                    }
                }
                assertThat(stopped).isTrue();
            }
        }
    }
}
