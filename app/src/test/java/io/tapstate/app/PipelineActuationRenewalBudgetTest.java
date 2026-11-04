package io.tapstate.app;

import io.tapstate.spi.store.CacheBusinessScope;
import io.tapstate.spi.store.ClusterMembership;
import io.tapstate.spi.store.DeadlineWorkloadClaimStore;
import io.tapstate.spi.store.PipelineIntentScope;
import io.tapstate.spi.store.WorkloadClaim;
import io.tapstate.spi.store.WorkloadClaimFence;
import io.tapstate.spi.store.WorkloadClaimKey;
import io.tapstate.spi.store.WorkloadClaimType;
import io.tapstate.spi.store.WorkloadOwner;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Supplier;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.ValueSource;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.anySet;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

class PipelineActuationRenewalBudgetTest {
    private static final Duration TTL = Duration.ofSeconds(30);
    private static final Duration RENEW = Duration.ofSeconds(5);
    private static final CacheBusinessScope BUSINESS = new CacheBusinessScope("cluster-a", 3, "cache-a");
    private static final WorkloadOwner OWNER = new WorkloadOwner("node-a", "boot-a");
    // All seven identifiers share a HashMap bucket, including the newly admitted tail.
    private static final List<String> IDS = List.of("AaAaAa", "AaAaBB", "AaBBAa", "AaBBBB",
            "BBAaAa", "BBAaBB", "BBBBAa");

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    void aQueuedFastPathPermitSpendsItsDeadlineWhileWaitingForIo(boolean scoped) throws Exception {
        try (var io = new ControlledIo(); var callers = Executors.newFixedThreadPool(2)) {
            var owner = io.owner(scoped);
            var original = scoped ? seedOne(io, owner, IDS.getFirst()) : owner.permit(IDS.getFirst()).claim();
            assertThat(original).isNotNull();
            io.makeDue();io.release = new CountDownLatch(1);
            var renewal = callers.submit(owner::renewDue);
            try {
                assertThat(io.entered.await(2, TimeUnit.SECONDS)).isTrue();
                var queued = callers.submit(() -> {
                    var granted = new java.util.concurrent.atomic.AtomicBoolean(true);
                    new ShutdownAwait().complete(deadline(100), () -> granted.set(scoped
                            ? owner.permit(scope(IDS.getFirst())).granted() : owner.permit(IDS.getFirst()).granted()));
                    return granted.get();
                });
                assertThat(queued.get(1, TimeUnit.SECONDS)).isFalse();
                assertThat(renewal.isDone()).isFalse();
                assertThat(io.attempts).containsExactly(IDS.getFirst());
            } finally {
                owner.stopForShutdown();io.release.countDown();
                renewal.get(2, TimeUnit.SECONDS);owner.awaitStopped(deadline(2000));
            }
        }
    }

    @Test void independentRenewalsDoNotSpendTheUncontactedTailsBudget() throws Exception {
        try (var io = new ControlledIo()) {
            var owner = io.owner();
            var originals = seed(io, owner, 5);
            io.makeDue();
            io.delayMillis = 3000;
            owner.renewDue();
            io.awaitQuiescence(deadline(5000));
            assertThat(io.attempts).containsExactlyElementsOf(IDS.subList(0, 5));
            assertThat(io.allowances).allSatisfy(remaining -> assertThat(remaining).isGreaterThan(Duration.ofSeconds(9).toNanos()));
            io.delayMillis = 0;
            io.makeDue();owner.renewDue();
            io.makeDue();owner.renewDue();
            io.raw.clockAt(io.epoch.plusSeconds(31));
            assertLive(io, owner, originals);
        }
    }

    @ParameterizedTest
    @CsvSource({"6,5", "4,10"})
    void aNormallyAdmittedMidBatchClaimIsRenewedBeforeItsLeaseExpires(int oldCount, int intervalSeconds) throws Exception {
        try (var io = new ControlledIo(); var callers = Executors.newFixedThreadPool(2)) {
            var interval = Duration.ofSeconds(intervalSeconds);
            var owner = io.owner(true, interval);
            var originals = seed(io, owner, oldCount);
            io.localOffset.addAndGet(interval.toNanos());io.delayMillis = 4000;
            io.release = new CountDownLatch(1);
            var renewal = callers.submit(owner::renewDue);
            try {
                assertThat(io.entered.await(2, TimeUnit.SECONDS)).isTrue();
                String newcomer = IDS.get(oldCount);
                var admitting = callers.submit(() -> seedOne(io, owner, newcomer));
                awaitQueuedAdmission(owner);
                io.release.countDown();
                originals.add(admitting.get(15, TimeUnit.SECONDS));
                renewal.get(40, TimeUnit.SECONDS);
                assertThat(io.attempts).containsExactlyElementsOf(IDS.subList(0, oldCount));
                Thread.sleep(1000);
                owner.renewDue();
                // The newcomer's acquire reply precedes later tail renewals, so it gets an earlier turn.
                assertThat(io.attempts.subList(oldCount, oldCount + 2)).contains(newcomer);
                assertLive(io, owner, originals);
            } finally {
                io.release.countDown();
                owner.stopForShutdown();
                renewal.get(12, TimeUnit.SECONDS);
                owner.awaitStopped(deadline(5000));
            }
        }
    }

    @Test void shutdownKeepsActualIoOwnedAndNeverAttemptsTheRemainingTail() throws Exception {
        try (var io = new ControlledIo(); var callers = Executors.newFixedThreadPool(2)) {
            var owner = io.owner();
            var originals = seed(io, owner, 5);
            io.makeDue();io.release = new CountDownLatch(1);
            var renewal = callers.submit(owner::renewDue);
            try {
                assertThat(io.entered.await(2, TimeUnit.SECONDS)).isTrue();
                var queued = callers.submit(() -> owner.permit(scope(IDS.get(6))));
                awaitQueuedAdmission(owner);
                owner.stopForShutdown();
                assertThatThrownBy(() -> owner.awaitStopped(deadline(50)))
                        .isInstanceOf(io.tapstate.core.common.TapstateException.class);
                assertThat(renewal.isDone()).isFalse();assertThat(queued.isDone()).isFalse();
                assertThat(io.tasks).anyMatch(task -> !task.isDone());
                io.release.countDown();
                renewal.get(2, TimeUnit.SECONDS);
                assertThat(queued.get(2, TimeUnit.SECONDS).granted()).isFalse();
                owner.awaitStopped(deadline(2000));
                assertThat(io.attempts).containsExactly(IDS.getFirst());
                for (var original : originals) {
                    var reading = io.raw.read(original.key()).orElseThrow();
                    assertThat(reading.leased()).isTrue();
                    assertThat(WorkloadClaimFence.from(reading.claim())).isEqualTo(WorkloadClaimFence.from(original));
                }
                assertThat(owner.permit(scope(IDS.get(6))).granted()).isFalse();
            } finally {
                io.release.countDown();owner.stopForShutdown();
                renewal.get(12, TimeUnit.SECONDS);owner.awaitStopped(deadline(5000));
            }
        }
    }

    private static ArrayList<WorkloadClaim> seed(ControlledIo io, PipelineActuationOwnership owner, int count) {
        var originals = new ArrayList<WorkloadClaim>();
        for (var id : IDS.subList(0, count)) originals.add(seedOne(io, owner, id));
        return originals;
    }

    private static WorkloadClaim seedOne(ControlledIo io, PipelineActuationOwnership owner, String id) {
        var permit = owner.permit(scope(id));assertThat(permit.granted()).isTrue();
        var execution = owner.beginExecution(scope(id), WorkloadClaimFence.from(permit.claim()));
        assertThat(execution.allowed()).isTrue();
        var advanced = io.raw.read(permit.claim().key()).orElseThrow().claim();
        assertThat(advanced.executionGeneration()).isPositive();
        return advanced;
    }

    private static void assertLive(ControlledIo io, PipelineActuationOwnership owner, List<WorkloadClaim> originals) {
        for (var original : originals) {
            var reading = io.raw.read(original.key()).orElseThrow();
            assertThat(reading.leased()).as(original.key().resourceId()).isTrue();
            assertThat(WorkloadClaimFence.from(reading.claim())).isEqualTo(WorkloadClaimFence.from(original));
            assertThat(owner.heldExecutionGeneration(original.key().resourceId())).isEqualTo(original.executionGeneration());
        }
    }

    private static void awaitQueuedAdmission(PipelineActuationOwnership owner) throws Exception {
        var field = PipelineActuationOwnership.class.getDeclaredField("ioAdmission");field.setAccessible(true);
        var gate = (java.util.concurrent.locks.ReentrantLock) field.get(owner);
        long limit = deadline(2000);
        while (!gate.hasQueuedThreads() && System.nanoTime() < limit) Thread.sleep(1);
        assertThat(gate.hasQueuedThreads()).isTrue();
    }

    private static PipelineIntentScope scope(String id) {
        return new PipelineIntentScope("cluster-a", 3, "cache-a", id, 1);
    }

    private static long deadline(long millis) { return System.nanoTime() + Duration.ofMillis(millis).toNanos(); }

    /** Caller timeout retains the actual task; only explicit release ends blocked IO. */
    private static final class ControlledIo implements AutoCloseable {
        private final InMemoryWorkloadClaimStore raw = new InMemoryWorkloadClaimStore();
        private final DeadlineWorkloadClaimStore store = mock(DeadlineWorkloadClaimStore.class);
        private final java.util.concurrent.ExecutorService worker = Executors.newSingleThreadExecutor();
        private final List<Future<?>> tasks = new CopyOnWriteArrayList<>();
        private final List<String> attempts = new CopyOnWriteArrayList<>();
        private final List<Long> allowances = new CopyOnWriteArrayList<>();
        private final ThreadLocal<Long> budget = new ThreadLocal<>();
        private final AtomicLong localOffset = new AtomicLong();
        private final Instant epoch = Instant.parse("2026-09-18T00:00:00Z");
        private final long started = System.nanoTime();
        private final CountDownLatch entered = new CountDownLatch(1);
        private volatile CountDownLatch release = new CountDownLatch(0);
        private volatile long delayMillis;

        private ControlledIo() {
            doAnswer(call -> bounded(call.getArgument(0), call.getArgument(1), call.getArgument(2)))
                    .when(store).withDeadline(anyLong(), any(), any());
            doAnswer(call -> { awaitQuiescence(call.getArgument(0));return null; })
                    .when(store).awaitQuiescence(anyLong());
            when(store.acquirePipeline(any(), any(), anyLong(), any())).thenAnswer(call -> {
                syncClock();PipelineIntentScope scope = call.getArgument(0);
                return raw.acquire(new WorkloadClaimKey(scope.clusterId(), WorkloadClaimType.PIPELINE_ACTUATION,
                        scope.pipelineId()), call.getArgument(1), call.getArgument(2), call.getArgument(3));
            });
            when(store.acquire(any(), any(), anyLong(), any())).thenAnswer(call -> {
                syncClock();return raw.acquire(call.getArgument(0), call.getArgument(1), call.getArgument(2), call.getArgument(3));
            });
            when(store.advanceExecution(any(), anyLong(), anySet())).thenAnswer(call -> {
                syncClock();return raw.advanceExecution(call.getArgument(0), call.getArgument(1), call.getArgument(2));
            });
            when(store.renew(any(), any())).thenAnswer(call -> {
                WorkloadClaim original = call.getArgument(0);
                attempts.add(original.key().resourceId());allowances.add(budget.get() - System.nanoTime());
                entered.countDown();
                if (!release.await(20, TimeUnit.SECONDS)) throw new AssertionError("IO was not released");
                Thread.sleep(delayMillis);
                syncClock();return raw.renew(original, call.getArgument(1));
            });
        }

        private PipelineActuationOwnership owner() { return owner(true); }

        private PipelineActuationOwnership owner(boolean scoped) { return owner(scoped, RENEW); }

        private PipelineActuationOwnership owner(boolean scoped, Duration interval) {
            var properties = new ClusterProperties();properties.setProfile(ClusterProperties.Profile.PRODUCTION_HA);
            var membership = new ClusterMembershipGate(properties);membership.observeNodeSession(() -> true);
            membership.install(new ClusterMembership("cluster-a", 7, Set.of("node-a", "node-b", "node-c")));
            membership.canCommit(Set.of("node-a", "node-b"));
            assertThat(membership.businessEligible()).isTrue();
            if (!scoped) return new PipelineActuationOwnership("cluster-a", OWNER, membership,
                    new ClusterWorkloadClaims(store, membership), TTL, interval,
                    () -> System.nanoTime() + localOffset.get());
            return PipelineActuationOwnership.scoped(BUSINESS, OWNER, membership,
                    new ClusterWorkloadClaims(store, membership), TTL, interval,
                    () -> System.nanoTime() + localOffset.get());
        }

        private void makeDue() { localOffset.addAndGet(RENEW.toNanos()); }
        private void syncClock() { raw.clockAt(epoch.plusNanos(System.nanoTime() - started)); }

        private Object bounded(long deadline, Runnable guard, Supplier<?> action) {
            Future<?> task = worker.submit(() -> {
                budget.set(deadline);
                try { guard.run();var value = action.get();guard.run();return value; }
                finally { budget.remove(); }
            });
            tasks.add(task);
            try { return task.get(Math.max(0, deadline - System.nanoTime()), TimeUnit.NANOSECONDS); }
            catch (java.util.concurrent.TimeoutException expired) { throw StorageRuntime.unavailable(); }
            catch (InterruptedException interrupted) { Thread.currentThread().interrupt();throw new AssertionError(interrupted); }
            catch (java.util.concurrent.ExecutionException failed) {
                if (failed.getCause() instanceof RuntimeException runtime) throw runtime;
                if (failed.getCause() instanceof Error error) throw error;
                throw new AssertionError(failed.getCause());
            }
        }

        private void awaitQuiescence(long deadline) {
            for (var task : tasks) {
                try { task.get(Math.max(0, deadline - System.nanoTime()), TimeUnit.NANOSECONDS); }
                catch (java.util.concurrent.ExecutionException ended) { /* The actual task has returned. */ }
                catch (java.util.concurrent.TimeoutException expired) { throw StorageRuntime.unavailable(); }
                catch (InterruptedException interrupted) { Thread.currentThread().interrupt();throw new AssertionError(interrupted); }
            }
        }

        @Override public void close() throws Exception {
            release.countDown();worker.shutdown();
            assertThat(worker.awaitTermination(20, TimeUnit.SECONDS)).isTrue();
        }
    }
}
