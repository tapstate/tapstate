package io.tapstate.app;

import io.tapstate.core.common.TapstateException;
import io.tapstate.core.event.ChainPosition;
import io.tapstate.core.event.SourceOrder;
import io.tapstate.spi.store.*;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;

import java.time.Duration;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.assertj.core.api.Assertions.*;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

class CheckpointProgressWritesTest {
    private static final PipelineIntentScope SCOPE = new PipelineIntentScope("cluster", 3, "cache", "pipeline", 7);
    private static final WorkloadClaimFence FENCE = new WorkloadClaimFence(
            new WorkloadClaimKey("cluster", WorkloadClaimType.PIPELINE_ACTUATION, "pipeline"),
            new WorkloadOwner("node", "boot"), 2, 5, 9);
    private static final SrsConsumerRun RUN = SrsConsumerRun.of(SCOPE, new SrsConsumerMembership(
            new SrsConsumerContext(new SrsConsumerKey("chain", "pipeline", "source"), 7), 4, false, Set.of("orders"), FENCE));

    @Test void foldsBoundsWithoutLosingTheLastResumableTokenAndWaitsForCommit() throws Exception {
        var store = mock(SrsConsumerStore.class);
        when(store.advanceWriter(any(), anyLong(), anyString(), anyString(), anyString(), any(), any())).thenReturn(true);
        var bound = bound(store, () -> {});
        var threads = new CopyOnWriteArrayList<Thread>();
        try (var callers = Executors.newFixedThreadPool(2, task -> thread(task, threads));
             var writes = new CheckpointProgressWrites(8, 8192, 8, Duration.ofHours(1))) {
            var first = callers.submit(() -> writes.writer(bound, null, "run", "orders", "writer", progress(10, "token-10")));
            awaitRequests(writes, 1);
            var second = callers.submit(() -> writes.writer(bound, null, "run", "orders", "writer", progress(20, null)));
            awaitRequests(writes, 2);
            assertThat(first).isNotDone();
            assertThat(second).isNotDone();
            verifyNoInteractions(store);
            writes.flush();
            first.get(5, TimeUnit.SECONDS);
            second.get(5, TimeUnit.SECONDS);
            var report = ArgumentCaptor.forClass(WriterProgress.class);
            verify(store).advanceWriter(eq(RUN.context()), eq(4L), eq("run"), eq("orders"), eq("writer"), report.capture(), eq(FENCE));
            assertThat(report.getValue().durableThrough()).isEqualTo(new SourceOrder(1, 20));
            assertThat(report.getValue().lastTokened()).isEqualTo(new ChainPosition(new SourceOrder(1, 10), "token-10"));
        }
    }

    @Test void batchesDistinctWritersOnlyWithinTheirOriginalRun() throws Exception {
        var store = mock(SrsConsumerStore.class);
        var committed = new WriterProgressOutcome(true, null);
        when(store.advanceWriters(any(), anyLong(), anyString(), anyList(), any())).thenReturn(List.of(committed, committed));
        var bound = bound(store, () -> {});
        var threads = new CopyOnWriteArrayList<Thread>();
        try (var callers = Executors.newFixedThreadPool(2, task -> thread(task, threads));
             var writes = new CheckpointProgressWrites(8, 8192, 8, Duration.ofHours(1))) {
            var first = callers.submit(() -> writes.writer(bound, null, "run", "orders", "one", progress(10, "token-10")));
            awaitRequests(writes, 1);
            var second = callers.submit(() -> writes.writer(bound, null, "run", "orders", "two", progress(20, "token-20")));
            awaitRequests(writes, 2);
            writes.flush();
            first.get(5, TimeUnit.SECONDS);
            second.get(5, TimeUnit.SECONDS);
            verify(store).advanceWriters(RUN.context(), 4, "run", List.of(
                    new WriterProgressUpdate("orders", "one", progress(10, "token-10")),
                    new WriterProgressUpdate("orders", "two", progress(20, "token-20"))), FENCE);
            verify(store, never()).advanceWriter(any(), anyLong(), anyString(), anyString(), anyString(), any(), any());
        }
    }

    @Test void aRefusedAtomicBatchRefusesEveryIncludedCaller() throws Exception {
        var store = mock(SrsConsumerStore.class);
        var refused = new WriterProgressOutcome(false, null);
        when(store.advanceWriters(any(), anyLong(), anyString(), anyList(), any())).thenReturn(List.of(refused, refused));
        var bound = bound(store, () -> {});
        var threads = new CopyOnWriteArrayList<Thread>();
        try (var callers = Executors.newFixedThreadPool(2, task -> thread(task, threads));
             var writes = new CheckpointProgressWrites(8, 8192, 8, Duration.ofHours(1))) {
            var first = callers.submit(() -> writes.writer(bound, null, "run", "orders", "one", progress(10, null)));
            awaitRequests(writes, 1);
            var second = callers.submit(() -> writes.writer(bound, null, "run", "orders", "two", progress(20, null)));
            awaitRequests(writes, 2);
            writes.flush();
            for (var caller : List.of(first, second))
                assertThatThrownBy(() -> caller.get(5, TimeUnit.SECONDS)).hasCauseInstanceOf(TapstateException.class);
            verify(store).advanceWriters(eq(RUN.context()), eq(4L), eq("run"), anyList(), eq(FENCE));
        }
    }

    @Test void aDeterministicallyRefusedSiblingKeepsItsExactErrorWithoutFailingAValidReceipt() throws Exception {
        var store = mock(SrsConsumerStore.class);
        var refused = new TapstateException(IoError.SRS_CONSUMER_UNAVAILABLE, java.util.Map.of("chain", "chain"), null);
        when(store.advanceWriters(any(), anyLong(), anyString(), anyList(), any())).thenReturn(List.of(
                new WriterProgressOutcome(true, null), new WriterProgressOutcome(false, refused)));
        var bound = bound(store, () -> {});
        var threads = new CopyOnWriteArrayList<Thread>();
        try (var callers = Executors.newFixedThreadPool(2, task -> thread(task, threads));
             var writes = new CheckpointProgressWrites(8, 8192, 8, Duration.ofHours(1))) {
            var valid = callers.submit(() -> writes.writer(bound, null, "run", "orders", "valid", progress(10, "token-10")));
            awaitRequests(writes, 1);
            var invalid = callers.submit(() -> writes.writer(bound, null, "run", "orders", "unknown", progress(20, "token-20")));
            awaitRequests(writes, 2);
            writes.flush();
            Throwable validFailure = catchThrowable(() -> valid.get(5, TimeUnit.SECONDS));
            assertThatThrownBy(() -> invalid.get(5, TimeUnit.SECONDS)).hasCause(refused);
            assertThat(validFailure).as("a valid original receipt must remain independent of its refused sibling").isNull();
            verify(store, never()).advanceWriter(any(), anyLong(), anyString(), anyString(), anyString(), any(), any());
        }
    }

    @Test void anOutcomeUnknownBatchMustNotReplayIndividualWriterReports() throws Exception {
        var store = mock(SrsConsumerStore.class);
        var unknown = new TapstateException(IoError.STORE_UNAVAILABLE, java.util.Map.of("detail", "fixture outcome unknown"), null);
        when(store.advanceWriters(any(), anyLong(), anyString(), anyList(), any())).thenThrow(unknown);
        var bound = bound(store, () -> {});
        var threads = new CopyOnWriteArrayList<Thread>();
        try (var callers = Executors.newFixedThreadPool(2, task -> thread(task, threads));
             var writes = new CheckpointProgressWrites(8, 8192, 8, Duration.ofHours(1))) {
            var first = callers.submit(() -> writes.writer(bound, null, "run", "orders", "first", progress(10, "token-10")));
            awaitRequests(writes, 1);
            var second = callers.submit(() -> writes.writer(bound, null, "run", "orders", "second", progress(20, "token-20")));
            awaitRequests(writes, 2);
            writes.flush();
            assertThatThrownBy(() -> first.get(5, TimeUnit.SECONDS)).hasCause(unknown);
            assertThatThrownBy(() -> second.get(5, TimeUnit.SECONDS)).hasCause(unknown);
            verify(store, never()).advanceWriter(any(), anyLong(), anyString(), anyString(), anyString(), any(), any());
        }
    }

    @Test void distinctWriterRunsUseDistinctOriginalTransactions() throws Exception {
        var store = mock(SrsConsumerStore.class);
        when(store.advanceWriter(any(), anyLong(), anyString(), anyString(), anyString(), any(), any())).thenReturn(true);
        var bound = bound(store, () -> {});
        var threads = new CopyOnWriteArrayList<Thread>();
        try (var callers = Executors.newFixedThreadPool(2, task -> thread(task, threads));
             var writes = new CheckpointProgressWrites(8, 8192, 8, Duration.ofHours(1))) {
            var first = callers.submit(() -> writes.writer(bound, null, "first", "orders", "writer", progress(10, null)));
            awaitRequests(writes, 1);
            var second = callers.submit(() -> writes.writer(bound, null, "second", "orders", "writer", progress(20, null)));
            awaitRequests(writes, 2);
            writes.flush();
            first.get(5, TimeUnit.SECONDS);
            second.get(5, TimeUnit.SECONDS);
            verify(store).advanceWriter(RUN.context(), 4, "first", "orders", "writer", progress(10, null), FENCE);
            verify(store).advanceWriter(RUN.context(), 4, "second", "orders", "writer", progress(20, null), FENCE);
            verify(store, never()).advanceWriters(any(), anyLong(), anyString(), anyList(), any());
        }
    }

    @Test void anIndependentPipelineReceiptDoesNotWaitForAStalledCommit() throws Exception {
        var slowStore = mock(SrsConsumerStore.class);
        var fastStore = mock(SrsConsumerStore.class);
        var entered = new CountDownLatch(1);
        var release = new CountDownLatch(1);
        var healthyScope = new PipelineIntentScope("cluster", 3, "cache", "shipments", 7);
        var healthyClaim = new WorkloadClaimFence(
                new WorkloadClaimKey("cluster", WorkloadClaimType.PIPELINE_ACTUATION, "shipments"), FENCE.owner(), 2, 5, 9);
        var healthyRun = SrsConsumerRun.of(healthyScope, new SrsConsumerMembership(
                new SrsConsumerContext(new SrsConsumerKey("chain", "shipments", "source"), 7), 4, false,
                Set.of("orders"), healthyClaim));
        var healthy = new ScopedSrsConsumerAccess.Bound(healthyRun, fastStore, null, () -> {}, new SrsConsumerProgressReads(),
                fastStore, null, new ExecutionFence(healthyScope, healthyClaim), new ThreadLocal<>());
        var slow = bound(slowStore, () -> {});
        when(slowStore.advanceWriter(any(), anyLong(), anyString(), anyString(), anyString(), any(), any())).thenAnswer(call -> {
            entered.countDown();
            assertThat(release.await(5, TimeUnit.SECONDS)).isTrue();
            return true;
        });
        when(fastStore.advanceWriter(any(), anyLong(), anyString(), anyString(), anyString(), any(), any())).thenReturn(true);
        var threads = new CopyOnWriteArrayList<Thread>();
        try (var callers = Executors.newFixedThreadPool(3, task -> thread(task, threads));
             var flushing = Executors.newSingleThreadExecutor();
             var writes = new CheckpointProgressWrites(8, 8192, 8, Duration.ofHours(1))) {
            var first = callers.submit(() -> writes.writer(slow, null, "run", "orders", "writer", progress(10, null)));
            awaitRequests(writes, 1);
            var second = callers.submit(() -> writes.writer(healthy, null, "run", "orders", "writer", progress(10, null)));
            awaitRequests(writes, 2);
            var flush = flushing.submit(writes::flush);
            try {
                assertThat(entered.await(5, TimeUnit.SECONDS)).isTrue();
                second.get(1, TimeUnit.SECONDS);
                assertThat(first).isNotDone();
                awaitRequests(writes, 1);
                var samePipeline = callers.submit(() -> writes.writer(slow, null, "run", "orders", "writer", progress(20, null)));
                awaitRequests(writes, 2);
                writes.flush();
                assertThat(samePipeline).isNotDone();
                verify(slowStore, times(1)).advanceWriter(any(), anyLong(), anyString(), anyString(), anyString(), any(), any());
                release.countDown();
                flush.get(5, TimeUnit.SECONDS);
                first.get(5, TimeUnit.SECONDS);
                writes.flush();
                samePipeline.get(5, TimeUnit.SECONDS);
            } finally {
                release.countDown();
            }
        }
    }

    @Test void anInFlightRequestRetainsItsCapacityUntilItsTransactionFinishes() throws Exception {
        var store = mock(SrsConsumerStore.class);
        var entered = new CountDownLatch(1);
        var release = new CountDownLatch(1);
        when(store.advanceWriter(any(), anyLong(), anyString(), anyString(), anyString(), any(), any())).thenAnswer(call -> {
            entered.countDown();
            assertThat(release.await(5, TimeUnit.SECONDS)).isTrue();
            return true;
        });
        var bound = bound(store, () -> {});
        var threads = new CopyOnWriteArrayList<Thread>();
        try (var callers = Executors.newFixedThreadPool(2, task -> thread(task, threads));
             var flushing = Executors.newSingleThreadExecutor();
             var writes = new CheckpointProgressWrites(1, 8192, 1, Duration.ofHours(1))) {
            var first = callers.submit(() -> writes.writer(bound, null, "run", "orders", "one", progress(10, null)));
            awaitRequests(writes, 1);
            var commit = flushing.submit(writes::flush);
            try {
                assertThat(entered.await(5, TimeUnit.SECONDS)).isTrue();
                var second = callers.submit(() -> writes.writer(bound, null, "run", "orders", "two", progress(20, null)));
                awaitWaiting(threads, 2);
                assertThat(writes.retainedRequests()).isEqualTo(1);
                assertThat(second).isNotDone();
                verify(store, times(1)).advanceWriter(any(), anyLong(), anyString(), anyString(), anyString(), any(), any());
                release.countDown();
                commit.get(5, TimeUnit.SECONDS);
                first.get(5, TimeUnit.SECONDS);
                awaitRequests(writes, 1);
                writes.flush();
                second.get(5, TimeUnit.SECONDS);
            } finally { release.countDown(); }
        }
    }

    @Test void refusesExcessBackpressureWaitersAndCommitsTheAdmittedWaiterAfterRecovery() throws Exception {
        var store = mock(SrsConsumerStore.class);
        var entered = new CountDownLatch(1);
        var release = new CountDownLatch(1);
        when(store.advanceWriter(any(), anyLong(), anyString(), anyString(), anyString(), any(), any())).thenAnswer(call -> {
            entered.countDown();
            assertThat(release.await(5, TimeUnit.SECONDS)).isTrue();
            return true;
        });
        var bound = bound(store, () -> {});
        var threads = new CopyOnWriteArrayList<Thread>();
        try (var callers = Executors.newFixedThreadPool(2, task -> thread(task, threads));
             var flushing = Executors.newSingleThreadExecutor();
             var writes = new CheckpointProgressWrites(1, 1024, 1, Duration.ofHours(1))) {
            var first = callers.submit(() -> writes.writer(bound, null, "run", "orders", "one", progress(10, null)));
            awaitRequests(writes, 1);
            var commit = flushing.submit(writes::flush);
            try {
                assertThat(entered.await(5, TimeUnit.SECONDS)).isTrue();
                var waiting = callers.submit(() -> writes.writer(bound, null, "run", "orders", "two", progress(20, null)));
                awaitWaiting(threads, 2);
                assertThat(writes.retainedRequests()).isEqualTo(1);
                assertThat(waiting).isNotDone();
                assertThatThrownBy(() -> writes.writer(bound, null, "run", "orders", "excess", progress(30, null)))
                        .isInstanceOf(TapstateException.class)
                        .satisfies(failure -> assertThat(((TapstateException) failure).code()).isEqualTo(StorageRuntime.unavailable().code()));
                verify(store, times(1)).advanceWriter(any(), anyLong(), anyString(), anyString(), anyString(), any(), any());
                release.countDown();
                commit.get(5, TimeUnit.SECONDS);
                first.get(5, TimeUnit.SECONDS);
                awaitRequests(writes, 1);
                writes.flush();
                waiting.get(5, TimeUnit.SECONDS);
                verify(store).advanceWriter(RUN.context(), 4, "run", "orders", "two", progress(20, null), FENCE);
                verify(store, never()).advanceWriter(eq(RUN.context()), eq(4L), eq("run"), eq("orders"), eq("excess"), any(), eq(FENCE));
                assertThat(writes.retainedRequests()).isZero();
            } finally { release.countDown(); }
        }
    }

    @Test void rejectsAnOversizedReportBeforeAllocatingAWaiter() {
        var store = mock(SrsConsumerStore.class);
        try (var writes = new CheckpointProgressWrites(8, 1024, 8, Duration.ofHours(1))) {
            assertThatThrownBy(() -> writes.writer(bound(store, () -> {}), null, "run", "orders", "writer", progress(1, "x".repeat(1000))))
                    .isInstanceOf(TapstateException.class);
            verifyNoInteractions(store);
        }
    }

    @Test void propagatesStoreFailureAndReleasesCapacityForTheNextReport() throws Exception {
        var store = mock(SrsConsumerStore.class);
        var expected = new TapstateException(IoError.STORE_UNAVAILABLE, java.util.Map.of("detail", "test conflict"), null);
        when(store.advanceWriter(any(), anyLong(), anyString(), anyString(), anyString(), any(), any())).thenThrow(expected).thenReturn(true);
        var bound = bound(store, () -> {});
        var threads = new CopyOnWriteArrayList<Thread>();
        try (var callers = Executors.newSingleThreadExecutor(task -> thread(task, threads));
             var writes = new CheckpointProgressWrites(1, 8192, 1, Duration.ofHours(1))) {
            var first = callers.submit(() -> writes.writer(bound, null, "run", "orders", "writer", progress(10, null)));
            awaitRequests(writes, 1);
            writes.flush();
            assertThatThrownBy(() -> first.get(5, TimeUnit.SECONDS)).hasCause(expected);
            var second = callers.submit(() -> writes.writer(bound, null, "run", "orders", "writer", progress(20, null)));
            awaitRequests(writes, 1);
            writes.flush();
            second.get(5, TimeUnit.SECONDS);
        }
    }

    @Test void rechecksTheOriginalGuardWhenAQueuedReportReachesTheWriter() throws Exception {
        var active = new AtomicBoolean(true);
        var store = mock(SrsConsumerStore.class);
        var bound = bound(store, () -> { if (!active.get()) throw ScopedSrsConsumerAccess.refused(RUN); });
        var threads = new CopyOnWriteArrayList<Thread>();
        try (var callers = Executors.newSingleThreadExecutor(task -> thread(task, threads));
             var writes = new CheckpointProgressWrites(8, 8192, 8, Duration.ofHours(1))) {
            var first = callers.submit(() -> writes.writer(bound, null, "run", "orders", "writer", progress(10, null)));
            awaitRequests(writes, 1);
            active.set(false);
            writes.flush();
            assertThatThrownBy(() -> first.get(5, TimeUnit.SECONDS)).hasCauseInstanceOf(TapstateException.class);
            verifyNoInteractions(store);
        }
    }

    @Test void coalescedDistinctTokensKeepEachAdmittedCallersBytesReserved() throws Exception {
        var store = mock(SrsConsumerStore.class);
        when(store.advanceWriter(any(), anyLong(), anyString(), anyString(), anyString(), any(), any())).thenReturn(true);
        var bound = bound(store, () -> {});
        var threads = new CopyOnWriteArrayList<Thread>();
        try (var callers = Executors.newFixedThreadPool(3, task -> thread(task, threads));
             var writes = new CheckpointProgressWrites(8, 4096, 8, Duration.ofHours(1))) {
            var first = callers.submit(() -> writes.writer(bound, null, "run", "orders", "writer", progress(10, "a".repeat(200))));
            awaitRequests(writes, 1);
            var second = callers.submit(() -> writes.writer(bound, null, "run", "orders", "writer", progress(20, "b".repeat(200))));
            awaitRequests(writes, 2);
            var third = callers.submit(() -> writes.writer(bound, null, "run", "orders", "writer", progress(30, "c".repeat(200))));
            awaitWaiting(threads, 3);
            assertThat(writes.retainedRequests()).isEqualTo(2);
            assertThat(writes.retainedBytes()).isBetween(2900L, 4096L);
            writes.flush();
            first.get(5, TimeUnit.SECONDS);
            second.get(5, TimeUnit.SECONDS);
            awaitRequests(writes, 1);
            writes.flush();
            third.get(5, TimeUnit.SECONDS);
            var report = ArgumentCaptor.forClass(WriterProgress.class);
            verify(store, times(2)).advanceWriter(eq(RUN.context()), eq(4L), eq("run"), eq("orders"), eq("writer"), report.capture(), eq(FENCE));
            assertThat(report.getAllValues()).extracting(value -> value.lastTokened().token()).containsExactly("b".repeat(200), "c".repeat(200));
            assertThat(writes.retainedBytes()).isZero();
            assertThat(writes.retainedRequests()).isZero();
        }
    }

    @Test void aCommittedReceiptRetainsItsCallerChargeUntilPostCommitVerificationReturns() throws Exception {
        var store = mock(SrsConsumerStore.class);
        var committed = new AtomicBoolean();
        var callerThread = new java.util.concurrent.atomic.AtomicReference<Thread>();
        var verifying = new CountDownLatch(1);
        var release = new CountDownLatch(1);
        var returned = new CountDownLatch(1);
        when(store.advanceWriter(any(), anyLong(), anyString(), anyString(), anyString(), any(), any())).thenAnswer(call -> {
            assertThat(org.slf4j.MDC.get(PipelineLogAppender.PIPELINE_ID_MDC_KEY)).isEqualTo("pipeline");
            committed.set(true);
            return true;
        });
        var bound = bound(store, () -> {
            if (Thread.currentThread() == callerThread.get() && committed.get()) {
                verifying.countDown();
                try { assertThat(release.await(5, TimeUnit.SECONDS)).isTrue(); }
                catch (InterruptedException interrupted) {
                    Thread.currentThread().interrupt();
                    throw StorageRuntime.unavailable();
                }
            }
        });
        try (var callers = Executors.newSingleThreadExecutor();
             var writes = new CheckpointProgressWrites(1, 8192, 1, Duration.ofHours(1))) {
            var first = callers.submit(() -> {
                callerThread.set(Thread.currentThread());
                try { writes.writer(bound, null, "run", "orders", "writer", progress(10, "token")); }
                finally { returned.countDown(); }
            });
            try {
                awaitRequests(writes, 1);
                org.slf4j.MDC.put(PipelineLogAppender.PIPELINE_ID_MDC_KEY, "outer");
                writes.flush();
                assertThat(org.slf4j.MDC.get(PipelineLogAppender.PIPELINE_ID_MDC_KEY)).isEqualTo("outer");
                assertThat(verifying.await(5, TimeUnit.SECONDS)).isTrue();
                assertThat(writes.retainedRequests()).isEqualTo(1);
                assertThat(writes.retainedBytes()).isPositive();
                first.cancel(true);
                assertThat(returned.await(5, TimeUnit.SECONDS)).isTrue();
                awaitRequests(writes, 0);
                assertThat(writes.retainedBytes()).isZero();
            } finally {
                release.countDown();
                org.slf4j.MDC.remove(PipelineLogAppender.PIPELINE_ID_MDC_KEY);
            }
        }
    }

    @Test void anInterruptedCallerCannotReleaseItsReceiptBeforeTheWriterRetiresIt() throws Exception {
        var store = mock(SrsConsumerStore.class);
        when(store.advanceWriter(any(), anyLong(), anyString(), anyString(), anyString(), any(), any())).thenReturn(true);
        var returned = new CountDownLatch(1);
        try (var callers = Executors.newSingleThreadExecutor();
             var writes = new CheckpointProgressWrites(1, 8192, 1, Duration.ofHours(1))) {
            var first = callers.submit(() -> {
                try { writes.writer(bound(store, () -> {}), null, "run", "orders", "writer", progress(10, "token")); }
                finally { returned.countDown(); }
            });
            awaitRequests(writes, 1);
            first.cancel(true);
            assertThat(returned.await(5, TimeUnit.SECONDS)).isTrue();
            assertThat(writes.retainedRequests()).isEqualTo(1);
            assertThat(writes.retainedBytes()).isPositive();
            writes.flush();
            assertThat(writes.retainedRequests()).isZero();
            assertThat(writes.retainedBytes()).isZero();
        }
    }

    private static ScopedSrsConsumerAccess.Bound bound(SrsConsumerStore store, Runnable active) {
        return new ScopedSrsConsumerAccess.Bound(RUN, store, null, active, new SrsConsumerProgressReads(), store,
                null, new ExecutionFence(SCOPE, FENCE), new ThreadLocal<>());
    }

    private static WriterProgress progress(long sequence, String token) {
        var order = new SourceOrder(1, sequence);
        return new WriterProgress(order, token == null ? null : new ChainPosition(order, token));
    }

    private static Thread thread(Runnable task, List<Thread> threads) {
        var thread = new Thread(task);
        threads.add(thread);
        return thread;
    }

    private static void awaitRequests(CheckpointProgressWrites writes, int count) throws InterruptedException {
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(5);
        while (writes.retainedRequests() != count) {
            if (System.nanoTime() >= deadline) throw new AssertionError("progress reports did not reach the bounded entrance");
            Thread.sleep(1);
        }
    }

    private static void awaitWaiting(List<Thread> threads, int count) throws InterruptedException {
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(5);
        while (threads.stream().filter(thread -> thread.getState() == Thread.State.WAITING).count() < count) {
            if (System.nanoTime() >= deadline) throw new AssertionError("progress callers did not reach their bounded wait");
            Thread.sleep(1);
        }
    }
}
