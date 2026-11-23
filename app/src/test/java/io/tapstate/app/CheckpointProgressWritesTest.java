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

    @Test void offersReturnBeforePersistenceAndFoldBoundsWithoutLosingTheLastResumableToken() throws Exception {
        var store = successfulStore();
        var bound = bound(store, () -> {});
        try (var writes = manualWrites(8, 8192)) {
            var first = writes.offerWriter(bound, null, "run", "orders", "writer", progress(10, "token-10"));
            var second = writes.offerWriter(bound, null, "run", "orders", "writer", progress(20, null));
            assertThat(first).isNotDone();
            assertThat(second).isNotDone();
            verifyNoInteractions(store);
            writes.flush();
            assertThat(first.get(5, TimeUnit.SECONDS)).isTrue();
            assertThat(second.get(5, TimeUnit.SECONDS)).isTrue();
            var report = ArgumentCaptor.forClass(WriterProgress.class);
            verify(store).advanceWriter(eq(RUN.context()), eq(4L), eq("run"), eq("orders"), eq("writer"), report.capture(), eq(FENCE));
            assertThat(report.getValue().durableThrough()).isEqualTo(new SourceOrder(1, 20));
            assertThat(report.getValue().lastTokened()).isEqualTo(new ChainPosition(new SourceOrder(1, 10), "token-10"));
            assertThat(writes.retainedRequests()).isZero();
            assertThat(writes.retainedBytes()).isZero();
        }
    }

    @Test void batchesDistinctWritersOnlyWithinTheirOriginalRun() throws Exception {
        var store = mock(SrsConsumerStore.class);
        when(store.advanceWriters(any(), anyLong(), anyString(), anyList(), any())).thenReturn(List.of(
                new WriterProgressOutcome(true, null), new WriterProgressOutcome(true, null)));
        var bound = bound(store, () -> {});
        try (var writes = manualWrites(8, 8192)) {
            var first = writes.offerWriter(bound, null, "run", "orders", "one", progress(10, "token-10"));
            var second = writes.offerWriter(bound, null, "run", "orders", "two", progress(20, "token-20"));
            writes.flush();
            assertThat(first.get(5, TimeUnit.SECONDS)).isTrue();
            assertThat(second.get(5, TimeUnit.SECONDS)).isTrue();
            verify(store).advanceWriters(RUN.context(), 4, "run", List.of(
                    new WriterProgressUpdate("orders", "one", progress(10, "token-10")),
                    new WriterProgressUpdate("orders", "two", progress(20, "token-20"))), FENCE);
            verify(store, never()).advanceWriter(any(), anyLong(), anyString(), anyString(), anyString(), any(), any());
        }
    }

    @Test void eachRefusedReportKeepsItsOriginalOutcomeAndCause() throws Exception {
        var store = mock(SrsConsumerStore.class);
        var refused = new TapstateException(IoError.SRS_CONSUMER_UNAVAILABLE, java.util.Map.of("chain", "chain"), new IllegalStateException("fixture cause"));
        when(store.advanceWriters(any(), anyLong(), anyString(), anyList(), any())).thenReturn(List.of(
                new WriterProgressOutcome(true, null), new WriterProgressOutcome(false, refused), new WriterProgressOutcome(false, null)));
        var bound = bound(store, () -> {});
        try (var writes = manualWrites(8, 8192)) {
            var valid = writes.offerWriter(bound, null, "run", "orders", "valid", progress(10, "token-10"));
            var invalid = writes.offerWriter(bound, null, "run", "orders", "invalid", progress(20, "token-20"));
            var stale = writes.offerWriter(bound, null, "run", "orders", "stale", progress(30, null));
            writes.flush();
            assertThat(valid.get(5, TimeUnit.SECONDS)).isTrue();
            assertThatThrownBy(() -> invalid.get(5, TimeUnit.SECONDS)).hasCause(refused);
            assertThat(refused.getCause()).hasMessage("fixture cause");
            assertThat(stale.get(5, TimeUnit.SECONDS)).isFalse();
            verify(store, never()).advanceWriter(any(), anyLong(), anyString(), anyString(), anyString(), any(), any());
        }
    }

    @Test void directOffersKeepTheirOriginalPerReportOutcomes() throws Exception {
        var store = mock(DirectChannelStore.class);
        var run = new DirectSourceRun(new ExecutionFence(SCOPE, FENCE),
                new DirectChannelContext(SCOPE, "channel", "source", "capture"), 1, 0, Set.of("orders"),
                "capture-node", "capture-boot", 1, 1, 1);
        var bound = new ScopedDirectSinkAckFactory.Bound(run, store, () -> {});
        var refused = new TapstateException(IoError.DIRECT_CHANNEL_UNAVAILABLE, java.util.Map.of("channel", "channel"), null);
        when(store.advanceWriters(any(), anyLong(), anyString(), anyList(), any())).thenReturn(List.of(
                new WriterProgressOutcome(true, null), new WriterProgressOutcome(false, refused)));
        try (var writes = manualWrites(8, 8192)) {
            var first = writes.offerWriter(bound, FENCE, "run", "orders", "one", progress(10, "token-10"));
            var second = writes.offerWriter(bound, FENCE, "run", "orders", "two", progress(20, "token-20"));
            assertThat(first).isNotDone();
            verifyNoInteractions(store);
            writes.flush();
            assertThat(first.get(5, TimeUnit.SECONDS)).isTrue();
            assertThatThrownBy(() -> second.get(5, TimeUnit.SECONDS)).hasCause(refused);
            verify(store).advanceWriters(run.context(), 1, "run", List.of(
                    new WriterProgressUpdate("orders", "one", progress(10, "token-10")),
                    new WriterProgressUpdate("orders", "two", progress(20, "token-20"))), FENCE);
        }
    }

    @Test void anOutcomeUnknownBatchMustNotReplayIndividualWriterReports() {
        var store = mock(SrsConsumerStore.class);
        var unknown = new TapstateException(IoError.STORE_UNAVAILABLE, java.util.Map.of("detail", "fixture outcome unknown"), null);
        when(store.advanceWriters(any(), anyLong(), anyString(), anyList(), any())).thenThrow(unknown);
        var bound = bound(store, () -> {});
        try (var writes = manualWrites(8, 8192)) {
            var first = writes.offerWriter(bound, null, "run", "orders", "first", progress(10, "token-10"));
            var second = writes.offerWriter(bound, null, "run", "orders", "second", progress(20, "token-20"));
            writes.flush();
            assertThatThrownBy(() -> first.get(5, TimeUnit.SECONDS)).hasCause(unknown);
            assertThatThrownBy(() -> second.get(5, TimeUnit.SECONDS)).hasCause(unknown);
            verify(store, never()).advanceWriter(any(), anyLong(), anyString(), anyString(), anyString(), any(), any());
            assertThat(writes.retainedRequests()).isZero();
        }
    }

    @Test void distinctWriterRunsUseDistinctOriginalTransactions() throws Exception {
        var store = successfulStore();
        var bound = bound(store, () -> {});
        try (var writes = manualWrites(8, 8192)) {
            var first = writes.offerWriter(bound, null, "first", "orders", "writer", progress(10, null));
            var second = writes.offerWriter(bound, null, "second", "orders", "writer", progress(20, null));
            writes.flush();
            assertThat(first.get(5, TimeUnit.SECONDS)).isTrue();
            assertThat(second.get(5, TimeUnit.SECONDS)).isTrue();
            verify(store).advanceWriter(RUN.context(), 4, "first", "orders", "writer", progress(10, null), FENCE);
            verify(store).advanceWriter(RUN.context(), 4, "second", "orders", "writer", progress(20, null), FENCE);
            verify(store, never()).advanceWriters(any(), anyLong(), anyString(), anyList(), any());
        }
    }

    @Test void anIndependentPipelineDoesNotWaitForAStalledCommitAndTheSamePipelineDoesNotOverlap() throws Exception {
        var slowStore = mock(SrsConsumerStore.class);
        var fastStore = successfulStore();
        var entered = new CountDownLatch(1);
        var release = new CountDownLatch(1);
        var slow = bound(slowStore, () -> {});
        when(slowStore.advanceWriter(any(), anyLong(), anyString(), anyString(), anyString(), any(), any())).thenAnswer(call -> {
            entered.countDown();
            assertThat(release.await(5, TimeUnit.SECONDS)).isTrue();
            return true;
        });
        try (var flushing = Executors.newSingleThreadExecutor(); var writes = manualWrites(8, 8192)) {
            var first = writes.offerWriter(slow, null, "run", "orders", "writer", progress(10, null));
            var fast = writes.offerWriter(bound("shipments", fastStore), null, "run", "orders", "writer", progress(10, null));
            var flush = flushing.submit(writes::flush);
            try {
                assertThat(entered.await(5, TimeUnit.SECONDS)).isTrue();
                assertThat(fast.get(1, TimeUnit.SECONDS)).isTrue();
                assertThat(first).isNotDone();
                var later = writes.offerWriter(slow, null, "run", "orders", "writer", progress(20, null));
                writes.flush();
                assertThat(later).isNotDone();
                verify(slowStore, times(1)).advanceWriter(any(), anyLong(), anyString(), anyString(), anyString(), any(), any());
                release.countDown();
                flush.get(5, TimeUnit.SECONDS);
                writes.flush();
                assertThat(later.get(5, TimeUnit.SECONDS)).isTrue();
            } finally { release.countDown(); }
        }
    }

    @Test void inFlightCapacityRejectsAnOfferImmediatelyAndIsReleasedAfterCommit() throws Exception {
        var store = mock(SrsConsumerStore.class);
        var entered = new CountDownLatch(1);
        var release = new CountDownLatch(1);
        when(store.advanceWriter(any(), anyLong(), anyString(), anyString(), anyString(), any(), any())).thenAnswer(call -> {
            entered.countDown();
            assertThat(release.await(5, TimeUnit.SECONDS)).isTrue();
            return true;
        });
        var bound = bound(store, () -> {});
        try (var flushing = Executors.newSingleThreadExecutor(); var writes = manualWrites(1, 8192)) {
            var first = writes.offerWriter(bound, null, "run", "orders", "one", progress(10, null));
            var commit = flushing.submit(writes::flush);
            try {
                assertThat(entered.await(5, TimeUnit.SECONDS)).isTrue();
                assertThatThrownBy(() -> writes.offerWriter(bound, null, "run", "orders", "two", progress(20, null)))
                        .isInstanceOfSatisfying(TapstateException.class,
                                failure -> assertThat(failure.code()).isEqualTo(StorageRuntime.unavailable().code()));
                assertThat(first).isNotDone();
                release.countDown();
                commit.get(5, TimeUnit.SECONDS);
                var later = writes.offerWriter(bound, null, "run", "orders", "two", progress(20, null));
                writes.flush();
                assertThat(later.get(5, TimeUnit.SECONDS)).isTrue();
            } finally { release.countDown(); }
        }
    }

    @Test void aFifthHealthyPipelineCanCommitWhileFourIndependentCommitsAreHeld() throws Exception {
        var blockedStore = mock(SrsConsumerStore.class);
        var healthyStore = successfulStore();
        var entered = new CountDownLatch(4);
        var release = new CountDownLatch(1);
        when(blockedStore.advanceWriter(any(), anyLong(), anyString(), anyString(), anyString(), any(), any())).thenAnswer(call -> {
            entered.countDown();
            assertThat(release.await(5, TimeUnit.SECONDS)).isTrue();
            return true;
        });
        try (var flushing = Executors.newSingleThreadExecutor(); var writes = manualWrites(16, 16384)) {
            for (int pipeline = 0; pipeline < 4; pipeline++)
                writes.offerWriter(bound("blocked-" + pipeline, blockedStore), null, "run", "orders", "writer", progress(10, null));
            var firstFlush = flushing.submit(writes::flush);
            try {
                assertThat(entered.await(5, TimeUnit.SECONDS)).isTrue();
                var healthy = writes.offerWriter(bound("healthy-fifth", healthyStore), null, "run", "orders", "writer", progress(10, null));
                writes.flush();
                assertThat(healthy.get(1, TimeUnit.SECONDS)).isTrue();
            } finally { release.countDown(); }
            firstFlush.get(5, TimeUnit.SECONDS);
            assertThat(writes.retainedRequests()).isZero();
        }
    }

    @Test void rejectsAnOversizedOfferWithoutTouchingTheStore() {
        var store = mock(SrsConsumerStore.class);
        try (var writes = manualWrites(8, 1024)) {
            assertThatThrownBy(() -> writes.offerWriter(bound(store, () -> {}), null, "run", "orders", "writer", progress(1, "x".repeat(1000))))
                    .isInstanceOf(TapstateException.class);
            verifyNoInteractions(store);
            assertThat(writes.retainedRequests()).isZero();
        }
    }

    @Test void failedPersistenceKeepsItsExactCauseAndReleasesCapacityForTheNextOffer() throws Exception {
        var store = mock(SrsConsumerStore.class);
        var expected = new TapstateException(IoError.STORE_UNAVAILABLE, java.util.Map.of("detail", "test conflict"), null);
        when(store.advanceWriter(any(), anyLong(), anyString(), anyString(), anyString(), any(), any())).thenThrow(expected).thenReturn(true);
        var bound = bound(store, () -> {});
        try (var writes = manualWrites(1, 8192)) {
            var first = writes.offerWriter(bound, null, "run", "orders", "writer", progress(10, null));
            writes.flush();
            assertThatThrownBy(() -> first.get(5, TimeUnit.SECONDS)).hasCause(expected);
            var second = writes.offerWriter(bound, null, "run", "orders", "writer", progress(20, null));
            writes.flush();
            assertThat(second.get(5, TimeUnit.SECONDS)).isTrue();
        }
    }

    @Test void queuedOffersRecheckTheirOriginalGuardOnTheWorker() {
        var active = new AtomicBoolean(true);
        var store = mock(SrsConsumerStore.class);
        var bound = bound(store, () -> { if (!active.get()) throw ScopedSrsConsumerAccess.refused(RUN); });
        try (var writes = manualWrites(8, 8192)) {
            var first = writes.offerWriter(bound, null, "run", "orders", "writer", progress(10, null));
            active.set(false);
            writes.flush();
            assertThatThrownBy(() -> first.get(5, TimeUnit.SECONDS)).hasCauseInstanceOf(TapstateException.class);
            verifyNoInteractions(store);
        }
    }

    @Test void coalescingReplacesTheOldPayloadAndReservesEachBoundedReceipt() throws Exception {
        var store = successfulStore();
        var bound = bound(store, () -> {});
        try (var writes = manualWrites(8, 4096)) {
            var first = writes.offerWriter(bound, null, "run", "orders", "writer", progress(10, "a".repeat(500)));
            var second = writes.offerWriter(bound, null, "run", "orders", "writer", progress(20, "b".repeat(500)));
            assertThat(writes.retainedRequests()).isEqualTo(2);
            assertThat(writes.retainedBytes()).isLessThan(4096L);
            assertThatThrownBy(() -> writes.offerWriter(bound, null, "run", "orders", "writer", progress(30, "x".repeat(2000))))
                    .isInstanceOf(TapstateException.class);
            writes.flush();
            assertThat(first.get(5, TimeUnit.SECONDS)).isTrue();
            assertThat(second.get(5, TimeUnit.SECONDS)).isTrue();
            var report = ArgumentCaptor.forClass(WriterProgress.class);
            verify(store).advanceWriter(eq(RUN.context()), eq(4L), eq("run"), eq("orders"), eq("writer"), report.capture(), eq(FENCE));
            assertThat(report.getValue().lastTokened().token()).isEqualTo("b".repeat(500));
            assertThat(writes.retainedBytes()).isZero();
        }
    }

    @Test void theWorkerKeepsAdmissionUntilPostCommitVerificationFinishes() throws Exception {
        var store = mock(SrsConsumerStore.class);
        var persisted = new AtomicBoolean();
        var verifying = new CountDownLatch(1);
        var release = new CountDownLatch(1);
        when(store.advanceWriter(any(), anyLong(), anyString(), anyString(), anyString(), any(), any())).thenAnswer(call -> {
            assertThat(org.slf4j.MDC.get(PipelineLogAppender.PIPELINE_ID_MDC_KEY)).isEqualTo("pipeline");
            persisted.set(true);
            return true;
        });
        var bound = bound(store, () -> {
            if (persisted.get()) {
                verifying.countDown();
                try { assertThat(release.await(5, TimeUnit.SECONDS)).isTrue(); }
                catch (InterruptedException interrupted) { Thread.currentThread().interrupt(); throw StorageRuntime.unavailable(); }
            }
        });
        try (var flushing = Executors.newSingleThreadExecutor(); var writes = manualWrites(1, 8192)) {
            var first = writes.offerWriter(bound, null, "run", "orders", "writer", progress(10, null));
            var flush = flushing.submit(writes::flush);
            try {
                assertThat(verifying.await(5, TimeUnit.SECONDS)).isTrue();
                assertThat(first).isNotDone();
                assertThat(writes.retainedRequests()).isEqualTo(1);
                assertThatThrownBy(() -> writes.offerWriter(bound, null, "run", "orders", "writer", progress(20, null)))
                        .isInstanceOf(TapstateException.class);
                release.countDown();
                flush.get(5, TimeUnit.SECONDS);
                assertThat(first.get(5, TimeUnit.SECONDS)).isTrue();
                assertThat(writes.retainedRequests()).isZero();
            } finally { release.countDown(); }
        }
    }

    @Test void explicitSynchronousWriterBoundariesStillWaitForTheOriginalRefusal() throws Exception {
        var store = mock(SrsConsumerStore.class);
        var entered = new CountDownLatch(1);
        var release = new CountDownLatch(1);
        when(store.advanceWriter(any(), anyLong(), anyString(), anyString(), anyString(), any(), any())).thenAnswer(call -> {
            entered.countDown();
            assertThat(release.await(5, TimeUnit.SECONDS)).isTrue();
            return false;
        });
        try (var callers = Executors.newSingleThreadExecutor(); var writes = new CheckpointProgressWrites()) {
            var caller = callers.submit(() -> writes.writer(bound(store, () -> {}), null, "run", "orders", "writer", progress(10, null)));
            try {
                assertThat(entered.await(5, TimeUnit.SECONDS)).isTrue();
                assertThat(caller).isNotDone();
                release.countDown();
                assertThatThrownBy(() -> caller.get(5, TimeUnit.SECONDS))
                        .hasCauseInstanceOf(TapstateException.class)
                        .satisfies(failure -> assertThat(((TapstateException) failure.getCause()).code())
                                .isEqualTo(ScopedSrsConsumerAccess.refused(RUN).code()));
            } finally { release.countDown(); }
        }
    }

    @Test void anOutstandingSnapshotBoundaryAlsoHonorsTheCloseDeadline() {
        var store = successfulStore();
        try (var writes = manualWrites(1, 8192)) {
            var bound = bound(store, () -> {});
            var snapshot = new WriterProgress(SourceOrder.snapshotRow(1), null);
            var shutdown = new ShutdownAwait();
            assertThatThrownBy(() -> shutdown.complete(System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(250),
                    () -> CheckpointWriteAcks.close(() -> writes.writer(bound, null, "run", "orders", "writer", snapshot))))
                    .isInstanceOfSatisfying(TapstateException.class,
                            failure -> assertThat(failure.code()).isSameAs(StorageRuntime.unavailable().code()));
            assertThat(writes.retainedRequests()).isOne();
            writes.flush();
            verify(store).advanceWriter(RUN.context(), 4, "run", "orders", "writer", snapshot, FENCE);
            assertThat(writes.retainedRequests()).isZero();
        }
    }

    @Test void cancellingAnOfferedReceiptDoesNotDiscardItsAlreadyAdmittedProgress() {
        var store = successfulStore();
        try (var writes = manualWrites(1, 8192)) {
            var first = writes.offerWriter(bound(store, () -> {}), null, "run", "orders", "writer", progress(10, null));
            first.cancel(true);
            assertThatThrownBy(() -> writes.offerWriter(bound(store, () -> {}), null, "run", "orders", "other", progress(20, null)))
                    .isInstanceOf(TapstateException.class);
            writes.flush();
            verify(store).advanceWriter(RUN.context(), 4, "run", "orders", "writer", progress(10, null), FENCE);
            assertThat(writes.retainedRequests()).isZero();
        }
    }

    @Test void closingRejectsPendingOffersWithTheExistingTypedUnavailableRefusal() {
        var writes = manualWrites(1, 8192);
        var first = writes.offerWriter(bound(mock(SrsConsumerStore.class), () -> {}), null, "run", "orders", "writer", progress(10, null));
        writes.close();
        assertThatThrownBy(() -> first.get(5, TimeUnit.SECONDS)).hasCauseInstanceOf(TapstateException.class);
        assertThat(writes.retainedRequests()).isZero();
        assertThatThrownBy(() -> writes.offerWriter(bound(mock(SrsConsumerStore.class), () -> {}), null, "run", "orders", "writer", progress(20, null)))
                .isInstanceOf(TapstateException.class);
    }

    private static CheckpointProgressWrites manualWrites(int entries, long bytes) {
        return new CheckpointProgressWrites(entries, bytes, entries, Duration.ofHours(1));
    }

    private static SrsConsumerStore successfulStore() {
        var store = mock(SrsConsumerStore.class);
        when(store.advanceWriter(any(), anyLong(), anyString(), anyString(), anyString(), any(), any())).thenReturn(true);
        return store;
    }

    private static ScopedSrsConsumerAccess.Bound bound(SrsConsumerStore store, Runnable active) {
        return new ScopedSrsConsumerAccess.Bound(RUN, store, null, active, new SrsConsumerProgressReads(), store,
                null, new ExecutionFence(SCOPE, FENCE), new ThreadLocal<>());
    }

    private static ScopedSrsConsumerAccess.Bound bound(String pipeline, SrsConsumerStore store) {
        var scope = new PipelineIntentScope("cluster", 3, "cache", pipeline, 7);
        var fence = new WorkloadClaimFence(new WorkloadClaimKey("cluster", WorkloadClaimType.PIPELINE_ACTUATION, pipeline), FENCE.owner(), 2, 5, 9);
        var run = SrsConsumerRun.of(scope, new SrsConsumerMembership(
                new SrsConsumerContext(new SrsConsumerKey("chain", pipeline, "source"), 7), 4, false, Set.of("orders"), fence));
        return new ScopedSrsConsumerAccess.Bound(run, store, null, () -> {}, new SrsConsumerProgressReads(), store,
                null, new ExecutionFence(scope, fence), new ThreadLocal<>());
    }

    private static WriterProgress progress(long sequence, String token) {
        var order = new SourceOrder(1, sequence);
        return new WriterProgress(order, token == null ? null : new ChainPosition(order, token));
    }
}
