package io.tapstate.cli;

import io.tapstate.core.common.Severity;
import io.tapstate.core.common.TapstateErrorCode;
import org.junit.jupiter.api.Test;

import java.util.ArrayDeque;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.Set;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatIllegalArgumentException;
import static org.assertj.core.api.Assertions.assertThatIllegalStateException;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class RefreshCoordinatorTest {

    @Test
    void publishesImmutableTypedOutcomesWithGenerationAndSequence() throws Exception {
        BlockingQueue<RefreshResult> published = new LinkedBlockingQueue<>();
        Map<String, String> mutableArguments = new LinkedHashMap<>();
        mutableArguments.put("resource", "source-1");

        try (RefreshCoordinator coordinator = new RefreshCoordinator(published::add)) {
            RefreshRequest success = coordinator.refresh((generation, sequence, token) ->
                    RefreshResult.success(new WorkbenchSnapshot(generation, sequence)));
            RefreshResult successResult = awaitResult(published);

            RefreshRequest empty = coordinator.refresh((generation, sequence, token) -> RefreshResult.empty());
            RefreshResult emptyResult = awaitResult(published);

            RefreshRequest offline = coordinator.refresh((generation, sequence, token) -> RefreshResult.offline());
            RefreshResult offlineResult = awaitResult(published);

            RefreshRequest diagnostic = coordinator.refresh((generation, sequence, token) ->
                    RefreshResult.diagnostic(TestError.READ_FAILED, mutableArguments));
            RefreshResult diagnosticResult = awaitResult(published);
            mutableArguments.put("resource", "changed");

            assertThat(List.of(successResult, emptyResult, offlineResult, diagnosticResult)).containsExactly(
                    new RefreshResult(0, success.requestSequence(),
                            new RefreshResult.Success(new WorkbenchSnapshot(0, success.requestSequence()))),
                    new RefreshResult(0, empty.requestSequence(), new RefreshResult.Empty()),
                    new RefreshResult(0, offline.requestSequence(), new RefreshResult.Offline()),
                    new RefreshResult(0, diagnostic.requestSequence(),
                            new RefreshResult.Diagnostic(TestError.READ_FAILED, Map.of("resource", "source-1"))));
            RefreshResult.Diagnostic failure = (RefreshResult.Diagnostic) diagnosticResult.outcome();
            assertThatThrownBy(() -> failure.arguments().put("resource", "changed"))
                    .isInstanceOf(UnsupportedOperationException.class);
            assertThatIllegalArgumentException().isThrownBy(() -> new RefreshResult(
                    0,
                    success.requestSequence(),
                    new RefreshResult.Success(new WorkbenchSnapshot(0, success.requestSequence() + 1))));
        }
    }

    @Test
    void replacementSuppressesCanceledCompletionAndKeepsOneWorker() throws Exception {
        BlockingQueue<RefreshResult> published = new LinkedBlockingQueue<>();
        CountDownLatch firstStarted = new CountDownLatch(1);
        CountDownLatch releaseFirst = new CountDownLatch(1);
        CountDownLatch latestStarted = new CountDownLatch(1);
        AtomicInteger activeWorkers = new AtomicInteger();
        AtomicInteger peakWorkers = new AtomicInteger();
        AtomicBoolean secondRan = new AtomicBoolean();

        try (RefreshCoordinator coordinator = new RefreshCoordinator(published::add)) {
            RefreshRequest first = coordinator.refresh((generation, sequence, token) -> {
                int active = activeWorkers.incrementAndGet();
                peakWorkers.accumulateAndGet(active, Math::max);
                firstStarted.countDown();
                awaitIgnoringInterrupt(releaseFirst);
                activeWorkers.decrementAndGet();
                return RefreshResult.success(new WorkbenchSnapshot(generation, sequence));
            });
            await(firstStarted);

            RefreshRequest second = coordinator.refresh((generation, sequence, token) -> {
                secondRan.set(true);
                return RefreshResult.success(new WorkbenchSnapshot(generation, sequence));
            });
            RefreshRequest latest = coordinator.refresh((generation, sequence, token) -> {
                int active = activeWorkers.incrementAndGet();
                peakWorkers.accumulateAndGet(active, Math::max);
                latestStarted.countDown();
                activeWorkers.decrementAndGet();
                return RefreshResult.success(new WorkbenchSnapshot(generation, sequence));
            });

            assertThat(first.cancellationToken().isCancelled()).isTrue();
            assertThat(second.cancellationToken().isCancelled()).isTrue();
            assertThat(latest.cancellationToken().isCancelled()).isFalse();
            assertThat(latest.requestSequence()).isEqualTo(second.requestSequence() + 1);
            assertThat(latestStarted.getCount()).isOne();

            releaseFirst.countDown();
            await(latestStarted);
            RefreshResult publishedResult = awaitResult(published);

            assertThat(secondRan).isFalse();
            assertThat(peakWorkers).hasValue(1);
            assertThat(publishedResult).isEqualTo(new RefreshResult(
                    0,
                    latest.requestSequence(),
                    new RefreshResult.Success(new WorkbenchSnapshot(0, latest.requestSequence()))));
            assertThat(published).isEmpty();
        }
    }

    @Test
    void canceledInterruptedWorkIsSuppressed() throws Exception {
        BlockingQueue<RefreshResult> published = new LinkedBlockingQueue<>();
        CountDownLatch firstStarted = new CountDownLatch(1);
        CountDownLatch neverReleased = new CountDownLatch(1);

        try (RefreshCoordinator coordinator = new RefreshCoordinator(published::add)) {
            RefreshRequest canceled = coordinator.refresh((generation, sequence, token) -> {
                firstStarted.countDown();
                neverReleased.await();
                return RefreshResult.empty();
            });
            await(firstStarted);

            RefreshRequest current = coordinator.refresh((generation, sequence, token) ->
                    RefreshResult.success(new WorkbenchSnapshot(generation, sequence)));
            RefreshResult publishedResult = awaitResult(published);

            assertThat(canceled.cancellationToken().isCancelled()).isTrue();
            assertThat(publishedResult.requestSequence()).isEqualTo(current.requestSequence());
            assertThat(published).isEmpty();
        }
    }

    @Test
    void contextChangeSuppressesCompletionThatFinishesAfterReplacementIsQueued() throws Exception {
        BlockingQueue<RefreshResult> published = new LinkedBlockingQueue<>();
        CountDownLatch oldStarted = new CountDownLatch(1);
        CountDownLatch releaseOld = new CountDownLatch(1);

        try (RefreshCoordinator coordinator = new RefreshCoordinator(published::add)) {
            RefreshRequest oldRequest = coordinator.refresh((generation, sequence, token) -> {
                oldStarted.countDown();
                awaitIgnoringInterrupt(releaseOld);
                return RefreshResult.success(new WorkbenchSnapshot(generation, sequence));
            });
            await(oldStarted);

            long currentGeneration = coordinator.advanceContext();
            RefreshRequest currentRequest = coordinator.refresh((generation, sequence, token) ->
                    RefreshResult.success(new WorkbenchSnapshot(generation, sequence)));
            releaseOld.countDown();

            RefreshResult currentResult = awaitResult(published);
            assertThat(oldRequest.cancellationToken().isCancelled()).isTrue();
            assertThat(currentResult.isCurrent(currentGeneration, currentRequest.requestSequence())).isTrue();
            assertThat(currentResult.outcome()).isEqualTo(new RefreshResult.Success(new WorkbenchSnapshot(
                    currentGeneration, currentRequest.requestSequence())));
            assertThat(published).isEmpty();
        }
    }

    @Test
    void queuedOldGenerationIsRejectedBeforeLatestOnlyBurstMutatesState() throws Exception {
        RecordingScheduler scheduler = new RecordingScheduler();
        AtomicReference<WorkbenchState> state = new AtomicReference<>(WorkbenchState.initial());
        WorkbenchReducer<WorkbenchState> reducer = WorkbenchReducer.workbench();
        BlockingQueue<Long> sinkSequences = new LinkedBlockingQueue<>();
        LatestOnlyMailbox<WorkbenchEvent.SnapshotPublished> mailbox = new LatestOnlyMailbox<>(scheduler, event ->
                state.updateAndGet(current -> reducer.reduce(current, event).state()));
        CountDownLatch firstBStarted = new CountDownLatch(1);
        CountDownLatch releaseFirstB = new CountDownLatch(1);

        try (RefreshCoordinator coordinator = new RefreshCoordinator(result -> {
            RefreshResult.Success success = (RefreshResult.Success) result.outcome();
            mailbox.publish(new WorkbenchEvent.SnapshotPublished(success.snapshot()));
            sinkSequences.add(result.requestSequence());
        })) {
            RefreshRequest oldRequest = coordinator.refresh((generation, sequence, token) ->
                    RefreshResult.success(new WorkbenchSnapshot(generation, sequence)));
            assertThat(awaitValue(sinkSequences)).isEqualTo(oldRequest.requestSequence());
            assertThat(scheduler.pendingCount()).isEqualTo(1);

            coordinator.advanceContext();
            RefreshRequest firstB = coordinator.refresh((generation, sequence, token) -> {
                firstBStarted.countDown();
                releaseFirstB.await();
                return RefreshResult.success(new WorkbenchSnapshot(generation, sequence));
            });
            expect(state, reducer, firstB);
            WorkbenchState generationBState = state.get();
            await(firstBStarted);

            scheduler.runNext();
            assertThat(state.get()).isSameAs(generationBState);
            assertThat(state.get().snapshot()).isEmpty();

            releaseFirstB.countDown();
            assertThat(awaitValue(sinkSequences)).isEqualTo(firstB.requestSequence());
            RefreshRequest secondB = coordinator.refresh((generation, sequence, token) ->
                    RefreshResult.success(new WorkbenchSnapshot(generation, sequence)));
            expect(state, reducer, secondB);
            assertThat(awaitValue(sinkSequences)).isEqualTo(secondB.requestSequence());
            RefreshRequest latestB = coordinator.refresh((generation, sequence, token) ->
                    RefreshResult.success(new WorkbenchSnapshot(generation, sequence)));
            expect(state, reducer, latestB);
            assertThat(awaitValue(sinkSequences)).isEqualTo(latestB.requestSequence());

            assertThat(scheduler.pendingCount()).isEqualTo(1);
            assertThat(scheduler.peakPendingCount()).isEqualTo(1);
            scheduler.runNext();

            WorkbenchSnapshot latestSnapshot =
                    new WorkbenchSnapshot(latestB.contextGeneration(), latestB.requestSequence());
            assertThat(state.get().expectedSnapshot()).contains(latestSnapshot);
            assertThat(state.get().snapshot()).contains(latestSnapshot);
            assertThat(scheduler.pendingCount()).isZero();
        }
    }

    @Test
    void activeWorkFailurePublishesCodedDiagnosticWithoutDetails() throws Exception {
        BlockingQueue<RefreshResult> published = new LinkedBlockingQueue<>();

        try (RefreshCoordinator coordinator = new RefreshCoordinator(published::add)) {
            RefreshRequest request = coordinator.refresh((generation, sequence, token) -> {
                throw new IllegalStateException("secret failure detail");
            });

            assertThat(awaitResult(published)).isEqualTo(new RefreshResult(
                    request.contextGeneration(),
                    request.requestSequence(),
                    new RefreshResult.Diagnostic(CliError.WORKBENCH_UNAVAILABLE, Map.of())));
            assertThat(coordinator.workerFailure()).isEmpty();
        }
    }

    @Test
    void sinkRunsOutsideLockAndFailureReachesObserverAfterCleanup() throws Exception {
        IllegalStateException sinkFailure = new IllegalStateException("sink failed");
        BlockingQueue<Throwable> observedFailures = new LinkedBlockingQueue<>();
        CountDownLatch sinkEntered = new CountDownLatch(1);
        CountDownLatch releaseSink = new CountDownLatch(1);
        CountDownLatch contextAdvanced = new CountDownLatch(1);
        AtomicReference<Throwable> contextFailure = new AtomicReference<>();

        try (RefreshCoordinator coordinator = new RefreshCoordinator(result -> {
            sinkEntered.countDown();
            awaitIgnoringInterrupt(releaseSink);
            throw sinkFailure;
        }, observedFailures::add)) {
            coordinator.refresh((generation, sequence, token) -> RefreshResult.empty());
            await(sinkEntered);

            Thread contextThread = new Thread(() -> {
                try {
                    coordinator.advanceContext();
                } catch (Throwable failure) {
                    contextFailure.set(failure);
                } finally {
                    contextAdvanced.countDown();
                }
            }, "test-context-advance");
            contextThread.start();
            await(contextAdvanced);
            assertThat(contextFailure).hasNullValue();

            releaseSink.countDown();
            contextThread.join(TimeUnit.SECONDS.toMillis(2));
            assertThat(contextThread.isAlive()).isFalse();
            assertThat(awaitValue(observedFailures)).isSameAs(sinkFailure);
            assertThat(coordinator.workerFailure()).containsSame(sinkFailure);
        }
    }

    @Test
    void closeWaitsForSinkThatAlreadyPassedThePublishGate() throws Exception {
        CountDownLatch sinkEntered = new CountDownLatch(1);
        CountDownLatch releaseSink = new CountDownLatch(1);
        CountDownLatch closeReturned = new CountDownLatch(1);
        RefreshCoordinator coordinator = new RefreshCoordinator(result -> {
            sinkEntered.countDown();
            awaitIgnoringInterrupt(releaseSink);
        });
        coordinator.refresh((generation, sequence, token) -> RefreshResult.empty());
        await(sinkEntered);

        Thread closeThread = new Thread(() -> {
            coordinator.close();
            closeReturned.countDown();
        }, "test-close-during-sink");
        closeThread.start();

        assertThatIllegalStateException()
                .isThrownBy(() -> coordinator.refresh(
                        (generation, sequence, token) -> RefreshResult.empty()))
                .withMessage("Refresh coordinator is closed");
        assertThat(closeReturned).as("close remains a barrier while the sink is running")
                .matches(latch -> latch.getCount() == 1);

        releaseSink.countDown();
        await(closeReturned);
        closeThread.join(TimeUnit.SECONDS.toMillis(2));
        assertThat(closeThread.isAlive()).isFalse();
    }

    @Test
    void closeWaitsForFailureObserverThatAlreadyStarted() throws Exception {
        IllegalStateException sinkFailure = new IllegalStateException("sink failed");
        CountDownLatch observerEntered = new CountDownLatch(1);
        CountDownLatch releaseObserver = new CountDownLatch(1);
        CountDownLatch closeReturned = new CountDownLatch(1);
        RefreshCoordinator coordinator = new RefreshCoordinator(result -> {
            throw sinkFailure;
        }, failure -> {
            observerEntered.countDown();
            awaitIgnoringInterrupt(releaseObserver);
        });
        coordinator.refresh((generation, sequence, token) -> RefreshResult.empty());
        await(observerEntered);

        Thread closeThread = new Thread(() -> {
            coordinator.close();
            closeReturned.countDown();
        }, "test-close-during-failure-observer");
        closeThread.start();

        assertThatIllegalStateException()
                .isThrownBy(() -> coordinator.refresh(
                        (generation, sequence, token) -> RefreshResult.empty()))
                .withMessage("Refresh coordinator is closed");
        assertThat(closeReturned).as("close remains a barrier while the observer is running")
                .matches(latch -> latch.getCount() == 1);

        releaseObserver.countDown();
        await(closeReturned);
        closeThread.join(TimeUnit.SECONDS.toMillis(2));
        assertThat(closeThread.isAlive()).isFalse();
    }

    @Test
    void closePreventsFailureObserverFromStartingAfterTheBarrier() throws Exception {
        BlockingQueue<Throwable> observedFailures = new LinkedBlockingQueue<>();
        CountDownLatch workStarted = new CountDownLatch(1);
        CountDownLatch releaseWork = new CountDownLatch(1);
        AtomicReference<Thread> workerThread = new AtomicReference<>();
        AssertionError workerFailure = new AssertionError("worker failed after close");
        RefreshCoordinator coordinator = new RefreshCoordinator(result -> {
        }, observedFailures::add);
        coordinator.refresh((generation, sequence, token) -> {
            workerThread.set(Thread.currentThread());
            workStarted.countDown();
            awaitIgnoringInterrupt(releaseWork);
            throw workerFailure;
        });
        await(workStarted);

        coordinator.close();
        releaseWork.countDown();
        workerThread.get().join(TimeUnit.SECONDS.toMillis(2));

        assertThat(workerThread.get().isAlive()).isFalse();
        assertThat(observedFailures).isEmpty();
        assertThat(coordinator.workerFailure()).containsSame(workerFailure);
    }

    @Test
    void closeIsIdempotentCancelsWorkAndRejectsNewRequests() throws Exception {
        BlockingQueue<RefreshResult> published = new LinkedBlockingQueue<>();
        CountDownLatch started = new CountDownLatch(1);
        CountDownLatch release = new CountDownLatch(1);
        CountDownLatch finished = new CountDownLatch(1);
        AtomicReference<Thread> workerThread = new AtomicReference<>();
        RefreshCoordinator coordinator = new RefreshCoordinator(published::add);
        RefreshRequest request = coordinator.refresh((generation, sequence, token) -> {
            workerThread.set(Thread.currentThread());
            started.countDown();
            awaitIgnoringInterrupt(release);
            finished.countDown();
            return RefreshResult.success(new WorkbenchSnapshot(generation, sequence));
        });
        await(started);

        coordinator.close();
        coordinator.close();

        assertThat(request.cancellationToken().isCancelled()).isTrue();
        assertThatIllegalStateException()
                .isThrownBy(() -> coordinator.refresh(
                        (generation, sequence, token) -> RefreshResult.empty()))
                .withMessage("Refresh coordinator is closed");
        release.countDown();
        await(finished);
        workerThread.get().join(TimeUnit.SECONDS.toMillis(2));
        assertThat(workerThread.get().isAlive()).isFalse();
        assertThat(published).isEmpty();
    }

    private static RefreshResult awaitResult(BlockingQueue<RefreshResult> results) throws InterruptedException {
        return awaitValue(results);
    }

    private static <T> T awaitValue(BlockingQueue<T> values) throws InterruptedException {
        T value = values.poll(2, TimeUnit.SECONDS);
        assertThat(value).as("a value published before the bounded wait elapsed").isNotNull();
        return value;
    }

    private static void await(CountDownLatch latch) throws InterruptedException {
        assertThat(latch.await(2, TimeUnit.SECONDS))
                .as("the coordinated step completed before the bounded wait elapsed")
                .isTrue();
    }

    private static void expect(
            AtomicReference<WorkbenchState> state,
            WorkbenchReducer<WorkbenchState> reducer,
            RefreshRequest request) {
        WorkbenchSnapshot expected =
                new WorkbenchSnapshot(request.contextGeneration(), request.requestSequence());
        state.updateAndGet(current -> reducer.reduce(
                current, new WorkbenchEvent.SnapshotExpected(expected)).state());
    }

    private static void awaitIgnoringInterrupt(CountDownLatch latch) {
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(2);
        boolean interrupted = false;
        boolean completed = false;
        while (!completed && System.nanoTime() < deadline) {
            try {
                completed = latch.await(deadline - System.nanoTime(), TimeUnit.NANOSECONDS);
            } catch (InterruptedException cancelled) {
                interrupted = true;
            }
        }
        if (interrupted) {
            Thread.currentThread().interrupt();
        }
        if (!completed) {
            throw new AssertionError("Timed out waiting for the coordinated test release");
        }
    }

    private enum TestError implements TapstateErrorCode {
        READ_FAILED;

        @Override
        public String code() {
            return "test.read-failed";
        }

        @Override
        public Severity severity() {
            return Severity.ERROR;
        }

        @Override
        public Set<String> placeholders() {
            return Set.of("resource");
        }
    }

    private static final class RecordingScheduler implements LatestOnlyMailbox.Scheduler {
        private final Queue<Runnable> callbacks = new ArrayDeque<>();
        private int peakPendingCount;

        @Override
        public synchronized void runLater(Runnable callback) {
            callbacks.add(callback);
            peakPendingCount = Math.max(peakPendingCount, callbacks.size());
        }

        private void runNext() {
            Runnable callback;
            synchronized (this) {
                callback = callbacks.remove();
            }
            callback.run();
        }

        private synchronized int pendingCount() {
            return callbacks.size();
        }

        private synchronized int peakPendingCount() {
            return peakPendingCount;
        }
    }
}
