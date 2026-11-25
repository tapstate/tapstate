package io.tapstate.cli;

import org.junit.jupiter.api.Test;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.List;
import java.util.Queue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

import static org.assertj.core.api.Assertions.assertThat;

class LatestOnlyMailboxTest {

    @Test
    void workerPublicationWaitsForTheOwnerThreadDrain() {
        RecordingScheduler scheduler = new RecordingScheduler();
        List<WorkbenchEvent.SnapshotPublished> delivered = new ArrayList<>();
        LatestOnlyMailbox<WorkbenchEvent.SnapshotPublished> mailbox =
                new LatestOnlyMailbox<>(scheduler, delivered::add);

        mailbox.publish(published(1));

        assertThat(delivered).isEmpty();
        assertThat(scheduler.scheduledCount()).isEqualTo(1);

        scheduler.runNext();

        assertThat(delivered).containsExactly(published(1));
    }

    @Test
    void burstPublicationKeepsOnlyTheLatestEventAndOneScheduledDrain() {
        RecordingScheduler scheduler = new RecordingScheduler();
        List<WorkbenchEvent.SnapshotPublished> delivered = new ArrayList<>();
        LatestOnlyMailbox<WorkbenchEvent.SnapshotPublished> mailbox =
                new LatestOnlyMailbox<>(scheduler, delivered::add);

        mailbox.publish(published(1));
        mailbox.publish(published(2));
        mailbox.publish(published(3));

        assertThat(scheduler.scheduledCount()).isEqualTo(1);
        assertThat(scheduler.pendingCount()).isEqualTo(1);

        scheduler.runNext();

        assertThat(delivered).containsExactly(published(3));
        assertThat(scheduler.pendingCount()).isZero();
    }

    @Test
    void publicationDuringDrainKeepsTheNewestEventBehindOneSuccessorCallback() throws Exception {
        RecordingScheduler scheduler = new RecordingScheduler();
        List<WorkbenchEvent.SnapshotPublished> delivered = new ArrayList<>();
        CountDownLatch firstDeliveryStarted = new CountDownLatch(1);
        CountDownLatch continueFirstDelivery = new CountDownLatch(1);
        LatestOnlyMailbox<WorkbenchEvent.SnapshotPublished> mailbox = new LatestOnlyMailbox<>(scheduler, event -> {
            delivered.add(event);
            if (event.snapshot().requestSequence() == 1) {
                firstDeliveryStarted.countDown();
                await(continueFirstDelivery);
            }
        });
        mailbox.publish(published(1));

        Thread ownerThread = new Thread(scheduler::runNext, "test-render-owner");
        ownerThread.start();
        assertThat(firstDeliveryStarted.await(5, TimeUnit.SECONDS)).isTrue();

        mailbox.publish(published(2));
        mailbox.publish(published(3));
        continueFirstDelivery.countDown();
        ownerThread.join(TimeUnit.SECONDS.toMillis(5));

        assertThat(ownerThread.isAlive()).isFalse();
        assertThat(delivered).containsExactly(published(1));
        assertThat(scheduler.scheduledCount()).isEqualTo(2);
        assertThat(scheduler.pendingCount()).isEqualTo(1);
        assertThat(scheduler.peakPendingCount()).isEqualTo(1);

        scheduler.runNext();

        assertThat(delivered).containsExactly(published(1), published(3));
        assertThat(scheduler.pendingCount()).isZero();
    }

    @Test
    void publicationBetweenOwnershipReleaseAndRecheckUsesOneQueuedDrain() {
        RecordingScheduler scheduler = new RecordingScheduler();
        List<WorkbenchEvent.SnapshotPublished> delivered = new ArrayList<>();
        AtomicBoolean firstRelease = new AtomicBoolean(true);
        AtomicReference<LatestOnlyMailbox<WorkbenchEvent.SnapshotPublished>> mailboxRef = new AtomicReference<>();
        LatestOnlyMailbox<WorkbenchEvent.SnapshotPublished> mailbox = new LatestOnlyMailbox<>(
                scheduler,
                delivered::add,
                () -> {
                    if (firstRelease.compareAndSet(true, false)) {
                        mailboxRef.get().publish(published(2));
                        mailboxRef.get().publish(published(3));
                    }
                });
        mailboxRef.set(mailbox);
        mailbox.publish(published(1));

        scheduler.runNext();

        assertThat(delivered).containsExactly(published(1));
        assertThat(scheduler.scheduledCount()).isEqualTo(2);
        assertThat(scheduler.pendingCount()).isEqualTo(1);
        assertThat(scheduler.peakPendingCount()).isEqualTo(1);

        scheduler.runNext();

        assertThat(delivered).containsExactly(published(1), published(3));
        assertThat(scheduler.scheduledCount()).isEqualTo(2);
        assertThat(scheduler.pendingCount()).isZero();
    }

    @Test
    void publicationDuringFailedSchedulingIsRescheduledWithoutHidingTheFailure() throws Exception {
        BlockingFailureScheduler scheduler = new BlockingFailureScheduler();
        List<WorkbenchEvent.SnapshotPublished> delivered = new ArrayList<>();
        LatestOnlyMailbox<WorkbenchEvent.SnapshotPublished> mailbox =
                new LatestOnlyMailbox<>(scheduler, delivered::add);
        AtomicReference<RuntimeException> observedFailure = new AtomicReference<>();

        Thread firstPublisher = new Thread(() -> {
            try {
                mailbox.publish(published(1));
            } catch (RuntimeException failure) {
                observedFailure.set(failure);
            }
        }, "test-first-publisher");
        firstPublisher.start();

        assertThat(scheduler.awaitFirstSchedule()).isTrue();
        mailbox.publish(published(2));
        assertThat(scheduler.scheduledCount()).isEqualTo(1);
        assertThat(scheduler.pendingCount()).isZero();

        scheduler.failFirstSchedule();
        firstPublisher.join(TimeUnit.SECONDS.toMillis(5));

        assertThat(firstPublisher.isAlive()).isFalse();
        assertThat(observedFailure.get()).isSameAs(scheduler.failure());
        assertThat(scheduler.scheduledCount()).isEqualTo(2);
        assertThat(scheduler.pendingCount()).isEqualTo(1);
        assertThat(scheduler.peakPendingCount()).isEqualTo(1);

        scheduler.runNext();

        assertThat(delivered).containsExactly(published(2));
        assertThat(scheduler.pendingCount()).isZero();
        assertThat(scheduler.peakPendingCount()).isEqualTo(1);
    }

    private static void await(CountDownLatch latch) {
        try {
            if (!latch.await(5, TimeUnit.SECONDS)) {
                throw new AssertionError("Timed out waiting for the test drain to continue");
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new AssertionError("Interrupted while waiting for the test drain to continue", e);
        }
    }

    private static WorkbenchEvent.SnapshotPublished published(long sequence) {
        return new WorkbenchEvent.SnapshotPublished(new WorkbenchSnapshot(1, sequence));
    }

    private static final class RecordingScheduler implements LatestOnlyMailbox.Scheduler {
        private final Queue<Runnable> callbacks = new ArrayDeque<>();
        private int scheduledCount;
        private int peakPendingCount;

        @Override
        public synchronized void runLater(Runnable callback) {
            scheduledCount++;
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

        private synchronized int scheduledCount() {
            return scheduledCount;
        }

        private synchronized int pendingCount() {
            return callbacks.size();
        }

        private synchronized int peakPendingCount() {
            return peakPendingCount;
        }
    }

    private static final class BlockingFailureScheduler implements LatestOnlyMailbox.Scheduler {
        private final CountDownLatch firstScheduleStarted = new CountDownLatch(1);
        private final CountDownLatch releaseFirstSchedule = new CountDownLatch(1);
        private final RuntimeException failure = new IllegalStateException("test scheduler failure");
        private final Queue<Runnable> callbacks = new ArrayDeque<>();
        private int scheduledCount;
        private int peakPendingCount;

        @Override
        public void runLater(Runnable callback) {
            boolean firstSchedule;
            synchronized (this) {
                scheduledCount++;
                firstSchedule = scheduledCount == 1;
                if (!firstSchedule) {
                    callbacks.add(callback);
                    peakPendingCount = Math.max(peakPendingCount, callbacks.size());
                }
            }
            if (firstSchedule) {
                firstScheduleStarted.countDown();
                await(releaseFirstSchedule);
                throw failure;
            }
        }

        private boolean awaitFirstSchedule() throws InterruptedException {
            return firstScheduleStarted.await(5, TimeUnit.SECONDS);
        }

        private void failFirstSchedule() {
            releaseFirstSchedule.countDown();
        }

        private RuntimeException failure() {
            return failure;
        }

        private void runNext() {
            Runnable callback;
            synchronized (this) {
                callback = callbacks.remove();
            }
            callback.run();
        }

        private synchronized int scheduledCount() {
            return scheduledCount;
        }

        private synchronized int pendingCount() {
            return callbacks.size();
        }

        private synchronized int peakPendingCount() {
            return peakPendingCount;
        }
    }
}
