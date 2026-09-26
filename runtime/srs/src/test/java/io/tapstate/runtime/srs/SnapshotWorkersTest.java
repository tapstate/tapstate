package io.tapstate.runtime.srs;

import org.junit.jupiter.api.Test;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;

class SnapshotWorkersTest {

    @Test
    void reservationPrecedesActivationAndACancelledQueuedReadReturnsItsCapacity() throws Exception {
        CountDownLatch firstEntered = new CountDownLatch(1);
        CountDownLatch releaseFirst = new CountDownLatch(1);
        CountDownLatch firstFinished = new CountDownLatch(1);
        CountDownLatch secondEntered = new CountDownLatch(1);
        try (SnapshotWorkers workers = new SnapshotWorkers(1, 1)) {
            SnapshotWorkers.Reservation first = workers.reserve().orElseThrow();
            SnapshotWorkers.Reservation second = workers.reserve().orElseThrow();
            assertThat(workers.reserve()).isEmpty();
            assertThat(firstEntered.getCount()).isEqualTo(1);

            first.activate(() -> {
                firstEntered.countDown();
                try {
                    if (!releaseFirst.await(5, TimeUnit.SECONDS)) {
                        throw new AssertionError("first snapshot was not released");
                    }
                } catch (InterruptedException interrupted) {
                    Thread.currentThread().interrupt();
                    throw new IllegalStateException(interrupted);
                } finally {
                    firstFinished.countDown();
                }
            });
            assertThat(firstEntered.await(5, TimeUnit.SECONDS)).isTrue();
            second.activate(secondEntered::countDown);
            second.close();
            assertThat(secondEntered.getCount()).isEqualTo(1);
            SnapshotWorkers.Reservation replacement = workers.reserve().orElseThrow();
            replacement.close();

            releaseFirst.countDown();
            assertThat(firstFinished.await(5, TimeUnit.SECONDS)).isTrue();
            first.close();
        } finally {
            releaseFirst.countDown();
        }
    }
}
