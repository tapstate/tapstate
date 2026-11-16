package io.tapstate.app;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import org.junit.jupiter.api.Test;

import static io.tapstate.app.PeriodicObservations.Offer.ACCEPTED;
import static io.tapstate.app.PeriodicObservations.Offer.COALESCED;
import static io.tapstate.app.PeriodicObservations.Offer.REJECTED;
import static org.assertj.core.api.Assertions.assertThat;

class PeriodicObservationsTest {

    @Test
    void aHeldPipelineLeavesAnotherWorkerFreeAndOnlyItsNewestFollowUpRuns() throws Exception {
        var held = new CountDownLatch(1);
        var release = new CountDownLatch(1);
        var other = new CountDownLatch(1);
        var newest = new CountDownLatch(1);
        var obsoleteRuns = new AtomicInteger();
        try (var observations = new PeriodicObservations(2, 1)) {
            try {
                assertThat(observations.offer("a", "run", ignored -> hold(held, release))).isEqualTo(ACCEPTED);
                assertThat(held.await(5, TimeUnit.SECONDS)).isTrue();
                assertThat(observations.offer("a", "run", ignored -> obsoleteRuns.incrementAndGet())).isEqualTo(ACCEPTED);
                assertThat(observations.offer("a", "run", ignored -> newest.countDown())).isEqualTo(COALESCED);
                assertThat(observations.offer("b", "run", ignored -> other.countDown())).isEqualTo(ACCEPTED);
                assertThat(other.await(5, TimeUnit.SECONDS)).isTrue();
                assertThat(newest.getCount()).isEqualTo(1);
                assertThat(obsoleteRuns).hasValue(0);
            } finally {
                release.countDown();
            }
            assertThat(newest.await(5, TimeUnit.SECONDS)).isTrue();
            assertThat(obsoleteRuns).hasValue(0);
        }
    }

    @Test
    void aFullQueueRefusesNewPipelinesInsteadOfRunningOnTheCaller() throws Exception {
        var held = new CountDownLatch(1);
        var release = new CountDownLatch(1);
        var queued = new CountDownLatch(1);
        var rejectedRuns = new AtomicInteger();
        try (var observations = new PeriodicObservations(1, 1)) {
            try {
                assertThat(observations.offer("a", "run", ignored -> hold(held, release))).isEqualTo(ACCEPTED);
                assertThat(held.await(5, TimeUnit.SECONDS)).isTrue();
                assertThat(observations.offer("b", "run", ignored -> queued.countDown())).isEqualTo(ACCEPTED);
                assertThat(observations.offer("c", "run", ignored -> rejectedRuns.incrementAndGet())).isEqualTo(REJECTED);
                assertThat(queued.getCount()).isEqualTo(1);
            } finally {
                release.countDown();
            }
            assertThat(queued.await(5, TimeUnit.SECONDS)).isTrue();
            assertThat(rejectedRuns).hasValue(0);
        }
    }

    @Test
    void aSuccessorInvalidatesTheHeldFrameWithoutStartingItOnASecondWorker() throws Exception {
        var entered = new CountDownLatch(1);
        var release = new CountDownLatch(1);
        var successor = new CountDownLatch(1);
        var oldCommitted = new AtomicInteger();
        try (var observations = new PeriodicObservations(2, 1)) {
            try {
                observations.offer("a", "original", current -> {
                    hold(entered, release);
                    if (current.getAsBoolean()) {
                        oldCommitted.incrementAndGet();
                    }
                });
                assertThat(entered.await(5, TimeUnit.SECONDS)).isTrue();
                observations.offer("a", "successor", current -> successor.countDown());
                assertThat(successor.getCount()).isEqualTo(1);
            } finally {
                release.countDown();
            }
            assertThat(successor.await(5, TimeUnit.SECONDS)).isTrue();
            assertThat(oldCommitted).hasValue(0);
        }
    }

    private static void hold(CountDownLatch entered, CountDownLatch release) {
        entered.countDown();
        try {
            release.await(10, TimeUnit.SECONDS);
        } catch (InterruptedException stopped) {
            Thread.currentThread().interrupt();
        }
    }
}
