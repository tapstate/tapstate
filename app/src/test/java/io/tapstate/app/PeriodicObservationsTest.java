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
                var dispatched = new CountDownLatch(1);
                assertThat(observations.offer("b", "independent", current -> dispatched.countDown())).isEqualTo(ACCEPTED);
                assertThat(dispatched.await(5, TimeUnit.SECONDS)).isTrue();
                assertThat(successor.getCount()).isEqualTo(1);
            } finally {
                release.countDown();
            }
            assertThat(successor.await(5, TimeUnit.SECONDS)).isTrue();
            assertThat(oldCommitted).hasValue(0);
        }
    }

    @Test
    void defaultLogsReportOneBackpressureEdgeAndItsRecovery() throws Exception {
        var entered = new CountDownLatch(1);
        var release = new CountDownLatch(1);
        var events = new java.util.concurrent.ConcurrentLinkedQueue<ch.qos.logback.classic.spi.ILoggingEvent>();
        var logger = (ch.qos.logback.classic.Logger) org.slf4j.LoggerFactory.getLogger(PeriodicObservations.class);
        var previous = logger.getLevel();
        var appender = new ch.qos.logback.core.AppenderBase<ch.qos.logback.classic.spi.ILoggingEvent>() {
            @Override protected void append(ch.qos.logback.classic.spi.ILoggingEvent event) {
                event.prepareForDeferredProcessing();
                events.add(event);
            }
        };
        appender.setContext(logger.getLoggerContext());
        appender.start();
        logger.addAppender(appender);
        logger.setLevel(ch.qos.logback.classic.Level.INFO);
        try (var observations = new PeriodicObservations(2, 1)) {
            observations.offer("a", "run", current -> hold(entered, release));
            assertThat(entered.await(5, TimeUnit.SECONDS)).isTrue();
            observations.offer("a", "run", current -> {});
            observations.offer("a", "run", current -> {});
            observations.offer("a", "run", current -> {});
            assertThat(observations.health().coalescedRequests()).isEqualTo(2);
            assertThat(observations.health().retainedPipelines()).isEqualTo(1);
            org.awaitility.Awaitility.await().atMost(java.time.Duration.ofSeconds(5)).untilAsserted(() ->
                    assertThat(events.stream().filter(event -> event.getLevel() == ch.qos.logback.classic.Level.WARN))
                            .hasSize(1));
            release.countDown();
            org.awaitility.Awaitility.await().atMost(java.time.Duration.ofSeconds(5)).until(() ->
                    events.stream().anyMatch(event -> event.getFormattedMessage().startsWith("Periodic observation backpressure cleared")));
            assertThat(observations.health().backpressured()).isFalse();
        } finally {
            release.countDown();
            logger.detachAppender(appender);
            appender.stop();
            logger.setLevel(previous);
        }
    }

    @Test
    void aRejectedFollowUpEntersBackpressureUntilTheQueueDrains() throws Exception {
        var entered = new CountDownLatch(1);
        var release = new CountDownLatch(1);
        var queuedEntered = new CountDownLatch(1);
        var queuedRelease = new CountDownLatch(1);
        try (var observations = new PeriodicObservations(1, 1)) {
            try {
                observations.offer("a", "run", ignored -> hold(entered, release));
                assertThat(entered.await(5, TimeUnit.SECONDS)).isTrue();
                observations.offer("a", "run", ignored -> {});
                observations.offer("b", "run", ignored -> hold(queuedEntered, queuedRelease));
                assertThat(observations.health().backpressured()).isFalse();
                release.countDown();
                assertThat(queuedEntered.await(5, TimeUnit.SECONDS)).isTrue();
                assertThat(observations.health().rejectedRequests()).isEqualTo(1);
                assertThat(observations.health().backpressured()).isTrue();
                queuedRelease.countDown();
                org.awaitility.Awaitility.await().atMost(java.time.Duration.ofSeconds(5)).until(() ->
                        observations.health().retainedPipelines() == 0);
                assertThat(observations.health().backpressured()).isFalse();
            } finally {
                release.countDown();
                queuedRelease.countDown();
            }
        }
    }

    @Test
    void aDelayedPressureLogDoesNotBlockOffersOrCompletionAndReportsRecoveryInOrder() throws Exception {
        var entered = new CountDownLatch(1);
        var release = new CountDownLatch(1);
        var warningEntered = new CountDownLatch(1);
        var warningRelease = new CountDownLatch(1);
        var independent = new CountDownLatch(1);
        var events = new java.util.concurrent.CopyOnWriteArrayList<ch.qos.logback.classic.spi.ILoggingEvent>();
        var logger = (ch.qos.logback.classic.Logger) org.slf4j.LoggerFactory.getLogger(PeriodicObservations.class);
        var previous = logger.getLevel();
        var appender = new ch.qos.logback.core.UnsynchronizedAppenderBase<ch.qos.logback.classic.spi.ILoggingEvent>() {
            @Override protected void append(ch.qos.logback.classic.spi.ILoggingEvent event) {
                if (event.getLevel() == ch.qos.logback.classic.Level.WARN) {
                    hold(warningEntered, warningRelease);
                }
                event.prepareForDeferredProcessing();
                events.add(event);
            }
        };
        appender.setContext(logger.getLoggerContext());
        appender.start();
        logger.addAppender(appender);
        logger.setLevel(ch.qos.logback.classic.Level.INFO);
        var caller = java.util.concurrent.Executors.newSingleThreadExecutor();
        try (var observations = new PeriodicObservations(1, 1)) {
            observations.offer("a", "run", ignored -> hold(entered, release));
            assertThat(entered.await(5, TimeUnit.SECONDS)).isTrue();
            observations.offer("a", "run", ignored -> {});
            var offered = caller.submit(() -> observations.offer("a", "run", ignored -> {}));
            assertThat(warningEntered.await(5, TimeUnit.SECONDS)).isTrue();
            assertThat(offered.get(2, TimeUnit.SECONDS)).isEqualTo(COALESCED);
            observations.discard("a");
            assertThat(observations.health().coalescedRequests()).isEqualTo(1);
            assertThat(caller.submit(() -> observations.offer("b", "run", ignored -> independent.countDown())).get(2, TimeUnit.SECONDS))
                    .isEqualTo(ACCEPTED);
            release.countDown();
            assertThat(independent.await(2, TimeUnit.SECONDS)).isTrue();
            org.awaitility.Awaitility.await().atMost(java.time.Duration.ofSeconds(2))
                    .until(() -> observations.pipelineIds().isEmpty() && !observations.health().backpressured());
            assertThat(events).isEmpty();
            warningRelease.countDown();
            org.awaitility.Awaitility.await().atMost(java.time.Duration.ofSeconds(5)).until(() -> events.size() == 2);
            assertThat(events.get(0).getLevel()).isEqualTo(ch.qos.logback.classic.Level.WARN);
            assertThat(events.get(0).getFormattedMessage()).contains("backpressured=true");
            assertThat(events.get(1).getLevel()).isEqualTo(ch.qos.logback.classic.Level.INFO);
            assertThat(events.get(1).getFormattedMessage()).contains("backpressured=false");
        } finally {
            release.countDown();
            warningRelease.countDown();
            caller.shutdownNow();
            logger.detachAppender(appender);
            appender.stop();
            logger.setLevel(previous);
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
