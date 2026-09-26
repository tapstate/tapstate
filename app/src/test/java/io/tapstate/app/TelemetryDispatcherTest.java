package io.tapstate.app;

import io.tapstate.core.lifecycle.Observation;
import io.tapstate.core.lifecycle.PipelineState;
import io.tapstate.runtime.scheduler.ObservationPublisher;
import io.tapstate.runtime.scheduler.RateSampler;
import io.tapstate.spi.metrics.MetricsExport;
import io.tapstate.spi.store.ObservationStore;
import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;

class TelemetryDispatcherTest {

    @Test
    void historyWorkerRetainsTheExecutionScopeOfTheOfferedFrame() throws Exception {
        InMemoryRateHistoryStore history = new InMemoryRateHistoryStore();
        ObservationStore latest = new ObservationStore() {
            @Override public void save(Observation observation) { }
            @Override public boolean saveScoped(Observation observation, Scope scope) { return true; }
            @Override public Optional<Observation> read(String id) { return Optional.empty(); }
            @Override public void delete(String id) { }
        };
        ObservationScopeRegistry scopes = new ObservationScopeRegistry();
        ObservationStore.Scope owner = scopes.begin("orders", "inc-current", 42);
        Observation observation = new Observation("orders", PipelineState.RUNNING,
                Map.of("records.out", 7L), Map.of(), Map.of(), null,
                Instant.parse("2026-09-26T10:00:00Z"));
        ObservationPublisher.Prepared prepared = new ObservationPublisher.Prepared(observation, false,
                Map.of(), Map.of(), Map.of());

        try (TelemetryDispatcher dispatcher = new TelemetryDispatcher(
                new ObservationPublisher(new InMemoryStateStore(), latest),
                new RateSampler(history, Duration.ofMinutes(1)), MetricsExport.none(), scopes, 1, 4)) {
            dispatcher.offer(prepared, owner);
            long until = System.nanoTime() + TimeUnit.SECONDS.toNanos(5);
            while (dispatcher.health().get(TelemetryDispatcher.Sink.HISTORY).successes() == 0
                    && System.nanoTime() < until) {
                TimeUnit.MILLISECONDS.sleep(5);
            }
            assertThat(dispatcher.health().get(TelemetryDispatcher.Sink.HISTORY).successes()).isEqualTo(1);
            assertThat(history.readPageVisible("orders", new io.tapstate.spi.store.RateHistoryStore.Visibility(
                    "inc-current", false), observation.observedAt(),
                    observation.observedAt().plusSeconds(1), null, 10).entries()).hasSize(1);
            assertThat(history.readPageVisible("orders", new io.tapstate.spi.store.RateHistoryStore.Visibility(
                    null, true), observation.observedAt(), observation.observedAt().plusSeconds(1),
                    null, 10).entries()).isEmpty();
        }
    }

    @Test
    void aTimedOutWriteOpensTheBreakerWithoutStartingUnboundedReplacementWorkers() throws Exception {
        CountDownLatch firstEntered = new CountDownLatch(1);
        CountDownLatch releaseFirst = new CountDownLatch(1);
        List<Long> saved = new ArrayList<>();
        ObservationStore store = new ObservationStore() {
            @Override public void save(Observation observation) {
                long sequence = observation.metrics().get("sequence");
                if (sequence == 1) {
                    firstEntered.countDown();
                    try {
                        if (!releaseFirst.await(5, TimeUnit.SECONDS)) {
                            throw new AssertionError("first latest write was not released");
                        }
                    } catch (InterruptedException interrupted) {
                        Thread.currentThread().interrupt();
                        throw new IllegalStateException(interrupted);
                    }
                }
                synchronized (saved) {
                    saved.add(sequence);
                }
            }
            @Override public Optional<Observation> read(String id) { return Optional.empty(); }
            @Override public void delete(String id) { }
        };
        ObservationPublisher publisher = new ObservationPublisher(new InMemoryStateStore(), store);
        try (TelemetryDispatcher dispatcher = new TelemetryDispatcher(
                publisher, null, MetricsExport.none(), null, 1, 1,
                java.time.Duration.ofMillis(80))) {
            dispatcher.offer(frame(1), null);
            assertThat(firstEntered.await(5, TimeUnit.SECONDS)).isTrue();
            long until = System.nanoTime() + TimeUnit.SECONDS.toNanos(5);
            while (dispatcher.health().get(TelemetryDispatcher.Sink.LATEST).timeouts() == 0
                    && System.nanoTime() < until) {
                TimeUnit.MILLISECONDS.sleep(5);
            }
            TelemetryDispatcher.Health timedOut = dispatcher.health().get(TelemetryDispatcher.Sink.LATEST);
            assertThat(timedOut.timeouts()).isEqualTo(1);
            assertThat(timedOut.inFlight()).isEqualTo(1);
            assertThat(timedOut.degraded()).isTrue();
            dispatcher.offer(frame(2), null);
            releaseFirst.countDown();
            until = System.nanoTime() + TimeUnit.SECONDS.toNanos(5);
            while (dispatcher.health().get(TelemetryDispatcher.Sink.LATEST).dropped() == 0
                    && System.nanoTime() < until) {
                TimeUnit.MILLISECONDS.sleep(5);
            }
            assertThat(dispatcher.health().get(TelemetryDispatcher.Sink.LATEST).dropped()).isGreaterThanOrEqualTo(1);
            until = System.nanoTime() + TimeUnit.SECONDS.toNanos(5);
            while (dispatcher.health().get(TelemetryDispatcher.Sink.LATEST).successes() == 0
                    && System.nanoTime() < until) {
                dispatcher.offer(frame(3), null);
                TimeUnit.MILLISECONDS.sleep(10);
            }
            assertThat(dispatcher.health().get(TelemetryDispatcher.Sink.LATEST).successes()).isGreaterThanOrEqualTo(1);
            synchronized (saved) {
                assertThat(saved).doesNotContain(2L);
            }
        } finally {
            releaseFirst.countDown();
        }
    }

    @Test
    void aFailedLatestWriteMarksLocalHealthDegradedUntilANewerFrameSucceeds() throws Exception {
        AtomicInteger writes = new AtomicInteger();
        CountDownLatch recovered = new CountDownLatch(1);
        ObservationStore store = new ObservationStore() {
            @Override public void save(Observation observation) {
                if (writes.incrementAndGet() == 1) {
                    throw new IllegalStateException("store unavailable");
                }
                recovered.countDown();
            }
            @Override public Optional<Observation> read(String id) { return Optional.empty(); }
            @Override public void delete(String id) { }
        };
        ObservationPublisher publisher = new ObservationPublisher(new InMemoryStateStore(), store);
        try (TelemetryDispatcher dispatcher = new TelemetryDispatcher(
                publisher, null, MetricsExport.none(), 1, 1)) {
            dispatcher.offer(frame(1), null);
            long until = System.nanoTime() + TimeUnit.SECONDS.toNanos(5);
            while (dispatcher.health().get(TelemetryDispatcher.Sink.LATEST).failures() == 0
                    && System.nanoTime() < until) {
                TimeUnit.MILLISECONDS.sleep(5);
            }
            TelemetryDispatcher.Health failed = dispatcher.health().get(TelemetryDispatcher.Sink.LATEST);
            assertThat(failed.failures()).isEqualTo(1);
            assertThat(failed.degraded()).isTrue();
            assertThat(failed.lastSuccessAgeMillis()).isEmpty();

            dispatcher.offer(frame(2), null);
            assertThat(recovered.await(5, TimeUnit.SECONDS)).isTrue();
            until = System.nanoTime() + TimeUnit.SECONDS.toNanos(5);
            while (dispatcher.health().get(TelemetryDispatcher.Sink.LATEST).successes() == 0
                    && System.nanoTime() < until) {
                TimeUnit.MILLISECONDS.sleep(5);
            }
            TelemetryDispatcher.Health healthy = dispatcher.health().get(TelemetryDispatcher.Sink.LATEST);
            assertThat(healthy.successes()).isEqualTo(1);
            assertThat(healthy.degraded()).isFalse();
            assertThat(healthy.lastSuccessAgeMillis()).isPresent();
        }
    }

    @Test
    void aBlockedLatestWriteKeepsOnlyTheNewestPendingFrameForThatPipeline() throws Exception {
        CountDownLatch firstEntered = new CountDownLatch(1);
        CountDownLatch releaseFirst = new CountDownLatch(1);
        CountDownLatch lastSaved = new CountDownLatch(1);
        List<Long> saved = new ArrayList<>();
        ObservationStore store = new ObservationStore() {
            @Override public void save(Observation observation) {
                long sequence = observation.metrics().get("sequence");
                if (sequence == 1) {
                    firstEntered.countDown();
                    try {
                        if (!releaseFirst.await(5, TimeUnit.SECONDS)) {
                            throw new AssertionError("first latest write was not released");
                        }
                    } catch (InterruptedException interrupted) {
                        Thread.currentThread().interrupt();
                        throw new IllegalStateException(interrupted);
                    }
                }
                synchronized (saved) {
                    saved.add(sequence);
                }
                if (sequence == 3) {
                    lastSaved.countDown();
                }
            }
            @Override public Optional<Observation> read(String id) { return Optional.empty(); }
            @Override public void delete(String id) { }
        };
        ObservationPublisher publisher = new ObservationPublisher(new InMemoryStateStore(), store);
        try (TelemetryDispatcher dispatcher = new TelemetryDispatcher(
                publisher, null, MetricsExport.none(), 1, 1)) {
            dispatcher.offer(frame(1), null);
            assertThat(firstEntered.await(5, TimeUnit.SECONDS)).isTrue();
            dispatcher.offer(frame(2), null);
            dispatcher.offer(frame(3), null);
            TelemetryDispatcher.Health blocked = dispatcher.health().get(TelemetryDispatcher.Sink.LATEST);
            assertThat(blocked.queueDepth()).isEqualTo(1);
            assertThat(blocked.highWater()).isEqualTo(1);
            assertThat(blocked.coalesced()).isEqualTo(1);
            assertThat(blocked.successes()).isZero();
            releaseFirst.countDown();
            assertThat(lastSaved.await(5, TimeUnit.SECONDS)).isTrue();
            long until = System.nanoTime() + TimeUnit.SECONDS.toNanos(5);
            while (dispatcher.health().get(TelemetryDispatcher.Sink.LATEST).successes() < 2
                    && System.nanoTime() < until) {
                TimeUnit.MILLISECONDS.sleep(5);
            }
            synchronized (saved) {
                assertThat(saved).containsExactly(1L, 3L);
            }
            TelemetryDispatcher.Health completed = dispatcher.health().get(TelemetryDispatcher.Sink.LATEST);
            assertThat(completed.successes()).isEqualTo(2);
            assertThat(completed.failures()).isZero();
            assertThat(completed.dropped()).isZero();
            assertThat(completed.lastSuccessAgeMillis()).isPresent();
            assertThat(completed.degraded()).isFalse();
        } finally {
            releaseFirst.countDown();
        }
    }

    private static ObservationPublisher.Prepared frame(long sequence) {
        Observation observation = new Observation("orders", PipelineState.RUNNING,
                Map.of("sequence", sequence), Map.of(), Map.of(), null,
                Instant.parse("2026-09-26T10:00:00Z").plusMillis(sequence));
        return new ObservationPublisher.Prepared(observation, false, Map.of(), Map.of(), Map.of());
    }
}
