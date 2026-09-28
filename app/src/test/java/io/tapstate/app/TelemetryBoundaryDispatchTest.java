package io.tapstate.app;

import io.tapstate.core.lifecycle.MetricFact;
import io.tapstate.core.lifecycle.Observation;
import io.tapstate.core.lifecycle.PipelineEvent;
import io.tapstate.core.lifecycle.PipelineState;
import io.tapstate.runtime.scheduler.ObservationPublisher;
import io.tapstate.runtime.scheduler.RateSampler;
import io.tapstate.spi.metrics.MetricsExport;
import io.tapstate.spi.store.ObservationStore;
import io.tapstate.spi.store.PipelineEventStore;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.time.Instant;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.BooleanSupplier;

import static org.assertj.core.api.Assertions.assertThat;

class TelemetryBoundaryDispatchTest {

    @Test
    void everyDataSinkOffersOneDegradedAndRestoredBoundaryForRepeatedFailures() throws Exception {
        List<PipelineEvent> events = new CopyOnWriteArrayList<>();
        AtomicInteger latestWrites = new AtomicInteger();
        AtomicInteger exportOffers = new AtomicInteger();
        ObservationStore latest = new ObservationStore() {
            @Override public void save(Observation observation) { throw new AssertionError("unscoped write"); }
            @Override public boolean saveScoped(Observation observation, Scope scope) {
                if (latestWrites.incrementAndGet() <= 2) {
                    throw new IllegalStateException("test latest store unavailable");
                }
                return true;
            }
            @Override public Optional<Observation> read(String id) { return Optional.empty(); }
            @Override public void delete(String id) { }
        };
        MetricsExport export = new MetricsExport() {
            @Override public void offer(String id, PipelineState state, Instant at, List<MetricFact> facts) {
                if (exportOffers.incrementAndGet() <= 2) {
                    throw new IllegalStateException("test local export unavailable");
                }
            }
            @Override public void forgetPipelinesOutside(Collection<String> ids) { }
        };
        InMemoryRateHistoryStore history = new InMemoryRateHistoryStore();
        ObservationScopeRegistry scopes = new ObservationScopeRegistry();
        try (TelemetryDispatcher dispatcher = new TelemetryDispatcher(
                new ObservationPublisher(new InMemoryStateStore(), latest),
                new RateSampler(history, Duration.ofMinutes(1)), export, scopes,
                eventStore(events), 1, 8)) {
            ObservationStore.Scope scope = scopes.begin("orders", "inc-a", 41);
            for (int sequence = 1; sequence <= 2; sequence++) {
                history.failNextAppend();
                dispatcher.offer(frame(sequence), scope);
                long expected = sequence;
                await(() -> List.of(TelemetryDispatcher.Sink.LATEST, TelemetryDispatcher.Sink.HISTORY,
                        TelemetryDispatcher.Sink.EXPORT).stream().allMatch(sink ->
                        dispatcher.health().get(sink).failures() == expected));
            }
            await(() -> events.size() == 3);
            dispatcher.offer(frame(3), scope);
            await(() -> events.size() == 6);
            dispatcher.offer(frame(4), scope);
            await(() -> dispatcher.health().get(TelemetryDispatcher.Sink.LATEST).successes() >= 2
                    && dispatcher.health().get(TelemetryDispatcher.Sink.EXPORT).successes() >= 2);
            assertThat(events).hasSize(6);
            for (String reason : List.of("latest observation write", "history sample", "local metrics export offer")) {
                assertThat(events.stream().filter(event -> reason.equals(event.reason())))
                        .extracting(PipelineEvent::kind).containsExactly(
                                PipelineEvent.Kind.TELEMETRY_DEGRADED, PipelineEvent.Kind.TELEMETRY_RESTORED);
            }
            assertThat(events).allSatisfy(event -> {
                assertThat(event.pipelineIncarnationId()).isEqualTo("inc-a");
                assertThat(event.executionGeneration()).isEqualTo(41);
            });
        }
    }

    @Test
    void theDeadlineReportsDegradedBeforeTheBlockedWriteFinishesAndLateSuccessCannotRestoreIt()
            throws Exception {
        List<PipelineEvent> events = new CopyOnWriteArrayList<>();
        CountDownLatch blocked = new CountDownLatch(1);
        CountDownLatch release = new CountDownLatch(1);
        AtomicInteger writes = new AtomicInteger();
        ObservationStore latest = new ObservationStore() {
            @Override public void save(Observation observation) { throw new AssertionError("unscoped write"); }
            @Override public boolean saveScoped(Observation observation, Scope scope) {
                if (writes.incrementAndGet() == 1) {
                    blocked.countDown();
                    try {
                        if (!release.await(5, TimeUnit.SECONDS)) {
                            throw new AssertionError("blocked latest write was not released");
                        }
                    } catch (InterruptedException interrupted) {
                        Thread.currentThread().interrupt();
                        throw new IllegalStateException(interrupted);
                    }
                }
                return true;
            }
            @Override public Optional<Observation> read(String id) { return Optional.empty(); }
            @Override public void delete(String id) { }
        };
        ObservationScopeRegistry scopes = new ObservationScopeRegistry();
        try (TelemetryDispatcher dispatcher = new TelemetryDispatcher(
                new ObservationPublisher(new InMemoryStateStore(), latest), null, MetricsExport.none(),
                scopes, eventStore(events), 1, 2, Duration.ofMillis(80))) {
            ObservationStore.Scope scope = scopes.begin("orders", "inc-a", 41);
            dispatcher.offer(frame(1), scope);
            assertThat(blocked.await(5, TimeUnit.SECONDS)).isTrue();
            await(() -> events.size() == 1);
            assertThat(events.getFirst().kind()).isEqualTo(PipelineEvent.Kind.TELEMETRY_DEGRADED);
            assertThat(dispatcher.health().get(TelemetryDispatcher.Sink.LATEST).inFlight()).isEqualTo(1);
            release.countDown();
            await(() -> dispatcher.health().get(TelemetryDispatcher.Sink.LATEST).failures() == 1);
            assertThat(events).hasSize(1);

            long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(5);
            while (events.size() < 2 && System.nanoTime() - deadline < 0) {
                dispatcher.offer(frame(2), scope);
                TimeUnit.MILLISECONDS.sleep(5);
            }
            assertThat(events).extracting(PipelineEvent::kind).containsExactly(
                    PipelineEvent.Kind.TELEMETRY_DEGRADED, PipelineEvent.Kind.TELEMETRY_RESTORED);
        } finally {
            release.countDown();
        }
    }

    @Test
    void aCompletedOlderExportCannotClearDegradedAfterANewerOfferWasDropped() throws Exception {
        List<PipelineEvent> events = new CopyOnWriteArrayList<>();
        CountDownLatch firstEntered = new CountDownLatch(1);
        CountDownLatch releaseFirst = new CountDownLatch(1);
        CountDownLatch secondEntered = new CountDownLatch(1);
        CountDownLatch releaseSecond = new CountDownLatch(1);
        AtomicInteger offers = new AtomicInteger();
        MetricsExport export = new MetricsExport() {
            @Override public void offer(String id, PipelineState state, Instant at, List<MetricFact> facts) {
                int number = offers.incrementAndGet();
                if (number <= 2) {
                    CountDownLatch entered = number == 1 ? firstEntered : secondEntered;
                    CountDownLatch release = number == 1 ? releaseFirst : releaseSecond;
                    entered.countDown();
                    try {
                        if (!release.await(5, TimeUnit.SECONDS)) {
                            throw new AssertionError("export was not released");
                        }
                    } catch (InterruptedException interrupted) {
                        Thread.currentThread().interrupt();
                        throw new IllegalStateException(interrupted);
                    }
                }
            }
            @Override public void forgetPipelinesOutside(Collection<String> ids) { }
        };
        ObservationScopeRegistry scopes = new ObservationScopeRegistry();
        try (TelemetryDispatcher dispatcher = new TelemetryDispatcher(
                new ObservationPublisher(new InMemoryStateStore(), acceptingLatest()),
                null, export, scopes, eventStore(events), 1, 1)) {
            try {
                ObservationStore.Scope scope = scopes.begin("orders", "inc-a", 41);
                dispatcher.offer(frame(1), scope);
                assertThat(firstEntered.await(5, TimeUnit.SECONDS)).isTrue();
                dispatcher.offer(frame(2), scope);
                dispatcher.offer(frame(3), scope);
                await(() -> dispatcher.health().get(TelemetryDispatcher.Sink.EXPORT).dropped() == 1);
                await(() -> events.size() == 1);
                releaseFirst.countDown();
                assertThat(secondEntered.await(5, TimeUnit.SECONDS)).isTrue();
                assertThat(events).extracting(PipelineEvent::kind)
                        .containsExactly(PipelineEvent.Kind.TELEMETRY_DEGRADED);
                assertThat(dispatcher.health().get(TelemetryDispatcher.Sink.EXPORT).degraded()).isTrue();
                releaseSecond.countDown();
                await(() -> events.size() == 2);
                assertThat(events).extracting(PipelineEvent::kind).containsExactly(
                        PipelineEvent.Kind.TELEMETRY_DEGRADED, PipelineEvent.Kind.TELEMETRY_RESTORED);
                assertThat(dispatcher.health().get(TelemetryDispatcher.Sink.EXPORT).degraded()).isFalse();
            } finally {
                releaseFirst.countDown();
                releaseSecond.countDown();
            }
        }
    }

    static ObservationStore acceptingLatest() {
        return new ObservationStore() {
            @Override public void save(Observation observation) { }
            @Override public boolean saveScoped(Observation observation, Scope scope) { return true; }
            @Override public Optional<Observation> read(String id) { return Optional.empty(); }
            @Override public void delete(String id) { }
        };
    }

    static PipelineEventStore eventStore(List<PipelineEvent> events) {
        return new PipelineEventStore() {
            @Override public void append(PipelineEvent event) { events.add(event); }
            @Override public Page readPage(String id, String incarnation, Instant from, Instant to,
                    Key after, int limit) { return new Page(List.of(), false); }
            @Override public void deleteIncarnation(String id, String incarnation) { }
            @Override public Duration retention() { return Duration.ofDays(15); }
        };
    }

    private static void await(BooleanSupplier complete) throws Exception {
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(5);
        while (!complete.getAsBoolean() && System.nanoTime() - deadline < 0) {
            TimeUnit.MILLISECONDS.sleep(5);
        }
        assertThat(complete.getAsBoolean()).isTrue();
    }

    private static ObservationPublisher.Prepared frame(long sequence) {
        Observation observation = new Observation("orders", PipelineState.RUNNING,
                Map.of("records.out", sequence), Map.of(), Map.of(), null,
                Instant.parse("2026-09-28T01:00:00Z").plusSeconds(sequence));
        return new ObservationPublisher.Prepared(observation, false, Map.of(), Map.of(), Map.of());
    }
}
