package io.tapstate.app;

import io.tapstate.core.lifecycle.MetricFact;
import io.tapstate.core.lifecycle.MetricType;
import io.tapstate.core.lifecycle.Observation;
import io.tapstate.core.lifecycle.PipelineState;
import io.tapstate.runtime.scheduler.ObservationPublisher;
import io.tapstate.spi.metrics.MetricsExport;
import io.tapstate.spi.store.ObservationStore;
import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.BooleanSupplier;

import static org.assertj.core.api.Assertions.assertThat;

class TelemetryBreakerFactsTest {

    private static final Instant START = Instant.parse("2026-09-28T00:00:00Z");

    @Test
    void failedProbesStayOpenAndASuccessfulCloseCountsOneRecovery() throws Exception {
        AtomicInteger writes = new AtomicInteger();
        CountDownLatch probeEntered = new CountDownLatch(1);
        CountDownLatch releaseProbe = new CountDownLatch(1);
        ObservationStore store = new ObservationStore() {
            @Override public void save(Observation observation) {
                if (writes.incrementAndGet() <= 4) {
                    throw new IllegalStateException("test store unavailable");
                }
                if (writes.get() == 5) {
                    probeEntered.countDown();
                    try {
                        if (!releaseProbe.await(5, TimeUnit.SECONDS)) {
                            throw new AssertionError("recovery probe was not released");
                        }
                    } catch (InterruptedException interrupted) {
                        Thread.currentThread().interrupt();
                        throw new IllegalStateException(interrupted);
                    }
                }
            }
            @Override public Optional<Observation> read(String id) { return Optional.empty(); }
            @Override public void delete(String id) { }
        };
        try (TelemetryDispatcher dispatcher = new TelemetryDispatcher(
                new ObservationPublisher(new InMemoryStateStore(), store), null,
                MetricsExport.none(), 1, 1)) {
            assertFacts(dispatcher, 0, 0);
            for (int attempt = 1; attempt <= 3; attempt++) {
                dispatcher.offer(frame(attempt), null);
                long expected = attempt;
                await(() -> dispatcher.health().get(TelemetryDispatcher.Sink.LATEST).failures() == expected);
            }
            awaitBreaker(dispatcher, TelemetryDispatcher.BreakerState.OPEN, 0);
            assertFacts(dispatcher, 1, 0);

            offerUntil(dispatcher, 4, () ->
                    dispatcher.health().get(TelemetryDispatcher.Sink.LATEST).failures() == 4);
            awaitBreaker(dispatcher, TelemetryDispatcher.BreakerState.OPEN, 0);
            assertFacts(dispatcher, 1, 0);
            offerUntil(dispatcher, 5, () -> probeEntered.getCount() == 0);
            assertThat(writes).hasValue(5);
            assertFacts(dispatcher, 2, 0);
            releaseProbe.countDown();
            await(() -> dispatcher.health().get(TelemetryDispatcher.Sink.LATEST).successes() >= 1);
            awaitBreaker(dispatcher, TelemetryDispatcher.BreakerState.CLOSED, 1);
            assertFacts(dispatcher, 0, 1);

            dispatcher.offer(frame(6), null);
            await(() -> dispatcher.health().get(TelemetryDispatcher.Sink.LATEST).successes() >= 2);
            assertFacts(dispatcher, 0, 1);
        } finally {
            releaseProbe.countDown();
        }
    }

    private static void awaitBreaker(TelemetryDispatcher dispatcher,
            TelemetryDispatcher.BreakerState state, long recoveries) throws Exception {
        await(() -> {
            TelemetryDispatcher.Health health = dispatcher.health().get(TelemetryDispatcher.Sink.LATEST);
            return health.breakerState() == state && health.breakerRecoveries() == recoveries;
        });
    }

    private static void assertFacts(TelemetryDispatcher dispatcher, long state, long recoveries) {
        List<MetricFact> facts = TelemetryProcessFacts.snapshot(dispatcher.health(),
                Set.of(TelemetryDispatcher.Sink.LATEST), START, START.plusSeconds(10));
        assertFact(facts, "tapstate.process.telemetry.breaker.state", MetricType.GAUGE, "1", state);
        assertFact(facts, "tapstate.process.telemetry.breaker.recovered", MetricType.COUNTER,
                "{recovery}", recoveries);
        assertThat(facts.stream().flatMap(fact -> fact.points().stream()))
                .allSatisfy(point -> assertThat(point.attributes()).containsExactlyEntriesOf(
                        Map.of("sink", "latest")));
    }

    private static void assertFact(List<MetricFact> facts, String name, MetricType type,
            String unit, long value) {
        MetricFact fact = facts.stream().filter(item -> item.name().equals(name)).findFirst().orElseThrow();
        assertThat(fact.type()).isEqualTo(type);
        assertThat(fact.unit()).isEqualTo(unit);
        assertThat(fact.points()).singleElement().satisfies(point -> {
            assertThat(point.value()).isEqualTo(value);
            if (type == MetricType.COUNTER) {
                assertThat(point.startTime()).isEqualTo(START);
            }
        });
    }

    private static void offerUntil(TelemetryDispatcher dispatcher, long sequence,
            BooleanSupplier complete) throws Exception {
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(5);
        while (!complete.getAsBoolean() && System.nanoTime() - deadline < 0) {
            dispatcher.offer(frame(sequence), null);
            TimeUnit.MILLISECONDS.sleep(5);
        }
        assertThat(complete.getAsBoolean()).isTrue();
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
                Map.of("sequence", sequence), Map.of(), Map.of(), null, START.plusSeconds(sequence));
        return new ObservationPublisher.Prepared(observation, false, Map.of(), Map.of(), Map.of());
    }
}
