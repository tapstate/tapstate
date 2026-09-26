package io.tapstate.app;

import io.tapstate.core.lifecycle.Observation;
import io.tapstate.core.lifecycle.PipelineState;
import io.tapstate.runtime.scheduler.ObservationPublisher;
import io.tapstate.spi.metrics.MetricsExport;
import io.tapstate.spi.store.ObservationStore;
import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;

class TelemetryDispatcherTest {

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
            releaseFirst.countDown();
            assertThat(lastSaved.await(5, TimeUnit.SECONDS)).isTrue();
            synchronized (saved) {
                assertThat(saved).containsExactly(1L, 3L);
            }
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
