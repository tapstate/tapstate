package io.tapstate.app;

import io.tapstate.core.lifecycle.Observation;
import io.tapstate.core.lifecycle.PipelineState;
import io.tapstate.runtime.scheduler.ObservationPublisher;
import io.tapstate.spi.metrics.MetricsExport;
import io.tapstate.spi.store.ObservationStore;
import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;

class StaleReconcileFailureWriteHealthTest {

    @Test
    void aRejectedFailureProjectionCannotReportASuccessfulLatestWrite() throws Exception {
        CountDownLatch firstRejected = new CountDownLatch(1);
        CountDownLatch secondEntered = new CountDownLatch(1);
        CountDownLatch releaseSecond = new CountDownLatch(1);
        AtomicInteger writes = new AtomicInteger();
        ObservationStore store = new ObservationStore() {
            @Override public void save(Observation observation) { throw new AssertionError("unscoped write"); }
            @Override public boolean saveScoped(Observation observation, Scope scope) {
                if (writes.incrementAndGet() == 1) {
                    firstRejected.countDown();
                } else {
                    secondEntered.countDown();
                    try {
                        if (!releaseSecond.await(5, TimeUnit.SECONDS)) {
                            throw new AssertionError("second write was not released");
                        }
                    } catch (InterruptedException interrupted) {
                        Thread.currentThread().interrupt();
                        throw new IllegalStateException(interrupted);
                    }
                }
                return false;
            }
            @Override public Optional<Observation> read(String id) { return Optional.empty(); }
            @Override public void delete(String id) { }
        };
        ObservationScopeRegistry scopes = new ObservationScopeRegistry();
        try (TelemetryDispatcher dispatcher = new TelemetryDispatcher(
                new ObservationPublisher(new InMemoryStateStore(), store), null,
                MetricsExport.none(), scopes, 1, 1)) {
            ObservationStore.Scope scope = scopes.begin("orders", "inc-a", 41);
            dispatcher.offerReconcileFailure("orders", 3, scope);
            assertThat(firstRejected.await(5, TimeUnit.SECONDS)).isTrue();
            Observation next = new Observation("orders", PipelineState.RUNNING,
                    Map.of(), Map.of(), Map.of(), null, Instant.now());
            dispatcher.offer(new ObservationPublisher.Prepared(next, false,
                    Map.of(), Map.of(), Map.of()), scope);
            assertThat(secondEntered.await(5, TimeUnit.SECONDS)).isTrue();

            TelemetryDispatcher.Health health = dispatcher.health().get(TelemetryDispatcher.Sink.LATEST);
            assertThat(health.successes()).as("the first fenced write was a stale no-op").isZero();
            assertThat(health.lastSuccessAgeMillis()).isEmpty();
            assertThat(health.failures()).as("stale rejection is not an outage").isZero();
            releaseSecond.countDown();
        } finally {
            releaseSecond.countDown();
        }
    }
}
