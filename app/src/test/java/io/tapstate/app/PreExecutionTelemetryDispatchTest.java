package io.tapstate.app;

import io.tapstate.core.common.TapstateException;
import io.tapstate.core.lifecycle.CheckpointDoc;
import io.tapstate.core.lifecycle.DesiredState;
import io.tapstate.core.lifecycle.Observation;
import io.tapstate.core.lifecycle.ObservationFailure;
import io.tapstate.core.lifecycle.PipelineEvent;
import io.tapstate.core.lifecycle.PipelineState;
import io.tapstate.core.lifecycle.StateJson;
import io.tapstate.runtime.scheduler.ConvergeResult;
import io.tapstate.runtime.scheduler.ConvergeStatus;
import io.tapstate.runtime.scheduler.ObservationPublisher;
import io.tapstate.runtime.scheduler.RateSampler;
import io.tapstate.spi.metrics.MetricsExport;
import io.tapstate.spi.store.ArtifactStore;
import io.tapstate.spi.store.ExecutionGenerationStore;
import io.tapstate.spi.store.IoError;
import io.tapstate.spi.store.ObservationStore;
import io.tapstate.spi.store.PreExecutionFailure;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BooleanSupplier;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verifyNoInteractions;

/** Storage is a controlled boundary; the publisher, bounded workers, coalescing and recovery execute. */
class PreExecutionTelemetryDispatchTest {
    private static final String PIPE = "orders";
    private static final Instant AT = Instant.parse("2026-10-08T03:00:00Z");
    private static final ObservationFailure CAUSE = new ObservationFailure(
            "actuation.source-schema-not-discovered", Map.of("source", "src_x"));

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    void activeAndParkedCoalescingKeepTheOriginalCauseThroughAWriteOutage(boolean historical) throws Exception {
        Fixture f = new Fixture(historical);
        Observation old = diagnostic(AT.minusSeconds(10));
        f.store.saved.put(PIPE, new ObservationStore.Stored(old, Optional.empty(), Optional.of(f.receipt.owner())));
        f.store.blockFirstSave.set(true);
        try (TelemetryDispatcher dispatcher = f.dispatcher()) {
            dispatcher.offerPreExecutionFailure(f.receipt, CAUSE, () -> TelemetryDispatcher.PublicationQualification.CURRENT, null);
            assertThat(f.store.entered.await(5, TimeUnit.SECONDS)).isTrue();
            dispatcher.offerScopeRecovery(PIPE, f.neutralResult(), null, () -> true);
            assertThat(f.store.read(PIPE)).contains(old);
            f.store.release.countDown();
            await(() -> dispatcher.health().get(TelemetryDispatcher.Sink.LATEST).failures() == 1
                    && dispatcher.health().get(TelemetryDispatcher.Sink.LATEST).inFlight() == 0);
            assertThat(f.store.read(PIPE)).contains(old);
            assertThat(dispatcher.health().get(TelemetryDispatcher.Sink.LATEST).queueDepth()).isEqualTo(1);

            awaitOffering(() -> f.store.saves.get() >= 2, () -> dispatcher.offerReconcileFailure(PIPE, 99, null));
            await(() -> dispatcher.health().get(TelemetryDispatcher.Sink.LATEST).successes() >= 1);
            assertThat(f.store.saves).hasValue(2);
            assertThat(dispatcher.health().get(TelemetryDispatcher.Sink.LATEST).coalesced()).isGreaterThanOrEqualTo(1);
            f.assertDiagnosticOnly();
            assertThat(f.store.read(PIPE).orElseThrow().observedAt()).isAfter(old.observedAt());
        } finally { f.store.release.countDown(); }
    }

    @Test
    void aRetryableQualificationRetainsOneSlotAndAllowsAnotherPipelineToPublish() throws Exception {
        Fixture f = new Fixture(false);
        AtomicReference<TelemetryDispatcher.PublicationQualification> qualification =
                new AtomicReference<>(TelemetryDispatcher.PublicationQualification.RETRY);
        try (TelemetryDispatcher dispatcher = f.dispatcher()) {
            dispatcher.offerPreExecutionFailure(f.receipt, CAUSE, qualification::get, null);
            await(() -> dispatcher.health().get(TelemetryDispatcher.Sink.LATEST).inFlight() == 0
                    && dispatcher.health().get(TelemetryDispatcher.Sink.LATEST).queueDepth() == 1);
            assertThat(f.store.saves).hasValue(0);
            assertThat(f.store.read(PIPE)).isEmpty();
            Observation other = new Observation("other", PipelineState.RUNNING, Map.of(), Map.of(), Map.of(), null, AT);
            var otherScope = f.scopes.begin("other", "inc-other", 1);
            dispatcher.offer(new ObservationPublisher.Prepared(other, false, Map.of(), Map.of(), Map.of()), otherScope);
            await(() -> f.store.read("other").isPresent());
            assertThat(f.store.read("other")).contains(other);
            qualification.set(TelemetryDispatcher.PublicationQualification.CURRENT);
            awaitOffering(() -> f.store.saves.get() >= 1, () -> dispatcher.offerReconcileFailure(PIPE, 99, null));
            assertThat(f.store.read(PIPE).orElseThrow().failure()).isEqualTo(CAUSE);
            assertThat(f.store.read(PIPE).orElseThrow().facts()).isEmpty();
            assertThat(f.nativeReads).hasValue(0);
            assertThat(f.exported).doesNotContain(PIPE);
            assertThat(f.scopes.current(PIPE)).isEmpty();
        }
    }

    @Test
    void aNewDispatcherRefreshesThePersistedOwnerOnlyAfterTheColdStoreRecovers() throws Exception {
        Fixture f = new Fixture(true);
        Observation old = diagnostic(AT);
        f.store.saved.put(PIPE, new ObservationStore.Stored(old, Optional.empty(), Optional.of(f.receipt.owner())));
        f.store.refreshUnavailable.set(true);
        try (TelemetryDispatcher dispatcher = f.dispatcher()) {
            dispatcher.offerScopeRecovery(PIPE, f.neutralResult(), null, () -> true);
            await(() -> dispatcher.health().get(TelemetryDispatcher.Sink.LATEST).failures() == 1
                    && dispatcher.health().get(TelemetryDispatcher.Sink.LATEST).inFlight() == 0);
            assertThat(f.store.read(PIPE)).contains(old);
            f.store.refreshUnavailable.set(false);
            dispatcher.offerScopeRecovery(PIPE, f.neutralResult(), null, () -> true);
            await(() -> dispatcher.health().get(TelemetryDispatcher.Sink.LATEST).successes() == 1);
            assertThat(f.store.saves).hasValue(0);
            assertThat(f.store.refreshes).hasValue(2);
            assertThat(f.store.read(PIPE).orElseThrow().observedAt()).isAfter(old.observedAt());
            f.assertDiagnosticOnly();
            verifyNoInteractions(f.artifacts, f.generations);
        }
    }

    private static Observation diagnostic(Instant at) {
        return new Observation(PIPE, PipelineState.FAILED, Map.of(), Map.of(), Map.of(), CAUSE, at, List.of());
    }

    private static void await(BooleanSupplier done) throws InterruptedException {
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(5);
        while (!done.getAsBoolean() && System.nanoTime() < deadline) { TimeUnit.MILLISECONDS.sleep(5); }
        assertThat(done.getAsBoolean()).isTrue();
    }

    private static void awaitOffering(BooleanSupplier done, Runnable tick) throws InterruptedException {
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(5);
        while (!done.getAsBoolean() && System.nanoTime() < deadline) {
            tick.run();
            TimeUnit.MILLISECONDS.sleep(5);
        }
        assertThat(done.getAsBoolean()).isTrue();
    }

    private static final class Fixture {
        final InMemoryStateStore state = new InMemoryStateStore();
        final BoundaryStore store = new BoundaryStore();
        final ObservationScopeRegistry scopes = new ObservationScopeRegistry();
        final ArtifactStore artifacts = mock(ArtifactStore.class);
        final ExecutionGenerationStore generations = mock(ExecutionGenerationStore.class);
        final AtomicInteger nativeReads = new AtomicInteger();
        final List<String> exported = new CopyOnWriteArrayList<>();
        final List<PipelineEvent> events = new CopyOnWriteArrayList<>();
        final InMemoryRateHistoryStore history = new InMemoryRateHistoryStore();
        final PreExecutionFailure.Receipt receipt;
        Fixture(boolean historical) {
            state.create(PIPE, StateJson.of(PipelineState.NEW), AT);
            CheckpointDoc original = state.read(PIPE).orElseThrow();
            state.compareAndSwap(PIPE, original.epoch(), StateJson.of(PipelineState.FAILED), AT);
            receipt = new PreExecutionFailure.Attempt(PIPE, "single", "inc-a", original,
                    new DesiredState(PIPE, PipelineState.RUNNING, "revision"), Map.of(PIPE, "a".repeat(64)),
                    historical ? OptionalLong.of(41) : OptionalLong.empty(), null).failed(state.read(PIPE).orElseThrow());
        }
        ConvergeResult neutralResult() {
            return new ConvergeResult(ConvergeStatus.CONVERGED, Optional.of(receipt.checkpoint()), Optional.empty());
        }
        TelemetryDispatcher dispatcher() {
            ObservationPublisher publisher = new ObservationPublisher(state, store,
                    id -> { nativeReads.incrementAndGet(); return OptionalLong.empty(); },
                    id -> { nativeReads.incrementAndGet(); return Map.of(); });
            MetricsExport export = new MetricsExport() {
                @Override public void offer(String id, PipelineState actual, Instant at, List<io.tapstate.core.lifecycle.MetricFact> facts) {
                    exported.add(id);
                }
                @Override public void forgetPipelinesOutside(java.util.Collection<String> ids) { }
            };
            return new TelemetryDispatcher(publisher, new RateSampler(history, Duration.ofSeconds(60)), export, scopes,
                    TelemetryBoundaryDispatchTest.eventStore(events), new ObservationScopeRecovery(artifacts, generations, store, state, "single"),
                    1, 1, Duration.ofSeconds(30));
        }
        void assertDiagnosticOnly() {
            ObservationStore.Stored current = store.readStored(PIPE).orElseThrow();
            assertThat(current.refusal()).contains(receipt.owner());
            assertThat(current.scope()).isEmpty();
            assertThat(current.observation().state()).isEqualTo(PipelineState.FAILED);
            assertThat(current.observation().failure()).isEqualTo(CAUSE);
            assertThat(current.observation().metrics()).isEmpty();
            assertThat(current.observation().positions()).isEmpty();
            assertThat(current.observation().snapshot()).isEmpty();
            assertThat(current.observation().facts()).isEmpty();
            assertThat(nativeReads).hasValue(0);
            assertThat(exported).isEmpty();
            assertThat(events).isEmpty();
            assertThat(history.readPage(PIPE, AT.minusSeconds(60), Instant.now().plusSeconds(60), null, 10).entries()).isEmpty();
            assertThat(scopes.current(PIPE)).isEmpty();
        }
    }

    private static final class BoundaryStore implements ObservationStore {
        final Map<String, Stored> saved = new ConcurrentHashMap<>();
        final AtomicInteger saves = new AtomicInteger(), refreshes = new AtomicInteger();
        final AtomicBoolean blockFirstSave = new AtomicBoolean(), refreshUnavailable = new AtomicBoolean();
        final CountDownLatch entered = new CountDownLatch(1), release = new CountDownLatch(1);
        @Override public void save(Observation observation) { saved.put(observation.pipelineId(), new Stored(observation, Optional.empty())); }
        @Override public boolean saveScoped(Observation observation, Scope scope) {
            saved.put(observation.pipelineId(), new Stored(observation, Optional.of(scope)));
            return true;
        }
        @Override public Optional<Observation> read(String id) { return readStored(id).map(Stored::observation); }
        @Override public Optional<Stored> readStored(String id) { return Optional.ofNullable(saved.get(id)); }
        @Override public void delete(String id) { saved.remove(id); }
        @Override public boolean savePreExecutionFailure(Observation observation, PreExecutionFailure.Receipt receipt) {
            saves.incrementAndGet();
            if (blockFirstSave.compareAndSet(true, false)) {
                entered.countDown();
                try {
                    if (!release.await(5, TimeUnit.SECONDS)) { throw new AssertionError("diagnostic write was not released"); }
                } catch (InterruptedException interrupted) { Thread.currentThread().interrupt(); throw new AssertionError(interrupted); }
                throw new TapstateException(IoError.STORE_UNAVAILABLE, Map.of("detail", "controlled write outage"), null);
            }
            saved.put(observation.pipelineId(), new Stored(observation, Optional.empty(), Optional.of(receipt.owner())));
            return true;
        }
        @Override public boolean refreshPreExecutionFailure(String id, Instant at) {
            refreshes.incrementAndGet();
            if (refreshUnavailable.get()) {
                throw new TapstateException(IoError.STORE_UNAVAILABLE, Map.of("detail", "controlled cold read outage"), null);
            }
            Stored previous = saved.get(id);
            if (previous == null || previous.refusal().isEmpty()) { return false; }
            Observation old = previous.observation();
            saved.put(id, new Stored(new Observation(id, old.state(), old.metrics(), old.snapshot(), old.positions(), old.failure(), at,
                    old.facts()), Optional.empty(), previous.refusal()));
            return true;
        }
    }
}
