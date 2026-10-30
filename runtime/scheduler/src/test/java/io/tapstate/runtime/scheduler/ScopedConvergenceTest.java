package io.tapstate.runtime.scheduler;

import io.tapstate.core.common.TapstateException;
import io.tapstate.core.lifecycle.CasOutcome;
import io.tapstate.core.lifecycle.CheckpointDoc;
import io.tapstate.core.lifecycle.DesiredState;
import io.tapstate.core.lifecycle.Observation;
import io.tapstate.core.lifecycle.ObservationFailure;
import io.tapstate.core.lifecycle.PipelineState;
import io.tapstate.core.lifecycle.StateJson;
import io.tapstate.spi.store.DesiredStore;
import io.tapstate.spi.store.IoError;
import io.tapstate.spi.store.ObservationStore;
import io.tapstate.spi.store.PipelineIntentScope;
import io.tapstate.spi.store.PipelineIntentSnapshot;
import io.tapstate.spi.store.StateStore;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

import java.time.Clock;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class ScopedConvergenceTest {
    private static final Instant T0 = Instant.parse("2026-10-18T00:00:00Z");
    private static final PipelineIntentScope SCOPE = new PipelineIntentScope("cluster", 3, "cache-a", "orders", 1);
    private static final Clock CLOCK = Clock.fixed(T0, ZoneOffset.UTC);
    private final InMemoryStateStore state = new InMemoryStateStore();
    private final RecordingActuator actuator = new RecordingActuator();
    private final PipelineConverger converger = new PipelineConverger(poisonDesired(), poisonState(),
            new RecordingActuator(), CLOCK);
    private final ObservationPublisher publisher = new ObservationPublisher(poisonState(), poisonObservations());

    @Test void aCapturedIntentAndItsBoundPortsSurviveAnEpochRetry() {
        prepare(PipelineState.NEW);
        AtomicBoolean raced = new AtomicBoolean();
        InMemoryDesiredStore live = new InMemoryDesiredStore();
        live.save(new DesiredState("orders", PipelineState.STOPPED, "newer"));
        PipelineConverger withLiveStore = new PipelineConverger(live, poisonState(), new RecordingActuator(), CLOCK);
        state.onBeforeSwap(() -> {
            if (raced.compareAndSet(false, true)) {
                state.applySwap("orders", 0, StateJson.of(PipelineState.PAUSED), T0);
                live.save(new DesiredState("orders", PipelineState.PAUSED, "newest"));
            }
        });
        var result = withLiveStore.convergeScoped(intent(PipelineState.RUNNING), state, actuator, RebuildAdmission.never());
        assertThat(result.status()).isEqualTo(ConvergeStatus.CONVERGED);
        assertThat(state.read("orders").orElseThrow().stateJson()).isEqualTo(StateJson.of(PipelineState.RUNNING));
        assertThat(state.swapAttempts()).isEqualTo(2);
        assertThat(actuator.calls()).containsExactly("resume:orders");
        assertThat(live.read("orders").orElseThrow().targetState()).isEqualTo(PipelineState.PAUSED);
    }

    @Test void aScopeLostDuringAnEpochRaceIsRefusedInsteadOfRebindingOnRetry() {
        prepare(PipelineState.NEW);
        AtomicBoolean current = new AtomicBoolean(true);
        AtomicBoolean raced = new AtomicBoolean();
        StateStore original = guarded(state, () -> { if (!current.get()) throw fenced(); });
        state.onBeforeSwap(() -> {
            if (raced.compareAndSet(false, true)) {
                state.applySwap("orders", 0, StateJson.of(PipelineState.PAUSED), T0);
                current.set(false);
            }
        });
        assertThatThrownBy(() -> converger.convergeScoped(intent(PipelineState.RUNNING), original,
                actuator, RebuildAdmission.never())).isInstanceOfSatisfying(TapstateException.class,
                        failure -> assertThat(failure.code()).isEqualTo(IoError.WORKLOAD_CLAIM_FENCED));
        assertThat(state.swapAttempts()).isEqualTo(1);
        assertThat(state.read("orders").orElseThrow().stateJson()).isEqualTo(StateJson.of(PipelineState.PAUSED));
        assertThat(actuator.calls()).isEmpty();
    }

    @Test void ordinaryScopedConvergenceNeverSeedsAMissingCheckpoint() {
        assertThatThrownBy(() -> converger.convergeScoped(intent(PipelineState.RUNNING), state,
                actuator, RebuildAdmission.never())).isInstanceOfSatisfying(TapstateException.class,
                        failure -> assertThat(failure.code()).isEqualTo(IoError.DOCUMENT_UNREADABLE));
        assertThat(state.read("orders")).isEmpty();
        assertThat(state.swapAttempts()).isZero();
        assertThat(actuator.calls()).isEmpty();
    }

    @Test void aDeletedCapturedIntentDoesNotTouchAnyStateOrActuator() {
        var deleted = new PipelineIntentSnapshot(SCOPE, 4, null);
        assertThat(converger.convergeScoped(deleted, poisonState(), actuator, RebuildAdmission.never()).status())
                .isEqualTo(ConvergeStatus.NOTHING_TO_DO);
        assertThat(actuator.calls()).isEmpty();
    }

    @Test void anExplicitNativeClearingStopIsActuatedEvenWhenTheRetainedRunIsAlreadyStopped() {
        prepare(PipelineState.STOPPED);
        var clearing = new PipelineIntentSnapshot(SCOPE, 3,
                new DesiredState("orders", PipelineState.STOPPED, "original", true));
        converger.convergeScoped(clearing, state, actuator, RebuildAdmission.never());
        assertThat(actuator.calls()).containsExactly("stop:orders:purge");

        // Completing native replacement consumes the clearing flag in the successor CONFIG intent.
        var consumed = new PipelineIntentSnapshot(new PipelineIntentScope("cluster", 3, "cache-a", "orders", 2), 4,
                new DesiredState("orders", PipelineState.STOPPED, "original", false));
        converger.convergeScoped(consumed, state, actuator, RebuildAdmission.never());
        assertThat(actuator.calls()).containsExactly("stop:orders:purge");
    }

    @ParameterizedTest
    @EnumSource(value = PipelineState.class, names = {"STOPPED", "RUNNING", "FAILED"})
    void aClearingStartCannotMistakeAnEarlierStopsStateCasForCompletedReplacement(PipelineState movedState) {
        prepare(PipelineState.RUNNING);
        // START captured epoch zero before the preceding STOP's state write won its CAS.
        var starting = new PipelineIntentSnapshot(SCOPE, 4,
                new DesiredState("orders", PipelineState.RUNNING, "original", true, null, true, 0L));
        state.applySwap("orders", 0, StateJson.of(movedState), T0);

        converger.convergeScoped(starting, state, actuator, RebuildAdmission.never());

        assertThat(actuator.calls()).containsExactly("stop:orders:purge", "start:orders");
    }

    @Test void aLaterStartFailureUsesTheSamePassPortsAfterItsActualAdvance() {
        prepare(PipelineState.NEW);
        AtomicLong passExecution = new AtomicLong();
        List<Long> writtenUnder = new ArrayList<>();
        StateStore commandViews = new StateStore() {
            @Override public Optional<CheckpointDoc> read(String id) { return state.read(id); }
            @Override public CasOutcome compareAndSwap(String id, long epoch, String next, Instant at) {
                writtenUnder.add(passExecution.get());
                return state.compareAndSwap(id, epoch, next, at);
            }
            @Override public void create(String id, String next, Instant at) { throw new AssertionError("no ordinary seed"); }
            @Override public void delete(String id) { throw new AssertionError("no ordinary delete"); }
        };
        LifecycleActuator failingStart = new ForwardingActuator(actuator) {
            @Override public void start(String id) {
                passExecution.set(1);
                throw fenced();
            }
        };
        var result = converger.convergeScoped(intent(PipelineState.RUNNING), commandViews,
                failingStart, RebuildAdmission.never());
        assertThat(result.status()).isEqualTo(ConvergeStatus.FAILED);
        assertThat(writtenUnder).containsExactly(0L, 1L);
        assertThat(state.read("orders").orElseThrow().stateJson()).isEqualTo(StateJson.of(PipelineState.FAILED));
        assertThat(actuator.calls()).containsExactly("stop:orders:keep");
    }

    @Test void scopedFailureRecordingUsesTheSuppliedAdmission() {
        prepare(PipelineState.RUNNING);
        actuator.failWith(fenced());
        List<String> recorded = new ArrayList<>();
        RebuildAdmission bound = new RebuildAdmission() {
            @Override public boolean admits(String id) { return false; }
            @Override public void recordFailure(String id) { recorded.add(id); }
        };
        assertThat(converger.convergeScoped(intent(PipelineState.RUNNING), state, actuator, bound).status())
                .isEqualTo(ConvergeStatus.FAILED);
        assertThat(recorded).containsExactly("orders");
    }

    @Test void normalScopedProjectionUsesOnlyItsBoundStateAndObservationViews() {
        prepare(PipelineState.RUNNING);
        var observations = new MemoryObservations();
        Observation published = publisher.publishScoped(SCOPE, state, observations, null).orElseThrow();
        assertThat(published.state()).isEqualTo(PipelineState.RUNNING);
        assertThat(observations.read("orders")).contains(published);
    }

    @Test void aFailedPassWithoutAProjectionReportsItsPreparedState() {
        prepare(PipelineState.PAUSED);
        var observations = new MemoryObservations();
        publisher.publishReconcileFailureScoped(SCOPE, state, observations, 4);
        Observation published = observations.read("orders").orElseThrow();
        assertThat(published.state()).isEqualTo(PipelineState.PAUSED);
        assertThat(published.metrics()).containsEntry("reconcileFailuresInARow", 4L);
    }

    @Test void aFailedPassPreservesItsLastProjectionReasonAndPositions() {
        prepare(PipelineState.FAILED);
        var observations = new MemoryObservations();
        var reason = new ObservationFailure(IoError.WORKLOAD_CLAIM_FENCED.code(), Map.of());
        Observation previous = new Observation("orders", PipelineState.FAILED, Map.of(), Map.of(), Map.of("items", "p7"), reason, T0);
        observations.save(previous);
        publisher.publishReconcileFailureScoped(SCOPE, state, observations, 2);
        var published = observations.read("orders").orElseThrow();
        assertThat(published.failure()).isEqualTo(reason);
        assertThat(published.positions()).isEqualTo(previous.positions());
        assertThat(published.metrics()).containsEntry("reconcileFailuresInARow", 2L);
    }

    @Test void neitherProjectionPathRepairsOrPublishesOverAMissingCheckpoint() {
        var observations = new MemoryObservations();
        assertThatThrownBy(() -> publisher.publishScoped(SCOPE, state, observations, null))
                .isInstanceOfSatisfying(TapstateException.class,
                        failure -> assertThat(failure.code()).isEqualTo(IoError.DOCUMENT_UNREADABLE));
        assertThatThrownBy(() -> publisher.publishReconcileFailureScoped(SCOPE, state, observations, 1))
                .isInstanceOfSatisfying(TapstateException.class,
                        failure -> assertThat(failure.code()).isEqualTo(IoError.DOCUMENT_UNREADABLE));
        assertThat(observations.read("orders")).isEmpty();
        assertThat(state.read("orders")).isEmpty();
    }

    @Test void aFencedPublicationCannotRetryThroughThePublishersDefaultPorts() {
        prepare(PipelineState.RUNNING);
        var observations = new MemoryObservations();
        observations.beforeSave = () -> { throw fenced(); };
        assertThatThrownBy(() -> publisher.publishScoped(SCOPE, state, observations, null))
                .isInstanceOfSatisfying(TapstateException.class,
                        failure -> assertThat(failure.code()).isEqualTo(IoError.WORKLOAD_CLAIM_FENCED));
        assertThatThrownBy(() -> publisher.publishReconcileFailureScoped(SCOPE, state, observations, 1))
                .isInstanceOfSatisfying(TapstateException.class,
                        failure -> assertThat(failure.code()).isEqualTo(IoError.WORKLOAD_CLAIM_FENCED));
        assertThat(observations.read("orders")).isEmpty();
    }

    private void prepare(PipelineState value) { state.create("orders", StateJson.of(value), T0); }
    private static PipelineIntentSnapshot intent(PipelineState target) {
        return new PipelineIntentSnapshot(SCOPE, 2, new DesiredState("orders", target, "original"));
    }
    private static TapstateException fenced() {
        return new TapstateException(IoError.WORKLOAD_CLAIM_FENCED, Map.of(), null);
    }
    private static StateStore guarded(StateStore target, Runnable guard) {
        return new StateStore() {
            @Override public Optional<CheckpointDoc> read(String id) { guard.run(); return target.read(id); }
            @Override public CasOutcome compareAndSwap(String id, long epoch, String next, Instant at) {
                guard.run(); return target.compareAndSwap(id, epoch, next, at);
            }
            @Override public void create(String id, String next, Instant at) { throw new AssertionError("no ordinary seed"); }
            @Override public void delete(String id) { throw new AssertionError("no ordinary delete"); }
        };
    }
    private static StateStore poisonState() {
        return new StateStore() {
            @Override public Optional<CheckpointDoc> read(String id) { throw new AssertionError("default state read"); }
            @Override public CasOutcome compareAndSwap(String id, long epoch, String next, Instant at) { throw new AssertionError("default state CAS"); }
            @Override public void create(String id, String next, Instant at) { throw new AssertionError("default state seed"); }
            @Override public void delete(String id) { throw new AssertionError("default state delete"); }
        };
    }
    private static DesiredStore poisonDesired() {
        return new DesiredStore() {
            @Override public Optional<DesiredState> read(String id) { throw new AssertionError("live intent resampled"); }
            @Override public void save(DesiredState value) { throw new AssertionError("intent write"); }
            @Override public List<String> pipelineIds() { throw new AssertionError("live intent list"); }
            @Override public void delete(String id) { throw new AssertionError("intent delete"); }
        };
    }
    private static ObservationStore poisonObservations() {
        return new ObservationStore() {
            @Override public void save(Observation value) { throw new AssertionError("default projection write"); }
            @Override public Optional<Observation> read(String id) { throw new AssertionError("default projection read"); }
            @Override public void delete(String id) { throw new AssertionError("default projection delete"); }
        };
    }
    private static final class MemoryObservations implements ObservationStore {
        private final Map<String, Observation> values = new HashMap<>();
        private Runnable beforeSave = () -> {};
        @Override public void save(Observation value) { beforeSave.run(); values.put(value.pipelineId(), value); }
        @Override public Optional<Observation> read(String id) { return Optional.ofNullable(values.get(id)); }
        @Override public void delete(String id) { throw new AssertionError("no ordinary projection delete"); }
    }
    private static class ForwardingActuator implements LifecycleActuator {
        private final RecordingActuator target;
        private ForwardingActuator(RecordingActuator target) { this.target = target; }
        @Override public void start(String id) { target.start(id); }
        @Override public void pause(String id) { target.pause(id); }
        @Override public void resume(String id) { target.resume(id); }
        @Override public void stop(String id, boolean purge) { target.stop(id, purge); }
        @Override public Optional<Throwable> failure(String id) { return target.failure(id); }
        @Override public Optional<Throwable> lost(String id) { return target.lost(id); }
        @Override public boolean isCarryingAJob(String id) { return target.isCarryingAJob(id); }
    }
}
