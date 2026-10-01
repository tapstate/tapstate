package io.tapstate.runtime.scheduler;

import io.tapstate.core.lifecycle.CheckpointDoc;
import io.tapstate.core.lifecycle.DesiredState;
import io.tapstate.core.lifecycle.StateJson;
import io.tapstate.spi.store.StopAuthority;
import io.tapstate.spi.store.StopReservation;
import org.junit.jupiter.api.Test;

import java.time.Clock;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.BooleanSupplier;

import static io.tapstate.core.lifecycle.PipelineState.NEW;
import static io.tapstate.core.lifecycle.PipelineState.PAUSED;
import static io.tapstate.core.lifecycle.PipelineState.RUNNING;
import static io.tapstate.core.lifecycle.PipelineState.STOPPED;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Control-flow tests use an absent-job port; native old-job identity is exercised by the engine binding. */
class DurableStopConvergenceTest {

    private static final Instant AT = Instant.parse("2026-10-01T00:00:00Z");
    private final InMemoryDesiredStore desired = new InMemoryDesiredStore();
    private final InMemoryStateStore state = new InMemoryStateStore();
    private final StopActuator actuator = new StopActuator();

    private PipelineConverger loop() {
        state.enableStops(desired, id -> actuator.authority);
        return new PipelineConverger(desired, state, actuator, Clock.fixed(AT, ZoneOffset.UTC));
    }

    @Test
    void anUnfinishedStopKeepsActualAndOneDurableTokenWithoutStartingAnything() {
        state.create("orders", StateJson.of(RUNNING), AT);
        desired.save(new DesiredState("orders", STOPPED, "rev-1", true));
        PipelineConverger loop = loop();

        ConvergeResult first = loop.converge("orders");
        StopReservation accepted = state.readStopReservation("orders").orElseThrow();
        ConvergeResult retry = loop.converge("orders");

        assertThat(first.status()).isEqualTo(ConvergeStatus.STOP_PENDING);
        assertThat(retry.status()).isEqualTo(ConvergeStatus.STOP_PENDING);
        assertThat(first.transitionFrom()).isEmpty();
        assertThat(state.read("orders").orElseThrow().stateJson()).isEqualTo(StateJson.of(RUNNING));
        assertThat(state.readStopReservation("orders")).contains(accepted);
        assertThat(accepted.sourceEpoch()).isZero();
        assertThat(accepted.reservedEpoch()).isEqualTo(1);
        assertThat(state.stopReservations()).isEqualTo(1);
        assertThat(actuator.firstAttempts.get()).isEqualTo(1);
        assertThat(actuator.finishes.get()).isEqualTo(2);
        assertThat(actuator.starts.get()).isZero();
        assertThat(state.compareAndSwap("orders", accepted.reservedEpoch(), StateJson.of(STOPPED), AT))
                .isInstanceOf(io.tapstate.core.lifecycle.CasOutcome.Fenced.class);
    }

    @Test
    void actualCompletionClearsTheExactMarkerAndOnlyThenPublishesStopped() {
        state.create("orders", StateJson.of(RUNNING), AT);
        desired.save(new DesiredState("orders", STOPPED, "rev-1"));
        PipelineConverger loop = loop();
        loop.converge("orders");
        actuator.over.set(true);

        ConvergeResult done = loop.converge("orders");

        assertThat(done.status()).isEqualTo(ConvergeStatus.CONVERGED);
        assertThat(done.transitionFrom()).contains(RUNNING);
        assertThat(done.checkpoint().orElseThrow().epoch()).isEqualTo(2);
        assertThat(state.readStopReservation("orders")).isEmpty();
        assertThat(state.read("orders").orElseThrow().stateJson()).isEqualTo(StateJson.of(STOPPED));
        loop.converge("orders");
        assertThat(actuator.finishes.get()).isEqualTo(2);
    }

    @Test
    void aCompletionFencedByANewIntentDoesNotRebaseOrStartAReplacement() {
        state.create("orders", StateJson.of(RUNNING), AT);
        DesiredState original = new DesiredState("orders", STOPPED, "rev-1", true);
        desired.save(original);
        PipelineConverger loop = loop();
        loop.converge("orders");
        actuator.over.set(true);
        state.onBeforeComplete(() -> desired.save(new DesiredState("orders", RUNNING, "rev-2")));

        ConvergeResult fenced = loop.converge("orders");

        assertThat(fenced.status()).isEqualTo(ConvergeStatus.SUPERSEDED);
        assertThat(state.read("orders").orElseThrow().stateJson()).isEqualTo(StateJson.of(RUNNING));
        assertThat(state.read("orders").orElseThrow().epoch()).isEqualTo(1);
        assertThat(state.readStopReservation("orders").orElseThrow().originalDesired()).isEqualTo(original);
        assertThat(actuator.starts.get()).isZero();
    }

    @Test
    void aSupersedingStartRetiresOldWorkBeforeOneFreshSubmission() {
        state.create("orders", StateJson.of(RUNNING), AT);
        desired.save(new DesiredState("orders", STOPPED, "rev-1", true));
        PipelineConverger loop = loop();
        loop.converge("orders");
        desired.save(new DesiredState("orders", RUNNING, "rev-2", false, "assembly-2", true, 1L));
        actuator.over.set(true);

        assertThat(loop.converge("orders").status()).isEqualTo(ConvergeStatus.SUPERSEDED);
        assertThat(state.readStopReservation("orders")).isEmpty();
        assertThat(actuator.retiring.get()).isEqualTo(1);
        assertThat(actuator.starts.get()).isZero();
        loop.converge("orders");
        loop.converge("orders");
        assertThat(actuator.starts.get()).isEqualTo(1);
        assertThat(state.stopReservations()).isEqualTo(1);
    }

    @Test
    void aRestartedConvergerUsesTheStoredOriginalStampAndStartsOnceAfterTheOldStop() {
        state.create("orders", StateJson.of(PAUSED), AT);
        DesiredState original = new DesiredState("orders", RUNNING, "rev-2", false, "assembly-2", true, 0L);
        desired.save(original);
        loop().converge("orders");
        StopReservation reserved = state.readStopReservation("orders").orElseThrow();
        assertThat(reserved.originalDesired().rebuiltAtStateEpoch()).isZero();
        assertThat(state.read("orders").orElseThrow().epoch()).isEqualTo(1);
        actuator.over.set(true);
        PipelineConverger restarted = new PipelineConverger(desired, state, actuator, Clock.fixed(AT, ZoneOffset.UTC));

        ConvergeResult resumed = restarted.converge("orders");
        restarted.converge("orders");

        assertThat(resumed.status()).isEqualTo(ConvergeStatus.CONVERGED);
        assertThat(state.read("orders").orElseThrow().stateJson()).isEqualTo(StateJson.of(RUNNING));
        assertThat(state.readStopReservation("orders")).isEmpty();
        assertThat(actuator.continuing.get()).isEqualTo(2);
        assertThat(actuator.firstAttempts.get()).isEqualTo(1);
        assertThat(actuator.starts.get()).isEqualTo(1);
    }

    @Test
    void aMarkerBlocksAnOldCompletionCallbackAndAnUnrelatedPreparedStart() {
        state.create("orders", StateJson.of(RUNNING), AT);
        desired.save(new DesiredState("orders", STOPPED, "rev-1"));
        PipelineConverger loop = loop();
        loop.converge("orders");
        StopReservation marker = state.readStopReservation("orders").orElseThrow();

        assertThat(loop.markCompleted("orders").status()).isEqualTo(ConvergeStatus.SUPERSEDED);
        assertThat(state.readStopReservation("orders")).contains(marker);
        assertThat(state.read("orders").orElseThrow().stateJson()).isEqualTo(StateJson.of(RUNNING));
        assertThat(actuator.starts.get()).isZero();
        assertThat(actuator.finishes.get()).isEqualTo(1);
    }

    @Test
    void aCapableStoreCannotFallBackToASubjectlessLegacyStop() {
        state.create("orders", StateJson.of(NEW), AT);
        desired.save(new DesiredState("orders", STOPPED, "rev-1"));
        state.enableStops(desired, id -> null);
        RecordingActuator legacy = new RecordingActuator();

        assertThatThrownBy(() -> new PipelineConverger(desired, state, legacy, Clock.fixed(AT, ZoneOffset.UTC))
                .converge("orders")).isInstanceOf(IllegalStateException.class);
        assertThat(legacy.calls()).isEmpty();
        assertThat(state.readStopReservation("orders")).isEmpty();
        assertThat(state.read("orders").orElseThrow().stateJson()).isEqualTo(StateJson.of(NEW));
    }

    private static final class StopActuator implements LifecycleActuator {
        private final AtomicBoolean over = new AtomicBoolean();
        private final AtomicInteger starts = new AtomicInteger();
        private final AtomicInteger finishes = new AtomicInteger();
        private final AtomicInteger continuing = new AtomicInteger();
        private final AtomicInteger firstAttempts = new AtomicInteger();
        private final AtomicInteger retiring = new AtomicInteger();
        private boolean carrying;
        private StopAuthority authority;

        @Override public void start(String id) { starts.incrementAndGet(); carrying = true; }
        @Override public void pause(String id) { throw new AssertionError("unexpected pause"); }
        @Override public void resume(String id) { throw new AssertionError("unexpected resume"); }
        @Override public void stop(String id, boolean purge) { throw new AssertionError("legacy stop was bypassed"); }
        @Override public Optional<Throwable> failure(String id) { return Optional.empty(); }
        @Override public boolean isCarryingAJob(String id) { return carrying; }
        @Override public Optional<StopReservation.Subject> stopSubject(String id) {
            return Optional.of(new StopReservation.NoJob("cluster-a", authority));
        }
        @Override public Optional<StopAuthority> stopAuthority(String id) { return Optional.ofNullable(authority); }
        @Override public boolean finishStop(StopReservation reservation, boolean carry, boolean first,
                boolean retire, BooleanSupplier current) {
            finishes.incrementAndGet();
            if (carry) { continuing.incrementAndGet(); }
            if (first) { firstAttempts.incrementAndGet(); }
            if (retire) { retiring.incrementAndGet(); }
            return current.getAsBoolean() && over.get();
        }
    }
}
