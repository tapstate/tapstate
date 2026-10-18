package io.tapstate.app;

import io.tapstate.core.common.TapstateException;
import io.tapstate.core.lifecycle.CasOutcome;
import io.tapstate.core.lifecycle.CheckpointDoc;
import io.tapstate.core.lifecycle.DesiredState;
import io.tapstate.core.lifecycle.Observation;
import io.tapstate.core.lifecycle.PipelineState;
import io.tapstate.core.lifecycle.StateJson;
import io.tapstate.spi.store.IoError;
import io.tapstate.spi.store.ObservationStore;
import io.tapstate.spi.store.PipelineIntentScope;
import io.tapstate.spi.store.PipelineIntentSnapshot;
import io.tapstate.spi.store.PipelineStoreViews;
import io.tapstate.spi.store.StateStore;
import io.tapstate.spi.store.WorkloadClaimFence;
import io.tapstate.spi.store.WorkloadClaimKey;
import io.tapstate.spi.store.WorkloadClaimType;
import io.tapstate.spi.store.WorkloadOwner;
import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class PipelineConvergencePassTest {
    private static final Instant T0 = Instant.parse("2026-10-18T00:00:00Z");
    private static final PipelineIntentScope SCOPE = new PipelineIntentScope("cluster", 3, "cache-a", "orders", 1);
    private static final WorkloadClaimKey KEY = new WorkloadClaimKey("cluster", WorkloadClaimType.PIPELINE_ACTUATION, "orders");
    private static final WorkloadOwner OWNER = new WorkloadOwner("node", "boot");
    private static final WorkloadClaimFence ORIGINAL = new WorkloadClaimFence(KEY, OWNER, 2, 0, 7);
    private static final PipelineIntentSnapshot INTENT = new PipelineIntentSnapshot(SCOPE, 4,
            new DesiredState("orders", PipelineState.RUNNING, "original"));
    private final Views views = new Views();

    @Test void everyCommandUsesTheSameScopeAndTheActualClaimOfThisPass() {
        AtomicInteger checked = new AtomicInteger();
        var pass = new PipelineConvergencePass(INTENT, ORIGINAL, views, checked::incrementAndGet);
        assertThat(pass.intent()).isSameAs(INTENT);
        assertThat(pass.state().read("orders")).isPresent();
        assertThat(pass.state().compareAndSwap("orders", 0, StateJson.of(PipelineState.RUNNING), T0))
                .isInstanceOf(CasOutcome.Applied.class);
        WorkloadClaimFence advanced = execution(1);
        views.actual.set(advanced); pass.advanced(advanced);
        Observation observation = observation("orders");
        pass.observations().save(observation);
        assertThat(pass.observations().read("orders")).contains(observation);
        assertThat(views.writes).containsExactly(ORIGINAL, advanced);
        assertThat(views.requestedScopes).containsOnly(SCOPE);
        assertThat(checked.get()).isGreaterThanOrEqualTo(4);
        assertThat(pass.claim()).isSameAs(advanced);
    }

    @Test void aStateCommandCapturesItsClaimBeforeAnyGuardCallback() {
        AtomicReference<PipelineConvergencePass> owner = new AtomicReference<>();
        AtomicBoolean advanceInGuard = new AtomicBoolean(true);
        var pass = new PipelineConvergencePass(INTENT, ORIGINAL, views, () -> {
            if (advanceInGuard.compareAndSet(true, false)) {
                var next = execution(1); views.actual.set(next); owner.get().advanced(next);
            }
        });
        owner.set(pass);
        assertThatThrownBy(() -> pass.state().compareAndSwap("orders", 0, StateJson.of(PipelineState.RUNNING), T0))
                .isInstanceOfSatisfying(TapstateException.class,
                        failure -> assertThat(failure.code()).isEqualTo(IoError.WORKLOAD_CLAIM_FENCED));
        assertThat(views.writes).containsExactly(ORIGINAL);
        assertThat(views.state.read("orders").orElseThrow().epoch()).isZero();
        assertThat(pass.state().compareAndSwap("orders", 0, StateJson.of(PipelineState.RUNNING), T0))
                .isInstanceOf(CasOutcome.Applied.class);
        assertThat(views.writes).containsExactly(ORIGINAL, execution(1));
    }

    @Test void anObservationCommandAlsoCapturesItsClaimBeforeAnyGuardCallback() {
        AtomicReference<PipelineConvergencePass> owner = new AtomicReference<>();
        AtomicBoolean advanceInGuard = new AtomicBoolean(true);
        var pass = new PipelineConvergencePass(INTENT, ORIGINAL, views, () -> {
            if (advanceInGuard.compareAndSet(true, false)) {
                var next = execution(1); views.actual.set(next); owner.get().advanced(next);
            }
        });
        owner.set(pass);
        assertThatThrownBy(() -> pass.observations().save(observation("orders")))
                .isInstanceOfSatisfying(TapstateException.class,
                        failure -> assertThat(failure.code()).isEqualTo(IoError.WORKLOAD_CLAIM_FENCED));
        assertThat(views.writes).containsExactly(ORIGINAL);
        assertThat(views.observations.read("orders")).isEmpty();
        pass.observations().save(observation("orders"));
        assertThat(views.writes).containsExactly(ORIGINAL, execution(1));
    }

    @Test void aPassNeverAdoptsAReplacementPhysicalScope() {
        var pass = new PipelineConvergencePass(INTENT, ORIGINAL, views, () -> {});
        views.current.set(new PipelineIntentScope("cluster", 3, "cache-a", "orders", 2));
        assertThatThrownBy(() -> pass.state().read("orders")).isInstanceOf(TapstateException.class);
        assertThatThrownBy(() -> pass.observations().save(observation("orders"))).isInstanceOf(TapstateException.class);
        assertThat(views.requestedScopes).containsOnly(SCOPE);
        assertThat(pass.scope()).isEqualTo(SCOPE);
        assertThat(pass.claim()).isSameAs(ORIGINAL);
    }

    @Test void theAdvanceCallbackCannotChangeOwnerTenureTopologyOrPipeline() {
        var pass = new PipelineConvergencePass(INTENT, ORIGINAL, views, () -> {});
        for (WorkloadClaimFence other : List.of(
                new WorkloadClaimFence(KEY, new WorkloadOwner("node", "other"), 2, 1, 7),
                new WorkloadClaimFence(KEY, OWNER, 3, 1, 7),
                new WorkloadClaimFence(KEY, OWNER, 2, 1, 8),
                new WorkloadClaimFence(KEY, OWNER, 2, 2, 7),
                new WorkloadClaimFence(new WorkloadClaimKey("cluster", WorkloadClaimType.PIPELINE_ACTUATION, "other"), OWNER, 2, 1, 7))) {
            assertThatThrownBy(() -> pass.advanced(other)).isInstanceOf(IllegalStateException.class);
            assertThat(pass.claim()).isSameAs(ORIGINAL);
        }
        pass.advanced(execution(1));
        assertThatThrownBy(() -> pass.advanced(execution(1))).isInstanceOf(IllegalStateException.class);
        assertThat(pass.claim()).isEqualTo(execution(1));
    }

    @Test void ordinaryPassViewsCannotCreateDeleteOrWriteAnotherPipeline() {
        var pass = new PipelineConvergencePass(INTENT, ORIGINAL, views, () -> {});
        assertThatThrownBy(() -> pass.state().create("orders", StateJson.of(PipelineState.NEW), T0)).isInstanceOf(TapstateException.class);
        assertThatThrownBy(() -> pass.state().delete("orders")).isInstanceOf(TapstateException.class);
        assertThatThrownBy(() -> pass.observations().delete("orders")).isInstanceOf(TapstateException.class);
        assertThatThrownBy(() -> pass.state().read("other")).isInstanceOf(TapstateException.class);
        assertThatThrownBy(() -> pass.state().compareAndSwap("other", 0, StateJson.of(PipelineState.RUNNING), T0))
                .isInstanceOf(TapstateException.class);
        assertThatThrownBy(() -> pass.observations().save(observation("other"))).isInstanceOf(TapstateException.class);
        assertThat(views.requestedScopes).isEmpty();
    }

    @Test void anInactivePassDoesNotOpenAnyView() {
        var pass = new PipelineConvergencePass(INTENT, ORIGINAL, views, () -> { throw fenced(); });
        assertThatThrownBy(() -> pass.state().read("orders")).isInstanceOf(TapstateException.class);
        assertThatThrownBy(() -> pass.state().compareAndSwap("orders", 0, StateJson.of(PipelineState.RUNNING), T0))
                .isInstanceOf(TapstateException.class);
        assertThatThrownBy(() -> pass.observations().save(observation("orders"))).isInstanceOf(TapstateException.class);
        assertThat(views.requestedScopes).isEmpty();
    }

    @Test void aDeletedIntentOrAnUnrelatedClaimCannotOpenAPass() {
        assertThatThrownBy(() -> new PipelineConvergencePass(new PipelineIntentSnapshot(SCOPE, 5, null),
                ORIGINAL, views, () -> {})).isInstanceOf(IllegalArgumentException.class);
        var other = new WorkloadClaimFence(new WorkloadClaimKey("other", WorkloadClaimType.PIPELINE_ACTUATION, "orders"),
                OWNER, 2, 0, 7);
        assertThatThrownBy(() -> new PipelineConvergencePass(INTENT, other, views, () -> {}))
                .isInstanceOf(IllegalArgumentException.class);
    }

    private static WorkloadClaimFence execution(long generation) { return new WorkloadClaimFence(KEY, OWNER, 2, generation, 7); }
    private static Observation observation(String id) {
        return new Observation(id, PipelineState.RUNNING, Map.of(), Map.of(), Map.of(), null, T0);
    }
    private static TapstateException fenced() { return new TapstateException(IoError.WORKLOAD_CLAIM_FENCED, Map.of(), null); }

    private static final class Views implements PipelineStoreViews {
        private final InMemoryStateStore state = new InMemoryStateStore();
        private final InMemoryObservationStore observations = new InMemoryObservationStore();
        private final AtomicReference<PipelineIntentScope> current = new AtomicReference<>(SCOPE);
        private final AtomicReference<WorkloadClaimFence> actual = new AtomicReference<>(ORIGINAL);
        private final List<PipelineIntentScope> requestedScopes = new ArrayList<>();
        private final List<WorkloadClaimFence> writes = new ArrayList<>();
        private Views() { state.create("orders", StateJson.of(PipelineState.NEW), T0); }
        private void check(PipelineIntentScope scope) {
            requestedScopes.add(scope);
            if (!scope.equals(current.get())) throw fenced();
        }
        private void check(PipelineIntentScope scope, WorkloadClaimFence original, Runnable active) {
            active.run(); check(scope);
            if (!original.equals(actual.get())) throw fenced();
        }
        @Override public StateStore pipelineStates(PipelineIntentScope scope) { check(scope); return state; }
        @Override public StateStore pipelineStates(PipelineIntentScope scope, WorkloadClaimFence original, Runnable active) {
            writes.add(original);
            return new StateStore() {
                @Override public Optional<CheckpointDoc> read(String id) { check(scope); return state.read(id); }
                @Override public CasOutcome compareAndSwap(String id, long epoch, String next, Instant at) {
                    check(scope, original, active); return state.compareAndSwap(id, epoch, next, at);
                }
                @Override public void create(String id, String next, Instant at) { throw new AssertionError("no seed"); }
                @Override public void delete(String id) { throw new AssertionError("no delete"); }
            };
        }
        @Override public ObservationStore pipelineObservations(PipelineIntentScope scope) { check(scope); return observations; }
        @Override public ObservationStore pipelineObservations(PipelineIntentScope scope, WorkloadClaimFence original, Runnable active) {
            writes.add(original);
            return new ObservationStore() {
                @Override public Optional<Observation> read(String id) { check(scope); return observations.read(id); }
                @Override public void save(Observation value) { check(scope, original, active); observations.save(value); }
                @Override public void delete(String id) { throw new AssertionError("no delete"); }
            };
        }
    }
}
