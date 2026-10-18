package io.tapstate.app;

import io.tapstate.core.common.TapstateException;
import io.tapstate.core.lifecycle.CasOutcome;
import io.tapstate.core.lifecycle.CheckpointDoc;
import io.tapstate.core.lifecycle.DesiredState;
import io.tapstate.core.lifecycle.Observation;
import io.tapstate.core.lifecycle.PipelineState;
import io.tapstate.core.lifecycle.StateJson;
import io.tapstate.runtime.scheduler.LifecycleActuator;
import io.tapstate.runtime.scheduler.ObservationPublisher;
import io.tapstate.runtime.scheduler.PipelineConverger;
import io.tapstate.runtime.scheduler.RebuildAdmission;
import io.tapstate.spi.metrics.MetricsExport;
import io.tapstate.spi.store.IoError;
import io.tapstate.spi.store.ObservationStore;
import io.tapstate.spi.store.PipelineIntentScope;
import io.tapstate.spi.store.PipelineIntentSnapshot;
import io.tapstate.spi.store.PipelineIntentStore;
import io.tapstate.spi.store.PipelineStoreViews;
import io.tapstate.spi.store.StateStore;
import io.tapstate.spi.store.WorkloadClaimFence;
import io.tapstate.spi.store.WorkloadClaimKey;
import io.tapstate.spi.store.WorkloadClaimType;
import io.tapstate.spi.store.WorkloadOwner;
import org.junit.jupiter.api.Test;

import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Consumer;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class ScopedConvergenceDriverTest {
    private static final Instant T0 = Instant.parse("2026-10-18T00:00:00Z");
    private static final PipelineIntentScope FIRST = scope(1);
    private static final WorkloadOwner OWNER = new WorkloadOwner("node", "boot");
    private static final WorkloadClaimKey KEY = new WorkloadClaimKey("cluster", WorkloadClaimType.PIPELINE_ACTUATION, "orders");
    private final InMemoryWorkloadClaimStore claims = new InMemoryWorkloadClaimStore();
    private final Intents intents = new Intents();
    private final Views views = new Views();
    private final PipelineActuationOwnership ownership = mock(PipelineActuationOwnership.class);
    private final AtomicBoolean eligible = new AtomicBoolean(true);
    private final AtomicBoolean carrying = new AtomicBoolean();
    private final List<PipelineConvergencePass> passes = new ArrayList<>();
    private final List<List<String>> retained = new ArrayList<>();
    private boolean deny;
    private RuntimeException startFailure;
    private io.tapstate.runtime.scheduler.RateSampler sampler;
    private boolean sampleMetrics;
    private Consumer<PipelineConvergencePass> onStart = this::advance;

    private ConvergenceDriver driver() {
        claims.acquire(KEY, OWNER, 7, Duration.ofMinutes(1));
        when(ownership.permit(any(PipelineIntentScope.class))).thenAnswer(call -> deny
                ? PipelineActuationOwnership.Permit.denied()
                : new PipelineActuationOwnership.Permit(true, claims.read(KEY).orElseThrow().claim()));
        var bindings = new ConvergenceDriver.ScopedPorts(intents, views, () -> {}, pass -> {
            passes.add(pass);
            return new LifecycleActuator() {
                @Override public void start(String id) { onStart.accept(pass); }
                @Override public void pause(String id) { carrying.set(false); }
                @Override public void resume(String id) { carrying.set(true); }
                @Override public void stop(String id, boolean purge) { carrying.set(false); }
                @Override public Optional<Throwable> failure(String id) { return Optional.empty(); }
                @Override public Optional<Throwable> lost(String id) { return Optional.empty(); }
                @Override public boolean isCarryingAJob(String id) { return carrying.get(); }
            };
        }, pass -> RebuildAdmission.never(), ids -> retained.add(List.copyOf(ids)));
        StateStore poisonState = mock(StateStore.class, call -> { throw new AssertionError("unscoped state accessed"); });
        ObservationStore poisonObservations = mock(ObservationStore.class, call -> { throw new AssertionError("unscoped observation accessed"); });
        var poisonDesired = mock(io.tapstate.spi.store.DesiredStore.class,
                call -> { throw new AssertionError("unscoped intent accessed"); });
        LifecycleActuator poisonActuator = mock(LifecycleActuator.class,
                call -> { throw new AssertionError("unscoped actuator accessed"); });
        RebuildAdmission poisonRebuilds = new RebuildAdmission() {
            @Override public boolean admits(String id) { throw new AssertionError("unscoped rebuild admission"); }
            @Override public void recordFailure(String id) { throw new AssertionError("unscoped failure recording"); }
            @Override public void retain(java.util.Collection<String> ids) { throw new AssertionError("unscoped rebuild retention"); }
        };
        var converger = new PipelineConverger(poisonDesired, poisonState, poisonActuator,
                Clock.fixed(T0, ZoneOffset.UTC), poisonRebuilds);
        return ConvergenceDriver.scoped(converger, new ObservationPublisher(poisonState, poisonObservations,
                id -> sampleMetrics ? java.util.OptionalLong.of(5) : java.util.OptionalLong.empty(), id -> Map.of()),
                sampler, MetricsExport.none(), eligible::get, ownership, bindings);
    }

    private void advance(PipelineConvergencePass pass) {
        assertThat(pass.claim()).isEqualTo(WorkloadClaimFence.from(claims.read(KEY).orElseThrow().claim()));
        var actual = claims.advanceExecution(claims.read(KEY).orElseThrow().claim(), 7, Set.of("node")).orElseThrow();
        pass.advanced(WorkloadClaimFence.from(actual));
        if (startFailure != null) throw startFailure;
        carrying.set(true);
    }

    @Test void thePublishedObservationIsSampledOnlyThroughItsSamePassAndActuallyAdvancedClaim() {
        var legacy = new InMemoryRateHistoryStore(); sampler = new io.tapstate.runtime.scheduler.RateSampler(legacy, Duration.ofSeconds(60)); sampleMetrics = true;
        driver().reconcile();
        assertThat(legacy.all()).isEmpty(); assertThat(views.rates.all()).hasSize(1);
        Observation published = views.observations(FIRST).read("orders").orElseThrow();
        assertThat(views.rates.all().getFirst().observedAt()).isEqualTo(published.observedAt());
        assertThat(views.rates.all().getFirst().counters()).containsEntry("recordCount", 5L);
        assertThat(views.writes.stream().filter(w -> w.kind().equals("rate")).map(w -> w.claim().executionGeneration())).containsExactly(1L);
        assertThat(views.requested).containsOnly(FIRST);
    }

    @Test void oneSnapshotAndPermitDriveThePassAndItsActualAdvancedPublication() {
        var driver = driver();
        driver.reconcile();
        assertThat(intents.loads).isEqualTo(1);
        verify(ownership).permit(FIRST);
        verify(ownership, never()).permit("orders");
        assertThat(passes).hasSize(1);
        assertThat(passes.getFirst().scope()).isEqualTo(FIRST);
        assertThat(views.writes.stream().filter(w -> w.kind().equals("state")).map(w -> w.claim().executionGeneration()))
                .containsExactly(0L);
        assertThat(views.writes.stream().filter(w -> w.kind().equals("observation")).map(w -> w.claim().executionGeneration()))
                .containsExactly(1L);
        assertThat(views.observations(FIRST).read("orders").orElseThrow().state()).isEqualTo(PipelineState.RUNNING);
    }

    @Test void aCodedFailureAfterAdvancePublishesUnderThatSameActualClaim() {
        startFailure = fenced();
        driver().reconcile();
        assertThat(intents.loads).isEqualTo(1);
        assertThat(views.writes.stream().filter(w -> w.kind().equals("state")).map(w -> w.claim().executionGeneration()))
                .containsExactly(0L, 1L);
        assertThat(views.writes.stream().filter(w -> w.kind().equals("observation")).map(w -> w.claim().executionGeneration()))
                .containsExactly(1L);
        Observation published = views.observations(FIRST).read("orders").orElseThrow();
        assertThat(published.state()).isEqualTo(PipelineState.FAILED);
        assertThat(published.failure().code()).isEqualTo(IoError.WORKLOAD_CLAIM_FENCED.code());
    }

    @Test void anUncodedFailureAfterAdvanceStillUsesTheSamePassForErrorProjection() {
        startFailure = new IllegalStateException("injected invariant failure");
        driver().reconcile();
        assertThat(intents.loads).isEqualTo(1);
        assertThat(views.writes.stream().filter(w -> w.kind().equals("observation")).map(w -> w.claim().executionGeneration()))
                .containsExactly(1L);
        assertThat(views.observations(FIRST).read("orders").orElseThrow().metrics())
                .containsEntry("reconcileFailuresInARow", 1L);
    }

    @Test void aPhysicalReplacementDuringStartIsNeverAdoptedByFailurePublication() {
        onStart = pass -> {
            advance(pass);
            intents.snapshot = snapshot(scope(2));
            views.current = scope(2);
        };
        driver().reconcile();
        assertThat(intents.loads).isEqualTo(1);
        verify(ownership, never()).permit(scope(2));
        assertThat(passes).hasSize(1);
        assertThat(passes.getFirst().scope()).isEqualTo(FIRST);
        assertThat(views.requested).containsOnly(FIRST);
        assertThat(views.observations(FIRST).read("orders")).isEmpty();
        assertThat(views.observations(scope(2)).read("orders")).isEmpty();
    }

    @Test void aRefusedPermitCannotPublishUnderAnUnscopedView() {
        deny = true;
        driver().reconcile();
        assertThat(intents.loads).isEqualTo(1);
        assertThat(passes).isEmpty();
        assertThat(views.requested).isEmpty();
        assertThat(views.writes).isEmpty();
    }

    @Test void aConfigReadFailureHasNoAuthorityToPublishByPipelineId() {
        intents.loadFailure = fenced();
        driver().reconcile();
        assertThat(intents.loads).isEqualTo(1);
        verify(ownership, never()).permit(any(PipelineIntentScope.class));
        assertThat(passes).isEmpty();
        assertThat(views.requested).isEmpty();
    }

    @Test void aDeletedOrRemovedIntentBetweenListingAndLoadOpensNoPass() {
        var driver = driver();
        intents.snapshot = null;
        driver.reconcile();
        intents.snapshot = new PipelineIntentSnapshot(FIRST, 5, null);
        driver.reconcile();
        assertThat(intents.loads).isEqualTo(2);
        verify(ownership, never()).permit(any(PipelineIntentScope.class));
        assertThat(passes).isEmpty();
        assertThat(views.requested).isEmpty();
    }

    @Test void anIneligibleMemberDoesNotEvenLoadTheIntent() {
        eligible.set(false);
        driver().reconcile();
        assertThat(intents.loads).isZero();
        assertThat(passes).isEmpty();
        assertThat(views.requested).isEmpty();
    }

    @Test void scopedCleanupCannotInvokeTheConvergersDefaultRebuildPolicy() {
        driver().reconcile();
        assertThat(retained).containsExactly(List.of("orders"));
    }

    @Test void cleanupWithoutAnAdmittedPassCannotInvokeTheDefaultRebuildPolicy() {
        deny = true;
        driver().reconcile();
        assertThat(passes).isEmpty();
        assertThat(retained).containsExactly(List.of("orders"));
    }

    @Test void consecutiveFailureCountsDoNotCrossPhysicalStateGenerations() {
        startFailure = new IllegalStateException("injected invariant failure");
        var driver = driver();
        driver.reconcile(); driver.reconcile();
        assertThat(views.observations(FIRST).read("orders").orElseThrow().metrics())
                .containsEntry("reconcileFailuresInARow", 2L);
        assertThat(claims.release(claims.read(KEY).orElseThrow().claim())).isTrue();
        claims.acquire(KEY, OWNER, 7, Duration.ofMinutes(1));
        intents.snapshot = snapshot(scope(2)); views.current = scope(2);
        driver.reconcile();
        assertThat(views.observations(scope(2)).read("orders").orElseThrow().metrics())
                .containsEntry("reconcileFailuresInARow", 1L);
        assertThat(intents.loads).isEqualTo(3);
    }

    private static PipelineIntentScope scope(long generation) { return new PipelineIntentScope("cluster", 3, "cache-a", "orders", generation); }
    private static PipelineIntentSnapshot snapshot(PipelineIntentScope scope) {
        return new PipelineIntentSnapshot(scope, 4, new DesiredState("orders", PipelineState.RUNNING, "original"));
    }
    private static TapstateException fenced() { return new TapstateException(IoError.WORKLOAD_CLAIM_FENCED, Map.of(), null); }

    private static final class Intents implements PipelineIntentStore {
        private PipelineIntentSnapshot snapshot = snapshot(FIRST);
        private RuntimeException loadFailure;
        private int loads;
        @Override public Optional<PipelineIntentSnapshot> load(String id) { throw new AssertionError("authoring load does not prove preparation readiness"); }
        @Override public Optional<PipelineIntentSnapshot> loadForActuation(String id) { loads++; if (loadFailure != null) throw loadFailure; return Optional.ofNullable(snapshot); }
        @Override public List<String> pipelineIds() { return List.of("orders"); }
        @Override public PipelineIntentStore guardedBy(Runnable active) { throw new AssertionError("read-only driver"); }
        @Override public boolean compareAndSet(PipelineIntentScope scope, long revision, DesiredState desired) { throw new AssertionError("read-only driver"); }
        @Override public boolean remove(PipelineIntentScope scope, long revision) { throw new AssertionError("read-only driver"); }
    }
    private record Write(String kind, PipelineIntentScope scope, WorkloadClaimFence claim) {}
    private final class Views implements PipelineStoreViews {
        private PipelineIntentScope current = FIRST;
        private final Map<PipelineIntentScope, InMemoryStateStore> states = new HashMap<>();
        private final Map<PipelineIntentScope, InMemoryObservationStore> projections = new HashMap<>();
        private final InMemoryRateHistoryStore rates = new InMemoryRateHistoryStore();
        private final List<PipelineIntentScope> requested = new ArrayList<>();
        private final List<Write> writes = new ArrayList<>();
        private InMemoryStateStore state(PipelineIntentScope scope) {
            return states.computeIfAbsent(scope, ignored -> { var store = new InMemoryStateStore(); store.create("orders", StateJson.of(PipelineState.NEW), T0); return store; });
        }
        private InMemoryObservationStore observations(PipelineIntentScope scope) {
            return projections.computeIfAbsent(scope, ignored -> new InMemoryObservationStore());
        }
        private void check(PipelineIntentScope scope) { requested.add(scope); if (!current.equals(scope)) throw fenced(); }
        private void check(PipelineIntentScope scope, WorkloadClaimFence original, Runnable active) {
            active.run(); check(scope);
            if (!original.equals(WorkloadClaimFence.from(claims.read(KEY).orElseThrow().claim()))) throw fenced();
        }
        @Override public io.tapstate.spi.store.RateHistoryStore pipelineRateHistory(PipelineIntentScope scope, WorkloadClaimFence original, Runnable active) {
            var writer = mock(io.tapstate.spi.store.RateHistoryStore.class);
            org.mockito.Mockito.doAnswer(call -> { check(scope, original, active); writes.add(new Write("rate", scope, original)); rates.append(call.getArgument(0)); return null; })
                    .when(writer).append(any(io.tapstate.core.lifecycle.RateSample.class)); return writer;
        }
        @Override public StateStore pipelineStates(PipelineIntentScope scope) { check(scope); return state(scope); }
        @Override public StateStore pipelineStates(PipelineIntentScope scope, WorkloadClaimFence original, Runnable active) {
            return new StateStore() {
                @Override public Optional<CheckpointDoc> read(String id) { check(scope); return state(scope).read(id); }
                @Override public CasOutcome compareAndSwap(String id, long epoch, String next, Instant at) {
                    check(scope, original, active); writes.add(new Write("state", scope, original));
                    return state(scope).compareAndSwap(id, epoch, next, at);
                }
                @Override public void create(String id, String next, Instant at) { throw new AssertionError("no ordinary seed"); }
                @Override public void delete(String id) { throw new AssertionError("no ordinary delete"); }
            };
        }
        @Override public ObservationStore pipelineObservations(PipelineIntentScope scope) { check(scope); return observations(scope); }
        @Override public ObservationStore pipelineObservations(PipelineIntentScope scope, WorkloadClaimFence original, Runnable active) {
            return new ObservationStore() {
                @Override public Optional<Observation> read(String id) { check(scope); return observations(scope).read(id); }
                @Override public void save(Observation value) { check(scope, original, active); writes.add(new Write("observation", scope, original)); observations(scope).save(value); }
                @Override public void delete(String id) { throw new AssertionError("no ordinary delete"); }
            };
        }
    }
}
