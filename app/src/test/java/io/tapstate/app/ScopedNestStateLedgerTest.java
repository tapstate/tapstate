package io.tapstate.app;

import io.tapstate.core.common.TapstateException;
import io.tapstate.core.lifecycle.DesiredState;
import io.tapstate.core.lifecycle.PipelineState;
import io.tapstate.runtime.engine.nest.NestError;
import io.tapstate.runtime.engine.nest.NestStateLedger;
import io.tapstate.spi.store.*;
import org.junit.jupiter.api.Test;
import java.util.*;
import java.util.concurrent.atomic.*;
import static org.assertj.core.api.Assertions.*;

class ScopedNestStateLedgerTest {
    private static final PipelineIntentScope SCOPE = new PipelineIntentScope("cluster",1,"cache","orders",3);
    private static final WorkloadClaimFence ORIGINAL = new WorkloadClaimFence(new WorkloadClaimKey("cluster",WorkloadClaimType.PIPELINE_ACTUATION,"orders"),
            new WorkloadOwner("node","boot"),2,0,1);
    private static WorkloadClaimFence advanced() { return new WorkloadClaimFence(ORIGINAL.key(),ORIGINAL.owner(),2,1,1); }

    @Test void theRealLedgerUsesTheFixedScopeResolvedDatabaseAndOwnAdvancedClaim() {
        var f = new Fixture(); var pass = f.pass(() -> {}); var routes = new HashMap<>(Map.of("nest","extra"));
        f.current.set(advanced()); pass.advanced(advanced());
        var ledger = new ScopedNestStateLedger(pass,"cache",routes); routes.put("nest","changed");
        assertThat(ledger.recall("orders","nest")).isEmpty();
        NestStateLedger.reconcile(ledger,"orders","nest",Set.of("root","root.items"));
        assertThat(ledger.recall("orders","nest")).containsExactlyInAnyOrder("root","root.items");
        assertThat(f.writes).containsExactly(advanced()); assertThat(f.routes).containsOnly("extra"); assertThat(f.scopes).containsOnly(SCOPE);
    }

    @Test void aConcurrentDifferentShapeWinnerIsNeverOverwritten() {
        var f = new Fixture(); var pass = f.pass(() -> {}); var ledger = new ScopedNestStateLedger(pass,"cache",Map.of());
        assertThat(ledger.recall("orders","nest")).isEmpty(); f.shapes.put("cache/nest",Set.of("winner"));
        assertThatThrownBy(() -> ledger.record("orders","nest",Set.of("loser"))).isInstanceOfSatisfying(TapstateException.class,
                failure -> assertThat(failure.code()).isEqualTo(NestError.STATE_PATHS_CHANGED));
        assertThat(f.shapes.get("cache/nest")).containsExactly("winner");
    }

    @Test void evenAnExplicitEmptyShapeWinnerCannotBeSilentlyExpandedInTheSameGeneration() {
        var f = new Fixture(); var ledger = new ScopedNestStateLedger(f.pass(() -> {}),"cache",Map.of());
        ledger.record("orders","nest",Set.of());
        assertThatThrownBy(() -> ledger.record("orders","nest",Set.of("root"))).isInstanceOfSatisfying(TapstateException.class,
                failure -> assertThat(failure.code()).isEqualTo(NestError.STATE_PATHS_CHANGED));
        assertThat(f.shapes.get("cache/nest")).isEmpty();
    }

    @Test void aForeignPipelineCannotReadOrRecordThroughTheLedger() {
        var f = new Fixture(); var ledger = new ScopedNestStateLedger(f.pass(() -> {}),"cache",Map.of());
        code(() -> ledger.recall("another","nest")); code(() -> ledger.record("another","nest",Set.of("root")));
        assertThat(f.scopes).isEmpty(); assertThat(f.writes).isEmpty();
    }

    @Test void aShapeCommandCapturesTheOriginalClaimBeforeTheGuardCanAdvanceIt() {
        var f = new Fixture(); var owner = new AtomicReference<PipelineConvergencePass>(); var once = new AtomicBoolean();
        var pass = f.pass(() -> { if (once.compareAndSet(false,true)) { f.current.set(advanced()); owner.get().advanced(advanced()); } }); owner.set(pass);
        code(() -> new ScopedNestStateLedger(pass,"cache",Map.of()).record("orders","nest",Set.of("root")));
        assertThat(f.writes).containsExactly(ORIGINAL); assertThat(f.shapes).isEmpty();
    }

    @Test void theRealLedgerCapturesItsClaimBeforeIteratingACallbackBearingCandidate() {
        var f = new Fixture(); var pass = f.pass(() -> {}); var once = new AtomicBoolean();
        Set<String> paths = new AbstractSet<>() {
            @Override public int size() { return 1; }
            @Override public Iterator<String> iterator() {
                if (once.compareAndSet(false,true)) { f.current.set(advanced()); pass.advanced(advanced()); }
                return Set.of("root").iterator();
            }
        };
        code(() -> new ScopedNestStateLedger(pass,"cache",Map.of()).record("orders","nest",paths));
        assertThat(f.writes).containsExactly(ORIGINAL); assertThat(f.shapes).isEmpty();
    }

    @Test void aShapeCandidateRemainsFixedWhenTheGuardMutatesTheCallersSet() {
        var f = new Fixture(); var paths = new LinkedHashSet<>(Set.of("root"));
        var pass = f.pass(() -> paths.add("mutated"));
        pass.nestShapes("cache").recordIfAbsent("nest",paths);
        assertThat(f.shapes.get("cache/nest")).containsExactly("root"); assertThat(paths).contains("mutated");
    }

    @Test void aLedgerCannotAdoptANewerPhysicalScope() {
        var f = new Fixture(); var ledger = new ScopedNestStateLedger(f.pass(() -> {}),"cache",Map.of());
        f.scope.set(new PipelineIntentScope("cluster",1,"cache","orders",4));
        code(() -> ledger.record("orders","nest",Set.of("root"))); assertThat(f.shapes).isEmpty();
    }

    private static void code(org.assertj.core.api.ThrowableAssert.ThrowingCallable action) { assertThatThrownBy(action).isInstanceOfSatisfying(TapstateException.class,
            failure -> assertThat(failure.code()).isEqualTo(IoError.WORKLOAD_CLAIM_FENCED)); }
    private static final class Fixture implements PipelineStoreViews {
        final Map<String,Set<String>> shapes = new HashMap<>(); final List<String> routes = new ArrayList<>();
        final List<PipelineIntentScope> scopes = new ArrayList<>(); final List<WorkloadClaimFence> writes = new ArrayList<>();
        final AtomicReference<WorkloadClaimFence> current = new AtomicReference<>(ORIGINAL);
        final AtomicReference<PipelineIntentScope> scope = new AtomicReference<>(SCOPE);
        PipelineConvergencePass pass(Runnable guard) { return new PipelineConvergencePass(new PipelineIntentSnapshot(SCOPE,1,
                new DesiredState("orders",PipelineState.RUNNING,"revision")),ORIGINAL,this,guard); }
        @Override public NestShapeStore nestShapes(PipelineIntentScope expected,String database) {
            scopes.add(expected); routes.add(database); if (!expected.equals(scope.get())) throw fenced();
            return view(database,null,() -> {});
        }
        @Override public NestShapeStore nestShapes(PipelineIntentScope expected,String database,WorkloadClaimFence original,Runnable active) {
            scopes.add(expected); routes.add(database); writes.add(original); if (!expected.equals(scope.get())) throw fenced();
            return view(database,original,active);
        }
        NestShapeStore view(String database,WorkloadClaimFence original,Runnable active) { return new NestShapeStore() {
            @Override public Optional<Set<String>> read(String step) { return Optional.ofNullable(shapes.get(database+"/"+step)); }
            @Override public Optional<Set<String>> recordIfAbsent(String step,Set<String> candidate) {
                active.run(); if (original == null || !original.equals(current.get())) throw fenced();
                return Optional.ofNullable(shapes.putIfAbsent(database+"/"+step,Set.copyOf(candidate)));
            }
        }; }
        @Override public StateStore pipelineStates(PipelineIntentScope scope) { throw new AssertionError("state not used"); }
        @Override public StateStore pipelineStates(PipelineIntentScope scope,WorkloadClaimFence original,Runnable active) { throw new AssertionError("state not used"); }
        @Override public ObservationStore pipelineObservations(PipelineIntentScope scope) { throw new AssertionError("observation not used"); }
        @Override public ObservationStore pipelineObservations(PipelineIntentScope scope,WorkloadClaimFence original,Runnable active) { throw new AssertionError("observation not used"); }
        private static TapstateException fenced() { return new TapstateException(IoError.WORKLOAD_CLAIM_FENCED,Map.of(),null); }
    }
}
