package io.tapstate.app;

import io.tapstate.core.common.TapstateException;
import io.tapstate.runtime.engine.*;
import io.tapstate.spi.store.*;
import org.junit.jupiter.api.Test;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.atomic.*;
import static org.assertj.core.api.Assertions.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;

class ScopedOperatorStateProviderTest {
    private static final PipelineIntentScope SCOPE = new PipelineIntentScope("cluster",1,"cache","orders",3);
    private static final CacheBusinessScope BUSINESS = new CacheBusinessScope("cluster",1,"cache");
    private static final WorkloadOwner OWNER = new WorkloadOwner("node","boot");
    private static final WorkloadClaimKey KEY = new WorkloadClaimKey("cluster",WorkloadClaimType.PIPELINE_ACTUATION,"orders");
    private static OperatorPlacement place() { return new OperatorPlacement(SCOPE,"cache",OperatorPlacement.Resource.NEST_STATE,"step","root","state"); }
    private static OperatorMapBinding binding(ExecutionFence fence) { return new OperatorMapBinding(place(),fence,fence.operatorExecutionKey()); }

    @Test void theExecutionKeyIncludesEveryOriginalAuthorityCoordinate() {
        var original = new ExecutionFence(SCOPE,new WorkloadClaimFence(KEY,OWNER,2,3,7));
        var variants = List.of(
            new ExecutionFence(SCOPE,new WorkloadClaimFence(KEY,new WorkloadOwner("other","boot"),2,3,7)),
            new ExecutionFence(SCOPE,new WorkloadClaimFence(KEY,new WorkloadOwner("node","other"),2,3,7)),
            new ExecutionFence(SCOPE,new WorkloadClaimFence(KEY,OWNER,3,3,7)),new ExecutionFence(SCOPE,new WorkloadClaimFence(KEY,OWNER,2,4,7)),
            new ExecutionFence(SCOPE,new WorkloadClaimFence(KEY,OWNER,2,3,8)),
            new ExecutionFence(new PipelineIntentScope("cluster",1,"cache","orders",4),new WorkloadClaimFence(KEY,OWNER,2,3,7)),
            new ExecutionFence(new PipelineIntentScope("cluster",2,"cache","orders",3),new WorkloadClaimFence(KEY,OWNER,2,3,7)),
            new ExecutionFence(new PipelineIntentScope("cluster",1,"other","orders",3),new WorkloadClaimFence(KEY,OWNER,2,3,7)));
        assertThat(variants).extracting(ExecutionFence::operatorExecutionKey).doesNotHaveDuplicates().doesNotContain(original.operatorExecutionKey());
        assertThat(binding(original).mapName()).isNotEqualTo(binding(variants.get(4)).mapName());
        assertThat(binding(original).configurationPattern()).isEqualTo(binding(variants.get(4)).configurationPattern());
    }

    @Test void aProviderCapturesTheExactClaimAndRetainsTheBindingGuard() {
        withFixture(f -> {
            var active = new AtomicBoolean(true);
            var store = f.provider.open(binding(f.fence),() -> { if (!active.get()) throw fenced(); });
            assertThat(store).isSameAs(f.cold); assertThat(f.received.get()).isEqualTo(WorkloadClaimFence.from(f.run));
            f.command.get().run(); active.set(false); code(() -> f.command.get().run(),IoError.WORKLOAD_CLAIM_FENCED);
        });
    }

    @Test void opaqueLegacyWrongScopeAndWrongAttemptKeysNeverReachTheCachePort() {
        withFixture(f -> {
            code(() -> f.provider.open(new OperatorMapBinding(place(),"opaque","0".repeat(64)),() -> {}),IoError.WORKLOAD_CLAIM_FENCED);
            code(() -> f.provider.open(new OperatorMapBinding(place(),new ExecutionFence("orders",1,1),"0".repeat(64)),() -> {}),IoError.WORKLOAD_CLAIM_FENCED);
            code(() -> f.provider.open(new OperatorMapBinding(place(),f.fence,"0".repeat(64)),() -> {}),IoError.WORKLOAD_CLAIM_FENCED);
            var other = new OperatorPlacement(new PipelineIntentScope("cluster",1,"cache","orders",4),"cache",place().resource(),"step","root","state");
            code(() -> f.provider.open(new OperatorMapBinding(other,f.fence,f.fence.operatorExecutionKey()),() -> {}),IoError.WORKLOAD_CLAIM_FENCED);
            verifyNoInteractions(f.stores);
        });
    }

    @Test void anOldStoreCommandCannotUseTheLaterTopologyAfterAnotherBindingIsOpened() {
        withFixture(f -> {
            f.provider.open(binding(f.fence),() -> {}); Runnable old = f.command.get();
            var later = f.raw.acquire(KEY,OWNER,8,Duration.ofSeconds(30)).claim(); var next = new ExecutionFence(SCOPE,WorkloadClaimFence.from(later));
            f.provider.open(binding(next),() -> {});
            assertThat(f.received.get().topologyRevision()).isEqualTo(8);
            code(old::run,EngineError.EXECUTION_NOT_AUTHORIZED);
            code(() -> f.provider.open(binding(f.fence),() -> {}),EngineError.EXECUTION_NOT_AUTHORIZED);
        });
    }

    @Test void nodeLossRefusesBothExistingHandlesAndNewBindingAttempts() {
        withFixture(f -> {
            f.provider.open(binding(f.fence),() -> {}); f.node.set(false);
            code(() -> f.command.get().run(),EngineError.EXECUTION_NOT_AUTHORIZED);
            code(() -> f.provider.open(binding(f.fence),() -> {}),EngineError.EXECUTION_NOT_AUTHORIZED);
            verify(f.stores,times(1)).operatorState(any(),any(),any());
        });
    }

    private static void code(org.assertj.core.api.ThrowableAssert.ThrowingCallable action,io.tapstate.core.common.TapstateErrorCode code) {
        assertThatThrownBy(action).isInstanceOfSatisfying(TapstateException.class,failure -> assertThat(failure.code()).isEqualTo(code));
    }
    private static TapstateException fenced() { return new TapstateException(IoError.WORKLOAD_CLAIM_FENCED,Map.of(),null); }
    private static void withFixture(java.util.function.Consumer<Fixture> action) {
        var raw = new InMemoryWorkloadClaimStore(); var acquired = raw.acquire(KEY,OWNER,7,Duration.ofSeconds(30)).claim();
        var run = raw.advanceExecution(acquired,7,Set.of(OWNER.nodeId())).orElseThrow(); var fence = new ExecutionFence(SCOPE,WorkloadClaimFence.from(run));
        WorkloadClaimStore claims = mock(WorkloadClaimStore.class); when(claims.readPipeline(SCOPE)).thenAnswer(call -> raw.read(KEY));
        var node = new AtomicBoolean(true); var received = new AtomicReference<WorkloadClaimFence>(); var command = new AtomicReference<Runnable>();
        var stores = mock(PipelineStoreViews.class); var cold = mock(KeyedStateStore.class);
        when(stores.operatorState(any(),any(),any())).thenAnswer(call -> { received.set(call.getArgument(1)); command.set(call.getArgument(2)); return cold; });
        try (var authorization = ExecutionAuthorization.scoped(BUSINESS,claims,Duration.ofSeconds(10),System::nanoTime,node::get)) {
            action.accept(new Fixture(raw,run,fence,node,stores,cold,received,command,new ScopedOperatorStateProvider(stores,authorization)));
        }
    }
    private record Fixture(InMemoryWorkloadClaimStore raw,WorkloadClaim run,ExecutionFence fence,AtomicBoolean node,PipelineStoreViews stores,
                           KeyedStateStore cold,AtomicReference<WorkloadClaimFence> received,AtomicReference<Runnable> command,ScopedOperatorStateProvider provider) {}
}
