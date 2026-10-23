package io.tapstate.app;

import io.tapstate.adapters.mongostore.MongoConfigurationDomain;
import io.tapstate.adapters.pdk.ConnectorProvisioner;
import io.tapstate.control.core.StorageSetupService;
import io.tapstate.core.lifecycle.*;
import io.tapstate.runtime.engine.Engine;
import io.tapstate.spi.store.*;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;
import org.junit.jupiter.api.Test;
import org.springframework.test.util.ReflectionTestUtils;
import static org.assertj.core.api.Assertions.*;
import static org.mockito.Mockito.*;

class RecoveryContinuationOrderingTest {
    @Test void aStopAfterContinuationWasQueuedCannotBeAdoptedAsItsNewStartRevision() {
        var auth=new CacheAuthScope("cluster",1,StorageBinding.Mode.OP,"new-cache",2);
        var scope=new PipelineIntentScope("cluster",1,"new-cache","p",2);
        var manifest=new StorageRecoveryCommands.Manifest(new StorageRecoveryCommands.Selection(StorageRecoveryCommands.Extent.PIPELINE,Set.of("p")),
                Set.of("p"),Map.of("p","c".repeat(64)),"a".repeat(64),0);
        var plan=new StorageRecoveryCommands.Plan(UUID.randomUUID().toString(),null,auth,manifest,"b".repeat(64),1,StorageRecoveryCommands.Phase.RUNNING);
        var configuration=mock(MongoConfigurationDomain.class);var commands=mock(StorageRecoveryCommands.class);var recoveries=mock(CacheRecoveryStore.class);
        var intents=mock(PipelineIntentStore.class);var notes=mock(RequiredNoteStore.class);var inspector=mock(PipelineRecoveryInspector.class);
        var preparer=mock(PipelineFirstStatePreparer.class);var inputs=mock(PipelinePreparationInputs.class);when(inputs.pipelineHash()).thenReturn("pipeline-hash");
        var permission=mock(CacheRecoveryStore.FullPreparation.class);
        when(permission.recovery()).thenReturn(new PipelineCacheRecovery(UUID.randomUUID().toString(),new PipelineIntentScope("cluster",1,"old-cache","p",1),scope));
        when(configuration.recoveryCommands()).thenReturn(commands);when(commands.find(auth,plan.operationId())).thenReturn(Optional.of(plan));
        when(configuration.cacheRecoveries()).thenReturn(recoveries);when(recoveries.guardedBy(any())).thenReturn(recoveries);when(recoveries.authorizeFull(plan,"p")).thenReturn(permission);
        var rebuilds=mock(PipelineStateRebuilds.class);when(configuration.rebuilds()).thenReturn(rebuilds);
        when(configuration.intents()).thenReturn(intents);when(intents.guardedBy(any())).thenReturn(intents);when(intents.startRecovered(any(),anyLong(),anyString())).thenReturn(true);
        when(configuration.requiredNotes()).thenReturn(notes);when(notes.guardedBy(any())).thenReturn(notes);
        var accepted=new PipelineIntentSnapshot(scope,2,new DesiredState("p",PipelineState.STOPPED,"pipeline-hash"));
        when(intents.load("p")).thenReturn(Optional.of(accepted));
        var recoveryScope=permission.recovery();when(recoveries.read("p")).thenReturn(Optional.of(recoveryScope));
        var executor=new ManualExecutor();var runtime=mock(StorageRuntime.class);var bound=mock(StorageRuntime.Bound.class);var business=mock(NativeBusinessContext.class);
        var recovery=new NativePipelineRecovery(configuration,auth,mock(StorePort.class),preparer,mock(ConnectorProvisioner.class),mock(Engine.class),
                mock(PipelineCaptureCoordinator.class),mock(ExecutionAuthorization.class),() -> {},() -> true,mock(PipelineStoreViews.class));
        ReflectionTestUtils.setField(recovery,"inspector",inspector);
        var root=mock(StorageRoot.class,CALLS_REAL_METHODS);
        ReflectionTestUtils.setField(root,"runtime",runtime);ReflectionTestUtils.setField(root,"installationActive",(Runnable)() -> {});
        ReflectionTestUtils.setField(root,"retainedAssembly",(Function<StorageRuntime.Bound,StorageRuntime.Business>)ignored -> business);
        ReflectionTestUtils.setField(root,"retainedTarget",(java.util.function.Supplier<StorageSetupService.DefaultTarget>)() -> mock(StorageSetupService.DefaultTarget.class));
        ReflectionTestUtils.setField(root,"recoveryWorker",executor);
        when(runtime.current()).thenReturn(Optional.of(bound));when(runtime.requireConfiguration()).thenReturn(configuration);when(runtime.business()).thenReturn(Optional.of(business));
        when(bound.preparation()).thenReturn(new CachePreparation("auth",auth));when(business.pipelineRecovery()).thenReturn(recovery);
        ReflectionTestUtils.invokeMethod(root,"scheduleRecovery",plan);
        when(intents.load("p")).thenReturn(Optional.of(new PipelineIntentSnapshot(scope,3,accepted.desired())));
        when(inspector.inspectContinuation(auth,manifest.selection())).thenReturn(new PipelineRecoveryInspector.Inspection(manifest,inputs,Map.of(),Map.of(),List.of(),3));
        executor.runQueued();
        verify(intents,never()).startRecovered(any(),anyLong(),anyString());
        verify(preparer,never()).prepareRecovered(any(),any(),any());
        assertThat(ReflectionTestUtils.getField(root,"recoveryFailure")).isInstanceOf(io.tapstate.core.common.TapstateException.class);
    }
    private static final class ManualExecutor extends AbstractExecutorService {
        final AtomicReference<Runnable> queued=new AtomicReference<>();
        public void execute(Runnable task){if(!queued.compareAndSet(null,task))throw new AssertionError("already queued");}
        void runQueued(){Objects.requireNonNull(queued.getAndSet(null)).run();}
        public void shutdown(){}public List<Runnable> shutdownNow(){return List.of();}public boolean isShutdown(){return false;}
        public boolean isTerminated(){return false;}public boolean awaitTermination(long timeout,TimeUnit unit){return false;}
    }
}
