package io.tapstate.app;

import io.tapstate.adapters.mongostore.MongoConfigurationDomain;
import io.tapstate.control.core.StorageSetupService;
import io.tapstate.spi.store.*;
import java.util.*;
import java.util.function.Function;
import org.junit.jupiter.api.Test;
import org.springframework.test.util.ReflectionTestUtils;
import static org.mockito.Mockito.*;

class StorageRecoveryPublicationTest {
    @Test void retryOfCommittedRecoveryReopensTheVerifiedRuntimeAfterItsCompletionReplyWasLost() {
        var scope=new CacheAuthScope("cluster",1,StorageBinding.Mode.OP,UUID.randomUUID().toString(),2);
        var manifest=new StorageRecoveryCommands.Manifest(new StorageRecoveryCommands.Selection(StorageRecoveryCommands.Extent.CLUSTER,Set.of()),
                Set.of(),Map.of(),"a".repeat(64),0);
        var completed=new StorageRecoveryCommands.Plan(UUID.randomUUID().toString(),null,scope,manifest,"b".repeat(64),1,StorageRecoveryCommands.Phase.COMPLETE);
        var root=mock(StorageRoot.class,CALLS_REAL_METHODS);var runtime=mock(StorageRuntime.class);var bound=mock(StorageRuntime.Bound.class);
        var configuration=mock(MongoConfigurationDomain.class);var commands=mock(StorageRecoveryCommands.class);var setup=mock(StorageSetupStateStore.class);
        @SuppressWarnings("unchecked") Function<StorageRuntime.Bound,StorageRuntime.Business> assembly=mock(Function.class);
        ReflectionTestUtils.setField(root,"runtime",runtime);ReflectionTestUtils.setField(root,"installationActive",(Runnable)() -> {});
        ReflectionTestUtils.setField(root,"retainedAssembly",assembly);
        ReflectionTestUtils.setField(root,"retainedTarget",(java.util.function.Supplier<StorageSetupService.DefaultTarget>)() -> mock(StorageSetupService.DefaultTarget.class));
        when(runtime.requireConfiguration()).thenReturn(configuration);when(runtime.current()).thenReturn(Optional.of(bound));when(runtime.business()).thenReturn(Optional.empty());
        when(bound.preparation()).thenReturn(new CachePreparation(UUID.randomUUID().toString(),scope));
        when(configuration.recoveryCommands()).thenReturn(commands);when(commands.find(scope,completed.operationId())).thenReturn(Optional.of(completed));
        when(configuration.setupStates()).thenReturn(setup);when(setup.load()).thenReturn(new StorageSetupStateStore.Status(
                new StorageSetupScope("cluster","installation","setup",1),StorageSetupStateStore.Phase.COMPLETE,1));
        ReflectionTestUtils.invokeMethod(root,"scheduleRecovery",completed);
        verify(runtime).startBusiness(bound,assembly);
        verify(commands,never()).complete(any());
    }
}
