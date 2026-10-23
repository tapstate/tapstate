package io.tapstate.app;

import io.tapstate.core.common.TapstateException;
import io.tapstate.runtime.engine.*;
import io.tapstate.spi.store.*;
import java.util.*;
import java.util.concurrent.atomic.*;
import org.junit.jupiter.api.Test;
import static org.assertj.core.api.Assertions.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;

class ScopedRequiredDeadLetterProviderTest {
    private static final PipelineIntentScope SCOPE=new PipelineIntentScope("cluster",1,"cache","pipeline",7);
    private static final WorkloadClaimFence CLAIM=new WorkloadClaimFence(new WorkloadClaimKey("cluster",WorkloadClaimType.PIPELINE_ACTUATION,"pipeline"),new WorkloadOwner("node","boot"),3,4,1);
    private static final ExecutionFence FENCE=new ExecutionFence(SCOPE,CLAIM);
    private static final OperatorPlacement PLACE=OperatorMapCoordinates.nest(SCOPE,"extra","nest",List.of(),OperatorMapCoordinates.NestMap.STATE,null);
    private static OperatorMapBinding binding(){return new OperatorMapBinding(PLACE,FENCE,FENCE.operatorExecutionKey());}
    @Test void configStoreRetainsBothTheOriginalExecutionAndInstalledSlotGuard() {
        var store=mock(RequiredDeadLetterStore.class);var auth=mock(ExecutionAuthorization.class);when(auth.require(FENCE)).thenReturn(CLAIM);var captured=new AtomicReference<Runnable>();
        when(store.guardedBy(any())).thenAnswer(call -> {captured.set(call.getArgument(0));return store;});var active=new AtomicBoolean(true);
        var provider=new ScopedRequiredDeadLetterProvider(store,auth);assertThat(provider.open(binding(),() -> {if(!active.get())throw fenced();})).isSameAs(store);
        captured.get().run();verify(auth,atLeastOnce()).require(FENCE);active.set(false);assertThatThrownBy(() -> captured.get().run()).isInstanceOf(TapstateException.class);
        active.set(true);when(auth.require(FENCE)).thenThrow(fenced());assertThatThrownBy(() -> captured.get().run()).isInstanceOf(TapstateException.class);
    }
    @Test void opaqueOrMismatchedPhysicalCoordinatesNeverOpenConfigState() {
        var store=mock(RequiredDeadLetterStore.class);var auth=mock(ExecutionAuthorization.class);var provider=new ScopedRequiredDeadLetterProvider(store,auth);
        assertThatThrownBy(() -> provider.open(new OperatorMapBinding(PLACE,"opaque","a".repeat(64)),() -> {})).isInstanceOf(TapstateException.class);
        assertThatThrownBy(() -> provider.open(new OperatorMapBinding(PLACE,FENCE,"a".repeat(64)),() -> {})).isInstanceOf(TapstateException.class);
        verifyNoInteractions(store,auth);
    }
    private static TapstateException fenced(){return new TapstateException(IoError.WORKLOAD_CLAIM_FENCED,Map.of(),null);}
}
