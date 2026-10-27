package io.tapstate.app;

import io.tapstate.adapters.mongostore.MongoCacheDomain;
import io.tapstate.adapters.mongostore.MongoConfigurationDomain;
import io.tapstate.adapters.pdk.ConnectorProvisioner;
import io.tapstate.core.common.TapstateException;
import io.tapstate.core.model.PipelineResource;
import io.tapstate.core.model.canonical.CanonicalHash;
import io.tapstate.runtime.engine.Engine;
import io.tapstate.spi.store.*;
import java.util.*;
import java.util.concurrent.atomic.AtomicLong;
import org.junit.jupiter.api.Test;
import static org.assertj.core.api.Assertions.*;
import static org.mockito.Mockito.*;

class NativePipelineDeletionTest {
    @org.junit.jupiter.params.ParameterizedTest
    @org.junit.jupiter.params.provider.ValueSource(booleans={false,true})
    void anUnpreparedDeletionDoesNotProbeForACacheReceiptThatCannotExist(boolean resume) {
        var configuration=mock(MongoConfigurationDomain.class); var cache=mock(MongoCacheDomain.class);
        var artifacts=mock(ArtifactStore.class); var mappings=mock(PipelineStateMappings.class);
        var preparations=mock(PipelineStatePreparations.class); var lifecycle=mock(PipelineRetirements.class);
        var retirement=mock(PipelineCacheRetirements.class); var sources=mock(SrsStores.class);
        var consumers=mock(SrsConsumerStore.class); var notes=mock(RequiredNoteStore.class);
        var ownership=mock(PipelineActuationOwnership.class); var control=mock(PipelineActuationOwnership.Control.class);
        var operators=mock(HazelcastOperatorReadiness.class);
        var original=new PipelineResource("pipeline",null,List.of(),null,null,null,null,null);
        var scope=new PipelineIntentScope("cluster",1,"cache","pipeline",1);
        var held=new java.util.concurrent.atomic.AtomicReference<PipelineRetirementPreparation>();
        var receipt=mock(PipelineRetirementReceipt.class);
        when(lifecycle.complete(receipt)).thenReturn(mock(PipelineRetirementResult.class));
        when(configuration.artifacts()).thenReturn(artifacts); when(artifacts.get("pipeline")).thenReturn(Optional.of(original));
        when(configuration.pipelineStates()).thenReturn(mappings); when(configuration.preparations()).thenReturn(preparations);
        when(mappings.read("pipeline")).thenAnswer(call -> Optional.of(new PipelineStateMapping("pipeline",1,
                held.get()==null?PipelineStateMapping.Phase.ACTIVE:PipelineStateMapping.Phase.RETIRING,
                held.get()==null?null:held.get().request().operationId())));
        when(preparations.read("pipeline")).thenReturn(Optional.of(new PipelinePreparation("pipeline",1,PipelinePreparation.Phase.UNASSIGNED,null,null,null)));
        when(configuration.intents()).thenReturn(mock(PipelineIntentStore.class)); when(configuration.retirements()).thenReturn(lifecycle);
        when(configuration.requiredNotes()).thenReturn(notes); when(configuration.rebuilds()).thenReturn(mock(PipelineStateRebuilds.class));
        when(configuration.retireValidation(any(),same(cache),any(),any())).thenAnswer(call -> { ((Runnable)call.getArgument(3)).run(); return true; });
        when(cache.pipelineRetirements(any(),any())).thenReturn(retirement); when(cache.srs(any(),any())).thenReturn(sources);
        when(sources.consumers()).thenReturn(consumers); when(ownership.unpreparedControl(scope)).thenReturn(control);
        when(lifecycle.begin(any())).thenAnswer(call -> {
            var preparation=mock(PipelineRetirementPreparation.class);
            when(preparation.request()).thenReturn(call.getArgument(0)); held.set(preparation); return preparation;
        });
        when(lifecycle.pending(eq(scope),anyString())).thenAnswer(call -> Optional.ofNullable(held.get()));
        when(retirement.completed(any(),any())).thenThrow(new AssertionError("no durable receipt can exist for an empty original cache"));
        when(retirement.retire(any(),any(),eq(Optional.empty()))).thenReturn(receipt);
        when(consumers.guardedBy(any())).thenAnswer(call -> { ((Runnable)call.getArgument(0)).run(); return consumers; });
        when(notes.guardedBy(any())).thenAnswer(call -> { ((Runnable)call.getArgument(0)).run(); return notes; });
        var node=new WorkloadClaimFence(new WorkloadClaimKey("cluster",WorkloadClaimType.NODE_SESSION,"node"),new WorkloadOwner("node","boot"),1,0,1);
        var deletion=new NativePipelineDeletion(configuration,cache,new CacheBusinessScope("cluster",1,"cache"),ownership,() -> node,() -> {},
                mock(Engine.class),mock(EngineLifecycleActuator.class),operators,mock(HazelcastExecutionPlans.class),mock(ConnectorProvisioner.class));
        if (resume) {
            var retained=mock(PipelineRetirementPreparation.class);
            when(retained.request()).thenReturn(new PipelineRetirementRequest("original-delete",scope,CanonicalHash.of(original),Optional.empty()));
            held.set(retained);
        }

        deletion.prepare(original,CanonicalHash.of(original)).run();

        verify(retirement).retire(held.get(),node,Optional.empty());
        verify(lifecycle).complete(receipt);
        verify(configuration).retireValidation(eq(held.get()),same(cache),eq(node),any());
        verify(lifecycle,times(resume?0:1)).begin(any());
        verify(operators,atLeastOnce()).requireRetired(eq(scope),isNull(),any());
        verify(control,atLeastOnce()).requireActive();
        verify(control).close();
    }

    @Test void aCommandDelayedByAuditCannotAdoptARecreatedSameHashPipeline() {
        var configuration=mock(MongoConfigurationDomain.class); var cache=mock(MongoCacheDomain.class);
        var artifacts=mock(ArtifactStore.class); var mappings=mock(PipelineStateMappings.class); var preparations=mock(PipelineStatePreparations.class);
        var lifecycle=mock(PipelineRetirements.class); var retirement=mock(PipelineCacheRetirements.class);
        var sources=mock(SrsStores.class); var ownership=mock(PipelineActuationOwnership.class);
        var original=new PipelineResource("pipeline",null,List.of(),null,null,null,null,null);
        var generation=new AtomicLong(1); var control=mock(PipelineActuationOwnership.Control.class);
        when(configuration.artifacts()).thenReturn(artifacts); when(artifacts.get("pipeline")).thenReturn(Optional.of(original));
        when(configuration.pipelineStates()).thenReturn(mappings); when(configuration.preparations()).thenReturn(preparations);
        when(mappings.read("pipeline")).thenAnswer(call -> Optional.of(new PipelineStateMapping("pipeline",generation.get(),PipelineStateMapping.Phase.ACTIVE,null)));
        when(preparations.read("pipeline")).thenAnswer(call -> Optional.of(new PipelinePreparation("pipeline",generation.get(),PipelinePreparation.Phase.UNASSIGNED,null,null,null)));
        when(configuration.intents()).thenReturn(mock(PipelineIntentStore.class)); when(configuration.retirements()).thenReturn(lifecycle);
        when(configuration.requiredNotes()).thenReturn(mock(RequiredNoteStore.class)); when(configuration.rebuilds()).thenReturn(mock(PipelineStateRebuilds.class));
        when(cache.pipelineRetirements(any(),any())).thenReturn(retirement); when(cache.srs(any(),any())).thenReturn(sources);
        when(sources.consumers()).thenReturn(mock(SrsConsumerStore.class)); when(ownership.unpreparedControl(any())).thenReturn(control);
        doThrow(new TapstateException(IoError.PIPELINE_INTENT_FENCED,Map.of(),null)).when(lifecycle).begin(any());
        var node=new WorkloadClaimFence(new WorkloadClaimKey("cluster",WorkloadClaimType.NODE_SESSION,"node"),new WorkloadOwner("node","boot"),1,0,1);
        var deletion=new NativePipelineDeletion(configuration,cache,new CacheBusinessScope("cluster",1,"cache"),ownership,() -> node,() -> {},
                mock(Engine.class),mock(EngineLifecycleActuator.class),mock(HazelcastOperatorReadiness.class),mock(HazelcastExecutionPlans.class),mock(ConnectorProvisioner.class));
        var command=deletion.prepare(original,CanonicalHash.of(original));
        generation.set(2);
        assertThatThrownBy(command::run).isInstanceOf(TapstateException.class);
        verify(lifecycle,never()).begin(any());
        verify(ownership,never()).unpreparedControl(any());
    }
}
