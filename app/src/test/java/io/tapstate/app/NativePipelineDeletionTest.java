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
    @org.junit.jupiter.params.provider.ValueSource(strings = {"none", "operation", "owner", "intent"})
    void onlyTheConfirmedPurgeSuccessorCanBeSelectedForDeletion(String change) {
        var configuration = mock(MongoConfigurationDomain.class, RETURNS_DEEP_STUBS);
        var cache = mock(MongoCacheDomain.class, RETURNS_DEEP_STUBS);
        var original = new PipelineResource("pipeline", null, List.of(), null, null, null, null, null);
        var hash = CanonicalHash.of(original);
        var old = new PipelineIntentScope("cluster", 1, "cache", "pipeline", 1);
        var purge = new PipelineIntentSnapshot(old, 7, new io.tapstate.core.lifecycle.DesiredState(
                "pipeline", io.tapstate.core.lifecycle.PipelineState.STOPPED, hash, true, hash, false, null));
        var target = new PipelineIntentScope("cluster", 1, "cache", "pipeline", 2);
        var stopped = new PipelineIntentSnapshot(target, change.equals("intent") ? 9 : 8,
                new io.tapstate.core.lifecycle.DesiredState("pipeline", io.tapstate.core.lifecycle.PipelineState.STOPPED, hash));
        var originalNode = mock(WorkloadClaimFence.class);
        @SuppressWarnings("unchecked")
        java.util.function.Supplier<WorkloadClaimFence> owner = mock(java.util.function.Supplier.class);
        when(owner.get()).thenReturn(originalNode, change.equals("owner") ? mock(WorkloadClaimFence.class) : originalNode);
        when(configuration.artifacts().get("pipeline")).thenReturn(Optional.of(original));
        when(configuration.preparations().read("pipeline")).thenReturn(Optional.of(
                new PipelinePreparation("pipeline", 2, PipelinePreparation.Phase.UNASSIGNED, null, null, null)));
        when(configuration.intents().load("pipeline")).thenReturn(Optional.of(purge), Optional.of(stopped));
        when(configuration.pipelineStates().read("pipeline")).thenReturn(
                Optional.of(new PipelineStateMapping("pipeline", 1, PipelineStateMapping.Phase.REBUILDING, PipelineRebuildCoordinator.operationId(purge))),
                Optional.of(new PipelineStateMapping("pipeline", 2, PipelineStateMapping.Phase.ACTIVE, change.equals("operation") ? "another-operation" : PipelineRebuildCoordinator.operationId(purge))));
        var ownership = mock(PipelineActuationOwnership.class);
        var deletion = new NativePipelineDeletion(configuration, cache, new CacheBusinessScope("cluster", 1, "cache"),
                ownership, owner, () -> {}, mock(Engine.class), mock(EngineLifecycleActuator.class),
                mock(HazelcastOperatorReadiness.class), mock(HazelcastExecutionPlans.class), mock(ConnectorProvisioner.class));
        if (change.equals("none")) assertThat(deletion.prepare(original, hash)).isNotNull();
        else assertThatThrownBy(() -> deletion.prepare(original, hash)).isInstanceOf(TapstateException.class);
        verifyNoInteractions(ownership);
    }

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
        when(sources.consumers()).thenReturn(consumers); when(ownership.controlAfterPass(scope,true,java.time.Duration.ofSeconds(5))).thenReturn(control);
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
        verify(ownership).controlAfterPass(scope,true,java.time.Duration.ofSeconds(5));
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
        when(sources.consumers()).thenReturn(mock(SrsConsumerStore.class)); when(ownership.controlAfterPass(any(),anyBoolean(),any())).thenReturn(control);
        doThrow(new TapstateException(IoError.PIPELINE_INTENT_FENCED,Map.of(),null)).when(lifecycle).begin(any());
        var node=new WorkloadClaimFence(new WorkloadClaimKey("cluster",WorkloadClaimType.NODE_SESSION,"node"),new WorkloadOwner("node","boot"),1,0,1);
        var deletion=new NativePipelineDeletion(configuration,cache,new CacheBusinessScope("cluster",1,"cache"),ownership,() -> node,() -> {},
                mock(Engine.class),mock(EngineLifecycleActuator.class),mock(HazelcastOperatorReadiness.class),mock(HazelcastExecutionPlans.class),mock(ConnectorProvisioner.class));
        var command=deletion.prepare(original,CanonicalHash.of(original));
        generation.set(2);
        assertThatThrownBy(command::run).isInstanceOf(TapstateException.class);
        verify(lifecycle,never()).begin(any());
        verify(ownership,never()).controlAfterPass(any(),anyBoolean(),any());
    }
    @org.junit.jupiter.params.ParameterizedTest
    @org.junit.jupiter.params.provider.ValueSource(strings={"generation","definition","preparation","running-intent"})
    void aWaitingDeletionRevalidatesItsOriginalAuthorityBeforeRetirement(String changed) {
        var configuration=mock(MongoConfigurationDomain.class); var cache=mock(MongoCacheDomain.class);
        var artifacts=mock(ArtifactStore.class); var mappings=mock(PipelineStateMappings.class);
        var preparations=mock(PipelineStatePreparations.class); var intents=mock(PipelineIntentStore.class);
        var lifecycle=mock(PipelineRetirements.class); var retirement=mock(PipelineCacheRetirements.class);
        var ownership=mock(PipelineActuationOwnership.class); var control=mock(PipelineActuationOwnership.Control.class);
        var original=new PipelineResource("pipeline",null,List.of(),null,null,null,null,null);
        var scope=new PipelineIntentScope("cluster",1,"cache","pipeline",1);
        var generation=new AtomicLong(1);
        var definition=new java.util.concurrent.atomic.AtomicReference<>(original);
        var preparation=new java.util.concurrent.atomic.AtomicReference<>(new PipelinePreparation(
                "pipeline",1,PipelinePreparation.Phase.UNASSIGNED,null,null,null));
        var intent=new java.util.concurrent.atomic.AtomicReference<PipelineIntentSnapshot>();
        when(configuration.artifacts()).thenReturn(artifacts);
        when(artifacts.get("pipeline")).thenAnswer(call -> Optional.of(definition.get()));
        when(configuration.pipelineStates()).thenReturn(mappings);
        when(mappings.read("pipeline")).thenAnswer(call -> Optional.of(new PipelineStateMapping(
                "pipeline",generation.get(),PipelineStateMapping.Phase.ACTIVE,null)));
        when(configuration.preparations()).thenReturn(preparations);
        when(preparations.read("pipeline")).thenAnswer(call -> Optional.of(preparation.get()));
        when(configuration.intents()).thenReturn(intents);
        when(intents.load("pipeline")).thenAnswer(call -> Optional.ofNullable(intent.get()));
        when(configuration.retirements()).thenReturn(lifecycle);
        when(configuration.requiredNotes()).thenReturn(mock(RequiredNoteStore.class));
        when(configuration.rebuilds()).thenReturn(mock(PipelineStateRebuilds.class));
        when(cache.pipelineRetirements(any(),any())).thenReturn(retirement);
        var srs=mock(SrsStores.class); when(cache.srs(any(),any())).thenReturn(srs);
        when(srs.consumers()).thenReturn(mock(SrsConsumerStore.class));
        when(ownership.controlAfterPass(scope,true,java.time.Duration.ofSeconds(5))).thenAnswer(call -> {
            switch (changed) {
                case "generation" -> generation.set(2);
                case "definition" -> definition.set(new PipelineResource("pipeline",null,List.of(io.tapstate.core.model.SourceRef.bare("another_source")),null,null,null,null,null));
                case "preparation" -> preparation.set(new PipelinePreparation("pipeline",2,PipelinePreparation.Phase.UNASSIGNED,null,null,null));
                case "running-intent" -> intent.set(new PipelineIntentSnapshot(scope,1,
                        new io.tapstate.core.lifecycle.DesiredState("pipeline",io.tapstate.core.lifecycle.PipelineState.RUNNING,CanonicalHash.of(original))));
                default -> throw new AssertionError(changed);
            }
            return control;
        });
        var node=new WorkloadClaimFence(new WorkloadClaimKey("cluster",WorkloadClaimType.NODE_SESSION,"node"),new WorkloadOwner("node","boot"),1,0,1);
        var deletion=new NativePipelineDeletion(configuration,cache,new CacheBusinessScope("cluster",1,"cache"),ownership,() -> node,() -> {},
                mock(Engine.class),mock(EngineLifecycleActuator.class),mock(HazelcastOperatorReadiness.class),
                mock(HazelcastExecutionPlans.class),mock(ConnectorProvisioner.class));
        var command=deletion.prepare(original,CanonicalHash.of(original));
        assertThatThrownBy(command::run).isInstanceOf(TapstateException.class);
        verify(lifecycle,never()).begin(any());
        verify(retirement,never()).retire(any(),any(),any());
        verify(control).close();
    }

}
