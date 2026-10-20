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
