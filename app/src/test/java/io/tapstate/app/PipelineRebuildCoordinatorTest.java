package io.tapstate.app;

import io.tapstate.core.lifecycle.*;
import io.tapstate.core.model.*;
import io.tapstate.core.model.canonical.CanonicalHash;
import io.tapstate.spi.store.*;
import java.time.*;
import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.Test;
import static org.assertj.core.api.Assertions.*;
import static org.mockito.Mockito.*;

class PipelineRebuildCoordinatorTest {
    @Test void ordinaryCacheRebuildKeepsNecessaryPrivateIdentityEvenWhenTheNodeIsNoLongerDrawn() {
        var scope=new PipelineIntentScope("cluster",1,"cache","flow",1);var owner=new WorkloadOwner("node","boot");
        var claim=new WorkloadClaimFence(new WorkloadClaimKey("cluster",WorkloadClaimType.PIPELINE_ACTUATION,"flow"),owner,1,1,1);
        var node=new WorkloadClaimFence(new WorkloadClaimKey("cluster",WorkloadClaimType.NODE_SESSION,"node"),owner,1,0,1);
        var pipeline=new PipelineResource("flow",null,List.of(),null,null,null,null,null);
        var artifacts=mock(ArtifactStore.class);when(artifacts.list()).thenReturn(List.of(pipeline));
        var views=mock(PipelineStoreViews.class);var state=mock(StateStore.class);
        when(views.pipelineStates(scope)).thenReturn(state);when(views.pipelineStates(eq(scope),any(),any())).thenReturn(state);
        when(state.read("flow")).thenReturn(Optional.of(CheckpointDoc.initial("flow","STOPPED",Instant.EPOCH)));
        var pass=new PipelineConvergencePass(new PipelineIntentSnapshot(scope,1,new DesiredState("flow",PipelineState.STOPPED,CanonicalHash.of(pipeline),true)),claim,views,() -> {});
        var configuration=mock(PipelineStateRebuilds.class);var preparation=mock(PipelineRebuildPreparation.class);
        when(configuration.begin(any())).thenAnswer(call -> {when(preparation.request()).thenReturn(call.getArgument(0));return preparation;});
        var consumers=mock(SrsConsumerStore.class);when(consumers.guardedBy(any())).thenReturn(consumers);when(consumers.forPipeline(scope)).thenReturn(List.of());
        var cache=mock(PipelineCacheRebuilds.class);when(cache.prepare(any(),any(),any(),any())).thenReturn(mock(PipelineRebuildReceipt.class));
        var notes=mock(RequiredNoteStore.class);when(notes.guardedBy(any())).thenReturn(notes);
        var retirement=mock(RequiredNoteRetirement.class);var token=new RequiredNoteScope(RequiredNoteOwner.forNode(1,new PipelineNode("flow","original-source"),PipelineNode.Purpose.CAPTURE,1),5);
        when(retirement.scope()).thenReturn(token);when(retirement.origin()).thenReturn(Optional.of(new RequiredNoteOrigin("original","a".repeat(64),new byte[]{1})));
        when(notes.retirements(eq(scope),anyString())).thenReturn(List.of(retirement));
        var releases=new AtomicInteger();var privateNotes=new PipelinePrivateNoteRetirement(notes,configuration,(held,config,purpose,cluster,guard) -> {releases.incrementAndGet();return Optional.empty();});
        var coordinator=new PipelineRebuildCoordinator(new PipelineRebuildCoordinator.Stores(artifacts,mock(SchemaStore.class),views,consumers,configuration,cache),
                privateNotes,() -> node,() -> {},(original,manifest) -> {},Duration.ofSeconds(30),Clock.systemUTC());
        coordinator.rebuild(pass,Optional.empty());
        assertThat(releases).hasValue(0);verify(retirement,never()).completeAfterVerifiedRelease();
    }
}
