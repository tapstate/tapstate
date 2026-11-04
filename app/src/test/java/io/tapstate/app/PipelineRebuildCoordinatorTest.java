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
        assertThat(releases).hasValue(0);verify(retirement,never()).completeAfterVerifiedRelease();verify(cache,never()).reclaim(any(),any());
    }
    @Test void privateCaptureSelectionConsumesTheClearingStopCarriedByAnImmediateStart() {
        var scope=new PipelineIntentScope("cluster",1,"cache","flow",1);
        var next=new PipelineIntentScope("cluster",1,"cache","flow",2);
        var source=new SourceResource("pg",null,"postgres",Map.of(),SourceMode.CDC,List.of(TableRef.literal("t")),null,null);
        var pipeline=new PipelineResource("flow",null,List.of(SourceRef.spec("pg",false)),null,null,null,null,null);
        var snapshot=mock(ArtifactStore.class);when(snapshot.get("pg")).thenReturn(Optional.of(source));
        var channel=DirectChannelPreparation.of(next,source,ReadMode.SNAPSHOT_AND_CDC,"latest",List.of("t"));
        var inputs=new PipelinePreparationInputs(CanonicalHash.of(pipeline),Map.of("pg",CanonicalHash.of(source)),Map.of("pg",Set.of("t")),List.of(channel));
        var request=mock(PipelineRebuildRequest.class);when(request.inputs()).thenReturn(inputs);
        var carried=new DesiredState("flow",PipelineState.RUNNING,CanonicalHash.of(pipeline),true,null,true,0L);
        when(request.intent()).thenReturn(new PipelineIntentSnapshot(scope,2,carried));
        assertThat(PipelineRebuildCoordinator.clearingDirectPostgres(request,snapshot,pipeline,pipeline))
                .containsOnlyKeys(RequiredNoteOwner.namespace(new PipelineNode("flow","pg"),PipelineNode.Purpose.CAPTURE));
        for(var intent:List.of(new DesiredState("flow",PipelineState.PAUSED,CanonicalHash.of(pipeline),false),
                new DesiredState("flow",PipelineState.STOPPED,CanonicalHash.of(pipeline),false),
                new DesiredState("flow",PipelineState.RUNNING,CanonicalHash.of(pipeline),false))) {
            when(request.intent()).thenReturn(new PipelineIntentSnapshot(scope,3,intent));
            assertThat(PipelineRebuildCoordinator.clearingDirectPostgres(request,snapshot,pipeline,pipeline)).isEmpty();
        }
        when(request.intent()).thenReturn(new PipelineIntentSnapshot(scope,4,new DesiredState("flow",PipelineState.STOPPED,"b".repeat(64),true)));
        assertThatThrownBy(() -> PipelineRebuildCoordinator.clearingDirectPostgres(request,snapshot,pipeline,pipeline))
                .isInstanceOf(io.tapstate.core.common.TapstateException.class);
        var shared=new PipelineResource("flow",null,List.of(SourceRef.spec("pg",true)),null,null,null,null,null);
        assertThat(PipelineRebuildCoordinator.clearingDirectPostgres(request,snapshot,shared,shared)).isEmpty();
    }

    @Test void aSuccessorDirectChannelDoesNotRetireAnOriginalBoundedCapture() {
        var scope=new PipelineIntentScope("cluster",1,"cache","flow",1);var owner=new WorkloadOwner("node","boot");
        var claim=new WorkloadClaimFence(new WorkloadClaimKey("cluster",WorkloadClaimType.PIPELINE_ACTUATION,"flow"),owner,1,1,1);
        var node=new WorkloadClaimFence(new WorkloadClaimKey("cluster",WorkloadClaimType.NODE_SESSION,"node"),owner,1,0,1);
        var source=new SourceResource("pg",null,"postgres",Map.of(),SourceMode.CDC,List.of(TableRef.literal("t")),null,null);
        var pipeline=new PipelineResource("flow",null,List.of(SourceRef.spec("pg",false)),null,null,null,null,null);
        var artifacts=mock(ArtifactStore.class);when(artifacts.list()).thenReturn(List.of(pipeline,source));
        var views=mock(PipelineStoreViews.class);var state=mock(StateStore.class);
        when(views.pipelineStates(scope)).thenReturn(state);when(views.pipelineStates(eq(scope),any(),any())).thenReturn(state);
        when(state.read("flow")).thenReturn(Optional.of(CheckpointDoc.initial("flow","STOPPED",Instant.EPOCH)));
        var desired=new DesiredState("flow",PipelineState.RUNNING,CanonicalHash.of(pipeline),true,null,true,0L);
        var pass=new PipelineConvergencePass(new PipelineIntentSnapshot(scope,2,desired),claim,views,() -> {});
        var configuration=mock(PipelineStateRebuilds.class);var preparation=mock(PipelineRebuildPreparation.class);
        when(configuration.begin(any())).thenAnswer(call -> {when(preparation.request()).thenReturn(call.getArgument(0));return preparation;});
        var consumers=mock(SrsConsumerStore.class);when(consumers.guardedBy(any())).thenReturn(consumers);when(consumers.forPipeline(scope)).thenReturn(List.of());
        var cache=mock(PipelineCacheRebuilds.class);when(cache.prepare(any(),any(),any(),any())).thenReturn(mock(PipelineRebuildReceipt.class));
        var schemas=mock(SchemaStore.class);when(schemas.get("pg")).thenReturn(Optional.of(new DiscoveredSourceModel("pg","postgres",0,new SourceModel(List.of(new SourceTable("t",List.of(),List.of(),List.of()))))));
        var notes=mock(RequiredNoteStore.class);when(notes.guardedBy(any())).thenReturn(notes);
        var token=new RequiredNoteScope(RequiredNoteOwner.forNode(1,new PipelineNode("flow","pg"),PipelineNode.Purpose.CAPTURE,1),1);
        var origin=io.tapstate.adapters.pdk.ConnectorNoteOrigin.capture("postgres","a".repeat(64),Map.of());
        when(notes.privateContexts(scope,"full-"+CanonicalHash.ofText(scope.toString()+":2"))).thenReturn(List.of(new RequiredNoteContext(token,Optional.of(origin),false)));
        var retirement=mock(RequiredNoteRetirement.class);when(retirement.scope()).thenReturn(token);when(retirement.origin()).thenReturn(Optional.of(origin));
        when(notes.retirement(eq(token),anyString())).thenReturn(retirement);
        var privateNotes=new PipelinePrivateNoteRetirement(notes,configuration,(id,settings) -> origin,
                (held,config,purpose,cluster,guard) -> {throw new AssertionError("bounded capture has no original PostgreSQL slot");});
        var coordinator=new PipelineRebuildCoordinator(new PipelineRebuildCoordinator.Stores(artifacts,schemas,views,consumers,configuration,cache),
                privateNotes,() -> node,() -> {},(original,manifest) -> {},Duration.ofSeconds(30),Clock.systemUTC());
        assertThatCode(() -> coordinator.rebuild(pass,Optional.empty())).doesNotThrowAnyException();
        verify(retirement,never()).completeAfterVerifiedRelease();
        verify(cache).prepare(any(),eq(node),any(),any());
    }

}
