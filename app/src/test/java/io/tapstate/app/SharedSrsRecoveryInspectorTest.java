package io.tapstate.app;

import io.tapstate.adapters.mongostore.*;
import io.tapstate.adapters.pdk.ConnectorNoteOrigin;
import io.tapstate.core.common.TapstateException;
import io.tapstate.core.lifecycle.*;
import io.tapstate.core.model.*;
import io.tapstate.core.model.canonical.CanonicalHash;
import io.tapstate.core.model.canonical.SourceCaptureIdentity;
import io.tapstate.spi.store.*;
import java.time.Instant;
import java.util.*;
import org.junit.jupiter.api.Test;
import static org.assertj.core.api.Assertions.*;
import static org.mockito.Mockito.*;

class SharedSrsRecoveryInspectorTest {
    private static final CacheBusinessScope BUSINESS=new CacheBusinessScope("cluster",1,"cache");
    private static final WorkloadOwner OWNER=new WorkloadOwner("node","boot");
    private static final WorkloadClaimFence NODE=new WorkloadClaimFence(new WorkloadClaimKey("cluster",WorkloadClaimType.NODE_SESSION,"node"),OWNER,1,0,1);
    private final MongoConfigurationDomain configuration=mock(MongoConfigurationDomain.class);
    private final MongoCacheDomain cache=mock(MongoCacheDomain.class);
    private final InMemoryStorePort store=new InMemoryStorePort();
    private final ArtifactStore artifacts=mock(ArtifactStore.class,org.mockito.AdditionalAnswers.delegatesTo(store.artifacts()));
    private final CaptureStatePreparations captures=mock(CaptureStatePreparations.class);
    private final PipelineStateMappings mappings=mock(PipelineStateMappings.class);
    private final PipelineStatePreparations preparations=mock(PipelineStatePreparations.class);
    private final PipelineIntentStore intents=mock(PipelineIntentStore.class);
    private final RequiredNoteStore notes=mock(RequiredNoteStore.class);
    private final SrsConsumerStore consumers=mock(SrsConsumerStore.class);
    private final Map<String,StateStore> states=new HashMap<>();
    private final SourceResource input=new SourceResource("input",null,"mongo",Map.of("host","original.invalid"),SourceMode.CDC,List.of(TableRef.literal("orders")),null,null);
    private final SourceResource target=new SourceResource("target",null,"mongo",Map.of("host","target.invalid"),null,null,null,null);
    private final String canonical=SourceCaptureIdentity.resolve(input.connector(),input.config(),null);
    private final CapturePreparation previous=new CapturePreparation(BUSINESS,CapturePreparation.Mode.SHARED_SRS,canonical,
            SourceCaptureIdentity.sharedCapture(canonical),SourceCaptureIdentity.physical(input.connector(),input.config()),UUID.randomUUID().toString());
    private final StorageRecoveryCommands.Selection selection=new StorageRecoveryCommands.Selection(StorageRecoveryCommands.Extent.SHARED_SRS,Set.of(),canonical,canonical);
    private final SharedSrsRecoverySnapshot snapshot;
    private final SharedSrsRecoveryInspector inspector;

    SharedSrsRecoveryInspectorTest() {
        store.artifacts().saveAll(List.of(input,target,pipeline("a"),pipeline("b"),pipeline("c")));
        when(artifacts.managedDefaultSourceId()).thenReturn(Optional.of("target"));when(configuration.artifacts()).thenReturn(artifacts);
        when(configuration.capturePreparations()).thenReturn(captures);when(configuration.pipelineStates()).thenReturn(mappings);
        when(configuration.preparations()).thenReturn(preparations);when(configuration.intents()).thenReturn(intents);
        when(configuration.requiredNotes()).thenReturn(notes);when(notes.guardedBy(any())).thenReturn(notes);
        var token=new RequiredNoteScope(new RequiredNoteOwner(1,"pdk.chain."+canonical,RequiredNoteOwner.Kind.CHAIN,canonical,0),1);
        when(notes.open(token.owner())).thenReturn(token);
        when(notes.origin(token)).thenReturn(ConnectorNoteOrigin.capture(input.connector(),"a".repeat(64),input.config()));
        when(configuration.configurationRevision()).thenReturn(10L);when(captures.read(BUSINESS,previous.captureId())).thenReturn(Optional.of(previous));
        when(cache.srs(eq(BUSINESS),any())).thenReturn(new SrsStores(mock(SrsChainStore.class),mock(SrsModelStore.class),mock(SrsLogStore.class),consumers));
        var members=new ArrayList<SrsConsumerMembership>();var pipelines=new TreeMap<String,SharedSrsRecoverySnapshot.Pipeline>();
        for(String id:List.of("a","b")) {
            var scope=new PipelineIntentScope("cluster",1,"cache",id,1);
            var fence=new WorkloadClaimFence(new WorkloadClaimKey("cluster",WorkloadClaimType.PIPELINE_ACTUATION,id),OWNER,1,1,1);
            pipelines.put(id,new SharedSrsRecoverySnapshot.Pipeline(scope,fence));
            members.add(new SrsConsumerMembership(new SrsConsumerContext(new SrsConsumerKey(canonical,id,"input"),1),1,false,Set.of("orders"),fence));
            when(mappings.read(id)).thenReturn(Optional.of(new PipelineStateMapping(id,1,PipelineStateMapping.Phase.ACTIVE,null)));
            when(preparations.read(id)).thenReturn(Optional.of(new PipelinePreparation(id,1,PipelinePreparation.Phase.COMPLETE,scope,UUID.randomUUID().toString(),NODE)));
            desired(id,PipelineState.STOPPED,false,null);
            var state=mock(StateStore.class);states.put(id,state);when(cache.pipelineStates(scope)).thenReturn(state);
            when(state.read(id)).thenReturn(Optional.of(new CheckpointDoc(id,StateJson.of(PipelineState.STOPPED),10,Instant.EPOCH)));
        }
        snapshot=new SharedSrsRecoverySnapshot(previous,new WorkloadClaimFence(new WorkloadClaimKey("cluster",WorkloadClaimType.CAPTURE,previous.captureId()),OWNER,1,0,1),
                "b".repeat(64),members,pipelines);
        when(consumers.inspectSharedRecovery(previous)).thenReturn(snapshot);
        inspector=new SharedSrsRecoveryInspector(configuration,cache,BUSINESS,() -> {});
    }
    private PipelineResource pipeline(String id) {
        return new PipelineResource(id,null,List.of(SourceRef.spec("input",true)),null,
                new ViewBlock.Inline("view-"+id,FromRef.literal("orders"),"id",null,WriteMode.UPSERT,OnFullLoad.APPEND),null,null,null);
    }
    private void desired(String id,PipelineState desired,boolean reassemble,Long epoch) {
        var scope=new PipelineIntentScope("cluster",1,"cache",id,1);
        when(intents.load(id)).thenReturn(Optional.of(new PipelineIntentSnapshot(scope,2,
                new DesiredState(id,desired,CanonicalHash.of(pipeline(id)),false,null,reassemble,epoch))));
    }

    @Test void actualMembershipIncludesStoppedConsumersAndExcludesAnUnrelatedAuthoredPipeline() {
        var result=inspector.inspect(selection);
        assertThat(result.sharedSrs()).isSameAs(snapshot);assertThat(result.affectedPipelineIds()).containsExactly("a","b");
        assertThat(result.definitionHashes()).containsOnlyKeys("a","b","input","target");
        verify(mappings,never()).read("c");verifyNoInteractionsExceptRead();
    }
    @Test void configurationGuardDoesNotReadAnyCacheBusinessFacet() {
        var manifest=inspector.inspect(selection);clearInvocations(cache,consumers,states.get("a"),states.get("b"));
        inspector.requireConfiguration(manifest);verifyNoInteractions(cache,consumers,states.get("a"),states.get("b"));
        var changed=new SourceResource("target",null,"mongo",Map.of("host","changed.invalid"),null,null,null,null);
        store.artifacts().saveAll(List.of(changed));
        assertThatThrownBy(() -> inspector.requireConfiguration(manifest)).isInstanceOf(TapstateException.class);
        verifyNoInteractions(cache,consumers,states.get("a"),states.get("b"));
    }
    @Test void completionCanConsumeTheOriginalRunButNotANewUnconsumedStart() {
        when(states.get("b").read("b")).thenReturn(Optional.of(new CheckpointDoc("b",StateJson.of(PipelineState.COMPLETED),10,Instant.EPOCH)));
        desired("b",PipelineState.RUNNING,false,null);assertThatCode(() -> inspector.inspect(selection)).doesNotThrowAnyException();
        desired("b",PipelineState.RUNNING,true,9L);
        assertThatThrownBy(() -> inspector.inspect(selection)).isInstanceOf(TapstateException.class);
    }
    @Test void missingPrivateStateOrAChangedPreparationCannotDefaultToNew() {
        when(states.get("b").read("b")).thenReturn(Optional.empty());
        assertThatThrownBy(() -> inspector.inspect(selection)).isInstanceOf(TapstateException.class);
        when(states.get("b").read("b")).thenReturn(Optional.of(new CheckpointDoc("b","STOPPED",10,Instant.EPOCH)));
        when(mappings.read("b")).thenReturn(Optional.of(new PipelineStateMapping("b",2,PipelineStateMapping.Phase.ACTIVE,null)));
        assertThatThrownBy(() -> inspector.inspect(selection)).isInstanceOf(TapstateException.class);verifyNoInteractionsExceptRead();
    }
    @Test void aChangedSourceIdentityCannotReuseTheOriginalSharedNotes() {
        store.artifacts().saveAll(List.of(new SourceResource("input",null,"mongo",Map.of("host","other.invalid"),SourceMode.CDC,input.tables(),null,null)));
        assertThatThrownBy(() -> inspector.inspect(selection)).isInstanceOf(TapstateException.class);verifyNoInteractionsExceptRead();
    }
    @Test void aRevisionChangeDuringThePreviewCannotPublishAMixedDefinitionClosure() {
        when(configuration.configurationRevision()).thenReturn(10L,11L);
        assertThatThrownBy(() -> inspector.inspect(selection)).isInstanceOf(TapstateException.class);verifyNoInteractionsExceptRead();
    }
    private void verifyNoInteractionsExceptRead() {
        verify(notes,never()).initialize(any());verify(notes,never()).prepareOrigin(any(),any());verify(captures,never()).issueFirstCreate(any());
    }
}
