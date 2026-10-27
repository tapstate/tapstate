package io.tapstate.app;

import io.tapstate.adapters.mongostore.*;
import io.tapstate.core.common.TapstateException;
import io.tapstate.core.event.*;
import io.tapstate.core.model.*;
import io.tapstate.core.model.canonical.SourceCaptureIdentity;
import io.tapstate.spi.store.*;
import java.util.*;
import org.junit.jupiter.api.Test;
import static org.assertj.core.api.Assertions.*;
import static org.mockito.Mockito.*;

class SharedSrsObservationRoutingTest {
    private static final CacheBusinessScope BUSINESS=new CacheBusinessScope("cluster",1,"cache");
    private final MongoConfigurationDomain configuration=mock(MongoConfigurationDomain.class);
    private final MongoCacheDomain cache=mock(MongoCacheDomain.class);
    private final StorageRuntime.Bound bound=mock(StorageRuntime.Bound.class);
    private final PipelineStateMappings mappings=mock(PipelineStateMappings.class);
    private final CaptureStatePreparations captures=mock(CaptureStatePreparations.class);
    private final SrsConsumerStore consumers=mock(SrsConsumerStore.class);
    private final SrsMetaStore meta=mock(SrsMetaStore.class);
    private final SrsChainStore chains=mock(SrsChainStore.class);
    private final WorkloadClaimStore claims=mock(WorkloadClaimStore.class);
    private final InMemoryStorePort memory=new InMemoryStorePort();
    private final SourceResource source=new SourceResource("input",null,"mongo",Map.of("host","original.invalid"),SourceMode.CDC,
            List.of(TableRef.literal("orders")),new Srs("stable",null,null,null,true),null);
    private final PipelineResource pipeline=new PipelineResource("flow",null,List.of(SourceRef.spec("input",true)),null,null,null,null,null);
    private final SourceModel model=new SourceModel(List.of(new SourceTable("orders",List.of(new SourceField("id","BIGINT")),List.of("id"),List.of())));
    private final String canonical="mck-stable",physical="srs-recovery-fixed";
    private final CapturePreparation preparation=new CapturePreparation(BUSINESS,CapturePreparation.Mode.SHARED_SRS,physical,
            SourceCaptureIdentity.sharedCapture(canonical),SourceCaptureIdentity.physical(source.connector(),source.config()),
            UUID.randomUUID().toString(),canonical,UUID.randomUUID().toString());
    private final ThreeLayerStorePort port;
    SharedSrsObservationRoutingTest() {
        memory.artifacts().saveAll(List.of(source,pipeline));memory.schemas().save(new DiscoveredSourceModel("input","mongo",1,model));
        when(configuration.artifacts()).thenReturn(memory.artifacts());when(configuration.pipelineStates()).thenReturn(mappings);
        when(mappings.read("flow")).thenReturn(Optional.of(new PipelineStateMapping("flow",2,PipelineStateMapping.Phase.ACTIVE,null)));
        when(configuration.capturePreparations()).thenReturn(captures);when(captures.read(BUSINESS,preparation.captureId())).thenReturn(Optional.of(preparation));
        when(bound.preparation()).thenReturn(new CachePreparation("auth",new CacheAuthScope("cluster",1,StorageBinding.Mode.OP,"cache",1)));
        when(bound.cache()).thenReturn(cache);when(cache.schemas(eq(BUSINESS),any())).thenReturn(memory.schemas());
        when(cache.srs(eq(BUSINESS),any())).thenReturn(new SrsStores(chains,mock(SrsModelStore.class),mock(SrsLogStore.class),consumers));
        when(cache.workloadClaims(BUSINESS)).thenReturn(claims);
        when(consumers.attached(physical)).thenReturn(topology(2));
        var ack=new ChainPosition(new SourceOrder(2,2),"new-physical-confirmation");
        var offset=new ConsumerOffset(SrsConsumerId.of("flow","input").value(),Map.of("orders",2L),ack,List.of(),null,0L,Map.of("orders",ack),ConsumerProgressKind.SRS);
        when(meta.read(physical)).thenReturn(Optional.of(new SrsMeta(physical,ack,List.of(offset),List.of(),null,2,null,true)));
        when(meta.read(canonical)).thenThrow(new TapstateException(IoError.CAPTURE_STATE_REBUILD_REQUIRED,Map.of("capture",preparation.captureId()),null));
        port=spy(new ThreeLayerStorePort(configuration,bound,ignored -> {throw new AssertionError("observation cannot prepare state");}));
        doReturn(meta).when(port).meta();
    }
    private SrsConsumerTopology topology(long generation) {
        var fence=new WorkloadClaimFence(new WorkloadClaimKey("cluster",WorkloadClaimType.PIPELINE_ACTUATION,"flow"),new WorkloadOwner("node","boot"),1,1,1);
        return new SrsConsumerTopology(BUSINESS,physical,1,List.of(new SrsConsumerMembership(
                new SrsConsumerContext(new SrsConsumerKey(physical,"flow","input"),generation),1,false,Set.of("orders"),fence)));
    }
    @Test void currentObservationReadsOnlyTheExactAttachedReplacementAndNeverTheRetiredCanonicalRoot() {
        assertThat(new StoreBackedSinkPositions(port).apply("flow")).containsOnly(entry("orders","new-physical-confirmation"));
        var chains=new StoreBackedPipelineChains(port);
        assertThat(chains.of("flow")).extracting(io.tapstate.control.core.PipelineChains.Chain::chainId).containsExactly(physical);
        assertThat(chains.progressChainsOf("flow")).containsExactly(physical);
        verify(meta,never()).read(canonical);verify(captures,never()).issueFirstCreate(any());
    }
    @Test void aStoppedOldScopeWithoutAnAttachmentDoesNotInheritTheNewScopesConfirmation() {
        when(consumers.attached(physical)).thenReturn(new SrsConsumerTopology(BUSINESS,physical,0,List.of()));
        assertThat(new StoreBackedSinkPositions(port).apply("flow")).isEmpty();
        assertThat(new StoreBackedPipelineChains(port).progressChainsOf("flow")).isEmpty();verifyNoInteractions(meta);
    }
    @Test void aDifferentStateGenerationCannotReadTheReplacementConsumersAcknowledgement() {
        when(consumers.attached(physical)).thenReturn(topology(1));
        assertThatThrownBy(() -> new StoreBackedSinkPositions(port).apply("flow")).isInstanceOf(TapstateException.class);verifyNoInteractions(meta);
    }
    @Test void aBindingOrScopeChangeDuringTheReadCannotRetargetTheOriginalObservation() {
        when(consumers.attached(physical)).thenAnswer(call -> {
            when(mappings.read("flow")).thenReturn(Optional.of(new PipelineStateMapping("flow",3,PipelineStateMapping.Phase.ACTIVE,null)));
            return topology(2);
        });
        assertThatThrownBy(() -> new StoreBackedSinkPositions(port).apply("flow")).isInstanceOf(TapstateException.class);verifyNoInteractions(meta);
    }
    @Test void aSourceWithDifferentCoordinatesCannotUseTheOldCanonicalAssertionAsAnAlias() {
        var changed=new SourceResource(source.id(),null,source.connector(),Map.of("host","other.invalid"),source.mode(),source.tables(),source.srs(),null);
        memory.artifacts().save(changed);
        assertThatThrownBy(() -> new StoreBackedSinkPositions(port).apply("flow")).isInstanceOf(TapstateException.class);verifyNoInteractions(meta);
    }
    @Test void aNativeDirectScopeDoesNotReadAnotherPipelinesSharedHistoryAsItsOwnProgress() {
        var direct=new PipelineResource("flow",null,List.of(SourceRef.spec("input",false)),null,null,null,null,null);
        memory.artifacts().save(direct);String channel=SourceCaptureResolution.forPipeline(direct,source,model).orElseThrow().chainId().value();
        assertThat(new StoreBackedPipelineChains(port).progressChainsOf("flow")).containsExactly(channel);
        verifyNoInteractions(captures,consumers,meta);
    }
    @Test void missingKnownConfigurationControlCannotEraseTheObservedProgress() {
        when(captures.read(BUSINESS,preparation.captureId())).thenReturn(Optional.empty());
        when(claims.read(any())).thenReturn(Optional.of(mock(WorkloadClaimReading.class)));
        assertThatThrownBy(() -> new StoreBackedSinkPositions(port).apply("flow")).isInstanceOf(TapstateException.class);
    }
    @Test void aKnownRootAlsoRequiresItsConfigurationControlWhenItsCaptureClaimWasLost() {
        when(captures.read(BUSINESS,preparation.captureId())).thenReturn(Optional.empty());
        when(chains.load(canonical)).thenReturn(Optional.of(mock(SrsChainState.class)));
        assertThatThrownBy(() -> new StoreBackedSinkPositions(port).apply("flow")).isInstanceOf(TapstateException.class);verifyNoInteractions(meta);
    }
    @Test void beforeTheFirstCaptureNoPositionIsInventedOrControlCreated() {
        when(captures.read(BUSINESS,preparation.captureId())).thenReturn(Optional.empty());
        assertThat(new StoreBackedSinkPositions(port).apply("flow")).isEmpty();verifyNoInteractions(meta);
        verify(captures,never()).issueFirstCreate(any());verify(claims,never()).acquire(any(),any(),anyLong(),any());
    }
    @Test void aNeverStartedChainIsStillListedWithEmptyPositionsAndNoMetadataRead() {
        when(captures.read(BUSINESS,preparation.captureId())).thenReturn(Optional.empty());
        assertEmptyPosition(canonical);
    }
    @Test void anUnattachedPipelineListsTheTrustedPhysicalDescriptorWithoutBorrowingAnotherReadersProgress() {
        when(consumers.attached(physical)).thenReturn(new SrsConsumerTopology(BUSINESS,physical,0,List.of()));
        assertEmptyPosition(physical);
    }
    private void assertEmptyPosition(String chain) {
        var service=new io.tapstate.control.core.PipelinePositionService(new StoreBackedPipelineChains(port),meta,memory.artifacts(),
                new io.tapstate.control.core.LivePipelines(memory.desired(),memory.state()),mock(io.tapstate.control.core.AuditGate.class));
        assertThat(service.read("flow").chains()).containsExactly(new io.tapstate.control.core.PipelinePosition.Chain(
                chain,"input",List.of("orders"),null,null,null,List.of()));
        verifyNoInteractions(meta);verify(captures,never()).issueFirstCreate(any());
    }
    @Test void aChangeAfterResolutionCannotReturnTheNextGenerationsAck() {
        changeDuringMeta();
        assertThatThrownBy(() -> new StoreBackedSinkPositions(port).apply("flow")).isInstanceOf(TapstateException.class);
    }
    @Test void positionAndStartPlanningKeepTheScopeUntilAllProgressReadsFinish() {
        var service=new io.tapstate.control.core.PipelinePositionService(new StoreBackedPipelineChains(port),meta,memory.artifacts(),
                new io.tapstate.control.core.LivePipelines(memory.desired(),memory.state()),mock(io.tapstate.control.core.AuditGate.class));
        changeDuringMeta();assertThatThrownBy(() -> service.read("flow")).isInstanceOf(TapstateException.class);
        when(mappings.read("flow")).thenReturn(Optional.of(new PipelineStateMapping("flow",2,PipelineStateMapping.Phase.ACTIVE,null)));
        var planner=new io.tapstate.control.core.StartPlanner(new StoreBackedPipelineChains(port),meta,ignored -> List.of());
        assertThatThrownBy(() -> planner.plan(pipeline,Optional.empty(),io.tapstate.control.core.StartIntent.START)).isInstanceOf(TapstateException.class);
    }
    private void changeDuringMeta() {
        var previous=meta.read(physical);
        when(meta.read(physical)).thenAnswer(call -> {
            when(mappings.read("flow")).thenReturn(Optional.of(new PipelineStateMapping("flow",3,PipelineStateMapping.Phase.ACTIVE,null)));
            return previous;
        });
    }
}
