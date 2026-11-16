package io.tapstate.app;

import io.tapstate.adapters.mongostore.*;
import io.tapstate.core.common.TapstateException;
import io.tapstate.core.event.*;
import io.tapstate.core.model.*;
import io.tapstate.core.model.canonical.SourceCaptureIdentity;
import io.tapstate.spi.store.*;
import java.util.*;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
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
        when(consumers.observationCursor(topology(2).consumers().getFirst().context())).thenReturn(Optional.of(offset));
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
    @Test void observingOnePipelineDoesNotReadASiblingConsumersSnapshot() {
        var target = topology(2).consumers().getFirst();
        var siblingContext = new SrsConsumerContext(new SrsConsumerKey(physical,"sibling","input"),1);
        var sibling = new SrsConsumerMembership(siblingContext,1,false,Set.of("orders"),
                new WorkloadClaimFence(new WorkloadClaimKey("cluster",WorkloadClaimType.PIPELINE_ACTUATION,"sibling"),
                        new WorkloadOwner("node","boot"),1,1,1));
        when(consumers.attached(physical)).thenReturn(new SrsConsumerTopology(BUSINESS,physical,2,List.of(target,sibling)));
        var offset = meta.read(physical).orElseThrow().consumerOffset(SrsConsumerId.of("flow","input").value()).orElseThrow();
        when(consumers.observationCursor(target.context())).thenReturn(Optional.of(offset));
        when(consumers.observationCursor(siblingContext)).thenReturn(Optional.of(new ConsumerOffset(
                siblingContext.key().consumerId(),Map.of(),null,List.of(),null,0L)));
        when(chains.load(physical)).thenReturn(Optional.of(new SrsChainState(physical,preparation.captureId(),null,2,
                Set.of("orders"),Set.of("orders"),0,null,null,2,2)));
        doCallRealMethod().when(port).meta();

        assertThat(new StoreBackedSinkPositions(port).apply("flow"))
                .containsOnly(entry("orders","new-physical-confirmation"));
        verify(consumers).observationCursor(target.context());
        verify(consumers,never()).attached(anyString());
        verify(consumers,never()).observationCursor(siblingContext);
    }
    @ParameterizedTest
    @ValueSource(ints = {1, 2})
    void nativeStartPlanningReadsEveryOwnSourceWithoutReadingSiblingCursors(int sourceCount) {
        var sources = new ArrayList<SourceRef>();
        var members = new ArrayList<SrsConsumerMembership>();
        var own = new ArrayList<SrsConsumerContext>();
        var values = new ArrayList<SrsConsumerProgress>();
        for (int index = 0; index < sourceCount; index++) {
            String id = index == 0 ? source.id() : "second-input";
            var selected = new SourceResource(id,null,source.connector(),source.config(),source.mode(),source.tables(),source.srs(),null);
            memory.artifacts().save(selected);
            memory.schemas().save(new DiscoveredSourceModel(id,source.connector(),1,model));
            sources.add(SourceRef.spec(id,true));
            var context = new SrsConsumerContext(new SrsConsumerKey(physical,pipeline.id(),id),2);
            own.add(context);
            members.add(new SrsConsumerMembership(context,1,false,Set.of("orders"),topology(2).consumers().getFirst().commandFence()));
            var offset = new ConsumerOffset(context.key().consumerId(),Map.of("orders",2L),null,List.of(),null,0L,Map.of(),ConsumerProgressKind.SRS);
            when(consumers.cursor(context)).thenReturn(Optional.of(offset));
            values.add(new SrsConsumerProgress(context,1,0,offset,null,null,Map.of()));
        }
        var selectedPipeline = new PipelineResource(pipeline.id(),null,sources,null,null,null,null,null);
        memory.artifacts().save(selectedPipeline);
        var sibling = new SrsConsumerContext(new SrsConsumerKey(physical,"sibling",source.id()),1);
        members.add(new SrsConsumerMembership(sibling,1,false,Set.of("orders"),
                new WorkloadClaimFence(new WorkloadClaimKey("cluster",WorkloadClaimType.PIPELINE_ACTUATION,"sibling"),
                        new WorkloadOwner("node","boot"),1,1,1)));
        when(consumers.attached(physical)).thenReturn(new SrsConsumerTopology(BUSINESS,physical,3,members));
        when(consumers.cursor(sibling)).thenThrow(new AssertionError("start planning must not load another pipeline's private cursor"));
        var state = new SrsChainState(physical,preparation.captureId(),null,2,
                Set.of("orders"),Set.of("orders"),0,null,null,members.size(),3);
        when(chains.load(physical)).thenReturn(Optional.of(state));
        var scope = new PipelineIntentScope(BUSINESS.clusterId(),BUSINESS.bindingVersion(),BUSINESS.cacheIncarnation(),pipeline.id(),2);
        var ids = own.stream().map(context -> context.key().sourceId()).collect(java.util.stream.Collectors.toSet());
        when(consumers.planningProgress(scope,physical,ids)).thenReturn(Optional.of(new SrsConsumerStore.PlanningProgress(state,values)));
        when(cache.startPlanningSrs(eq(BUSINESS),any())).thenReturn(new SrsStores(chains,mock(SrsModelStore.class),mock(SrsLogStore.class),consumers));

        var reads = new StoreBackedPipelineWriteTargets(port).capture(port,new StoreBackedPipelineChains(port),selectedPipeline,Optional.empty());
        assertThat(reads.progressChains().get()).containsExactly(physical);
        var progress = reads.progress().apply(physical).orElseThrow();
        reads.verify().run();
        assertThat(progress.consumerOffsets()).extracting(ConsumerOffset::pipelineId)
                .containsExactlyInAnyOrderElementsOf(own.stream().map(context -> context.key().consumerId()).toList());
        assertThat(StartLoad.of(null,pipeline.id(),List.of(progress))).isEqualTo(StartLoad.RESUME);
        verify(consumers,times(1)).planningProgress(scope,physical,ids);
        verify(consumers,never()).progress(any());
        verify(consumers,never()).cursor(sibling);
        verify(consumers,never()).attached(physical);
    }
    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    void nativeStartPlanningTreatsOnlyAnOlderDetachedMembershipAsAbsent(boolean detached) {
        var old = topology(1).consumers().getFirst();
        var marker = new SrsConsumerMembership(old.context(),old.revision(),detached,
                detached ? Set.of() : old.tables(),old.commandFence());
        when(consumers.attached(physical)).thenReturn(new SrsConsumerTopology(BUSINESS,physical,2,
                detached ? List.of() : List.of(marker)));
        var scope = new PipelineIntentScope(BUSINESS.clusterId(),BUSINESS.bindingVersion(),BUSINESS.cacheIncarnation(),pipeline.id(),2);
        if (detached) when(consumers.planningProgress(scope,physical,Set.of(source.id())))
                .thenReturn(Optional.of(new SrsConsumerStore.PlanningProgress(new SrsChainState(physical,preparation.captureId(),null,2,
                        Set.of("orders"),Set.of("orders"),0,null,null,0,2),List.of())));
        else when(consumers.planningProgress(scope,physical,Set.of(source.id())))
                .thenThrow(new TapstateException(IoError.SRS_CONSUMER_UNAVAILABLE,Map.of("chain",physical),null));
        when(cache.startPlanningSrs(eq(BUSINESS),any())).thenReturn(new SrsStores(chains,mock(SrsModelStore.class),mock(SrsLogStore.class),consumers));
        var reads = new StoreBackedPipelineWriteTargets(port).capture(port,new StoreBackedPipelineChains(port),pipeline,Optional.empty());
        if (detached) assertThat(reads.progressChains().get()).isEmpty();
        else assertThatThrownBy(() -> reads.progressChains().get()).isInstanceOf(TapstateException.class);
    }

    @ParameterizedTest @ValueSource(strings={"snapshot","direct"})
    void nativePrivateModesUseTheSameOwnOnlyPlanningSnapshot(String mode) {
        boolean snapshot=mode.equals("snapshot");
        var selected=new PipelineResource(pipeline.id(),null,List.of(SourceRef.spec(source.id(),snapshot)),null,null,null,
                snapshot ? new Settings(null,null,null,null,ReadMode.SNAPSHOT_ONLY,null) : null,null);
        memory.artifacts().save(selected);
        String chain=SourceCaptureResolution.forPipeline(selected,source,model).orElseThrow().chainId().value();
        var context=new SrsConsumerContext(new SrsConsumerKey(chain,pipeline.id(),source.id()),2);
        var offset=new ConsumerOffset(context.key().consumerId(),Map.of("orders",2L),null,List.of(),null,0L,Map.of(),ConsumerProgressKind.SRS);
        var progress=new SrsConsumerProgress(context,1,0,offset,null);
        var state=new SrsChainState(chain,"original-capture",null,2,Set.of("orders"),Set.of("orders"),0,null,null,1,1);
        var scope=new PipelineIntentScope(BUSINESS.clusterId(),BUSINESS.bindingVersion(),BUSINESS.cacheIncarnation(),pipeline.id(),2);
        when(consumers.planningProgress(scope,chain,Set.of(source.id())))
                .thenReturn(Optional.of(new SrsConsumerStore.PlanningProgress(state,List.of(progress))));
        when(cache.startPlanningSrs(eq(BUSINESS),any())).thenReturn(new SrsStores(chains,mock(SrsModelStore.class),mock(SrsLogStore.class),consumers));
        when(consumers.attached(chain)).thenThrow(new AssertionError("private-mode planning must not enumerate peer payloads"));
        var reads=new StoreBackedPipelineWriteTargets(port).capture(port,new StoreBackedPipelineChains(port),selected,Optional.empty());
        assertThat(reads.progressChains().get()).containsExactly(chain);
        assertThat(StartLoad.of(null,pipeline.id(),List.of(reads.progress().apply(chain).orElseThrow()))).isEqualTo(StartLoad.RESUME);
        reads.verify().run();
        verify(consumers,times(1)).planningProgress(scope,chain,Set.of(source.id()));
        verify(consumers,never()).attached(chain); verifyNoInteractions(captures);
    }

    @Test void nativeStartPlanningGroupsOnlySourcesOnTheSamePhysicalChain() {
        var second=new SourceResource("second-input",null,source.connector(),source.config(),source.mode(),source.tables(),
                new Srs("second",null,null,null,true),null);
        var selected=new PipelineResource(pipeline.id(),null,List.of(SourceRef.spec(source.id(),true),SourceRef.spec(second.id(),true)),null,null,null,null,null);
        memory.artifacts().saveAll(List.of(second,selected));
        memory.schemas().save(new DiscoveredSourceModel(second.id(),second.connector(),1,model));
        String canonicalSecond=SourceCaptureResolution.forPipeline(selected,second,model).orElseThrow().chainId().value();
        var preparedSecond=new CapturePreparation(BUSINESS,CapturePreparation.Mode.SHARED_SRS,"second-physical",
                SourceCaptureIdentity.sharedCapture(canonicalSecond),SourceCaptureIdentity.physical(second.connector(),second.config()),
                UUID.randomUUID().toString(),canonicalSecond,UUID.randomUUID().toString());
        when(captures.read(BUSINESS,preparedSecond.captureId())).thenReturn(Optional.of(preparedSecond));
        var scope=new PipelineIntentScope(BUSINESS.clusterId(),BUSINESS.bindingVersion(),BUSINESS.cacheIncarnation(),pipeline.id(),2);
        for (var descriptor:List.of(Map.entry(source.id(),physical),Map.entry(second.id(),preparedSecond.chainId()))) {
            var context=new SrsConsumerContext(new SrsConsumerKey(descriptor.getValue(),pipeline.id(),descriptor.getKey()),2);
            var offset=new ConsumerOffset(context.key().consumerId(),Map.of("orders",2L),null,List.of(),null,0L,Map.of(),ConsumerProgressKind.SRS);
            var state=new SrsChainState(descriptor.getValue(),"capture",null,2,Set.of("orders"),Set.of("orders"),0,null,null,1,1);
            when(consumers.planningProgress(scope,descriptor.getValue(),Set.of(descriptor.getKey())))
                    .thenReturn(Optional.of(new SrsConsumerStore.PlanningProgress(state,List.of(new SrsConsumerProgress(context,1,0,offset,null)))));
        }
        when(cache.startPlanningSrs(eq(BUSINESS),any())).thenReturn(new SrsStores(chains,mock(SrsModelStore.class),mock(SrsLogStore.class),consumers));
        var reads=new StoreBackedPipelineWriteTargets(port).capture(port,new StoreBackedPipelineChains(port),selected,Optional.empty());
        assertThat(reads.progressChains().get()).containsExactly(physical,preparedSecond.chainId());
        assertThat(reads.progress().apply(physical).orElseThrow().consumerOffsets()).hasSize(1);
        assertThat(reads.progress().apply(preparedSecond.chainId()).orElseThrow().consumerOffsets()).hasSize(1);
        reads.verify().run();
        verify(consumers,times(1)).planningProgress(scope,physical,Set.of(source.id()));
        verify(consumers,times(1)).planningProgress(scope,preparedSecond.chainId(),Set.of(second.id()));
        verify(consumers,never()).attached(anyString());
    }
    @Test void aStoppedOldScopeWithoutAnAttachmentDoesNotInheritTheNewScopesConfirmation() {
        when(consumers.attached(physical)).thenReturn(new SrsConsumerTopology(BUSINESS,physical,0,List.of()));
        when(consumers.observationCursor(topology(2).consumers().getFirst().context())).thenReturn(Optional.empty());
        assertThat(new StoreBackedSinkPositions(port).apply("flow")).isEmpty();
        assertThat(new StoreBackedPipelineChains(port).progressChainsOf("flow")).isEmpty();verifyNoInteractions(meta);
    }
    @Test void aDifferentStateGenerationCannotReadTheReplacementConsumersAcknowledgement() {
        when(consumers.observationCursor(topology(2).consumers().getFirst().context())).thenThrow(
                new TapstateException(IoError.WORKLOAD_CLAIM_FENCED, Map.of(), null));
        assertThatThrownBy(() -> new StoreBackedSinkPositions(port).apply("flow")).isInstanceOf(TapstateException.class);verifyNoInteractions(meta);
    }
    @Test void aBindingOrScopeChangeDuringTheReadCannotRetargetTheOriginalObservation() {
        when(consumers.observationCursor(topology(2).consumers().getFirst().context())).thenAnswer(call -> {
            when(mappings.read("flow")).thenReturn(Optional.of(new PipelineStateMapping("flow",3,PipelineStateMapping.Phase.ACTIVE,null)));
            return Optional.empty();
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
    @Test void aSnapshotOnlyPipelineDoesNotRequireASharedConsumerForItsFinalObservation() {
        var snapshot = new PipelineResource("flow", null, List.of(SourceRef.spec("input", true)),
                null, null, null, new Settings(null, null, null, null, ReadMode.SNAPSHOT_ONLY, null), null);
        memory.artifacts().save(snapshot);
        String channel = SourceCaptureResolution.forPipeline(snapshot, source, model).orElseThrow().chainId().value();
        doReturn(Optional.empty()).when(meta).read(channel);
        assertThat(new StoreBackedSinkPositions(port).apply("flow")).isEmpty();
        verify(port, never()).sharedSinkOffset(any(), any(), any());
        verifyNoInteractions(captures, consumers, chains);
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
        var context = topology(2).consumers().getFirst().context();
        var offset = consumers.observationCursor(context);
        when(consumers.observationCursor(context)).thenAnswer(call -> {
            when(mappings.read("flow")).thenReturn(Optional.of(new PipelineStateMapping("flow",3,PipelineStateMapping.Phase.ACTIVE,null)));
            return offset;
        });
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
