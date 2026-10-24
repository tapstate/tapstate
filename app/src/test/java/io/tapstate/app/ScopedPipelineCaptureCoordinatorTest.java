package io.tapstate.app;

import com.hazelcast.core.HazelcastInstance;
import io.tapstate.adapters.pdk.*;
import io.tapstate.core.common.TapstateException;
import io.tapstate.core.event.*;
import io.tapstate.core.lifecycle.*;
import java.util.concurrent.atomic.*;
import java.util.concurrent.*;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import io.tapstate.core.model.*;
import io.tapstate.core.model.canonical.SourceCaptureIdentity;
import io.tapstate.runtime.srs.*;
import io.tapstate.spi.capture.*;
import io.tapstate.spi.store.*;
import java.time.*;
import java.util.*;
import org.junit.jupiter.api.Test;
import static org.assertj.core.api.Assertions.*;
import static org.mockito.Mockito.*;

class ScopedPipelineCaptureCoordinatorTest {
    private static final CacheBusinessScope BUSINESS=new CacheBusinessScope("cluster",1,"cache");
    private static final PipelineIntentScope SCOPE=new PipelineIntentScope("cluster",1,"cache","flow",7);
    private static final WorkloadOwner OWNER=new WorkloadOwner("node","boot");
    private static final WorkloadClaimFence PIPELINE=new WorkloadClaimFence(new WorkloadClaimKey("cluster",WorkloadClaimType.PIPELINE_ACTUATION,"flow"),OWNER,2,3,1);
    private static final ExecutionFence ORIGINAL=new ExecutionFence(SCOPE,PIPELINE);
    @Test void anAbsentLocalOwnerDoesNotProveItsExactCaptureTenureEnded() {
        try(var f=new Fixture(ReadMode.SNAPSHOT_AND_CDC)) {
            var properties=new ClusterProperties();properties.setProfile(ClusterProperties.Profile.PRODUCTION_HA);
            var gate=new ClusterMembershipGate(properties);gate.install(new ClusterMembership("cluster",1,Set.of("node","node-b","node-c")));
            gate.canCommit(Set.of("node","node-b"));var raw=new InMemoryWorkloadClaimStore();
            var ownership=new CaptureOwnership("cluster",OWNER,gate,new ClusterWorkloadClaims(raw,gate),Duration.ofSeconds(60));
            var permit=ownership.acquire(new CaptureId("mck-key"));
            var prepared=new CapturePreparation(BUSINESS,CapturePreparation.Mode.SHARED_SRS,"mck-key","mck-key",
                    SourceCaptureIdentity.physical(f.source.connector(),f.source.config()),UUID.randomUUID().toString());
            var node=new WorkloadClaimFence(new WorkloadClaimKey("cluster",WorkloadClaimType.NODE_SESSION,"node"),OWNER,1,0,1);
            try(var coordinator=new ScopedPipelineCaptureCoordinator(new ScopedPipelineCaptureCoordinator.Stores(BUSINESS,f.pipelines,f.sources,
                    f.configuration,f.cache,f.notes),new ScopedPipelineCaptureCoordinator.Execution(() -> Optional.of(node),f.authorization,
                    ownership,Duration.ofSeconds(10),() -> {},Clock.systemUTC()),f.runtime,f.buffer,f.connectors,
                    (original,config,guard,preparation,stream) -> {throw new AssertionError("no capture may open");})) {
                assertThatThrownBy(() -> coordinator.retireShared(prepared,permit.fence())).isInstanceOf(TapstateException.class);
                assertThatThrownBy(() -> coordinator.requireSharedRetired(prepared,permit.fence())).isInstanceOf(TapstateException.class);
                assertThat(raw.read(permit.claim().key()).orElseThrow().leased()).isTrue();
                assertThat(raw.release(permit.claim())).isTrue();
                coordinator.retireShared(prepared,permit.fence());coordinator.requireSharedRetired(prepared,permit.fence());
                assertThat(raw.read(permit.claim().key()).orElseThrow().claim().claimGeneration()).isEqualTo(permit.claim().claimGeneration());
            }
        }
    }
    @Test void failedOriginalCaptureProofReleasesOnlyTheLeaseTakenByThisStart() {
        try(var f=new Fixture()) {
            when(f.authorization.requireCapture(any(),any())).thenThrow(new TapstateException(IoError.WORKLOAD_CLAIM_FENCED,Map.of(),null));
            var run=f.coordinator.forRun(SCOPE,PIPELINE,ORIGINAL);
            assertThatThrownBy(() -> run.startCapture("flow",f.snapshot(),f.models())).isInstanceOf(TapstateException.class);
            verify(f.ownership).release(f.capture);
            verify(f.snapshots,never()).advanceOrder(anyLong(),anyLong(),anyMap());
            assertThat(run.isCapturing("flow")).isFalse();
        }
    }
    @Test void originalBoundedStartUsesPreparedFactsAndStopKeepsDurableStateAndNotes() throws Exception {
        try(var f=new Fixture()) {
            var run=f.coordinator.forRun(SCOPE,PIPELINE,ORIGINAL); run.startCapture("flow",f.snapshot(),f.models());
            var bindings=run.captureBindings(); assertThat(bindings.snapshots().get("input").epoch()).isOne();
            verify(f.snapshots,timeout(3000)).recordCount("input","orders",1,1);
            var rows=f.buffer.drain(bindings.bufferKey("input"),SrsRingbuffer.ringName("mck-key","orders"));
            assertThat(rows).singleElement().satisfies(row -> {
                assertThat(row.after()).containsEntry("source","input"); assertThat(row.position().order()).isEqualTo(SourceOrder.snapshotRow(1));
            });
            long deadline=System.nanoTime()+Duration.ofSeconds(3).toNanos();
            while(run.snapshotProgress("flow").byTable().isEmpty() && System.nanoTime()<deadline)Thread.sleep(5);
            assertThat(run.snapshotProgress("flow").byTable().get("orders").rowsDone()).isOne();
            assertThat(run.loadDelivered("flow")).isFalse();
            run.startCapture("flow",f.snapshot(),f.models()); assertThat(f.opened).hasValue(1);
            verify(f.configuration).issueFirstCreate(any()); verify(f.snapshots).advanceOrder(eq(0L),eq(0L),anyMap());
            verify(f.notes).open(RequiredNoteOwner.forNode(1,new PipelineNode("flow","input"),PipelineNode.Purpose.CAPTURE,7));
            run.stopCapture("flow",false); run.stopCapture("flow",false);
            verify(f.ownership).release(f.capture); verify(f.notes,never()).clear(any(),anyBoolean());
            assertThat(f.closedPorts).hasValue(1); assertThat(run.isCapturing("flow")).isFalse();
            assertThat(run.snapshotProgress("flow")).isEqualTo(SnapshotReading.NONE);
            assertThat(run.capturedRows("flow")).isEqualTo(CaptureReading.NONE);
        }
    }

    @Test void streamingSnapshotProgressRequiresItsOwnDurableCompletionMarker() throws Exception {
        try (var f = new Fixture(ReadMode.SNAPSHOT_AND_CDC)) {
            var run = f.coordinator.forRun(SCOPE, PIPELINE, ORIGINAL);
            run.startCapture("flow", f.snapshot(), f.models());
            assertThat(run.loadDelivered("flow")).isFalse();
            assertThat(run.snapshotProgress("flow").byTable().get("orders"))
                    .isEqualTo(new TableSnapshot(0, null, null, false));
            f.streamingProgress.set(f.progress(true, 1, 1));
            assertThat(run.loadDelivered("flow")).isTrue();
            assertThat(run.snapshotProgress("flow").byTable().get("orders"))
                    .isEqualTo(new TableSnapshot(0, 0L, 100, true));
            assertThat(run.runSnapshotProgress("flow").byTable().get("orders"))
                    .isEqualTo(new TableSnapshot(0, 0L, 100, true));
        }
    }

    @ParameterizedTest @ValueSource(strings = {"missing", "membership", "snapshot"})
    void streamingSnapshotReadCannotBorrowAnotherProgressIdentity(String damage) throws Exception {
        try (var f = new Fixture(ReadMode.SNAPSHOT_AND_CDC)) {
            var run = f.coordinator.forRun(SCOPE, PIPELINE, ORIGINAL);
            run.startCapture("flow", f.snapshot(), f.models());
            f.streamingProgress.set("missing".equals(damage) ? null
                    : f.progress(true, "membership".equals(damage) ? 2 : 1, "snapshot".equals(damage) ? 2 : 1));
            assertThatThrownBy(() -> run.snapshotProgress("flow")).isInstanceOf(TapstateException.class);
            assertThatThrownBy(() -> run.runSnapshotProgress("flow")).isInstanceOf(TapstateException.class);
        }
    }

    @Test void currentRunCountsDoNotAdoptTheRetainedSnapshotTotal() throws Exception {
        try(var f=new Fixture()) {
            var run=f.coordinator.forRun(SCOPE,PIPELINE,ORIGINAL); run.startCapture("flow",f.snapshot(),f.models());
            verify(f.snapshots,timeout(3000)).recordCount("input","orders",1,1);
            long deadline=System.nanoTime()+Duration.ofSeconds(3).toNanos();
            while(run.snapshotProgress("flow").byTable().isEmpty() && System.nanoTime()<deadline)Thread.sleep(5);
            assertThat(run.snapshotProgress("flow").byTable()).containsKey("orders");
            when(f.snapshots.count("input","orders",1)).thenReturn(OptionalLong.of(20));
            assertThat(run.runSnapshotProgress("flow").byTable().get("orders").rowsDone()).isOne();
            assertThat(run.snapshotProgress("flow").byTable().get("orders").rowsDone()).isEqualTo(20);
        }
    }
    @Test void twoSourceNodesSharingOneCaptureKeepSeparatePrivateRowsAndOneLease() throws Exception {
        try(var f=new Fixture(List.of("left","right"))) {
            var run=f.coordinator.forRun(SCOPE,PIPELINE,ORIGINAL); run.startCapture("flow",f.snapshot(),f.models()); var bindings=run.captureBindings();
            verify(f.snapshots,timeout(3000)).recordCount("left","orders",1,1);
            verify(f.snapshots,timeout(3000)).recordCount("right","orders",1,1);
            String ring=SrsRingbuffer.ringName("mck-key","orders");
            assertThat(f.buffer.drain(bindings.bufferKey("left"),ring)).singleElement().satisfies(row -> assertThat(row.after()).containsEntry("source","left"));
            assertThat(f.buffer.drain(bindings.bufferKey("right"),ring)).singleElement().satisfies(row -> assertThat(row.after()).containsEntry("source","right"));
            verify(f.ownership).acquire(any()); assertThat(f.opened).hasValue(2);
            run.stopCapture("flow",false); verify(f.ownership).release(f.capture); assertThat(f.closedPorts).hasValue(2);
        }
    }
    @Test void anotherCommandCannotStartTheOriginalRunOrReadArtifactsOnItsBehalf() {
        try(var f=new Fixture()) {
            var other=new PipelineIntentScope("cluster",1,"cache","flow",8);
            var run=f.coordinator.forRun(other,PIPELINE,ORIGINAL);
            assertThatThrownBy(() -> run.startCapture("flow",f.snapshot(),f.models())).isInstanceOf(TapstateException.class);
            verifyNoInteractions(f.configuration,f.snapshots,f.ownership);
        }
    }
    @Test void anOldCallbackResumedAfterReleaseCannotRecreateItsHandoff() throws Exception {
        try(var f=new Fixture()) {
            var output=new AtomicReference<CaptureHandoff>();
            doAnswer(call -> { output.set(call.getArgument(3));return mock(CaptureRun.class); }).when(f.runtime).beginSnapshotOnly(any(),anyLong(),any(),any(),any());
            var run=f.coordinator.forRun(SCOPE,PIPELINE,ORIGINAL); run.startCapture("flow",f.snapshot(),f.models());
            var bindings=run.captureBindings(); var entered=new CountDownLatch(1); var resume=new CountDownLatch(1);
            when(f.authorization.requireCapture(any(),any())).thenAnswer(call -> {
                if(Thread.currentThread().getName().equals("old-source-callback")) { entered.countDown();if(!resume.await(3,TimeUnit.SECONDS))throw new AssertionError("resume not released"); }
                return call.getArgument(1);
            });
            try(var executor=Executors.newSingleThreadExecutor(task -> new Thread(task,"old-source-callback"))) {
                var pending=executor.submit(() -> output.get().accept(Envelope.read(1,"orders",Map.of("id",1),null)));
                try {
                    assertThat(entered.await(3,TimeUnit.SECONDS)).isTrue(); run.stopCapture("flow",false); resume.countDown(); pending.get(3,TimeUnit.SECONDS);
                    assertThat(f.buffer.drain(bindings.bufferKey("input"),SrsRingbuffer.ringName("mck-key","orders"))).isEmpty();
                } finally { resume.countDown(); }
            }
        }
    }
    @ParameterizedTest @ValueSource(booleans={false,true})
    void aSourceCloseFailureRetainsTheOriginalSessionAndLeaseForRetry(boolean error) throws Exception {
        try(var f=new Fixture(List.of("input"),1)) {
            Throwable failure=error?new AssertionError("close invariant"):new IllegalStateException("close invariant");
            f.closeFailure.set(failure);f.rowCount.set(3);
            var run=f.coordinator.forRun(SCOPE,PIPELINE,ORIGINAL);run.startCapture("flow",f.snapshot(),f.models());var bindings=run.captureBindings();
            String key=bindings.bufferKey("input"),ring=SrsRingbuffer.ringName("mck-key","orders");
            long deadline=System.nanoTime()+Duration.ofSeconds(3).toNanos();
            while(f.readRows.get()<2 && System.nanoTime()<deadline)Thread.sleep(5);
            try {
                assertThat(f.readRows).hasValue(2);
                assertThatThrownBy(() -> run.stopCapture("flow",false)).isSameAs(failure);
                verify(f.ownership,never()).release(f.capture);assertThat(run.isCapturing("flow")).isTrue();
                run.stopCapture("flow",false);verify(f.ownership).release(f.capture);assertThat(run.isCapturing("flow")).isFalse();
                assertThat(f.buffer.snapshotState(key,ring).declared()).isFalse();
            } finally { f.buffer.release(key); }
        }
    }
    @Test void wholeCoordinatorCloseRetainsUnclosedOriginalSessionsForRetry() throws Exception {
        try(var f=new Fixture(List.of("input"),1)) {
            f.rowCount.set(3);var failure=new IllegalStateException("original reader close pending");f.closeFailure.set(failure);
            var run=f.coordinator.forRun(SCOPE,PIPELINE,ORIGINAL);run.startCapture("flow",f.snapshot(),f.models());
            long deadline=System.nanoTime()+Duration.ofSeconds(3).toNanos();while(f.readRows.get()<2 && System.nanoTime()<deadline)Thread.sleep(5);
            assertThatThrownBy(f.coordinator::close).isSameAs(failure);verify(f.ownership,never()).release(f.capture);
            f.coordinator.close();verify(f.ownership).release(f.capture);
        }
    }

    @Test void aTrustedReplacementUsesPhysicalRingsWhileKeepingCanonicalCaptureNotesAndHistory() {
        try(var f=new Fixture(ReadMode.SNAPSHOT_AND_CDC)) {
            String canonical="mck-key",physical="srs-recovery-"+"b".repeat(64),operation=UUID.randomUUID().toString();
            String captureId=SourceCaptureIdentity.sharedCapture(canonical);
            var prepared=new CapturePreparation(BUSINESS,CapturePreparation.Mode.SHARED_SRS,physical,captureId,
                    SourceCaptureIdentity.physical(f.source.connector(),f.source.config()),operation,canonical,UUID.randomUUID().toString());
            var capture=new WorkloadClaim(new WorkloadClaimKey("cluster",WorkloadClaimType.CAPTURE,captureId),OWNER,4,2,1,Instant.now().plusSeconds(60));
            var context=new SrsConsumerContext(new SrsConsumerKey(physical,"flow","input"),7);
            var membership=new SrsConsumerMembership(context,1,false,Set.of("orders"),PIPELINE);
            var chains=mock(SrsChainStore.class);var models=mock(SrsModelStore.class);var consumers=mock(SrsConsumerStore.class);
            var chain=new AtomicReference<>(new SrsChainState(physical,captureId,"1h",0,Set.of("orders"),Set.of(),0,null,null,1,1));
            var cursor=new ConsumerOffset(context.key().consumerId(),Map.of("orders",-1L),null,List.of(),null,0,Map.of(),ConsumerProgressKind.SRS);
            var progress=new AtomicReference<>(new SrsConsumerProgress(context,1,0,cursor,null));
            when(f.configuration.issueFirstCreate(any())).thenReturn(new CaptureCreationResult.AlreadyIssued(prepared));
            when(f.configuration.inspectSharedHistory(eq(prepared),anyString(),anyString())).thenReturn(new CaptureSharedHistory(prepared,true));
            when(f.cache.load(prepared)).thenReturn(new CaptureCacheState(prepared,WorkloadClaimFence.from(capture),Optional.of(chain.get())));
            when(f.ownership.acquire(any())).thenReturn(new CaptureOwnership.Permit(true,capture));
            when(consumers.find(context.key())).thenReturn(Optional.of(membership));
            when(consumers.progress(context)).thenAnswer(ignored -> Optional.of(progress.get()));
            when(consumers.attached(physical)).thenReturn(new SrsConsumerTopology(BUSINESS,physical,1,List.of(membership)));
            when(consumers.startReadingAfter(eq(context),eq(1L),eq(0L),anyMap(),eq(PIPELINE))).thenAnswer(call -> {
                progress.set(new SrsConsumerProgress(context,1,1,cursor,null,null,call.getArgument(3)));return true;
            });
            when(chains.load(physical)).thenAnswer(ignored -> Optional.of(chain.get()));
            when(chains.requestTables(eq(physical),anySet(),any())).thenAnswer(ignored -> chain.get());
            when(chains.openEpoch(eq(physical),eq(0L),any())).thenAnswer(ignored -> {
                chain.set(new SrsChainState(physical,captureId,"1h",1,Set.of("orders"),Set.of("orders"),0,null,null,1,1));
                return OptionalLong.of(1);
            });
            when(chains.publishServingTables(eq(physical),eq(1L),anySet(),any())).thenReturn(true);
            var model=new SrsModel(new SrsModelKey(physical,"orders","cache",1,0),SrsRingbuffer.ringName(physical,"orders"),
                    new SourceTable("orders",List.of(),List.of(),List.of()),0);
            when(models.publishedForNewSnapshot(anyString(),anyMap())).thenReturn(Optional.of(Map.of("orders",model)));
            when(f.sources.srs(any(),any())).thenReturn(new SrsStores(chains,models,mock(SrsLogStore.class),consumers));
            doReturn(-1L).when(f.runtime).initialReadAfter(any(),anyString(),anyString(),any(),anyString(),any());
            doNothing().when(f.runtime).retain(any(),anyString(),any(),any());
            var tail=mock(NativeSrsCaptureRunUnit.Tail.class);when(tail.health()).thenReturn(new CaptureHealth());
            doReturn(tail).when(f.runtime).startTail(any(),any(),any(),any(),any(),any(),any(),anyBoolean());
            var run=f.coordinator.forRun(SCOPE,PIPELINE,ORIGINAL);run.startCapture("flow",f.snapshot(),f.models());
            var bindings=run.captureBindings();String ring=SrsRingbuffer.ringName(physical,"orders");
            assertThat(bindings.logged().get("input").run().miningChainId()).isEqualTo(physical);
            verify(models).publishedForNewSnapshot(physical,Map.of("orders",ring));
            verify(f.runtime).initialReadAfter(eq(BUSINESS),eq(physical),eq("orders"),any(),eq("1h"),any());
            assertThat(f.buffer.snapshotState(bindings.bufferKey("input"),ring).declared()).isTrue();
            assertThat(f.buffer.snapshotState(bindings.bufferKey("input"),SrsRingbuffer.ringName(canonical,"orders")).declared()).isFalse();
            verify(f.notes,atLeastOnce()).open(new RequiredNoteOwner(1,"pdk.chain."+canonical,RequiredNoteOwner.Kind.CHAIN,canonical,0));
            verify(f.notes,never()).open(new RequiredNoteOwner(1,"pdk.chain."+physical,RequiredNoteOwner.Kind.CHAIN,physical,0));
            verify(f.ownership).acquire(new CaptureId(captureId));
            verify(f.runtime).startTail(eq(BUSINESS),any(),eq(WorkloadClaimFence.from(capture)),any(),any(),any(),any(),eq(true));
            verify(f.configuration).inspectSharedHistory(eq(prepared),eq("input"),anyString());
        }
    }

    @Test void aReplacementFreshFullCallbackKeepsItsCanonicalIdentityInThePublishedCaptureBinding() {
        try(var f=new Fixture(ReadMode.SNAPSHOT_AND_CDC)) {
            String canonical="mck-key",physical="srs-recovery-"+"c".repeat(64),captureId=SourceCaptureIdentity.sharedCapture(canonical);
            var prepared=new CapturePreparation(BUSINESS,CapturePreparation.Mode.SHARED_SRS,physical,captureId,
                    SourceCaptureIdentity.physical(f.source.connector(),f.source.config()),UUID.randomUUID().toString(),canonical,UUID.randomUUID().toString());
            var capture=new WorkloadClaim(new WorkloadClaimKey("cluster",WorkloadClaimType.CAPTURE,captureId),OWNER,4,2,1,Instant.now().plusSeconds(60));
            var context=new SrsConsumerContext(new SrsConsumerKey(physical,"flow","input"),7);
            var membership=new SrsConsumerMembership(context,1,false,Set.of("orders"),PIPELINE);
            var oldModel=new SrsModelKey(physical,"orders","cache",1,0);
            var checkpoint=new SrsCaptureCheckpoint(new ChainPosition(new SourceOrder(1,13),"fixed-cut"),Map.of("orders",new SrsTableCheckpoint(oldModel,13)));
            var originalChain=new SrsChainState(physical,captureId,"1h",1,Set.of("orders"),Set.of("orders"),1,checkpoint,Instant.EPOCH,1,1);
            var openedChain=new SrsChainState(physical,captureId,"1h",2,Set.of("orders"),Set.of("orders"),1,checkpoint,Instant.EPOCH,1,1);
            var cursor=new ConsumerOffset(context.key().consumerId(),Map.of("orders",13L),null,List.of(),null,0,Map.of(),ConsumerProgressKind.SRS);
            var retained=new SrsConsumerProgress(context,1,1,cursor,null);
            var consumers=mock(SrsConsumerStore.class);var chains=mock(SrsChainStore.class);
            when(f.configuration.issueFirstCreate(any())).thenReturn(new CaptureCreationResult.AlreadyIssued(prepared));
            when(f.configuration.inspectSharedHistory(eq(prepared),anyString(),anyString())).thenReturn(new CaptureSharedHistory(prepared,true));
            when(f.cache.load(prepared)).thenReturn(new CaptureCacheState(prepared,WorkloadClaimFence.from(capture),Optional.of(originalChain)));
            when(f.ownership.acquire(any())).thenReturn(new CaptureOwnership.Permit(true,capture));
            when(consumers.find(context.key())).thenReturn(Optional.of(membership));
            when(consumers.progress(context)).thenReturn(Optional.of(retained));
            when(consumers.attached(physical)).thenReturn(new SrsConsumerTopology(BUSINESS,physical,1,List.of(membership)));
            var current=new AtomicReference<>(originalChain);
            when(chains.load(physical)).thenAnswer(ignored -> Optional.of(current.get()));
            when(chains.requestTables(eq(physical),anySet(),any())).thenAnswer(ignored -> current.get());
            when(chains.openEpoch(eq(physical),eq(1L),any())).thenAnswer(ignored -> {current.set(openedChain);return OptionalLong.of(2);});
            when(chains.publishServingTables(eq(physical),eq(2L),anySet(),any())).thenReturn(true);
            when(f.sources.srs(any(),any())).thenReturn(new SrsStores(chains,mock(SrsModelStore.class),mock(SrsLogStore.class),consumers));
            var completed=mock(CompletedPipelineFull.class);
            when(f.completedFull.find(eq(SCOPE),any(),anyMap())).thenReturn(Optional.of(completed));
            var full=mock(SrsFreshFullPreparation.class);when(full.originalChain()).thenReturn(openedChain);
            when(consumers.inspectFreshFull(eq(completed),any(),eq(1L),any(),eq(WorkloadClaimFence.from(capture)))).thenReturn(full);
            var origin=new SrsFreshFullOrigin("clear-full",SCOPE,"full-preparation","a".repeat(64),"input");
            var nextModel=new SrsModelKey(physical,"orders","cache",2,0);
            var pending=new SrsFreshFull(origin,2,Map.of("orders",nextModel));
            var published=new SrsChainState(physical,captureId,"1h",2,Set.of("orders"),Set.of("orders"),1,checkpoint,Instant.EPOCH,1,1,
                    new SourceResumeOverride("fixed-cut",1,Instant.EPOCH),pending);
            var nextCursor=new ConsumerOffset(context.key().consumerId(),Map.of("orders",13L),null,List.of(),"fixed-cut",2,Map.of(),ConsumerProgressKind.SRS);
            var progress=new SrsConsumerProgress(context,1,2,nextCursor,new SrsSnapshotPin(2,"fixed-cut",Map.of("orders",nextModel)),null,Map.of("orders",13L));
            var receipt=new SrsFreshFullReceipt(published,progress);
            var mismatch=mock(SrsBaselineModelMismatch.class);when(mismatch.chainId()).thenReturn(physical);when(mismatch.getSuppressed()).thenReturn(new Throwable[0]);
            var tail=mock(NativeSrsCaptureRunUnit.Tail.class);when(tail.health()).thenReturn(new CaptureHealth());
            doThrow(mismatch).doReturn(tail).when(f.runtime).startTail(any(),any(),any(),any(),any(),any(),any(),anyBoolean());
            doNothing().when(f.runtime).retain(any(),anyString(),any(),any());
            doAnswer(call -> {
                Runnable guard=call.getArgument(5);guard.run();
                java.util.function.Consumer<SrsFreshFullReceipt> callback=call.getArgument(8);callback.accept(receipt);
                return mock(CaptureRun.class);
            }).when(f.runtime).beginSnapshot(any(),any(),eq(2L),any(),any(),any(),eq(full),any(),any());
            var run=f.coordinator.forRun(SCOPE,PIPELINE,ORIGINAL);run.startCapture("flow",f.snapshot(),f.models());
            var binding=run.captureBindings().logged().get("input");
            assertThat(binding.canonicalChainId()).isEqualTo(canonical);
            assertThat(binding.run().miningChainId()).isEqualTo(physical);assertThat(binding.run().scope()).isEqualTo(SCOPE);
            assertThat(binding.snapshotEpoch()).isEqualTo(2);assertThat(binding.recoveryAfter()).containsExactly(Map.entry("orders",13L));
            verify(f.runtime,times(2)).startTail(eq(BUSINESS),any(),eq(WorkloadClaimFence.from(capture)),any(),any(),any(),any(),eq(true));
        }
    }

    @Test void aReservedTrustedMappingRefusesBeforeTakingCaptureOwnershipOrOpeningPorts() {
        try(var f=new Fixture(ReadMode.SNAPSHOT_AND_CDC)) {
            when(f.configuration.issueFirstCreate(any())).thenThrow(new TapstateException(IoError.CAPTURE_STATE_REBUILD_REQUIRED,Map.of("capture","canonical"),null));
            var run=f.coordinator.forRun(SCOPE,PIPELINE,ORIGINAL);
            assertThatThrownBy(() -> run.startCapture("flow",f.snapshot(),f.models())).isInstanceOfSatisfying(TapstateException.class,
                    failure -> assertThat(failure.code()).isEqualTo(IoError.CAPTURE_STATE_REBUILD_REQUIRED));
            verifyNoInteractions(f.ownership);assertThat(f.opened).hasValue(0);verify(f.cache,never()).load(any());
            assertThat(run.isCapturing("flow")).isFalse();
        }
    }

    @Test void aPreparedReplacementForAnotherCanonicalSourceRefusesBeforeCaptureOwnership() {
        try(var f=new Fixture(ReadMode.SNAPSHOT_AND_CDC)) {
            var prepared=new CapturePreparation(BUSINESS,CapturePreparation.Mode.SHARED_SRS,"srs-recovery-"+"b".repeat(64),"foreign-capture",
                    SourceCaptureIdentity.physical(f.source.connector(),f.source.config()),UUID.randomUUID().toString(),"foreign-canonical",UUID.randomUUID().toString());
            var claim=new WorkloadClaimFence(new WorkloadClaimKey("cluster",WorkloadClaimType.CAPTURE,prepared.captureId()),OWNER,1,0,1);
            var chain=new SrsChainState(prepared.chainId(),prepared.captureId(),"1h",1,Set.of("orders"),Set.of("orders"),0,null,null,1,1);
            var cached=new CaptureCacheState(prepared,claim,Optional.of(chain));
            when(f.configuration.issueFirstCreate(any())).thenReturn(new CaptureCreationResult.AlreadyIssued(prepared));
            when(f.cache.load(prepared)).thenReturn(cached);
            var run=f.coordinator.forRun(SCOPE,PIPELINE,ORIGINAL);
            assertThatThrownBy(() -> run.startCapture("flow",f.snapshot(),f.models())).isInstanceOfSatisfying(TapstateException.class,
                    failure -> assertThat(failure.code()).isEqualTo(IoError.PIPELINE_INTENT_FENCED));
            verifyNoInteractions(f.ownership);assertThat(f.opened).hasValue(0);assertThat(run.isCapturing("flow")).isFalse();
        }
    }

    private static final class Port implements CapturePort,SnapshotOnlyCapture {
        final String source; final Runnable guard; final AtomicInteger closed,read; final int count; final AtomicReference<Throwable> closeFailure;
        Port(String source,Runnable guard,AtomicInteger closed,AtomicInteger read,int count,AtomicReference<Throwable> closeFailure) {
            this.source=source;this.guard=guard;this.closed=closed;this.read=read;this.count=count;this.closeFailure=closeFailure;
        }
        public CaptureBatch snapshotOnly(CaptureConfig config) {
            guard.run(); var rows=java.util.stream.IntStream.range(0,count).mapToObj(i -> Envelope.read(1,"orders",Map.of("source",source,"id",i),null)).iterator(); var ended=new AtomicBoolean();
            return new CaptureBatch() {
                public boolean hasNext() { guard.run(); return rows.hasNext(); }
                public Envelope next() { guard.run();read.incrementAndGet();return rows.next(); }
                public Optional<SourcePosition> seam() { throw new AssertionError("bounded reads have no seam"); }
                public void close() { if(ended.compareAndSet(false,true)) {
                    closed.incrementAndGet();var failure=closeFailure.get();if(failure instanceof RuntimeException runtime)throw runtime;if(failure instanceof Error error)throw error;
                } }
            };
        }
        public CaptureBatch snapshot(CaptureConfig config) { throw new AssertionError(); }
        public Subscription cdc(CaptureConfig config,CaptureStart start,CaptureListener listener) { throw new AssertionError(); }
        public ConnectionReport testConnection(CaptureConfig config) { throw new AssertionError(); }
        public DiscoveredSchema discoverSchema(CaptureConfig config) { throw new AssertionError(); }
    }
    private static final class Fixture implements AutoCloseable {
        final InMemoryStorePort memory=new InMemoryStorePort();
        final PipelineStoreViews pipelines=mock(PipelineStoreViews.class);
        final SrsStoreViews sources=mock(SrsStoreViews.class);
        final CaptureStatePreparations configuration=mock(CaptureStatePreparations.class);
        final CaptureCachePreparations cache=mock(CaptureCachePreparations.class);
        final RequiredNoteStore notes=mock(RequiredNoteStore.class);
        final SnapshotStateStore snapshots=mock(SnapshotStateStore.class);
        final ExecutionAuthorization authorization=mock(ExecutionAuthorization.class);
        final CaptureOwnership ownership=mock(CaptureOwnership.class);
        final ConnectorProvisioner connectors=mock(ConnectorProvisioner.class);
        final ScopedPipelineCaptureCoordinator.CompletedFullLookup completedFull=mock(ScopedPipelineCaptureCoordinator.CompletedFullLookup.class);
        final SnapshotBuffer buffer;
        final NativeSrsCaptureRunUnit runtime;
        final ScopedPipelineCaptureCoordinator coordinator;
        final SourceResource source;
        final List<SourceResource> definitions=new ArrayList<>();
        final AtomicInteger opened=new AtomicInteger(),closedPorts=new AtomicInteger(),readRows=new AtomicInteger(),rowCount=new AtomicInteger(1);
        final AtomicReference<Throwable> closeFailure=new AtomicReference<>();
        final WorkloadClaim capture;
        final SrsConsumerContext streamingContext = new SrsConsumerContext(new SrsConsumerKey("mck-key", "flow", "input"), 7);
        final AtomicReference<SrsConsumerProgress> streamingProgress = new AtomicReference<>();
        Fixture(ReadMode mode) { this(List.of("input"), 8, mode); }
        Fixture() { this(List.of("input")); }
        Fixture(List<String> ids) { this(ids,8); }
        Fixture(List<String> ids,int capacity) { this(ids, capacity, ReadMode.SNAPSHOT_ONLY); }
        Fixture(List<String> ids,int capacity,ReadMode mode) {
            buffer=new SnapshotBuffer(capacity);
            for(String id:ids) {
                var definition=new SourceResource(id,null,"fixture",Map.of("database","x"),SourceMode.CDC,List.of(TableRef.literal("orders")),new Srs("key",null,null,null,true),null);
                definitions.add(definition); memory.artifacts().create(definition);
            }
            source=definitions.getFirst();
            var pipeline=new PipelineResource("flow",null,ids.stream().<SourceRef>map(id -> SourceRef.spec(id,true)).toList(),null,null,null,
                    new Settings(null,null,null,null,mode,"latest"),null);
            memory.artifacts().create(pipeline);
            boolean bounded = mode == ReadMode.SNAPSHOT_ONLY;
            String id = bounded ? SourceCaptureIdentity.snapshotCapture("mck-key", Set.of("orders")) : "mck-key";
            var prepared=new CapturePreparation(BUSINESS,bounded?CapturePreparation.Mode.SNAPSHOT_ONLY:CapturePreparation.Mode.SHARED_SRS,"mck-key",id,SourceCaptureIdentity.physical(source.connector(),source.config()),UUID.randomUUID().toString());
            capture=new WorkloadClaim(new WorkloadClaimKey("cluster",WorkloadClaimType.CAPTURE,id),bounded?OWNER:new WorkloadOwner("remote","remote-boot"),1,0,1,Instant.now().plusSeconds(60));
            when(configuration.issueFirstCreate(any())).thenReturn(new CaptureCreationResult.AlreadyIssued(prepared));
            if (!bounded) when(configuration.inspectSharedHistory(eq(prepared),anyString(),anyString())).thenReturn(new CaptureSharedHistory(prepared,false));
            var cached = new CaptureCacheState(prepared, WorkloadClaimFence.from(capture),
                    bounded ? Optional.empty() : Optional.of(streamingChain()));
            when(cache.load(prepared)).thenReturn(cached);
            when(pipelines.snapshotStates(any(),any(),any())).thenReturn(snapshots);
            when(snapshots.advanceOrder(anyLong(),anyLong(),anyMap())).thenReturn(1L);
            when(snapshots.count(anyString(),anyString(),anyLong())).thenReturn(OptionalLong.of(1));
            when(authorization.require(any())).thenReturn(PIPELINE);
            when(authorization.requireCapture(any(),any())).thenAnswer(call -> call.getArgument(1));
            when(ownership.acquire(any())).thenReturn(new CaptureOwnership.Permit(true,capture));
            when(ownership.renew(any())).thenAnswer(call -> Optional.of(call.getArgument(0)));
            when(ownership.ttl()).thenReturn(Duration.ofSeconds(60));
            when(notes.guardedBy(any())).thenReturn(notes);
            when(notes.open(any())).thenAnswer(call -> new RequiredNoteScope(call.getArgument(0),1));
            when(notes.prepareOrigin(any(),any())).thenAnswer(call -> call.getArgument(1));
            when(connectors.resolve("fixture")).thenReturn(new ConnectorRef(List.of(),"not.loaded","2.0.8",null,null,"a".repeat(64),false));
            runtime=spy(new NativeSrsCaptureRunUnit(BUSINESS,new SrsStores(mock(SrsChainStore.class),mock(SrsModelStore.class),mock(SrsLogStore.class),mock(SrsConsumerStore.class)),mock(HazelcastInstance.class)));
            var node=new WorkloadClaimFence(new WorkloadClaimKey("cluster",WorkloadClaimType.NODE_SESSION,"node"),OWNER,1,0,1);
            coordinator=new ScopedPipelineCaptureCoordinator(new ScopedPipelineCaptureCoordinator.Stores(BUSINESS,pipelines,sources,configuration,cache,notes,ignored -> false,
                            ScopedPipelineCaptureCoordinator.BoundedLoads.NONE,completedFull),
                    new ScopedPipelineCaptureCoordinator.Execution(() -> Optional.of(node),authorization,ownership,Duration.ofSeconds(10),() -> {},Clock.systemUTC()),
                    runtime,buffer,connectors,(original,config,guard,preparation,stream) -> { guard.run(); opened.incrementAndGet(); return new Port(config.node().nodeId(),guard,closedPorts,readRows,rowCount.get(),closeFailure); });
            if (!bounded) streaming();
        }
        private SrsConsumerProgress progress(boolean complete, long membership, long epoch) {
            var key = new SrsModelKey("mck-key", "orders", "cache", 1, 0);
            var cursor = new ConsumerOffset(streamingContext.key().consumerId(), Map.of("orders", 0L), null,
                    complete ? List.of("orders") : List.of(), "seam", epoch, Map.of(), ConsumerProgressKind.SRS);
            return new SrsConsumerProgress(streamingContext, membership, 1, cursor,
                    new SrsSnapshotPin(epoch, "seam", Map.of("orders", key)), null, Map.of("orders", 0L));
        }

        private static SrsChainState streamingChain() {
            return new SrsChainState("mck-key", "mck-key", "1h", 1, Set.of("orders"), Set.of("orders"),
                    0, null, null, 1, 1);
        }

        private void streaming() {
            when(ownership.acquire(any())).thenReturn(new CaptureOwnership.Permit(false, capture));
            when(snapshots.count(anyString(), anyString(), anyLong())).thenReturn(OptionalLong.of(0));
            var consumers = mock(SrsConsumerStore.class);
            streamingProgress.set(progress(false, 1, 1));
            when(consumers.find(streamingContext.key())).thenReturn(Optional.of(
                    new SrsConsumerMembership(streamingContext, 1, false, Set.of("orders"), PIPELINE)));
            when(consumers.progress(streamingContext)).thenAnswer(ignored -> Optional.ofNullable(streamingProgress.get()));
            var chains = mock(SrsChainStore.class);
            when(chains.load("mck-key")).thenReturn(Optional.of(streamingChain()));
            var models = mock(SrsModelStore.class);
            var key = new SrsModelKey("mck-key", "orders", "cache", 1, 0);
            when(models.publishedForNewSnapshot(anyString(), anyMap())).thenReturn(Optional.of(Map.of("orders",
                    new SrsModel(key, SrsRingbuffer.ringName("mck-key", "orders"), new SourceTable("orders", List.of(), List.of(), List.of()), 0))));
            when(sources.srs(any(), any())).thenReturn(new SrsStores(chains, models, mock(SrsLogStore.class), consumers));
            doAnswer(call -> {
                var preparation = mock(SrsSnapshotPreparation.class);
                when(preparation.request()).thenReturn(new SnapshotPreparation(1, new SourcePosition("seam")));
                when(preparation.stillOwed()).thenReturn(List.of("orders"));
                java.util.function.Function<SrsSnapshotPreparation, CapturePort> ports = call.getArgument(3);
                ports.apply(preparation);
                CaptureHandoff output = call.getArgument(4); output.loaded("orders");
                return mock(CaptureRun.class);
            }).when(runtime).beginSnapshot(any(), any(), anyLong(), any(), any(), any());
        }

        ArtifactStore snapshot() { return ReadOnlyArtifactSnapshot.capture(memory.artifacts()); }
        SourceModelSnapshot models() { return SourceModelSnapshot.capture("flow",definitions,memory.schemas()); }
        @Override public void close() { try { coordinator.close(); } finally { runtime.close(); } }
    }
}
