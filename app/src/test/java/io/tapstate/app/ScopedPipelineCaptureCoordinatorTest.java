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
    void aSourceCloseFailureStillReleasesTheOriginalBufferAndLease(boolean error) throws Exception {
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
                verify(f.ownership).release(f.capture);
                assertThat(f.buffer.snapshotState(key,ring).declared()).isFalse();
            } finally { f.buffer.release(key); }
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
        final SnapshotBuffer buffer;
        final NativeSrsCaptureRunUnit runtime;
        final ScopedPipelineCaptureCoordinator coordinator;
        final SourceResource source;
        final List<SourceResource> definitions=new ArrayList<>();
        final AtomicInteger opened=new AtomicInteger(),closedPorts=new AtomicInteger(),readRows=new AtomicInteger(),rowCount=new AtomicInteger(1);
        final AtomicReference<Throwable> closeFailure=new AtomicReference<>();
        final WorkloadClaim capture;
        Fixture() { this(List.of("input")); }
        Fixture(List<String> ids) { this(ids,8); }
        Fixture(List<String> ids,int capacity) {
            buffer=new SnapshotBuffer(capacity);
            for(String id:ids) {
                var definition=new SourceResource(id,null,"fixture",Map.of("database","x"),SourceMode.CDC,List.of(TableRef.literal("orders")),new Srs("key",null,null,null,true),null);
                definitions.add(definition); memory.artifacts().create(definition);
            }
            source=definitions.getFirst();
            var pipeline=new PipelineResource("flow",null,ids.stream().<SourceRef>map(id -> SourceRef.spec(id,true)).toList(),null,null,null,
                    new Settings(null,null,null,null,ReadMode.SNAPSHOT_ONLY,"latest"),null);
            memory.artifacts().create(pipeline);
            String id=SourceCaptureIdentity.snapshotCapture("mck-key",Set.of("orders"));
            var prepared=new CapturePreparation(BUSINESS,CapturePreparation.Mode.SNAPSHOT_ONLY,"mck-key",id,SourceCaptureIdentity.physical(source.connector(),source.config()),UUID.randomUUID().toString());
            capture=new WorkloadClaim(new WorkloadClaimKey("cluster",WorkloadClaimType.CAPTURE,id),OWNER,1,0,1,Instant.now().plusSeconds(60));
            when(configuration.issueFirstCreate(any())).thenReturn(new CaptureCreationResult.AlreadyIssued(prepared));
            when(cache.load(prepared)).thenReturn(new CaptureCacheState(prepared,WorkloadClaimFence.from(capture),Optional.empty()));
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
            coordinator=new ScopedPipelineCaptureCoordinator(new ScopedPipelineCaptureCoordinator.Stores(BUSINESS,pipelines,sources,configuration,cache,notes),
                    new ScopedPipelineCaptureCoordinator.Execution(() -> Optional.of(node),authorization,ownership,Duration.ofSeconds(10),() -> {},Clock.systemUTC()),
                    runtime,buffer,connectors,(original,config,guard,preparation) -> { guard.run(); opened.incrementAndGet(); return new Port(config.node().nodeId(),guard,closedPorts,readRows,rowCount.get(),closeFailure); });
        }
        ArtifactStore snapshot() { return ReadOnlyArtifactSnapshot.capture(memory.artifacts()); }
        SourceModelSnapshot models() { return SourceModelSnapshot.capture("flow",definitions,memory.schemas()); }
        @Override public void close() { try { coordinator.close(); } finally { runtime.close(); } }
    }
}
