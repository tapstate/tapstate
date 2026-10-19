package io.tapstate.runtime.srs;

import io.tapstate.core.common.TapstateException;
import io.tapstate.core.event.*;
import io.tapstate.core.model.PipelineNode;
import io.tapstate.spi.capture.*;
import io.tapstate.spi.store.*;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.*;
import org.junit.jupiter.api.Test;
import static org.assertj.core.api.Assertions.*;

class DirectCaptureWriterTest {
    private static final PipelineIntentScope SCOPE=new PipelineIntentScope("cluster",1,"cache","flow",7);
    private static final DirectChannelContext CONTEXT=new DirectChannelContext(SCOPE,"channel","input","capture");
    private static final WorkloadOwner OWNER=new WorkloadOwner("node","boot");
    private static final WorkloadClaimFence PIPELINE=new WorkloadClaimFence(new WorkloadClaimKey("cluster",WorkloadClaimType.PIPELINE_ACTUATION,"flow"),OWNER,3,8,2);
    private static final WorkloadClaimFence CAPTURE=new WorkloadClaimFence(new WorkloadClaimKey("cluster",WorkloadClaimType.CAPTURE,"capture"),OWNER,5,0,2);
    private static final CaptureConfig CONFIG=new CaptureConfig("demo",Map.of(),List.of("a","b"),new PipelineNode("flow","input"));

    @Test void durableOriginalReceiptPrecedesRowsAndContainsOnlyTablesChangedSinceThePreviousBoundary() {
        var f=new Fixture(8); f.start();
        f.listener.onBatch(List.of(row("a",0),row("b",1)),Optional.of(new SourcePosition("batch-one")));
        assertThat(f.batches).singleElement().satisfies(batch -> {
            assertThat(batch.targets()).containsExactlyInAnyOrderEntriesOf(Map.of("a",0L,"b",1L));
            assertThat(batch.boundary()).isEqualTo(new ChainPosition(new SourceOrder(1,2),"batch-one"));
        });
        assertThat(f.delivered).extracting(event -> event.position().order()).containsExactly(new SourceOrder(1,0),new SourceOrder(1,1));
        assertThat(f.delivered.getFirst().position().token()).isNull(); assertThat(f.delivered.getLast().position().token()).isEqualTo("batch-one");
        f.listener.onBatch(List.of(row("a",2)),Optional.of(new SourcePosition("batch-two")));
        assertThat(f.batches.getLast().targets()).containsExactlyEntriesOf(Map.of("a",3L));
        assertThat(f.batches.getLast().ordinal()).isEqualTo(2);
    }
    @Test void tokenlessRowsRemainCoveredByTheNextPositionedBoundary() {
        var f=new Fixture(8); f.start();
        f.listener.onBatch(List.of(row("a",0)),Optional.empty()); assertThat(f.batches).isEmpty();
        f.listener.onBatch(List.of(row("b",1)),Optional.of(new SourcePosition("later")));
        assertThat(f.batches).singleElement().satisfies(batch -> assertThat(batch.targets()).containsExactlyInAnyOrderEntriesOf(Map.of("a",0L,"b",1L)));
        assertThat(f.delivered).hasSize(2);
    }
    @Test void anEmptyPositionedBatchIsARealOrderedReceipt() {
        var f=new Fixture(8); f.start(); f.listener.onBatch(List.of(),Optional.of(new SourcePosition("heartbeat")));
        assertThat(f.batches).singleElement().satisfies(batch -> {assertThat(batch.targets()).isEmpty();assertThat(batch.boundary().token()).isEqualTo("heartbeat");});
        assertThat(f.delivered).isEmpty();
    }
    @Test void invalidSelectionFailsBeforeAReceiptOrPartialDelivery() {
        var f=new Fixture(8); f.start();
        assertUnavailable(() -> f.listener.onBatch(List.of(row("a",0),row("foreign",1)),Optional.of(new SourcePosition("unproven"))));
        assertThat(f.batches).isEmpty(); assertThat(f.delivered).isEmpty();
        assertUnavailable(() -> f.listener.onBatch(List.of(row("a",0)),Optional.of(new SourcePosition("late"))));
    }
    @Test void aRefusedReceiptNeverForwardsAnyRow() {
        var f=new Fixture(8); f.start(); f.refuseBatch.set(true);
        assertUnavailable(() -> f.listener.onBatch(List.of(row("a",0)),Optional.of(new SourcePosition("not-committed"))));
        assertThat(f.delivered).isEmpty(); assertThat(f.batches).isEmpty();
    }
    @Test void guardLossAfterTheReceiptPreventsTheFirstExternalDelivery() {
        var f=new Fixture(8); f.start(); f.afterReceipt=() -> f.active.set(false);
        assertThatThrownBy(() -> f.listener.onBatch(List.of(row("a",0)),Optional.of(new SourcePosition("committed"))))
                .isInstanceOfSatisfying(TapstateException.class,error -> assertThat(error.code()).isEqualTo(IoError.WORKLOAD_CLAIM_FENCED));
        assertThat(f.batches).hasSize(1); assertThat(f.delivered).isEmpty();
    }
    @Test void aBatchLargerThanHeadroomMakesProgressAsItsOriginalRowsAreConfirmed()throws Exception {
        var f=new Fixture(2); f.acknowledgeEveryRow=true; f.start();
        try(var executor=Executors.newSingleThreadExecutor()) {
            executor.submit(() -> f.listener.onBatch(List.of(row("a",0),row("b",1),row("a",2),row("b",3)),Optional.of(new SourcePosition("large")))).get(3,TimeUnit.SECONDS);
        }
        assertThat(f.delivered).hasSize(4); assertThat(f.batches).hasSize(1);
    }
    @Test void closingABlockedProducerStopsFurtherRowsWithoutBorrowingAnotherRun()throws Exception {
        var f=new Fixture(2); f.start();
        try(var executor=Executors.newSingleThreadExecutor()) {
            var pending=executor.submit(() -> f.listener.onBatch(List.of(row("a",0),row("b",1),row("a",2)),Optional.of(new SourcePosition("blocked"))));
            assertThat(f.blocked.await(3,TimeUnit.SECONDS)).isTrue(); assertThat(f.delivered).hasSize(2); assertThat(pending.isDone()).isFalse();
            f.subscription.close(); assertThatThrownBy(() -> pending.get(3,TimeUnit.SECONDS)).hasCauseInstanceOf(TapstateException.class);
            assertThat(f.delivered).hasSize(2); assertThat(f.source.closed).hasValue(1);
        }
    }
    @Test void aSourceErrorCannotBeFollowedByALateSuccessfulBatch() {
        var f=new Fixture(8); f.start(); var defect=new IllegalStateException("source-invariant"); f.listener.onError(defect);
        assertThatThrownBy(() -> f.listener.onBatch(List.of(row("a",0)),Optional.of(new SourcePosition("late")))).isSameAs(defect);
        assertThat(f.batches).isEmpty(); assertThat(f.delivered).isEmpty();
    }
    @Test void anUnprovenResumeCannotOpenTheSource() {
        var f=new Fixture(8);
        assertUnavailable(() -> f.writer.start(f.port,CONFIG,CaptureStart.resume(new SourcePosition("invented")),new CaptureHealth()));
        assertThat(f.port.opens).hasValue(0);
    }
    @Test void returnedSourceIsClosedOnlyOnceAndNeverAcknowledgedAfterClose() {
        var f=new Fixture(8); f.start(); f.subscription.close(); f.subscription.close();
        assertThat(f.source.closed).hasValue(1);
        assertUnavailable(() -> f.subscription.acknowledge(new SourcePosition("late")));
    }
    @Test void aFastTableCannotBorrowTheCapacityHeldByAnEarlierSlowTable()throws Exception {
        var f=new Fixture(2); f.acknowledgeOnlyA=true; f.expectedBlockedAfter=3; f.start();
        try(var executor=Executors.newSingleThreadExecutor()) {
            var pending=executor.submit(() -> f.listener.onBatch(List.of(row("a",0),row("b",1),row("a",2),row("a",3)),Optional.of(new SourcePosition("slow-table"))));
            assertThat(f.blocked.await(3,TimeUnit.SECONDS)).isTrue();assertThat(f.delivered).hasSize(3);assertThat(pending.isDone()).isFalse();
            f.acknowledge(f.delivered.get(1));pending.get(3,TimeUnit.SECONDS);assertThat(f.delivered).hasSize(4);
        }
    }
    @Test void aSuccessorWindowCannotSupplyHeadroomForAnOldProducer()throws Exception {
        var f=new Fixture(2); f.start();
        try(var executor=Executors.newSingleThreadExecutor()) {
            var pending=executor.submit(() -> f.listener.onBatch(List.of(row("a",0),row("b",1),row("a",2)),Optional.of(new SourcePosition("old-run"))));
            assertThat(f.blocked.await(3,TimeUnit.SECONDS)).isTrue();
            f.progress.updateAndGet(old -> {
                var nextCapture=new WorkloadClaimFence(CAPTURE.key(),OWNER,6,0,2);
                var nextPipeline=new WorkloadClaimFence(PIPELINE.key(),OWNER,3,9,2);
                var next=new DirectCaptureWindow(2,nextCapture,nextPipeline,null,0,0,null,null);
                return new DirectChannelProgress(CONTEXT,old.tables(),old.revision()+1,next,old.current(),old.cursor(),null);
            });
            assertThatThrownBy(() -> pending.get(3,TimeUnit.SECONDS)).hasCauseInstanceOf(TapstateException.class);assertThat(f.delivered).hasSize(2);
        }
    }
    private static Envelope row(String table,int value) { return Envelope.insert(1,table,Map.of("id",value),null); }
    private static void assertUnavailable(Runnable action) {
        assertThatThrownBy(action::run).isInstanceOfSatisfying(TapstateException.class,error -> assertThat(error.code()).isEqualTo(IoError.DIRECT_CHANNEL_UNAVAILABLE));
    }
    /** Explicit confirmed control values isolate producer sequencing; native adapter transactions are tested separately. */
    private static final class Fixture {
        final Store store=new Store(); final Port port=new Port(); final Source source=new Source();
        final AtomicBoolean active=new AtomicBoolean(true),refuseBatch=new AtomicBoolean();
        final List<Envelope> delivered=new CopyOnWriteArrayList<>(); final List<DirectBatch> batches=new CopyOnWriteArrayList<>();
        final CountDownLatch blocked=new CountDownLatch(1); final AtomicReference<DirectChannelProgress> progress;
        final DirectCaptureWriter writer; final int capacity; CaptureStartedListener listener; Subscription subscription;
        volatile Runnable afterReceipt=() -> {}; volatile boolean acknowledgeEveryRow,acknowledgeOnlyA; volatile int expectedBlockedAfter;
        Fixture(int capacity) {
            this.capacity=capacity; expectedBlockedAfter=capacity;
            var cursor=new ConsumerOffset(CONTEXT.consumerId(),Map.of(),null,List.of(),null,0,Map.of(),ConsumerProgressKind.DIRECT_SOURCE);
            var window=new DirectCaptureWindow(1,CAPTURE,PIPELINE,null,0,0,null,null);
            progress=new AtomicReference<>(new DirectChannelProgress(CONTEXT,Set.of("a","b"),0,window,null,cursor,null));
            writer=new DirectCaptureWriter(progress.get(),CAPTURE,PIPELINE,store,event -> {
                if(event.position().token()!=null)assertThat(batches).isNotEmpty();
                delivered.add(event);if(acknowledgeEveryRow || acknowledgeOnlyA && event.src().equals("a"))acknowledge(event);
            },capacity,() -> {if(!active.get())throw new TapstateException(IoError.WORKLOAD_CLAIM_FENCED,Map.of(),null);});
        }
        void start() { subscription=writer.start(port,CONFIG,CaptureStart.present(),new CaptureHealth()); }
        void acknowledge(Envelope event) {
            progress.updateAndGet(old -> {
                var cursor=old.cursor(); var byTable=new HashMap<>(cursor.sinkAckedByTable());byTable.put(event.src(),event.position());
                var updated=new ConsumerOffset(cursor.pipelineId(),Map.of(),cursor.sinkAcked(),List.of(),null,0,byTable,ConsumerProgressKind.DIRECT_SOURCE);
                return new DirectChannelProgress(CONTEXT,old.tables(),old.revision()+1,old.current(),null,updated,null);
            });
        }
        private final class Store implements DirectChannelStore {
            public DirectChannelStore guardedBy(Runnable guard) { return this; }
            public DirectChannelProgress load(DirectChannelContext context) {
                assertThat(context).isEqualTo(CONTEXT);if(delivered.size()>=expectedBlockedAfter)blocked.countDown();return progress.get();
            }
            public boolean confirmAnchor(DirectChannelContext context,long epoch,String token,WorkloadClaimFence capture,WorkloadClaimFence pipeline) {
                assertThat(context).isEqualTo(CONTEXT);assertThat(epoch).isEqualTo(1);assertThat(capture).isEqualTo(CAPTURE);assertThat(pipeline).isEqualTo(PIPELINE);
                var old=progress.get();var anchored=new DirectCaptureWindow(1,CAPTURE,PIPELINE,token,0,0,null,null);
                var next=new ConsumerOffset(CONTEXT.consumerId(),Map.of(),new ChainPosition(SourceOrder.snapshotRow(1),token),List.of(),null,0,Map.of(),ConsumerProgressKind.DIRECT_SOURCE);
                progress.set(new DirectChannelProgress(CONTEXT,old.tables(),old.revision()+1,anchored,null,next,null));return true;
            }
            public boolean recordBatch(DirectBatch batch,long expected,WorkloadClaimFence capture,WorkloadClaimFence pipeline) {
                assertThat(capture).isEqualTo(CAPTURE);assertThat(pipeline).isEqualTo(PIPELINE);if(refuseBatch.get())return false;
                var old=progress.get();assertThat(expected).isEqualTo(old.current().recordedThrough());assertThat(batch.ordinal()).isEqualTo(expected+1);
                batches.add(batch);var next=new DirectCaptureWindow(1,CAPTURE,PIPELINE,old.current().anchor(),batch.ordinal(),0,batch.boundary(),"b".repeat(64));
                progress.set(new DirectChannelProgress(CONTEXT,old.tables(),old.revision()+1,next,null,old.cursor(),null));afterReceipt.run();return true;
            }
            public List<DirectBatch> pending(DirectChannelContext context,long epoch) { throw new AssertionError("producer does not adopt old batches"); }
            public OptionalLong beginCapture(DirectChannelContext context,long revision,long epoch,WorkloadClaimFence capture,WorkloadClaimFence pipeline) { throw new AssertionError("coordinator opens the epoch"); }
            public boolean pinSnapshot(DirectChannelContext context,long revision,long epoch,String token,WorkloadClaimFence pipeline) { throw new AssertionError("snapshot preparation owns its pin"); }
            public boolean beginWriterRun(DirectChannelContext context,long epoch,String run,Map<String,List<String>> expected,WorkloadClaimFence pipeline) { throw new AssertionError("the compiled writer roster owns this command"); }
            public boolean advanceWriter(DirectChannelContext context,long epoch,String run,String table,String writer,WriterProgress report,WorkloadClaimFence pipeline) { throw new AssertionError("producer cannot acknowledge its own rows"); }
        }
        private final class Port implements CapturePort {
            final AtomicInteger opens=new AtomicInteger();
            public Subscription cdc(CaptureConfig config,CaptureStart start,CaptureListener target) {
                opens.incrementAndGet();listener=(CaptureStartedListener)target;listener.onStart(new SourcePosition("start"));return source;
            }
            public CaptureBatch snapshot(CaptureConfig config) { throw new AssertionError("tail cannot read a new snapshot"); }
            public ConnectionReport testConnection(CaptureConfig config) { throw new AssertionError("tail cannot probe another context"); }
            public DiscoveredSchema discoverSchema(CaptureConfig config) { throw new AssertionError("actual host preparation owns discovery"); }
        }
        private static final class Source implements Subscription {
            final AtomicInteger closed=new AtomicInteger();
            public void acknowledge(SourcePosition position) { throw new AssertionError("producer does not acknowledge its own delivery"); }
            public void close() { closed.incrementAndGet(); }
        }
    }
}
