package io.tapstate.runtime.srs;

import io.tapstate.core.common.TapstateException;
import io.tapstate.core.event.*;
import io.tapstate.core.model.PipelineNode;
import io.tapstate.spi.capture.*;
import io.tapstate.spi.store.*;
import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.Test;
import static org.assertj.core.api.Assertions.*;

class DirectSnapshotPreparationTest {
    private static final PipelineIntentScope SCOPE=new PipelineIntentScope("cluster",1,"cache","flow",7);
    private static final DirectChannelContext CONTEXT=new DirectChannelContext(SCOPE,"channel","input","capture");
    private static final WorkloadOwner OWNER=new WorkloadOwner("node","boot");
    private static final WorkloadClaimFence PIPELINE=new WorkloadClaimFence(new WorkloadClaimKey("cluster",WorkloadClaimType.PIPELINE_ACTUATION,"flow"),OWNER,2,3,1);
    private static final WorkloadClaimFence CAPTURE=new WorkloadClaimFence(new WorkloadClaimKey("cluster",WorkloadClaimType.CAPTURE,"capture"),OWNER,1,0,1);
    private static final CaptureConfig CONFIG=new CaptureConfig("fixture",Map.of(),List.of("a","b"),new PipelineNode("flow","input"));

    @Test void sameSessionPinAndOriginalAnchorPrecedeEveryRow() {
        var f=new Fixture(false,Set.of());var rows=new ArrayList<Envelope>();var loaded=new ArrayList<String>();
        try(var load=SnapshotPhase.openDirectPrepared(f.port,CONFIG,f.preparation)) {
            assertThat(f.actions).containsExactly("pin:sampled","anchor:sampled");
            load.read(rows::add,loaded::add);
        }
        assertThat(rows).hasSize(2).allSatisfy(row -> assertThat(row.position().order()).isEqualTo(SourceOrder.snapshotRow(1)));
        assertThat(loaded).containsExactly("a","b");assertThat(f.closed).hasValue(1);
    }
    @Test void partialRestartPreservesItsLoadOrderAndPinButResumesTheConfirmedSourcePrefix() {
        var f=new Fixture(true,Set.of("a"));var rows=new ArrayList<Envelope>();var loaded=new ArrayList<String>();
        assertThat(f.preparation.request()).isEqualTo(new SnapshotPreparation(1,new SourcePosition("original-seam")));
        try(var load=SnapshotPhase.openDirectPrepared(f.port,CONFIG,f.preparation)) {load.read(rows::add,loaded::add);}
        assertThat(f.actions).containsExactly("pin:original-seam","anchor:settled");
        assertThat(f.reads).containsExactly("b");assertThat(rows).singleElement().satisfies(row -> assertThat(row.position().order()).isEqualTo(SourceOrder.snapshotRow(1)));
        assertThat(loaded).containsExactly("b","a");
    }
    @Test void aFullyLandedLoadUsesOnlyRetainedFactsAndNeverOpensAnotherSnapshot() {
        var f=new Fixture(true,Set.of("a","b"));var loaded=new ArrayList<String>();
        try(var load=SnapshotPhase.openDirectPrepared(f.port,CONFIG,f.preparation)) {assertThat(load.read(row -> {throw new AssertionError("no row owed");},loaded::add)).isZero();}
        assertThat(f.actions).containsExactly("anchor:settled");assertThat(f.reads).isEmpty();assertThat(f.closed).hasValue(0);assertThat(loaded).containsExactly("a","b");
    }
    @Test void aRejectedPinCannotConfirmAnAnchorOrReadAnyRow() {
        var f=new Fixture(false,Set.of());f.allowPin=false;
        assertThatThrownBy(() -> SnapshotPhase.openDirectPrepared(f.port,CONFIG,f.preparation)).isInstanceOf(TapstateException.class);
        assertThat(f.actions).containsExactly("pin:sampled");assertThat(f.next).hasValue(0);assertThat(f.closed).hasValue(1);
    }
    @Test void aClosedPreparationCannotPublishLateMetadata() {
        var f=new Fixture(false,Set.of());f.preparation.beginRead();f.preparation.close();
        assertThatThrownBy(() -> f.preparation.prepare(f.preparation.request(),new SourcePosition("late"),Map.of("a",table("a"),"b",table("b"))))
                .isInstanceOf(TapstateException.class);
        assertThat(f.actions).isEmpty();
    }
    private static SourceTable table(String name){return new SourceTable(name,List.of(new SourceField("id","INT")),List.of("id"),List.of());}
    private static final class Fixture implements DirectChannelStore {
        final List<String> actions=new ArrayList<>(),reads=new ArrayList<>();final AtomicInteger closed=new AtomicInteger(),next=new AtomicInteger();
        final DirectChannelProgress original;final DirectSnapshotPreparation preparation;final CapturePort port;boolean allowPin=true;
        Fixture(boolean retained,Set<String> complete) {
            long epoch=retained?2:1;var point=retained?new ChainPosition(new SourceOrder(1,4),"settled"):null;
            var cursor=new ConsumerOffset(CONTEXT.consumerId(),Map.of(),point,List.copyOf(complete),retained?"original-seam":null,retained?1:0,Map.of(),ConsumerProgressKind.DIRECT_SOURCE);
            original=new DirectChannelProgress(CONTEXT,Set.of("a","b"),3,new DirectCaptureWindow(epoch,CAPTURE,PIPELINE,null,0,0,null,null),null,cursor,null);
            preparation=new DirectSnapshotPreparation(original,this,() -> {});port=new Port();
        }
        final class Port implements CapturePort,SnapshotSession.PreparedProvider {
            public SnapshotSession snapshotSession(CaptureConfig config){throw new AssertionError("unprepared snapshot");}
            public SnapshotSession snapshotSession(CaptureConfig config,SnapshotPreparation request) {
                return new SnapshotSession() {
                    boolean prepared;
                    public CaptureBatch read(String name) {
                        reads.add(name);
                        if(!prepared) {var tables=new HashMap<String,SourceTable>();config.streams().forEach(table -> tables.put(table,table(table)));
                            preparation.prepare(request,new SourcePosition("sampled"),tables);prepared=true;}
                        var rows=List.of(Envelope.read(1,name,Map.of("id",1),null)).iterator();
                        return new CaptureBatch() {
                            public boolean hasNext(){return rows.hasNext();}
                            public Envelope next(){assertThat(actions).hasSize(2);next.incrementAndGet();return rows.next();}
                            public Optional<SourcePosition> seam(){return Optional.of(new SourcePosition("sampled"));}
                            public void close(){}
                        };
                    }
                    public void close(){closed.incrementAndGet();}
                };
            }
            public CaptureBatch snapshot(CaptureConfig config){throw new AssertionError("unprepared read");}
            public Subscription cdc(CaptureConfig config,CaptureStart start,CaptureListener listener){throw new AssertionError("no tail here");}
            public ConnectionReport testConnection(CaptureConfig config){throw new AssertionError();}
            public DiscoveredSchema discoverSchema(CaptureConfig config){throw new AssertionError();}
        }
        public DirectChannelStore guardedBy(Runnable guard){return this;}
        public boolean pinSnapshot(DirectChannelContext context,long revision,long epoch,String token,WorkloadClaimFence pipeline){assertThat(context).isEqualTo(CONTEXT);assertThat(revision).isEqualTo(3);assertThat(epoch).isEqualTo(1);assertThat(pipeline).isEqualTo(PIPELINE);actions.add("pin:"+token);return allowPin;}
        public boolean confirmAnchor(DirectChannelContext context,long epoch,String token,WorkloadClaimFence capture,WorkloadClaimFence pipeline){assertThat(context).isEqualTo(CONTEXT);assertThat(epoch).isEqualTo(original.epoch());assertThat(capture).isEqualTo(CAPTURE);assertThat(pipeline).isEqualTo(PIPELINE);actions.add("anchor:"+token);return true;}
        public DirectChannelProgress load(DirectChannelContext context){throw new AssertionError();}
        public List<DirectBatch> pending(DirectChannelContext context,long epoch){throw new AssertionError();}
        public OptionalLong beginCapture(DirectChannelContext context,long revision,long epoch,WorkloadClaimFence capture,WorkloadClaimFence pipeline){throw new AssertionError();}
        public boolean beginWriterRun(DirectChannelContext context,long epoch,String run,Map<String,List<String>> expected,WorkloadClaimFence pipeline){throw new AssertionError();}
        public boolean recordBatch(DirectBatch original,long expected,WorkloadClaimFence capture,WorkloadClaimFence pipeline){throw new AssertionError();}
        public boolean advanceWriter(DirectChannelContext context,long epoch,String run,String table,String writer,WriterProgress report,WorkloadClaimFence pipeline){throw new AssertionError();}
    }
}
