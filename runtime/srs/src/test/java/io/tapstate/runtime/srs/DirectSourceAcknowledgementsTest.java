package io.tapstate.runtime.srs;

import io.tapstate.core.common.TapstateException;
import io.tapstate.core.event.ChainPosition;
import io.tapstate.core.event.SourceOrder;
import io.tapstate.spi.capture.SourcePosition;
import io.tapstate.spi.capture.Subscription;
import io.tapstate.spi.store.*;
import java.util.*;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.*;
import org.junit.jupiter.api.Test;
import static org.assertj.core.api.Assertions.*;

class DirectSourceAcknowledgementsTest {
    private static final PipelineIntentScope SCOPE = new PipelineIntentScope("cluster",1,"cache","flow",7);
    private static final DirectChannelContext CONTEXT = new DirectChannelContext(SCOPE,"channel","input","capture");
    private static final WorkloadOwner OWNER = new WorkloadOwner("node","boot");
    private static final WorkloadClaimFence PIPELINE = new WorkloadClaimFence(new WorkloadClaimKey("cluster",WorkloadClaimType.PIPELINE_ACTUATION,"flow"),OWNER,3,8,2);
    private static final WorkloadClaimFence CAPTURE = new WorkloadClaimFence(new WorkloadClaimKey("cluster",WorkloadClaimType.CAPTURE,"capture"),OWNER,5,0,2);

    @Test void anUnanchoredOriginalWindowTellsNothingUntilTheSourceConfirmsItsActualStart() {
        var current = new AtomicReference<>(state(1,CAPTURE,PIPELINE,null,0,0,null));
        withFollowed(current,() -> {},(followed,health,told) -> {
            assertThat(told).isEmpty();
            current.set(state(1,CAPTURE,PIPELINE,"anchor",0,0,null)); sample(followed);
            assertThat(told).containsExactly("anchor");
            assertThat(health.consecutiveAcknowledgeFailures()).isZero();
        });
    }
    @Test void pendingReceiptsAndFastTablePositionsCannotReleaseTheSourceAheadOfTheSettledPrefix() {
        var current = new AtomicReference<>(state(1,CAPTURE,PIPELINE,"anchor",2,0,null));
        withFollowed(current,() -> {},(followed,health,told) -> {
            assertThat(told).containsExactly("anchor"); sample(followed);
            current.set(state(1,CAPTURE,PIPELINE,"anchor",2,1,"first")); sample(followed);
            assertThat(told).containsExactly("anchor","first");
            current.set(state(1,CAPTURE,PIPELINE,"anchor",2,2,"last")); sample(followed); sample(followed);
            assertThat(told).containsExactly("anchor","first","last");
        });
    }
    @Test void laterEpochAndSameEpochDifferentFullClaimCannotAcknowledgeTheOldTail() {
        for (String changed : List.of("epoch","pipeline","capture")) {
            var current = new AtomicReference<>(state(1,CAPTURE,PIPELINE,"anchor",0,0,null));
            withFollowed(current,() -> {},(followed,health,told) -> {
                var nextPipeline = new WorkloadClaimFence(PIPELINE.key(),OWNER,3,9,2);
                var nextCapture = new WorkloadClaimFence(CAPTURE.key(),OWNER,6,0,2);
                current.set(state(changed.equals("epoch")?2:1,changed.equals("capture")?nextCapture:CAPTURE,
                        changed.equals("pipeline")?nextPipeline:PIPELINE,"anchor",0,0,null)); sample(followed);
                assertThat(told).containsExactly("anchor");
                assertThat(health.lastAcknowledgeFailureCode()).contains(IoError.DIRECT_CHANNEL_UNAVAILABLE.code());
            });
        }
    }
    @Test void revocationDuringTheDurableReadCannotReachTheOriginalSource() {
        var active = new AtomicBoolean(true);
        var current = new AtomicReference<>(state(1,CAPTURE,PIPELINE,"anchor",0,0,null));
        try (var acknowledgements = new SourceAcknowledgements()) {
            var told = new ArrayList<String>(); var health = new CaptureHealth();
            var channels = new Channels(() -> { active.set(false); return current.get(); });
            try (var followed = acknowledgements.follow(channels,current.get(),tail(told),health,() -> {
                if (!active.get()) throw new TapstateException(IoError.WORKLOAD_CLAIM_FENCED,Map.of(),null);
            })) {
                sample(followed); assertThat(told).isEmpty();
                assertThat(health.lastAcknowledgeFailureCode()).contains(IoError.WORKLOAD_CLAIM_FENCED.code());
            }
        }
    }
    private static DirectChannelProgress state(long epoch,WorkloadClaimFence capture,WorkloadClaimFence pipeline,
            String anchor,long recorded,long settled,String token) {
        var last = recorded==0?null:new ChainPosition(new SourceOrder(epoch,20),"last");
        var window = new DirectCaptureWindow(epoch,capture,pipeline,anchor,recorded,settled,last,last==null?null:"b".repeat(64));
        var point = anchor==null?null:settled==0?new ChainPosition(SourceOrder.snapshotRow(epoch),anchor):
                new ChainPosition(new SourceOrder(epoch,settled==recorded?20:10),token);
        var cursor = new ConsumerOffset(CONTEXT.consumerId(),Map.of(),point,List.of(),null,0,
                anchor==null?Map.of():Map.of("a",new ChainPosition(new SourceOrder(epoch,100),null)),ConsumerProgressKind.DIRECT_SOURCE);
        return new DirectChannelProgress(CONTEXT,Set.of("a","b"),recorded+settled+1,window,null,cursor,null);
    }
    private static Subscription tail(List<String> told) {
        return new Subscription() {
            @Override public void acknowledge(SourcePosition point) { told.add(point.token()); }
            @Override public void close() {}
        };
    }
    private static void sample(Subscription followed) { ((SourceAcknowledgements.Followed)followed).handOverQuietly(); }
    private static void withFollowed(AtomicReference<DirectChannelProgress> current,Runnable guard,Scenario scenario) {
        try (var acknowledgements = new SourceAcknowledgements()) {
            var health = new CaptureHealth(); var told = new CopyOnWriteArrayList<String>();
            try (var followed = acknowledgements.follow(new Channels(current::get),current.get(),tail(told),health,guard)) {
                scenario.run(followed,health,told);
            }
        }
    }
    @FunctionalInterface private interface Scenario { void run(Subscription followed,CaptureHealth health,List<String> told); }
    private record Channels(java.util.function.Supplier<DirectChannelProgress> read) implements DirectChannelStore {
        @Override public DirectChannelProgress load(DirectChannelContext context) { assertThat(context).isEqualTo(CONTEXT); return read.get(); }
        @Override public DirectChannelStore guardedBy(Runnable guard) { throw new AssertionError(); }
        @Override public List<DirectBatch> pending(DirectChannelContext c,long e) { throw new AssertionError(); }
        @Override public OptionalLong beginCapture(DirectChannelContext c,long r,long e,WorkloadClaimFence a,WorkloadClaimFence b) { throw new AssertionError(); }
        @Override public boolean confirmAnchor(DirectChannelContext c,long e,String s,WorkloadClaimFence a,WorkloadClaimFence b) { throw new AssertionError(); }
        @Override public boolean pinSnapshot(DirectChannelContext c,long r,long e,String s,WorkloadClaimFence f) { throw new AssertionError(); }
        @Override public boolean beginWriterRun(DirectChannelContext c,long e,String r,Map<String,List<String>> t,WorkloadClaimFence f) { throw new AssertionError(); }
        @Override public boolean recordBatch(DirectBatch b,long r,WorkloadClaimFence a,WorkloadClaimFence f) { throw new AssertionError(); }
        @Override public boolean advanceWriter(DirectChannelContext c,long e,String r,String t,String w,WriterProgress p,WorkloadClaimFence f) { throw new AssertionError(); }
    }
}
