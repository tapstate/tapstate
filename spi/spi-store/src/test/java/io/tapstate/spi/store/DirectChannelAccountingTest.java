package io.tapstate.spi.store;

import io.tapstate.core.event.ChainPosition;
import io.tapstate.core.event.SourceOrder;
import java.util.*;
import org.junit.jupiter.api.Test;
import static org.assertj.core.api.Assertions.*;

class DirectChannelAccountingTest {
    private static final PipelineIntentScope SCOPE = new PipelineIntentScope("cluster",1,"incarnation","pipeline",1);
    private static final DirectChannelContext CONTEXT = new DirectChannelContext(SCOPE,"channel","source","capture");
    private static final WorkloadClaimFence PIPELINE = fence(WorkloadClaimType.PIPELINE_ACTUATION,"pipeline");
    private static final WorkloadClaimFence CAPTURE = fence(WorkloadClaimType.CAPTURE,"capture");
    private static WorkloadClaimFence fence(WorkloadClaimType type,String id) {
        return new WorkloadClaimFence(new WorkloadClaimKey("cluster",type,id),new WorkloadOwner("node","boot"),1,1,1);
    }
    private static ChainPosition at(long sequence,String token) { return new ChainPosition(new SourceOrder(1,sequence),token); }
    private static DirectBatch batch(long ordinal,long sequence,Map<String,Long> targets) {
        return new DirectBatch(CONTEXT,1,ordinal,at(sequence,"token-"+sequence),targets);
    }
    private static DirectCaptureWindow window(long count,ChainPosition last) {
        return new DirectCaptureWindow(1,CAPTURE,PIPELINE,"anchor",count,0,last,count == 0 ? null : "a".repeat(64));
    }
    private static ConsumerOffset cursor(Map<String,ChainPosition> acked,long snapshot) {
        return new ConsumerOffset(CONTEXT.consumerId(),Map.of(),at(SourceOrder.SNAPSHOT_SEQ,"anchor"),List.of(),snapshot == 0 ? null : "seam",snapshot,acked,ConsumerProgressKind.DIRECT_SOURCE);
    }
    private static DirectChannelProgress progress(Map<String,List<String>> roster,ConsumerOffset cursor) {
        return new DirectChannelProgress(CONTEXT,roster.keySet(),0,window(0,null),null,cursor,
                new DirectWriterRun(1,PIPELINE,new WriterRun("run",roster,Map.of(),cursor.snapshotEpoch() == 0 ? null : cursor.snapshotEpoch())));
    }
    private static WriterProgress point(long sequence) { return new WriterProgress(new SourceOrder(1,sequence),sequence == Long.MAX_VALUE ? null : at(sequence,"writer-token")); }
    private static DirectChannelProgress with(DirectChannelProgress before,DirectChannelAccounting.Report report) {
        return new DirectChannelProgress(before.context(),before.tables(),before.revision(),before.current(),null,report.cursor(),report.writers());
    }

    private static DirectChannelAccounting.Settled settle(ConsumerOffset cursor,DirectCaptureWindow window,List<DirectBatch> batches) {
        return DirectChannelAccounting.settle(new DirectChannelProgress(CONTEXT,Set.of("orders","products"),0,window,null,cursor,null),batches);
    }

    @Test void aMissingOrSlowWriterHoldsItsTableAndNeverAdvancesThePrivateSourceCheckpoint() {
        var initial = progress(Map.of("orders",List.of("fast","slow")),cursor(Map.of(),0));
        var fast = DirectChannelAccounting.report(initial,"orders","fast",point(10));
        assertThat(fast.cursor()).isEqualTo(initial.cursor());
        var slow = DirectChannelAccounting.report(with(initial,fast),"orders","slow",point(3));
        assertThat(slow.cursor().sinkAckedByTable()).containsEntry("orders",at(3,"writer-token"));
        assertThat(slow.cursor().sinkAcked()).isEqualTo(initial.cursor().sinkAcked());
        var late = DirectChannelAccounting.report(with(initial,slow),"orders","fast",point(1));
        assertThat(late).isEqualTo(slow);
    }

    @Test void onlyAContiguousCompletedPrefixSettlesAndQuietTablesDoNotBlockIt() {
        var batches = List.of(batch(1,2,Map.of("orders",1L)),batch(2,4,Map.of("products",3L)),batch(3,6,Map.of("orders",5L)));
        var first = settle(cursor(Map.of("orders",at(5,null)),0),window(3,at(6,"token-6")),batches);
        assertThat(first.through()).isOne(); assertThat(first.cursor().sinkAcked()).isEqualTo(batches.getFirst().boundary());
        var all = settle(cursor(Map.of("orders",at(5,null),"products",at(3,null)),0),window(3,at(6,"token-6")),batches);
        assertThat(all.through()).isEqualTo(3); assertThat(all.cursor().sinkAcked()).isEqualTo(batches.getLast().boundary());
    }

    @Test void anEmptyBatchCanSettleItsActualSourceBoundaryWithoutFabricatedWriterAcknowledgements() {
        var b = batch(1,0,Map.of());
        var settled = settle(cursor(Map.of(),0),window(1,b.boundary()),List.of(b));
        assertThat(settled.cursor().sinkAcked()).isEqualTo(b.boundary()); assertThat(settled.cursor().sinkAckedByTable()).isEmpty();
    }

    @Test void aMissingManifestCannotMasqueradeAsAnEmptyPendingQueue() {
        assertThatThrownBy(() -> settle(cursor(Map.of(),0),window(1,at(2,"token-2")),List.of()))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> settle(cursor(Map.of(),0),window(1,at(2,"token-2")),List.of(batch(2,2,Map.of()))))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test void aPendingBatchOfAnotherPrivateChannelCannotSupplyThisChannelsCheckpoint() {
        var other = new DirectChannelContext(SCOPE,"another-channel","other-source","another-capture");
        var batch = new DirectBatch(other,1,1,at(2,"foreign"),Map.of());
        assertThatThrownBy(() -> settle(cursor(Map.of(),0),window(1,batch.boundary()),List.of(batch)))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test void aBlockedPrefixStillRequiresEveryLaterManifestToHaveIncreasingSourceOrder() {
        var batches = List.of(batch(1,4,Map.of("orders",3L)),batch(2,2,Map.of("orders",1L)));
        assertThatThrownBy(() -> settle(cursor(Map.of(),0),window(2,batches.getLast().boundary()),batches))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test void writerAccountingCannotReportAnEpochBeyondItsOriginalPrivateCapture() {
        var future = new WriterProgress(new SourceOrder(2,0),new ChainPosition(new SourceOrder(2,0),"future"));
        assertThatThrownBy(() -> new DirectWriterRun(1,PIPELINE,new WriterRun("run",Map.of("orders",List.of("writer")),Map.of("orders",Map.of("writer",future)),null)))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test void aFirstSnapshotWaitingBoundIsRetainedWithoutCompletingOrFailingTheLoad() {
        var initial = progress(Map.of("orders",List.of("writer")),cursor(Map.of(),1));
        var waiting = new WriterProgress(new SourceOrder(0,(1L << 47)-2),null);
        var report = DirectChannelAccounting.report(initial,"orders","writer",waiting);
        assertThat(report.cursor()).isEqualTo(initial.cursor());
        assertThat(report.writers().accounting().progressFor("orders")).containsEntry("writer",waiting);
        var complete = DirectChannelAccounting.report(with(initial,report),"orders","writer",point(SourceOrder.SNAPSHOT_SEQ));
        assertThat(complete.cursor().snapshotCompletedTables()).containsExactly("orders");
    }

    @Test void snapshotCompletionUsesOnlyTheOwningSeamAndDoesNotReplaceThePrivateSourceCheckpoint() {
        var initial = progress(Map.of("orders",List.of("writer")),cursor(Map.of(),1));
        var report = DirectChannelAccounting.report(initial,"orders","writer",point(SourceOrder.SNAPSHOT_SEQ));
        assertThat(report.cursor().snapshotCompletedTables()).containsExactly("orders");
        assertThat(report.cursor().sinkAckedByTable()).containsEntry("orders",at(SourceOrder.SNAPSHOT_SEQ,"seam"));
        assertThat(report.cursor().sinkAcked()).isEqualTo(initial.cursor().sinkAcked());
    }
}
