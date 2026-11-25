package io.tapstate.spi.store;

import io.tapstate.core.event.ChainPosition;
import io.tapstate.core.event.SourceOrder;
import org.junit.jupiter.api.Test;
import java.util.*;
import static org.assertj.core.api.Assertions.*;

class SrsConsumerAcknowledgementsTest {
    private static final WorkloadClaimFence FENCE = new WorkloadClaimFence(new WorkloadClaimKey("cluster",WorkloadClaimType.PIPELINE_ACTUATION,"pipeline"),
            new WorkloadOwner("node","boot"),3,1,1);
    private static SrsConsumerWriterRun run(Map<String,List<String>> expected) { return new SrsConsumerWriterRun(1,FENCE,"original-run",expected,Map.of()); }
    private static ConsumerOffset cursor(Set<String> tables,long snapshotEpoch) {
        var sequences = new LinkedHashMap<String,Long>(); tables.forEach(table -> sequences.put(table,-1L));
        return new ConsumerOffset("consumer",sequences,null,List.of(),snapshotEpoch == 0 ? null : "seam",snapshotEpoch,Map.of(),ConsumerProgressKind.SRS);
    }
    private static WriterProgress point(long epoch,long sequence,String token) {
        var order = new SourceOrder(epoch,sequence); return new WriterProgress(order,token == null ? null : new ChainPosition(order,token));
    }

    @Test void missingWritersHoldProgressAndLateReportsNeverReplaceTheSlowestWriterMinimum() {
        var original = run(Map.of("orders",List.of("fast","slow"))); var initial = cursor(Set.of("orders"),0);
        var first = SrsConsumerAcknowledgements.advance(original,initial,"orders","fast",point(1,10,"t10"));
        assertThat(first.cursor()).isEqualTo(initial);
        var second = SrsConsumerAcknowledgements.advance(first.writerRun(),first.cursor(),"orders","slow",point(1,6,"t6"));
        assertThat(second.cursor().sinkAcked()).isEqualTo(new ChainPosition(new SourceOrder(1,6),"t6"));
        assertThat(second.cursor().sinkAckedByTable()).containsEntry("orders",second.cursor().sinkAcked());
        var late = SrsConsumerAcknowledgements.advance(second.writerRun(),second.cursor(),"orders","fast",point(1,3,"old"));
        assertThat(late).isEqualTo(second);
        assertThat(late.writerRun().commandFence()).isSameAs(FENCE); assertThat(late.writerRun().membershipRevision()).isOne();
        assertThat(late.cursor().perTableSeq()).containsEntry("orders",-1L);
    }

    @Test void aDurableBoundDoesNotPretendItsLowerTokenReachedTheSameOrder() {
        var original = run(Map.of("orders",List.of("a","b"))); var initial = cursor(Set.of("orders"),0);
        var a = SrsConsumerAcknowledgements.advance(original,initial,"orders","a",point(1,10,"t10"));
        var report = new WriterProgress(new SourceOrder(1,9),new ChainPosition(new SourceOrder(1,7),"t7"));
        var b = SrsConsumerAcknowledgements.advance(a.writerRun(),a.cursor(),"orders","b",report);
        assertThat(b.cursor().sinkAckedByTable()).containsEntry("orders",new ChainPosition(new SourceOrder(1,9),null));
        assertThat(b.cursor().sinkAcked()).isEqualTo(new ChainPosition(new SourceOrder(1,7),"t7"));
        var tokenCatchesUp = SrsConsumerAcknowledgements.advance(b.writerRun(),b.cursor(),"orders","b",point(1,8,"t8"));
        assertThat(tokenCatchesUp.cursor().sinkAcked()).isEqualTo(new ChainPosition(new SourceOrder(1,8),"t8"));
        assertThat(tokenCatchesUp.cursor().sinkAckedByTable()).isEqualTo(b.cursor().sinkAckedByTable());
    }

    @Test void independentTableSequencesNeverBecomeAFabricatedGlobalCdcCursor() {
        var original = run(Map.of("orders",List.of("a"),"products",List.of("b"))); var initial = cursor(original.expected().keySet(),0);
        var a = SrsConsumerAcknowledgements.advance(original,initial,"orders","a",point(1,1,"source-token-100"));
        var b = SrsConsumerAcknowledgements.advance(a.writerRun(),a.cursor(),"products","b",point(1,100,"source-token-5"));
        assertThat(b.cursor().sinkAckedByTable()).containsKeys("orders","products");
        assertThat(b.cursor().sinkAcked()).isNull(); assertThat(WriterFrontier.resumableAt(b.writerRun().withSnapshotEpoch(null))).isEmpty();
    }

    @Test void tablesShareOnlyTheirSnapshotSeamAndCompletionWaitsForEveryExpectedWriter() {
        var original = run(Map.of("orders",List.of("a"),"products",List.of("b"))); var initial = cursor(original.expected().keySet(),7);
        var a = SrsConsumerAcknowledgements.advance(original,initial,"orders","a",point(7,SourceOrder.SNAPSHOT_SEQ,"seam"));
        assertThat(a.cursor().snapshotCompletedTables()).containsExactly("orders"); assertThat(a.cursor().sinkAcked()).isNull();
        var b = SrsConsumerAcknowledgements.advance(a.writerRun(),a.cursor(),"products","b",point(7,3,"products-cdc"));
        assertThat(b.cursor().snapshotCompletedTables()).containsExactly("orders","products");
        assertThat(b.cursor().sinkAcked()).isEqualTo(new ChainPosition(SourceOrder.snapshotRow(7),"seam"));
        var cdc = SrsConsumerAcknowledgements.advance(b.writerRun(),b.cursor(),"orders","a",point(7,500,"orders-cdc"));
        assertThat(cdc.cursor().sinkAcked()).isEqualTo(b.cursor().sinkAcked());
    }

    @Test void anEarlierEpochBoundCannotBecomeASnapshotRecoveryCursor() {
        var original = run(Map.of("orders",List.of("writer"))); var initial = cursor(Set.of("orders"),7);
        var beforeLoad = SrsConsumerAcknowledgements.advance(original,initial,"orders","writer",point(6,Long.MAX_VALUE,"earlier"));
        assertThat(beforeLoad.cursor()).isEqualTo(initial);
        var loaded = SrsConsumerAcknowledgements.advance(beforeLoad.writerRun(),initial,"orders","writer",point(7,SourceOrder.SNAPSHOT_SEQ,"seam"));
        assertThat(loaded.cursor().snapshotCompletedTables()).containsExactly("orders");
        assertThat(loaded.cursor().sinkAcked()).isEqualTo(new ChainPosition(SourceOrder.snapshotRow(7),"seam"));
    }

    @Test void snapshotOrderIsReadFromTheOwningCursorWhenItBecomesAvailable() {
        var original = run(Map.of("orders",List.of("writer")));
        assertThat(original.withSnapshotEpoch(null).snapshotEpoch()).isNull();
        var pinned = cursor(Set.of("orders"),7);
        var loaded = SrsConsumerAcknowledgements.advance(original,pinned,"orders","writer",point(7,SourceOrder.SNAPSHOT_SEQ,"seam"));
        assertThat(loaded.cursor().snapshotEpoch()).isEqualTo(7); assertThat(loaded.cursor().snapshotCompletedTables()).containsExactly("orders");
    }

    @Test void onlyTheOwningSnapshotPinCanSupplyASharedRecoveryToken() {
        var original = run(Map.of("orders",List.of("a"),"products",List.of("b")));
        var pinned = cursor(original.expected().keySet(),7);
        var a = SrsConsumerAcknowledgements.advance(original,pinned,"orders","a",point(7,SourceOrder.SNAPSHOT_SEQ,"foreign-a"));
        var b = SrsConsumerAcknowledgements.advance(a.writerRun(),a.cursor(),"products","b",point(7,SourceOrder.SNAPSHOT_SEQ,"foreign-b"));
        var seam = new ChainPosition(SourceOrder.snapshotRow(7),"seam");
        assertThat(b.cursor().sinkAcked()).isEqualTo(seam);
        assertThat(b.cursor().sinkAckedByTable()).containsEntry("orders",seam).containsEntry("products",seam);
    }

    @Test void anUnpinnedSnapshotMarkerCannotInventARecoverySeamOrCompleteTheLoad() {
        var original = run(Map.of("orders",List.of("writer")));
        var first = SrsConsumerAcknowledgements.advance(original,cursor(Set.of("orders"),0),"orders","writer",point(7,SourceOrder.SNAPSHOT_SEQ,"unowned"));
        assertThat(first.cursor().sinkAcked()).isNull();
        assertThat(first.cursor().snapshotCompletedTables()).isEmpty();
        assertThat(first.cursor().sinkAckedByTable()).containsEntry("orders",new ChainPosition(SourceOrder.snapshotRow(7),null));
        var pinned = new ConsumerOffset("consumer",first.cursor().perTableSeq(),null,List.of(),"seam",7,first.cursor().sinkAckedByTable(),ConsumerProgressKind.SRS);
        var retry = SrsConsumerAcknowledgements.advance(first.writerRun(),pinned,"orders","writer",point(7,SourceOrder.SNAPSHOT_SEQ,"unowned"));
        assertThat(retry.cursor().sinkAcked()).isEqualTo(new ChainPosition(SourceOrder.snapshotRow(7),"seam"));
        assertThat(retry.cursor().sinkAckedByTable()).containsEntry("orders",retry.cursor().sinkAcked());
    }

    @Test void laterSnapshotMarkersProveTheOriginalLoadWithoutReplacingItsSavedSeam() {
        var original = run(Map.of("orders",List.of("a"),"products",List.of("b")));
        var pinned = cursor(original.expected().keySet(),7);
        var a = SrsConsumerAcknowledgements.advance(original,pinned,"orders","a",point(8,SourceOrder.SNAPSHOT_SEQ,"later-a"));
        var b = SrsConsumerAcknowledgements.advance(a.writerRun(),a.cursor(),"products","b",point(8,SourceOrder.SNAPSHOT_SEQ,"later-b"));
        assertThat(b.cursor().sinkAcked()).isEqualTo(new ChainPosition(SourceOrder.snapshotRow(7),"seam"));
        assertThat(b.cursor().sinkAckedByTable()).containsEntry("orders",new ChainPosition(SourceOrder.snapshotRow(8),null));
        assertThat(b.cursor().snapshotCompletedTables()).containsExactly("orders","products");
    }

    @Test void writerAccountingIsImmutableAndRejectsUnknownOrDuplicateWritersAndWrongAuthority() {
        var names = new ArrayList<>(List.of("writer")); var expected = new LinkedHashMap<String,List<String>>(); expected.put("orders",names);
        var original = run(expected); names.add("late"); expected.clear();
        assertThat(original.expected()).containsEntry("orders",List.of("writer"));
        assertThatThrownBy(() -> original.expected().clear()).isInstanceOf(UnsupportedOperationException.class);
        assertThatThrownBy(() -> run(Map.of("orders",List.of("writer","writer")))).isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> new SrsConsumerWriterRun(1,new WorkloadClaimFence(new WorkloadClaimKey("cluster",WorkloadClaimType.CAPTURE,"capture"),FENCE.owner(),1,1,1),
                "run",Map.of("orders",List.of("writer")),Map.of())).isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> SrsConsumerAcknowledgements.advance(original,cursor(Set.of("orders"),0),"orders","unknown",point(1,1,"token")))
                .isInstanceOf(IllegalArgumentException.class);
    }
}
