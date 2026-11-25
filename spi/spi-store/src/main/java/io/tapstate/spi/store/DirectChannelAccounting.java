package io.tapstate.spi.store;

import io.tapstate.core.event.ChainPosition;
import io.tapstate.core.event.SourceOrder;
import java.util.*;

/** Pure writer folding and contiguous completion; the owner proves transaction fencing and cross-resource receipt hashes. */
public final class DirectChannelAccounting {
    private DirectChannelAccounting() {}
    public record Report(ConsumerOffset cursor,DirectWriterRun writers) {}
    public record Settled(ConsumerOffset cursor,long through) {}
    public static Report report(DirectChannelProgress original,String table,String writer,WriterProgress point) {
        var before = Objects.requireNonNull(original.writerRun(),"writerRun");
        var accounting = before.accounting().advance(table,writer,point);
        var writers = new DirectWriterRun(before.epoch(),before.commandFence(),accounting); var cursor = original.cursor();
        var landed = WriterFrontier.landed(accounting,table);
        if (landed.isEmpty() || landed.get().durableThrough().epoch() == 0 || cursor.snapshotEpoch() > 0 && !WriterFrontier.passedLoad(landed.get(),cursor.snapshotEpoch())) return new Report(cursor,writers);
        var confirmed = WriterFrontier.confirmedAt(landed.get());
        if (confirmed.order().seq() == SourceOrder.SNAPSHOT_SEQ) confirmed = new ChainPosition(confirmed.order(),
                confirmed.order().epoch() == cursor.snapshotEpoch() ? cursor.cdcStartPosition() : null);
        var byTable = new HashMap<>(cursor.sinkAckedByTable()); var prior = byTable.get(table);
        if (prior == null || confirmed.order().compareTo(prior.order()) > 0 || confirmed.order().equals(prior.order()) && prior.token() == null && confirmed.token() != null)
            byTable.put(table,confirmed);
        var complete = new TreeSet<>(cursor.snapshotCompletedTables());
        if (cursor.snapshotEpoch() > 0 && WriterFrontier.passedLoad(landed.get(),cursor.snapshotEpoch())) complete.add(table);
        return new Report(new ConsumerOffset(cursor.pipelineId(),Map.of(),cursor.sinkAcked(),List.copyOf(complete),cursor.cdcStartPosition(),
                cursor.snapshotEpoch(),byTable,ConsumerProgressKind.DIRECT_SOURCE),writers);
    }
    public static Settled settle(DirectChannelProgress progress,List<DirectBatch> pending) {
        Objects.requireNonNull(progress,"progress"); pending = List.copyOf(pending);
        ConsumerOffset original = progress.cursor(); DirectCaptureWindow window = Objects.requireNonNull(progress.current(),"current");
        if (original.progressKind() != ConsumerProgressKind.DIRECT_SOURCE || window.anchor() == null || pending.size() != window.pendingCount())
            throw new IllegalArgumentException("direct batch closure is not proven");
        long ordinal = window.settledThrough(); long through = ordinal; ChainPosition confirmed = original.sinkAcked(); boolean blocked = false;
        SourceOrder previous = confirmed.order();
        if (!pending.isEmpty() && !window.lastBoundary().equals(pending.getLast().boundary())) throw new IllegalArgumentException("direct batch receipt does not close the pending queue");
        for (var batch : pending) {
            if (!batch.context().equals(progress.context()) || !progress.tables().containsAll(batch.targets().keySet())
                    || batch.epoch() != window.epoch() || batch.ordinal() != ++ordinal || batch.boundary().order().compareTo(previous) <= 0)
                throw new IllegalArgumentException("direct batch order or context is not contiguous");
            previous = batch.boundary().order();
            boolean landed = !blocked;
            for (var target : batch.targets().entrySet()) {
                var ack = original.sinkAckedByTable().get(target.getKey());
                if (ack == null || ack.order().epoch() != batch.epoch() || ack.order().seq() < target.getValue()) landed = false;
            }
            if (!landed) { blocked = true; continue; }
            if (confirmed != null && batch.boundary().order().compareTo(confirmed.order()) <= 0) throw new IllegalArgumentException("direct checkpoint is not monotonic");
            confirmed = batch.boundary(); through = batch.ordinal();
        }
        return new Settled(new ConsumerOffset(original.pipelineId(),Map.of(),confirmed,original.snapshotCompletedTables(),original.cdcStartPosition(),
                original.snapshotEpoch(),original.sinkAckedByTable(),ConsumerProgressKind.DIRECT_SOURCE),through);
    }
}
