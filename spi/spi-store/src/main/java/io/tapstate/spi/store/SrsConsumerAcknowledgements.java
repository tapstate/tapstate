package io.tapstate.spi.store;

import io.tapstate.core.event.ChainPosition;
import io.tapstate.core.event.SourceOrder;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;

/** Pure monotonic writer folding; the owning store supplies scope, membership and transaction fencing. */
public final class SrsConsumerAcknowledgements {
    private SrsConsumerAcknowledgements() {}
    public record Advance(SrsConsumerWriterRun writerRun,ConsumerOffset cursor) {}

    public static Advance advance(SrsConsumerWriterRun original,ConsumerOffset cursor,String table,String writer,WriterProgress report) {
        Objects.requireNonNull(original,"original"); Objects.requireNonNull(cursor,"cursor"); Objects.requireNonNull(report,"report");
        if (cursor.progressKind() != ConsumerProgressKind.SRS || !original.expected().getOrDefault(table,java.util.List.of()).contains(writer))
            throw new IllegalArgumentException("the report must belong to this consumer run's expected writer");
        var progress = new LinkedHashMap<>(original.progress());
        var byWriter = new LinkedHashMap<>(progress.getOrDefault(table,Map.of()));
        WriterProgress before = byWriter.get(writer);
        SourceOrder durable = before == null || report.durableThrough().compareTo(before.durableThrough()) > 0 ? report.durableThrough() : before.durableThrough();
        ChainPosition tokened = before == null ? null : before.lastTokened();
        if (report.lastTokened() != null && (tokened == null || report.lastTokened().order().compareTo(tokened.order()) > 0)) tokened = report.lastTokened();
        byWriter.put(writer,new WriterProgress(durable,tokened)); progress.put(table,byWriter);
        var saved = new SrsConsumerWriterRun(original.membershipRevision(),original.commandFence(),original.runId(),original.expected(),progress);
        WriterRun run = saved.withSnapshotEpoch(cursor.snapshotEpoch() == 0 ? null : cursor.snapshotEpoch());
        var answer = WriterFrontier.landed(run,table);
        if (answer.isEmpty() || run.snapshotEpoch() != null && !WriterFrontier.passedLoad(answer.get(),run.snapshotEpoch())) return new Advance(saved,cursor);
        var landed = answer.get(); var acked = new LinkedHashMap<>(cursor.sinkAckedByTable());
        ChainPosition confirmed = WriterFrontier.confirmedAt(landed), previous = acked.get(table);
        if (confirmed.order().seq() == SourceOrder.SNAPSHOT_SEQ) {
            String seam = confirmed.order().epoch() == cursor.snapshotEpoch() ? cursor.cdcStartPosition() : null;
            confirmed = new ChainPosition(confirmed.order(),seam);
        }
        if (previous == null || confirmed.order().compareTo(previous.order()) > 0
                || confirmed.order().equals(previous.order()) && previous.token() == null && confirmed.token() != null) acked.put(table,confirmed);
        var completed = new LinkedHashSet<>(cursor.snapshotCompletedTables());
        if (WriterFrontier.passedLoad(landed,run.snapshotEpoch())) completed.add(table);
        ChainPosition aggregate = cursor.sinkAcked(); var candidate = resumableAt(run,cursor);
        if (candidate.isPresent() && (run.snapshotEpoch() == null || candidate.get().order().compareTo(SourceOrder.snapshotRow(run.snapshotEpoch())) >= 0)
                && (aggregate == null || candidate.get().order().compareTo(aggregate.order()) > 0)) aggregate = candidate.get();
        var completedTables = new ArrayList<>(completed); completedTables.sort(String::compareTo);
        return new Advance(saved,new ConsumerOffset(cursor.pipelineId(),cursor.perTableSeq(),aggregate,completedTables,cursor.cdcStartPosition(),
                cursor.snapshotEpoch(),acked,cursor.progressKind()));
    }

    private static Optional<ChainPosition> resumableAt(WriterRun run,ConsumerOffset cursor) {
        var candidate = WriterFrontier.resumableAt(run);
        if (run.expected().size() == 1 && candidate.isPresent() && candidate.get().order().seq() != SourceOrder.SNAPSHOT_SEQ)
            return candidate;
        if (cursor.snapshotEpoch() == 0 || cursor.cdcStartPosition() == null) return Optional.empty();
        for (String table : run.expected().keySet()) {
            var landed = WriterFrontier.landed(run,table);
            if (landed.isEmpty() || !WriterFrontier.passedLoad(landed.get(),cursor.snapshotEpoch())) return Optional.empty();
        }
        // Writer markers prove landing; only the owning pin supplies a source recovery seam.
        return Optional.of(new ChainPosition(SourceOrder.snapshotRow(cursor.snapshotEpoch()),cursor.cdcStartPosition()));
    }
}
