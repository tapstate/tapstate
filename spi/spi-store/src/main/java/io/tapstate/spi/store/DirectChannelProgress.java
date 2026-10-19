package io.tapstate.spi.store;

import java.util.Objects;
import io.tapstate.core.event.ChainPosition;
import io.tapstate.core.event.SourceOrder;
import java.util.Set;

/** Private channel progress and its small ordering control; pending batches are a separate resource. */
public record DirectChannelProgress(DirectChannelContext context,Set<String> tables,long revision,
        DirectCaptureWindow current,DirectCaptureWindow retained,ConsumerOffset cursor,DirectWriterRun writerRun) {
    public DirectChannelProgress {
        Objects.requireNonNull(context,"context"); Objects.requireNonNull(cursor,"cursor"); tables = Set.copyOf(tables);
        if (tables.isEmpty() || tables.stream().anyMatch(String::isBlank) || revision < 0 || revision == Long.MAX_VALUE
                || cursor.progressKind() != ConsumerProgressKind.DIRECT_SOURCE || !cursor.pipelineId().equals(context.consumerId())
                || !cursor.perTableSeq().isEmpty() || !tables.containsAll(cursor.sinkAckedByTable().keySet())
                || !tables.containsAll(cursor.snapshotCompletedTables()) || new java.util.HashSet<>(cursor.snapshotCompletedTables()).size() != cursor.snapshotCompletedTables().size()
                || cursor.snapshotEpoch() == Long.MAX_VALUE || (cursor.snapshotEpoch() == 0) != (cursor.cdcStartPosition() == null)
                || cursor.cdcStartPosition() != null && cursor.cdcStartPosition().isBlank()
                || cursor.snapshotEpoch() == 0 && !cursor.snapshotCompletedTables().isEmpty()) throw new IllegalArgumentException("invalid private channel progress");
        if (current == null && (retained != null || writerRun != null || cursor.sinkAcked() != null || cursor.snapshotEpoch() != 0 || !cursor.sinkAckedByTable().isEmpty()))
            throw new IllegalArgumentException("an unopened channel cannot have capture progress");
        if (current != null) {
            requireWindow(context,current);
            if (cursor.snapshotEpoch() > current.epoch() || cursor.sinkAcked() != null && !validBound(cursor.sinkAcked(),current.epoch(),false)
                    || cursor.sinkAckedByTable().values().stream().anyMatch(bound -> !validBound(bound,current.epoch(),true)))
                throw new IllegalArgumentException("direct cursor contains an invalid ordering bound");
            if (current.anchor() != null && (cursor.sinkAcked() == null || cursor.sinkAcked().order().epoch() != current.epoch()))
                throw new IllegalArgumentException("an anchored channel requires its own confirmed baseline");
        }
        if (current != null && current.anchor() != null) {
            var baseline = new ChainPosition(SourceOrder.snapshotRow(current.epoch()),current.anchor());
            if (current.settledThrough() == 0 && !baseline.equals(cursor.sinkAcked())
                    || current.recordedThrough() > 0 && current.recordedThrough() == current.settledThrough() && !current.lastBoundary().equals(cursor.sinkAcked())
                    || current.lastBoundary() != null && current.lastBoundary().order().compareTo(cursor.sinkAcked().order()) < 0)
                throw new IllegalArgumentException("direct checkpoint is not covered by its original batch receipt");
        }
        if (retained != null) {
            requireWindow(context,retained);
            if (current == null || retained.epoch() >= current.epoch() || retained.pendingCount() < 1 || current.anchor() != null
                    || current.recordedThrough() != 0) throw new IllegalArgumentException("invalid retained direct recovery window");
        }
        if (cursor.sinkAcked() != null && (cursor.sinkAcked().order().epoch() < 1 || cursor.sinkAcked().order().epoch() == Long.MAX_VALUE
                || cursor.sinkAcked().token() == null || cursor.sinkAcked().token().isBlank())) throw new IllegalArgumentException("direct checkpoint requires an actual source token");
        if (writerRun != null && (current == null || writerRun.epoch() != current.epoch() || !writerRun.commandFence().equals(current.pipelineFence())
                || !tables.equals(writerRun.accounting().expected().keySet()) || !Objects.equals(writerRun.accounting().snapshotEpoch(),cursor.snapshotEpoch() == 0 ? null : cursor.snapshotEpoch())))
            throw new IllegalArgumentException("writer accounting belongs to another direct execution");
    }
    private static void requireWindow(DirectChannelContext context,DirectCaptureWindow window) {
        if (!window.captureFence().key().clusterId().equals(context.scope().clusterId()) || !window.captureFence().key().resourceId().equals(context.captureId())
                || !window.pipelineFence().key().clusterId().equals(context.scope().clusterId()) || !window.pipelineFence().key().resourceId().equals(context.scope().pipelineId()))
            throw new IllegalArgumentException("direct window authority belongs to another channel");
    }
    private static boolean validBound(ChainPosition point,long epoch,boolean durable) {
        if (point == null || point.order() == null || point.order().epoch() < 1 || point.order().epoch() > epoch
                || point.order().seq() < 0 && point.order().seq() != SourceOrder.SNAPSHOT_SEQ
                || point.token() != null && point.token().isBlank()) return false;
        return point.order().seq() != Long.MAX_VALUE || durable && point.token() == null;
    }
    public long epoch() { return current == null ? 0 : current.epoch(); }
}
