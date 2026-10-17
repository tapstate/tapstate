package io.tapstate.spi.store;

import java.util.Objects;

/** One fixed membership's original progress revision and its saved snapshot references, if any. */
public record SrsConsumerProgress(SrsConsumerContext context, long membershipRevision, long revision,
                                  ConsumerOffset cursor, SrsSnapshotPin snapshot) {
    public SrsConsumerProgress {
        Objects.requireNonNull(context, "context"); Objects.requireNonNull(cursor, "cursor");
        if (membershipRevision < 1 || membershipRevision == Long.MAX_VALUE || revision < 0 || revision == Long.MAX_VALUE
                || !context.key().consumerId().equals(cursor.pipelineId()) || cursor.progressKind() != ConsumerProgressKind.SRS) {
            throw new IllegalArgumentException("invalid consumer progress identity");
        }
        if (snapshot == null) {
            if (cursor.snapshotEpoch() != 0 || cursor.cdcStartPosition() != null || !cursor.snapshotCompletedTables().isEmpty()) {
                throw new IllegalArgumentException("snapshot progress has no model references");
            }
        } else if (snapshot.snapshotEpoch() != cursor.snapshotEpoch() || !snapshot.sourcePosition().equals(cursor.cdcStartPosition())
                || !cursor.perTableSeq().keySet().containsAll(snapshot.models().keySet())
                || !snapshot.models().keySet().containsAll(cursor.snapshotCompletedTables())
                || snapshot.models().values().stream().anyMatch(key -> !context.key().miningChainId().equals(key.miningChainId()))) {
            throw new IllegalArgumentException("incoherent saved snapshot references");
        }
    }
}
