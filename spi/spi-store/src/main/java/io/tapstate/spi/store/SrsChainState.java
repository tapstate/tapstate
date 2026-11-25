package io.tapstate.spi.store;

import java.time.Instant;
import java.util.Objects;
import java.util.Set;

/** Chain-owned control and source progress; models and consumer facts have separate stores. */
public record SrsChainState(String miningChainId, String captureId, String retention, long epoch,
                            Set<String> requestedTables, Set<String> servingTables,
                            long checkpointRevision, SrsCaptureCheckpoint checkpoint, Instant checkpointAt, long consumerCount, long consumerRevision,
                            SourceResumeOverride resumeOverride, SrsFreshFull pendingFreshFull) {
    public SrsChainState(String miningChainId, String captureId, String retention, long epoch,
                         Set<String> requestedTables, Set<String> servingTables, long checkpointRevision,
                         SrsCaptureCheckpoint checkpoint, Instant checkpointAt, long consumerCount, long consumerRevision,
                         SourceResumeOverride resumeOverride) {
        this(miningChainId, captureId, retention, epoch, requestedTables, servingTables, checkpointRevision,
                checkpoint, checkpointAt, consumerCount, consumerRevision, resumeOverride, null);
    }
    public SrsChainState(String miningChainId, String captureId, String retention, long epoch,
                         Set<String> requestedTables, Set<String> servingTables, long checkpointRevision,
                         SrsCaptureCheckpoint checkpoint, Instant checkpointAt, long consumerCount, long consumerRevision) {
        this(miningChainId, captureId, retention, epoch, requestedTables, servingTables, checkpointRevision,
                checkpoint, checkpointAt, consumerCount, consumerRevision, null);
    }
    public SrsChainState {
        if (miningChainId == null || miningChainId.isBlank() || captureId == null || captureId.isBlank()
                || epoch < 0 || epoch == Long.MAX_VALUE || checkpointRevision < 0 || checkpointRevision == Long.MAX_VALUE
                || consumerCount < 0 || consumerCount == Long.MAX_VALUE || consumerRevision < 0 || consumerRevision == Long.MAX_VALUE
                || consumerCount > consumerRevision) {
            throw new IllegalArgumentException("invalid SRS chain control");
        }
        requestedTables = Set.copyOf(Objects.requireNonNull(requestedTables, "requestedTables"));
        servingTables = Set.copyOf(Objects.requireNonNull(servingTables, "servingTables"));
        if (requestedTables.stream().anyMatch(String::isBlank) || servingTables.stream().anyMatch(String::isBlank)
                || (checkpoint == null) != (checkpointAt == null) || (checkpointRevision == 0) != (checkpoint == null)
                || (checkpoint != null && checkpoint.position().order().epoch() > epoch)
                || (resumeOverride != null && (resumeOverride.fromEpoch() > epoch
                    || checkpoint != null && checkpoint.position().order().epoch() > resumeOverride.fromEpoch()))) {
            throw new IllegalArgumentException("incoherent SRS chain control");
        }
        if (pendingFreshFull != null && (checkpoint == null || resumeOverride == null || pendingFreshFull.snapshotEpoch() > epoch
                || pendingFreshFull.snapshotEpoch() != resumeOverride.fromEpoch() + 1
                || !pendingFreshFull.models().keySet().containsAll(requestedTables)
                || pendingFreshFull.models().values().stream().anyMatch(key -> !miningChainId.equals(key.miningChainId()))))
            throw new IllegalArgumentException("incoherent pending full baseline");
    }
    public String resumeToken() { return resumeOverride != null ? resumeOverride.token() : checkpoint == null ? null : checkpoint.position().token(); }
}
