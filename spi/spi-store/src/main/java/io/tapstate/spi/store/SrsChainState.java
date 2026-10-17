package io.tapstate.spi.store;

import java.time.Instant;
import java.util.Objects;
import java.util.Set;

/** Chain-owned control and source progress; models and consumer facts have separate stores. */
public record SrsChainState(String miningChainId, String captureId, String retention, long epoch,
                            Set<String> requestedTables, Set<String> servingTables,
                            long checkpointRevision, SrsCaptureCheckpoint checkpoint, Instant checkpointAt) {
    public SrsChainState {
        if (miningChainId == null || miningChainId.isBlank() || captureId == null || captureId.isBlank()
                || epoch < 0 || epoch == Long.MAX_VALUE || checkpointRevision < 0 || checkpointRevision == Long.MAX_VALUE) {
            throw new IllegalArgumentException("invalid SRS chain control");
        }
        requestedTables = Set.copyOf(Objects.requireNonNull(requestedTables, "requestedTables"));
        servingTables = Set.copyOf(Objects.requireNonNull(servingTables, "servingTables"));
        if (requestedTables.stream().anyMatch(String::isBlank) || servingTables.stream().anyMatch(String::isBlank)
                || (checkpoint == null) != (checkpointAt == null) || (checkpointRevision == 0) != (checkpoint == null)
                || (checkpoint != null && checkpoint.position().order().epoch() > epoch)) {
            throw new IllegalArgumentException("incoherent SRS chain control");
        }
    }
}
