package io.tapstate.core.validation;

import java.time.Instant;
import java.util.Objects;
import java.util.Optional;

/**
 * Candidate facts from independent reads, never a confirmed difference or a production change.
 * Pairing and participation are separate: a source-only row may be correctly excluded by a filter.
 */
public record ValidationCheckEvent(ValidationObservation observation, ValidationKey comparisonKey,
                                   Optional<Side> source, Optional<Side> target,
                                   Participation participation) {
    public ValidationCheckEvent {
        Objects.requireNonNull(observation, "observation");
        Objects.requireNonNull(comparisonKey, "comparisonKey");
        Objects.requireNonNull(source, "source");
        Objects.requireNonNull(target, "target");
        Objects.requireNonNull(participation, "participation");
        if (source.isEmpty() && target.isEmpty()) throw ValidationError.invalid("pair.sides");
    }

    public PairState pairState() {
        if (source.isEmpty()) return PairState.TARGET_ONLY;
        return target.isEmpty() ? PairState.SOURCE_ONLY : PairState.MATCHED;
    }

    public enum PairState { MATCHED, SOURCE_ONLY, TARGET_ONLY }
    public enum Participation { UNKNOWN, SHOULD_EXIST, SHOULD_NOT_EXIST }
    public enum GroupStatus { OPEN, COMPLETE }

    public boolean groupsComplete() {
        return source.map(side -> side.groupStatus() == GroupStatus.COMPLETE).orElse(true)
                && target.map(side -> side.groupStatus() == GroupStatus.COMPLETE).orElse(true);
    }

    /** A bounded sample and observed multiplicity; the reader must close the whole group safely. */
    public record Side(ValidationPhysicalIdentity identity, ValidationValues.Row row,
                       long observedOccurrences, GroupStatus groupStatus, ReadBoundary boundary) {
        public Side {
            Objects.requireNonNull(identity, "identity");
            Objects.requireNonNull(row, "row");
            Objects.requireNonNull(boundary, "boundary");
            Objects.requireNonNull(groupStatus, "groupStatus");
            ValidationError.positive(observedOccurrences, "observedOccurrences");
        }
    }

    /** Read timing is diagnostic; these timestamps alone never prove capture or commit completion. */
    public record ReadBoundary(String readId, String contractVersion, Instant startedAt, Instant finishedAt) {
        public ReadBoundary {
            readId = ValidationError.identifier(readId, "readId");
            contractVersion = ValidationError.identifier(contractVersion, "read.contractVersion");
            Objects.requireNonNull(startedAt, "startedAt");
            Objects.requireNonNull(finishedAt, "finishedAt");
            if (finishedAt.isBefore(startedAt)) throw ValidationError.invalid("read.interval");
        }
    }
}
