package io.tapstate.core.validation;

import java.util.Objects;
import java.time.Instant;

/** One-to-one repair correlation, separate from production source positions and validation events. */
public final class ValidationRepair {
    private ValidationRepair() {}

    /** A retry after UNKNOWN is a new repairId; a repeated requestId denotes the same operation. */
    public record Attempt(String requestId, String repairId, ValidationObservation original,
                          ValidationObservation current, ValidationTiming.Execution execution,
                          ValidationPhysicalIdentity source, ValidationPhysicalIdentity target) {
        public Attempt {
            requestId = ValidationError.identifier(requestId, "repair.requestId");
            repairId = ValidationError.identifier(repairId, "repair.repairId");
            Objects.requireNonNull(original, "original"); Objects.requireNonNull(current, "current");
            Objects.requireNonNull(execution, "execution"); Objects.requireNonNull(source, "source");
            Objects.requireNonNull(target, "target");
            if (original.run().mode() != ValidationMode.INCREMENTAL || !original.root().ancestors().isEmpty()
                    || !original.run().equals(current.run()) || !original.root().equals(current.root())
                    || current.revision() < original.revision()
                    || !execution.clusterId().equals(current.run().owner().clusterId())
                    || !execution.pipelineId().equals(current.run().owner().pipelineId())) throw invalid("attempt");
        }
    }

    /** Only fresh independent source data may enter the admitted normal Pipeline ingress. */
    public record Snapshot(Attempt attempt, ValidationPhysicalIdentity identity, ValidationValues.Row row,
                           ValidationCheckEvent.ReadBoundary read) {
        public Snapshot {
            Objects.requireNonNull(attempt, "attempt"); Objects.requireNonNull(identity, "identity");
            Objects.requireNonNull(row, "row"); Objects.requireNonNull(read, "read");
            if (!attempt.source().equals(identity)) throw invalid("snapshotIdentity");
        }
    }

    public enum WriteStatus { COMMITTED, NO_OUTPUT, FAILED, UNKNOWN }
    /** COMMITTED means this actual single-output write, not queue acceptance or a capture frontier. */
    public record Ack(Attempt attempt, String submissionId, String outputId,
                      ValidationPhysicalIdentity target, WriteStatus status, Instant completedAt) {
        public Ack {
            Objects.requireNonNull(attempt, "attempt"); Objects.requireNonNull(target, "target");
            Objects.requireNonNull(status, "status"); Objects.requireNonNull(completedAt, "completedAt");
            submissionId = ValidationError.identifier(submissionId, "repair.submissionId");
            outputId = ValidationError.identifier(outputId, "repair.outputId");
            if (!attempt.current().root().outputId().equals(outputId) || !attempt.target().equals(target)) throw invalid("ackRoute");
        }
    }
    public enum Outcome { REPAIRED, STILL_DIFFERENT, FAILED, UNKNOWN }

    /** The current attempt and exact submission must be checked before any result can change. */
    public static Outcome afterRecheck(Attempt current, String submissionId, Ack ack, ValidationRootCheck recheck) {
        Objects.requireNonNull(current, "current"); Objects.requireNonNull(ack, "ack");
        if (!current.equals(ack.attempt()) || !ack.submissionId().equals(submissionId)) throw invalid("staleAck");
        if (ack.status() == WriteStatus.UNKNOWN) return Outcome.UNKNOWN;
        if (ack.status() != WriteStatus.COMMITTED) return Outcome.FAILED;
        Objects.requireNonNull(recheck, "recheck");
        if (!current.current().equals(recheck.observation()) || recheck.method() != ValidationMethod.FULL_COLUMN) throw invalid("recheck");
        if (recheck.reads().stream().anyMatch(read -> read.boundary().startedAt().isBefore(ack.completedAt()))) throw invalid("recheckBeforeAck");
        return recheck.observedAgreement()
                && recheck.participation() == ValidationCheckEvent.Participation.SHOULD_EXIST
                && recheck.targetPresence() == ValidationRootCheck.TargetPresence.PRESENT
                ? Outcome.REPAIRED : Outcome.STILL_DIFFERENT;
    }

    /** Necessary evidence binding only; never replaces current authorization or CDC handoff. */
    public static void requireWindow(Attempt attempt, ValidationTiming.Window window) {
        Objects.requireNonNull(attempt, "attempt"); Objects.requireNonNull(window, "window");
        if (!window.scope().observation().equals(attempt.current())
                || !window.scope().execution().equals(attempt.execution()) || !window.settled()) throw invalid("window");
    }

    private static io.tapstate.core.common.TapstateException invalid(String field) { return ValidationError.invalid("repair." + field); }
}
