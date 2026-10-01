package io.tapstate.spi.store;

import io.tapstate.core.lifecycle.DesiredState;

import java.util.Objects;

/** One durable stop attempt beside the actual checkpoint, including the original restart instruction. */
public record StopReservation(
        String pipelineId, String token, long sourceEpoch, long reservedEpoch,
        DesiredState originalDesired, Subject subject) {

    public StopReservation {
        pipelineId = required(pipelineId, "pipelineId");
        token = required(token, "token");
        Objects.requireNonNull(originalDesired, "originalDesired");
        Objects.requireNonNull(subject, "subject");
        if (token.length() > 256 || sourceEpoch < 0 || reservedEpoch <= sourceEpoch
                || !pipelineId.equals(originalDesired.pipelineId())
                || subject instanceof ExistingJob existing && existing.authority().claim() != null
                        && !pipelineId.equals(existing.authority().claim().key().resourceId())) {
            throw new IllegalArgumentException("stop reservation identities or epochs disagree");
        }
    }

    /** The only two factual inputs: no live job, or one exact old job with a durable execution. */
    public sealed interface Subject permits NoJob, ExistingJob { }

    /** An actually absent job; authority is absent only before an execution was admitted. */
    public record NoJob(String clusterId, StopAuthority knownAuthority) implements Subject {
        public NoJob {
            clusterId = required(clusterId, "clusterId");
            if (knownAuthority != null && !clusterId.equals(knownAuthority.clusterId())) {
                throw new IllegalArgumentException("no-job authority belongs to another cluster");
            }
        }
    }

    /** A live old job has no optional identity fields. */
    public record ExistingJob(String pipelineIncarnationId, long executionGeneration,
            JobIdentity oldJob, StopAuthority authority) implements Subject {
        public ExistingJob {
            pipelineIncarnationId = required(pipelineIncarnationId, "pipelineIncarnationId");
            Objects.requireNonNull(oldJob, "oldJob");
            Objects.requireNonNull(authority, "authority");
            if (executionGeneration < 1 || executionGeneration != authority.executionGeneration()
                    || !oldJob.clusterId().equals(authority.clusterId())) {
                throw new IllegalArgumentException("old job and current stop authority disagree");
            }
        }
    }

    /** The exact old Jet job to finish; a name lookup may select a replacement execution. */
    public record JobIdentity(String clusterId, long jobId, String bootId) {
        public JobIdentity {
            clusterId = required(clusterId, "clusterId");
            bootId = required(bootId, "bootId");
        }
    }

    /** Transfers only the completing worker's authority; the old job and original intent remain pinned. */
    public StopReservation rebind(StopAuthority nextAuthority, long nextEpoch) {
        Objects.requireNonNull(nextAuthority, "nextAuthority");
        if (nextEpoch <= reservedEpoch) {
            throw new IllegalArgumentException("a rebinding must advance the checkpoint epoch");
        }
        Subject rebound = switch (subject) {
            case NoJob absent -> new NoJob(absent.clusterId(), nextAuthority);
            case ExistingJob old -> new ExistingJob(old.pipelineIncarnationId(), old.executionGeneration(),
                    old.oldJob(), nextAuthority);
        };
        return new StopReservation(pipelineId, token, sourceEpoch, nextEpoch, originalDesired, rebound);
    }

    public StopAuthority authorityOrNull() {
        return switch (subject) {
            case NoJob absent -> absent.knownAuthority();
            case ExistingJob old -> old.authority();
        };
    }

    private static String required(String value, String name) {
        Objects.requireNonNull(value, name);
        if (value.isBlank()) { throw new IllegalArgumentException(name + " must not be blank"); }
        return value;
    }
}
