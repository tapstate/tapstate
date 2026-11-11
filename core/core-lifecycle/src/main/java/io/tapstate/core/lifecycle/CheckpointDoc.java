package io.tapstate.core.lifecycle;

import java.time.Instant;
import java.util.Objects;

/**
 * The per-pipeline checkpoint: one doc per pipeline, and the unit the fencing CAS swaps. The shape
 * is an external contract — adding a field is backward compatible, changing or removing one is a
 * breaking change. The real Mongo serialization lives in an adapter; this record is the shape.
 *
 * <ul>
 *   <li>{@code pipelineId} — the primary key, one doc per pipeline.</li>
 *   <li>{@code stateJson} — the serialized current state plus its minimal payload; opaque here,
 *       it is simply the payload the CAS swaps.</li>
 *   <li>{@code epoch} — the monotonic fencing token the CAS compares and increments.</li>
 *   <li>{@code touchTime} — the last successful-write timestamp; diagnostic only, it never takes
 *       part in the CAS comparison, so clock drift can never corrupt the fencing decision.</li>
 * </ul>
 */
public record CheckpointDoc(String pipelineId, String stateJson, long epoch, Instant touchTime, LeaseRecovery leaseRecovery) {

    public CheckpointDoc(String pipelineId, String stateJson, long epoch, Instant touchTime) {
        this(pipelineId, stateJson, epoch, touchTime, null);
    }

    /** The attempts and last failure for one accepted RUNNING intent. */
    public record LeaseRecovery(long intentRevision, long failedExecution, int attempts, Instant retryAfter) {
        public LeaseRecovery {
            if (intentRevision < 1 || failedExecution < 1 || attempts < 1)
                throw new IllegalArgumentException("invalid lease recovery");
            Objects.requireNonNull(retryAfter, "retryAfter");
        }
    }

    public CheckpointDoc {
        Objects.requireNonNull(pipelineId, "pipelineId");
        Objects.requireNonNull(stateJson, "stateJson");
        Objects.requireNonNull(touchTime, "touchTime");
        if (epoch < 0) {
            throw new IllegalArgumentException("epoch must be non-negative, was " + epoch);
        }
    }

    /** The first checkpoint for a pipeline: epoch 0, before any compare-and-swap has run. */
    public static CheckpointDoc initial(String pipelineId, String stateJson, Instant touchTime) {
        return new CheckpointDoc(pipelineId, stateJson, 0, touchTime);
    }
}
