package io.tapstate.runtime.srs;

import java.util.Objects;

/**
 * The cluster refused one ring operation on this member, so the write did not happen -- and may well
 * happen on the very next attempt.
 *
 * <p>Every change ring is guarded by the cluster's split brain protection, and each member answers that
 * protection from a verdict its own library recomputes on its own schedule. A ring write is a partitioned
 * operation and runs on whichever member owns the partition, so the member that refuses is not always the
 * member that wrote: just after a cluster forms, the writer's library has agreed while the owner's has
 * not. That disagreement closes within seconds, which is why this is carried as a refusal of one attempt
 * rather than as a failure of the capture.
 *
 * <p><strong>Nothing was written.</strong> The protection is checked before the operation runs at all, so
 * a refused append leaves behind neither a change nor part of a run, and retrying it cannot duplicate
 * anything.
 *
 * <p>This is bounded control flow and carries no error code, exactly as a refused headroom write does.
 * What a person is finally shown is decided where the waiting stops -- {@link CdcPhase}, which codes the
 * one case that is not transient: the cluster that never came back.
 */
public final class RingWriteRefusedException extends RuntimeException {

    private final String ring;

    RingWriteRefusedException(String ring, Throwable cause) {
        super("the cluster refused an operation on ring " + ring, Objects.requireNonNull(cause, "cause"));
        this.ring = Objects.requireNonNull(ring, "ring");
    }

    /** The ring whose operation was refused. */
    public String ring() {
        return ring;
    }
}
