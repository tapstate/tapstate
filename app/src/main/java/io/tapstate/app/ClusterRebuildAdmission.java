package io.tapstate.app;

import io.tapstate.runtime.scheduler.RebuildAdmission;

import java.time.Duration;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.function.LongSupplier;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * The one way a failed run is replaced without anybody asking for it, and the only thing that decides
 * so. A run is admitted when a member it was planned over has gone away -- the pipeline's driver
 * answers that from this cluster's own committed membership -- and refused for every other death, so a
 * connector that keeps failing keeps the pipeline failed rather than restarting it forever. A member
 * <em>joining</em> is not one of these: it takes nothing away from a run already planned, and the run
 * is left alone until somebody asks for a rebalance.
 *
 * <p>Admissions are counted and spaced. The count is per pipeline and bounded, because a rebuild that
 * cannot succeed will not succeed on the tenth attempt either, and an unbounded retry is indistinguishable
 * from a restart loop from the outside. The spacing is there because the cluster is often still settling:
 * rebuilding into the same half-formed membership is how a handover turns into a sequence of them.
 *
 * <p>The count resets by itself. Once a rebuilt run is submitted, it is planned over the members that
 * are here now, so nobody is missing from it and the budget is given back -- there is nothing to clear
 * and nobody has to remember to. A pipeline that changes hands to another member is refused here for a
 * different reason: this member is not driving it, so no run of its own is recorded against it.
 *
 * <p>Not synchronized: one convergence pass at a time asks this, on a single scheduler thread with a
 * fixed delay, so passes never overlap.
 */
final class ClusterRebuildAdmission implements RebuildAdmission {

    private static final Logger LOG = LoggerFactory.getLogger(ClusterRebuildAdmission.class);

    /** How many rebuilds one run may be replaced by before the pipeline is left failed for a person. */
    static final int MAX_ATTEMPTS = 3;

    private final PipelineActuationOwnership actuation;
    private final long backoffNanos;
    private final LongSupplier nanoTime;
    private final Map<String, Attempts> attempts = new HashMap<>();

    /** What one pipeline has spent so far, and the earliest this member may spend the next of it. */
    private static final class Attempts {
        private int made;
        private long nextAllowedNanos;
        private boolean started;
    }

    ClusterRebuildAdmission(PipelineActuationOwnership actuation, Duration backoff) {
        this(actuation, backoff, System::nanoTime);
    }

    ClusterRebuildAdmission(PipelineActuationOwnership actuation, Duration backoff, LongSupplier nanoTime) {
        this.actuation = Objects.requireNonNull(actuation, "actuation");
        this.nanoTime = Objects.requireNonNull(nanoTime, "nanoTime");
        Objects.requireNonNull(backoff, "backoff");
        if (backoff.isNegative()) {
            throw new IllegalArgumentException("the rebuild backoff must not be negative");
        }
        this.backoffNanos = backoff.toNanos();
    }

    @Override
    public boolean admits(String pipelineId) {
        Objects.requireNonNull(pipelineId, "pipelineId");
        if (!actuation.aMemberLeftUnderTheRun(pipelineId)) {
            // Either every member it was planned over is still here -- so this death is the pipeline's
            // own and stays its own -- or this member is not the one driving it. Both give the budget back.
            attempts.remove(pipelineId);
            return false;
        }
        Attempts spent = attempts.computeIfAbsent(pipelineId, id -> new Attempts());
        long now = nanoTime.getAsLong();
        if (spent.made >= MAX_ATTEMPTS) {
            if (!spent.started || now - spent.nextAllowedNanos >= 0) {
                // Said once per backoff rather than once per tick: the pipeline stays failed from here on,
                // and the reason is worth having in the log beside the failure it is about.
                spent.nextAllowedNanos = now + backoffNanos;
                LOG.warn("Pipeline {} was rebuilt {} times after losing a member and is still failing;"
                        + " leaving it failed for an operator", pipelineId, spent.made);
            }
            return false;
        }
        if (spent.started && now - spent.nextAllowedNanos < 0) {
            return false;
        }
        spent.made++;
        spent.started = true;
        spent.nextAllowedNanos = now + backoffNanos;
        LOG.warn("Rebuilding pipeline {} after a member it was running on left (attempt {} of {})",
                pipelineId, spent.made, MAX_ATTEMPTS);
        return true;
    }

    /** Releases what pipelines no longer desired have spent, so a deleted one leaks no counter. */
    void retain(java.util.Collection<String> pipelineIds) {
        attempts.keySet().retainAll(pipelineIds);
    }
}
