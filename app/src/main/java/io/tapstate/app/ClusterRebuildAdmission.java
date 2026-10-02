package io.tapstate.app;

import io.tapstate.runtime.scheduler.RebuildAdmission;

import java.time.Duration;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.LongSupplier;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * The one way a failed run is replaced without anybody asking for it, and the only thing that decides
 * so. A run is admitted when a member it was planned over has gone away -- the pipeline's driver
 * answers that from the members this cluster can see -- and refused for every other death, so a
 * connector that keeps failing keeps the pipeline failed rather than restarting it forever. A member
 * <em>joining</em> is not one of these: it takes nothing away from a run already planned, and the run
 * is left alone until somebody asks for a rebalance.
 *
 * <p>Admissions are counted and spaced. The count is per pipeline and bounded, because a rebuild that
 * cannot succeed will not succeed on the tenth attempt either, and an unbounded retry is indistinguishable
 * from a restart loop from the outside. The spacing is there because the cluster is often still settling:
 * rebuilding into the same half-formed membership is how a handover turns into a sequence of them.
 *
 * <p><b>A departure answers for more than the run it ended.</b> The run submitted in its place is
 * planned over the members that are here now, so nothing is missing from it -- and it is being started
 * into a cluster still settling from the departure, which goes on ending runs: a member reconnecting, a
 * topology that moved again, an execution fenced out by the one replacing it. Asked at the instant of
 * each death, every one of those reads as the pipeline's own, and the pipeline is left failed for a
 * person over a member that left. So the question carries a stretch, and it is over that stretch that
 * the count and the spacing below are spent.
 *
 * <p>The count resets by itself, once the stretch is over and nothing is missing: the departure has
 * stopped being the answer, and whatever ends a run after that is the pipeline's own. A failure already
 * recorded as the pipeline's own keeps that answer across a claim handover.
 * A failure without a known cause waits through the configured heartbeat detection window before an
 * intact membership view makes that answer durable. A sink failure recorded at its source needs no wait.
 * An unmarked failure whose driver leaves during the window can be rebuilt by its next holder.
 *
 * <p>Calls for different pipelines may overlap. Budget updates are atomic per pipeline, with ownership
 * queries performed before those short updates and logging after them.
 */
final class ClusterRebuildAdmission implements RebuildAdmission {

    private static final Logger LOG = LoggerFactory.getLogger(ClusterRebuildAdmission.class);

    /** How many rebuilds one run may be replaced by before the pipeline is left failed for a person. */
    static final int MAX_ATTEMPTS = 3;

    private final PipelineActuationOwnership actuation;
    private final long backoffNanos;
    private final long detectionWindowNanos;
    private final LongSupplier nanoTime;
    private final Map<String, Attempts> attempts = new ConcurrentHashMap<>();

    /** What one pipeline has spent so far, and the earliest this member may spend the next of it. */
    private static final class Attempts {
        private int made;
        private long nextAllowedNanos;
        private boolean started;
    }

    /** One call's result, carried out of its budget update before logging. */
    private static final class AdmissionDecision {
        private boolean admitted;
        private boolean warnExhausted;
        private int made;
    }

    ClusterRebuildAdmission(PipelineActuationOwnership actuation, Duration backoff, Duration detectionWindow) {
        this(actuation, backoff, detectionWindow, System::nanoTime);
    }

    ClusterRebuildAdmission(PipelineActuationOwnership actuation, Duration backoff,
            Duration detectionWindow, LongSupplier nanoTime) {
        this.actuation = Objects.requireNonNull(actuation, "actuation");
        this.nanoTime = Objects.requireNonNull(nanoTime, "nanoTime");
        Objects.requireNonNull(backoff, "backoff");
        Objects.requireNonNull(detectionWindow, "detectionWindow");
        if (backoff.isNegative()) {
            throw new IllegalArgumentException("the rebuild backoff must not be negative");
        }
        if (detectionWindow.isZero() || detectionWindow.isNegative()) {
            throw new IllegalArgumentException("the member-loss detection window must be positive");
        }
        this.backoffNanos = backoff.toNanos();
        this.detectionWindowNanos = detectionWindow.toNanos();
    }

    @Override
    public void recordFailure(String pipelineId) {
        actuation.recordFailure(pipelineId, MAX_ATTEMPTS * backoffNanos, detectionWindowNanos);
    }

    @Override
    public boolean admits(String pipelineId) {
        Objects.requireNonNull(pipelineId, "pipelineId");
        // Only admission is asked for a FAILED checkpoint. Ownership's membership query is also used
        // before a run fails, so recording its answer there would manufacture an earlier failure.
        actuation.recordFailure(pipelineId, MAX_ATTEMPTS * backoffNanos, detectionWindowNanos);
        if (!actuation.aMemberLeftUnderTheRun(pipelineId, MAX_ATTEMPTS * backoffNanos)) {
            // Either no member it was planned over is gone, and none went recently enough to still be
            // answering for this death -- so it is the pipeline's own and stays its own -- or this member
            // is not the one driving it. Both give the budget back.
            //
            // The stretch is as long as spending the whole budget takes at the spacing below, so the two
            // numbers are one idea rather than two that can disagree: shorter and part of the budget
            // would be unreachable, longer and a departure would go on answering after its answer ran
            // out.
            attempts.remove(pipelineId);
            return false;
        }
        long now = nanoTime.getAsLong();
        AdmissionDecision decision = new AdmissionDecision();
        attempts.compute(pipelineId, (id, previous) -> {
            Attempts spent = previous == null ? new Attempts() : previous;
            if (spent.made >= MAX_ATTEMPTS) {
                if (!spent.started || now - spent.nextAllowedNanos >= 0) {
                    // Exhaustion is reported once per backoff while the pipeline remains failed.
                    spent.nextAllowedNanos = now + backoffNanos;
                    decision.warnExhausted = true;
                    decision.made = spent.made;
                }
                return spent;
            }
            if (spent.started && now - spent.nextAllowedNanos < 0) { return spent; }
            spent.made++;
            spent.started = true;
            spent.nextAllowedNanos = now + backoffNanos;
            decision.admitted = true;
            decision.made = spent.made;
            return spent;
        });
        if (decision.warnExhausted) {
            LOG.warn("Pipeline {} was rebuilt {} times after losing a member and is still failing;"
                    + " leaving it failed for an operator", pipelineId, decision.made);
        } else if (decision.admitted) {
            LOG.warn("Rebuilding pipeline {} after a member it was running on left (attempt {} of {})",
                    pipelineId, decision.made, MAX_ATTEMPTS);
        }
        return decision.admitted;
    }

    /** Releases what pipelines no longer desired have spent, so a deleted one leaks no counter. */
    void retain(java.util.Collection<String> pipelineIds) {
        attempts.keySet().retainAll(pipelineIds);
    }
}
