package io.tapstate.app;

import io.tapstate.core.common.TapstateException;
import io.tapstate.runtime.engine.EngineError;
import io.tapstate.runtime.scheduler.RebuildAdmission;
import java.time.Duration;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.LongSupplier;
import java.util.function.Predicate;
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
 * stopped being the answer, and whatever ends a run after that is the pipeline's own. It also resets
 * when the run a rebuild put in place outlives that stretch: a member lost after that is another
 * departure, with a budget of its own. A failure already
 * recorded as the pipeline's own keeps that answer across a claim handover.
 * A failure without a known cause waits through the configured heartbeat detection window before an
 * intact membership view makes that answer durable. A sink failure recorded at its source needs no wait.
 * An unmarked failure whose driver leaves during the window can be rebuilt by its next holder.
 *
 * <p>Calls for different pipelines may overlap. Budget updates are atomic per pipeline, with ownership
 * queries performed before those short updates and logging after them.
 * <p>A refusal is logged once per execution and reason.
 */
final class ClusterRebuildAdmission implements RebuildAdmission {

    private static final Logger LOG = LoggerFactory.getLogger(ClusterRebuildAdmission.class);

    /** How many rebuilds one run may be replaced by before the pipeline is left failed for a person. */
    static final int MAX_ATTEMPTS = 3;

    private final PipelineActuationOwnership actuation;
    private final Predicate<String> refusedForAChangedMembership;
    private final long backoffNanos;
    private final long detectionWindowNanos;
    private final LongSupplier nanoTime;
    private final Map<String, Attempts> attempts = new ConcurrentHashMap<>();
    /** The refusal last logged for each pipeline, so the same refusal of the same run is logged once. */
    private final Map<String, Refusal> refusals = new ConcurrentHashMap<>();

    /** Which run a refusal was about, and why it was refused. */
    private record Refusal(long executionGeneration, PipelineActuationOwnership.Departure why) {}

    /** What one pipeline has spent so far, and the earliest this member may spend the next of it. */
    private static final class Attempts {
        private int made;
        private long nextAllowedNanos;
        private boolean started;
        /** When the last rebuild was admitted. */
        private long admittedAtNanos;
        /** The execution generation of the run the last rebuild replaced. */
        private long replacedExecution;
        /** The failed run this was last asked about, and when it was first asked about it. */
        private long failedExecution;
        private long failureSeenAtNanos;
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
        this(actuation, pipelineId -> false, backoff, detectionWindow, nanoTime);
    }

    /**
     * As above, and also admitting a run that was refused before it started because the members it started
     * on were not the ones its widths were worked out for. {@code refusedForAChangedMembership} answers that
     * from the run's recorded failure. Such a run never ran anything, and the membership change that refused
     * it may be a member joining - which no departure reading sees - so without this it would stay failed
     * for a person over nothing but the moment it happened to be submitted in.
     */
    ClusterRebuildAdmission(PipelineActuationOwnership actuation,
            Predicate<String> refusedForAChangedMembership, Duration backoff, Duration detectionWindow) {
        this(actuation, refusedForAChangedMembership, backoff, detectionWindow, System::nanoTime);
    }

    ClusterRebuildAdmission(PipelineActuationOwnership actuation,
            Predicate<String> refusedForAChangedMembership, Duration backoff, Duration detectionWindow,
            LongSupplier nanoTime) {
        this.actuation = Objects.requireNonNull(actuation, "actuation");
        this.refusedForAChangedMembership =
                Objects.requireNonNull(refusedForAChangedMembership, "refusedForAChangedMembership");
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
        long failedRun = actuation.heldExecutionGeneration(pipelineId);
        long now = nanoTime.getAsLong();
        attempts.computeIfPresent(pipelineId, (id, spent) -> {
            observeFailure(spent, failedRun, now);
            return spent;
        });
    }

    /**
     * Takes the moment {@code failedRun} was first seen failed: where its failure is recorded, which is as the
     * pipeline is marked failed, or otherwise the first time admission is asked about it - only a failed run is
     * asked about, on every pass. The moment is kept for that run however often either is asked again.
     */
    private static void observeFailure(Attempts spent, long failedRun, long now) {
        if (failedRun != spent.failedExecution) {
            spent.failedExecution = failedRun;
            spent.failureSeenAtNanos = now;
        }
    }

    @Override
    public boolean admits(String pipelineId) {
        Objects.requireNonNull(pipelineId, "pipelineId");
        // Only admission is asked for a FAILED checkpoint. Ownership's membership query is also used
        // before a run fails, so recording its answer there would manufacture an earlier failure.
        actuation.recordFailure(pipelineId, MAX_ATTEMPTS * backoffNanos, detectionWindowNanos);
        PipelineActuationOwnership.Departure departure =
                actuation.departure(pipelineId, MAX_ATTEMPTS * backoffNanos);
        // A start refused before it took a run is not a run the engine refused: whatever the engine still
        // holds is about the run before it, so it cannot speak for this failure.
        boolean membershipRefusedTheRun = departure != PipelineActuationOwnership.Departure.START_REFUSED
                && refusedForAChangedMembership.test(pipelineId);
        if (!departure.admits() && !membershipRefusedTheRun) {
            // Either no member it was planned over is gone, and none went recently enough to still be
            // answering for this death -- so it is the pipeline's own and stays its own -- or this member
            // is not the one driving it. Both give the budget back.
            //
            // The stretch is as long as spending the whole budget takes at the spacing below, so the two
            // numbers are one idea rather than two that can disagree: shorter and part of the budget
            // would be unreachable, longer and a departure would go on answering after its answer ran
            // out.
            attempts.remove(pipelineId);
            // Said once for each run and reason rather than on every pass: this is asked every tick for as
            // long as the pipeline stays failed, and the reason belongs in the log beside the failure it is
            // about - including when a later run of the same pipeline fails the same way.
            Refusal refusal = new Refusal(actuation.heldExecutionGeneration(pipelineId), departure);
            if (!refusal.equals(refusals.put(pipelineId, refusal))) {
                LOG.info("Not rebuilding failed pipeline {}: {}", pipelineId, departure.refusal());
            }
            return false;
        }
        refusals.remove(pipelineId);
        long now = nanoTime.getAsLong();
        long failedRun = actuation.heldExecutionGeneration(pipelineId);
        AdmissionDecision decision = new AdmissionDecision();
        attempts.compute(pipelineId, (id, previous) -> {
            Attempts spent = previous == null ? new Attempts() : previous;
            observeFailure(spent, failedRun, now);
            if (spent.started && failedRun > spent.replacedExecution
                    && spent.failureSeenAtNanos - spent.admittedAtNanos >= MAX_ATTEMPTS * backoffNanos) {
                // A replacement that outlived the settling stretch starts a new departure budget.
                // Waiting beside an immediately failed replacement never resets the spent budget.
                spent.made = 0;
                spent.started = false;
            }
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
            spent.admittedAtNanos = now;
            spent.replacedExecution = failedRun;
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

    /**
     * Whether {@code failure} is a run refused before it started because its members changed between the plan
     * and the start. The engine hands back the exact cause where the member that refused the run recorded it,
     * and otherwise only a rendering of it, so the canonical code is looked for in both: the code is a stable
     * contract, locked with every other, rather than the wording of a message.
     */
    static boolean isMembershipChangedBeforeStart(Throwable failure) {
        String code = EngineError.MEMBERSHIP_CHANGED_BEFORE_START.code();
        for (Throwable cause = failure; cause != null; cause = cause.getCause()) {
            if (cause instanceof TapstateException coded && coded.code() == EngineError.MEMBERSHIP_CHANGED_BEFORE_START) {
                return true;
            }
            if (cause.getMessage() != null && cause.getMessage().contains(code)) {
                return true;
            }
        }
        return false;
    }

    /** Releases what pipelines no longer desired have spent, so a deleted one leaks no counter. */
    @Override
    public void retain(java.util.Collection<String> pipelineIds) {
        attempts.keySet().retainAll(pipelineIds);
        refusals.keySet().retainAll(pipelineIds);
    }
}
