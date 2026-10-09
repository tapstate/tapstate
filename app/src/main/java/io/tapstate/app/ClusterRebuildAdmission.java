package io.tapstate.app;

import io.tapstate.core.common.TapstateException;
import io.tapstate.runtime.engine.EngineError;
import io.tapstate.runtime.scheduler.RebuildAdmission;
import java.time.Duration;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
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
 * <p>A refusal is logged with its reason, once for each reason, so a pipeline left failed says what kept
 * it there.
 *
 * <p>Not synchronized: one convergence pass at a time asks this, on a single scheduler thread with a
 * fixed delay, so passes never overlap.
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
    private final Map<String, Attempts> attempts = new HashMap<>();
    /** The refusal last logged for each pipeline, so the same refusal of the same run is logged once. */
    private final Map<String, Refusal> refusals = new HashMap<>();

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
        Attempts spent = attempts.computeIfAbsent(pipelineId, id -> new Attempts());
        long now = nanoTime.getAsLong();
        long failedRun = actuation.heldExecutionGeneration(pipelineId);
        if (failedRun != spent.failedExecution) {
            // Only a failed run is asked about, on every pass, so the first time it is asked about is within a
            // pass of when it failed.
            spent.failedExecution = failedRun;
            spent.failureSeenAtNanos = now;
        }
        if (spent.started && failedRun > spent.replacedExecution
                && spent.failureSeenAtNanos - spent.admittedAtNanos >= MAX_ATTEMPTS * backoffNanos) {
            // The run the last rebuild put in place went on running for longer than the whole stretch a
            // departure answers for, and only then failed. The departure that budget was spent on is over;
            // this is another one, with a budget of its own. Counted over the pipeline's life instead, a
            // cluster that has lost members three times would leave every pipeline failed at the fourth.
            // Measured to when the run failed rather than to now: a replacement that died at once and then
            // stayed failed has not outlived anything, however long it is waited beside.
            spent.made = 0;
            spent.started = false;
        }
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
        spent.admittedAtNanos = now;
        spent.replacedExecution = failedRun;
        LOG.warn("Rebuilding pipeline {} after a member it was running on left (attempt {} of {})",
                pipelineId, spent.made, MAX_ATTEMPTS);
        return true;
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
