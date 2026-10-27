package io.tapstate.spi.store;

import io.tapstate.core.common.TapstateException;
import io.tapstate.core.validation.ValidationError;
import io.tapstate.core.validation.ValidationRunIdentity;
import io.tapstate.core.validation.ValidationOwner;
import io.tapstate.core.validation.ValidationRunState;
import java.util.Map;
import java.util.Objects;
import java.util.List;
import java.util.Optional;
import io.tapstate.core.validation.ValidationRunSpec;

/**
 * Minimum CACHE run-control seam over the existing VALIDATION claim. Normal mutations compete with
 * ValidationClaims.fence(run.owner()) in the SAME verified CACHE client/session/transaction as the
 * protected run/control record. Its protected CACHE callback never touches CONFIG; native assembly
 * may verify original CONFIG authority before and after it. This port adds no lease or work recovery.
 * Selection uses stable eligible-member ranking, real local reservation and the existing claim store;
 * an existing owner is reused. Insufficient owner capacity rejects work rather than moving it.
 */
public interface ValidationControlStore {
    record Run(ValidationRunIdentity identity, long countingGeneration, ValidationReportAdmission.Start admission, ValidationRunSpec snapshot) {
        public Run(ValidationRunIdentity identity, long countingGeneration) { this(identity, countingGeneration, null, null); }
        public Run(ValidationRunIdentity identity, long countingGeneration, ValidationReportAdmission.Start admission) { this(identity, countingGeneration, admission, null); }
        public Run {
            Objects.requireNonNull(identity, "identity");
            if (countingGeneration < 1) throw invalid("control.countingGeneration");
            if (admission != null && !identity.equals(admission.run())) throw invalid("control.admission");
            if (snapshot != null) new ValidationReportAdmission.Open(Objects.requireNonNull(admission), snapshot);
        }
        public WorkloadClaimFence fence() { return ValidationClaims.fence(identity.owner()); }
        public ValidationResultStore.Report report() {
            if (admission == null) throw invalid("control.admission"); return admission.report();
        }
    }
    record Transition(Run run, ValidationRunState expected, ValidationRunState next) {
        public Transition {
            Objects.requireNonNull(run, "run"); Objects.requireNonNull(expected, "expected"); Objects.requireNonNull(next, "next");
            expected.requireTransitionTo(next, run.identity().mode());
        }
    }
    record Retirement(Run run, ValidationResultStore.Report sealedReport, ValidationRunState terminal) {
        public Retirement {
            Objects.requireNonNull(run, "run"); Objects.requireNonNull(sealedReport, "sealedReport"); Objects.requireNonNull(terminal, "terminal");
            if (!run.identity().equals(sealedReport.run()) || run.admission() != null && !run.report().equals(sealedReport) || !terminal.terminal()
                    || terminal == ValidationRunState.COMPLETED && run.identity().mode() == io.tapstate.core.validation.ValidationMode.INCREMENTAL) {
                throw invalid("control.retirement");
            }
        }
    }
    /** Current authority and obsolete target identity are deliberately separate. */
    record LostOwner(ValidationOwner authority, Run obsolete, ValidationResultStore.Report sealedReport) {
        public LostOwner {
            Objects.requireNonNull(authority, "authority"); Objects.requireNonNull(obsolete, "obsolete");
            Objects.requireNonNull(sealedReport, "sealedReport");
            var previous = obsolete.identity().owner();
            if (!obsolete.identity().equals(sealedReport.run()) || obsolete.admission() != null && !obsolete.report().equals(sealedReport)
                    || !authority.clusterId().equals(previous.clusterId()) || !authority.pipelineId().equals(previous.pipelineId())
                    || authority.claimGeneration() <= previous.claimGeneration()) throw invalid("control.lostOwner");
        }
        public WorkloadClaimFence fence() { return ValidationClaims.fence(authority); }
    }
    enum Join { JOINED, ALREADY_JOINED, MODE_BUSY, GENERATION_CLOSED }
    enum Mutation { APPLIED, ALREADY_APPLIED, STALE }
    record Status(Run run, ValidationRunState state, boolean retired) {
        public Status { Objects.requireNonNull(run); Objects.requireNonNull(state); if (retired && !state.terminal()) throw invalid("control.retiredState"); }
    }
    /** Active entries carry bounded control coordinates; read retrieves one full frozen snapshot. No query resumes work. */
    record Snapshot(ValidationOwner authority, ValidationOwner ledgerOwner, long countingGeneration, boolean closed,
                    boolean problemLimit, long retainedRuns, List<Status> active) {
        public Snapshot {
            Objects.requireNonNull(authority);
            if (countingGeneration < 0 || retainedRuns < 0 || active == null || active.size() > 128 || retainedRuns < active.size()
                    || countingGeneration == 0 && ledgerOwner != null || countingGeneration > 0 && ledgerOwner == null
                    || countingGeneration == 0 && (!closed || problemLimit || retainedRuns != 0 || !active.isEmpty())
                    || ledgerOwner != null && (!ledgerOwner.clusterId().equals(authority.clusterId()) || !ledgerOwner.pipelineId().equals(authority.pipelineId()))
                    || problemLimit && !closed) throw invalid("control.snapshot");
            var slots = new java.util.HashSet<ValidationRunIdentity.ModeSlot>();
            for (var status : active) {
                if (status.retired() || status.run().countingGeneration() != countingGeneration || !status.run().identity().owner().equals(ledgerOwner)
                        || !status.run().identity().owner().clusterId().equals(authority.clusterId())
                        || !status.run().identity().owner().pipelineId().equals(authority.pipelineId()) || !slots.add(status.run().identity().slot()))
                    throw invalid("control.snapshotRun");
            }
            active = List.copyOf(active);
        }
    }

    default Snapshot snapshot(ValidationOwner authority) { throw invalid("control.snapshotUnsupported"); }
    default Optional<Status> read(ValidationOwner authority, Run original) { throw invalid("control.readUnsupported"); }

    /** Saved intent and runtime confirmation are separate, bounded facts for the original run. */
    record Delay(Run run, long schedulingRevision, java.util.UUID commandId, long firstCheckDelayMillis) {
        public Delay {
            Objects.requireNonNull(run); Objects.requireNonNull(commandId);
            if (run.identity().mode() != io.tapstate.core.validation.ValidationMode.INCREMENTAL
                    || schedulingRevision < 1 || schedulingRevision == Long.MAX_VALUE
                    || firstCheckDelayMillis < 0 || firstCheckDelayMillis > 86_400_000L) throw invalid("control.delay");
        }
    }
    record Delays(Delay requested, Delay applied) {
        public Delays {
            if (applied != null && (requested == null || !applied.run().identity().equals(requested.run().identity())
                    || applied.run().countingGeneration() != requested.run().countingGeneration()
                    || applied.schedulingRevision() > requested.schedulingRevision()
                    || applied.schedulingRevision() == requested.schedulingRevision()
                    && (!applied.commandId().equals(requested.commandId()) || applied.firstCheckDelayMillis() != requested.firstCheckDelayMillis())))
                throw invalid("control.delays");
        }
    }
    /** Verify the actual saved CONFIG tuple before and after recording intent in CACHE. */
    default Mutation requestDelay(Delay change) { throw invalid("control.delayUnsupported"); }
    /** Only the owning live runtime, after applying that exact tuple, may confirm it. */
    default Mutation acknowledgeDelay(Delay change) { throw invalid("control.delayUnsupported"); }
    default Delays delays(ValidationOwner authority, Run original) { throw invalid("control.delayUnsupported"); }

    /**
     * Atomically join the exact owner/counting generation and task+mode slot, initially STARTING.
     * Exact-run replay is idempotent; another run, method or spec cannot occupy the same mode slot.
     * Join competes with retirement and the stop latch. Closed generations never reopen. Stale
     * owner/boot/fence is a coded refusal, not a successful join or proof the old worker exited.
     */
    Join join(Run run);

    /**
     * After known original CONFIG closure, fence an unsuccessful join against a late join commit.
     * Records a bounded retired original when no row exists; otherwise ends only this exact
     * original, preserving a known terminal outcome. Never adopts or starts a Job. The tombstone uses
     * the existing run budget and participates in the same CACHE fence/transaction as join.
     */
    record Abandonment(Run run, ValidationRunState terminal) {
        public Abandonment {
            Objects.requireNonNull(run); Objects.requireNonNull(terminal);
            if (terminal != ValidationRunState.FAILED && terminal != ValidationRunState.STOPPED) throw invalid("control.abandonTerminal");
        }
    }
    default Mutation abandonClosedStart(Run run) { return abandonClosedStart(new Abandonment(run, ValidationRunState.FAILED)); }
    default Mutation abandonClosedStart(Abandonment command) { throw invalid("control.abandonUnsupported"); }

    /** CAS only the original run/state. Unknown responses may retry that operation, never a new run. */
    Mutation transition(Transition transition);

    /**
     * Latch stop for every active run of this owner/counting generation. The bounded owner-local
     * quota detects the 1001st confirmed problem root; candidate observations do not consume slots.
     * This operation never stops the production Pipeline or disables future schedule configuration.
     */
    Mutation latchProblemLimit(Run run);

    /**
     * Caller first closes local publication and obtains known CONFIG report closure. Only then may
     * a known terminal run retire here. The implementation atomically races the last exit against
     * joins; a retired empty generation cannot be reused. Unknown closure retains reservations.
     * Ordinary run failure leaves other runs alive. Owner loss closes every old run; recovery only
     * terminates old records, never reconstructs their jobs, keys or local budgets.
     */
    Mutation retire(Retirement retirement);

    /**
     * After known closure of the original CONFIG report, terminate an obsolete unfinished run as
     * FAILED under the CURRENT successor fence, in the same CACHE transaction. The transaction
     * verifies the old target identity and strictly older tenure; it never accepts the old fence,
     * changes a known terminal outcome, resumes old work or releases new-run slots. A successor
     * must obtain known old-result closure and this cleanup before admitting work that depends on
     * it. Unknown outcomes keep that gate closed. Old workers independently stop on authority loss.
     */
    Mutation terminateLostOwner(LostOwner command);

    private static TapstateException invalid(String field) {
        return new TapstateException(ValidationError.INVALID_CONTRACT, Map.of("field", field), null);
    }
}
