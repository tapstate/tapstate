package io.tapstate.spi.store;

import io.tapstate.core.common.TapstateException;
import io.tapstate.core.validation.ValidationError;
import io.tapstate.core.validation.ValidationRunIdentity;
import io.tapstate.core.validation.ValidationOwner;
import io.tapstate.core.validation.ValidationRunState;
import java.util.Map;
import java.util.Objects;

/**
 * Minimum CACHE run-control seam over the existing VALIDATION claim. Normal mutations compete with
 * ValidationClaims.fence(run.owner()) in the SAME verified CACHE client/session/transaction as the
 * protected run/control record. This port adds no lease, restores no work and never touches CONFIG.
 * Selection uses stable eligible-member ranking, real local reservation and the existing claim store;
 * an existing owner is reused. Insufficient owner capacity rejects work rather than moving it.
 */
public interface ValidationControlStore {
    record Run(ValidationRunIdentity identity, long countingGeneration) {
        public Run {
            Objects.requireNonNull(identity, "identity");
            if (countingGeneration < 1) throw invalid("control.countingGeneration");
        }
        public WorkloadClaimFence fence() { return ValidationClaims.fence(identity.owner()); }
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
            if (!run.identity().equals(sealedReport.run()) || !terminal.terminal()
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
            if (!obsolete.identity().equals(sealedReport.run())
                    || !authority.clusterId().equals(previous.clusterId()) || !authority.pipelineId().equals(previous.pipelineId())
                    || authority.claimGeneration() <= previous.claimGeneration()) throw invalid("control.lostOwner");
        }
        public WorkloadClaimFence fence() { return ValidationClaims.fence(authority); }
    }
    enum Join { JOINED, ALREADY_JOINED, MODE_BUSY, GENERATION_CLOSED }
    enum Mutation { APPLIED, ALREADY_APPLIED, STALE }

    /**
     * Atomically join the exact owner/counting generation and task+mode slot, initially STARTING.
     * Exact-run replay is idempotent; another run, method or spec cannot occupy the same mode slot.
     * Join competes with retirement and the stop latch. Closed generations never reopen. Stale
     * owner/boot/fence is a coded refusal, not a successful join or proof the old worker exited.
     */
    Join join(Run run);

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
