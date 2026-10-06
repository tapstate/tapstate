package io.tapstate.control.restapi;

import io.tapstate.control.core.StartCheckReport;
import io.tapstate.control.core.StartDecision;
import io.tapstate.control.core.StartOutcome;
import io.tapstate.core.lifecycle.DesiredState;
import io.tapstate.core.lifecycle.PipelineState;

import java.util.List;

/**
 * What a start that went ahead answers with: the intent it wrote, field for field as every lifecycle verb
 * has always answered, and beside it what the start checks said and which answers were taken.
 *
 * <p>The intent's fields are repeated rather than nested, so a client that read the start's answer before
 * start checks existed reads the same fields in the same places.
 */
record PipelineStartResponse(
        String pipelineId, PipelineState targetState, String revision, boolean purgeState,
        String assemblyRevision, boolean reassemble, Long rebuiltAtStateEpoch,
        StartCheckReport startChecks, List<StartOutcome.Applied> decisionsApplied,
        List<StartDecision> staleDecisions) {

    static PipelineStartResponse of(StartOutcome outcome) {
        DesiredState desired = outcome.desired();
        return new PipelineStartResponse(desired.pipelineId(), desired.targetState(), desired.revision(),
                desired.purgeState(), desired.assemblyRevision(), desired.reassemble(),
                desired.rebuiltAtStateEpoch(), outcome.report(), outcome.decisionsApplied(),
                outcome.staleDecisions());
    }
}
