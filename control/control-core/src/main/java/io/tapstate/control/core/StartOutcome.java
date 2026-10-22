package io.tapstate.control.core;

import io.tapstate.core.lifecycle.DesiredState;

import java.util.List;
import java.util.Objects;

/**
 * A start that went ahead.
 *
 * @param desired          the intent written, the same shape every lifecycle verb answers with
 * @param report           what the start checks said about the definition it went ahead with
 * @param decisionsApplied the answers taken, including any change made to the definition
 * @param staleDecisions   answers given to questions this start no longer asks, and so ignored
 */
public record StartOutcome(
        DesiredState desired, StartCheckReport report, List<Applied> decisionsApplied,
        List<StartDecision> staleDecisions) {

    public StartOutcome {
        Objects.requireNonNull(desired, "desired");
        Objects.requireNonNull(report, "report");
        decisionsApplied = List.copyOf(decisionsApplied);
        staleDecisions = List.copyOf(staleDecisions);
    }

    /**
     * One answer the start took.
     *
     * @param finding     the question answered
     * @param action      the action chosen
     * @param kind        the action's kind
     * @param changes     what it changed in the definition; empty when it changed nothing
     * @param contentHash the definition's content hash after the change, null when nothing changed
     */
    public record Applied(String finding, String action, String kind, List<StartAction.Change> changes,
            String contentHash) {

        public Applied {
            changes = List.copyOf(changes);
        }
    }
}
