package io.tapstate.control.core;

import java.time.Instant;
import java.util.List;
import java.util.Objects;
import java.util.Set;

/**
 * What every start check found about one start, before it is rendered for anybody.
 *
 * @param contentHash the stored definition the start is held to
 * @param plan        how the start would load each target, null when that could not be worked out
 */
public record StartEvaluation(String pipelineId, StartIntent intent, String contentHash, Instant evaluatedAt,
        StartPlan plan, List<StartFinding> findings) {

    public StartEvaluation {
        Objects.requireNonNull(pipelineId, "pipelineId");
        Objects.requireNonNull(intent, "intent");
        Objects.requireNonNull(contentHash, "contentHash");
        Objects.requireNonNull(evaluatedAt, "evaluatedAt");
        findings = List.copyOf(findings);
    }

    /** The finding under {@code key}, if this start has one. */
    public java.util.Optional<StartFinding> finding(String key) {
        return findings.stream().filter(finding -> finding.key().equals(key)).findFirst();
    }

    /** The findings with {@code behavior}, in order. */
    public List<StartFinding> withBehavior(StartFinding.Behavior behavior) {
        return findings.stream().filter(finding -> finding.behavior() == behavior).toList();
    }

    /**
     * Whether the start may go ahead once the confirmations in {@code answered} are answered: a refusal
     * stops it whatever is answered, and any confirmation left open stops it until somebody answers.
     */
    public StartCheckReport.Outcome outcome(Set<String> answered) {
        if (!withBehavior(StartFinding.Behavior.BLOCK).isEmpty()) {
            return StartCheckReport.Outcome.BLOCKED;
        }
        boolean open = withBehavior(StartFinding.Behavior.CONFIRM).stream()
                .anyMatch(finding -> !answered.contains(finding.key()));
        return open ? StartCheckReport.Outcome.NEEDS_CONFIRMATION : StartCheckReport.Outcome.READY;
    }
}
