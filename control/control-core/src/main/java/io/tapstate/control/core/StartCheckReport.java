package io.tapstate.control.core;

import io.tapstate.spi.store.StartLoad;

import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/**
 * The start checks of one start, as every client receives them: the same shape from the read-only
 * preview, from a start that goes ahead, and from one that was stopped to ask.
 *
 * <p>A client renders it from what it says, never from which check said it: text arrives rendered,
 * kinds and values a client does not know are shown as written, and anything that decides whether a start
 * may go ahead -- an unknown behavior, an unknown outcome -- is read as refusing it.
 *
 * @param pipelineId  the pipeline
 * @param intent      the start these checks are for
 * @param contentHash the stored definition the checks were evaluated against; the precondition a start
 *                    carrying answers sends back
 * @param evaluatedAt when, as an ISO-8601 instant
 * @param outcome     whether the start may go ahead, worked out here rather than by any client
 * @param plan        how the start would load each target; empty when that could not be worked out
 * @param findings    what each check found, in the order the checks are registered
 */
public record StartCheckReport(
        String pipelineId, StartIntent intent, String contentHash, String evaluatedAt, Outcome outcome,
        List<PlanEntry> plan, List<Finding> findings) {

    public StartCheckReport {
        Objects.requireNonNull(pipelineId, "pipelineId");
        Objects.requireNonNull(intent, "intent");
        Objects.requireNonNull(contentHash, "contentHash");
        Objects.requireNonNull(evaluatedAt, "evaluatedAt");
        Objects.requireNonNull(outcome, "outcome");
        plan = List.copyOf(plan);
        findings = List.copyOf(findings);
    }

    /** Whether the start may go ahead. */
    public enum Outcome {
        /** Nothing refuses it and every question is answered. */
        READY,
        /** A question is open. */
        NEEDS_CONFIRMATION,
        /** A check refuses it, whatever is answered. */
        BLOCKED
    }

    /**
     * How the start would load one target.
     *
     * @param element    the write element
     * @param target     where the target is
     * @param load       how this start loads it
     * @param onFullLoad what a new full load does to rows already there, as the definition spells it
     */
    public record PlanEntry(String element, Target target, StartLoad load, String onFullLoad) {
    }

    /** A table on a connection. */
    public record Target(String connection, String table) {
    }

    /**
     * One finding, rendered.
     *
     * @param key        what a decision names to answer it
     * @param params     the values its message is rendered from, for a client laying out its own
     * @param message    the text to show; a client shows it as it is and reads no fact back out of it
     */
    public record Finding(String key, String check, Subject subject, StartFinding.Behavior behavior,
            StartFinding.Evaluation evaluation, String code, Map<String, Object> params, String message,
            List<Action> actions) {

        public Finding {
            params = Collections.unmodifiableMap(new LinkedHashMap<>(params));
            actions = List.copyOf(actions);
        }
    }

    /** What a finding is about; see {@link StartFinding.Subject}. */
    public record Subject(String kind, String id, String element, String label) {
    }

    /**
     * One answer a finding offers, rendered.
     *
     * @param changes what it changes in the definition; empty for an action that changes nothing
     */
    public record Action(String id, String kind, boolean destructive, String code, String message,
            List<StartAction.Change> changes) {

        public Action {
            changes = List.copyOf(changes);
        }
    }
}
