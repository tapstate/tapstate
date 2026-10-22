package io.tapstate.control.core;

import io.tapstate.core.common.TapstateErrorCode;

import java.util.Collections;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

/**
 * What one start check found about one subject of a start: whether the start goes ahead, and if it
 * needs an answer first, the actions a person can answer with.
 *
 * <p>Its key -- {@code <check>/<subject id>} -- is what a decision names, and it is stable for as long as
 * the definition is: a start is evaluated afresh every time it is asked for, and the answers a person
 * gave the last time are matched to the findings of this time by key.
 *
 * <p><strong>A finding that asks for confirmation offers exactly one way to go ahead as configured.</strong>
 * If going ahead as configured were not acceptable the finding would refuse the start instead; and with
 * exactly one such action, a client can answer every question "yes, as it is" without knowing anything
 * about the check that asked it. That is held here, at construction, because nothing can see the values
 * a check produces before it runs. Every other behavior offers no action at all.
 *
 * @param check      the id of the check that made it
 * @param subject    what it is about
 * @param behavior   what it does to the start
 * @param evaluation whether the check could tell; a finding it could not evaluate never passes
 * @param code       names its text; every one of the code's placeholders has a value in {@code params}
 * @param params     the named values its text is rendered from, numbers kept as numbers
 * @param actions    what a person can answer with; only a confirmation has any
 */
public record StartFinding(
        String check, Subject subject, Behavior behavior, Evaluation evaluation,
        TapstateErrorCode code, Map<String, Object> params, List<StartAction> actions) {

    public StartFinding {
        Objects.requireNonNull(check, "check");
        Objects.requireNonNull(subject, "subject");
        Objects.requireNonNull(behavior, "behavior");
        Objects.requireNonNull(evaluation, "evaluation");
        Objects.requireNonNull(code, "code");
        params = Collections.unmodifiableMap(new LinkedHashMap<>(Objects.requireNonNull(params, "params")));
        actions = List.copyOf(Objects.requireNonNull(actions, "actions"));
        for (String name : code.placeholders()) {
            if (!params.containsKey(name)) {
                throw new IllegalArgumentException(
                        "finding " + check + "/" + subject.id() + " gives no value for " + name);
            }
        }
        if (behavior == Behavior.CONFIRM) {
            long goAhead = actions.stream().filter(StartAction::acknowledges).count();
            if (goAhead != 1) {
                throw new IllegalArgumentException("finding " + check + "/" + subject.id()
                        + " asks for confirmation, so it offers exactly one way to go ahead as configured; it offers "
                        + goAhead);
            }
            Set<String> ids = new HashSet<>();
            for (StartAction action : actions) {
                if (!ids.add(action.id())) {
                    throw new IllegalArgumentException(
                            "finding " + check + "/" + subject.id() + " offers action " + action.id() + " twice");
                }
            }
        } else if (!actions.isEmpty()) {
            throw new IllegalArgumentException(
                    "finding " + check + "/" + subject.id() + " is " + behavior + ", which offers no actions");
        }
    }

    /** What a decision names to answer this finding. */
    public String key() {
        return check + "/" + subject.id();
    }

    /** The action this finding offers under {@code id}, if it offers one. */
    public java.util.Optional<StartAction> action(String id) {
        return actions.stream().filter(action -> action.id().equals(id)).findFirst();
    }

    /** What a finding does to the start it is about. */
    public enum Behavior {
        /** Nothing to say that stops it; shown on request. */
        PASS,
        /** Nothing that stops it, and something the person has to be told. */
        WARN,
        /** Stops it until somebody answers. */
        CONFIRM,
        /** Stops it whatever is answered. */
        BLOCK
    }

    /** Whether the check could tell. */
    public enum Evaluation {
        COMPLETE,
        /** It could not -- a target out of reach, a refusal, no answer in time -- and says so. */
        UNAVAILABLE
    }

    /**
     * What a finding is about.
     *
     * @param kind    an open set; {@code TARGET} for a table a pipeline writes, {@code PIPELINE} for the
     *                pipeline as a whole
     * @param id      unique among one check's subjects; for a target, its {@code <connection>/<table>}
     * @param element the write element a target belongs to, or null
     * @param label   how a person reads it
     */
    public record Subject(String kind, String id, String element, String label) {

        public static final String TARGET = "TARGET";
        public static final String PIPELINE = "PIPELINE";

        public Subject {
            Objects.requireNonNull(kind, "kind");
            Objects.requireNonNull(id, "id");
            Objects.requireNonNull(label, "label");
        }
    }
}
