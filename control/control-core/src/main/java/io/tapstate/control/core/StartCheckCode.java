package io.tapstate.control.core;

import io.tapstate.core.common.Severity;
import io.tapstate.core.common.TapstateErrorCode;

import java.util.Set;

/**
 * The {@code start-check} domain: the codes that name the text of a start check report -- each finding a
 * check makes, and each action a person can take on one.
 *
 * <p><strong>None of these is ever thrown.</strong> They are codes because the report is read by scripts
 * as well as people, and a code is the one identity this product locks: unique, registered, documented in
 * the catalog with exactly the arguments it is rendered from, and append-only. A start the checks refuse
 * is refused under {@code lifecycle.start-needs-confirmation} or {@code lifecycle.start-blocked}, which is
 * where severity means something; here it has no reader, and every code records {@link Severity#WARNING}.
 *
 * <p>{@code holding} is how much a target holds, as the sentence says it: the row count when the target
 * reported one, otherwise that it holds at least one row.
 */
public enum StartCheckCode implements TapstateErrorCode {

    TARGET_IDENTITY_AUTOMATIC("start-check.target-identity-automatic", Set.of(Parameters.TARGET, Parameters.CONNECTION, "field", "preserved", "key")),
    TARGET_IDENTITY_COMPATIBLE("start-check.target-identity-compatible", Set.of(Parameters.TARGET, Parameters.CONNECTION, "key")),
    TARGET_IDENTITY_REFUSED("start-check.target-identity-refused", Set.of(Parameters.TARGET, Parameters.CONNECTION, Parameters.REASON, "field", "preserved")),

    /** A new full load into a target that holds nothing, or that does not exist yet. */
    TARGET_EMPTY("start-check.target-empty", Set.of(Parameters.TARGET, Parameters.CONNECTION)),

    /** A new full load into a non-empty target this pipeline is set to clear first. */
    TARGET_SET_TO_CLEAR("start-check.target-set-to-clear", Set.of(Parameters.TARGET, Parameters.CONNECTION, Parameters.HOLDING)),

    /** A new full load into a non-empty target, kept as it is: the question the first check asks. */
    TARGET_NOT_EMPTY("start-check.target-not-empty", Set.of(Parameters.TARGET, Parameters.CONNECTION, Parameters.HOLDING)),

    /**
     * The same question about a target whose element comes from a shared definition, which the start
     * offers no way to change: {@code definition} names where to change it instead.
     */
    TARGET_NOT_EMPTY_SHARED(
            "start-check.target-not-empty-shared", Set.of(Parameters.TARGET, Parameters.CONNECTION, Parameters.HOLDING, "definition")),

    /** A new full load into a non-empty target whose policy refuses one. */
    TARGET_NOT_EMPTY_REFUSED("start-check.target-not-empty-refused", Set.of(Parameters.TARGET, Parameters.CONNECTION, Parameters.HOLDING)),

    /** Whether a target holds rows could not be found out: {@code reason} says why. */
    TARGET_ROWS_UNKNOWN("start-check.target-rows-unknown", Set.of(Parameters.TARGET, Parameters.CONNECTION, Parameters.REASON)),

    /** A whole check could not be evaluated: {@code check} is its id, {@code reason} why. */
    UNAVAILABLE("start-check.unavailable", Set.of("check", Parameters.REASON)),

    /** The action that clears a target before the full load and records that on its element. */
    CLEAR_BEFORE_FULL_LOAD("start-check.clear-before-full-load", Set.of(Parameters.TARGET, "element")),

    /** The action that keeps a target's rows and starts as configured. */
    KEEP_EXISTING_ROWS("start-check.keep-existing-rows", Set.of(Parameters.TARGET));

    private static final class Parameters {
        static final String HOLDING = "holding";
        static final String CONNECTION = "connection";
        static final String TARGET = "target";
        static final String REASON = "reason";
        private Parameters() { }
    }

    private final String code;
    private final Set<String> placeholders;

    StartCheckCode(String code, Set<String> placeholders) {
        this.code = code;
        this.placeholders = placeholders;
    }

    @Override
    public String code() {
        return code;
    }

    @Override
    public Severity severity() {
        return Severity.WARNING;
    }

    @Override
    public Set<String> placeholders() {
        return placeholders;
    }
}
