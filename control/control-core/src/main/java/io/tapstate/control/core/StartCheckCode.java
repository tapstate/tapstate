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

    /** A new full load into a target that holds nothing, or that does not exist yet. */
    TARGET_EMPTY("start-check.target-empty", Set.of("target", "connection")),

    /** A new full load into a non-empty target this pipeline is set to clear first. */
    TARGET_SET_TO_CLEAR("start-check.target-set-to-clear", Set.of("target", "connection", "holding")),

    /** A new full load into a non-empty target, kept as it is: the question the first check asks. */
    TARGET_NOT_EMPTY("start-check.target-not-empty", Set.of("target", "connection", "holding")),

    /**
     * The same question about a target whose element comes from a shared definition, which the start
     * offers no way to change: {@code definition} names where to change it instead.
     */
    TARGET_NOT_EMPTY_SHARED(
            "start-check.target-not-empty-shared", Set.of("target", "connection", "holding", "definition")),

    /** A new full load into a non-empty target whose policy refuses one. */
    TARGET_NOT_EMPTY_REFUSED("start-check.target-not-empty-refused", Set.of("target", "connection", "holding")),

    /** Whether a target holds rows could not be found out: {@code reason} says why. */
    TARGET_ROWS_UNKNOWN("start-check.target-rows-unknown", Set.of("target", "connection", "reason")),

    /** A whole check could not be evaluated: {@code check} is its id, {@code reason} why. */
    UNAVAILABLE("start-check.unavailable", Set.of("check", "reason")),

    /** The action that clears a target before the full load and records that on its element. */
    CLEAR_BEFORE_FULL_LOAD("start-check.clear-before-full-load", Set.of("target", "element")),

    /** The action that keeps a target's rows and starts as configured. */
    KEEP_EXISTING_ROWS("start-check.keep-existing-rows", Set.of("target"));

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
