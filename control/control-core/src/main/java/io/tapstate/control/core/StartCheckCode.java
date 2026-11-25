package io.tapstate.control.core;

import io.tapstate.core.common.Severity;
import io.tapstate.core.common.ProductErrorDefinition;
import io.tapstate.core.common.StartCheckDefinitions;
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

    TARGET_IDENTITY_AUTOMATIC(StartCheckDefinitions.TARGET_IDENTITY_AUTOMATIC),
    TARGET_IDENTITY_COMPATIBLE(StartCheckDefinitions.TARGET_IDENTITY_COMPATIBLE),
    TARGET_IDENTITY_REFUSED(StartCheckDefinitions.TARGET_IDENTITY_REFUSED),

    /** A new full load into a target that holds nothing, or that does not exist yet. */
    TARGET_EMPTY(StartCheckDefinitions.TARGET_EMPTY),

    /** A new full load into a non-empty target this pipeline is set to clear first. */
    TARGET_SET_TO_CLEAR(StartCheckDefinitions.TARGET_SET_TO_CLEAR),

    /** A new full load into a non-empty target, kept as it is: the question the first check asks. */
    TARGET_NOT_EMPTY(StartCheckDefinitions.TARGET_NOT_EMPTY),

    /**
     * The same question about a target whose element comes from a shared definition, which the start
     * offers no way to change: {@code definition} names where to change it instead.
     */
    TARGET_NOT_EMPTY_SHARED(StartCheckDefinitions.TARGET_NOT_EMPTY_SHARED),

    /** A new full load into a non-empty target whose policy refuses one. */
    TARGET_NOT_EMPTY_REFUSED(StartCheckDefinitions.TARGET_NOT_EMPTY_REFUSED),

    /** Whether a target holds rows could not be found out: {@code reason} says why. */
    TARGET_ROWS_UNKNOWN(StartCheckDefinitions.TARGET_ROWS_UNKNOWN),

    /** A whole check could not be evaluated: {@code check} is its id, {@code reason} why. */
    UNAVAILABLE(StartCheckDefinitions.UNAVAILABLE),

    /** The action that clears a target before the full load and records that on its element. */
    CLEAR_BEFORE_FULL_LOAD(StartCheckDefinitions.CLEAR_BEFORE_FULL_LOAD),

    /** The action that keeps a target's rows and starts as configured. */
    KEEP_EXISTING_ROWS(StartCheckDefinitions.KEEP_EXISTING_ROWS);

    private final ProductErrorDefinition definition;

    StartCheckCode(ProductErrorDefinition definition) {
        this.definition = definition;
    }

    @Override
    public String code() {
        return definition.code();
    }

    @Override
    public Severity severity() {
        return definition.severity();
    }

    @Override
    public Set<String> placeholders() {
        return definition.parameters();
    }
}
