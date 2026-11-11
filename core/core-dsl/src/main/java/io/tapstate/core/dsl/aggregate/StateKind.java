package io.tapstate.core.dsl.aggregate;

/**
 * What a function has to keep to be maintained incrementally rather than recomputed: one running value, or
 * the inputs themselves. Rollups recompute and keep neither; the kind is declared for the step that will.
 */
public enum StateKind {
    /** A running value that an added or retracted input adjusts: count, sum, avg. */
    VALUE,
    /** The inputs themselves, since a retracted extreme or distinct value cannot be undone from a total. */
    MATERIALIZED_INPUT
}
