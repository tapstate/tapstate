package io.tapstate.core.dsl.aggregate;

/**
 * One application of a function in progress: inputs go in through {@link #add}, the value comes out of
 * {@link #result}. A null argument never reaches {@code add} for the value functions - nulls are skipped -
 * so an accumulator sees only inputs that contribute.
 */
public interface Accumulator {

    void add(Contribution input);

    /**
     * Takes back an input added earlier. Declared for the functions that keep a running value; rollups
     * recompute and never call it.
     */
    default void retract(Contribution input) {
        throw new UnsupportedOperationException("this function keeps its inputs and cannot retract one");
    }

    /** Whether any input contributed, which is what decides that a field's {@code default:} applies. */
    boolean contributed();

    /** The function's value: null when no input contributed, except for the counts, which are 0. */
    Object result();
}
