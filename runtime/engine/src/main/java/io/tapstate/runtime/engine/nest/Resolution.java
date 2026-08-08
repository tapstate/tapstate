package io.tapstate.runtime.engine.nest;

/** What became of a child event handed to the key its join key points at. */
public enum Resolution {

    /** The parent row is known and the child can be stamped with it and passed on. */
    RESOLVED,

    /** The parent row has not arrived; the child is held until it does. */
    HELD,

    /**
     * The parent row is known to be deleted, so this child can never resolve. It is not held: holding it
     * would pin the frontier for an event that will never be emitted.
     */
    PARENT_ABSENT
}
