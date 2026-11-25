package io.tapstate.runtime.engine.nest;

/** What became of a child event handed to the key its join key points at. */
public enum Resolution {

    /** The parent row is known and the child can be stamped with it and passed on. */
    RESOLVED,

    /** The parent mapping is missing or deleted; the child is held for a later declaration. */
    HELD
}
