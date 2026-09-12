package io.tapstate.adapters.transform;

import io.tapstate.core.common.TapstateException;

/**
 * Where an expansion says the thing it has no way to refuse.
 *
 * <p>There is exactly one such thing, and it is a property of the data rather than of the
 * declaration: two elements of one row expanding to rows that carry the same key. Both rows are
 * still sent, so nothing downstream changes and nothing downstream can report it either - the
 * target simply keeps whichever arrived last. Saying it out loud is the only way anyone finds out,
 * and a seam rather than a log call so a case can assert that it was said.
 */
@FunctionalInterface
interface UnwindAlert {

    /** Says that two rows of one expansion carry the same key; both are sent regardless. */
    void rowsShareAKey(TapstateException coded);
}
