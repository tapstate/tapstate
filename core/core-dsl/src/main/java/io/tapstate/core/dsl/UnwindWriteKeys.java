package io.tapstate.core.dsl;

import io.tapstate.core.model.TransformBody;

/**
 * What an unwind tells one of its output rows from another by. Expanding a row is the first thing
 * in this grammar that changes how many rows there are, so it is also the first that has to say
 * something about their identity: the parent's key is one value shared by all of them.
 *
 * <p>Three consumers ask this question and they must not answer it separately - the offline check
 * that refuses a declaration naming nothing, the derivation that publishes the target's key, and
 * the port that pairs an update's old rows against its new ones. Two of those disagreeing is not a
 * failure anybody sees: the pipeline runs, the target fills, and the rows that were overwritten are
 * not counted anywhere. So the answer lives here, in the one module all three already depend on.
 */
public final class UnwindWriteKeys {

    private UnwindWriteKeys() {
    }

    /**
     * The thing that varies from one expanded row to the next, or {@code null} when the declaration
     * names none - which is the state the offline check refuses.
     *
     * <p>Declaring both is not a conflict. The element's own field is the identity and the ordinal
     * is an ordinary column alongside it, which is how an author keeps the array's order without
     * keying on a position that moves whenever an element is inserted ahead of it.
     */
    public static String elementLocator(TransformBody.Unwind unwind) {
        return unwind.elementKey() != null ? unwind.elementKey() : unwind.includeArrayIndex();
    }
}
