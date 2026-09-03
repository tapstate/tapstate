package io.tapstate.core.sql;

import java.io.Serializable;
import java.util.List;
import java.util.Map;

/**
 * What a join declaration means, in terms no execution carrier appears in.
 *
 * <p>This type is the boundary the carrier is swapped behind: nothing in its signature may name a
 * type from the SQL library that produced it, because a consumer that can see those types ends up
 * depending on them and the swap stops being possible.
 *
 * @param outputFields the flat row this join publishes, in the order it publishes them
 * @param from         the sources it reads and the joins that combine them
 * @param readColumns  every column each source is read for, keyed by the name the plan calls that
 *                     source by, sorted, with an entry for each source even when it is read for
 *                     nothing. Key columns and filtered columns are routinely not projected, so a
 *                     carrier that mirrored the output row alone could not evaluate the join -- and
 *                     what it would emit is rows merely missing their matches, which reads as
 *                     ordinary output
 */
public record JoinPlan(List<OutputField> outputFields, JoinTree from,
                       Map<String, List<String>> readColumns) implements Serializable {

    /**
     * The source rows are driven from: the leftmost leaf of the tree.
     *
     * <p>Once a right outer join has been rewritten this is its preserved side -- the same table a
     * reader of the original SQL would have called the driving one. That the two agree is part of
     * what the rewrite has to get right, not a coincidence of how the tree is walked.
     */
    public JoinTree.Source factSource() {
        JoinTree node = from;
        while (node instanceof JoinTree.Join join) {
            node = join.left();
        }
        return (JoinTree.Source) node;
    }
}
