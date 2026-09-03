package io.tapstate.core.sql;

import java.io.Serializable;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;

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
     * Every column of {@code source} that some published value is computed from.
     *
     * <p><b>This is deliberately narrower than {@link #readColumns()}, and the difference is the whole
     * point.</b> A source is read for its join keys and for anything a condition mentions as well as
     * for what it publishes, and those are routinely not projected. A carrier deciding whether a change
     * to a dimension row can possibly alter the output has to ask about the published columns alone:
     * asked about the read columns instead, every edit touches something and nothing is ever ruled out.
     *
     * <p>The columns are collected from the whole of each expression rather than its outermost form. A
     * column reference sits at any depth - {@code UPPER(c.first || c.last)} publishes two of them - and
     * a reader that looked only at the top level would report that expression as reading nothing, which
     * is the answer that makes an edit to it disappear.
     *
     * @return the columns, sorted, empty for a source this plan publishes nothing from
     */
    public Set<String> outputColumns(String source) {
        Set<String> columns = new TreeSet<>();
        for (OutputField field : outputFields) {
            collect(field.from(), source, columns);
        }
        return Collections.unmodifiableSet(columns);
    }

    private static void collect(Expr expression, String source, Set<String> into) {
        switch (expression) {
            case Expr.Column column -> {
                if (column.ref().source().equals(source)) {
                    into.add(column.ref().column());
                }
            }
            case Expr.Literal ignored -> {
            }
            case Expr.Call call -> {
                for (Expr argument : call.arguments()) {
                    collect(argument, source, into);
                }
            }
        }
    }

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
