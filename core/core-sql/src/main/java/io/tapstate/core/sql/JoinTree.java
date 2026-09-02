package io.tapstate.core.sql;

import java.util.List;
import java.util.stream.Stream;

/**
 * What a join declaration reads from: sources, and the joins that combine them.
 *
 * <p>A tree rather than a chain, because rewriting a right outer join swaps that join's two sides
 * and the side moving up may itself be a join. Nothing here names a type from the SQL library that
 * produced it -- a consumer able to see those types ends up depending on them, and swapping the
 * execution carrier stops being possible.
 */
public sealed interface JoinTree {

    /** One column of one source, under the name the rest of the SQL refers to that source by. */
    record ColumnRef(String source, String column) {
    }

    /** One equality a join matches on: the left side's column, then the right side's. */
    record KeyPair(ColumnRef left, ColumnRef right) {
    }

    /**
     * Rows read from one table.
     *
     * @param name  what the SQL calls it -- its alias where it has one, otherwise the table name
     * @param table the table the rows come from
     */
    record Source(String name, String table) implements JoinTree {
    }

    /**
     * Two sides matched on {@code on}.
     *
     * @param hasUncapturedCondition whether the join's condition holds something {@code on} does not
     *                               represent: a comparison that is not an equality between the two
     *                               sides, or a form this front end does not take apart. Dropping
     *                               such a predicate silently emits rows that should not exist, and
     *                               those rows look exactly like real matches -- so it is reported
     *                               rather than ignored, and the validation layer refuses on it.
     */
    record Join(JoinTree left, JoinTree right, JoinKind kind, List<KeyPair> on,
                boolean hasUncapturedCondition) implements JoinTree {
    }

    /** Every source under this node, left to right. */
    default List<Source> sources() {
        return switch (this) {
            case Source source -> List.of(source);
            case Join join -> Stream.concat(join.left().sources().stream(),
                    join.right().sources().stream()).toList();
        };
    }
}
