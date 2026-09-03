package io.tapstate.core.sql;

import java.util.List;
import java.util.Objects;

/**
 * How one output column is computed from the rows a match is made of, in terms no execution carrier
 * and no SQL library appears in.
 *
 * <p>Without this the plan says what the output row is called and what type each column has, and
 * nothing at all about where the values come from -- so a carrier can publish a row of the right shape
 * only by guessing, and the shape being right is what makes the guess invisible.
 *
 * <p>Three forms, and no more on purpose. Anything a select item can be that is not one of them is
 * refused by name while the SQL is still text, so that the set a carrier must evaluate and the set the
 * front end admits are the same set rather than two lists that drift.
 */
public sealed interface Expr {

    /** A column of one of the joined sources, under the name the plan calls that source by. */
    record Column(JoinTree.ColumnRef ref) implements Expr {

        public Column {
            Objects.requireNonNull(ref, "ref");
        }
    }

    /** A constant written into the SQL. Null is a value here, and means SQL's NULL. */
    record Literal(Object value) implements Expr {
    }

    /**
     * An operator applied to arguments, named the way SQL names it -- {@code +}, {@code CASE},
     * {@code COALESCE}. The name is a plain string rather than an enum because it crosses from a SQL
     * library to a carrier that must not be able to see that library's types; what keeps it honest is
     * that only the names in {@link Expressions#SUPPORTED} ever reach a plan.
     */
    record Call(String operator, List<Expr> arguments) implements Expr {

        public Call {
            Objects.requireNonNull(operator, "operator");
            arguments = List.copyOf(Objects.requireNonNull(arguments, "arguments"));
        }
    }
}
