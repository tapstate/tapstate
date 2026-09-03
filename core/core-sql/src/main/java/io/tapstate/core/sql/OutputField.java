package io.tapstate.core.sql;

import io.tapstate.core.common.TapstateType;

/**
 * One column of the flat row the join produces: the name it is published under, what it holds, and
 * how it is worked out.
 *
 * <p>The type is derived statically from the SQL and the source columns, never from data, so it is
 * known before a single row moves. A type the shared vocabulary has no member for is reported as
 * {@code UNKNOWN} rather than guessed or dropped -- that is a named outcome a caller has to rule
 * on, and ruling on it is the validation layer's job, not this one's.
 *
 * <p>{@code from} is what makes the plan executable rather than merely descriptive. Without it a
 * carrier knows the row's shape and nothing about where the values come from, and a row of the right
 * shape holding the wrong values is the one failure nothing downstream can see.
 */
public record OutputField(String name, TapstateType type, boolean nullable, Expr from) {
}
