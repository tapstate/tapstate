package io.tapstate.core.sql;

import io.tapstate.core.common.TapstateType;

/**
 * One column of the flat row the join produces: the name it is published under and what it holds.
 *
 * <p>The type is derived statically from the SQL and the source columns, never from data, so it is
 * known before a single row moves. A type the shared vocabulary has no member for is reported as
 * {@code UNKNOWN} rather than guessed or dropped -- that is a named outcome a caller has to rule
 * on, and ruling on it is the validation layer's job, not this one's.
 */
public record OutputField(String name, TapstateType type, boolean nullable) {
}
