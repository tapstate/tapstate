package io.tapstate.core.sql;

import io.tapstate.core.common.TapstateType;

/**
 * One column of a table the SQL may read, in the shared type vocabulary rather than in any
 * database's own spelling. The front end never discovers these: whoever knows what the sources
 * hold hands them in, so the same SQL text derives the same plan wherever it is checked.
 */
public record SourceColumn(String name, TapstateType type, boolean nullable) {
}
