package io.tapstate.core.sql;

import java.util.List;

/** A table the SQL may name, and the columns it may select from it. */
public record SourceTable(String name, List<SourceColumn> columns) {
}
