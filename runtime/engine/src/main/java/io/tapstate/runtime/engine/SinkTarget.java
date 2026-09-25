package io.tapstate.runtime.engine;

import java.io.Serializable;
import java.util.List;
import java.util.Objects;

/**
 * The target table one stream reaching a sink writes into, and the columns that key a row there - none for a
 * table with no key.
 *
 * <p>It is what a sink running on several processors routes that stream's rows by. Rows go by the table they
 * land in and their key there, not by the stream they came in on: two source tables written into one target
 * table share its keys, and a row of each with the same key is one row of the target, whose changes have to
 * be applied on one writer in the order they were read. A table with no key has nothing to tell its rows
 * apart by, so all of them go to one writer and that table is written serially - while the other tables of
 * the same sink are not held to it.
 */
public record SinkTarget(String table, List<String> keyColumns) implements Serializable {

    public SinkTarget {
        Objects.requireNonNull(table, "table");
        if (table.isBlank()) {
            throw new IllegalArgumentException("a sink target names the table it writes");
        }
        keyColumns = List.copyOf(Objects.requireNonNull(keyColumns, "keyColumns"));
    }

    /** Whether rows of this table can be told apart by a key, and so spread over the sink's writers. */
    public boolean keyed() {
        return !keyColumns.isEmpty();
    }
}
