package io.tapstate.core.model;

import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;

/**
 * One {@code serve.sync} element — table-model write to an external store (ADR-0016 §8).
 * {@code source} references a {@code kind: source} connection supplier (X18); the element
 * never carries connector/config. An id is only required when referenced by
 * {@code query[].backend} (validate-layer rule).
 */
@Doc("One serve.sync element — a table-model write of pipeline output to an external store.")
public record SyncElement(
        @Doc("Optional id for this sync element; required only when referenced by a query backend.")
        String id,
        @Doc(value = "Reference to a kind: source connection supplier that provides the target connector and config.", required = true)
        String source,
        @Doc(value = "How rows are written to the target — for example upsert or append.", def = "upsert")
        WriteMode writeMode,
        @Doc("Rules for renaming the target table and columns relative to the pipeline output.")
        RenameSpec rename,
        @Doc(value = "Policy controlling how schema changes are applied to the target store.",
                def = "fail")
        DdlPolicy ddl) {

    public SyncElement {
        Objects.requireNonNull(source, "source");
    }
}
