package io.tapstate.runtime.engine.nest;

import io.tapstate.core.dsl.aggregate.ValueType;

import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/**
 * What compiling a nest tree has to be told about the table behind one alias: the name to say when
 * something about it is wrong, the key and unique indexes it declares, and the fields its discovered
 * model carries. Keys settle row identity; fields let flat embeds refuse known output collisions before
 * the job starts, and the fields' types fix what a rollup over the table computes in. An empty field list means no model was available, not that the table has no fields.
 *
 * <p>{@code uniqueIndexes} holds one entry per unique index, each the ordered columns it covers. It is
 * the third and last place an identity can come from, and it is taken only when there is exactly one:
 * two of them identify a row equally well, so choosing between them would settle the identity of a
 * whole level on which index the source happened to report first.
 *
 * <p>The compiler takes this rather than reaching for a schema itself, so it never learns the table
 * universe: resolving an alias to a table stays with whoever wired the pipeline.
 */
public record NestTable(
        String name,
        List<String> primaryKey,
        List<List<String>> uniqueIndexes,
        List<String> fields,
        Map<String, ValueType> types) {

    public NestTable {
        Objects.requireNonNull(name, "name");
        primaryKey = primaryKey == null ? List.of() : List.copyOf(primaryKey);
        uniqueIndexes = uniqueIndexes == null ? List.of() : copyOfEach(uniqueIndexes);
        fields = fields == null ? List.of() : List.copyOf(fields);
        types = types == null ? Map.of() : Collections.unmodifiableMap(new LinkedHashMap<>(types));
    }

    /** A discovered table whose columns' types were not asked for. */
    public NestTable(String name, List<String> primaryKey, List<List<String>> uniqueIndexes, List<String> fields) {
        this(name, primaryKey, uniqueIndexes, fields, Map.of());
    }

    /** A discovered table whose field model is not needed by the caller. */
    public NestTable(String name, List<String> primaryKey, List<List<String>> uniqueIndexes) {
        this(name, primaryKey, uniqueIndexes, List.of());
    }

    /** A table known only by the key it declares, which is every caller that has no index information. */
    public NestTable(String name, List<String> primaryKey) {
        this(name, primaryKey, List.of(), List.of());
    }

    private static List<List<String>> copyOfEach(List<List<String>> indexes) {
        List<List<String>> copied = new ArrayList<>(indexes.size());
        for (List<String> index : indexes) {
            copied.add(List.copyOf(index));
        }
        return List.copyOf(copied);
    }
}
