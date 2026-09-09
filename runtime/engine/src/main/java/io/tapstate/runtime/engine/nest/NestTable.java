package io.tapstate.runtime.engine.nest;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

/**
 * What compiling a nest tree has to be told about the table behind one alias: the name to say when
 * something about it is wrong, the key it declares, and the unique indexes it carries. The last two are
 * only ever read to fill in a level that left its key out; a level that declares one is never asked,
 * because a business identity and a physical key are allowed to differ and only the author knows.
 *
 * <p>{@code uniqueIndexes} holds one entry per unique index, each the ordered columns it covers. It is
 * the third and last place an identity can come from, and it is taken only when there is exactly one:
 * two of them identify a row equally well, so choosing between them would settle the identity of a
 * whole level on which index the source happened to report first.
 *
 * <p>The compiler takes this rather than reaching for a schema itself, so it never learns the table
 * universe: resolving an alias to a table stays with whoever wired the pipeline.
 */
public record NestTable(String name, List<String> primaryKey, List<List<String>> uniqueIndexes) {

    public NestTable {
        Objects.requireNonNull(name, "name");
        primaryKey = primaryKey == null ? List.of() : List.copyOf(primaryKey);
        uniqueIndexes = uniqueIndexes == null ? List.of() : copyOfEach(uniqueIndexes);
    }

    /** A table known only by the key it declares, which is every caller that has no index information. */
    public NestTable(String name, List<String> primaryKey) {
        this(name, primaryKey, List.of());
    }

    private static List<List<String>> copyOfEach(List<List<String>> indexes) {
        List<List<String>> copied = new ArrayList<>(indexes.size());
        for (List<String> index : indexes) {
            copied.add(List.copyOf(index));
        }
        return List.copyOf(copied);
    }
}
