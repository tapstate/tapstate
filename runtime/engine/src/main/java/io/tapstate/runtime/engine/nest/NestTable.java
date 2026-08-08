package io.tapstate.runtime.engine.nest;

import java.util.List;
import java.util.Objects;

/**
 * What compiling a nest tree has to be told about the table behind one alias: the name to say when
 * something about it is wrong, and the key it declares. The key is only ever read to fill in an embed
 * that left {@code arrayKey} out; an embed that declares one is never asked.
 *
 * <p>The compiler takes this rather than reaching for a schema itself, so it never learns the table
 * universe: resolving an alias to a table stays with whoever wired the pipeline.
 */
public record NestTable(String name, List<String> primaryKey) {

    public NestTable {
        Objects.requireNonNull(name, "name");
        primaryKey = primaryKey == null ? List.of() : List.copyOf(primaryKey);
    }
}
