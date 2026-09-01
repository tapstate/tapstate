package io.tapstate.runtime.engine.nest;

import java.io.Serializable;
import java.util.List;
import java.util.Objects;

/**
 * Where the rows one level points at are kept: one entry per row, keyed by what identifies that row, and
 * holding the row's fields and nothing else. It is not a second copy of anything - it is the only copy
 * this tree keeps of that table, which is why a row referred to by ten thousand documents is stored once.
 *
 * <p>It is deliberately not a {@link NestVertex}. A vertex holds assembly - what has been gathered under
 * one key of it, and what is still waiting - and is read and written by the one processor that owns the
 * partition. This holds no assembly at all, is written by one vertex and read by another, and takes no
 * parking area because nothing here ever moves between documents.
 *
 * <p>{@code partitionKey} names the columns on the referred-to row that identify it, in the order the
 * embed's {@code on} map wrote them. The level pointing at it reads the matching columns of its own row in
 * that same order, so the two sides build the same key without either having to know the other's table.
 */
public record NestLookup(
        List<String> pathId,
        String alias,
        String name,
        String mapName,
        List<String> partitionKey) implements Serializable {

    public NestLookup {
        Objects.requireNonNull(alias, "alias");
        Objects.requireNonNull(name, "name");
        Objects.requireNonNull(mapName, "mapName");
        pathId = List.copyOf(pathId);
        partitionKey = List.copyOf(partitionKey);
        if (partitionKey.isEmpty()) {
            throw new IllegalArgumentException("lookup " + name + " has nothing to key rows by");
        }
    }
}
