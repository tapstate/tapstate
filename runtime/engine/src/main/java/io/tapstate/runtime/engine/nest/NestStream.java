package io.tapstate.runtime.engine.nest;

import java.io.Serializable;
import java.util.List;
import java.util.Objects;

/**
 * One input stream of a nest tree and what the tree costs it: which vertex its rows enter, and how many
 * resolvers they pass through before reaching the assembler. Latency scales with that count, so it is
 * stated here rather than left to be discovered by measuring.
 *
 * <p>An embed with children of its own enters its own vertex and takes as many hops as it is deep. A leaf
 * has no mapping to declare and takes one hop fewer, entering its parent's vertex directly - which is why
 * a leaf hanging off the root reaches the assembler with no hop at all, exactly like a root row.
 *
 * <p>{@code arrayKey} is the element identity as finally settled: what the embed declared, or the key its
 * table declares when it declared none. It is deliberately not the key the vertex partitions by - a
 * document usually shows a business key while child rows carry a surrogate one.
 */
public record NestStream(
        String alias,
        List<String> pathId,
        int hops,
        String entryVertex,
        List<String> arrayKey) implements Serializable {

    public NestStream {
        Objects.requireNonNull(alias, "alias");
        Objects.requireNonNull(entryVertex, "entryVertex");
        pathId = List.copyOf(pathId);
        arrayKey = arrayKey == null ? null : List.copyOf(arrayKey);
        if (hops < 0) {
            throw new IllegalArgumentException("stream " + alias + " cannot take " + hops + " hops");
        }
    }

    /** Whether these are the root's own rows rather than an embedded stream's. */
    public boolean isRoot() {
        return pathId.isEmpty();
    }
}
