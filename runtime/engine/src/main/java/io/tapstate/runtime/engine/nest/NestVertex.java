package io.tapstate.runtime.engine.nest;

import java.io.Serializable;
import java.util.List;
import java.util.Objects;

/**
 * One vertex a nest tree compiles to: a resolver for each embed that has children of its own, and one
 * assembler at the root. Both hold their state in a map of their own, keyed by {@code partitionKey}, and
 * both take one inbound edge per child embed plus one for their own rows.
 *
 * <p>{@code pathId} is the field path from the root down to the embed this vertex serves, empty for the
 * assembler. It is what makes the name stable: adding an embed elsewhere in the tree moves nobody, where
 * numbering the vertices would shift every later one and orphan everything already written under the old
 * name. Renaming a path or inserting a level therefore is a breaking change to the stored state.
 *
 * <p>Two vertices can never collide on a name because no two embeds under one parent may take the same
 * path or a prefix of one, which makes the rendered path a document location and unique by construction.
 *
 * <p>{@code partitionKey} and {@code parentKeyFields} are the two halves of what a resolver does with a
 * row of its own embed: the first says which entry the row is filed under - the value its children will
 * point at - and the second says where on that same row to read the key of the level above, which is
 * what the entry then answers with. The assembler names no parent fields; the root hangs from nothing.
 */
public record NestVertex(
        List<String> pathId,
        String name,
        String mapName,
        List<String> partitionKey,
        List<String> parentKeyFields,
        List<NestInbound> inbound) implements Serializable {

    public NestVertex {
        Objects.requireNonNull(name, "name");
        Objects.requireNonNull(mapName, "mapName");
        pathId = List.copyOf(pathId);
        partitionKey = List.copyOf(partitionKey);
        parentKeyFields = List.copyOf(parentKeyFields);
        inbound = List.copyOf(inbound);
        if (partitionKey.isEmpty()) {
            throw new IllegalArgumentException("vertex " + name + " has nothing to partition by");
        }
    }

    /** Whether this vertex assembles whole documents rather than resolving one embed's parent keys. */
    public boolean isAssembler() {
        return pathId.isEmpty();
    }

    /**
     * The edge carrying the stream at {@code pathId}. Every stream reaching this vertex has exactly one,
     * so asking for a stream that does not arrive here is a wiring bug and bare-throws.
     */
    public NestInbound inboundFor(List<String> pathId) {
        for (NestInbound edge : inbound) {
            if (edge.pathId().equals(pathId)) {
                return edge;
            }
        }
        throw new IllegalArgumentException("no edge into " + name + " carries " + pathId);
    }
}
