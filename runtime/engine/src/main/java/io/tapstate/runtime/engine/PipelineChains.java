package io.tapstate.runtime.engine;

import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Which chains reach which vertex, accumulated while a job's graph is drawn. A vertex reading a source
 * carries that source's chain; every other vertex carries whatever the vertices behind it carry, and the
 * answer is settled before any edge is drawn into it because a graph is drawn from its sources outward.
 *
 * <p>This is where "which edges must have promised before this level may speak" comes from. It is taken
 * from the graph rather than from what a running instance has seen, because at runtime an edge that has
 * not spoken yet and an edge that will never carry the chain look exactly alike - and treating the first
 * as the second promises a stretch of changes that are still in flight, which no later message takes back.
 */
final class PipelineChains {

    private final Map<String, List<String>> byVertex = new LinkedHashMap<>();
    // Per vertex: over how many distinct paths each chain reaching it gets there.
    private final Map<String, Map<String, Integer>> pathsByVertex = new LinkedHashMap<>();
    // Per vertex: the chains whose own rows reach it, rather than rows a nest or a join assembled from them.
    private final Map<String, Set<String>> unassembledByVertex = new LinkedHashMap<>();

    /** Records that the vertex keyed {@code vertexKey} reads {@code chain} and nothing else. */
    void source(String vertexKey, String chain) {
        byVertex.put(vertexKey, List.of(chain));
        pathsByVertex.put(vertexKey, Map.of(chain, 1));
        unassembledByVertex.put(vertexKey, Set.of(chain));
    }

    /** Records that the vertex keyed {@code vertexKey} carries whatever the vertices behind it carry. */
    void derived(String vertexKey, List<String> upstreamKeys) {
        byVertex.put(vertexKey, union(upstreamKeys));
        pathsByVertex.put(vertexKey, paths(upstreamKeys));
        unassembledByVertex.put(vertexKey, unassembled(upstreamKeys));
    }

    /**
     * Records that the vertex keyed {@code vertexKey} carries the chains behind it in rows of its own: a nest's
     * documents and a join's widened rows move with those chains' bounds, but none of them is a row one of
     * the chains' sources read.
     */
    void assembled(String vertexKey, List<String> upstreamKeys) {
        byVertex.put(vertexKey, union(upstreamKeys));
        pathsByVertex.put(vertexKey, paths(upstreamKeys));
        unassembledByVertex.put(vertexKey, Set.of());
    }

    /**
     * The chains whose own rows reach a vertex fed by all of {@code upstreamKeys}: rows their source read,
     * passed along or reshaped one at a time, a snapshot row still a snapshot row of its table.
     */
    Set<String> unassembled(List<String> upstreamKeys) {
        Set<String> merged = new LinkedHashSet<>();
        for (String key : upstreamKeys) {
            of(key);
            merged.addAll(unassembledByVertex.get(key));
        }
        return Collections.unmodifiableSet(merged);
    }

    /**
     * Whether some chain reaches a vertex fed by all of {@code upstreamKeys} at once over more than one path.
     *
     * <p>Such a chain's changes arrive in whatever order its paths drain in - somewhere along the way, or at
     * the vertex itself, two queues carrying it are drained independently - so a later change can arrive
     * before an earlier one, and nothing that arrives says by itself how far the chain has travelled.
     */
    boolean anyOverSeveralPaths(List<String> upstreamKeys) {
        return paths(upstreamKeys).values().stream().anyMatch(count -> count > 1);
    }

    /** For each chain reaching a vertex fed by all of {@code upstreamKeys}, how many paths it arrives over. */
    private Map<String, Integer> paths(List<String> upstreamKeys) {
        Map<String, Integer> paths = new LinkedHashMap<>();
        for (String key : upstreamKeys) {
            // Through of() first, so a vertex wired before whatever feeds it is refused by name.
            of(key);
            pathsByVertex.get(key).forEach((chain, count) -> paths.merge(chain, count, Integer::sum));
        }
        return paths;
    }

    /**
     * The chains reaching the vertex keyed {@code vertexKey}. Asking before that vertex was recorded is a
     * builder ordering bug rather than a condition to fall back from: an empty answer would compile to a
     * level that waits for nothing.
     */
    List<String> of(String vertexKey) {
        List<String> chains = byVertex.get(vertexKey);
        if (chains == null) {
            throw new IllegalStateException("no chain is known to reach vertex '" + vertexKey
                    + "' yet; it is being wired before whatever feeds it");
        }
        return chains;
    }

    /** Every chain reaching a vertex fed by all of {@code upstreamKeys} at once. */
    List<String> union(List<String> upstreamKeys) {
        Set<String> merged = new LinkedHashSet<>();
        for (String key : upstreamKeys) {
            merged.addAll(of(key));
        }
        return List.copyOf(merged);
    }

    /**
     * What each of {@code upstreamKeys} carries, in that order and kept apart. A vertex gathering several
     * producers of one stream needs them apart: the total of what reaches it says nothing about which of
     * its edges any one chain arrives on, and waiting on every edge for every chain never ends.
     */
    List<List<String>> perProducer(List<String> upstreamKeys) {
        return upstreamKeys.stream().map(this::of).toList();
    }

    /** What each inbound ordinal carries, given the upstreams wired in that order, one edge each. */
    Map<Integer, List<String>> perOrdinal(List<String> upstreamKeys) {
        Map<Integer, List<String>> byOrdinal = new LinkedHashMap<>();
        for (int ordinal = 0; ordinal < upstreamKeys.size(); ordinal++) {
            byOrdinal.put(ordinal, of(upstreamKeys.get(ordinal)));
        }
        return byOrdinal;
    }
}
