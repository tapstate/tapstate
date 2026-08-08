package io.tapstate.runtime.engine;

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

    /** Records that the vertex keyed {@code vertexKey} reads {@code chain} and nothing else. */
    void source(String vertexKey, String chain) {
        byVertex.put(vertexKey, List.of(chain));
    }

    /** Records that the vertex keyed {@code vertexKey} carries whatever the vertices behind it carry. */
    void derived(String vertexKey, List<String> upstreamKeys) {
        byVertex.put(vertexKey, union(upstreamKeys));
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
