package io.tapstate.runtime.engine;

import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * Which vertices of a drawn graph run at each pipeline node's width, by node, as the graph is drawn.
 *
 * <p>A node is not always one vertex. A sink running on several processors reads through a router of its own,
 * and a nest or a join draws several vertices - and the vertices a nest or a join draws to gather several
 * producers into one run as one processor whatever the node's width. Only the drawing knows which is which, so it
 * says so here as it draws, and a reader matching a running vertex to the width its node was worked out to run at
 * never has to guess from a name.
 */
public final class NodeVertices {

    private final Map<String, List<String>> byNode = new LinkedHashMap<>();
    private final Map<String, List<String>> feeding = new LinkedHashMap<>();

    /** Notes that {@code vertex} runs at {@code node}'s width. */
    void add(String node, String vertex) {
        byNode.computeIfAbsent(node, ignored -> new ArrayList<>()).add(vertex);
    }

    /** Notes that {@code vertex}, drawn for another node or for none, sends its rows into {@code node}. */
    void feeds(String node, String vertex) {
        feeding.computeIfAbsent(node, ignored -> new ArrayList<>()).add(vertex);
    }

    /** Every node's vertices that run at its width, in the order they were drawn. */
    public Map<String, List<String>> byNode() {
        return copyOf(byNode);
    }

    /**
     * The vertices sending their rows into each sink, in the order they were connected: what the queues of the
     * edges carrying a sink's input are sized by, since there is one from every processor of each of them to
     * every processor the sink takes its input on.
     */
    public Map<String, List<String>> feedingByNode() {
        return copyOf(feeding);
    }

    private static Map<String, List<String>> copyOf(Map<String, List<String>> source) {
        Map<String, List<String>> copy = new LinkedHashMap<>();
        source.forEach((node, vertices) -> copy.put(node, List.copyOf(vertices)));
        return Collections.unmodifiableMap(copy);
    }
}
