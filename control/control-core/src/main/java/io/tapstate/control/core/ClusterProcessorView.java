package io.tapstate.control.core;

import java.util.Collections;
import java.util.Map;
import java.util.Objects;
import java.util.TreeMap;

/**
 * One working processor instance of one vertex, and the member running it.
 *
 * <p>The pair {@code (vertex, index)} is the identity that survives the work being spread out: once a
 * vertex runs on several members at once, a single member named against the vertex could not say which
 * part of it ran where, and there would be no way to add that later without taking a field away.
 *
 * <p>Beside where it runs, what it was last read to be carrying: the rows queued into it, and for a sink's
 * writer, how far each chain it writes trails and how long a pinned one has been pinned. Read per processor,
 * these say which writer is behind rather than only that the sink is.
 *
 * @param index                 the processor's index within this execution of its vertex, across the cluster
 * @param localIndex            its index among the vertex's processors on its member
 * @param memberUuid            the engine identity of the member running it
 * @param nodeId                that member's stable id, or null when it carries no Tapstate identity
 * @param backlog               the rows waiting in the queues into it at the last reading, or null when
 *                              nothing has been read
 * @param frontierGaps          how far each chain's bound runs ahead of the position this writer's frontier
 *                              reached, by chain; empty for any processor but a sink's writer
 * @param frontierStalledMillis how long each pinned chain's durable position has been where it is, by chain;
 *                              empty where none is pinned
 */
public record ClusterProcessorView(int index, int localIndex, String memberUuid, String nodeId, Long backlog,
        Map<String, Long> frontierGaps, Map<String, Long> frontierStalledMillis) {

    public ClusterProcessorView {
        frontierGaps = Collections.unmodifiableMap(
                new TreeMap<>(Objects.requireNonNull(frontierGaps, "frontierGaps")));
        frontierStalledMillis = Collections.unmodifiableMap(
                new TreeMap<>(Objects.requireNonNull(frontierStalledMillis, "frontierStalledMillis")));
    }
}
