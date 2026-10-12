package io.tapstate.cli;

import java.util.Map;

/**
 * One processor instance of a vertex, as read back from the server (rule R6: mirrored, not shared).
 *
 * @param index                 the processor's index within this execution of its vertex
 * @param localIndex            its index among the vertex's processors on its member, or null when not sent
 * @param memberUuid            the engine identity of the member running it
 * @param nodeId                that member's stable id, or null when it carries none
 * @param backlog               the rows queued into it at the last reading, or null when nothing was read
 * @param frontierGaps          how far each chain this writer writes trails its bound, by chain
 * @param frontierStalledMillis how long each pinned chain's durable position has stood, by chain
 * @param queuedByStream        the rows a writer has taken in and not yet written, by stream
 * @param inFlightByTable       the rows a writer is writing and has not had settled, by table
 */
record RemoteProcessor(Integer index, Integer localIndex, String memberUuid, String nodeId, Long backlog,
        Map<String, Long> frontierGaps, Map<String, Long> frontierStalledMillis, Map<String, Long> queuedByStream,
        Map<String, Long> inFlightByTable) {

    RemoteProcessor {
        frontierGaps = frontierGaps == null ? Map.of() : Map.copyOf(frontierGaps);
        frontierStalledMillis = frontierStalledMillis == null ? Map.of() : Map.copyOf(frontierStalledMillis);
        queuedByStream = queuedByStream == null ? Map.of() : Map.copyOf(queuedByStream);
        inFlightByTable = inFlightByTable == null ? Map.of() : Map.copyOf(inFlightByTable);
    }

    /** A processor the server said nothing about by stream or table. */
    RemoteProcessor(Integer index, Integer localIndex, String memberUuid, String nodeId, Long backlog,
            Map<String, Long> frontierGaps, Map<String, Long> frontierStalledMillis) {
        this(index, localIndex, memberUuid, nodeId, backlog, frontierGaps, frontierStalledMillis, Map.of(), Map.of());
    }

    /** A processor the server said nothing about but where it runs. */
    RemoteProcessor(Integer index, String memberUuid, String nodeId) {
        this(index, null, memberUuid, nodeId, null, Map.of(), Map.of(), Map.of(), Map.of());
    }
}
