package io.tapstate.control.core;

import java.util.Map;
import java.util.Objects;

/**
 * One processor instance of one vertex, as the engine currently reports it.
 *
 * <p>{@code index} is the index the engine gives the instance across the whole execution, not within its
 * member. It is the half of the identity that stays meaningful once a vertex runs on several members at
 * once: two members each numbering their own instances from zero would produce two processors that
 * cannot be told apart.
 *
 * @param index                 the processor's index within this execution of its vertex
 * @param memberUuid            the engine identity of the member running it
 * @param working               whether this instance does the vertex's work. A vertex the plan pins to one
 *                              member still has an instance on every other member, and that instance does
 *                              nothing. Reporting those as workers would say a pinned vertex runs everywhere,
 *                              which is the opposite of what pinning it means
 * @param backlog               the rows waiting in the queues into this processor at the last reading, or null
 *                              when nothing has been read
 * @param frontierGaps          for a sink processor, how far each chain's bound runs ahead of the position its
 *                              frontier reached, by chain; empty where the processor reported none
 * @param frontierStalledMillis for a sink processor, how long each pinned chain's durable position has been
 *                              where it is, by chain; empty where none is pinned
 * @param queuedByStream        for a sink processor, the rows taken in and not yet handed to its writer, by the
 *                              stream they came on; empty where none are waiting
 * @param inFlightByTable       for a sink processor, the rows handed to its writer whose writes have not
 *                              settled, by the table they go to; empty where none are in flight
 */
public record LivePipelineProcessor(int index, String memberUuid, boolean working, Long backlog,
        Map<String, Long> frontierGaps, Map<String, Long> frontierStalledMillis, Map<String, Long> queuedByStream,
        Map<String, Long> inFlightByTable) {

    public LivePipelineProcessor {
        frontierGaps = Map.copyOf(Objects.requireNonNull(frontierGaps, "frontierGaps"));
        frontierStalledMillis = Map.copyOf(Objects.requireNonNull(frontierStalledMillis, "frontierStalledMillis"));
        queuedByStream = Map.copyOf(Objects.requireNonNull(queuedByStream, "queuedByStream"));
        inFlightByTable = Map.copyOf(Objects.requireNonNull(inFlightByTable, "inFlightByTable"));
    }

    /** A processor with nothing read about what is waiting in it by table. */
    public LivePipelineProcessor(int index, String memberUuid, boolean working, Long backlog,
            Map<String, Long> frontierGaps, Map<String, Long> frontierStalledMillis) {
        this(index, memberUuid, working, backlog, frontierGaps, frontierStalledMillis, Map.of(), Map.of());
    }

    /** A processor nothing has been read about but where it runs. */
    public LivePipelineProcessor(int index, String memberUuid, boolean working) {
        this(index, memberUuid, working, null, Map.of(), Map.of());
    }
}
