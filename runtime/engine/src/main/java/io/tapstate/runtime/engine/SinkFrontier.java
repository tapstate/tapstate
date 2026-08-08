package io.tapstate.runtime.engine;

import com.hazelcast.jet.core.Watermark;
import io.tapstate.core.event.ChainPosition;
import io.tapstate.core.event.Envelope;
import java.util.List;
import java.util.Map;

/**
 * How far a sink may say each chain has durably landed. What that takes depends on the shape of the graph
 * above the sink, and the two shapes need different answers, so the answer is settled while the graph is
 * compiled rather than worked out from what arrives.
 *
 * <p>Where the stream reaching the sink is one chain's changes in their own order, an event's position and
 * the order it settles in are the same thing, and the frontier is the prefix of positions that are provably
 * complete. Where the stream was assembled from several chains at once, neither holds: a document draws on
 * changes of several chains, and the changes of one chain reach the sink in whatever order the assembly let
 * them out. Nothing that arrives can then say by itself how far a chain has travelled, and the sink is told
 * separately - by a bound combined across every input queue - how far it may go.
 *
 * <p>Both are one-way: a position acked is a position a restart will not replay, so an answer that is too
 * high loses data with nothing reporting it, while an answer that is too low costs a repeated write against
 * an idempotent target. Every choice here is made in that direction.
 */
interface SinkFrontier {

    /** One chain's position, as one settled event reported it. */
    record ChainEntry(String chain, ChainPosition position) {
    }

    /**
     * What {@code batch} contributes to the frontier, in arrival order; empty when nothing in it takes
     * part. Called when the batch is handed to the writer, so what a settled batch proves is fixed at the
     * point the events are still owned here.
     */
    List<ChainEntry> positions(List<Envelope> batch);

    /**
     * Takes in the positions of a batch that has now settled - every event of it durably written - and
     * acks whatever they let the frontier reach.
     */
    void settled(List<ChainEntry> positions, SinkAck ack);

    /**
     * Takes in a bound from upstream and acks whatever it now lets the frontier reach. A frontier that
     * does not use bounds discards it: the source stamps them whatever the shape of the graph, so they
     * arrive at every sink, and only one shape has anything to do with them.
     */
    void bound(Watermark bound, SinkAck ack);

    /**
     * How far each chain's bound runs ahead of the position this frontier reached, keyed by chain; empty
     * where there is no such distance to report.
     *
     * <p>This is what tells apart the two ways a frontier stalls, which are acted on from opposite ends. A
     * chain held back by changes still pending upstream has a bound that stops climbing, so the frontier
     * sits right on it and reports no distance however long the stall lasts — the work is upstream, on
     * whatever is holding those changes. A chain whose bound keeps climbing over positions it was never
     * given reports a distance that grows, and nothing upstream is stuck at all — it is short of positions
     * to advance to, and only supplying them shortens it. Without the number the two are one symptom.
     */
    Map<String, Long> gaps();
}
