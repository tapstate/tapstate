package io.tapstate.runtime.engine;

import io.tapstate.core.event.ChainPosition;
import io.tapstate.core.event.SourceOrder;
import java.io.Serializable;

/**
 * The seam a sink uses to advance one chain's durable sink-acked source position. The processor knows a
 * chain only by the {@code src} stream name its events carry, so it advances {@code (chain, position)} and
 * leaves the rest to the binding: the assembly root wires an implementation that maps the stream to its
 * mining chain and pipeline and writes the durable store.
 *
 * <p>The position is a pair, and it has to be. What is compared — here and by everything that later asks
 * whether a change may be dropped — is the order the engine assigned, because a connector's token is
 * opaque and carries no order at all. What is written down is the token, because that is what a read
 * resumes from. Neither stands in for the other.
 *
 * <p>A position with an order but no token is a snapshot row: ordered, because the engine saw where it
 * sat, and tokenless, because it is not a spot in a change stream. It advances the frontier like any
 * other, and what gets persisted for it is the chain's cdc start position — no change has been confirmed
 * yet, so a read resumes where changes begin. Passing over it because the token is absent leaves a
 * snapshot-only read with a frontier that never moves at all.
 *
 * <p>{@link Serializable} so it can travel on the DAG to the member that runs the sink, carrying only the
 * coordinates it needs and resolving the store member-side, the same way the source's read-cursor
 * publisher does — the durable store itself never crosses the wire.
 */
@FunctionalInterface
public interface SinkAck extends Serializable {

    /**
     * Advances {@code chain}'s durable watermark to {@code position}. The caller only ever advances, never
     * lowers, so the store persists what it is given.
     */
    void advance(String chain, ChainPosition position);

    /**
     * Says that nothing on {@code chain} at or below {@code through} is still unwritten at this writer: every
     * change it was given up to there has landed, and a bound says none further down is on its way. It is how
     * a writer given none of a chain's rows still says it is holding none of them back - without it, such a
     * writer would pin how far the chain has landed at wherever it last had anything to write, for as long
     * as the run lasts. An ack that records per writer keeps it; any other has nothing to do with it.
     */
    default void bounded(String chain, SourceOrder through) {
    }

    /**
     * This ack, reporting as the writer {@code writerId}. Where several writers land one pipeline's changes,
     * how far the pipeline has landed a chain is the lowest of what each has landed, so each has to report as
     * itself; an ack that does not keep writers apart answers with itself.
     */
    default SinkAck forWriter(String writerId) {
        return this;
    }
}
