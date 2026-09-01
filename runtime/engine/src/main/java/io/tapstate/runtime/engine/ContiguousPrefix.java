package io.tapstate.runtime.engine;

import com.hazelcast.jet.core.Watermark;
import io.tapstate.core.event.ChainPosition;
import io.tapstate.core.event.Envelope;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * The frontier of a sink fed by one chain's changes in their own order: the prefix of positions that are
 * provably complete. A settled batch proves every event at or below its positions is durably written, but a
 * position stays open until a strictly higher one settles — a fan-out could still place more of it in a
 * later batch. When a higher position closes the open one, the old open position is a complete prefix and
 * is acked exactly once.
 *
 * <p>Which of two positions is higher is the engine's own order, never the connector's token. The token is
 * opaque by contract and only equality is defined on it; ranking positions by it needed an order no
 * connector can supply, and the one implementation that existed ranked test values. The order the engine
 * assigned as it read is the same sequence, observed rather than computed.
 *
 * <p>A position is closed by either of two proofs, and the second is not a refinement of the first. A
 * strictly higher settled position closes it, and so does a bound: the source saying nothing at or below
 * that value is still to come. Only the second reaches a table read to exhaustion. Every row of one
 * snapshot carries the same reserved position, so while a load runs no higher position of that table ever
 * settles, and on the first proof alone the frontier stands still over a table it has written in full --
 * which is a table nobody can record as done, and so a table read again from the start on every resume.
 *
 * <p>A graph with no chain numbering takes no bound in at all, because a bound names its chain by an axis
 * and nothing there can say which chain an axis carries. Standing still is the direction to fail in: the
 * alternative acks one chain on a promise made about another.
 */
final class ContiguousPrefix implements SinkFrontier {

    // Per chain: the highest position that has settled but is not yet proven closed. The frontier lags this
    // by one position, so a fan-out's every output is in before the position is acked.
    private final Map<String, ChainPosition> openByChain = new HashMap<>();
    private final ChainAxes axes;

    /** A frontier over a graph with no chain numbering: bounds cannot be attributed, so none is taken in. */
    ContiguousPrefix() {
        this(null);
    }

    /**
     * A frontier over the chains {@code axes} numbers, which is what lets a bound name the chain it is for.
     * An axis the numbering does not know tears down rather than being passed over: a bound that cannot be
     * attributed is not a bound about nothing, it is one about a chain this frontier failed to identify.
     */
    ContiguousPrefix(ChainAxes axes) {
        this.axes = axes;
    }

    /**
     * The last position each chain contributes to this batch. Under the single-in-flight, order-preserving
     * contract a chain's last event in the batch is its highest position there. An event a transform built
     * carries no position of its own and does not take part: it inherits one a real event already reported.
     *
     * <p>Taking part is decided on the order alone. A snapshot row has one and carries no token, which is
     * the chain's cdc start being persisted for it rather than a reason to pass it over -- passing it over
     * leaves a table whose rows are the whole of what the sink was given as a table the sink is never
     * recorded as having written.
     */
    @Override
    public List<ChainEntry> positions(List<Envelope> batch) {
        Map<String, ChainPosition> last = new LinkedHashMap<>();
        for (Envelope event : batch) {
            event.positions().forEach((chain, position) -> {
                if (position.order() != null) {
                    last.put(chain, position);
                }
            });
        }
        List<ChainEntry> entries = new ArrayList<>(last.size());
        last.forEach((chain, position) -> entries.add(new ChainEntry(chain, position)));
        return entries;
    }

    @Override
    public void settled(List<ChainEntry> positions, SinkAck ack) {
        for (ChainEntry entry : positions) {
            ChainPosition open = openByChain.get(entry.chain());
            if (open == null) {
                openByChain.put(entry.chain(), entry.position());
            } else if (entry.position().order().compareTo(open.order()) > 0) {
                ack.advance(entry.chain(), open);
                openByChain.put(entry.chain(), entry.position());
            }
            // At or below the open position: the same position again (a fan-out continuation), or an
            // out-of-order arrival if a precondition were violated - either way do not advance and do not
            // regress.
        }
    }

    /**
     * Takes in a bound and acks the open position it covers. What is acked is the position that settled,
     * never the bound itself: a bound is a promise about what is still coming, not a report of what is
     * durable, and the two differ by every batch still in flight beneath it.
     *
     * <p>A bound beneath the open position closes nothing. Acting on one would report a position durable on
     * the strength of a promise made about lower ones.
     *
     * <p>Nothing is kept for later here, because a bound arrives only once it can be acted on: the sink
     * holds it back until no write is in flight, and by then every event beneath it has settled and the
     * position it closes is already open. Keeping one would be state that no case can reach - measured
     * 2026-09-01 by removing this method's whole settle-side counterpart, which reddened nothing at all.
     */
    @Override
    public void bound(Watermark bound, SinkAck ack) {
        if (axes == null) {
            return;
        }
        String chain = axes.chainOn(bound.key());
        ChainPosition open = openByChain.get(chain);
        if (open != null && FrontierOrders.pack(chain, open.order()) <= bound.timestamp()) {
            ack.advance(chain, open);
            openByChain.remove(chain);
        }
    }

    /**
     * Reports no distance. A bound does run ahead of this frontier now, but only for as long as the writes
     * it covers take to settle - it is handed over once nothing is left in flight, and acted on there and
     * then. What it never does is accumulate: the distance is bounded by one batch rather than growing with
     * how long a stall lasts, so there is no burn here of the kind this reading is watched for.
     */
    @Override
    public Map<String, Long> gaps() {
        return Map.of();
    }

    /**
     * Reports no chain as pinned. What this frontier holds back is one position and never more: a position
     * is acked as soon as either proof reaches it, so the durable position trails the newest change by at
     * most one however long a run lasts. Nothing accumulates here, so there is no pin whose age could grow
     * into a retention window - which is the same structural reason there is no distance to report.
     *
     * <p>The one case that used to sit outside this is now covered: a chain going quiet no longer leaves
     * its last position open for as long as the quiet lasts, because the source's bound closes it. What is
     * still not covered is a write that never settles - the bound is then held rather than acted on, which
     * is the safe direction and is a stall of the writer rather than of the frontier.
     */
    @Override
    public Map<String, Long> stalls() {
        return Map.of();
    }
}
