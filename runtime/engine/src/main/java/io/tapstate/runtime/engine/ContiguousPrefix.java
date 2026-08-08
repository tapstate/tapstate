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
 * <p>Bounds arriving from upstream are discarded here. The source stamps them whether or not anything
 * downstream needs them, and this shape needs nothing: what a settled batch proves is already complete on
 * its own, so taking a bound into account could only lower it.
 */
final class ContiguousPrefix implements SinkFrontier {

    // Per chain: the highest position that has settled but is not yet proven closed. The frontier lags this
    // by one position, so a fan-out's every output is in before the position is acked.
    private final Map<String, ChainPosition> openByChain = new HashMap<>();

    /**
     * The last position each chain contributes to this batch. Under the single-in-flight, order-preserving
     * contract a chain's last event in the batch is its highest position there. An event with no position
     * of its own — a snapshot row, or an event a transform built — does not take part: it is either not a
     * spot in a change stream at all, or it inherits one that a real event already reported.
     */
    @Override
    public List<ChainEntry> positions(List<Envelope> batch) {
        Map<String, ChainPosition> last = new LinkedHashMap<>();
        for (Envelope event : batch) {
            event.positions().forEach((chain, position) -> {
                if (position.order() != null && position.token() != null) {
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

    @Override
    public void bound(Watermark bound, SinkAck ack) {
    }

    /**
     * Reports no distance, because there is none to report rather than because none was measured. Nothing
     * runs ahead of this frontier: what a settled batch proves is already complete on its own, so the
     * position acked is the highest one provably durable and there is no second reading to fall behind.
     */
    @Override
    public Map<String, Long> gaps() {
        return Map.of();
    }
}
