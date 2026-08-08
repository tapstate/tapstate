package io.tapstate.runtime.srs;

import io.tapstate.core.event.ChainPosition;
import io.tapstate.core.event.Envelope;
import io.tapstate.core.event.SourceOrder;
import java.util.Map;

/**
 * Projects a cdc change out of the per-table change ring into the transform-facing envelope currency.
 * This is the one place a cdc change's position and order enter that currency: the item's opaque position
 * token and the ring's sequence within the running generation become the one spot the envelope covers, on
 * the stream it was read from. The transform chain carries that through and the sink acks it back, so the
 * durable source-read frontier never passes an unacked change.
 *
 * <p>The two are separate answers to separate questions and are stamped together here so that they always
 * belong to the same change. The token is a connector value the engine never parses — only equality is
 * defined on it — so it can resume a read but can never say which of two changes came first. The order is
 * the engine's own and is what every stateful node compares. The sequence comes from the reader rather
 * than the item because the ring assigns it on append and keeps it.
 *
 * <p>The stream name is injected, not read from the item — the ring is per-table, so the item does not
 * carry one; the reader is bound to a source vertex that knows the logical stream it feeds. Schema stays
 * null in the lean tier: the item points at schema history by version rather than repeating it inline.
 * Snapshot reads never reach here (they bypass the ring), so a projected event always has a position.
 */
final class SrsProjection {

    private SrsProjection() {
    }

    /**
     * The envelope for one ring item on the stream {@code src}, carrying the item's source position and
     * the {@code order} the engine assigns it.
     */
    static Envelope toEnvelope(SrsItem item, String src, SourceOrder order) {
        return new Envelope(item.op(), item.ts(), src, item.before(), item.after(), null,
                Map.of(src, new ChainPosition(order, item.srcPos().token())));
    }
}
