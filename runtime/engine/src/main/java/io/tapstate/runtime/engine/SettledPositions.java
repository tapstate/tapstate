package io.tapstate.runtime.engine;

import io.tapstate.core.event.ChainPosition;
import java.io.Serializable;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;

/**
 * Word that a chain has got past these positions with nothing left to deliver for them: the changes they
 * cover are durable where they were absorbed, and no record about them is on its way to any sink.
 *
 * <p><b>It exists because a frontier is two things and only one of them travels on a record.</b> How far a
 * sink may say a chain has landed is the highest position that is both provably durable and at or below the
 * bound, and the sink learns a position is durable by writing the record that carries it. A change absorbed
 * somewhere upstream, that no record will ever carry, is just as durable and has no way to say so - so the
 * chain stops at the last position that did produce a record, for the rest of the job's life, while its
 * bound climbs on without it. Nothing reports that: the job runs, every record is correct, and the only
 * trace is a restart replaying a table from further back every day.
 *
 * <p><b>It travels the same edge as the records it must not overtake, and that is the whole of its ordering
 * contract.</b> A position is only safe to ack once every record carrying a lower position on that chain
 * has settled, and the one thing that guarantees it is arriving behind them at a sink that settles its
 * batches in order. Whoever holds records back has to hold this back with them; sent ahead, it would ack
 * past a record that is still to be written and leave that change neither delivered nor replayable.
 *
 * <p>What it carries is the highest such position per chain rather than each of them, because nothing is
 * ever advanced to a position beneath one already proven: the frontier takes the highest at or below its
 * bound and forgets the rest. Folding them costs an advance to nothing, and saves a word per change on a
 * stream where these are the ordinary case rather than the exception.
 */
public record SettledPositions(Map<String, ChainPosition> positions) implements Serializable {

    public SettledPositions {
        positions = Collections.unmodifiableMap(new LinkedHashMap<>(positions));
    }

    /**
     * Folds {@code arrived} into {@code highest}, keeping the higher position on each chain it names. A
     * position with no order is passed over: it cannot be placed against a bound, so a frontier can do
     * nothing with it and keeping it would only displace one that can.
     */
    public static void fold(Map<String, ChainPosition> highest, Map<String, ChainPosition> arrived) {
        arrived.forEach((chain, position) -> {
            if (position.order() == null) {
                return;
            }
            ChainPosition held = highest.get(chain);
            if (held == null || position.order().compareTo(held.order()) > 0) {
                highest.put(chain, position);
            }
        });
    }
}
