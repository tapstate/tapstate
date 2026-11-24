package io.tapstate.runtime.engine;

import io.tapstate.core.event.Envelope;
import io.tapstate.spi.sink.SinkWriter;
import java.util.function.Predicate;

/**
 * An execution-bound writer with an upstream handoff protocol. A marked row has a reserved place
 * in this writer's FIFO before its queued notice can release more computation. The binding owns
 * fan-out accounting and execution identity. Plain sink writers need no per-row lookup.
 */
public interface KeyMoveSinkWriter extends SinkWriter {
    /** The receipt for this effect, or null if it may join an ordinary target batch. */
    KeyMoveHandoffs.Receipt receiptFor(Envelope row);

    /**
     * Builds the admission check for one reserved target batch. A successful test adds one
     * ordinary row to the group. It must be safe for the connector to reorder that row with
     * the reserved effect and every previously accepted row, including all unique constraints.
     * The sink can discard a check and start again when an earlier queue cannot join as a whole.
     */
    default Predicate<Envelope> batchAdmission(Envelope reserved) {
        return other -> false;
    }
}
