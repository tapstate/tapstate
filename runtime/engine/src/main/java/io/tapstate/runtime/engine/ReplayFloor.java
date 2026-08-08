package io.tapstate.runtime.engine;

import io.tapstate.core.event.SourceOrder;
import java.io.Serializable;
import java.util.Optional;

/**
 * Where a restart would resume reading a chain: the position a sink has durably acked, read back by an
 * operator that sits upstream of that sink. Nothing below it can ever be delivered again, which is what
 * makes it safe to forget the record that a change was already applied.
 *
 * <p>It is read, never written. The sink advances the durable position as it confirms writes; this is the
 * other direction, and the graph gives it no path of its own — the operators that need it run upstream of
 * the sink and the graph has no cycles, so the value cannot travel back along the data flow. It is read
 * beside the flow instead, on the member, and never per event: one crossing to the durable plane for every
 * change would cost more than the work the change itself does.
 *
 * <p>An absent answer means "not known", never "nothing acked yet". A member with no store bound, a chain
 * with no consumer record, and a store that cannot be read all answer the same way, and every one of them
 * must leave the caller doing less rather than more — forgetting a record because the floor could not be
 * read is how a change that was already deleted comes back.
 *
 * <p>{@link Serializable} so it can travel on the graph to the member that runs the operator, carrying only
 * the coordinates it needs and resolving the store member-side; the store itself never crosses the wire.
 */
@FunctionalInterface
public interface ReplayFloor extends Serializable {

    /** A floor that knows nothing, so every caller holds on to whatever it was weighing up dropping. */
    ReplayFloor NONE = chain -> Optional.empty();

    /**
     * The position on {@code chain} a restart would resume from, or empty when it is not known. Everything
     * strictly below the answer has been durably acked and will not be delivered again.
     */
    Optional<SourceOrder> of(String chain);
}
