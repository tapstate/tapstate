package io.tapstate.spi.transform;

import io.tapstate.core.event.Envelope;
import java.util.List;

/**
 * The in-pipeline transform of the standard event envelope: the row-level seam a stateless node
 * (map / filter / an array expansion / a scripted row transform) implements. A pure interface over the core ring only
 * (rule R2); it names no engine type.
 *
 * <p>One event maps to zero-or-more events, so the same seam covers the stateless family: a map
 * keeps the one event, a filter drops it by producing none, and a row expansion fans out to several.
 * The transform is a function of the single event it is handed; it holds no cross-event state, and
 * it does not pace itself — how transformed events are batched and the backpressure that bounds them
 * are the runtime's concern, not the port's.
 *
 * <p>This is the predeclared seam: it gives the runtime a compile-time contract for the stateless
 * row-level nodes. The stateful nodes ({@link NodeType#NEST} / {@link NodeType#JOIN}) and the fan-in
 * node ({@link NodeType#UNION}) are not served by this method — they accumulate state or merge many
 * input streams, and their execution contracts land with the execution engine.
 */
@FunctionalInterface
public interface TransformPort {

    /**
     * Transforms one event into the events it becomes: a single event for a map, none for a filtered
     * drop, or several for a fan-out. Returns an empty list to drop; never {@code null}.
     */
    List<Envelope> transform(Envelope event);
}
