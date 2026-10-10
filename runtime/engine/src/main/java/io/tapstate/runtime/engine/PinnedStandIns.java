package io.tapstate.runtime.engine;

import com.hazelcast.jet.core.Processor;
import java.util.Set;

/**
 * What stands in for a vertex that runs one processor for the whole cluster, on each member it does not run on: the
 * engine's own placeholders, and the one that passes the vertex's bounds on. None of them does any work, so a read of
 * where a run's work is done counts them apart - and knows them by the processor type the engine tags every instance
 * with, which is its class's simple name.
 */
public final class PinnedStandIns {

    private static final Set<String> TYPES =
            Set.of("ExpectNothingP", "NoopP", TotalOne.BoundsStandIn.class.getSimpleName());

    private PinnedStandIns() {
    }

    /** Whether an instance the engine tagged {@code processorType} stands in for a pinned vertex. */
    public static boolean isStandIn(String processorType) {
        return processorType != null && TYPES.contains(processorType);
    }

    /** Whether {@code processor} stands in for a pinned vertex. */
    static boolean isStandIn(Processor processor) {
        return isStandIn(processor.getClass().getSimpleName());
    }
}
