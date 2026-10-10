package io.tapstate.runtime.engine;

import com.hazelcast.function.SupplierEx;
import com.hazelcast.jet.core.AbstractProcessor;
import com.hazelcast.jet.core.Processor;
import com.hazelcast.jet.core.ProcessorMetaSupplier;
import com.hazelcast.jet.core.ProcessorSupplier;
import com.hazelcast.jet.core.Watermark;
import io.tapstate.core.event.Envelope;
import io.tapstate.core.event.Op;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

/**
 * Stands in front of a sink that runs on several processors, and sends each row the way it has to reach the
 * sink's writers.
 *
 * <p>A snapshot row of a keyed target table goes to whichever writer has room: a load's rows may land in any
 * order, and handing them out as writers free up is what lets every writer take part rather than leaving a
 * slow one with a fixed share of the table. Every other row - a change, a row of a table with no key, a schema
 * change, word that a chain got past positions with nothing to deliver - goes to the writer its target table
 * and key belong to, so one row's changes are applied on one writer in the order they were read. The two ways
 * are two outbound edges, and a row takes exactly one of them.
 *
 * <p>Rows reach it over a distributed edge keyed the same way, so every instance hears every upstream
 * processor's bounds and speaks for every chain the sink receives. It combines them edge by edge, over the
 * edges that carry each chain, and passes the combined bound on along both of its edges. An instance fed only
 * by the members it runs on would say nothing about a chain whose only upstream runs elsewhere while it kept
 * forwarding another chain's rows, and every writer would wait on it for that chain for as long as the run
 * lasts.
 */
final class SinkRouter extends AbstractProcessor {

    /** The outbound ordinal a keyed table's snapshot rows take: to whichever writer has room. */
    static final int SPREAD = 0;

    /** The outbound ordinal everything else takes: to the writer its target table and key belong to. */
    static final int BY_KEY = 1;

    private final Set<String> spreadStreams;
    private final LevelBounds edges;

    SinkRouter(Set<String> spreadStreams, LevelBounds edges) {
        this.spreadStreams = Set.copyOf(spreadStreams);
        this.edges = edges;
    }

    /**
     * A meta-supplier for the router of the sink whose streams write the targets given, as wide on every member
     * as the sink it stands in front of. {@code chainsByOrdinal} says which chains each inbound edge carries;
     * with no chain numbering, bounds are not passed on at all, which leaves the sink's frontier standing still
     * rather than running ahead.
     */
    static ProcessorMetaSupplier metaSupplier(Map<String, SinkTarget> targetsByStream, ChainAxes axes,
            Map<Integer, List<String>> chainsByOrdinal, int plannedMembers) {
        Objects.requireNonNull(targetsByStream, "targetsByStream");
        Set<String> spread = spreadStreamsOf(targetsByStream);
        SupplierEx<Processor> supplier = axes == null
                ? () -> new SinkRouter(spread, null)
                : () -> new SinkRouter(spread, new LevelBounds(chainsByOrdinal, axes, LevelBounds.HOLDS_NOTHING));
        return PlannedMembersGuard.of(ProcessorMetaSupplier.of(ProcessorSupplier.of(supplier)), plannedMembers);
    }

    /** The streams whose snapshot rows may go to any writer: those writing a table rows of which have a key. */
    static Set<String> spreadStreamsOf(Map<String, SinkTarget> targetsByStream) {
        return targetsByStream.entrySet().stream()
                .filter(entry -> entry.getValue().keyed())
                .map(Map.Entry::getKey)
                .collect(java.util.stream.Collectors.toUnmodifiableSet());
    }

    @Override
    protected boolean tryProcess(int ordinal, Object item) {
        return tryEmit(spread(item) ? SPREAD : BY_KEY, item);
    }

    private boolean spread(Object item) {
        return item instanceof Envelope event && event.op() == Op.READ && spreadStreams.contains(event.src());
    }

    /**
     * Passes on, along both edges, what the edges carrying the bound's chain have together promised. Per edge
     * is the only variant that reaches a chain not every edge carries.
     */
    @Override
    public boolean tryProcessWatermark(int ordinal, Watermark watermark) {
        return edges == null || edges.advance(ordinal, watermark, this::tryEmit);
    }

    /**
     * Refuses the bound the engine combined across every edge; see the per-edge variant. The engine forwards
     * it by default, silently, which is why saying otherwise is explicit.
     */
    @Override
    public boolean tryProcessWatermark(Watermark watermark) {
        return true;
    }
}
