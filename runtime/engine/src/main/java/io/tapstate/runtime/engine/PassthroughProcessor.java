package io.tapstate.runtime.engine;

import com.hazelcast.function.SupplierEx;
import com.hazelcast.jet.core.AbstractProcessor;
import com.hazelcast.jet.core.Processor;
import com.hazelcast.jet.core.ProcessorMetaSupplier;
import com.hazelcast.jet.core.ProcessorSupplier;
import com.hazelcast.jet.core.Watermark;
import java.util.List;
import java.util.Map;

/**
 * A vertex that passes every item on unchanged, and works out its own frontier while doing so. It is what
 * a gathering of several streams into one is made of - a union, and the merge that gives a nest node a
 * single edge per stream where one of its aliases resolves to several producers. Nothing is transformed
 * here: the gathering is the topology, and the vertex exists so the ordinals downstream stay unique.
 *
 * <p>Passing items on needs no code at all, but passing bounds on does, and the engine's own default is
 * the wrong one twice over. Its per-edge default takes a bound in and says nothing, and its combined
 * default forwards a value this vertex never worked out. Worse, the combined callback is only ever
 * delivered for a chain that <em>every</em> edge has spoken about — so on the one shape this vertex
 * exists for, two edges each reading their own table, neither chain's bound is ever handed over at all,
 * and everything downstream waits forever with nothing thrown and nothing logged.
 *
 * <p>So the per-edge callback is where the work happens: each edge's promise is recorded against the
 * chains that edge was compiled to carry, and this vertex promises the lowest of them. Holding nothing
 * back is the whole of its own contribution.
 */
public final class PassthroughProcessor extends AbstractProcessor {

    private final LevelBounds bounds;

    public PassthroughProcessor() {
        this(null);
    }

    /**
     * A passthrough that also passes a frontier on: {@code bounds} works out how far each chain has really
     * got from what arrived on each edge. A null one propagates nothing, which is a frontier that stands
     * still rather than one that runs ahead.
     */
    public PassthroughProcessor(LevelBounds bounds) {
        this.bounds = bounds;
    }

    /** A meta-supplier for a passthrough that propagates no frontier, for a job built without one. */
    public static ProcessorMetaSupplier metaSupplier() {
        return metaSupplier(null, null);
    }

    /**
     * A meta-supplier for a passthrough vertex. {@code chainsByOrdinal} says which chains each inbound edge
     * is compiled to carry, which is what the vertex waits on before promising anything about one; it
     * travels with the graph rather than being worked out on the member, because two members that
     * disagreed would combine promises about different chains.
     *
     * <p>Pinned to total parallelism one, like the vertices either side of it: a gathering that re-laned
     * events would break the order a sink downstream acks positions in.
     */
    public static ProcessorMetaSupplier metaSupplier(ChainAxes axes,
            Map<Integer, List<String>> chainsByOrdinal) {
        SupplierEx<Processor> supplier = axes == null
                ? PassthroughProcessor::new
                // Holding nothing back is the whole of this vertex's own contribution: what it may promise
                // is exactly the lowest of what its edges promised.
                : () -> new PassthroughProcessor(new LevelBounds(chainsByOrdinal, axes, chain -> null));
        return ProcessorMetaSupplier.forceTotalParallelismOne(ProcessorSupplier.of(supplier));
    }

    @Override
    protected boolean tryProcess(int ordinal, Object item) {
        return tryEmit(item);
    }

    /**
     * Works out what this vertex may promise on the chain the bound travels on, and passes that on rather
     * than the bound itself. Per edge is the only variant that reaches a chain not every edge carries.
     */
    @Override
    public boolean tryProcessWatermark(int ordinal, Watermark watermark) {
        return bounds == null || bounds.advance(ordinal, watermark, this::tryEmit);
    }

    /**
     * Refuses to pass on a bound that arrived with no edge attached to it. It has already been combined
     * across every edge feeding this vertex, and repeating it here would make this vertex's promise a copy
     * of a value it never worked out rather than the lowest of what each of its edges promised. The engine
     * forwards it by default, silently, which is why saying otherwise is explicit.
     */
    @Override
    public boolean tryProcessWatermark(Watermark watermark) {
        return true;
    }
}
