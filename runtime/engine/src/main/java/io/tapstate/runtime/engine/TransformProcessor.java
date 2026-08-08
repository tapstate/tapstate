package io.tapstate.runtime.engine;

import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.function.SupplierEx;
import com.hazelcast.jet.Traversers;
import com.hazelcast.jet.core.AbstractProcessor;
import com.hazelcast.jet.core.Processor;
import com.hazelcast.jet.core.ProcessorMetaSupplier;
import com.hazelcast.jet.core.ProcessorSupplier;
import com.hazelcast.jet.core.Watermark;
import io.tapstate.core.event.Envelope;
import io.tapstate.spi.transform.TransformPort;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/**
 * Wraps a stateless {@link TransformPort} into a Jet processor: for each inbound {@link Envelope} it
 * emits every envelope the port returns — the one event a map keeps, none for a filtered drop, or
 * the several of a fan-out. One adapter serves the whole stateless family (map / filter / a scripted
 * row transform); each is only a different pure function behind the same seam.
 *
 * <p>Carrying the source position across the transform is the adapter's concern, not the port's: the
 * port is a pure function that never sets a position, so the adapter stamps the inbound event's
 * position onto every event the port returns. A fan-out's several outputs all share the one inbound
 * position; its completion is the sink's concern (every output must settle before the position is
 * acked). An inbound event with no position stamps none.
 *
 * <p>Emit-side backpressure is the adapter's concern too: the flat mapper resumes a partially emitted
 * event when the outbox is full, so the port stays a pure function that does not pace itself.
 *
 * <p>The vertex runs at total parallelism one: a sink downstream acks an ordered position stream, and
 * a parallelism-greater-than-one transform would re-lane events and break that order.
 */
public final class TransformProcessor extends AbstractProcessor {

    private final FlatMapper<Envelope, Envelope> flatMapper;
    private final LevelBounds bounds;

    // Resolved at init from the running job — see reapSettled's counterpart in SinkProcessor and
    // JobFailureRegistry for why this is captured here rather than reconstructed after the job fails.
    private String pipelineId;
    private JobFailureRegistry failureRegistry;

    public TransformProcessor(TransformPort port) {
        this(port, null);
    }

    /**
     * An adapter that also passes a frontier on: {@code bounds} works out how far each chain has really
     * got from what arrived on each edge. A null one propagates nothing, which is a frontier that stands
     * still rather than one that runs ahead.
     */
    public TransformProcessor(TransformPort port, LevelBounds bounds) {
        Objects.requireNonNull(port, "port");
        this.bounds = bounds;
        // A port is a pure function over rows and knows nothing about where its input sat, so what the
        // event covered is stamped back onto everything it produced. The whole of it travels, not the
        // token alone: a downstream frontier decides what it may pass on the order, and an output that
        // arrived with a token but no order is one the frontier can only ignore.
        this.flatMapper = flatMapper(event ->
                Traversers.traverseIterable(port.transform(event))
                        .map(out -> out.withPositions(event.positions())));
    }

    /** A meta-supplier for a vertex that propagates no frontier, for a job built without one. */
    public static ProcessorMetaSupplier metaSupplier(SupplierEx<? extends TransformPort> portFactory) {
        return metaSupplier(portFactory, null, null);
    }

    /** Resolves this pipeline's id and the shared failure registry; see {@link SinkProcessor#init}. */
    @Override
    protected void init(Processor.Context context) {
        this.pipelineId = context.jobConfig().getName();
        HazelcastInstance instance = context.hazelcastInstance();
        this.failureRegistry = instance != null ? JobFailureRegistry.of(instance) : null;
    }

    /**
     * A meta-supplier for a DAG vertex that runs this adapter over the port the factory builds. The
     * factory (not a prebuilt port) is what the DAG carries, so the port is constructed on the member
     * that runs the vertex. The vertex is pinned to total parallelism one so it preserves the source
     * position order the sink acks.
     *
     * <p>{@code chainsByOrdinal} says which chains each inbound edge is compiled to carry, which is what
     * the vertex waits on before promising anything about one. It travels with the graph rather than
     * being worked out on the member, for the same reason {@code axes} does: two members that disagreed
     * would combine promises about different chains.
     */
    public static ProcessorMetaSupplier metaSupplier(SupplierEx<? extends TransformPort> portFactory,
            ChainAxes axes, Map<Integer, List<String>> chainsByOrdinal) {
        Objects.requireNonNull(portFactory, "portFactory");
        SupplierEx<Processor> supplier = axes == null
                ? () -> new TransformProcessor(portFactory.get())
                // Holding nothing back is the whole of this level's own contribution: what it may promise
                // is exactly the lowest of what its edges promised.
                : () -> new TransformProcessor(portFactory.get(),
                        new LevelBounds(chainsByOrdinal, axes, chain -> null));
        return ProcessorMetaSupplier.forceTotalParallelismOne(ProcessorSupplier.of(supplier));
    }

    @Override
    protected boolean tryProcess(int ordinal, Object item) {
        try {
            return flatMapper.tryProcess((Envelope) item);
        } catch (RuntimeException | Error failure) {
            // Recorded here, synchronously, before this rethrow ever reaches Jet's own tasklet
            // machinery — see SinkProcessor.reapSettled for why that matters.
            if (failureRegistry != null) {
                failureRegistry.record(pipelineId, failure);
            }
            throw failure;
        }
    }

    /**
     * Works out what this level may promise on the chain the bound travels on, and passes that on rather
     * than the bound itself. Per edge is the only variant that reaches a chain not every edge carries: the
     * combined one is never delivered for such a chain at all, with nothing said anywhere, so a union of
     * two streams would simply never advance.
     */
    @Override
    public boolean tryProcessWatermark(int ordinal, Watermark watermark) {
        return bounds == null || bounds.advance(ordinal, watermark, this::tryEmit);
    }

    /**
     * Refuses to pass on a bound that arrived with no edge attached to it. This adapter holds nothing, so
     * such a bound is not wrong about it — but it has already been combined across every edge feeding the
     * vertex, and repeating it here would make this vertex's promise a copy of a value it never worked out
     * rather than the lowest of what each of its edges promised. The engine forwards it by default,
     * silently, which is why saying otherwise is explicit.
     */
    @Override
    public boolean tryProcessWatermark(Watermark watermark) {
        return true;
    }
}
