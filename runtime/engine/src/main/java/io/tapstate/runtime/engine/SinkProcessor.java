package io.tapstate.runtime.engine;

import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.function.SupplierEx;
import com.hazelcast.jet.core.AbstractProcessor;
import com.hazelcast.jet.core.Inbox;
import com.hazelcast.jet.core.Processor;
import com.hazelcast.jet.core.ProcessorMetaSupplier;
import com.hazelcast.jet.core.ProcessorSupplier;
import com.hazelcast.jet.core.Watermark;
import io.tapstate.core.event.Envelope;
import io.tapstate.runtime.engine.SinkFrontier.ChainEntry;
import io.tapstate.spi.sink.SinkWriter;
import io.tapstate.spi.sink.WriteResult;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;

/**
 * Drives a {@link SinkWriter} from a Jet vertex: it batches inbound events, keeps a bounded number of
 * writes in flight, and completes only once every write has settled. The write side of the contract
 * hands pacing to the runtime, and this is where it lives — batching, the in-flight bound, and the
 * backpressure it produces are all here, while the writer stays a pure delivery contract. One adapter
 * serves every sink; the write mode and ddl policy fold into the writer the factory opens, not here.
 *
 * <p>Backpressure is by refusal: when the in-flight bound is reached the processor stops draining its
 * inbox, so Jet holds the upstream back until an outstanding write settles and frees a slot. A batch's
 * events are handed to the writer and never touched again, honouring the writer's ownership window.
 *
 * <p>The vertex runs at total parallelism one and, by default, keeps a single write in flight: one
 * {@code serve.sync} is one external target, and applying one batch to completion before the next is
 * issued is what keeps a key's change events in their arrival order. Two batches that straddle a key
 * (an insert last in one, its update first in the next) would otherwise be free to apply out of order,
 * since the writer runs them off the caller's thread with no ordering of its own — the contract hands
 * in-flight ordering to the runtime, and this is where the runtime keeps it. Raising the in-flight
 * bound pipelines writes for throughput and is therefore only for a sink whose target applies writes
 * order-independently or is append-only; it is not the default. Snapshotting is not implemented: the
 * durable offset is the source's, not Jet's, so a restart replays from the source rather than
 * resuming a sink snapshot.
 */
public final class SinkProcessor extends AbstractProcessor {

    // One write in flight by default: a batch is applied to completion before the next is issued, so a
    // key's events can never be applied out of their arrival order. Raising this pipelines writes and
    // is only safe for an order-independent or append-only target.
    private static final int DEFAULT_MAX_IN_FLIGHT = 1;
    private static final int DEFAULT_MAX_BATCH_SIZE = 1024;

    private final SinkWriter writer;
    private final SinkAck sinkAck;
    private final SinkFrontier frontier;
    private final FrontierGauge gauge;
    private final int maxInFlight;
    private final int maxBatchSize;
    private final List<InFlightBatch> inFlight = new ArrayList<>();
    private boolean closed;

    // Resolved at init from the running job, so a failed write can be recorded against this pipeline's id
    // before it leaves this processor — see reapSettled and JobFailureRegistry.
    private String pipelineId;
    private JobFailureRegistry failureRegistry;

    /** No sink-ack watermark: the order-independent or append-only path (any in-flight bound is allowed). */
    public SinkProcessor(SinkWriter writer, int maxInFlight, int maxBatchSize) {
        this(writer, null, null, maxInFlight, maxBatchSize);
    }

    /** An ack-bearing sink whose frontier readings go nowhere: for driving one outside a running job. */
    public SinkProcessor(SinkWriter writer, SinkAck sinkAck, SinkFrontier frontier,
            int maxInFlight, int maxBatchSize) {
        this(writer, sinkAck, frontier, maxInFlight, maxBatchSize, FrontierGauge.none());
    }

    SinkProcessor(SinkWriter writer, SinkAck sinkAck, SinkFrontier frontier,
            int maxInFlight, int maxBatchSize, FrontierGauge gauge) {
        this.writer = Objects.requireNonNull(writer, "writer");
        this.gauge = Objects.requireNonNull(gauge, "gauge");
        if (maxInFlight < 1) {
            throw new IllegalArgumentException("maxInFlight must be at least 1: " + maxInFlight);
        }
        if (maxBatchSize < 1) {
            throw new IllegalArgumentException("maxBatchSize must be at least 1: " + maxBatchSize);
        }
        if (sinkAck != null) {
            Objects.requireNonNull(frontier, "frontier");
            if (maxInFlight != 1) {
                throw new IllegalArgumentException(
                        "a sink-ack watermark requires maxInFlight == 1 so batches settle in order; got "
                                + maxInFlight);
            }
        }
        this.sinkAck = sinkAck;
        this.frontier = frontier;
        this.maxInFlight = maxInFlight;
        this.maxBatchSize = maxBatchSize;
    }

    /**
     * A meta-supplier for a sink vertex that drives the writer the factory opens. The factory (not a
     * prebuilt writer) is what the DAG carries, so the writer is opened on the member that runs the
     * vertex. The vertex is pinned to total parallelism one.
     */
    public static ProcessorMetaSupplier metaSupplier(SupplierEx<? extends SinkWriter> writerFactory) {
        Objects.requireNonNull(writerFactory, "writerFactory");
        SupplierEx<Processor> supplier =
                () -> new SinkProcessor(writerFactory.get(), DEFAULT_MAX_IN_FLIGHT, DEFAULT_MAX_BATCH_SIZE);
        return ProcessorMetaSupplier.forceTotalParallelismOne(ProcessorSupplier.of(supplier));
    }

    /**
     * A meta-supplier for a sink vertex that also advances a durable sink-ack watermark. The ack is carried
     * as a {@link SinkAckFactory}, not a prebuilt {@link SinkAck}: the durable store it writes is not
     * serializable, so only the factory travels on the DAG and the store is resolved on the member that runs
     * the vertex. The vertex is pinned to total parallelism one and keeps a single write in flight, the
     * order-preserving contract every shape of frontier below it depends on.
     *
     * <p>{@code frontierFactory} settles which shape that is, and it is settled here rather than run-time:
     * how far a sink may say a chain has landed depends on whether what reaches it is one chain in order or
     * an assembly of several, and that is a property of the graph that was compiled, not of any event.
     */
    static ProcessorMetaSupplier metaSupplier(SupplierEx<? extends SinkWriter> writerFactory,
            SinkAckFactory sinkAckFactory, SupplierEx<SinkFrontier> frontierFactory) {
        Objects.requireNonNull(writerFactory, "writerFactory");
        Objects.requireNonNull(sinkAckFactory, "sinkAckFactory");
        Objects.requireNonNull(frontierFactory, "frontierFactory");
        return ProcessorMetaSupplier.forceTotalParallelismOne(
                new AckSinkSupplier(writerFactory, sinkAckFactory, frontierFactory));
    }

    /**
     * Resolves this pipeline's id and the shared failure registry, both keyed off the running job. A
     * context with no Hazelcast instance (a bare unit test driving the processor directly, never through
     * a real job) leaves the registry unset; {@link #reapSettled} tolerates that and simply does not
     * record — a failure still fails the job exactly as before, only unrecorded.
     */
    @Override
    protected void init(Processor.Context context) {
        this.pipelineId = context.jobConfig().getName();
        HazelcastInstance instance = context.hazelcastInstance();
        this.failureRegistry = instance != null ? JobFailureRegistry.of(instance) : null;
    }

    @Override
    public void process(int ordinal, Inbox inbox) {
        reapSettled();
        while (!inbox.isEmpty() && inFlight.size() < maxInFlight) {
            List<Envelope> batch = new ArrayList<>();
            while (batch.size() < maxBatchSize && !inbox.isEmpty()) {
                batch.add((Envelope) inbox.poll());
            }
            inFlight.add(new InFlightBatch(
                    writer.write(batch).toCompletableFuture(), positionsOf(batch)));
        }
        // A saturated in-flight set leaves the rest of the inbox unread; Jet backpressures upstream
        // until reapSettled frees a slot on a later call.
    }

    @Override
    public boolean complete() {
        reapSettled();
        return inFlight.isEmpty();
    }

    /**
     * Reaps settled writes while the inbox is idle. Jet calls this when there is no input to hand the
     * processor, which is the only call a streaming sink is guaranteed after its last batch: its source
     * never completes, so {@link #complete()} never runs, and a batch that fails may be the last one, so no
     * later {@link #process} arrives to reap it either. Left to those two, a failed write would sit
     * unsurfaced and the job would run on moving nothing - an error behind a healthy-looking state. Reaping
     * here surfaces it, and also lets a write that settles during a lull advance the sink-ack watermark
     * without waiting for the next batch.
     */
    @Override
    public boolean tryProcess() {
        reapSettled();
        return true;
    }

    /**
     * Takes in a bound that arrived with no edge attached to it, and never passes it on. This variant is
     * the one a sink wants: the engine calls it with the value combined across every input queue, which is
     * the guarantee the frontier rests on - a queue that has said nothing still holds it down. The
     * per-edge variant would answer with one edge's promise while data covered by it sits on another.
     *
     * <p>Nothing is emitted from here. This is the end of the line for a bound, and anything emitted would
     * be offered to the target as a record. The engine forwards it by default, silently, which is why
     * saying otherwise is explicit.
     */
    @Override
    public boolean tryProcessWatermark(Watermark watermark) {
        if (frontier != null) {
            frontier.bound(watermark, sinkAck);
            reportTrailing();
        }
        return true;
    }

    /**
     * Takes no notice of a bound that arrived on one edge. Answering true is what lets the engine combine
     * it with the other edges and deliver the combined value to the variant above; acting on it here would
     * be acting on one edge's promise alone. Written out rather than left to the interface default, which
     * is the same answer for a different reason.
     */
    @Override
    public boolean tryProcessWatermark(int ordinal, Watermark watermark) {
        return true;
    }

    @Override
    public void close() {
        if (closed) {
            return;
        }
        closed = true;
        writer.close();
    }

    /** Removes every settled write, surfacing the cause of a failed one so it fails the job. */
    private void reapSettled() {
        inFlight.removeIf(batch -> {
            if (!batch.future().isDone()) {
                return false;
            }
            try {
                settle(batch.future()); // throws on a failed write, before any position advances
            } catch (RuntimeException | Error failure) {
                // Recorded here, synchronously, before this rethrow ever reaches Jet's own tasklet
                // machinery: once the job's terminal result is durable Jet can no longer hand back this
                // exact cause (see JobFailureRegistry), so the last point this processor still holds the
                // real, unwrapped cause is the only reliable place to keep it.
                if (failureRegistry != null) {
                    failureRegistry.record(pipelineId, failure);
                }
                throw failure;
            }
            if (frontier != null) {
                frontier.settled(batch.positions(), sinkAck);
            }
            return true;
        });
        if (frontier != null) {
            reportTrailing();
        }
    }

    /**
     * Takes a reading of how far the frontier trails its bounds. Taken both here and where a bound arrives,
     * because either one alone goes quiet in the case that matters: a chain starved of positions to advance
     * to has nothing settling, and a chain whose last batch settles into a lull gets no further bound. Left
     * to one of them the last reading before the stall would stand as the current one for as long as the
     * stall lasts, which reads as a healthy distance rather than a stalled measurement.
     */
    private void reportTrailing() {
        gauge.trailing(frontier.gaps());
    }

    /** What this batch contributes to the frontier, empty when no frontier is tracked. */
    private List<ChainEntry> positionsOf(List<Envelope> batch) {
        return frontier == null ? List.of() : frontier.positions(batch);
    }

    /**
     * Settles one completed write. A failed write is a user-diagnosable delivery error the writer
     * already raised (coded, for a real connector); the cause is rethrown as-is so it fails the job
     * unwrapped rather than buried in a {@link CompletionException}.
     */
    private static void settle(CompletableFuture<WriteResult> future) {
        try {
            future.join();
        } catch (CompletionException wrapper) {
            Throwable cause = wrapper.getCause();
            if (cause instanceof RuntimeException runtime) {
                throw runtime;
            }
            if (cause instanceof Error error) {
                throw error;
            }
            throw wrapper;
        }
    }

    /** One outstanding write and what its batch contributes to the frontier once it settles. */
    private record InFlightBatch(CompletableFuture<WriteResult> future, List<ChainEntry> positions) {
    }

    /**
     * Resolves the sink-ack member-side, then supplies the ack-bound sink processors. The factory travels
     * serialized; only on the member - where {@link ProcessorSupplier#init} hands it the running instance -
     * is the durable store resolved and the ack bound. The writer is likewise opened per processor on the
     * member, so nothing but serializable coordinates crosses the wire.
     */
    private static final class AckSinkSupplier implements ProcessorSupplier {

        private final SupplierEx<? extends SinkWriter> writerFactory;
        private final SinkAckFactory sinkAckFactory;
        private final SupplierEx<SinkFrontier> frontierFactory;
        private transient SinkAck sinkAck;

        AckSinkSupplier(SupplierEx<? extends SinkWriter> writerFactory,
                SinkAckFactory sinkAckFactory, SupplierEx<SinkFrontier> frontierFactory) {
            this.writerFactory = writerFactory;
            this.sinkAckFactory = sinkAckFactory;
            this.frontierFactory = frontierFactory;
        }

        @Override
        public void init(Context context) {
            sinkAck = sinkAckFactory.resolve(context.hazelcastInstance());
        }

        @Override
        public Collection<? extends Processor> get(int count) {
            List<Processor> processors = new ArrayList<>(count);
            for (int i = 0; i < count; i++) {
                // A gauge per processor, not one shared: the handles it keeps belong to the sink that took
                // the reading, and a shared one would have each sink's readings land under the other's.
                processors.add(new SinkProcessor(writerFactory.get(), sinkAck, frontierFactory.get(),
                        DEFAULT_MAX_IN_FLIGHT, DEFAULT_MAX_BATCH_SIZE, new JetFrontierGauge()));
            }
            return processors;
        }
    }
}
