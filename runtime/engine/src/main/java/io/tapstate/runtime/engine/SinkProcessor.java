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
import io.tapstate.core.event.PayloadBytes;
import io.tapstate.core.lifecycle.HistogramBounds;
import io.tapstate.core.lifecycle.HistogramValue;
import io.tapstate.core.lifecycle.Stage;
import io.tapstate.core.lifecycle.Staged;
import io.tapstate.runtime.engine.SinkFrontier.ChainEntry;
import io.tapstate.spi.sink.SinkWriter;
import io.tapstate.spi.sink.WriteResult;
import java.util.ArrayList;
import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.function.LongSupplier;
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
 * <p><b>Not everything that arrives is a record.</b> A vertex upstream may also send word that a chain got
 * past changes it has nothing to deliver for — absorbed where they arrived, with no record coming for them
 * ever. Those are never offered to the writer, but they take part in the frontier, and they take part
 * <em>with the batch they arrived in</em> rather than on arrival: a position may only be acked once every
 * record carrying a lower one has landed, and settling with the batch is what makes that true here without
 * anything having to work out what is still outstanding.
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
public final class SinkProcessor extends AbstractProcessor implements Staged {

    @Override
    public Stage stage() {
        return Stage.SINK;
    }

    // One write in flight by default: a batch is applied to completion before the next is issued, so a
    // key's events can never be applied out of their arrival order. Raising this pipelines writes and
    // is only safe for an order-independent or append-only target.
    private static final int DEFAULT_MAX_IN_FLIGHT = 1;
    private static final int DEFAULT_MAX_BATCH_SIZE = 1024;

    private final SinkWriter writer;
    private final SinkAck sinkAck;
    private final SinkFrontier frontier;
    private final FrontierGauge gauge;
    private final DeliveryGauge supplied;
    // Which of the two the readings actually go to, settled at init: the one supplied, or one that reads
    // nothing when this processor turns out not to be running inside a job. See init.
    private DeliveryGauge delivery;
    // What has settled since this processor started, kept here because the readings are cumulative and a
    // batch only knows its own rows. Keyed by table, then by the source operation within it.
    private final Map<String, Map<String, Long>> deliveredByTableAndOp = new LinkedHashMap<>();
    // The newest event time settled per table, epoch milliseconds. Kept beside the counts rather than
    // derived from them: a count says how much arrived and this says how current it is, and a table that
    // is being written steadily with hours-old events reads healthy on the first and not on the second.
    private final Map<String, Long> newestSettledEventTime = new LinkedHashMap<>();
    // Payload bytes settled per table, kept beside the counts for the reason the event times are: how
    // many rows arrived and how much data they were are different questions, and a table whose rows
    // doubled in width answers the first identically.
    private final Map<String, Long> settledBytes = new LinkedHashMap<>();
    // How long each settled row took, from the source's stamp to the confirmed write, bucketed per table
    // over the registered bounds and accumulated since this processor started. A distribution and not an
    // average: the slow rows are the ones anybody reading a delivery time came for, and an average is
    // where they disappear.
    private final Map<String, DurationTotals> settledDurations = new LinkedHashMap<>();
    // The clock a row's delivery is measured against, at the moment its write is confirmed. A seam so a
    // duration can be witnessed at a known instant rather than by waiting for real time to pass.
    private final LongSupplier clock;
    private final int maxInFlight;
    private final int maxBatchSize;
    private final List<InFlightBatch> inFlight = new ArrayList<>();
    // Bounds that arrived while writes were still in flight, held until they settle, one per axis. A bound
    // proves what is still coming, never what is durable: every event it covers has been taken in by the
    // time it arrives, but the ones sitting in an unsettled batch are not written yet. Handing it to the
    // frontier then would let one settled batch of a fan-out stand for the whole of what its change
    // produced.
    //
    // One slot per axis rather than one in total, because a bound names the chain it is for: one chain's
    // promise is not a newer version of another's, and a single slot lets whichever arrives second
    // overwrite the first. The overwritten chain then waits for a strictly higher position of its own to
    // settle, which on a chain that has gone quiet never comes -- so the position it was holding stays
    // open for the life of the run, and on a snapshot that is a table nothing records as loaded and every
    // resume reads again in full.
    private final Map<Byte, Watermark> heldBounds = new LinkedHashMap<>();
    private boolean closed;

    // Resolved at init from the running job, so a failed write can be recorded against this pipeline's id
    // before it leaves this processor — see reapSettled and JobFailureRegistry.
    private String pipelineId;
    private JobFailureRegistry failureRegistry;
    // When this processor began counting, epoch milliseconds. Taken here rather than from the job because
    // the counters are this processor's: an execution that restarts inside a job builds a new processor
    // with its totals back at zero, and a start that did not move with them would describe a stream that
    // no longer exists.
    private long countingSince;

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
        this(writer, sinkAck, frontier, maxInFlight, maxBatchSize, gauge, DeliveryGauge.none());
    }

    SinkProcessor(SinkWriter writer, SinkAck sinkAck, SinkFrontier frontier,
            int maxInFlight, int maxBatchSize, FrontierGauge gauge, DeliveryGauge delivery) {
        this(writer, sinkAck, frontier, maxInFlight, maxBatchSize, gauge, delivery, System::currentTimeMillis);
    }

    SinkProcessor(SinkWriter writer, SinkAck sinkAck, SinkFrontier frontier,
            int maxInFlight, int maxBatchSize, FrontierGauge gauge, DeliveryGauge delivery, LongSupplier clock) {
        this.writer = Objects.requireNonNull(writer, "writer");
        this.gauge = Objects.requireNonNull(gauge, "gauge");
        this.supplied = Objects.requireNonNull(delivery, "delivery");
        this.delivery = this.supplied;
        this.clock = Objects.requireNonNull(clock, "clock");
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
     * vertex. The vertex is pinned to total parallelism one, on the member that owns {@code vertexName}.
     *
     * <p>Naming the member is half of a pair, and the other half is not optional: every edge into this
     * vertex must be {@code distributed().allToOne(vertexName)}. Pinned says there is one processor;
     * reachable says the items get to it. A member with no processor of this vertex answers input with an
     * {@code IllegalStateException} the moment the first event lands, which on one member never happens -
     * the only processor there is the local one - so nothing short of a real cluster tells the two apart.
     */
    public static ProcessorMetaSupplier metaSupplier(String vertexName,
            SupplierEx<? extends SinkWriter> writerFactory) {
        Objects.requireNonNull(vertexName, "vertexName");
        Objects.requireNonNull(writerFactory, "writerFactory");
        SupplierEx<Processor> supplier = () -> new SinkProcessor(writerFactory.get(), null, null,
                DEFAULT_MAX_IN_FLIGHT, DEFAULT_MAX_BATCH_SIZE, FrontierGauge.none(), new JetDeliveryGauge());
        return ProcessorMetaSupplier.forceTotalParallelismOne(ProcessorSupplier.of(supplier), vertexName);
    }

    /**
     * A meta-supplier for a sink vertex that also advances a durable sink-ack watermark. The ack is carried
     * as a {@link SinkAckFactory}, not a prebuilt {@link SinkAck}: the durable store it writes is not
     * serializable, so only the factory travels on the DAG and the store is resolved on the member that runs
     * the vertex. The vertex is pinned to total parallelism one - on the member that owns {@code vertexName},
     * which every edge into it must route to with {@code allToOne} - and keeps a single write in flight, the
     * order-preserving contract every shape of frontier below it depends on.
     *
     * <p>{@code frontierFactory} settles which shape that is, and it is settled here rather than run-time:
     * how far a sink may say a chain has landed depends on whether what reaches it is one chain in order or
     * an assembly of several, and that is a property of the graph that was compiled, not of any event.
     */
    static ProcessorMetaSupplier metaSupplier(String vertexName,
            SupplierEx<? extends SinkWriter> writerFactory,
            SinkAckFactory sinkAckFactory, SupplierEx<SinkFrontier> frontierFactory) {
        Objects.requireNonNull(vertexName, "vertexName");
        Objects.requireNonNull(writerFactory, "writerFactory");
        Objects.requireNonNull(sinkAckFactory, "sinkAckFactory");
        Objects.requireNonNull(frontierFactory, "frontierFactory");
        return ProcessorMetaSupplier.forceTotalParallelismOne(
                new AckSinkSupplier(writerFactory, sinkAckFactory, frontierFactory), vertexName);
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
        this.countingSince = clock.getAsLong();
        HazelcastInstance instance = context.hazelcastInstance();
        this.failureRegistry = instance != null ? JobFailureRegistry.of(instance) : null;
        // A gauge that writes into a job's statistics can only do so from that job's own threads, and a
        // sink is also driven by hand - which is how its behaviour is pinned at all. Outside a job there
        // is nothing to write into and asking for a handle fails outright, taking the sink down with it,
        // so the readings go nowhere instead. The same absence already decides the failure registry
        // above, and for the same reason: neither exists until there is a job to hold it.
        if (instance == null && supplied.readableOnlyOnAJobThread()) {
            this.delivery = DeliveryGauge.none();
        }
    }

    @Override
    public void process(int ordinal, Inbox inbox) {
        reapSettled();
        while (!inbox.isEmpty() && inFlight.size() < maxInFlight) {
            List<Envelope> batch = new ArrayList<>();
            List<ChainEntry> absorbed = new ArrayList<>();
            int taken = 0;
            while (taken < maxBatchSize && !inbox.isEmpty()) {
                Object item = inbox.poll();
                taken++;
                // Word that a chain got past some changes with nothing to deliver for them. It is not a
                // record and is never offered to the writer, but it settles with this batch rather than on
                // arrival: it may only be acked once every record before it has landed, and riding the
                // batch is what makes that true without anything here having to work it out.
                if (item instanceof SettledPositions settled) {
                    settled.positions().forEach(
                            (chain, position) -> absorbed.add(new ChainEntry(chain, position)));
                    continue;
                }
                batch.add((Envelope) item);
            }
            List<ChainEntry> positions = new ArrayList<>(positionsOf(batch));
            positions.addAll(absorbed);
            // Tallied while the rows are still here and counted only once the write settles. Holding the
            // envelopes themselves until then would keep a batch's worth of rows alive for the length of a
            // write; this keeps one number per table and operation in it instead.
            inFlight.add(new InFlightBatch(settlementOf(batch), positions, DeliveredRows.of(batch)));
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
            // The newest bound on an axis subsumes any older one held for that axis, so only the newest of
            // each is kept. Across axes nothing subsumes anything.
            heldBounds.put(watermark.key(), watermark);
            releaseHeldBounds();
            reportTrailing();
        }
        return true;
    }

    /**
     * Hands every held bound to the frontier once nothing is in flight. That is the moment every event they
     * cover is durable: the engine delivers a bound only after the events beneath it, and this processor
     * takes those straight from the inbox into batches, so once no batch is in flight none of them is
     * unwritten.
     *
     * <p>All of them, not the newest: they are bounds on different chains, and a chain whose bound was
     * dropped here has no second one coming while it stays quiet.
     */
    private void releaseHeldBounds() {
        if (heldBounds.isEmpty() || !inFlight.isEmpty()) {
            return;
        }
        for (Watermark bound : heldBounds.values()) {
            frontier.bound(bound, sinkAck);
        }
        heldBounds.clear();
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
            // Counted here and nowhere earlier: this is the first line after the write is known to have
            // succeeded, which is the boundary the count is defined at. A row counted on hand-off would be
            // counted again when a failed write was retried, and would already have been counted for a
            // write that never succeeded at all.
            batch.delivered().foldInto(deliveredByTableAndOp, settledBytes, newestSettledEventTime);
            // Measured against the clock at this line and no earlier one: a row's delivery is how long it
            // was from the source's stamp until its write was known to have succeeded, and the wait in
            // this processor's queue and in flight at the target is part of that, not noise around it.
            batch.delivered().foldDurationsInto(settledDurations, clock.getAsLong());
            reportDelivered();
            return true;
        });
        if (frontier != null) {
            releaseHeldBounds();
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
        gauge.pinned(frontier.stalls());
    }

    /** Hands out what has settled so far, both readings from the one set of totals this processor keeps. */
    private void reportDelivered() {
        if (deliveredByTableAndOp.isEmpty()) {
            return;
        }
        delivery.delivered(deliveredByTableAndOp);
        delivery.carried(settledBytes);
        delivery.reached(newestSettledEventTime);
        Map<String, HistogramValue> durations = new LinkedHashMap<>();
        settledDurations.forEach((table, totals) -> durations.put(table, totals.value()));
        delivery.took(durations);
        // Published with them and never alone: a total is readable only against what it accumulates from,
        // and the two arriving by different routes is how they come to disagree.
        delivery.countingSince(countingSince);
    }

    /** What this batch contributes to the frontier, empty when no frontier is tracked. */
    private List<ChainEntry> positionsOf(List<Envelope> batch) {
        return frontier == null ? List.of() : frontier.positions(batch);
    }

    /**
     * The write that settles this batch, or an already-settled one where the batch holds no record. A drain
     * of nothing but words about chains has nothing to deliver, and handing the writer an empty list would
     * put an empty call on an external target on every one of them - on a pointed-at stream nobody names,
     * that is every drain of it.
     */
    private CompletableFuture<WriteResult> settlementOf(List<Envelope> batch) {
        return batch.isEmpty()
                ? CompletableFuture.completedFuture(new WriteResult(0))
                : writer.write(batch).toCompletableFuture();
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

    /** One outstanding write, what its batch contributes to the frontier, and what it delivers. */
    private record InFlightBatch(CompletableFuture<WriteResult> future, List<ChainEntry> positions,
            DeliveredRows delivered) {
    }

    /**
     * What one batch would add to the delivery totals once it settles: how many rows of each table and
     * operation it holds, and the newest event time among them per table. Taken when the batch is formed
     * and applied when the write succeeds, so nothing here depends on the envelopes still being reachable.
     */
    private record DeliveredRows(Map<String, Map<String, Long>> rows, Map<String, Long> bytes,
            Map<String, Long> newestEventTime, Map<String, List<Long>> eventTimes) {

        static DeliveredRows of(List<Envelope> batch) {
            Map<String, Map<String, Long>> rows = new LinkedHashMap<>();
            Map<String, Long> bytes = new LinkedHashMap<>();
            Map<String, Long> newest = new LinkedHashMap<>();
            Map<String, List<Long>> stamps = new LinkedHashMap<>();
            for (Envelope event : batch) {
                rows.computeIfAbsent(event.src(), table -> new LinkedHashMap<>())
                        .merge(event.op().symbol(), 1L, Long::sum);
                // Weighed while the batch still holds the envelopes, alongside the count, so that what
                // settles later adds a figure taken from the rows themselves rather than from whatever
                // is still reachable by then.
                bytes.merge(event.src(), PayloadBytes.of(event), Long::sum);
                newest.merge(event.src(), event.ts(), Math::max);
                // Every row's stamp, not only the newest: a delivery time is measured per row when the
                // write settles, and a batch that kept only its newest stamp would report the whole batch
                // as fast as its freshest row.
                stamps.computeIfAbsent(event.src(), table -> new ArrayList<>()).add(event.ts());
            }
            return new DeliveredRows(rows, bytes, newest, stamps);
        }

        void foldInto(Map<String, Map<String, Long>> totals, Map<String, Long> bytesByTable,
                Map<String, Long> newestByTable) {
            rows.forEach((table, byOp) -> byOp.forEach((op, count) ->
                    totals.computeIfAbsent(table, ignored -> new LinkedHashMap<>())
                            .merge(op, count, Long::sum)));
            bytes.forEach((table, size) -> bytesByTable.merge(table, size, Long::sum));
            newestEventTime.forEach((table, ts) -> newestByTable.merge(table, ts, Math::max));
        }

        /** Buckets every row's age at {@code settledAtMillis} into its table's running distribution. */
        void foldDurationsInto(Map<String, DurationTotals> totals, long settledAtMillis) {
            eventTimes.forEach((table, stamps) -> {
                DurationTotals running = totals.computeIfAbsent(table, ignored -> new DurationTotals());
                for (long stamp : stamps) {
                    running.add(settledAtMillis - stamp);
                }
            });
        }
    }

    /**
     * One table's running distribution of delivery times: how many rows, their total in milliseconds, and
     * one count per registered bucket. Milliseconds are kept as a whole number so the sum never drifts by
     * rounding; the value handed out converts once.
     */
    private static final class DurationTotals {

        private static final HistogramBounds BOUNDS = HistogramBounds.RECORD_DELIVERY_DURATION;

        private long count;
        private long sumMillis;
        private final long[] buckets = new long[BOUNDS.buckets()];

        /**
         * Counts one row that took {@code millis}. A source clock ahead of ours reads as nought rather than
         * as a negative time: an age below zero is not a state a delivery can be in, and it would land in
         * no bucket at all.
         */
        void add(long millis) {
            long age = Math.max(0L, millis);
            count++;
            sumMillis += age;
            buckets[bucketOf(age / 1000.0)]++;
        }

        /** The first bucket whose upper bound the value does not exceed; the last bucket for everything above. */
        private static int bucketOf(double seconds) {
            List<Double> bounds = BOUNDS.bounds();
            for (int index = 0; index < bounds.size(); index++) {
                if (seconds <= bounds.get(index)) {
                    return index;
                }
            }
            return bounds.size();
        }

        HistogramValue value() {
            List<Long> counts = new ArrayList<>(buckets.length);
            for (long bucket : buckets) {
                counts.add(bucket);
            }
            return BOUNDS.value(count, sumMillis / 1000.0, counts);
        }
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
                        DEFAULT_MAX_IN_FLIGHT, DEFAULT_MAX_BATCH_SIZE, new JetFrontierGauge(),
                        new JetDeliveryGauge()));
            }
            return processors;
        }
    }
}
