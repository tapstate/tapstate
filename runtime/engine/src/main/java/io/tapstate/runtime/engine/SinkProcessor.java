package io.tapstate.runtime.engine;

import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.function.SupplierEx;
import com.hazelcast.jet.core.AbstractProcessor;
import com.hazelcast.jet.core.Inbox;
import com.hazelcast.jet.core.Processor;
import com.hazelcast.jet.core.ProcessorMetaSupplier;
import com.hazelcast.jet.core.ProcessorSupplier;
import com.hazelcast.jet.core.Watermark;
import io.tapstate.core.event.ChainPosition;
import io.tapstate.core.event.Envelope;
import io.tapstate.core.event.PayloadBytes;
import io.tapstate.core.lifecycle.HistogramBounds;
import io.tapstate.core.lifecycle.HistogramValue;
import io.tapstate.core.lifecycle.Stage;
import io.tapstate.core.lifecycle.Staged;
import io.tapstate.runtime.engine.SinkFrontier.ChainEntry;
import io.tapstate.spi.sink.SinkWriter;
import io.tapstate.spi.sink.WriteResult;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
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
 * <p><b>A batch holds one stream's rows, and closes on count or on time.</b> Rows taken from the inbox wait in
 * their stream's queue, at most one batch's worth across all of them, and a stream's rows go to the writer
 * together once they are due: enough of them for a full batch, or the oldest having waited as long as a batch
 * waits (nothing, by default), or no room left to take in more, or the input over. The streams take turns, so a
 * busy one does not keep a quiet one waiting for more than a batch. One stream per batch is what a writer
 * appending rows needs - it cannot write two tables in one call and take neither back when the second fails -
 * and what every writer's calls are then made of.
 *
 * <p><b>Not everything that arrives is a record.</b> A vertex upstream may also send word that a chain got
 * past changes it has nothing to deliver for — absorbed where they arrived, with no record coming for them
 * ever. Those are never offered to the writer, but they take part in the frontier, and they take part only
 * once every row taken in before them has landed: a position may only be acked once every record carrying a
 * lower one has. A bound waits the same way. Each row is numbered as it is taken in, and each word and bound
 * waits for every row numbered before it - never for the rows after it, so a stream of rows that never goes
 * quiet does not hold the frontier back for good, and never cutting a batch short, so the wait a batch was
 * given is the wait it gets.
 *
 * <p>Each processor keeps a single write in flight by default: applying one batch to completion before the
 * next is issued is what keeps a key's change events in their arrival order - on the one processor a vertex
 * running at total parallelism one has, and on the one writer a key is routed to where the vertex runs
 * several. Two batches that straddle a key
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
    // The vertex this processor writes for, and whether that vertex runs as one processor for the cluster;
    // together with the processor's own index they name it as a writer. Null where it was built by hand
    // outside a graph, which reports through the ack as it stands.
    private final String writerOf;
    private final boolean totalOne;
    // The ack this processor reports through: the one supplied, bound to this processor as a writer once
    // init knows its index. See init.
    private SinkAck ack;
    private final SinkFrontier frontier;
    private final FrontierGauge suppliedGauge;
    // Which of the two the frontier readings go to, settled at init for the reason the delivery readings'
    // is: the one supplied, or one that reads nothing when this processor is not running inside a job.
    private FrontierGauge gauge;
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
    // Times each batch this sink forms and issues, which is this stage's unit of work.
    private StageTimer timer = StageTimer.none(Stage.SINK);
    private final int maxInFlight;
    private final int maxBatchSize;
    private final List<InFlightBatch> inFlight = new ArrayList<>();
    // Bounds held until every row taken in before them has landed, per axis, oldest first. A bound proves
    // what is still coming, never what is durable: every event it covers has been taken in by the time it
    // arrives, but the ones queued or in an unsettled batch are not written yet. Handing it to the frontier
    // then would let one settled batch of a fan-out stand for the whole of what its change produced.
    //
    // Per axis rather than one in total, because a bound names the chain it is for: one chain's promise is
    // not a newer version of another's, and letting whichever arrives second overwrite the first leaves the
    // overwritten chain waiting for a strictly higher position of its own to settle, which on a chain that
    // has gone quiet never comes -- so the position it was holding stays open for the life of the run, and on
    // a snapshot that is a table nothing records as loaded and every resume reads again in full. And oldest
    // first on an axis, each waiting for its own rows only: a newer bound that replaced an older one would
    // make it wait for the newer one's rows too, and on a chain whose rows never stop coming that is for good.
    private final Map<Byte, ArrayDeque<Held<Watermark>>> heldBounds = new LinkedHashMap<>();
    // Words that chains got past positions with nothing to deliver, each waiting, as a bound does, for every
    // row taken in before it to land; the highest position per chain among words waiting for the same rows.
    private final ArrayDeque<Held<Map<String, ChainPosition>>> heldWords = new ArrayDeque<>();
    // Rows taken in and not yet handed to the writer, per stream, in the order the streams get their turn: a
    // stream whose rows go leaves, and comes back at the end with its next row.
    private final Map<String, ArrayDeque<Queued>> queued = new LinkedHashMap<>();
    // When each queued stream's oldest row was taken in, by the clock the wait is measured against.
    private final Map<String, Long> waitingSince = new LinkedHashMap<>();
    private int queuedRows;
    // The number the next row taken in gets. A word or a bound taken in waits for every row numbered below it.
    private long nextOrder;
    // How long a stream's oldest queued row waits for more before its rows go, and the clock it is measured
    // against. Zero waits for nothing: rows go as soon as the writer can take them.
    private final long maxWaitNanos;
    private final LongSupplier nanoClock;
    // Set once the input is over: whatever is queued goes, however few rows and however recently they came.
    private boolean draining;
    // Combines the bounds that arrive edge by edge, each chain over the edges that carry it; null where the
    // engine's combination across every edge is taken instead. See tryProcessWatermark.
    private final LevelBounds edges;
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
        this(writer, sinkAck, frontier, maxInFlight, maxBatchSize, gauge, delivery, clock, null, true);
    }

    /**
     * A sink that reports as one writer of the vertex {@code writerOf}: the processor names itself by that
     * vertex and its own index once it knows the index, so where several writers land one pipeline's changes
     * each one's progress is kept apart and the slowest decides how far the pipeline has landed. A vertex
     * running as one processor for the cluster names its one writer by index zero whichever member it runs
     * on; the index the engine gives it depends on that member, and a writer that changed name with the
     * member it landed on would never be the one the run expects.
     */
    SinkProcessor(SinkWriter writer, SinkAck sinkAck, SinkFrontier frontier,
            int maxInFlight, int maxBatchSize, FrontierGauge gauge, DeliveryGauge delivery, LongSupplier clock,
            String writerOf, boolean totalOne) {
        this(writer, sinkAck, frontier, maxInFlight, maxBatchSize, gauge, delivery, clock, writerOf, totalOne,
                null);
    }

    /**
     * As above, combining the bounds it takes in edge by edge through {@code edges}, which knows which chains
     * each inbound edge carries: a chain is then held down only by the edges that carry it. Null leaves the
     * combining to the engine, which holds every chain down by every edge - right only where every edge
     * carries every chain.
     */
    SinkProcessor(SinkWriter writer, SinkAck sinkAck, SinkFrontier frontier,
            int maxInFlight, int maxBatchSize, FrontierGauge gauge, DeliveryGauge delivery, LongSupplier clock,
            String writerOf, boolean totalOne, LevelBounds edges) {
        this(writer, sinkAck, frontier, maxInFlight, maxBatchSize, gauge, delivery, clock, writerOf, totalOne, edges,
                0L, System::nanoTime);
    }

    /**
     * As above, holding a stream's rows for up to {@code maxWaitMillis} after its oldest was taken in, by
     * {@code nanoClock}, before they go with fewer than {@code maxBatchSize} of them.
     */
    SinkProcessor(SinkWriter writer, SinkAck sinkAck, SinkFrontier frontier,
            int maxInFlight, int maxBatchSize, FrontierGauge gauge, DeliveryGauge delivery, LongSupplier clock,
            String writerOf, boolean totalOne, LevelBounds edges, long maxWaitMillis, LongSupplier nanoClock) {
        if (maxWaitMillis < 0) {
            throw new IllegalArgumentException("maxWaitMillis must not be negative: " + maxWaitMillis);
        }
        this.maxWaitNanos = maxWaitMillis * 1_000_000L;
        this.nanoClock = Objects.requireNonNull(nanoClock, "nanoClock");
        this.edges = edges;
        this.writerOf = writerOf;
        this.totalOne = totalOne;
        this.writer = Objects.requireNonNull(writer, "writer");
        this.suppliedGauge = Objects.requireNonNull(gauge, "gauge");
        this.gauge = this.suppliedGauge;
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
        this.ack = sinkAck;
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
        return metaSupplier(vertexName, writerFactory, DEFAULT_MAX_BATCH_SIZE, 0L);
    }

    /**
     * As above, in batches of at most {@code maxRecords} rows of one stream, each going once full or once its
     * first row has waited {@code maxWaitMillis}.
     */
    static ProcessorMetaSupplier metaSupplier(String vertexName, SupplierEx<? extends SinkWriter> writerFactory,
            int maxRecords, long maxWaitMillis) {
        Objects.requireNonNull(vertexName, "vertexName");
        Objects.requireNonNull(writerFactory, "writerFactory");
        SupplierEx<Processor> supplier = () -> new SinkProcessor(writerFactory.get(), null, null,
                DEFAULT_MAX_IN_FLIGHT, maxRecords, FrontierGauge.none(), new JetDeliveryGauge(),
                System::currentTimeMillis, null, true, null, maxWaitMillis, System::nanoTime);
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
     * an assembly of several, and that is a property of the graph that was compiled, not of any event. The
     * bounds are combined by the engine, across every inbound edge.
     */
    static ProcessorMetaSupplier metaSupplier(String vertexName,
            SupplierEx<? extends SinkWriter> writerFactory,
            SinkAckFactory sinkAckFactory, SupplierEx<SinkFrontier> frontierFactory) {
        return metaSupplier(vertexName, writerFactory, sinkAckFactory, frontierFactory, null);
    }

    /**
     * As above, with the bounds combined edge by edge by what {@code edgesFactory} makes - over the chains
     * each inbound edge carries - where it is not null, and by the engine where it is.
     */
    static ProcessorMetaSupplier metaSupplier(String vertexName,
            SupplierEx<? extends SinkWriter> writerFactory,
            SinkAckFactory sinkAckFactory, SupplierEx<SinkFrontier> frontierFactory,
            SupplierEx<LevelBounds> edgesFactory) {
        return metaSupplier(vertexName, writerFactory, sinkAckFactory, frontierFactory, edgesFactory,
                DEFAULT_MAX_BATCH_SIZE, 0L);
    }

    /** As above, batching as {@code maxRecords} and {@code maxWaitMillis} say. */
    static ProcessorMetaSupplier metaSupplier(String vertexName,
            SupplierEx<? extends SinkWriter> writerFactory,
            SinkAckFactory sinkAckFactory, SupplierEx<SinkFrontier> frontierFactory,
            SupplierEx<LevelBounds> edgesFactory, int maxRecords, long maxWaitMillis) {
        Objects.requireNonNull(vertexName, "vertexName");
        Objects.requireNonNull(writerFactory, "writerFactory");
        Objects.requireNonNull(sinkAckFactory, "sinkAckFactory");
        Objects.requireNonNull(frontierFactory, "frontierFactory");
        return ProcessorMetaSupplier.forceTotalParallelismOne(
                new AckSinkSupplier(writerFactory, sinkAckFactory, frontierFactory, edgesFactory, vertexName, true,
                        maxRecords, maxWaitMillis),
                vertexName);
    }

    /**
     * A meta-supplier for a sink vertex that runs the same number of writers on every member, each a writer
     * of its own: it lands whatever rows reach it and reports as the writer at its own index among all of the
     * vertex's processors, so the run can wait on each of them. The vertex is as wide on every member as the
     * builder sets it, and refuses an execution that starts on a member count other than the one its width
     * was worked out for - a run on more members would have writers nobody waits on.
     *
     * <p>Rows reach a writer over more than one queue - from every instance of the router in front of it, and
     * over two edges - so a later position can land before an earlier one: {@code frontierFactory} is the
     * shape that goes by bounds, and {@code edgesFactory} combines them over the edges that carry each chain.
     * Without an ack factory the writers land rows and report nothing.
     */
    static ProcessorMetaSupplier nativeMetaSupplier(String vertexName,
            SupplierEx<? extends SinkWriter> writerFactory, SinkAckFactory sinkAckFactory,
            SupplierEx<SinkFrontier> frontierFactory, SupplierEx<LevelBounds> edgesFactory, int plannedMembers,
            int maxRecords, long maxWaitMillis) {
        Objects.requireNonNull(vertexName, "vertexName");
        Objects.requireNonNull(writerFactory, "writerFactory");
        ProcessorSupplier supplier = sinkAckFactory == null
                ? ProcessorSupplier.of((SupplierEx<Processor>) () -> new SinkProcessor(writerFactory.get(), null,
                        null, DEFAULT_MAX_IN_FLIGHT, maxRecords, FrontierGauge.none(), new JetDeliveryGauge(),
                        System::currentTimeMillis, null, false, null, maxWaitMillis, System::nanoTime))
                : new AckSinkSupplier(writerFactory, sinkAckFactory,
                        Objects.requireNonNull(frontierFactory, "frontierFactory"), edgesFactory, vertexName, false,
                        maxRecords, maxWaitMillis);
        return PlannedMembersGuard.of(ProcessorMetaSupplier.of(supplier), plannedMembers);
    }

    /**
     * Resolves this pipeline's id and the shared failure registry, both keyed off the running job. A
     * context with no Hazelcast instance (a bare unit test driving the processor directly, never through
     * a real job) leaves the registry unset; {@link #reapSettled} tolerates that and simply does not
     * record — a failure still fails the job exactly as before, only unrecorded.
     */
    @Override
    protected void init(Processor.Context context) {
        this.timer = StageTimer.of(stage(), context);
        this.pipelineId = context.jobConfig().getName();
        this.countingSince = clock.getAsLong();
        if (sinkAck != null && writerOf != null) {
            ack = sinkAck.forWriter(writerId(writerOf, totalOne ? 0 : context.globalProcessorIndex()));
        }
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
        if (instance == null && suppliedGauge.readableOnlyOnAJobThread()) {
            this.gauge = FrontierGauge.none();
        }
    }

    /** What this stage has timed so far, for a witness driving it by hand. */
    StageTimer timing() {
        return timer;
    }

    /**
     * The name of the writer at {@code index} of the sink vertex {@code vertex}: how a writer reports its
     * progress, and how a run names the writers it expects progress from. One spelling in one place, because
     * a writer and the run expecting it that spelled it differently would each wait on the other forever.
     */
    public static String writerId(String vertex, int index) {
        return vertex + "#" + index;
    }

    @Override
    public void process(int ordinal, Inbox inbox) {
        long started = timer.begin();
        try {
            processTimed(inbox);
        } finally {
            timer.end(started);
        }
    }

    private void processTimed(Inbox inbox) {
        reapSettled();
        // Taking in more is worth it only while writes are going out: with the queues full and nothing due,
        // the rest of the inbox waits where it is.
        do {
            takeIn(inbox);
        } while (writeWhatIsDue() && !inbox.isEmpty());
    }

    /**
     * Takes rows from the inbox into their stream's queue, numbering each, for as long as there is room: at most
     * one batch's worth waits here across every stream. The rest stays in the inbox, where the engine holds the
     * upstream back until this writer has taken more.
     *
     * <p>Word that a chain got past changes with nothing to deliver for them is not a row and never reaches the
     * writer. It waits for every row taken in before it to land, and is handed over then.
     */
    private void takeIn(Inbox inbox) {
        while (!inbox.isEmpty() && queuedRows < maxBatchSize) {
            Object item = inbox.poll();
            if (item instanceof SettledPositions settled) {
                if (frontier != null) {
                    holdWord(settled.positions());
                }
                continue;
            }
            Envelope row = (Envelope) item;
            ArrayDeque<Queued> queue = queued.get(row.src());
            if (queue == null) {
                queue = new ArrayDeque<>();
                queued.put(row.src(), queue);
                waitingSince.put(row.src(), nanoClock.getAsLong());
            }
            queue.add(new Queued(nextOrder++, row));
            queuedRows++;
        }
        if (frontier != null) {
            // A word taken in behind rows that have all landed already has nothing to wait for.
            releaseHeld();
        }
    }

    /** Holds {@code positions} until every row taken in before them has landed. */
    private void holdWord(Map<String, ChainPosition> positions) {
        Held<Map<String, ChainPosition>> last = heldWords.peekLast();
        if (last != null && last.before() == nextOrder) {
            // Waiting for the same rows as the word before it, so the two are handed over together.
            SettledPositions.fold(last.value(), positions);
            return;
        }
        Map<String, ChainPosition> highest = new LinkedHashMap<>();
        SettledPositions.fold(highest, positions);
        heldWords.add(new Held<>(nextOrder, highest));
    }

    /**
     * Hands the writer the next stream's rows for as long as it can take another write: one stream at a time,
     * taking turns, and only a stream whose rows are due. Answers whether it handed over any.
     */
    private boolean writeWhatIsDue() {
        long now = nanoClock.getAsLong();
        boolean wrote = false;
        while (inFlight.size() < maxInFlight) {
            String stream = nextDue(now);
            if (stream == null) {
                return wrote;
            }
            wrote = true;
            ArrayDeque<Queued> queue = queued.remove(stream);
            waitingSince.remove(stream);
            List<Envelope> batch = new ArrayList<>(queue.size());
            long firstOrder = queue.peek().order();
            for (Queued row : queue) {
                batch.add(row.row());
            }
            queuedRows -= batch.size();
            // Tallied while the rows are still here and counted only once the write settles. Holding the
            // envelopes themselves until then would keep a batch's worth of rows alive for the length of a
            // write; this keeps one number per table and operation in it instead.
            inFlight.add(new InFlightBatch(settlementOf(batch), positionsOf(batch), DeliveredRows.of(batch),
                    firstOrder));
        }
        // A saturated in-flight set leaves the queues as they are, and once they are full the inbox too; Jet
        // backpressures upstream until reapSettled frees a slot on a later call.
        return wrote;
    }

    /**
     * The first stream, in turn, whose rows are due: a full batch of them, or the oldest having waited as long
     * as a batch waits - no time at all, by default - or no room left to take in more, or the input over.
     */
    private String nextDue(long now) {
        boolean full = queuedRows >= maxBatchSize;
        for (Map.Entry<String, ArrayDeque<Queued>> stream : queued.entrySet()) {
            if (full || draining || maxWaitNanos == 0
                    || now - waitingSince.get(stream.getKey()) >= maxWaitNanos) {
                return stream.getKey();
            }
        }
        return null;
    }

    /**
     * Writes out everything still queued, however few rows and however recently they came, and reports done
     * once every write has settled. A write that settles at once lets the next go in the same call.
     */
    @Override
    public boolean complete() {
        draining = true;
        do {
            reapSettled();
        } while (writeWhatIsDue());
        return inFlight.isEmpty() && queued.isEmpty();
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
        // And hands over what has waited as long as a batch waits: nothing else would, while the inbox is idle.
        writeWhatIsDue();
        return true;
    }

    /**
     * Takes in a bound that arrived with no edge attached to it, and never passes it on. The engine calls
     * this with the value combined across every input queue, which is the guarantee the frontier rests on
     * - a queue that has said nothing still holds it down - wherever every edge carries every chain.
     *
     * <p>Where an edge carries only some of the chains, that same combination never arrives at all for a
     * chain another edge does not carry, since that edge never says anything about it. A sink compiled with
     * the chains each edge carries therefore takes its bounds edge by edge instead, below, and passes over
     * this one.
     *
     * <p>Nothing is emitted from here. This is the end of the line for a bound, and anything emitted would
     * be offered to the target as a record. The engine forwards it by default, silently, which is why
     * saying otherwise is explicit.
     */
    @Override
    public boolean tryProcessWatermark(Watermark watermark) {
        if (frontier != null && edges == null) {
            take(watermark);
        }
        return true;
    }

    /** Holds {@code bound} until every row taken in before it has landed, then hands it over; see releaseHeld. */
    private void take(Watermark bound) {
        ArrayDeque<Held<Watermark>> held = heldBounds.computeIfAbsent(bound.key(), axis -> new ArrayDeque<>());
        // The newer bound on an axis subsumes an older one waiting for the same rows. One waiting for fewer
        // rows is kept: it goes as soon as those have landed, rather than waiting on rows taken in after it.
        if (!held.isEmpty() && held.peekLast().before() == nextOrder) {
            held.pollLast();
        }
        held.add(new Held<>(nextOrder, bound));
        releaseHeld();
        reportTrailing();
    }

    /**
     * Hands the frontier every held word and bound whose rows have landed. The engine delivers a bound only
     * after the events beneath it, and a word travels behind the records it must not overtake, so each covers
     * exactly the rows taken in before it; once every one of those has landed, so has everything it speaks for.
     *
     * <p>Words go before bounds. For bounds, every axis with one due, and the newest due on each: they are
     * bounds on different chains, and a chain whose bound was dropped here has no second one coming while it
     * stays quiet.
     */
    private void releaseHeld() {
        long landedBelow = firstUnlanded();
        while (!heldWords.isEmpty() && heldWords.peek().before() <= landedBelow) {
            List<ChainEntry> entries = new ArrayList<>();
            heldWords.poll().value().forEach((chain, position) -> entries.add(new ChainEntry(chain, position)));
            frontier.settled(entries, ack);
        }
        for (ArrayDeque<Held<Watermark>> held : heldBounds.values()) {
            Watermark due = null;
            while (!held.isEmpty() && held.peek().before() <= landedBelow) {
                due = held.poll().value();
            }
            if (due == null) {
                continue;
            }
            frontier.bound(due, ack);
            // Every change this writer was given at or below the bound has landed, whatever the frontier made
            // of it: a writer given none of a chain's rows says so here and holds no chain back by having
            // nothing to write for it.
            String chain = frontier.chainOf(due);
            if (chain != null) {
                ack.bounded(chain, FrontierOrders.unpack(due.timestamp()));
            }
        }
    }

    /** The number of the oldest row not yet landed - queued or being written - or the next number if none is. */
    private long firstUnlanded() {
        long first = nextOrder;
        for (ArrayDeque<Queued> queue : queued.values()) {
            first = Math.min(first, queue.peek().order());
        }
        for (InFlightBatch batch : inFlight) {
            first = Math.min(first, batch.firstOrder());
        }
        return first;
    }

    /**
     * Takes in a bound that arrived on one edge, already combined across the queues of that edge. A sink
     * compiled with the chains each edge carries combines it here with what the other edges carrying the
     * same chain have promised, and takes the result once every one of them has spoken - the per-edge
     * counterpart of what the engine does across every edge, without waiting on an edge that never carries
     * the chain. Any other sink takes no notice: answering true is what lets the engine combine it and
     * deliver the combined value to the variant above.
     */
    @Override
    public boolean tryProcessWatermark(int ordinal, Watermark watermark) {
        if (frontier != null && edges != null) {
            edges.observe(ordinal, watermark.key(), watermark.timestamp())
                    .ifPresent(combined -> take(new Watermark(combined, watermark.key())));
        }
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
                frontier.settled(batch.positions(), ack);
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
            reportDelivered(batch.delivered().tables());
            return true;
        });
        if (frontier != null) {
            releaseHeld();
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

    /**
     * Hands out what has settled so far, every reading from the one set of totals this processor keeps.
     *
     * <p>{@code touched} names the tables the batch that just settled held rows of, and the distributions
     * handed over are theirs alone. Every other table's is the one already published and has not moved;
     * assembling all of them would build a boxed list per table on every settle — fifty of them for a
     * batch that touched one, a few hundred times a second.
     */
    private void reportDelivered(Set<String> touched) {
        if (deliveredByTableAndOp.isEmpty()) {
            return;
        }
        delivery.delivered(deliveredByTableAndOp);
        delivery.carried(settledBytes);
        delivery.reached(newestSettledEventTime);
        Map<String, HistogramValue> durations = new LinkedHashMap<>();
        for (String table : touched) {
            DurationTotals totals = settledDurations.get(table);
            if (totals != null) {
                durations.put(table, totals.value());
            }
        }
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
     * The write that settles this batch. A batch always holds a row: words about chains wait apart from the
     * rows rather than riding in a batch, so an external target is never handed an empty call for one.
     */
    private CompletableFuture<WriteResult> settlementOf(List<Envelope> batch) {
        return writer.write(batch).toCompletableFuture();
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

    /**
     * One outstanding write, what its batch contributes to the frontier, what it delivers, and the number of its
     * first row - the lowest in it, since a stream's rows go in the order they were taken in.
     */
    private record InFlightBatch(CompletableFuture<WriteResult> future, List<ChainEntry> positions,
            DeliveredRows delivered, long firstOrder) {
    }

    /** A row taken in and not yet handed to the writer, with the number it was taken in under. */
    private record Queued(long order, Envelope row) {
    }

    /** A word or a bound held until every row numbered below {@code before} has landed. */
    private record Held<T>(long before, T value) {
    }

    /**
     * What one batch would add to the delivery totals once it settles: how many rows of each table and
     * operation it holds, and the newest event time among them per table. Taken when the batch is formed
     * and applied when the write succeeds, so nothing here depends on the envelopes still being reachable.
     */
    private record DeliveredRows(Map<String, Map<String, Long>> rows, Map<String, Long> bytes,
            Map<String, Long> newestEventTime, Map<String, List<Long>> eventTimes) {

        /** The tables this batch settled rows of, which are the ones whose readings have moved. */
        Set<String> tables() {
            return rows.keySet();
        }

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
        private final SupplierEx<LevelBounds> edgesFactory;
        private final String vertexName;
        private final boolean totalOne;
        private final int maxRecords;
        private final long maxWaitMillis;
        private transient SinkAck sinkAck;

        AckSinkSupplier(SupplierEx<? extends SinkWriter> writerFactory,
                SinkAckFactory sinkAckFactory, SupplierEx<SinkFrontier> frontierFactory,
                SupplierEx<LevelBounds> edgesFactory, String vertexName, boolean totalOne,
                int maxRecords, long maxWaitMillis) {
            this.writerFactory = writerFactory;
            this.sinkAckFactory = sinkAckFactory;
            this.frontierFactory = frontierFactory;
            this.edgesFactory = edgesFactory;
            this.vertexName = vertexName;
            this.totalOne = totalOne;
            this.maxRecords = maxRecords;
            this.maxWaitMillis = maxWaitMillis;
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
                        DEFAULT_MAX_IN_FLIGHT, maxRecords, new JetFrontierGauge(),
                        new JetDeliveryGauge(), System::currentTimeMillis, vertexName, totalOne,
                        edgesFactory == null ? null : edgesFactory.get(), maxWaitMillis, System::nanoTime));
            }
            return processors;
        }
    }
}
