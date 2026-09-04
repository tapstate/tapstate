package io.tapstate.runtime.srs;

import com.hazelcast.function.SupplierEx;
import com.hazelcast.jet.core.AbstractProcessor;
import com.hazelcast.jet.core.Processor;
import com.hazelcast.jet.core.ProcessorMetaSupplier;
import com.hazelcast.jet.core.ProcessorSupplier;
import com.hazelcast.jet.core.Watermark;
import com.hazelcast.ringbuffer.Ringbuffer;
import io.tapstate.core.event.ChainPosition;
import io.tapstate.core.event.Envelope;
import io.tapstate.core.event.SourceOrder;
import java.util.ArrayDeque;
import java.util.Objects;

/**
 * The core-API self-built source over one per-table change ring: a Jet processor with no inbound edge that
 * emits this ring's buffered snapshot rows first, then tails the ring in sequence order emitting each change
 * already projected to the {@link Envelope} currency, the source vertex the engine's DAG builder wires. It is
 * the core-API sibling of {@link SrsRingSource} (the pipeline-API stream source over raw items): the DAG
 * builder speaks in processor suppliers, and the source position must enter the envelope at the source, so
 * the projection lives here rather than in a later stage.
 *
 * <p>Snapshot rows and cdc changes flow through this one ordered source: the rows a member-side
 * {@link SnapshotBuffer} holds for this ring are emitted ahead of any cdc change off the ring, so the older
 * snapshot value can never land at the sink after a newer change of the same key. A member with no buffer
 * bound, or a ring with none buffered, is a pure ring tail.
 *
 * <p><strong>The buffer is looked at on every pass, not once at init.</strong> It is not only a snapshot
 * seed: a tail running with the shared ring switched off has no ring anyone fills, so every change it
 * captures reaches the sink through here, for as long as the pipeline runs. Draining once delivers whatever
 * happened to be buffered when the job started and strands the rest in a member-local queue nothing reads
 * again -- with the job still running, nothing thrown, and the tail reporting healthy.
 *
 * <p>Non-cooperative, exactly as Jet's own SourceBuilder-built source is: it runs on its own thread and backs
 * off between empty fills, so an idle ring never spins a shared cooperative thread. It is not fault-tolerant -
 * it keeps no snapshot and its read position never enters Jet state; on an L1 restart the ring is re-mined and
 * replayed from the durable source offset. The ring and the read-cursor sink are resolved on the member the
 * processor runs on, so nothing but serializable coordinates crosses the wire.
 */
public final class SrsSourceProcessor extends AbstractProcessor {

    /** The most changes one fill drains before yielding - a bounded batch that lets Jet pace the source. */
    private static final int FILL_BATCH = 256;

    private final String ringName;
    private final String src;
    private final StartFrom start;
    private final long epoch;
    private final SrsReadCursorPublisherFactory publisherFactory;
    private final SourceBoundStamp stamp;
    private final ArrayDeque<Envelope> pending = new ArrayDeque<>();
    private SnapshotBuffer buffered;
    private SrsRingReader reader;
    private SourceOrder read;
    private Watermark unannounced;
    private long announced;
    // Whether anything has been announced at all, kept apart from the value rather than encoded in it. The
    // lowest long is a real bound - it is what the reserved snapshot position packs to under some stampings
    // - and a sentinel that a real value can equal is a first bound silently swallowed.
    private boolean announcedAny;
    // Whether this source still owes the bound covering the snapshot rows it was seeded with.
    private boolean snapshotBoundDue;

    private SrsSourceProcessor(String ringName, String src, StartFrom start, long epoch,
            SrsReadCursorPublisherFactory publisherFactory, SourceBoundStamp stamp) {
        this.ringName = ringName;
        this.src = src;
        this.start = start;
        this.epoch = epoch;
        this.publisherFactory = publisherFactory;
        this.stamp = stamp;
    }

    @Override
    protected void init(Context context) {
        // Take what the capture side has already buffered for this ring before opening the ring reader, so
        // the source emits every snapshot row (op r, no source position) ahead of the first cdc change -- the
        // ordering that keeps a stale snapshot from landing at the sink after a newer change of the same key.
        // A member with no buffer bound, or a ring with none buffered, takes nothing here and is a pure ring
        // tail. Rows are preserved as-is: a null source position is what the sink-ack watermark skips.
        // This is a streaming source: it assumes a cdc tail follows the snapshot. A snapshot-only read (no
        // tail, a bounded source that emits the buffer then completes rather than tailing an empty ring) is a
        // later increment; it is not driven through this vertex yet.
        //
        // The reference is kept, not just read: the buffer is looked at again on every pass, because a tail
        // with the shared ring switched off never stops appending to it.
        Object bound = context.hazelcastInstance().getUserContext().get(SnapshotBuffer.USER_CONTEXT_KEY);
        buffered = bound instanceof SnapshotBuffer resolved ? resolved : null;
        drainBuffered();
        Ringbuffer<SrsItem> rb = context.hazelcastInstance().getRingbuffer(ringName);
        SrsRingbuffer ring = new SrsRingbuffer(rb);
        reader = SrsRingReader.from(ring, start, publisherFactory.resolve(context.hazelcastInstance()));
    }

    @Override
    public boolean isCooperative() {
        return false;
    }

    @Override
    public boolean complete() {
        // Emit anything held back by earlier backpressure before reading more: the cursor only advances as
        // fill reads, so a change the outbox refused stays buffered until it is taken, never re-read nor lost.
        // A bound the outbox refused is offered again the same way, before anything more is read: it was
        // already recorded as announced, so it has to leave rather than be worked out afresh.
        if (!emitPending() || !announce()) {
            return false;
        }
        // Every seeded snapshot row has left, so the bound covering them can be promised now. Waiting for a
        // change to promise it with is waiting for something that does not exist while a load runs: every
        // row of one snapshot carries the same reserved position, so downstream never sees a higher position
        // of this table settle, and a sink with no bound to close them on never records the table as
        // written -- which is a table read again from the start on every resume until the load ends.
        if (snapshotBoundDue) {
            snapshotBoundDue = false;
            stampAt(SourceOrder.snapshotRow(epoch));
            if (!announce()) {
                return false;
            }
        }
        // Whatever the capture has handed over since the last pass, ahead of the ring as always.
        drainBuffered();
        // The ring's sequence pairs with the generation this reader runs under to give each change its
        // order. The sequence alone is not comparable across generations: a rebuilt ring numbers from zero
        // again, so a change of the new ring would otherwise read as older than one of the ring before it.
        reader.fill((item, seq) -> {
            SourceOrder order = orderOf(seq);
            pending.add(SrsProjection.toEnvelope(item, src, order));
            read = order;
        }, FILL_BATCH);
        if (emitPending()) {
            stampWhatHasLeft();
            announce();
        }
        // A streaming source never completes: on an empty ring it returns having emitted nothing and, being
        // non-cooperative, its worker backs off before the next call rather than spinning.
        return false;
    }

    /**
     * Takes whatever the capture side has buffered for this ring since the last look, in buffered order.
     *
     * <p>What a drained item counts as depends on the position it carries, and the two cases are not
     * interchangeable. A change carrying an order of its own counts as read, exactly as one off the ring
     * does, so the bound this source promises comes to cover it -- without that, a tail running with the
     * ring switched off emits changes no bound ever reaches, no sink ever confirms, and nothing ever
     * records, which is the same silence seen one layer down. A snapshot row does not: every row of one
     * snapshot carries the same reserved position, so the bound closing them is promised on its own once
     * they have all left.
     */
    private void drainBuffered() {
        if (buffered == null) {
            return;
        }
        for (Envelope row : buffered.drain(ringName)) {
            pending.add(row);
            ChainPosition at = row.position();
            if (at == null || at.order() == null || at.order().seq() == SourceOrder.SNAPSHOT_SEQ) {
                // Rows to emit means a bound to promise once they have left. A source that took none owes
                // nothing: there is no snapshot of this table in this run for a sink to be waiting on.
                snapshotBoundDue = true;
            } else {
                read = at.order();
            }
        }
    }

    /**
     * The order for the change at ring sequence {@code seq}.
     *
     * <p>A source reading no chain of its own carries no generation, and for it this is unreachable: no
     * chain means no capture writing that ring, so the reader only ever finds it empty and every row this
     * source emits comes from the snapshot buffer instead. Reaching here without one therefore means a ring
     * is being filled for a chain nobody opened — a wiring error, and one that would otherwise put every
     * change of this job on a generation below every real one, so it crashes bare rather than ordering on it.
     */
    private SourceOrder orderOf(long seq) {
        if (epoch < 1) {
            throw new IllegalStateException("ring '" + ringName + "' holds changes on stream '" + src
                    + "' but its mining chain has no ring generation open");
        }
        return new SourceOrder(epoch, seq);
    }

    /**
     * Works out the bound standing for everything this source has read, once all of it has left. Only what
     * came off the ring counts: the buffered snapshot rows of a generation all sit beneath its first change,
     * so the first bound covers them, and until a change is read there is nothing to promise - a frontier
     * that has not started rather than one that has run ahead.
     *
     * <p>A bound that does not climb is not sent: the engine treats a repeated bound as a torn contract and
     * fails the job, so silence is what "nothing new was read" looks like. An idle source therefore says
     * nothing at all, and the levels behind it keep whatever they last promised.
     */
    private void stampWhatHasLeft() {
        if (read == null) {
            return;
        }
        stampAt(read);
    }

    /**
     * Holds the bound standing for {@code covered} to be offered, unless it is no advance on what has
     * already been announced. A source with no stamp is one whose job wires no bounds at all.
     */
    private void stampAt(SourceOrder covered) {
        if (stamp == null) {
            return;
        }
        Watermark bound = stamp.boundFor(covered);
        if (!announcedAny || bound.timestamp() > announced) {
            announced = bound.timestamp();
            announcedAny = true;
            unannounced = bound;
        }
    }

    /**
     * Offers the bound worked out but not yet taken; true when none is held back. The value is kept rather
     * than worked out again, because a second pass would find it no advance on what was already recorded as
     * announced and would say nothing at all.
     */
    private boolean announce() {
        if (unannounced == null) {
            return true;
        }
        if (!tryEmit(unannounced)) {
            return false;
        }
        unannounced = null;
        return true;
    }

    /** Emits buffered envelopes until the outbox refuses one; true when the buffer is fully drained. */
    private boolean emitPending() {
        while (!pending.isEmpty()) {
            if (!tryEmit(pending.peek())) {
                return false;
            }
            pending.remove();
        }
        return true;
    }

    /**
     * A meta-supplier for a source vertex tailing {@code ringName} from {@code start}, tagging every change
     * with the logical stream name {@code src} and the generation {@code epoch} the ring was opened under,
     * and reporting its read cursor through {@code publisherFactory}. The vertex is pinned to total
     * parallelism one: one reader per ring keeps the change stream in order.
     *
     * <p>The generation is resolved when the job is assembled, not read per change: the ring is opened
     * before the job is submitted and does not change generation while it runs, so carrying it here keeps
     * the durable store off the per-change path entirely. Zero means the source reads no chain of its own —
     * a snapshot-only or srs-disabled read, whose rows come from the snapshot buffer and whose ring nobody
     * fills; a change found on such a ring is rejected rather than ordered.
     */
    public static ProcessorMetaSupplier metaSupplier(String ringName, String src, StartFrom start, long epoch,
            SrsReadCursorPublisherFactory publisherFactory) {
        return metaSupplier(ringName, src, start, epoch, publisherFactory, null);
    }

    /**
     * The same source vertex, announcing how far the frontier may go as it reads. {@code stamp} encodes a
     * read position as the bound the rest of the job combines; a null one announces nothing at all, which
     * is the frontier standing still rather than running ahead.
     */
    public static ProcessorMetaSupplier metaSupplier(String ringName, String src, StartFrom start, long epoch,
            SrsReadCursorPublisherFactory publisherFactory, SourceBoundStamp stamp) {
        Objects.requireNonNull(ringName, "ringName");
        Objects.requireNonNull(src, "src");
        Objects.requireNonNull(start, "start");
        Objects.requireNonNull(publisherFactory, "publisherFactory");
        if (epoch < 0) {
            throw new IllegalArgumentException("a ring generation is never negative, got " + epoch);
        }
        SupplierEx<Processor> supplier =
                () -> new SrsSourceProcessor(ringName, src, start, epoch, publisherFactory, stamp);
        return ProcessorMetaSupplier.forceTotalParallelismOne(ProcessorSupplier.of(supplier));
    }
}
