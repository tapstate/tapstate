package io.tapstate.runtime.srs;

import io.tapstate.core.event.Envelope;
import io.tapstate.core.common.TapstateException;
import io.tapstate.spi.capture.CaptureConfig;
import io.tapstate.spi.capture.CaptureListener;
import io.tapstate.spi.capture.CapturePort;
import io.tapstate.spi.capture.CaptureStart;
import io.tapstate.spi.capture.SourcePosition;
import io.tapstate.spi.capture.Subscription;
import io.tapstate.spi.store.ConsumerOffset;

import java.time.Duration;
import java.util.Collection;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.concurrent.CancellationException;
import java.util.concurrent.locks.LockSupport;
import java.util.function.Function;
import java.util.function.Supplier;

/**
 * The unbounded cdc phase of a capture. Starts the change stream and, for every change event (ops
 * {@code i} / {@code u} / {@code d} / {@code ddl}), projects it to a change-ring item and admits it
 * through the headroom gate into the per-table ring — the only hot buffer the SRS keeps. A snapshot read
 * (op {@code r}) never reaches here; the ring item rejects it by construction.
 *
 * <p>The per-event source position is threaded at this seam: the event envelope carries no position slot,
 * so each change is stamped with the position the source reported for it. Most changes carry none — a
 * source names one position for a run of changes — and each run is handed, with that position, to the
 * reader's account, which alone writes a position down once everyone the run was owed to has landed it.
 *
 * <p>Where a tail begins is the caller's to say, and it says it: {@link CaptureStart#present()} for a run
 * asked to take only new changes, a recorded position for one picking up where it left off.
 */
public final class CdcPhase {

    /**
     * The off-CPU pause between headroom re-checks while a cdc write is backpressured. Parking for a coarse
     * fixed interval, rather than busy-spinning, keeps a stalled write from burning a core; each wake re-reads
     * the slowest consumer's read cursor, so the write resumes within one interval of a consumer freeing a slot.
     * A signal-driven wake on true consumer advance is a later refinement.
     */
    private static final long BACKPRESSURE_PARK_NANOS = 1_000_000L;

    /**
     * The off-CPU pause between attempts while the cluster itself is refusing the write. Coarser than the
     * headroom pause because what clears it is not a consumer moving but a member's library recomputing
     * the cluster's verdict, which happens on the cluster's schedule -- measured in seconds.
     */
    private static final long REFUSAL_PARK_NANOS = 50_000_000L;

    /**
     * How long a run of cluster refusals is waited out before the capture stops with a coded failure.
     *
     * <p>The refusal this absorbs is the one that clears as members' verdicts converge, seconds after a
     * cluster forms; the bound is an order of magnitude past that, so a transient refusal is never turned
     * into a terminal one. What the bound keeps is the other half: a member being refused for good has to
     * say so rather than pause for ever, because a capture waiting in silence is indistinguishable from
     * one that is running, and a pipeline that reads as healthy while nothing moves is worse than one that
     * failed.
     *
     * <p>Its size is the workload claim's default lease: once the cluster has refused this member's writes
     * for as long as its work could legitimately have been handed to somebody else, waiting longer cannot
     * be the right answer. A deployment that shortens that lease only makes this bound generous -- a
     * capture that has genuinely lost its claim is stopped by the lease itself, on its own thread, and
     * this bound governs the other case: the claim still held, and the cluster still saying no.
     */
    static final long REFUSAL_BOUND_NANOS = Duration.ofSeconds(30).toNanos();

    private CdcPhase() {
    }

    /**
     * Starts the chain's reader: one connector subscription over every table it subscribes to, each change
     * routed to its table's ring, and the chain's durable positions moved only by {@code prefix} -- which
     * releases a run of changes once everyone it was owed to has landed it, and never on the strength of one
     * table's confirmation alone. The subscription it returns closes the account with the stream.
     *
     * <p>Where the stream began is handed to the account before anything else, and a run that carried no
     * change but named a position is recorded like any other: behind every run before it, it is what moves
     * a quiet chain.
     */
    static Subscription run(
            CapturePort port,
            CaptureConfig config,
            CaptureStart start,
            Map<String, TableRoute> routes,
            CaptureHealth health,
            PhysicalSourcePrefix prefix) {
        Objects.requireNonNull(port, "port");
        Objects.requireNonNull(config, "config");
        Objects.requireNonNull(start, "start");
        Objects.requireNonNull(routes, "routes");
        Objects.requireNonNull(health, "health");
        Objects.requireNonNull(prefix, "prefix");
        Map<String, TableRoute> routeSnapshot = Map.copyOf(routes);
        Subscription stream;
        try {
            stream = port.cdc(config, start, health.recording(new CaptureListener() {
                @Override
                public void onStart(Optional<SourcePosition> position) {
                    prefix.start(position);
                }

                @Override
                public void onBatch(List<Envelope> events, Optional<SourcePosition> position) {
                    writeReleased(events, position, routeSnapshot::get, prefix);
                }
            }));
        } catch (RuntimeException | Error failure) {
            prefix.close();
            throw failure;
        }
        return closingWith(stream, prefix);
    }

    /** {@code stream}, closing {@code prefix} with it and handing every acknowledgement straight through. */
    static Subscription closingWith(Subscription stream, PhysicalSourcePrefix prefix) {
        return new Subscription() {
            @Override
            public void acknowledge(SourcePosition durable) {
                stream.acknowledge(durable);
            }

            @Override
            public void close() {
                try {
                    stream.close();
                } finally {
                    prefix.close();
                }
            }
        };
    }

    /**
     * One table's wiring: its ring and the chain's consumer offsets, whose cursors in that ring bound how far
     * ahead of its slowest reader it may be written. What the chain may let go of, and how far each table's
     * durable log may be cut, is the reader's account to decide, not this route's.
     */
    public record TableRoute(CdcChain chain, Supplier<Collection<ConsumerOffset>> consumers) {

        public TableRoute {
            Objects.requireNonNull(chain, "chain");
            Objects.requireNonNull(consumers, "consumers");
        }
    }

    /**
     * The slowest subscribed consumer's read cursor into one table's ring — how far ahead of its readers
     * the ring may be written. {@link Long#MAX_VALUE} when no consumer subscribes to the table, and
     * {@code -1} for a subscribed consumer that has read nothing of it.
     *
     * <p>Derived here rather than fetched separately because it is a function of the same cursors the
     * durable frontier is: asking a store for it on its own means reading one record twice per run.
     */
    static long headroomBound(Collection<ConsumerOffset> offsets, String table) {
        return offsets.stream()
                .filter(offset -> offset.perTableSeq().containsKey(table))
                .mapToLong(offset -> offset.perTableSeq().get(table))
                .min()
                .orElse(Long.MAX_VALUE);
    }

    /** One table's admitted share: the sequence its last change took, and the cursors that let it in. */
    private record Admitted(long lastSeq, Collection<ConsumerOffset> offsets) {
    }

    /**
     * Writes one run of changes into the rings and hands it to the chain reader's account, which alone decides
     * when the position the source named for it may be written down.
     *
     * <p>The account is asked for room before anything is written, so a reader whose account is full holds
     * its source back instead of running ahead of what it can ever release. A run that carried no change but
     * named a position is recorded too, with nothing owed: a source that reports where a transaction ends only
     * after it has handed the transaction's changes over names that position on exactly such a run. One that
     * named nothing and carried nothing tells nobody anything, and is let go.
     */
    private static void writeReleased(
            List<Envelope> events,
            Optional<SourcePosition> position,
            Function<String, TableRoute> routes,
            PhysicalSourcePrefix prefix) {
        String token = position.map(SourcePosition::token).orElse(null);
        if (events.isEmpty() && token == null) {
            return;
        }
        // Routed before the account is asked for room: a change naming a table this reader does not carry
        // fails the run whole, before any of it is written or recorded.
        Map<String, List<SrsItem>> byTable = events.isEmpty() ? Map.of() : byTable(events, position, routes);
        prefix.awaitRoom();
        Map<String, Long> lastSeqByTable = new LinkedHashMap<>();
        for (Map.Entry<String, List<SrsItem>> entry : byTable.entrySet()) {
            lastSeqByTable.put(entry.getKey(),
                    admit(routes.apply(entry.getKey()), entry.getKey(), entry.getValue()).lastSeq());
        }
        prefix.admitted(lastSeqByTable, token);
    }

    /**
     * Projects one run of changes to ring items split by table, each table's share in the order the source
     * read it. Every change is routed before any of them is written: a change naming a table this chain does
     * not carry fails the whole run, and failing it after half of it is in the ring would leave the source
     * read offset unable to describe what happened.
     */
    private static Map<String, List<SrsItem>> byTable(
            List<Envelope> events, Optional<SourcePosition> position, Function<String, TableRoute> routes) {
        int last = events.size() - 1;
        Map<String, List<SrsItem>> byTable = new LinkedHashMap<>();
        for (int i = 0; i < events.size(); i++) {
            Envelope event = events.get(i);
            TableRoute route = routes.apply(event.src());
            if (route == null) {
                throw new TapstateException(
                        CaptureError.EVENT_TABLE_NOT_SELECTED, Map.of("table", event.src()), null);
            }
            // The position the source named for the run rides with the change that closes it and no other.
            // Carried on the earlier ones it would say of each that the source had already read past the
            // last, and a run interrupted between them would resume past changes never delivered.
            SourcePosition pos = i == last ? position.orElse(null) : null;
            byTable.computeIfAbsent(event.src(), table -> new ArrayList<>()).add(new SrsItem(
                    pos, event.op(), event.ts(), event.before(), event.after(), route.chain().schemaVer(),
                    route.chain().captureFence()));
        }
        return byTable;
    }

    /**
     * Admits one table's share of a run and returns the sequence its last change took.
     *
     * <p>A refused write is backpressure, not a drop: park off-CPU and re-check against the live consumer
     * cursor, so this call -- and with it the source read -- pauses until a consumer frees room, rather
     * than overwriting a change no consumer has read or burning a core spinning while it waits.
     *
     * <p><strong>A share longer than the ring is admitted in ring-sized pieces.</strong> A run that can
     * never fit at once would otherwise park forever waiting for room that no consumer can free, and the
     * whole capture would stop with nothing thrown -- the source asks for a bounded batch, but what a
     * connector hands over is the connector's to decide.
     */
    private static Admitted admit(TableRoute route, String table, List<SrsItem> items) {
        SrsWriteGate gate = route.chain().gate();
        int capacity = (int) Math.min(capacityOnceTheClusterAllowsIt(gate, table), Integer.MAX_VALUE);
        long lastSeq = -1;
        Collection<ConsumerOffset> offsets = List.of();
        for (int from = 0; from < items.size(); from += capacity) {
            List<SrsItem> piece = items.subList(from, Math.min(from + capacity, items.size()));
            OptionalLong refusedUntil = OptionalLong.empty();
            while (true) {
                // Re-read on every attempt, not once for the run: this is the only thing that can tell a
                // parked write that a consumer has moved, so a bound hoisted out of the loop would park
                // for ever waiting for room it could no longer see being freed.
                offsets = route.consumers().get();
                OptionalLong appended;
                try {
                    appended = gate.appendAll(piece, headroomBound(offsets, table));
                } catch (RingWriteRefusedException refused) {
                    // The cluster refused, not the headroom: nothing was written, and what refused clears
                    // itself as the members' verdicts converge. Waiting here is what pauses the source
                    // read, which is the same answer a full ring already gets.
                    refusedUntil = OptionalLong.of(waitOutTheRefusal(refused, refusedUntil, table));
                    continue;
                }
                if (appended.isPresent()) {
                    lastSeq = appended.getAsLong();
                    break;
                }
                LockSupport.parkNanos(BACKPRESSURE_PARK_NANOS);
                if (Thread.currentThread().isInterrupted()) {
                    throw new CancellationException("the cdc write was interrupted while it waited for headroom");
                }
            }
        }
        return new Admitted(lastSeq, offsets);
    }

    /**
     * The ring's capacity, waited for the same way a write is.
     *
     * <p>Reading it is a guarded operation like any other, so taking it outside the wait would end the
     * capture on precisely the refusal the wait exists to absorb -- and would do so before a single change
     * had been offered, which is the moment just after a cluster forms.
     */
    private static long capacityOnceTheClusterAllowsIt(SrsWriteGate gate, String table) {
        OptionalLong refusedUntil = OptionalLong.empty();
        while (true) {
            try {
                return gate.capacity();
            } catch (RingWriteRefusedException refused) {
                refusedUntil = OptionalLong.of(waitOutTheRefusal(refused, refusedUntil, table));
            }
        }
    }

    /**
     * Waits out one refusal and returns the instant this run of them has to end by — armed on the first
     * refusal and carried across the rest, so the bound measures the stretch the cluster has been saying
     * no rather than one attempt.
     */
    private static long waitOutTheRefusal(
            RingWriteRefusedException refused, OptionalLong refusedUntil, String table) {
        if (Thread.currentThread().isInterrupted()) {
            // The capture is being torn down: closing a subscription interrupts the thread the source
            // reads on before it stops the connector. Waiting on through that would not be waiting at all
            // -- an interrupt makes every park return at once, so the wait becomes a spin -- and it would
            // end in a verdict about the cluster for what is an ordinary close.
            throw refused;
        }
        long now = System.nanoTime();
        long until = refusedUntil.orElse(now + REFUSAL_BOUND_NANOS);
        stopIfTheClusterNeverCameBack(refused, until, now, table);
        LockSupport.parkNanos(REFUSAL_PARK_NANOS);
        return until;
    }

    /**
     * Ends the capture when the cluster has been refusing for the whole bound.
     *
     * <p>Split out from the waiting so the decision can be read at the instants that matter rather than by
     * sleeping the bound out: a case that has to wait thirty seconds to reach a branch is a case that gets
     * deleted the first time somebody is in a hurry.
     */
    static void stopIfTheClusterNeverCameBack(
            RuntimeException refused, long untilNanos, long nowNanos, String table) {
        if (nowNanos - untilNanos >= 0) {
            throw new TapstateException(
                    CaptureError.CLUSTER_REFUSED_WRITES,
                    Map.of("table", table, "seconds", REFUSAL_BOUND_NANOS / 1_000_000_000L),
                    refused);
        }
    }
}
