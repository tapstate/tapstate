package io.tapstate.runtime.srs;

import io.tapstate.core.event.Envelope;
import io.tapstate.core.common.TapstateException;
import io.tapstate.spi.capture.CaptureConfig;
import io.tapstate.spi.capture.CaptureListener;
import io.tapstate.spi.capture.CapturePort;
import io.tapstate.spi.capture.CaptureStart;
import io.tapstate.core.event.ChainPosition;
import io.tapstate.core.event.SourceOrder;
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
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.LockSupport;
import java.util.function.Function;
import java.util.function.Consumer;
import java.util.function.LongConsumer;
import java.util.function.LongSupplier;
import java.util.function.Supplier;

/**
 * The unbounded cdc phase of a capture. Starts the change stream and, for every change event (ops
 * {@code i} / {@code u} / {@code d} / {@code ddl}), projects it to a change-ring item and admits it
 * through the headroom gate into the per-table ring — the only hot buffer the SRS keeps. A snapshot read
 * (op {@code r}) never reaches here; the ring item rejects it by construction.
 *
 * <p>The per-event source position is threaded at this seam: the event envelope carries no position slot,
 * so each change is stamped with the position the source reported for it. Most changes carry none — a
 * source names one position for a run of changes — and the durable read offset therefore advances at
 * those boundaries rather than on every change, which is exactly where the claim "everything up to here
 * has been read" is true.
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
     * Starts the cdc stream and returns the subscription that stops it. Each change event is projected to
     * a ring item carrying the position the source reported for it and appended through the headroom gate, which
     * refuses a write that would overwrite a change the slowest consumer has not read.
     *
     * @param consumers the chain's consumer cursors, which bound both how far the ring may be written
     *                  ahead of its slowest reader and how far the durable read offset may advance
     */
    public static Subscription run(
            CapturePort port,
            CaptureConfig config,
            CdcChain chain,
            Supplier<Collection<ConsumerOffset>> consumers,
            CaptureHealth health) {
        Objects.requireNonNull(port, "port");
        Objects.requireNonNull(config, "config");
        Objects.requireNonNull(chain, "chain");
        Objects.requireNonNull(consumers, "consumers");
        Objects.requireNonNull(health, "health");
        return run(port, config, CaptureStart.present(), chain, consumers, health);
    }

    /** As above, beginning where {@code start} says rather than always at the source's present moment. */
    public static Subscription run(
            CapturePort port,
            CaptureConfig config,
            CaptureStart start,
            CdcChain chain,
            Supplier<Collection<ConsumerOffset>> consumers,
            CaptureHealth health) {
        Objects.requireNonNull(port, "port");
        Objects.requireNonNull(config, "config");
        Objects.requireNonNull(start, "start");
        Objects.requireNonNull(chain, "chain");
        Objects.requireNonNull(consumers, "consumers");
        Objects.requireNonNull(health, "health");
        // One chain serves every table this subscription sees, so the route resolves to it whatever the
        // change names -- the same run-writing path the multi-table entry point takes.
        // No ring name and no log reach this entry point, so there is nothing here that could name what
        // to cut. The caller that owns both wires a real cut through the other entry point.
        TableRoute route = new TableRoute(chain, consumers, seq -> { });
        AtomicReference<ChainPosition> lastWritten = new AtomicReference<>();
        return port.cdc(config, start, health.recording(
                (events, position) -> writeBatch(events, position, table -> route, lastWritten, null)));
    }

    /** Starts one connector subscription and routes each event to the ring for its source table. */
    public static Subscription run(
            CapturePort port,
            CaptureConfig config,
            Map<String, TableRoute> routes,
            CaptureHealth health) {
        return run(port, config, CaptureStart.present(), routes, health);
    }

    /**
     * As above, beginning where {@code start} says. One subscription serves every table of the chain, so
     * the start is the chain's — the tail is one log read, not one per table.
     */
    public static Subscription run(
            CapturePort port,
            CaptureConfig config,
            CaptureStart start,
            Map<String, TableRoute> routes,
            CaptureHealth health) {
        return run(port, config, start, routes, health, null);
    }

    /** The shared-chain path releases only capture-owned physical batch barriers. */
    static Subscription run(
            CapturePort port,
            CaptureConfig config,
            CaptureStart start,
            Map<String, TableRoute> routes,
            CaptureHealth health,
            PhysicalSourcePrefix prefix) {
        return run(port, config, start, routes, health, prefix, null);
    }

    static Subscription run(
            CapturePort port,
            CaptureConfig config,
            CaptureStart start,
            Map<String, TableRoute> routes,
            CaptureHealth health,
            PhysicalSourcePrefix prefix,
            Consumer<Optional<SourcePosition>> singleTableAnchor) {
        Objects.requireNonNull(port, "port");
        Objects.requireNonNull(config, "config");
        Objects.requireNonNull(start, "start");
        Objects.requireNonNull(routes, "routes");
        Objects.requireNonNull(health, "health");
        Map<String, TableRoute> routeSnapshot = Map.copyOf(routes);
        String chainId = routeSnapshot.isEmpty() ? "unknown"
                : routeSnapshot.values().iterator().next().chain().miningChainId();
        AtomicBoolean startReported = new AtomicBoolean(prefix == null && singleTableAnchor == null);
        // The last position actually persisted, for the life of this subscription. It is what lets a run
        // that resolves the same frontier as the one before it write nothing: see writeBatch.
        AtomicReference<ChainPosition> lastWritten = new AtomicReference<>();
        try {
            CaptureListener listener = new CaptureListener() {
                @Override
                public void onStart(Optional<SourcePosition> position) {
                    if (prefix != null) {
                        prefix.anchor(position);
                    } else if (singleTableAnchor != null) {
                        singleTableAnchor.accept(position);
                    }
                    startReported.set(true);
                }

                @Override
                public void onBatch(List<Envelope> events, Optional<SourcePosition> position) {
                    if (!startReported.get()) {
                        throw new TapstateException(CaptureError.RESUME_ANCHOR_UNAVAILABLE,
                                Map.of("chain", chainId), null);
                    }
                    writeBatch(events, position, routeSnapshot::get, lastWritten, prefix);
                }
            };
            Subscription source = port.cdc(config, start, health.recording(listener));
            if (prefix == null) {
                return source;
            }
            return () -> {
                try {
                    source.close();
                } finally {
                    prefix.close();
                }
            };
        } catch (RuntimeException | Error failure) {
            if (prefix != null) {
                prefix.close();
            }
            throw failure;
        }
    }

    /**
     * One table's wiring: its ring, the slowest consumer's cursor in it, the chain's consumer offsets, and
     * a cut of the durable log behind it.
     *
     * <p>{@code trimThrough} is handed this table's admitted sequence. The caller may record it and cut
     * only after every consumer selecting this table in the same ring generation has persisted a completion
     * at or beyond it. The chain-level source prefix is not a per-table ring completion.
     */
    public record TableRoute(
            CdcChain chain,
            Supplier<Collection<ConsumerOffset>> consumers,
            LongConsumer trimThrough) {

        public TableRoute {
            Objects.requireNonNull(chain, "chain");
            Objects.requireNonNull(consumers, "consumers");
            Objects.requireNonNull(trimThrough, "trimThrough");
        }
    }

    /**
     * The slowest subscribed consumer's read cursor into one table's ring — how far ahead of its readers
     * the ring may be written. An older record with no selection protects every table. A selected table
     * without a read cursor, or with one from another ring generation, protects the ring from its start.
     * The answer is {@link Long#MAX_VALUE} when no consumer is subscribed to this table.
     *
     * <p>Derived here rather than fetched separately because it is a function of the same cursors the
     * durable frontier is: asking a store for it on its own means reading one record twice per run.
     */
    static long headroomBound(Collection<ConsumerOffset> offsets, String table, long epoch) {
        return offsets.stream()
                .filter(offset -> offset.selectedTables() == null || offset.selectedTables().contains(table))
                .mapToLong(offset -> offset.selectedTables() == null
                        || !Objects.equals(offset.selectedTablesEpoch(), epoch)
                        ? -1L : offset.perTableSeq().getOrDefault(table, -1L))
                .min()
                .orElse(Long.MAX_VALUE);
    }

    /** One table's admitted share: the sequence its last change took, and the cursors that let it in. */
    private record Admitted(long lastSeq, Collection<ConsumerOffset> offsets) {
    }

    /**
     * Projects one run of changes to ring items, admits each table's share of the run into that table's
     * ring in one act, and advances the durable read offset once, to the position the source named for the
     * run.
     *
     * <p>The run is split by table because the rings are per table, and each table's share stays in the
     * order the source read it. <strong>Every change is routed before any of them is written</strong>: a
     * change naming a table this chain does not carry fails the whole run, and failing it after half of it
     * is in the ring would leave the source read offset unable to describe what happened.
     */
    private static void writeBatch(
            List<Envelope> events,
            Optional<SourcePosition> position,
            Function<String, TableRoute> routes,
            AtomicReference<ChainPosition> lastWritten,
            PhysicalSourcePrefix prefix) {
        if (events.isEmpty()) {
            // The source handed over only events that carry no change -- a heartbeat and its like. There is
            // nothing to write, and nothing has been read past, so the offset does not move either.
            return;
        }
        if (prefix != null) {
            prefix.awaitRoom();
        }
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
                    route.chain().captureFence(), route.chain().epoch()));
        }
        String closingTable = events.get(last).src();
        long closingSeq = -1;
        Collection<ConsumerOffset> closingOffsets = List.of();
        Map<String, Long> lastRingSeqByTable = new LinkedHashMap<>();
        for (Map.Entry<String, List<SrsItem>> entry : byTable.entrySet()) {
            Admitted admitted = admit(routes.apply(entry.getKey()), entry.getKey(), entry.getValue());
            lastRingSeqByTable.put(entry.getKey(), admitted.lastSeq());
            if (entry.getKey().equals(closingTable)) {
                closingSeq = admitted.lastSeq();
                // The cursors the admission read, rather than a second reading of them. They are the same
                // record, and a reading taken a moment earlier can only be behind -- which clamps the
                // advance shorter, never further, so the bound it enforces still holds.
                closingOffsets = admitted.offsets();
            }
        }
        if (prefix != null) {
            prefix.admitted(lastRingSeqByTable, position.map(SourcePosition::token).orElse(null));
            lastRingSeqByTable.forEach((table, seq) -> routes.apply(table).trimThrough().accept(seq));
            prefix.trimIfDrained();
            return;
        }
        // The run is in the rings; advance the durable read offset to the position that closes it, clamped
        // so it never passes the slowest consumer's sink-acked position -- a change only ever in the
        // volatile ring must stay re-minable from the source until a sink has durably landed it.
        // The sequence the ring just assigned, paired with the generation it is running under, is what
        // ranks this position against the consumers' acked ones: a token says nothing about order.
        TableRoute closing = routes.apply(closingTable);
        CdcChain chain = closing.chain();
        ChainPosition read = new ChainPosition(new SourceOrder(chain.epoch(), closingSeq),
                position.map(SourcePosition::token).orElse(null));
        SrsDurableFrontier.safeAdvance(read, closingOffsets).ifPresent(safe -> {
            // A backpressured or idle chain resolves the same frontier run after run: the advance is
            // clamped to the slowest sink's acked position, and that does not move while the sink is not
            // landing anything. Persisting it again writes the value the record already holds, and cutting
            // to it again cuts what is already gone. Neither is wrong, and on a real endpoint both are a
            // synchronous round trip on the thread the source reads on, so the run pays to say nothing.
            if (safe.equals(lastWritten.get())) {
                return;
            }
            chain.meta().advanceSourceReadOffset(chain.miningChainId(), safe);
            lastWritten.set(safe);
            // The same frontier bounds what the log still has to keep: every consumer has durably landed
            // the change at that sequence, so nothing will ever replay it or anything before it. The cut
            // rides the frontier rather than running on its own clock because this is the only moment the
            // frontier is known to have moved -- and it costs one more call on a path that already makes
            // one, rather than one per change.
            closing.trimThrough().accept(safe.order().seq());
        });
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
                    appended = gate.appendAll(piece, headroomBound(offsets, table, route.chain().epoch()));
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
