package io.tapstate.runtime.srs;

import io.tapstate.core.event.Envelope;
import io.tapstate.core.common.TapstateException;
import io.tapstate.spi.capture.CaptureConfig;
import io.tapstate.spi.capture.CapturePort;
import io.tapstate.spi.capture.CaptureStart;
import io.tapstate.core.event.ChainPosition;
import io.tapstate.core.event.SourceOrder;
import io.tapstate.spi.capture.SourcePosition;
import io.tapstate.spi.capture.Subscription;
import io.tapstate.spi.store.ConsumerOffset;

import java.util.Collection;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.LockSupport;
import java.util.function.Function;
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
                (events, position) -> writeBatch(events, position, table -> route, lastWritten)));
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
        Objects.requireNonNull(port, "port");
        Objects.requireNonNull(config, "config");
        Objects.requireNonNull(start, "start");
        Objects.requireNonNull(routes, "routes");
        Objects.requireNonNull(health, "health");
        Map<String, TableRoute> routeSnapshot = Map.copyOf(routes);
        // The last position actually persisted, for the life of this subscription. It is what lets a run
        // that resolves the same frontier as the one before it write nothing: see writeBatch.
        AtomicReference<ChainPosition> lastWritten = new AtomicReference<>();
        return port.cdc(config, start, health.recording(
                (events, position) -> writeBatch(events, position, routeSnapshot::get, lastWritten)));
    }

    /**
     * One table's wiring: its ring, the slowest consumer's cursor in it, the chain's consumer offsets, and
     * a cut of the durable log behind it.
     *
     * <p>{@code trimThrough} is handed the sequence every consumer has durably landed, and drops the log
     * at or below it -- a change every consumer has landed has no replay value left, and without the cut
     * the log grows without bound. <strong>Whether that sequence can be attributed to this ring at all is
     * the caller's to know</strong>, not this phase's: a chain carrying several tables records one acked
     * position for the whole chain, and its sequence came from whichever ring held that change. A caller
     * that cannot attribute it passes a cut that does nothing, and says why where it does so.
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
     * The slowest consumer's read cursor into one table's ring — how far ahead of its readers the ring may
     * be written. {@link Long#MAX_VALUE} when nothing constrains it (no consumer has a durable cursor yet),
     * and {@code -1} for a consumer that has read nothing of the table.
     *
     * <p>Derived here rather than fetched separately because it is a function of the same cursors the
     * durable frontier is: asking a store for it on its own means reading one record twice per run.
     */
    static long headroomBound(Collection<ConsumerOffset> offsets, String table) {
        return offsets.stream()
                .mapToLong(offset -> offset.perTableSeq().getOrDefault(table, -1L))
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
            AtomicReference<ChainPosition> lastWritten) {
        if (events.isEmpty()) {
            // The source handed over only events that carry no change -- a heartbeat and its like. There is
            // nothing to write, and nothing has been read past, so the offset does not move either.
            return;
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
                    pos, event.op(), event.ts(), event.before(), event.after(), route.chain().schemaVer()));
        }
        String closingTable = events.get(last).src();
        long closingSeq = -1;
        Collection<ConsumerOffset> closingOffsets = List.of();
        for (Map.Entry<String, List<SrsItem>> entry : byTable.entrySet()) {
            Admitted admitted = admit(routes.apply(entry.getKey()), entry.getKey(), entry.getValue());
            if (entry.getKey().equals(closingTable)) {
                closingSeq = admitted.lastSeq();
                // The cursors the admission read, rather than a second reading of them. They are the same
                // record, and a reading taken a moment earlier can only be behind -- which clamps the
                // advance shorter, never further, so the bound it enforces still holds.
                closingOffsets = admitted.offsets();
            }
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
        int capacity = (int) Math.min(route.chain().gate().capacity(), Integer.MAX_VALUE);
        long lastSeq = -1;
        Collection<ConsumerOffset> offsets = List.of();
        for (int from = 0; from < items.size(); from += capacity) {
            List<SrsItem> piece = items.subList(from, Math.min(from + capacity, items.size()));
            while (true) {
                // Re-read on every attempt, not once for the run: this is the only thing that can tell a
                // parked write that a consumer has moved, so a bound hoisted out of the loop would park
                // for ever waiting for room it could no longer see being freed.
                offsets = route.consumers().get();
                OptionalLong appended = route.chain().gate()
                        .appendAll(piece, headroomBound(offsets, table));
                if (appended.isPresent()) {
                    lastSeq = appended.getAsLong();
                    break;
                }
                LockSupport.parkNanos(BACKPRESSURE_PARK_NANOS);
            }
        }
        return new Admitted(lastSeq, offsets);
    }
}
