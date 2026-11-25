package io.tapstate.runtime.engine;

import com.hazelcast.jet.core.AbstractProcessor;
import com.hazelcast.jet.core.Processor;
import com.hazelcast.jet.core.Watermark;
import io.tapstate.core.event.Envelope;
import io.tapstate.core.event.Op;
import io.tapstate.core.event.SourceOrder;
import io.tapstate.spi.sink.TargetKeyConstraints;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;

/** Orders moves before the first row-key fan-out of a direct copy. All CDC for a physical table enters in FIFO order. */
final class SinkLaneRouter extends AbstractProcessor {
    private static final int MAX_HELD = 32;
    private static final long ACK_TIMEOUT = TimeUnit.MINUTES.toNanos(5);
    private record Output(int ordinal, Object item) {}
    private static final class Pending {
        final SinkLaneClaims.Row row;
        final List<SinkLaneAcks.Ticket> fences = new ArrayList<>();
        final long started = System.nanoTime();
        SinkLaneAcks.Ticket applied;

        Pending(SinkLaneClaims.Row row) { this.row = row; }
    }

    private final Set<String> spread;
    private final Map<String, SinkLaneClaims> claims = new HashMap<>();
    private final List<SinkLaneClaims.Row> waiting = new ArrayList<>();
    private final List<Pending> active = new ArrayList<>();
    private final ArrayDeque<Output> outbound = new ArrayDeque<>();
    private final LevelBounds bounds;
    private SinkLaneAcks replies;
    private boolean retryBound;
    private boolean boundsChanged;

    SinkLaneRouter(String node, Map<String, SinkTarget> targets, Map<String, TargetKeyConstraints> evidence,
            ChainAxes axes, Map<Integer, List<String>> chains) {
        spread = SinkRouter.spreadStreamsOf(targets);
        targets.forEach((stream, target) -> {
            if (target.keyed()) claims.put(stream, new SinkLaneClaims(node, target,
                    evidence.getOrDefault(stream, TargetKeyConstraints.unknown())));
        });
        bounds = axes == null ? null : new LevelBounds(chains, axes, this::lowestHeld);
    }

    @Override protected void init(Processor.Context context) {
        replies = new SinkLaneAcks(context.hazelcastInstance());
    }

    @Override public void close() {
        if (replies != null) replies.close();
    }

    @Override protected boolean tryProcess(int ordinal, Object item) {
        if (!pump()) return false;
        if (!(item instanceof Envelope row) || row.op() == Op.DDL || row.op() == Op.READ) {
            if (!active.isEmpty() || !waiting.isEmpty()) return false;
            int edge = item instanceof Envelope e && e.op() == Op.READ && spread.contains(e.src())
                    ? SinkRouter.SPREAD : SinkRouter.BY_KEY;
            return send(edge, item);
        }
        var domain = claims.get(row.src());
        if (domain == null) return send(SinkRouter.BY_KEY, row);
        boolean reserve = domain.requiresReservation(row);
        // Key-stable updates remain plain rows on their usual writer edge.
        if (!reserve && !hasPending(row.src())) return send(SinkRouter.BY_KEY, row);
        if (active.size() + waiting.size() >= MAX_HELD) return false;
        waiting.add(domain.describe(row, reserve));
        pump();
        return true;
    }

    // Each coordinated stream is the only direct source for its physical target.
    private boolean hasPending(String stream) {
        if (active.isEmpty() && waiting.isEmpty()) return false;
        for (var move : active) if (move.row.event().src().equals(stream)) return true;
        for (var row : waiting) if (row.event().src().equals(stream)) return true;
        return false;
    }

    private boolean send(int edge, Object item) {
        if (!tryEmit(edge, item)) outbound.add(new Output(edge, item));
        return true;
    }

    private boolean flush() {
        while (!outbound.isEmpty()) {
            Output next = outbound.element();
            if (!tryEmit(next.ordinal(), next.item())) return false;
            outbound.remove();
        }
        return true;
    }

    private boolean pump() {
        if (retryBound && !bounds.release(this::emitBound)) return false;
        if (!flush()) return false;
        if (active.isEmpty() && waiting.isEmpty()) return true;
        Set<SinkLaneClaims.Key> blocked = new HashSet<>();
        for (var it = active.iterator(); it.hasNext();) {
            var move = it.next();
            if (move.applied != null && replies.take(move.applied)) {
                it.remove();
                boundsChanged = true;
                continue;
            }
            if (System.nanoTime() - move.started >= ACK_TIMEOUT) throw SinkLaneAcks.unconfirmed();
            blocked.addAll(move.row.keys());
            move.fences.removeIf(replies::take);
            if (move.fences.isEmpty() && move.applied == null) {
                move.applied = replies.reserve();
                enqueue(new SinkLaneCommand.Move(move.row.destination(), move.row.event(), move.applied));
            }
        }
        for (var it = waiting.iterator(); it.hasNext();) {
            var row = it.next();
            if (!Collections.disjoint(blocked, row.keys())) {
                blocked.addAll(row.keys());
                continue;
            }
            it.remove();
            if (row.reserved()) {
                var move = new Pending(row);
                active.add(move);
                blocked.addAll(row.keys());
                for (var lane : row.fences()) {
                    var ticket = replies.reserve();
                    move.fences.add(ticket);
                    enqueue(new SinkLaneCommand.Fence(lane, ticket));
                }
            } else {
                enqueue(row.event());
                boundsChanged = true;
            }
        }
        return flush();
    }

    private void enqueue(Object item) {
        outbound.add(new Output(SinkRouter.BY_KEY, item));
    }

    private SourceOrder lowestHeld(String chain) {
        SourceOrder lowest = null;
        for (var row : waiting) lowest = lower(lowest, row.event(), chain);
        for (var move : active) lowest = lower(lowest, move.row.event(), chain);
        return lowest;
    }

    private static SourceOrder lower(SourceOrder floor, Envelope row, String chain) {
        var pos = row.positions().get(chain);
        if (pos == null || pos.order() == null) return floor;
        return floor == null || floor.compareTo(pos.order()) > 0 ? pos.order() : floor;
    }

    private boolean emitBound(Watermark mark) {
        retryBound = !tryEmit(mark);
        return !retryBound;
    }

    @Override public boolean tryProcess() {
        if (!pump()) return false;
        if (boundsChanged && bounds != null && !bounds.release(this::emitBound)) return false;
        boundsChanged = false;
        return true;
    }

    @Override public boolean tryProcessWatermark(int ordinal, Watermark mark) {
        return pump() && (bounds == null || bounds.advance(ordinal, mark, this::emitBound));
    }

    @Override public boolean tryProcessWatermark(Watermark mark) { return true; }

    @Override public boolean complete() {
        return tryProcess() && waiting.isEmpty() && active.isEmpty();
    }
}
