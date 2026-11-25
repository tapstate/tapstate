package io.tapstate.runtime.engine.nest;

import com.hazelcast.jet.core.AbstractProcessor;
import com.hazelcast.jet.core.Inbox;
import com.hazelcast.jet.core.Processor;
import com.hazelcast.jet.core.Watermark;
import com.hazelcast.jet.datamodel.Tuple2;
import io.tapstate.core.event.Envelope;
import io.tapstate.core.event.Op;
import io.tapstate.core.event.SourceOrder;
import io.tapstate.core.lifecycle.Stage;
import io.tapstate.core.lifecycle.Staged;
import io.tapstate.runtime.engine.ChainAxes;
import io.tapstate.runtime.engine.LevelBounds;
import io.tapstate.runtime.engine.StageTimer;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

/** One ordered root input. Only conflicting root changes wait for a subtree to reach its new owner. */
final class RootMoveDispatcher extends AbstractProcessor implements Staged {
    private StageTimer timer = StageTimer.none(Stage.NEST);
    private static final int CAPACITY = DrainFolding.MAX_KEYS_HELD;
    private record Row(Envelope event, List<Object> from, List<Object> to) {
        boolean moves() { return !from.equals(to); }
        boolean conflicts(Set<List<Object>> keys) { return keys.contains(from) || keys.contains(to); }
    }

    private final NestVertex vertex;
    private final long protectionMillis;
    private final NestClock clock;
    private record Move(RootMoveAcks.Ticket ticket, long started) {}
    private final List<Row> waiting = new ArrayList<>();
    private final Map<Row, Move> moving = new LinkedHashMap<>();
    private final Set<List<Object>> occupied = new HashSet<>();
    private RootMoveAcks.Inbox notices;
    private boolean retryBound;
    private Row offering;
    private Object departure;
    private Object arrival;
    private final LevelBounds bounds;

    RootMoveDispatcher(NestVertex vertex, NestSettings settings, NestClock clock, ChainAxes axes, Map<Integer, List<String>> chains) {
        this.vertex = vertex;
        this.clock = clock;
        protectionMillis = settings.migrationProtectionIn(vertex.mapName());
        bounds = axes == null ? null : new LevelBounds(chains, axes, this::lowestHeld);
    }

    @Override public Stage stage() { return Stage.NEST; }

    @Override protected void init(Processor.Context context) {
        timer = StageTimer.of(stage(), context);
        notices = new RootMoveAcks.Inbox(context.hazelcastInstance());
    }

    @Override public void close() { if (notices != null) notices.close(); }

    @Override public boolean isCooperative() { return false; }

    @Override public void process(int ordinal, Inbox inbox) {
        long started = timer.begin();
        try {
            super.process(ordinal, inbox);
        } finally {
            timer.end(started);
        }
    }

    @Override protected boolean tryProcess(int ordinal, Object item) {
        if (!pump()) return false;
        if (!(item instanceof Envelope event)) {
            if (!moving.isEmpty() || !waiting.isEmpty()) return false;
            return pass(item);
        }
        boolean mayMove = changesRootKey(event);
        if (moving.isEmpty() && waiting.isEmpty() && !mayMove) return pass(item);
        if (waiting.size() + moving.size() >= CAPACITY) return false;
        List<Object> to = key(NestKeys.rowOf(event));
        List<Object> from = mayMove ? key(event.before()) : to;
        waiting.add(new Row(event, from, to));
        pump();
        return true;
    }

    private boolean pass(Object item) {
        if (!tryEmit(0, item)) arrival = item;
        return true;
    }

    private boolean emitBound(Watermark mark) {
        retryBound = !tryEmit(mark);
        return !retryBound;
    }

    private List<Object> key(Map<String, Object> row) {
        return NestKeys.valuesOf(row, vertex.partitionKey(), vertex.mapName());
    }

    private boolean changesRootKey(Envelope event) {
        if (event.op() != Op.UPDATE || event.before() == null || event.after() == null) return false;
        for (String field : vertex.partitionKey()) {
            if (!event.before().containsKey(field) || !event.after().containsKey(field)) return false;
        }
        for (String field : vertex.partitionKey()) {
            if (!java.util.Objects.equals(event.before().get(field), event.after().get(field))) return true;
        }
        return false;
    }

    private boolean pump() {
        if (retryBound && !bounds.release(this::emitBound)) return false;
        if (arrival != null && !offer()) return false;
        if (moving.isEmpty() && waiting.isEmpty()) return true;
        for (var it = moving.entrySet().iterator(); it.hasNext();) {
            var entry = it.next();
            if (notices.take(entry.getValue().ticket())) {
                occupied.remove(entry.getKey().from());
                occupied.remove(entry.getKey().to());
                it.remove();
            } else if (clock.millis() - entry.getValue().started() >= protectionMillis) {
                throw RootMoveAcks.unconfirmed(vertex);
            }
        }
        if (waiting.isEmpty()) return true;
        var blocked = new HashSet<>(occupied);
        for (Iterator<Row> it = waiting.iterator(); it.hasNext();) {
            Row row = it.next();
            if (row.conflicts(blocked)) {
                blocked.add(row.from());
                blocked.add(row.to());
                continue;
            }
            it.remove();
            offering = row;
            arrival = row.event();
            if (row.moves()) {
                var ticket = notices.reserve();
                departure = Tuple2.tuple2(ticket, Tuple2.tuple2(row.from(), row.event()));
                arrival = Tuple2.tuple2(ticket, row.event());
                moving.put(row, new Move(ticket, clock.millis()));
                occupied.add(row.from());
                occupied.add(row.to());
                blocked.add(row.from());
                blocked.add(row.to());
            }
            if (!offer()) return false;
        }
        return true;
    }

    private boolean offer() {
        if (departure != null) {
            if (!tryEmit(0, departure)) return false;
            departure = null;
        }
        if (!tryEmit(0, arrival)) return false;
        arrival = null;
        offering = null;
        return true;
    }

    private SourceOrder lowestHeld(String chain) {
        SourceOrder floor = null;
        for (Row row : moving.keySet()) floor = lower(floor, row, chain);
        for (Row row : waiting) floor = lower(floor, row, chain);
        if (offering != null) floor = lower(floor, offering, chain);
        return floor;
    }

    private SourceOrder lower(SourceOrder floor, Row row, String chain) {
        var held = row.event().positions().get(chain);
        if (held == null || held.order() == null) return floor;
        return floor == null || floor.compareTo(held.order()) > 0 ? held.order() : floor;
    }

    @Override public boolean tryProcess() {
        return pump() && (bounds == null || bounds.release(this::emitBound));
    }

    @Override public boolean tryProcessWatermark(int ordinal, Watermark mark) {
        return pump() && (bounds == null || bounds.advance(ordinal, mark, this::emitBound));
    }

    @Override public boolean tryProcessWatermark(Watermark mark) {
        return bounds != null || (pump() && moving.isEmpty() && waiting.isEmpty() && super.tryProcessWatermark(mark));
    }

    @Override public boolean complete() {
        return pump() && waiting.isEmpty() && moving.isEmpty() && (bounds == null || bounds.release(this::emitBound));
    }
}
