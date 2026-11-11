package io.tapstate.runtime.engine.aggregate;

import com.hazelcast.jet.core.AbstractProcessor;
import com.hazelcast.jet.core.Inbox;
import com.hazelcast.jet.core.Processor;
import com.hazelcast.jet.core.Watermark;
import io.tapstate.core.common.TapstateException;
import io.tapstate.core.dsl.AggregateRow;
import io.tapstate.core.dsl.aggregate.AggregateDefinition;
import io.tapstate.core.dsl.aggregate.AggregateError;
import io.tapstate.core.dsl.aggregate.AggregateStepSpec;
import io.tapstate.core.dsl.aggregate.Contribution;
import io.tapstate.core.dsl.aggregate.OrderedBytes;
import io.tapstate.core.event.ChainPosition;
import io.tapstate.core.event.Envelope;
import io.tapstate.core.event.Op;
import io.tapstate.core.lifecycle.Stage;
import io.tapstate.core.lifecycle.Staged;
import io.tapstate.runtime.engine.LevelBounds;
import io.tapstate.runtime.engine.OperatorMapCoordinates.AggregateStore;
import io.tapstate.runtime.engine.SettledPositions;
import io.tapstate.runtime.engine.StageTimer;
import io.tapstate.runtime.engine.aggregate.GroupChange.FieldInput;
import io.tapstate.runtime.engine.aggregate.GroupStage.GroupOutcome;
import io.tapstate.runtime.engine.aggregate.MembershipStage.RowEvent;
import io.tapstate.spi.store.OrderedStateStore;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Deque;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/**
 * One aggregate step as one vertex: the rows of its input in, one row per changed group out.
 *
 * <p><b>Store, then send.</b> Everything taken from the inbox in one go is applied to the membership
 * side and then to the group side, each committing before it answers, and only then is anything
 * emitted. A stop anywhere before the last emit leaves the events untaken as far as the source is
 * concerned; they arrive again with the orders they had, the stages recognise them, and the groups
 * they touched are sent again. Nothing is sent that is not stored.
 *
 * <p><b>One processor for the whole cluster.</b> Both sides run here, one after the other, so a row's
 * leaving one group and arriving in another are applied by the same thread in the order the
 * membership side tells them. The stores block, so the processor is not cooperative.
 *
 * <p>An input row is told apart by the step's key and ordered by the position it carries on the chain
 * it came through. A row without an order cannot be placed against what is stored and is refused as a
 * defect of whatever wired the step, never guessed at.
 *
 * <p>A group that cannot be given exactly is never sent as if it could: it goes out with the values it
 * last had and a marker saying so, and every other group carries on.
 */
public final class AggregateProcessor extends AbstractProcessor implements Staged {

    /** The reserved field a held group's marker travels under, and the name it is filed under inside it. */
    static final String MARKER_FIELD = "__tap";
    static final String MARKER_OWNER = "aggregate";

    /** Sorts before every entry of a group, whose keys start at 0x01. */
    private static final byte[] DEFINITION_KEY = {0x00};

    private final String step;
    private final AggregateStepSpec spec;
    private final AggregateDefinition definition;
    private final AggregateStores stores;
    private final LevelBounds bounds;
    private final List<String> groupNames;

    private MembershipStage membership;
    private GroupStage groups;
    private StageTimer timer = StageTimer.none(Stage.TRANSFORM);

    /** What the events taken from the inbox came to and has not been emitted yet. */
    private final Deque<Object> pending = new ArrayDeque<>();
    private int taken;

    private long withheldGroups;
    private long conflicts;
    private long dropped;

    /**
     * @param definition what the step is defined as now; state built by another definition refuses the run
     */
    public AggregateProcessor(String step, AggregateStepSpec spec, AggregateDefinition definition,
            AggregateStores stores, LevelBounds bounds) {
        this.step = Objects.requireNonNull(step, "step");
        this.spec = Objects.requireNonNull(spec, "spec");
        this.definition = Objects.requireNonNull(definition, "definition");
        this.stores = Objects.requireNonNull(stores, "stores");
        this.bounds = bounds;
        this.groupNames = spec.outputKey();
    }

    @Override
    public Stage stage() {
        return Stage.TRANSFORM;
    }

    @Override
    public boolean isCooperative() {
        return false;
    }

    @Override
    protected void init(Processor.Context context) {
        this.timer = StageTimer.of(stage(), context);
        OrderedStateStore byGroup = stores.open(context.hazelcastInstance(), step, AggregateStore.GROUPS);
        admit(byGroup);
        this.membership = new MembershipStage(stores.open(context.hazelcastInstance(), step, AggregateStore.MEMBERS));
        this.groups = new GroupStage(spec, byGroup);
    }

    /**
     * Refuses state built by another definition, and marks empty state as built by this one. The mark is
     * written before the first row is, under the same claim, so state never exists without it.
     */
    private void admit(OrderedStateStore byGroup) {
        byte[] stored = byGroup.loadAll(List.of(DEFINITION_KEY)).getFirst();
        if (stored == null) {
            byGroup.commit(List.of(new OrderedStateStore.Entry(DEFINITION_KEY, definition.stored())), List.of());
            return;
        }
        List<String> changed = definition.changedSince(AggregateDefinition.read(stored));
        if (!changed.isEmpty()) {
            throw new TapstateException(AggregateError.DEFINITION_CHANGED,
                    Map.of("step", step, "changed", String.join(", ", changed)), null);
        }
    }

    @Override
    public void process(int ordinal, Inbox inbox) {
        long started = timer.begin();
        try {
            while (!inbox.isEmpty()) {
                if (inbox.peek() instanceof SettledPositions settled) {
                    if (!tryEmit(settled)) {
                        return;
                    }
                    inbox.poll();
                    continue;
                }
                if (taken == 0) {
                    take(inbox);
                }
                if (!drain()) {
                    return;
                }
                for (int i = 0; i < taken; i++) {
                    inbox.poll();
                }
                taken = 0;
            }
        } finally {
            timer.end(started);
        }
    }

    /** Applies every event up to the next settled marker and queues what the groups they touched now are. */
    private void take(Inbox inbox) {
        List<RowEvent> events = new ArrayList<>(inbox.size());
        Map<String, ChainPosition> positions = new LinkedHashMap<>();
        long ts = 0;
        int count = 0;
        for (Object item : inbox) {
            if (item instanceof SettledPositions) {
                break;
            }
            Envelope event = (Envelope) item;
            count++;
            ts = Math.max(ts, event.ts());
            SettledPositions.fold(positions, event.positions());
            rowEvents(event, events);
        }
        MembershipStage.Outcome told = membership.apply(events, Map.of());
        dropped += told.dropped();
        conflicts += told.conflicts() + told.spansInputs();
        for (GroupOutcome outcome : groups.apply(told.changes())) {
            dropped += outcome.dropped();
            conflicts += outcome.conflicts();
            if (!outcome.send()) {
                continue;
            }
            if (outcome.failure() != null && outcome.members() > 0) {
                withheldGroups++;
                pending.add(held(outcome, ts).withPositions(positions));
                continue;
            }
            pending.add(row(outcome, ts).withPositions(positions));
        }
        if (!positions.isEmpty()) {
            pending.add(new SettledPositions(positions));
        }
        taken = count;
    }

    private boolean drain() {
        while (!pending.isEmpty()) {
            if (!tryEmit(pending.peek())) {
                return false;
            }
            pending.poll();
        }
        return true;
    }

    /** The events one envelope is: its row as a member or as gone, and the row it replaced when the key moved. */
    private void rowEvents(Envelope event, List<RowEvent> out) {
        if (event.op() == Op.DDL) {
            return;
        }
        ChainPosition at = event.position();
        if (at == null || at.order() == null) {
            throw new IllegalStateException("aggregate step '" + step + "' was handed a row of '" + event.src()
                    + "' that carries no order on its own chain");
        }
        String chain = event.src();
        byte[] before = event.before() == null ? null : OrderedBytes.ofKey(AggregateRow.key(spec, event.before()));
        if (event.op() == Op.DELETE) {
            if (before == null) {
                throw new IllegalStateException("aggregate step '" + step + "' was handed a delete of '"
                        + event.src() + "' without the row it deletes");
            }
            out.add(RowEvent.gone(before, chain, at.order()));
            return;
        }
        byte[] key = OrderedBytes.ofKey(AggregateRow.key(spec, event.after()));
        if (before != null && !Arrays.equals(before, key)) {
            out.add(RowEvent.gone(before, chain, at.order()));
        }
        var evaluated = AggregateRow.of(spec, event.after());
        if (evaluated.isEmpty()) {
            out.add(RowEvent.gone(key, chain, at.order()));
            return;
        }
        List<FieldInput> inputs = new ArrayList<>(spec.fields().size());
        for (Contribution given : evaluated.get().inputs()) {
            inputs.add(given == null ? FieldInput.OUT : FieldInput.of(given.value(), given.by()));
        }
        out.add(RowEvent.member(key, chain, at.order(), evaluated.get().group(), inputs));
    }

    /** A group as the row it is sent as: an upsert of its names and fields, or a delete once it holds no row. */
    private Envelope row(GroupOutcome outcome, long ts) {
        Map<String, Object> row = new LinkedHashMap<>();
        for (int i = 0; i < groupNames.size(); i++) {
            row.put(groupNames.get(i), outcome.group().get(i));
        }
        if (outcome.members() == 0) {
            return Envelope.delete(ts, step, row, null);
        }
        row.putAll(outcome.values());
        return Envelope.insert(ts, step, row, null);
    }

    /**
     * A group that cannot be given exactly, as the row it is sent as: its names, the values it last had
     * when every field could be given, or nulls when it never had any, and a marker saying the row is
     * not current. A healthy row never carries the marker, and the next healthy row replaces this one
     * whole, so a reader's rule is one line: a row without it is current.
     */
    private Envelope held(GroupOutcome outcome, long ts) {
        Map<String, Object> row = new LinkedHashMap<>();
        for (int i = 0; i < groupNames.size(); i++) {
            row.put(groupNames.get(i), outcome.group().get(i));
        }
        for (var field : spec.fields()) {
            row.put(field.field(), outcome.lastGood() == null ? null : outcome.lastGood().get(field.field()));
        }
        Map<String, Object> marker = new LinkedHashMap<>();
        marker.put("status", "failed");
        marker.put("code", AggregateError.of(outcome.failure()).code());
        marker.put("failed_since", java.time.Instant.ofEpochMilli(outcome.failedSince()).toString());
        marker.put("last_good_at", outcome.lastGood() == null ? null
                : java.time.Instant.ofEpochMilli(outcome.lastGoodAt()).toString());
        row.put(MARKER_FIELD, Map.of(MARKER_OWNER, Map.of(step, java.util.Collections.unmodifiableMap(marker))));
        return Envelope.insert(ts, step, row, null);
    }

    @Override
    public boolean tryProcess() {
        return drain();
    }

    @Override
    public boolean complete() {
        return drain();
    }

    @Override
    public boolean tryProcessWatermark(int ordinal, Watermark watermark) {
        return bounds == null || (drain() && bounds.advance(ordinal, watermark, this::tryEmit));
    }

    @Override
    public boolean tryProcessWatermark(Watermark watermark) {
        return bounds != null || super.tryProcessWatermark(watermark);
    }

    /** Times a group was sent held at its last good values because a field of it could not be given exactly. */
    public long withheldGroups() {
        return withheldGroups;
    }

    /** Events not applied because what is stored under their order or their row disagrees with them. */
    public long conflicts() {
        return conflicts;
    }

    /** Events older than what is stored for their row. */
    public long dropped() {
        return dropped;
    }
}
