package io.tapstate.runtime.engine.aggregate;

import com.hazelcast.jet.core.AbstractProcessor;
import com.hazelcast.jet.core.Inbox;
import com.hazelcast.jet.core.Processor;
import com.hazelcast.jet.core.Watermark;
import io.tapstate.core.dsl.AggregateRow;
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
 */
public final class AggregateProcessor extends AbstractProcessor implements Staged {

    private final String step;
    private final AggregateStepSpec spec;
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

    public AggregateProcessor(String step, AggregateStepSpec spec, AggregateStores stores, LevelBounds bounds) {
        this.step = Objects.requireNonNull(step, "step");
        this.spec = Objects.requireNonNull(spec, "spec");
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
        this.membership = new MembershipStage(stores.open(context.hazelcastInstance(), step, AggregateStore.MEMBERS));
        this.groups = new GroupStage(spec, stores.open(context.hazelcastInstance(), step, AggregateStore.GROUPS));
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
            if (outcome.failure() != null) {
                // A group that cannot be given exactly is not given at all; what it last was stays downstream.
                withheldGroups++;
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

    /** Groups not sent because a field of theirs could not be given exactly. */
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
