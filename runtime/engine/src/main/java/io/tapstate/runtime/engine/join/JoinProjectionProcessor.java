package io.tapstate.runtime.engine.join;

import com.hazelcast.jet.core.Processor;
import io.tapstate.runtime.engine.StageTimer;
import com.hazelcast.jet.core.AbstractProcessor;
import com.hazelcast.jet.core.Inbox;
import io.tapstate.core.event.Envelope;
import io.tapstate.core.lifecycle.Stage;
import io.tapstate.core.lifecycle.Staged;
import io.tapstate.runtime.engine.SettledPositions;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Deque;
import java.util.List;

/** Publishes each fact key from one ordered processor, after reading its current joined value. */
final class JoinProjectionProcessor extends AbstractProcessor implements Staged {

    @Override
    public Stage stage() {
        return Stage.JOIN;
    }
    private final JoinProjection projection;
    private final Deque<Envelope> pending = new ArrayDeque<>();
    private int taken;

    JoinProjectionProcessor(JoinProjection projection) {
        this.projection = projection;
    }

    /**
     * Like every other vertex that reaches the state layer, and for the same reason: every call into the
     * join's state is a call into the cluster, and one the cluster refuses while a member joining catches up
     * is waited out where it was made. A call that waits on a cooperative thread stops every other vertex
     * sharing that thread rather than only this one.
     */
    @Override
    public boolean isCooperative() {
        return false;
    }

    // Times each drain of arrivals, which is this stage's unit of work.
    private StageTimer timer = StageTimer.none(Stage.JOIN);

    @Override
    protected void init(Processor.Context context) {
        this.timer = StageTimer.of(stage(), context);
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
        while (!inbox.isEmpty()) {
            if (inbox.peek() instanceof SettledPositions) {
                if (!tryEmit(inbox.peek())) {
                    return;
                }
                inbox.poll();
                continue;
            }
            if (taken == 0) {
                List<JoinUpdate> arrivals = new ArrayList<>(inbox.size());
                // A marker follows all output from the rows before it, including a refused emission.
                for (Object item : inbox) {
                    if (item instanceof SettledPositions) {
                        break;
                    }
                    arrivals.add((JoinUpdate) item);
                }
                pending.addAll(projection.refresh(arrivals));
                taken = arrivals.size();
            }
            while (!pending.isEmpty()) {
                if (!tryEmit(pending.peek())) {
                    return;
                }
                pending.remove();
            }
            for (int i = 0; i < taken; i++) {
                inbox.poll();
            }
            taken = 0;
        }
    }
}
