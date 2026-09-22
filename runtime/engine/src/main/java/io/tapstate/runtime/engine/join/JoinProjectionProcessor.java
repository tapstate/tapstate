package io.tapstate.runtime.engine.join;

import com.hazelcast.jet.core.Processor;
import io.tapstate.runtime.engine.StageTimer;
import com.hazelcast.jet.core.AbstractProcessor;
import com.hazelcast.jet.core.Inbox;
import io.tapstate.core.event.Envelope;
import io.tapstate.core.lifecycle.Stage;
import io.tapstate.core.lifecycle.Staged;

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
    private boolean taken;

    JoinProjectionProcessor(JoinProjection projection) {
        this.projection = projection;
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
        if (!taken) {
            List<JoinUpdate> arrivals = new ArrayList<>(inbox.size());
            for (Object item : inbox) {
                arrivals.add((JoinUpdate) item);
            }
            pending.addAll(projection.refresh(arrivals));
            taken = true;
        }
        while (!pending.isEmpty()) {
            if (!tryEmit(pending.peek())) {
                return;
            }
            pending.remove();
        }
        inbox.clear();
        taken = false;
    }
}
