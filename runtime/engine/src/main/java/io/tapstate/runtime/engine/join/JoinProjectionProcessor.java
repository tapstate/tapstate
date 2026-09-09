package io.tapstate.runtime.engine.join;

import com.hazelcast.jet.core.AbstractProcessor;
import com.hazelcast.jet.core.Inbox;
import io.tapstate.core.event.Envelope;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Deque;
import java.util.List;

/** Publishes each fact key from one ordered processor, after reading its current joined value. */
final class JoinProjectionProcessor extends AbstractProcessor {
    private final JoinProjection projection;
    private final Deque<Envelope> pending = new ArrayDeque<>();
    private boolean taken;

    JoinProjectionProcessor(JoinProjection projection) {
        this.projection = projection;
    }

    @Override
    public void process(int ordinal, Inbox inbox) {
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
