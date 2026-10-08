package io.tapstate.runtime.engine.join;

import com.hazelcast.internal.metrics.DynamicMetricsProvider;
import com.hazelcast.internal.metrics.MetricDescriptor;
import com.hazelcast.internal.metrics.MetricsCollectionContext;

import com.hazelcast.jet.core.Processor;
import io.tapstate.runtime.engine.StageTimer;
import com.hazelcast.jet.core.AbstractProcessor;
import com.hazelcast.jet.core.Inbox;
import com.hazelcast.jet.core.Watermark;
import io.tapstate.core.event.ChainPosition;
import io.tapstate.core.event.Envelope;
import io.tapstate.core.event.SourceOrder;
import io.tapstate.core.lifecycle.Stage;
import io.tapstate.core.lifecycle.Staged;
import io.tapstate.runtime.engine.ChainAxes;
import io.tapstate.runtime.engine.LevelBounds;
import io.tapstate.runtime.engine.SettledPositions;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Deque;
import java.util.List;
import java.util.Map;

/** Publishes each fact key from one ordered processor, after reading its current joined value. */
final class JoinProjectionProcessor extends AbstractProcessor implements Staged, DynamicMetricsProvider {

    @Override
    public void provideDynamicMetrics(MetricDescriptor descriptor, MetricsCollectionContext collection) {
        timer.provideDynamicMetrics(descriptor, collection);
    }

    @Override
    public Stage stage() {
        return Stage.JOIN;
    }
    private final JoinProjection projection;
    private final Deque<Object> pending = new ArrayDeque<>();
    private final LevelBounds bounds;
    private int taken;

    JoinProjectionProcessor(JoinProjection projection) {
        this(projection, null, null);
    }

    JoinProjectionProcessor(JoinProjection projection, ChainAxes axes, List<String> chains) {
        this.projection = projection;
        this.bounds = axes == null ? null : new LevelBounds(
                Map.of(0, chains), axes, this::lowestPendingOn);
    }

    // Times each drain of arrivals, which is this stage's unit of work.
    private volatile StageTimer timer = StageTimer.none(Stage.JOIN);

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
            if (taken == 0) {
                if (inbox.peek() instanceof SettledPositions word) {
                    pending.add(word);
                    taken = 1;
                } else {
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
            }
            if (!drainPending()) {
                return;
            }
            for (int i = 0; i < taken; i++) {
                inbox.poll();
            }
            taken = 0;
        }
    }

    private boolean drainPending() {
        while (!pending.isEmpty()) {
            if (!tryEmit(pending.peek())) {
                return false;
            }
            pending.remove();
        }
        return bounds == null || bounds.release(this::tryEmit);
    }

    private SourceOrder lowestPendingOn(String chain) {
        SourceOrder lowest = null;
        for (Object item : pending) {
            Map<String, ChainPosition> positions = item instanceof SettledPositions word
                    ? word.positions() : ((Envelope) item).positions();
            ChainPosition position = positions.get(chain);
            if (position != null && position.order() != null
                    && (lowest == null || position.order().compareTo(lowest) < 0)) {
                lowest = position.order();
            }
        }
        return lowest;
    }

    @Override
    public boolean tryProcess() {
        return drainPending();
    }

    @Override
    public boolean complete() {
        return drainPending();
    }

    @Override
    public boolean tryProcessWatermark(int ordinal, Watermark watermark) {
        return bounds == null || bounds.advance(ordinal, watermark, this::tryEmit);
    }

    @Override
    public boolean tryProcessWatermark(Watermark watermark) {
        return true;
    }
}
