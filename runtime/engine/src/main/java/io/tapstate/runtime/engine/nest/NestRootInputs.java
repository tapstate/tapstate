package io.tapstate.runtime.engine.nest;

import com.hazelcast.jet.core.AbstractProcessor;
import com.hazelcast.jet.core.Inbox;
import com.hazelcast.jet.core.Processor;
import com.hazelcast.jet.datamodel.Tuple2;
import io.tapstate.core.event.Envelope;
import io.tapstate.core.lifecycle.Stage;
import io.tapstate.core.lifecycle.Staged;
import io.tapstate.runtime.engine.StageTimer;
import java.util.List;

/** Places both owners of a root move on one producer FIFO before partitioning. */
final class NestRootInputs extends AbstractProcessor implements Staged {
    private StageTimer timer = StageTimer.none(Stage.NEST);

    private final NestInbound edge;
    private final String namespace;
    private Tuple2<List<Object>, Envelope> departure;
    private Envelope arrival;

    NestRootInputs(NestInbound edge, String namespace) {
        this.edge = edge;
        this.namespace = namespace;
    }

    @Override
    public Stage stage() {
        return Stage.NEST;
    }

    @Override
    protected void init(Processor.Context context) {
        timer = StageTimer.of(stage(), context);
    }

    @Override
    public void process(int ordinal, Inbox inbox) {
        long started = timer.begin();
        try {
            super.process(ordinal, inbox);
        } finally {
            timer.end(started);
        }
    }

    @Override
    protected boolean tryProcess(int ordinal, Object item) {
        if (departure == null && arrival == null) {
            if (!(item instanceof Envelope event)) return tryEmit(0, item);
            var was = NestKeys.replacedRow(edge, event);
            if (was == null) return tryEmit(0, event);
            var oldKey = NestKeys.valuesOf(was, edge.keyFields(), namespace);
            if (oldKey.equals(NestKeys.valuesOf(NestKeys.rowOf(event), edge.keyFields(), namespace))) {
                return tryEmit(0, item);
            }
            departure = Tuple2.tuple2(oldKey, event);
            arrival = event;
        }
        if (departure != null) {
            if (!tryEmit(0, departure)) return false;
            departure = null;
        }
        if (arrival != null) {
            if (!tryEmit(0, arrival)) return false;
            arrival = null;
        }
        return true;
    }
}
