package io.tapstate.runtime.engine.nest;

import com.hazelcast.jet.core.AbstractProcessor;
import com.hazelcast.jet.core.Inbox;
import com.hazelcast.jet.core.Processor;
import com.hazelcast.jet.datamodel.Tuple2;
import io.tapstate.core.event.Envelope;
import io.tapstate.core.event.Op;
import io.tapstate.core.lifecycle.Stage;
import io.tapstate.core.lifecycle.Staged;
import io.tapstate.runtime.engine.StageTimer;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/** Places both owners of a Nest key move on one producer FIFO before partitioning. */
final class NestRowOwners extends AbstractProcessor implements Staged {
    private StageTimer timer = StageTimer.none(Stage.NEST);

    private final List<String> keyFields;
    private final String namespace;
    private Tuple2<List<Object>, Envelope> departure;
    private Envelope arrival;

    NestRowOwners(List<String> keyFields, String namespace) {
        this.keyFields = List.copyOf(keyFields);
        this.namespace = namespace;
    }

    @Override
    public boolean isCooperative() {
        return true;
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
            if (!(item instanceof Envelope event) || event.op() != Op.UPDATE
                    || event.before() == null || event.after() == null
                    || !event.before().keySet().containsAll(keyFields)
                    || !event.after().keySet().containsAll(keyFields)) return tryEmit(0, item);
            // The common update keeps its key. Skip both key allocations in that case.
            if (sameKeyFields(event.before(), event.after())) {
                return tryEmit(0, item);
            }
            var oldKey = NestKeys.valuesOf(event.before(), keyFields, namespace);
            if (oldKey.equals(NestKeys.valuesOf(event.after(), keyFields, namespace))) return tryEmit(0, item);
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

    private boolean sameKeyFields(Map<String, Object> before, Map<String, Object> after) {
        for (String field : keyFields) {
            if (!Objects.equals(before.get(field), after.get(field))) return false;
        }
        return true;
    }
}
