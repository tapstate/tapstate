package io.tapstate.runtime.engine.join;

import com.hazelcast.jet.core.AbstractProcessor;
import com.hazelcast.jet.core.Inbox;
import com.hazelcast.jet.core.Processor;
import com.hazelcast.jet.datamodel.Tuple2;
import io.tapstate.core.event.ConvertedValue;
import io.tapstate.core.event.Envelope;
import io.tapstate.core.sql.JoinKey;
import io.tapstate.core.lifecycle.Stage;
import io.tapstate.core.lifecycle.Staged;
import io.tapstate.runtime.engine.StageTimer;

import java.util.List;
import java.util.Objects;

/** Addresses a dimension update to each bucket it mutates before the distributed partition edge. */
final class DimensionOwners extends AbstractProcessor implements Staged {
    private StageTimer timer = StageTimer.none(Stage.JOIN);
    private final List<String> columns;
    private Tuple2<String, Envelope> departure;
    private Tuple2<String, Envelope> arrival;

    DimensionOwners(List<String> columns) {
        this.columns = columns;
    }

    @Override
    public Stage stage() {
        return Stage.JOIN;
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
            if (!(item instanceof Envelope event) || event.before() == null || event.after() == null
                    || sameValues(event)) {
                return tryEmit(item);
            }
            JoinKey oldKey = JoinKey.of(JoinKeys.valuesOf(event.before(), columns));
            if (!oldKey.matchable()) return tryEmit(item);
            JoinKey newKey = JoinKey.of(JoinKeys.valuesOf(event.after(), columns));
            if (newKey.matchable() && oldKey.name().equals(newKey.name())) return tryEmit(item);
            departure = Tuple2.tuple2(oldKey.name(), event);
            if (newKey.matchable()) arrival = Tuple2.tuple2(newKey.name(), event);
        }
        // Both addresses carry the original input position. Backpressure must finish this input
        // before a later row or watermark can leave the producer's FIFO.
        if (departure != null) {
            if (!tryEmit(departure)) return false;
            departure = null;
        }
        if (arrival != null) {
            if (!tryEmit(arrival)) return false;
            arrival = null;
        }
        return true;
    }

    private boolean sameValues(Envelope event) {
        for (String column : columns) {
            if (!Objects.equals(ConvertedValue.unwrap(event.before().get(column)),
                    ConvertedValue.unwrap(event.after().get(column)))) return false;
        }
        return true;
    }
}
