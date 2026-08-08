package io.tapstate.runtime.engine.nest;

import io.tapstate.core.event.ChainPosition;
import io.tapstate.core.event.Envelope;
import io.tapstate.core.event.Op;
import io.tapstate.core.event.SourceOrder;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/** Reading the few things a nest vertex needs off an event, the same way at every vertex. */
final class NestKeys {

    private NestKeys() {
    }

    /**
     * The values {@code fields} name on {@code row}, in that order. A key is always a list even when one
     * field long, so a composite key and a single one are the same kind of value everywhere downstream.
     * Nulls are kept rather than rejected: a key column that is null is a data problem to be seen in the
     * document, not an invariant for this to decide.
     */
    static List<Object> valuesOf(Map<String, Object> row, List<String> fields) {
        List<Object> values = new ArrayList<>(fields.size());
        for (String field : fields) {
            values.add(row.get(field));
        }
        return Collections.unmodifiableList(values);
    }

    /** The row an event carries: what it became, or what it was when that is all a deletion leaves. */
    static Map<String, Object> rowOf(Envelope event) {
        Map<String, Object> row = event.after() != null ? event.after() : event.before();
        if (row == null) {
            throw new IllegalStateException("event " + event.op() + " on " + event.src() + " carries no row");
        }
        return row;
    }

    /**
     * The order to compare this event on. Absent is an engine invariant violation rather than a
     * diagnosable error: an event reaching a stateful node without one cannot be placed against what is
     * already there, and guessing would silently reorder data.
     */
    static SourceOrder orderOf(Envelope event) {
        ChainPosition at = event.position();
        return Objects.requireNonNull(at == null ? null : at.order(),
                "event on " + event.src() + " reached a stateful node with no order");
    }

    /** Whether this event removes what it names rather than putting a row there. */
    static boolean isDeletion(Envelope event) {
        return event.op() == Op.DELETE;
    }
}
