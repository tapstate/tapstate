package io.tapstate.adapters.transform;

import io.tapstate.core.event.Envelope;
import io.tapstate.core.event.Op;
import io.tapstate.spi.transform.TransformPort;
import java.util.List;

/**
 * The {@code filter} port: a CEL predicate keeps matching rows and retracts rows that stop matching.
 * The earlier row is evaluated as {@code after} for a rejected update; the original
 * operation and other envelope bindings still apply, so explicit operation filters are respected.
 * A {@code ddl} event bypasses the predicate because it carries no row.
 */
final class FilterPort implements TransformPort {

    private final RowExpressionProgram predicate;

    FilterPort(String expr) {
        this.predicate = RowExpressionProgram.predicate(expr);
    }

    @Override
    public List<Envelope> transform(Envelope event) {
        if (event.op() == Op.DDL) {
            return List.of(event);
        }
        if (Boolean.TRUE.equals(predicate.eval(event))) {
            return List.of(event);
        }
        if (event.op() == Op.UPDATE && event.before() != null
                && Boolean.TRUE.equals(predicate.eval(event, event.before()))) {
            return List.of(new Envelope(Op.DELETE, event.ts(), event.src(), event.before(), null,
                    event.schema(), event.positions(), event.removed()));
        }
        return List.of();
    }
}
