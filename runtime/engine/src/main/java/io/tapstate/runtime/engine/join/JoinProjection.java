package io.tapstate.runtime.engine.join;

import io.tapstate.core.event.Envelope;
import io.tapstate.core.sql.Expr;
import io.tapstate.core.sql.JoinKey;
import io.tapstate.core.sql.JoinPlan;
import io.tapstate.core.sql.JoinTree;

import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;

/**
 * Refreshes a join's output after changes from different source partitions meet at the fact key.
 * A projected row can wait in one processor's outbox while another publishes a newer match. The
 * arrival is therefore a request to publish the current row, not an image that may overwrite it.
 */
public final class JoinProjection {
    private final List<String> outputKeys;
    private final JoinStores stores;
    private final JoinDriver projector;

    public JoinProjection(JoinPlan plan, List<String> factKeys, String stream, JoinStores stores) {
        this.outputKeys = outputKeyColumns(plan, factKeys);
        this.stores = stores;
        this.projector = new JoinDriver(plan, factKeys, stream, stores);
    }

    static List<String> outputKeyColumns(JoinPlan plan, List<String> factKeys) {
        List<String> result = new ArrayList<>();
        for (String key : factKeys) {
            JoinTree.ColumnRef wanted = new JoinTree.ColumnRef(plan.factSource().name(), key);
            result.add(plan.outputFields().stream()
                    .filter(field -> field.from() instanceof Expr.Column column
                            && column.ref().equals(wanted))
                    .map(field -> field.name()).findFirst()
                    .orElseThrow(() -> new IllegalArgumentException("join output does not carry fact key " + key)));
        }
        return List.copyOf(result);
    }

    /** One mirror read per delivery, including missing facts that must now be deleted. */
    public List<Envelope> refresh(List<Envelope> arrivals) {
        List<String> keys = arrivals.stream().map(this::key).toList();
        Map<String, Map<String, Object>> facts = stores.factsUnder(new LinkedHashSet<>(keys));
        List<Envelope> result = new ArrayList<>(arrivals.size());
        for (int i = 0; i < arrivals.size(); i++) {
            Envelope arrival = arrivals.get(i);
            Map<String, Object> fact = facts.get(keys.get(i));
            if (fact == null) {
                Map<String, Object> old = arrival.after() != null ? arrival.after() : arrival.before();
                result.add(Envelope.delete(arrival.ts(), arrival.src(), old, null));
                continue;
            }
            Envelope current = projector.rowEvent(fact, arrival.ts(), false);
            result.add(current != null ? current : projector.rowEvent(fact, arrival.ts(), true));
        }
        return result;
    }

    private String key(Envelope event) {
        Map<String, Object> row = event.after() != null ? event.after() : event.before();
        return JoinKey.of(outputKeys.stream().map(row::get).toList()).name();
    }
}
