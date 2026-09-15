package io.tapstate.adapters.transform;

import io.tapstate.core.event.Envelope;
import io.tapstate.spi.transform.TransformPort;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * The {@code map} port: a field projection over the row image. Declared rules run first in declared
 * order (rename / drop / literal / computed), then every unlisted source field that a rename did not
 * consume passes through in its original order. A rule whose source field is absent is a no-op — the
 * output is simply not produced, never an error.
 *
 * <p><b>Both of an event's rows are projected, not just the new one.</b> An update carries the row
 * it replaces and a delete carries nothing else, and either one left unprojected leaves the two
 * sides of one event in different shapes - so a consumer lining them up compares a renamed column
 * against its old name, never matches, and a row the target was meant to lose stays behind. Only an
 * event with no row at all ({@code ddl}) passes through untouched.
 */
final class MapPort implements TransformPort {

    private final MapSpec spec;
    // Computed rules are compiled once, member-side, keyed by their (unique) output name.
    private final Map<String, RowExpressionProgram> computed;
    // Drop targets: removed from the projection, so a same-named source field is not passed through.
    private final Set<String> droppedNames;
    // Source fields a rename takes: consumed, so they are not passed through under their old name.
    private final Set<String> consumedSources;

    MapPort(MapSpec spec) {
        this.spec = spec;
        Map<String, RowExpressionProgram> compiled = new HashMap<>();
        Set<String> dropped = new HashSet<>();
        Set<String> consumed = new HashSet<>();
        for (MapRule rule : spec.rules()) {
            if (rule instanceof MapRule.Computed c) {
                compiled.put(c.output(), RowExpressionProgram.value(c.expr()));
            } else if (rule instanceof MapRule.Rename r) {
                consumed.add(r.source());
            } else if (rule instanceof MapRule.Drop d) {
                dropped.add(d.output());
            }
        }
        this.computed = compiled;
        this.droppedNames = dropped;
        this.consumedSources = consumed;
    }

    @Override
    public List<Envelope> transform(Envelope event) {
        if (event.before() == null && event.after() == null) {
            return List.of(event);
        }
        return List.of(new Envelope(event.op(), event.ts(), event.src(),
                project(event, event.before()), project(event, event.after()), event.schema())
                // Carried through, not added to. What travels beside a row is the set of fields an
                // earlier step said the row no longer has, and it is the only way a target ever hears
                // that one went - so a projection that rebuilds the row and forgets it silently
                // withdraws the statement, leaving the target on the old value with every row that
                // arrives still correct. Dropping a field here is a different thing and deliberately
                // adds nothing: it means the field is not carried onward, never that a target should
                // delete what it already holds. Rebuilding two rows is two chances to lose it.
                .withRemoved(event.removed()));
    }

    /**
     * One row through the rules, or null where there is no such row.
     *
     * <p>A computed value is a function of the event, and the event has two rows in it - so which
     * one it reads has to be said. It reads the row being projected: the earlier row's computed
     * column is what that column was. Evaluating both sides against the new row would be worse than
     * not projecting at all, because the two would then agree on a column that actually changed and
     * anything comparing them would conclude nothing moved.
     */
    private Map<String, Object> project(Envelope event, Map<String, Object> row) {
        if (row == null) {
            return null;
        }
        // A view for evaluation only, never emitted: it binds the row being projected where an
        // expression looks for the current one, so `after.x` reads that row's own x.
        Envelope reading = row == event.after()
                ? event
                : new Envelope(event.op(), event.ts(), event.src(), row, row, event.schema());
        Map<String, Object> out = new LinkedHashMap<>();
        for (MapRule rule : spec.rules()) {
            switch (rule) {
                case MapRule.Rename r -> {
                    if (row.containsKey(r.source())) {
                        out.put(r.output(), row.get(r.source()));
                    }
                }
                case MapRule.Drop ignored -> {
                    // the field is not carried into the projection
                }
                case MapRule.Literal l -> out.put(l.output(), l.value());
                case MapRule.Computed c -> out.put(c.output(), computed.get(c.output()).eval(reading));
            }
        }
        // A source field passes through unless a rule already produced its name (so a rename / literal
        // / computed output wins over the same-named source), a rename consumed it, or a drop removed
        // it. A rule that produced nothing (a rename whose source was absent) leaves the field alone.
        row.forEach((field, value) -> {
            if (!out.containsKey(field) && !consumedSources.contains(field) && !droppedNames.contains(field)) {
                out.put(field, value);
            }
        });
        return out;
    }
}
