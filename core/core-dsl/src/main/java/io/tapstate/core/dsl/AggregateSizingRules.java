package io.tapstate.core.dsl;

import io.tapstate.core.dsl.aggregate.Aggregates;
import io.tapstate.core.dsl.aggregate.StateKind;
import io.tapstate.core.model.PipelineResource;
import io.tapstate.core.model.Resource;
import io.tapstate.core.model.Step;
import io.tapstate.core.model.TransformBody;
import io.tapstate.core.model.TransformResource;
import java.util.ArrayList;
import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * States, for every aggregate step in a batch, how many state entries it keeps per input row and what
 * that comes to on the rows its inputs were discovered to hold.
 *
 * <p>The step stores each input row it has seen once, to know its group; on the group side the row
 * again, as its contribution; and one more entry for each field that keeps its inputs, since a retracted
 * extreme or distinct value cannot be undone from a total. So the cost per row is two and the number of
 * such fields, and nothing about the data changes it.
 *
 * <p><b>A count that was never taken is not a count of zero.</b> A total over inputs of which one
 * reports no rows would pass as the whole, so a step with any unsized input is told its cost per row and
 * no total.
 */
public final class AggregateSizingRules {

    private AggregateSizingRules() {
    }

    /** How many entries a step keeps for one input row. */
    public static int entriesPerRow(TransformBody.Aggregate step) {
        return 2 + keptInputFields(step).size();
    }

    /** The fields of {@code step} that keep their inputs, in declared order. */
    public static List<String> keptInputFields(TransformBody.Aggregate step) {
        List<String> kept = new ArrayList<>();
        step.fields().forEach((name, field) -> {
            if (field.function() != null
                    && Aggregates.of(field.function()).stateKind() == StateKind.MATERIALIZED_INPUT) {
                kept.add(name);
            }
        });
        return kept;
    }

    /**
     * The statements over {@code batch}, in the order the steps are written. {@code tablesBySource} is
     * what each source was discovered to hold, keyed by the source's id; a source it omits has not been
     * discovered, and its tables count as unsized rather than as empty.
     */
    public static List<Advisory> review(Collection<Resource> batch, Map<String, List<DiscoveredTable>> tablesBySource) {
        Map<String, Resource> byId = new LinkedHashMap<>();
        for (Resource resource : batch) {
            byId.putIfAbsent(resource.id(), resource);
        }
        List<Advisory> findings = new ArrayList<>();
        for (Resource resource : batch) {
            if (!(resource instanceof PipelineResource pipeline) || pipeline.transforms() == null) {
                continue;
            }
            Wiring wiring = null;
            for (Step step : pipeline.transforms()) {
                TransformBody body = switch (step) {
                    case Step.Inline inline -> inline.body();
                    case Step.Use use -> byId.get(use.use()) instanceof TransformResource definition
                            ? definition.body() : null;
                };
                if (!(body instanceof TransformBody.Aggregate aggregate) || step.from() == null) {
                    continue;
                }
                if (wiring == null) {
                    wiring = new Wiring(pipeline, byId);
                }
                int perRow = entriesPerRow(aggregate);
                Long rows = rowsOf(wiring.reaching(step.from()), tablesBySource);
                if (rows == null) {
                    findings.add(new Advisory(AggregateSizingError.STATE_ESTIMATE_UNSIZED, Map.of(
                            "pipeline", pipeline.id(), "step", step.id(), "entriesPerRow", perRow)));
                } else {
                    findings.add(new Advisory(AggregateSizingError.STATE_ESTIMATE, Map.of(
                            "pipeline", pipeline.id(), "step", step.id(), "entriesPerRow", perRow,
                            "inputRows", rows, "estimatedEntries", rows * perRow)));
                }
            }
        }
        return List.copyOf(findings);
    }

    /** The rows every input holds, added up; null when there is none or any of them was not counted. */
    private static Long rowsOf(Set<Upstream> inputs, Map<String, List<DiscoveredTable>> tablesBySource) {
        if (inputs.isEmpty()) {
            return null;
        }
        long rows = 0;
        for (Upstream input : inputs) {
            List<DiscoveredTable> discovered = input.table() == null ? null : tablesBySource.get(input.source());
            Long counted = null;
            if (discovered != null) {
                for (DiscoveredTable table : discovered) {
                    if (table.name().equals(input.table())) {
                        counted = table.approximateRowCount();
                    }
                }
            }
            if (counted == null) {
                return null;
            }
            rows += counted;
        }
        return rows;
    }
}
