package io.tapstate.control.core;

import io.tapstate.core.dsl.Advisory;
import io.tapstate.core.dsl.AggregateSizingRules;
import io.tapstate.core.dsl.DiscoveredTable;
import io.tapstate.core.model.Resource;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * The advisory pass that tells whoever applied a batch what each aggregate step in it will keep in
 * state: the entries per input row, and the total where the inputs' sizes were discovered.
 */
public final class AggregateSizingAdvisories implements PlanAdvisories {

    @Override
    public List<ValidationDiagnostic> review(
            List<Resource> resources, Map<String, List<DiscoveredTable>> tablesBySource) {
        List<ValidationDiagnostic> findings = new ArrayList<>();
        for (Advisory advisory : AggregateSizingRules.review(resources, tablesBySource)) {
            findings.add(new ValidationDiagnostic(advisory.code().code(), advisory.params()));
        }
        return List.copyOf(findings);
    }
}
