package io.tapstate.control.core;

import io.tapstate.core.dsl.Advisory;
import io.tapstate.core.dsl.DiscoveredTable;
import io.tapstate.core.dsl.ReservedColumnRules;
import io.tapstate.core.model.Resource;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * The advisory pass that reports every source column read under another name because its own name
 * is the one the product reserves on a row. It carries nothing of its own: the discovered model the
 * batch was judged against already records which columns were renamed and from what.
 */
public final class ReservedColumnAdvisories implements PlanAdvisories {

    @Override
    public List<ValidationDiagnostic> review(
            List<Resource> resources, Map<String, List<DiscoveredTable>> tablesBySource) {
        List<ValidationDiagnostic> findings = new ArrayList<>();
        for (Advisory advisory : ReservedColumnRules.review(resources, tablesBySource)) {
            findings.add(new ValidationDiagnostic(advisory.code().code(), advisory.params()));
        }
        return List.copyOf(findings);
    }
}
