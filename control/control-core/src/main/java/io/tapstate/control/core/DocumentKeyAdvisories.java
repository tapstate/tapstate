package io.tapstate.control.core;

import io.tapstate.core.dsl.Advisory;
import io.tapstate.core.dsl.DiscoveredTable;
import io.tapstate.core.dsl.DocumentKeyRules;
import io.tapstate.core.model.Resource;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * The advisory pass that reports every column a batch would write into a store which reads a dot in a
 * key as a step into a nested document, when the column's own name holds one.
 *
 * <p>It carries nothing of its own: the column names are already in the discovered model the batch was
 * judged against, and which stores address a key by path is a property of those stores rather than of
 * the deployment running the pipeline.
 */
public final class DocumentKeyAdvisories implements PlanAdvisories {

    @Override
    public List<ValidationDiagnostic> review(
            List<Resource> resources, Map<String, List<DiscoveredTable>> tablesBySource) {
        List<ValidationDiagnostic> findings = new ArrayList<>();
        for (Advisory advisory : DocumentKeyRules.review(resources, tablesBySource)) {
            // The code travels as its canonical string and the arguments by name, which is what the
            // catalog renders from -- the same shape a refusal takes on this endpoint.
            findings.add(new ValidationDiagnostic(advisory.code().code(), advisory.params()));
        }
        return List.copyOf(findings);
    }
}
