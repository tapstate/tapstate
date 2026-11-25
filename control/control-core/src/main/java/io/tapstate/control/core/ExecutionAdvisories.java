package io.tapstate.control.core;

import io.tapstate.core.dsl.Advisory;
import io.tapstate.core.dsl.DiscoveredTable;
import io.tapstate.core.dsl.ExecutionRules;
import io.tapstate.core.model.Resource;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * The advisory pass that reports what a batch asks of how its nodes run and will not get as written: a
 * pipeline-level parallelism or batch size nothing reads any more, and a source asking to be read by more
 * than one processor.
 *
 * <p>It needs nothing the batch does not already carry, so the discovered tables go unread.
 */
public final class ExecutionAdvisories implements PlanAdvisories {

    @Override
    public List<ValidationDiagnostic> review(
            List<Resource> resources, Map<String, List<DiscoveredTable>> tablesBySource) {
        List<ValidationDiagnostic> findings = new ArrayList<>();
        for (Advisory advisory : ExecutionRules.review(resources)) {
            // The code travels as its canonical string and the arguments by name, which is what the
            // catalog renders from -- the same shape a refusal takes on this endpoint.
            findings.add(new ValidationDiagnostic(advisory.code().code(), advisory.params()));
        }
        return List.copyOf(findings);
    }
}
