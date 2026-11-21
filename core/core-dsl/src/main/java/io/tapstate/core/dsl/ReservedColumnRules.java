package io.tapstate.core.dsl;

import io.tapstate.core.common.ReservedFieldError;
import io.tapstate.core.model.Resource;
import io.tapstate.core.model.SourceResource;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;

/**
 * Reports each source column that is read under another name because its own name is the one the
 * product reserves on a row.
 *
 * <p><b>Reported, not refused.</b> The column crosses with every value intact and the pipeline runs.
 * What the author needs to hear is that their targets receive a field name they did not write, and
 * that anything in the batch that refers to the column has to use that name.
 *
 * <p><b>Per source, not per pipeline.</b> The rename happens where rows enter, once, whatever reads
 * them afterwards, so it is said once for each column of each source in the batch, also for a source
 * no pipeline in this batch reads yet.
 */
public final class ReservedColumnRules {

    private ReservedColumnRules() {
    }

    /**
     * The findings over {@code batch}, in the order the sources and their columns were discovered. A
     * source {@code tablesBySource} omits has not been discovered, and nothing is said about it.
     */
    public static List<Advisory> review(
            Collection<Resource> batch, Map<String, List<DiscoveredTable>> tablesBySource) {
        List<Advisory> findings = new ArrayList<>();
        for (Resource resource : batch) {
            if (!(resource instanceof SourceResource source)) {
                continue;
            }
            for (DiscoveredTable table : tablesBySource.getOrDefault(source.id(), List.of())) {
                table.renamedColumns().forEach((renamed, column) -> findings.add(new Advisory(
                        ReservedFieldError.RENAMED, Map.of(
                                "source", source.id(),
                                "table", table.name(),
                                "column", column,
                                "renamed", renamed))));
            }
        }
        return List.copyOf(findings);
    }
}
