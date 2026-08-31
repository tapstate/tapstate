package io.tapstate.cli;

import io.tapstate.core.model.PipelineResource;
import io.tapstate.core.model.Resource;
import io.tapstate.core.model.SourceResource;

import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;

/**
 * The small local projection the dashboard needs to orient a user in a workspace. It deliberately
 * reuses {@link WorkspaceScan}, so the TUI follows the same structure-is-truth and unreadable-file
 * rules as the plain {@code ls} command without making the renderer perform I/O.
 */
final class TuiWorkspaceSnapshot {

    private TuiWorkspaceSnapshot() {
    }

    static List<TuiDashboard.ResourceSummary> scan(Path root) {
        if (root == null) {
            return List.of();
        }
        List<TuiDashboard.ResourceSummary> summaries = new ArrayList<>();
        for (WorkspaceScan.Artifact artifact : WorkspaceScan.of(root)) {
            summaries.add(summary(artifact));
        }
        return List.copyOf(summaries);
    }

    private static TuiDashboard.ResourceSummary summary(WorkspaceScan.Artifact artifact) {
        Resource resource = artifact.resource();
        if (resource == null) {
            return new TuiDashboard.ResourceSummary(artifact.kind(), artifact.id(),
                    "unreadable", null, false, false);
        }
        if (artifact.misplaced()) {
            return new TuiDashboard.ResourceSummary(artifact.kind(), artifact.id(),
                    "misplaced: declares '" + resource.kind() + "'", null, true, true);
        }
        if (resource instanceof SourceResource source) {
            String detail = source.mode() == null
                    ? source.connector()
                    : source.connector() + " · " + source.mode().yaml();
            return new TuiDashboard.ResourceSummary("source", source.id(), detail,
                    source.connector(), true, false);
        }
        if (resource instanceof PipelineResource pipeline) {
            StringBuilder detail = new StringBuilder()
                    .append(pipeline.sources().size())
                    .append(pipeline.sources().size() == 1 ? " source" : " sources");
            if (pipeline.view() != null) {
                detail.append(" · view");
            }
            if (pipeline.serve() != null) {
                detail.append(" · serve");
            }
            return new TuiDashboard.ResourceSummary("pipeline", pipeline.id(), detail.toString(),
                    null, true, false);
        }
        return new TuiDashboard.ResourceSummary(artifact.kind(), artifact.id(), "", null, true, false);
    }
}
