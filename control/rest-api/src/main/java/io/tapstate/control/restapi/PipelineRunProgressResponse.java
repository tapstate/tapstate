package io.tapstate.control.restapi;

import io.tapstate.control.core.PipelineRunProgressQuery;
import io.tapstate.core.lifecycle.PipelineRunProgress;
import io.tapstate.core.lifecycle.PipelineRunStage;
import java.util.Comparator;
import java.util.List;

record PipelineRunProgressResponse(
        Availability availability,
        String runId,
        Long version,
        List<PipelineRunProgress.Activity> active,
        List<PipelineRunProgress.Milestone> milestones,
        PipelineRunProgress.LoadProgress initialLoad,
        PipelineRunStage failedStage,
        boolean awaitingMilestones) {
    enum Availability { AVAILABLE, UNAVAILABLE }

    static PipelineRunProgressResponse unavailable() {
        return new PipelineRunProgressResponse(Availability.UNAVAILABLE, null, null, List.of(), List.of(), null, null, false);
    }

    static PipelineRunProgressResponse from(PipelineRunProgressQuery.Read read) {
        var progress = read.progress();
        var timeline = progress.milestones().stream().sorted(Comparator.comparing(
                PipelineRunProgress.Milestone::reachedAt, Comparator.nullsFirst(Comparator.naturalOrder()))).toList();
        return new PipelineRunProgressResponse(Availability.AVAILABLE, progress.runId(), progress.version(),
                progress.active(), timeline, progress.initialLoad(), progress.failedStage(), read.awaitingMilestones());
    }
}
