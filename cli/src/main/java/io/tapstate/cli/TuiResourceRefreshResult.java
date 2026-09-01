package io.tapstate.cli;

import java.util.List;

/** Safe projection of one background resource refresh, ready for UI-thread reduction. */
record TuiResourceRefreshResult(long requestId, long contextGeneration,
                                List<TuiDashboard.ResourceSummary> resources,
                                List<TuiDashboard.PipelineSummary> pipelines,
                                String refreshedAt, String notice) {

    TuiResourceRefreshResult {
        if (requestId <= 0 || contextGeneration < 0) {
            throw new IllegalArgumentException("refresh request id and context generation are required");
        }
        resources = resources == null ? List.of() : List.copyOf(resources);
        pipelines = pipelines == null ? List.of() : List.copyOf(pipelines);
        refreshedAt = refreshedAt == null || refreshedAt.isBlank() ? null : refreshedAt;
        notice = notice == null ? "" : notice;
    }
}
