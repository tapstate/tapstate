package io.tapstate.cli;

import java.net.URI;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.Executor;
import java.util.function.Consumer;

/** Runs a bounded remote dashboard refresh and posts its safe result back to the UI mailbox. */
final class TuiResourceRefresh {

    private final ControlPlaneClient controlPlane;
    private final Executor executor;

    TuiResourceRefresh(ControlPlaneClient controlPlane, Executor executor) {
        this.controlPlane = Objects.requireNonNull(controlPlane, "controlPlane");
        this.executor = Objects.requireNonNull(executor, "executor");
    }

    void refresh(long requestId, long contextGeneration, URI baseUrl, String credential, Consumer<TuiEvent> sink) {
        if (requestId <= 0 || contextGeneration < 0 || baseUrl == null || credential == null || sink == null) {
            throw new IllegalArgumentException("refresh inputs are required");
        }
        executor.execute(() -> {
            TuiResourceRefreshResult result;
            try {
                result = collect(requestId, contextGeneration, baseUrl, credential);
            } catch (RuntimeException failure) {
                result = result(requestId, contextGeneration, List.of(), List.of(),
                        "refresh failed; retry when the control plane is available");
            }
            sink.accept(new TuiEvent.ResourceRefreshCompleted(result));
        });
    }

    private TuiResourceRefreshResult collect(long requestId, long contextGeneration, URI baseUrl, String credential) {
        ListOutcome list = controlPlane.list(baseUrl, credential, null);
        if (list instanceof ListOutcome.Rejected rejected) {
            return result(requestId, contextGeneration, List.of(), List.of(), "refresh refused: " + rejected.code());
        }
        if (list instanceof ListOutcome.Unreachable) {
            return result(requestId, contextGeneration, List.of(), List.of(), "offline: control plane unreachable");
        }
        List<RemoteArtifact> artifacts = ((ListOutcome.Listed) list).artifacts();
        List<TuiDashboard.ResourceSummary> resources = new ArrayList<>();
        List<TuiDashboard.PipelineSummary> pipelines = new ArrayList<>();
        List<String> notices = new ArrayList<>();
        for (RemoteArtifact artifact : artifacts) {
            String kind = artifact.kind();
            String detail = artifact.readable() ? "available" : "unreadable";
            if ("pipeline".equals(kind)) {
                StatusOutcome status = controlPlane.status(baseUrl, credential, artifact.id());
                if (status instanceof StatusOutcome.Found found) {
                    detail = found.state();
                    String pipelineDetail = found.failureCode() == null ? "status refreshed"
                            : "failure: " + found.failureCode();
                    pipelines.add(new TuiDashboard.PipelineSummary(artifact.id(), found.state(), pipelineDetail, null));
                } else if (status instanceof StatusOutcome.Rejected rejected) {
                    detail = "status unavailable";
                    pipelines.add(new TuiDashboard.PipelineSummary(artifact.id(), "UNKNOWN",
                            "status refused: " + rejected.code(), null));
                    notices.add(rejected.code());
                } else {
                    detail = "status unavailable";
                    pipelines.add(new TuiDashboard.PipelineSummary(artifact.id(), "OFFLINE",
                            "control plane unreachable", null));
                    notices.add("unreachable");
                }
            }
            resources.add(new TuiDashboard.ResourceSummary(kind, artifact.id(), detail, null, artifact.readable(), false));
        }
        String notice = notices.isEmpty() ? "refreshed " + resources.size() + " resources"
                : "refreshed with status notices: " + String.join(", ", notices.stream().distinct().toList());
        return result(requestId, contextGeneration, resources, pipelines, notice);
    }

    private static TuiResourceRefreshResult result(long requestId, long contextGeneration,
                                                   List<TuiDashboard.ResourceSummary> resources,
                                                   List<TuiDashboard.PipelineSummary> pipelines, String notice) {
        return new TuiResourceRefreshResult(requestId, contextGeneration, resources, pipelines,
                Instant.now().toString(), notice);
    }
}
