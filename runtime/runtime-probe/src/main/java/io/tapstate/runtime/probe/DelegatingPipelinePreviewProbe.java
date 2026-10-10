package io.tapstate.runtime.probe;

import java.util.Objects;

/** Runtime-side delegate for the application-owned finite preview executor. */
public final class DelegatingPipelinePreviewProbe implements PipelinePreviewProbe {

    private final PipelinePreviewProbe executor;

    public DelegatingPipelinePreviewProbe(PipelinePreviewProbe executor) {
        this.executor = Objects.requireNonNull(executor, "executor");
    }

    @Override
    public PipelinePreviewStream preview(PipelinePreviewRequest request) {
        return executor.preview(request);
    }
}
