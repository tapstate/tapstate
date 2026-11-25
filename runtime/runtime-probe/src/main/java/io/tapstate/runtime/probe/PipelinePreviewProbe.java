package io.tapstate.runtime.probe;

/** The single control-to-runtime call that opens one bounded Pipeline preview stream. */
public interface PipelinePreviewProbe {

    /** Opens an isolated preview execution for the server-compiled candidate resources. */
    PipelinePreviewStream preview(PipelinePreviewRequest request);
}
