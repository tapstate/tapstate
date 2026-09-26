package io.tapstate.control.core;

import java.util.OptionalLong;

/** Reports best-effort telemetry cleanup failures without changing the completed artifact removal. */
public interface TelemetryCleanupReporter {

    /** The removed owner's identity is captured before asynchronous cleanup can race a recreation. */
    record Failure(String pipelineId, String incarnationId, OptionalLong executionGeneration,
            String step, boolean rejected) {
    }

    void failed(Failure failure);

    TelemetryCleanupReporter NONE = failure -> { };
}
