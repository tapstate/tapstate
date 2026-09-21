package io.tapstate.control.core;

import io.tapstate.core.common.Severity;
import io.tapstate.core.common.TapstateErrorCode;

import java.util.Set;

/** Stable client-attributable failures for the durable Pipeline draft HTTP surface. */
public enum PipelineDraftError implements TapstateErrorCode {

    NOT_FOUND("pipeline-draft.not-found", Set.of("id")),
    ALREADY_EXISTS("pipeline-draft.already-exists", Set.of("id")),
    PRECONDITION_REQUIRED("pipeline-draft.precondition-required", Set.of("id")),
    REVISION_CONFLICT("pipeline-draft.revision-conflict", Set.of("id")),
    INVALID("pipeline-draft.invalid", Set.of("id", "reason")),
    MODE_CONFLICT("pipeline-draft.mode-conflict", Set.of("id")),
    ARTIFACT_CONFLICT("pipeline-draft.artifact-conflict", Set.of("id"));

    private final String code;
    private final Set<String> placeholders;

    PipelineDraftError(String code, Set<String> placeholders) {
        this.code = code;
        this.placeholders = placeholders;
    }

    @Override
    public String code() {
        return code;
    }

    @Override
    public Severity severity() {
        return Severity.ERROR;
    }

    @Override
    public Set<String> placeholders() {
        return placeholders;
    }
}
