package io.tapstate.control.core;

import io.tapstate.core.common.Severity;
import io.tapstate.core.common.TapstateErrorCode;

import java.util.Set;

/** Stable coded failures for structured Pipeline artifact reads and conditional edits. */
public enum PipelineError implements TapstateErrorCode {

    ALREADY_EXISTS("pipeline.already-exists", Set.of("id")),
    NOT_FOUND("pipeline.not-found", Set.of("id")),
    ID_MISMATCH("pipeline.id-mismatch", Set.of("pathId", "bodyId")),
    PRECONDITION_REQUIRED("pipeline.precondition-required", Set.of("id")),
    VERSION_CONFLICT("pipeline.version-conflict", Set.of("id"));

    private final String code;
    private final Set<String> placeholders;

    PipelineError(String code, Set<String> placeholders) {
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
