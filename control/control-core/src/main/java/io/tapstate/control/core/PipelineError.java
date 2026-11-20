package io.tapstate.control.core;

import io.tapstate.core.common.Severity;
import io.tapstate.core.common.TapstateErrorCode;
import io.tapstate.core.common.ProductErrorDefinition;
import io.tapstate.core.common.PipelineErrorDefinitions;

import java.util.Set;

/** Stable coded failures for structured Pipeline artifact reads and conditional edits. */
public enum PipelineError implements TapstateErrorCode {

    ALREADY_EXISTS(PipelineErrorDefinitions.ALREADY_EXISTS),
    NOT_FOUND(PipelineErrorDefinitions.NOT_FOUND),
    ID_MISMATCH(PipelineErrorDefinitions.ID_MISMATCH),
    PRECONDITION_REQUIRED(PipelineErrorDefinitions.PRECONDITION_REQUIRED),
    VERSION_CONFLICT(PipelineErrorDefinitions.VERSION_CONFLICT),
    START_CHANGE_NOT_APPLIED(PipelineErrorDefinitions.START_CHANGE_NOT_APPLIED),
    DEFINITION_CHANGE_UNAVAILABLE(PipelineErrorDefinitions.DEFINITION_CHANGE_UNAVAILABLE);

    private final String code;
    private final Set<String> placeholders;

    PipelineError(ProductErrorDefinition definition) {
        this.code = definition.code();
        this.placeholders = definition.parameters();
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
