package io.tapstate.control.core;

import io.tapstate.core.common.Severity;
import io.tapstate.core.common.TapstateErrorCode;

import java.util.Set;

/** Stable coded failures for structured Pipeline artifact reads. */
public enum PipelineError implements TapstateErrorCode {

    NOT_FOUND("pipeline.not-found", Set.of("id"));

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
