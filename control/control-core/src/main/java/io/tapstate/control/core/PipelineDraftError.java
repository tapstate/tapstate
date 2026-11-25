package io.tapstate.control.core;

import io.tapstate.core.common.Severity;
import io.tapstate.core.common.TapstateErrorCode;
import io.tapstate.core.common.ProductErrorDefinition;
import io.tapstate.core.common.PipelineDraftErrorDefinitions;

import java.util.Set;

/** Stable client-attributable failures for the durable Pipeline draft HTTP surface. */
public enum PipelineDraftError implements TapstateErrorCode {

    NOT_FOUND(PipelineDraftErrorDefinitions.NOT_FOUND),
    ALREADY_EXISTS(PipelineDraftErrorDefinitions.ALREADY_EXISTS),
    PRECONDITION_REQUIRED(PipelineDraftErrorDefinitions.PRECONDITION_REQUIRED),
    REVISION_CONFLICT(PipelineDraftErrorDefinitions.REVISION_CONFLICT),
    INVALID(PipelineDraftErrorDefinitions.INVALID),
    MODE_CONFLICT(PipelineDraftErrorDefinitions.MODE_CONFLICT),
    ARTIFACT_CONFLICT(PipelineDraftErrorDefinitions.ARTIFACT_CONFLICT);

    private final String code;
    private final Set<String> placeholders;

    PipelineDraftError(ProductErrorDefinition definition) {
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
