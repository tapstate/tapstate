package io.tapstate.control.core;

import io.tapstate.core.common.TapstateErrorCode;
import io.tapstate.core.common.Severity;
import io.tapstate.core.common.ProductErrorDefinition;
import io.tapstate.core.common.SourceErrorDefinitions;

import java.util.Set;

/** Stable coded failures for Source-specific CRUD operations. */
public enum SourceError implements TapstateErrorCode {

    NOT_FOUND(SourceErrorDefinitions.NOT_FOUND),
    ALREADY_EXISTS(SourceErrorDefinitions.ALREADY_EXISTS),
    ID_MISMATCH(SourceErrorDefinitions.ID_MISMATCH),
    PRECONDITION_REQUIRED(SourceErrorDefinitions.PRECONDITION_REQUIRED),
    VERSION_CONFLICT(SourceErrorDefinitions.VERSION_CONFLICT),
    IN_USE(SourceErrorDefinitions.IN_USE),
    SRS_CHANGE_WHILE_RUNNING(SourceErrorDefinitions.SRS_CHANGE_WHILE_RUNNING);

    private final ProductErrorDefinition definition;

    SourceError(ProductErrorDefinition definition) {
        this.definition = definition;
    }

    @Override
    public String code() {
        return definition.code();
    }

    @Override
    public Severity severity() {
        return definition.severity();
    }

    @Override
    public Set<String> placeholders() {
        return definition.parameters();
    }
}
