package io.tapstate.control.core;

import io.tapstate.core.common.TapstateErrorCode;
import io.tapstate.core.common.Severity;
import io.tapstate.core.common.ProductErrorDefinition;
import io.tapstate.core.common.ArtifactErrorDefinitions;

import java.util.Set;

/**
 * Stable coded failures for the resource-type-agnostic artifact edit and delete face. Every resource
 * kind travels this one vocabulary, so a caller handles five codes rather than one set per kind.
 *
 * <p>These codes and their placeholder names are an external contract on every face that exposes the
 * verbs — a human CLI, a scripted HTTP client and a tool-calling agent all branch on them. Adding a
 * code is compatible; renaming or removing one, or renaming a placeholder, is a breaking change.
 */
public enum ArtifactError implements TapstateErrorCode {

    NOT_FOUND(ArtifactErrorDefinitions.NOT_FOUND),
    PRECONDITION_REQUIRED(ArtifactErrorDefinitions.PRECONDITION_REQUIRED),
    VERSION_CONFLICT(ArtifactErrorDefinitions.VERSION_CONFLICT),
    IN_USE(ArtifactErrorDefinitions.IN_USE),
    PIPELINE_NOT_STOPPED(ArtifactErrorDefinitions.PIPELINE_NOT_STOPPED),
    /**
     * The artifact was removed and some of its dependent bookkeeping was not. Distinct from the four
     * above because it is not a refusal: those mean nothing happened and the caller may retry, this one
     * means the removal stands and retrying it can only ever answer {@code artifact.not-found}. Sharing
     * a code with a refusal would make a partly-executed removal indistinguishable from one that never
     * started, on every face.
     */
    RECLAIM_INCOMPLETE(ArtifactErrorDefinitions.RECLAIM_INCOMPLETE);

    private final String code;
    private final Set<String> placeholders;

    ArtifactError(ProductErrorDefinition definition) {
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
