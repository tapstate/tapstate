package io.tapstate.control.core;

import io.tapstate.core.common.TapstateErrorCode;
import io.tapstate.core.common.Severity;

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

    NOT_FOUND("artifact.not-found", Set.of("id")),
    PRECONDITION_REQUIRED("artifact.precondition-required", Set.of("id")),
    VERSION_CONFLICT("artifact.version-conflict", Set.of("id")),
    IN_USE("artifact.in-use", Set.of("id", "referrers")),
    PIPELINE_NOT_STOPPED("artifact.pipeline-not-stopped", Set.of("id", "actual", "desired")),
    /**
     * The artifact was removed and some of its dependent bookkeeping was not. Distinct from the four
     * above because it is not a refusal: those mean nothing happened and the caller may retry, this one
     * means the removal stands and retrying it can only ever answer {@code artifact.not-found}. Sharing
     * a code with a refusal would make a partly-executed removal indistinguishable from one that never
     * started, on every face.
     */
    RECLAIM_INCOMPLETE("artifact.reclaim-incomplete", Set.of("id", "reason", "residue")),
    /**
     * A project applied a resource whose id another project on this server already owns. {@code owner}
     * is the project holding it, {@code project} the one that was refused. Resource ids are unique per
     * server, so the only ways forward are a different id or removing the resource from its owner.
     */
    PROJECT_ID_TAKEN("artifact.project-id-taken", Set.of("id", "kind", "owner", "project")),
    /**
     * Not a refusal: a resource that was stored with no project now belongs to the project that just
     * applied it. Said out loud because it is the one moment ownership changes without anybody having
     * written a project name, and an adoption nobody was told about is indistinguishable from a
     * resource that was always this project's.
     */
    PROJECT_CLAIMED("artifact.project-claimed", Set.of("id", "kind", "project"), Severity.WARNING),
    /**
     * A removal of the Default project. Every server has it and every unlabelled resource belongs to it,
     * so it can be emptied - its resources deleted one by one - but never removed as a whole.
     */
    DEFAULT_PROJECT_NOT_REMOVABLE("artifact.default-project-not-removable", Set.of("project"));

    private final String code;
    private final Set<String> placeholders;
    private final Severity severity;

    ArtifactError(String code, Set<String> placeholders) {
        this(code, placeholders, Severity.ERROR);
    }

    ArtifactError(String code, Set<String> placeholders, Severity severity) {
        this.code = code;
        this.placeholders = placeholders;
        this.severity = severity;
    }

    @Override
    public String code() {
        return code;
    }

    @Override
    public Severity severity() {
        return severity;
    }

    @Override
    public Set<String> placeholders() {
        return placeholders;
    }
}
