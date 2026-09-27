package io.tapstate.control.restapi;

import io.tapstate.control.core.ArtifactDraft;

import java.util.List;

/**
 * The apply request body: the batch of authored drafts to apply as one unit. Each draft carries its raw
 * YAML text (and an optional source label), which the server re-parses and re-validates — it never
 * trusts a client-parsed model. A wrapper object rather than a bare array so the request can grow
 * fields (dry-run, options) without changing the wire shape.
 *
 * <p>{@code project}, when present, is the project the batch is applied from: every resource in it is
 * labelled as that project's, and an id another project already owns on this server is refused.
 */
public record ApplyRequest(List<ArtifactDraft> drafts, String project) {

    /** A request that names no project: the batch is applied as written, with no project label. */
    public ApplyRequest(List<ArtifactDraft> drafts) {
        this(drafts, null);
    }
}
