package io.tapstate.control.core;

import java.util.Objects;

/**
 * One authored resource document submitted for apply: its raw YAML text plus an origin label used
 * only to attribute a parse error back to where it came from (e.g. a filename). The server re-parses
 * and re-validates the raw text — it never trusts a client-parsed model — which is why a draft
 * carries text, not a resource. A {@code null} source means an unnamed origin (the diagnostic then
 * carries no source label).
 *
 * <p>{@code expectedContentHash} is an optional precondition: the content hash of the stored version
 * this edit was written against. Supplied, the apply is refused unless that is still the stored
 * version, so two authors editing the same resource cannot silently overwrite each other. Omitted,
 * a full replacement keeps the original unconditional-write behavior. A partial Source draft is an
 * exception: omitted top-level fields are copied from the stored Source and guarded by its version.
 */
public record ArtifactDraft(String source, String content, String expectedContentHash) {

    public ArtifactDraft {
        Objects.requireNonNull(content, "draft content");
    }

    /** A draft submitted with no caller-declared precondition. */
    public ArtifactDraft(String source, String content) {
        this(source, content, null);
    }
}
