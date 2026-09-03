package io.tapstate.control.core;

import java.util.Objects;

/**
 * One row in the artifact browse view. The row is allowed to be unreadable because inventory is also
 * the way an operator discovers a damaged stored document; the single-artifact get verb remains strict.
 */
public record ArtifactListEntry(
        String id, String kind, String canonicalForm, String contentHash, boolean readable) {

    public ArtifactListEntry {
        Objects.requireNonNull(id, "id");
        Objects.requireNonNull(kind, "kind");
    }
}
