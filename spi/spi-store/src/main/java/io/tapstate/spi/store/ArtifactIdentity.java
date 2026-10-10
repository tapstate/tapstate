package io.tapstate.spi.store;

import java.util.Objects;

/** Durable resource incarnation, preserved across edits and replaced only by delete followed by create. */
public record ArtifactIdentity(String id, String incarnation, String contentHash) {
    public ArtifactIdentity {
        Objects.requireNonNull(id, "id");
        Objects.requireNonNull(incarnation, "incarnation");
        Objects.requireNonNull(contentHash, "contentHash");
        if (id.isBlank() || incarnation.isBlank() || contentHash.isBlank()) {
            throw new IllegalArgumentException("artifact identity fields must be nonblank");
        }
    }
}
