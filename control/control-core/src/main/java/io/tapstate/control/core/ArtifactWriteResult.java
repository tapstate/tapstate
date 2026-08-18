package io.tapstate.control.core;

import io.tapstate.spi.store.ArtifactBatchWrite;

import java.util.Objects;

/** The prepared typed resource and the generic conditional write outcome for it. */
public record ArtifactWriteResult(PreparedArtifact artifact, ArtifactBatchWrite write) {

    public ArtifactWriteResult {
        Objects.requireNonNull(artifact, "artifact");
        Objects.requireNonNull(write, "write");
    }
}
