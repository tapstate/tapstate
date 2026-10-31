package io.tapstate.spi.store;

import io.tapstate.core.model.SourceResource;
import java.util.Objects;

/** One stored Source read. Connector config remains privileged internal data, not a public projection. */
public record SourceBindingSnapshot(SourceResource source, String contentHash, SourceBindingIdentity identity) {
    public SourceBindingSnapshot {
        Objects.requireNonNull(source, "source"); Objects.requireNonNull(identity, "identity");
        if (!source.id().equals(identity.sourceId()) || contentHash == null || !contentHash.matches("[0-9a-f]{64}")) {
            throw new IllegalArgumentException("Source definition, logical hash and lifetime must describe the same original row");
        }
    }
}
