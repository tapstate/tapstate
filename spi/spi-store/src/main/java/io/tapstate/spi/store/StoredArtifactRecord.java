package io.tapstate.spi.store;

import io.tapstate.core.model.Resource;
import io.tapstate.core.model.canonical.CanonicalHash;
import io.tapstate.core.model.canonical.CanonicalWriter;

import java.util.Objects;

/**
 * The storage-layer projection of one artifact-list row. Unlike {@link Resource}, this record can
 * represent a stored body that this build cannot reconstruct. That distinction is important for browse
 * reads: a damaged row remains visible without making callers pretend it is executable.
 */
public record StoredArtifactRecord(
        String id, String kind, String canonicalForm, String contentHash, boolean readable) {

    private static final CanonicalWriter WRITER = new CanonicalWriter();

    public StoredArtifactRecord {
        Objects.requireNonNull(id, "id");
        Objects.requireNonNull(kind, "kind");
    }

    /** Creates a fully readable row from a reconstructed resource. */
    public static StoredArtifactRecord of(Resource resource) {
        Objects.requireNonNull(resource, "resource");
        String canonical = WRITER.write(resource);
        return new StoredArtifactRecord(
                resource.id(), resource.kind(), canonical, CanonicalHash.of(resource), true);
    }
}
