package io.tapstate.control.core;

import io.tapstate.core.model.Resource;
import io.tapstate.core.model.canonical.CanonicalHash;
import io.tapstate.core.model.canonical.CanonicalWriter;
import io.tapstate.spi.store.ArtifactStore;

import java.util.List;
import java.util.Objects;
import java.util.Optional;

/**
 * The resource-type-agnostic read side of the double-layer model: the store is the truth layer, and a
 * read returns an artifact as its canonical form straight from that layer — never from a local draft
 * (server-as-truth). {@link ApplyService} is the write side; this is its read peer, backing the artifact
 * read verbs (get / list).
 *
 * <p>The canonical form a read returns is produced by the same {@link CanonicalWriter} the offline
 * authoring path uses, so an applied artifact reads back byte-for-byte as its offline canonical form:
 * the online path reuses the one canonical contract rather than forking it. Reconstructing the stored
 * form is the store's concern; this layer only re-serializes the reconstructed resource to canonical.
 */
public final class ArtifactQueryService {

    private final ArtifactStore store;
    private final CanonicalWriter writer = new CanonicalWriter();

    public ArtifactQueryService(ArtifactStore store) {
        this.store = Objects.requireNonNull(store, "store");
    }

    /** Returns the stored artifact for the id as its canonical form, or empty when none is stored. */
    public Optional<StoredArtifact> get(String id) {
        Objects.requireNonNull(id, "id");
        return store.get(id).map(this::view);
    }

    /** Returns one typed stored resource and its canonical hash without parsing canonical text. */
    public Optional<StoredResource> getResource(String id) {
        Objects.requireNonNull(id, "id");
        return store.get(id).map(this::typedView);
    }

    /** Lists every stored artifact as its canonical form. */
    public List<StoredArtifact> list() {
        return store.list().stream().map(this::view).toList();
    }

    /** Lists typed stored resources and their canonical hashes without exposing canonical text. */
    public List<StoredResource> listResources() {
        return store.list().stream().map(this::typedView).toList();
    }

    /**
     * Lists stored artifacts of the given {@code kind} as their canonical form; a null or blank kind is
     * "no filter" and returns every artifact, the same as {@link #list()}. Read-by-kind lives here in
     * the read service so a face stays a pure projection of the verb rather than filtering results itself.
     */
    public List<StoredArtifact> list(String kind) {
        if (kind == null || kind.isBlank()) {
            return list();
        }
        return store.list().stream().filter(r -> r.kind().equals(kind)).map(this::view).toList();
    }

    private StoredArtifact view(Resource resource) {
        // The hash is taken over the very bytes this view returns, not over the resource or its id, so a
        // caller can hand it straight back as a precondition without re-deriving anything.
        String canonicalForm = writer.write(resource);
        return new StoredArtifact(
                resource.id(), resource.kind(), canonicalForm, CanonicalHash.of(canonicalForm));
    }

    private StoredResource typedView(Resource resource) {
        String canonicalForm = writer.write(resource);
        return new StoredResource(resource, CanonicalHash.of(canonicalForm));
    }
}
