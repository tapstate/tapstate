package io.tapstate.control.core;

import io.tapstate.core.model.Resource;
import io.tapstate.core.model.SourceResource;
import io.tapstate.core.model.canonical.AssemblyIdentity;
import io.tapstate.core.model.canonical.CanonicalHash;
import io.tapstate.core.model.canonical.CanonicalWriter;
import io.tapstate.spi.store.ArtifactStore;
import io.tapstate.spi.store.StoredArtifactRecord;
import io.tapstate.spi.store.RateHistoryStore;

import java.util.List;
import java.util.Objects;
import java.util.Optional;

/**
 * The resource-type-agnostic read side of the double-layer model: the store is the truth layer, and a
 * read returns an artifact from that layer — never from a local draft (server-as-truth). Public Source
 * reads redact Mongo URI userinfo while their stored resource, content hash, and typed internal reads
 * remain unchanged. {@link ApplyService} is the write side; this is its read peer.
 *
 * <p>Non-sensitive artifacts retain the byte-stable canonical form produced by the same {@link
 * CanonicalWriter} as offline authoring. A Source projection starts from that form and replaces only
 * Mongo URI userinfo; it is deliberately display-only and keeps the authoritative hash beside it.
 */
public final class ArtifactQueryService {

    private final ArtifactStore store;
    private final CanonicalWriter writer = new CanonicalWriter();
    private final SourceReadProjection sourceProjection = new SourceReadProjection();

    public ArtifactQueryService(ArtifactStore store) {
        this.store = Objects.requireNonNull(store, "store");
    }

    /** Returns the stored artifact for the id as its public canonical form, or empty when none is stored. */
    public Optional<StoredArtifact> get(String id) {
        Objects.requireNonNull(id, "id");
        return store.get(id).map(this::view);
    }

    /**
     * What the stored artifact's run would be assembled from, or empty when none is stored.
     *
     * <p>Read here rather than derived from {@link #get}: the reading needs the resource, and the view
     * a read returns has already been flattened to text. Both readings come off the same writer, which
     * is what keeps "what changed" and "what it hashes to" from drifting apart.
     */
    public Optional<String> assemblyIdentityOf(String id) {
        Objects.requireNonNull(id, "id");
        return store.get(id).map(AssemblyIdentity::of);
    }

    /** Lists every stored artifact, retaining rows whose stored body is unreadable. */
    public List<ArtifactListEntry> list() {
        return store.listStored().stream().map(this::view).toList();
    }

    /** Returns one typed stored resource and its canonical hash without parsing canonical text. */
    public Optional<StoredResource> getResource(String id) {
        Objects.requireNonNull(id, "id");
        return store.get(id).map(this::typedView);
    }

    /** The current artifact's internal history owner; absent for a missing or non-pipeline resource. */
    public Optional<RateHistoryStore.Visibility> historyVisibilityOf(String id) {
        Objects.requireNonNull(id, "id");
        return store.pipelineHistoryOwner(id).map(ArtifactStore.HistoryOwner::visibility);
    }

    /** Lists typed stored resources and their canonical hashes without exposing canonical text. */
    public List<StoredResource> listResources() {
        return ReadableArtifactInventory.list(store).stream().map(this::typedView).toList();
    }

    /**
     * Lists stored artifacts of the given {@code kind} as their canonical form; a null or blank kind is
     * "no filter" and returns every artifact, the same as {@link #list()}. Read-by-kind lives here in
     * the read service so a face stays a pure projection of the verb rather than filtering results itself.
     */
    public List<ArtifactListEntry> list(String kind) {
        if (kind == null || kind.isBlank()) {
            return list();
        }
        return store.listStored(kind).stream().map(this::view).toList();
    }

    private StoredArtifact view(Resource resource) {
        // The hash comes back beside the canonical form rather than being derivable from it: it is taken
        // over the resource's structure, so a caller holding only these bytes cannot recompute it and
        // must hand this field straight back as a precondition.
        String canonical = resource instanceof SourceResource source
                ? sourceProjection.canonicalForRead(source) : writer.write(resource);
        return new StoredArtifact(resource.id(), resource.kind(), canonical, CanonicalHash.of(resource));
    }

    private ArtifactListEntry view(StoredArtifactRecord row) {
        String canonical = row.canonicalForm();
        if ("source".equals(row.kind()) && canonical != null) {
            canonical = sourceProjection.canonicalForRead(canonical);
        }
        return new ArtifactListEntry(
                row.id(), row.kind(), canonical, row.contentHash(), row.readable());
    }

    private StoredResource typedView(Resource resource) {
        return new StoredResource(resource, CanonicalHash.of(resource));
    }
}
