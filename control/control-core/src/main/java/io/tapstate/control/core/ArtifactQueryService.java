package io.tapstate.control.core;

import io.tapstate.core.catalog.TapstateCatalog;
import io.tapstate.core.dsl.DslParser;
import io.tapstate.core.model.Resource;
import io.tapstate.core.model.SourceResource;
import io.tapstate.core.model.canonical.AssemblyIdentity;
import io.tapstate.core.model.canonical.CanonicalHash;
import io.tapstate.core.model.canonical.CanonicalWriter;
import io.tapstate.spi.store.ArtifactStore;
import io.tapstate.spi.store.StoredArtifactRecord;

import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.function.Supplier;

/**
 * The resource-type-agnostic read side of the double-layer model: the store is the truth layer, and a
 * read returns an artifact from that layer — never from a local draft (server-as-truth). Public reads of
 * credential-bearing Sources are a non-replayable redacted projection; their stored resource, content hash,
 * and typed internal reads are unchanged. {@link ApplyService} is the write side; this is its read peer.
 *
 * <p>The canonical form a read returns is produced by the same {@link CanonicalWriter} the offline
 * authoring path uses, so an applied artifact reads back byte-for-byte as its offline canonical form:
 * the online path reuses the one canonical contract rather than forking it. Reconstructing the stored
 * form is the store's concern; this layer only re-serializes the reconstructed resource to canonical.
 */
public final class ArtifactQueryService {

    private final ArtifactStore store;
    private final CanonicalWriter writer = new CanonicalWriter();
    private final DslParser parser = new DslParser();
    private final SourceReadProjection sourceProjection;

    public ArtifactQueryService(ArtifactStore store) {
        this(store, TapstateCatalog::load);
    }

    public ArtifactQueryService(ArtifactStore store, Supplier<TapstateCatalog> catalog) {
        this.store = Objects.requireNonNull(store, "store");
        this.sourceProjection = new SourceReadProjection(catalog);
    }

    /** Returns the stored artifact for the id as its canonical form, or empty when none is stored. */
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
        return views(store.listStored());
    }

    /** Returns one typed stored resource and its canonical hash without parsing canonical text. */
    public Optional<StoredResource> getResource(String id) {
        Objects.requireNonNull(id, "id");
        return store.get(id).map(this::typedView);
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
        return views(store.listStored(kind));
    }

    private List<ArtifactListEntry> views(List<StoredArtifactRecord> rows) {
        // The online catalog may read registered rows from a store; take one snapshot for the inventory,
        // not one catalog read per Source, while still withholding Source bodies if it is unavailable.
        TapstateCatalog catalog = rows.stream().anyMatch(row -> "source".equals(row.kind()))
                ? sourceProjection.catalogSnapshot() : null;
        return rows.stream().map(row -> view(row, catalog)).toList();
    }

    private StoredArtifact view(Resource resource) {
        // The hash comes back beside the canonical form rather than being derivable from it: it is taken
        // over the resource's structure, so a caller holding only these bytes cannot recompute it and
        // must hand this field straight back as a precondition.
        String canonical = writer.write(resource);
        if (resource instanceof SourceResource source) {
            canonical = sourceProjection.canonicalForRead(source, canonical);
        }
        return new StoredArtifact(resource.id(), resource.kind(), canonical, CanonicalHash.of(resource));
    }

    private ArtifactListEntry view(StoredArtifactRecord row, TapstateCatalog catalog) {
        String canonical = row.canonicalForm();
        if ("source".equals(row.kind())) {
            if (!row.readable() || canonical == null) {
                canonical = SourceReadProjection.WITHHELD;
            } else {
                try {
                    Resource parsed = parser.parse(canonical);
                    canonical = parsed instanceof SourceResource source
                            ? sourceProjection.canonicalForRead(source, canonical, catalog)
                            : SourceReadProjection.WITHHELD;
                } catch (RuntimeException unsafeSource) {
                    canonical = SourceReadProjection.WITHHELD;
                }
            }
        }
        return new ArtifactListEntry(row.id(), row.kind(), canonical, row.contentHash(), row.readable());
    }

    private StoredResource typedView(Resource resource) {
        return new StoredResource(resource, CanonicalHash.of(resource));
    }
}
