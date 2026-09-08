package io.tapstate.control.core;

import io.tapstate.core.catalog.TapstateCatalog;
import io.tapstate.core.common.TapstateException;
import io.tapstate.core.dsl.CapabilityRules;
import io.tapstate.core.dsl.ReferenceGraph;
import io.tapstate.core.dsl.Workspace;
import io.tapstate.core.model.Resource;
import io.tapstate.core.model.SourceResource;
import io.tapstate.core.model.canonical.CanonicalHash;
import io.tapstate.core.model.canonical.CanonicalWriter;
import io.tapstate.spi.store.ArtifactMutation;
import io.tapstate.spi.store.ArtifactStore;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.function.Supplier;

/** Source-specific CRUD over the canonical artifact truth layer. */
public final class SourceService {

    private final Supplier<TapstateCatalog> catalog;
    private final ArtifactStore store;
    private final SourceRepresentation representation;

    /**
     * The live follows against each source. A delete has to reach them because nothing else
     * does: the refusals that guard it read the artifact graph, and a watcher is not in the
     * graph -- so a source that exists only to be read passes both and its stream outlives it.
     */
    private final DataBrowserFollows follows;

    /**
     * The reading of which pipelines are up, or null when the caller supplied none.
     *
     * <p>Null is a real answer rather than an oversight: a service built to validate and render Sources
     * has no lifecycle to consult, and the one refusal that needs it says so where it is made. The
     * production assembly always supplies one.
     */
    private final LivePipelines live;

    private final CanonicalWriter writer = new CanonicalWriter();

    public SourceService(
            TapstateCatalog catalog, ArtifactStore store, SourceRepresentation representation,
            DataBrowserFollows follows) {
        this(() -> Objects.requireNonNull(catalog, "catalog"), store, representation, follows, null);
    }

    public SourceService(
            Supplier<TapstateCatalog> catalog, ArtifactStore store, SourceRepresentation representation,
            DataBrowserFollows follows) {
        this(catalog, store, representation, follows, null);
    }

    public SourceService(
            TapstateCatalog catalog, ArtifactStore store, SourceRepresentation representation,
            DataBrowserFollows follows, LivePipelines live) {
        this(() -> Objects.requireNonNull(catalog, "catalog"), store, representation, follows, live);
    }

    public SourceService(
            Supplier<TapstateCatalog> catalog, ArtifactStore store, SourceRepresentation representation,
            DataBrowserFollows follows, LivePipelines live) {
        this.catalog = Objects.requireNonNull(catalog, "catalog");
        this.store = Objects.requireNonNull(store, "store");
        this.representation = Objects.requireNonNull(representation, "representation");
        this.follows = Objects.requireNonNull(follows, "follows");
        this.live = live;
    }

    /** Validates one Source against the live connector contract and renders canonical YAML without writing. */
    public SourceDraftResult draft(SourceDraft draft) {
        Objects.requireNonNull(draft, "draft");
        SourceResource source = representation.toModel(draft, null);
        TapstateCatalog liveCatalog = catalog.get();
        Workspace.of(List.of(source), liveCatalog);
        CapabilityRules.validateOnline(source, liveCatalog);
        return new SourceDraftResult(writer.write(source));
    }

    /** Lists stored Sources ordered by id. */
    public List<SourceView> list() {
        return store.list().stream()
                .filter(SourceResource.class::isInstance)
                .map(SourceResource.class::cast)
                .sorted(Comparator.comparing(SourceResource::id))
                .map(this::view)
                .toList();
    }

    /** Returns one stored Source or a coded not-found failure. */
    public SourceView get(String id) {
        Objects.requireNonNull(id, "id");
        return view(storedSource(id));
    }

    /** Creates one Source only when its id is not already present. */
    public SourceView create(SourceDraft draft) {
        Objects.requireNonNull(draft, "draft");
        List<Resource> candidate = new ArrayList<>(store.list());
        if (candidate.stream().anyMatch(resource -> resource.id().equals(draft.id()))) {
            throw error(SourceError.ALREADY_EXISTS, Map.of("id", draft.id()));
        }

        SourceResource source = representation.toModel(draft, null);
        candidate.add(source);
        TapstateCatalog liveCatalog = catalog.get();
        Workspace.of(candidate, liveCatalog);
        CapabilityRules.validateOnline(source, liveCatalog);

        return switch (store.create(source)) {
            case CREATED -> view(source);
            case ALREADY_EXISTS -> throw error(
                    SourceError.ALREADY_EXISTS, Map.of("id", source.id()));
            default -> throw unexpectedMutation("create");
        };
    }

    /** Replaces one existing Source when the supplied canonical content hash is current. */
    public SourceView replace(String id, String expectedContentHash, SourceDraft draft) {
        Objects.requireNonNull(id, "id");
        Objects.requireNonNull(draft, "draft");
        if (!id.equals(draft.id())) {
            throw error(
                    SourceError.ID_MISMATCH,
                    Map.of("pathId", id, "bodyId", draft.id()));
        }
        requirePrecondition(id, expectedContentHash);

        List<Resource> candidate = new ArrayList<>(store.list());
        SourceResource existing = candidate.stream()
                .filter(resource -> resource.id().equals(id))
                .filter(SourceResource.class::isInstance)
                .map(SourceResource.class::cast)
                .findFirst()
                .orElseThrow(() -> error(SourceError.NOT_FOUND, Map.of("id", id)));
        SourceResource replacement = representation.toModel(draft, existing);
        for (int index = 0; index < candidate.size(); index++) {
            if (candidate.get(index) == existing) {
                candidate.set(index, replacement);
                break;
            }
        }
        TapstateCatalog liveCatalog = catalog.get();
        Workspace.of(candidate, liveCatalog);
        CapabilityRules.validateOnline(replacement, liveCatalog);
        // Judged after the draft is known to be well formed and before anything is written: an author
        // whose edit is also invalid is told that rather than being told about lifecycle state they
        // would then have to fix a second time.
        if (live != null) {
            live.refuseBufferingChangeWhileLive(existing, replacement, candidate);
        }

        return switch (store.replace(id, expectedContentHash, replacement)) {
            case REPLACED -> view(replacement);
            case NOT_FOUND -> throw error(SourceError.NOT_FOUND, Map.of("id", id));
            case VERSION_CONFLICT -> throw error(
                    SourceError.VERSION_CONFLICT, Map.of("id", id));
            default -> throw unexpectedMutation("replace");
        };
    }

    /** Deletes one unreferenced Source when the supplied canonical content hash is current. */
    public void delete(String id, String expectedContentHash) {
        Objects.requireNonNull(id, "id");
        requirePrecondition(id, expectedContentHash);

        List<Resource> stored = store.list();
        boolean sourceExists = stored.stream()
                .anyMatch(resource -> resource.id().equals(id) && resource instanceof SourceResource);
        if (!sourceExists) {
            throw error(SourceError.NOT_FOUND, Map.of("id", id));
        }
        List<String> referrers = ReferenceGraph.of(stored).referencedBy(id).stream()
                .map(ReferenceGraph.Edge::id)
                .sorted()
                .toList();
        if (!referrers.isEmpty()) {
            throw error(
                    SourceError.IN_USE,
                    Map.of("id", id, "referrers", referrers));
        }

        switch (store.delete(id, expectedContentHash)) {
            case DELETED ->
                // After the delete, never before. A delete that was refused -- because the source is
                // referenced, or the caller's precondition did not hold -- must leave its streams
                // running: the refusals are what decide whether the source is going away.
                    follows.closeFollowsOf(id);
            case NOT_FOUND -> throw error(SourceError.NOT_FOUND, Map.of("id", id));
            case VERSION_CONFLICT -> throw error(
                    SourceError.VERSION_CONFLICT, Map.of("id", id));
            default -> throw unexpectedMutation("delete");
        }
    }

    private SourceResource storedSource(String id) {
        return store.get(id)
                .filter(SourceResource.class::isInstance)
                .map(SourceResource.class::cast)
                .orElseThrow(() -> error(SourceError.NOT_FOUND, Map.of("id", id)));
    }

    private SourceView view(SourceResource source) {
        String contentHash = CanonicalHash.of(writer.write(source));
        return representation.toView(source, contentHash);
    }

    private static void requirePrecondition(String id, String expectedContentHash) {
        if (expectedContentHash == null) {
            throw error(SourceError.PRECONDITION_REQUIRED, Map.of("id", id));
        }
    }

    private static TapstateException error(SourceError code, Map<String, Object> args) {
        return new TapstateException(code, args, null);
    }

    private static IllegalStateException unexpectedMutation(String operation) {
        return new IllegalStateException("unexpected artifact mutation outcome for " + operation);
    }
}
