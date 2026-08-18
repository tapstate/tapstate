package io.tapstate.control.core;

import io.tapstate.core.common.TapstateException;
import io.tapstate.core.model.Resource;
import io.tapstate.core.model.SourceResource;
import io.tapstate.spi.store.ArtifactBatchWrite;
import io.tapstate.spi.store.ArtifactMutation;

import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/** Typed Source projection over the generic artifact query and mutation paths. */
public final class SourceProjectionService {

    private final ApplyService apply;
    private final ArtifactQueryService artifacts;
    private final ArtifactMutationService mutations;
    private final SourceRepresentation representation;

    public SourceProjectionService(
            ApplyService apply,
            ArtifactQueryService artifacts,
            ArtifactMutationService mutations,
            SourceRepresentation representation) {
        this.apply = Objects.requireNonNull(apply, "apply");
        this.artifacts = Objects.requireNonNull(artifacts, "artifacts");
        this.mutations = Objects.requireNonNull(mutations, "mutations");
        this.representation = Objects.requireNonNull(representation, "representation");
    }

    /** Lists stored Sources in stable id order. */
    public List<SourceView> list() {
        return artifacts.listResources().stream()
                .filter(stored -> stored.resource() instanceof SourceResource)
                .sorted(Comparator.comparing(stored -> stored.resource().id()))
                .map(this::view)
                .toList();
    }

    /** Returns a Source typed view or the stable Source not-found diagnostic. */
    public SourceView get(String id) {
        Objects.requireNonNull(id, "id");
        return view(requireSource(id));
    }

    /** Maps structured input, plans the full artifact workspace, then creates the Source conditionally. */
    public SourceView create(String principal, SourceInput input) {
        Objects.requireNonNull(input, "input");
        SourceResource source = representation.toModel(input, null);
        ArtifactWriteResult result = apply.create(principal, source);
        throwForWriteRefusal(result.write());
        return representation.toView(source(result.artifact().resource()), result.artifact().contentHash());
    }

    /** Maps structured input, plans the full artifact workspace, then replaces the Source conditionally. */
    public SourceView replace(String principal, String id, String expectedContentHash, SourceInput input) {
        Objects.requireNonNull(id, "id");
        Objects.requireNonNull(input, "input");
        requireMatchingId(id, input.id());
        requirePrecondition(id, expectedContentHash);
        SourceResource replacement = representation.toModel(input, source(requireSource(id).resource()));
        ArtifactWriteResult result = apply.replace(principal, replacement, expectedContentHash);
        throwForWriteRefusal(result.write());
        return representation.toView(source(result.artifact().resource()), result.artifact().contentHash());
    }

    /** Deletes a Source through the generic artifact delete path and reprojects its public diagnostics. */
    public void delete(String principal, String id, String expectedContentHash) {
        Objects.requireNonNull(id, "id");
        requirePrecondition(id, expectedContentHash);
        requireSource(id);
        try {
            mutations.delete(principal, id, expectedContentHash);
        } catch (TapstateException error) {
            throw sourceError(error);
        }
    }

    private StoredResource requireSource(String id) {
        return artifacts.getResource(id)
                .filter(stored -> stored.resource() instanceof SourceResource)
                .orElseThrow(() -> error(SourceError.NOT_FOUND, Map.of("id", id)));
    }

    private SourceView view(StoredResource stored) {
        return representation.toView(source(stored.resource()), stored.contentHash());
    }

    private static SourceResource source(Resource resource) {
        if (resource instanceof SourceResource source) {
            return source;
        }
        throw new IllegalStateException("Source projection received a non-Source resource");
    }

    private static void throwForWriteRefusal(ArtifactBatchWrite outcome) {
        if (outcome.appliedSuccessfully()) {
            return;
        }
        SourceError code = switch (outcome.refusal()) {
            case ALREADY_EXISTS -> SourceError.ALREADY_EXISTS;
            case NOT_FOUND -> SourceError.NOT_FOUND;
            case VERSION_CONFLICT -> SourceError.VERSION_CONFLICT;
            default -> throw new IllegalStateException("unexpected Source write outcome: " + outcome.refusal());
        };
        throw error(code, Map.of("id", outcome.refusedId()));
    }

    private static TapstateException sourceError(TapstateException error) {
        if (error.code() == ArtifactError.NOT_FOUND) {
            return error(SourceError.NOT_FOUND, error.args());
        }
        if (error.code() == ArtifactError.PRECONDITION_REQUIRED) {
            return error(SourceError.PRECONDITION_REQUIRED, error.args());
        }
        if (error.code() == ArtifactError.VERSION_CONFLICT) {
            return error(SourceError.VERSION_CONFLICT, error.args());
        }
        if (error.code() == ArtifactError.IN_USE) {
            return error(SourceError.IN_USE, error.args());
        }
        return error;
    }

    private static void requireMatchingId(String pathId, String bodyId) {
        if (!pathId.equals(bodyId)) {
            throw error(SourceError.ID_MISMATCH, Map.of("pathId", pathId, "bodyId", bodyId));
        }
    }

    private static void requirePrecondition(String id, String expectedContentHash) {
        if (expectedContentHash == null) {
            throw error(SourceError.PRECONDITION_REQUIRED, Map.of("id", id));
        }
    }

    private static TapstateException error(SourceError code, Map<String, Object> args) {
        return new TapstateException(code, args, null);
    }
}
