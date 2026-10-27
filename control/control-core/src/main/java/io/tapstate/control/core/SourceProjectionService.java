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
        var managed = artifacts.managedDefaultSourceId();
        return artifacts.listResources().stream()
                .filter(stored -> stored.resource() instanceof SourceResource)
                .sorted(Comparator.comparing(stored -> stored.resource().id()))
                .map(stored -> view(stored, managed))
                .toList();
    }

    /** Returns a Source typed view or the stable Source not-found diagnostic. */
    public SourceView get(String id) {
        Objects.requireNonNull(id, "id");
        return view(requireSource(id), artifacts.managedDefaultSourceId());
    }

    /** Maps structured input, plans the full artifact workspace, then creates the Source conditionally. */
    public SourceView create(String principal, SourceInput input) {
        Objects.requireNonNull(input, "input");
        refuseManaged(input.id());
        refuseStoreMarker(input.metadata());
        SourceResource source = representation.toModel(input, null);
        ArtifactWriteResult result = apply.create(principal, source, ControlOperations.SOURCE_CREATE);
        throwForWriteRefusal(source.id(), result.write());
        return representation.toView(source(result.artifact().resource()), result.artifact().contentHash());
    }

    /** Maps structured input, plans the full artifact workspace, then replaces the Source conditionally. */
    public SourceView replace(String principal, String id, String expectedContentHash, SourceInput input) {
        Objects.requireNonNull(id, "id");
        Objects.requireNonNull(input, "input");
        requireMatchingId(id, input.id());
        refuseManaged(input.id());
        refuseStoreMarker(input.metadata());
        requirePrecondition(id, expectedContentHash);
        SourceResource previous = source(requireSource(id).resource());
        refuseStoreMarker(previous.metadata());
        SourceResource replacement = representation.toModel(input, previous);
        ArtifactWriteResult result = apply.replace(
                principal, replacement, expectedContentHash, ControlOperations.SOURCE_UPDATE);
        throwForWriteRefusal(id, result.write());
        return representation.toView(source(result.artifact().resource()), result.artifact().contentHash());
    }

    /** Deletes a Source through the generic artifact delete path and reprojects its public diagnostics. */
    public void delete(String principal, String id, String expectedContentHash) {
        Objects.requireNonNull(id, "id");
        requirePrecondition(id, expectedContentHash);
        refuseManaged(id);
        refuseStoreMarker(source(requireSource(id).resource()).metadata());
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

    private void refuseManaged(String id) {
        if (artifacts.managedDefaultSourceId().filter(id::equals).isPresent())
            throw new TapstateException(ControlError.MALFORMED_REQUEST,
                    Map.of("reason", "the managed default target cannot be changed through authoring"), null);
    }
    private SourceView view(StoredResource stored, java.util.Optional<String> managed) {
        var resource = source(stored.resource());
        if (managed.isPresent()) {
            var metadata = resource.metadata();
            var labels = new java.util.LinkedHashMap<String,String>(metadata == null ? Map.of() : metadata.labels());
            labels.remove("store");
            if (managed.orElseThrow().equals(resource.id())) labels.put("store", "true");
            var display = new io.tapstate.core.model.Metadata(labels, metadata == null ? null : metadata.description(),
                    metadata == null ? null : metadata.cloud(), metadata == null ? null : metadata.userId());
            resource = new SourceResource(resource.id(), display, resource.connector(), resource.config(), resource.mode(),
                    resource.tables(), resource.srs(), resource.execution(), resource.experimental());
        }
        return representation.toView(resource, stored.contentHash());
    }

    private static SourceResource source(Resource resource) {
        if (resource instanceof SourceResource source) {
            return source;
        }
        throw new IllegalStateException("Source projection received a non-Source resource");
    }

    static void throwForWriteRefusal(String targetId, ArtifactBatchWrite outcome) {
        if (outcome.appliedSuccessfully()) {
            return;
        }
        if (outcome.refusal() == ArtifactMutation.VERSION_CONFLICT && !targetId.equals(outcome.refusedId())) {
            throw new TapstateException(
                    ArtifactError.VERSION_CONFLICT, Map.of("id", outcome.refusedId()), null);
        }
        SourceError code = switch (outcome.refusal()) {
            case ALREADY_EXISTS -> SourceError.ALREADY_EXISTS;
            case NOT_FOUND -> SourceError.NOT_FOUND;
            case VERSION_CONFLICT -> SourceError.VERSION_CONFLICT;
            default -> throw new IllegalStateException("unexpected Source write outcome: " + outcome.refusal());
        };
        throw error(code, Map.of("id", targetId));
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

    private static void refuseStoreMarker(io.tapstate.core.model.Metadata metadata) {
        if (metadata != null && "true".equals(metadata.labels().get("store"))) {
            throw new TapstateException(ControlError.MALFORMED_REQUEST,
                    Map.of("reason", "the state store must be configured through its dedicated setup API"), null);
        }
    }
}
