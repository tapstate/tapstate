package io.tapstate.control.core;

import io.tapstate.core.common.TapstateException;
import io.tapstate.core.model.PipelineResource;
import io.tapstate.core.model.Resource;
import io.tapstate.spi.store.ArtifactBatchWrite;

import java.util.Map;
import java.util.Objects;

/** Typed Pipeline projection over the generic conditional artifact write path. */
public final class PipelineProjectionService {

    private final ApplyService apply;
    private final ArtifactQueryService artifacts;
    private final PipelineRepresentation representation;
    private final PipelineViewService views;

    public PipelineProjectionService(
            ApplyService apply,
            ArtifactQueryService artifacts,
            PipelineRepresentation representation,
            PipelineViewService views) {
        this.apply = Objects.requireNonNull(apply, "apply");
        this.artifacts = Objects.requireNonNull(artifacts, "artifacts");
        this.representation = Objects.requireNonNull(representation, "representation");
        this.views = Objects.requireNonNull(views, "views");
    }

    /** Maps structured input, validates the complete workspace, then creates the Pipeline conditionally. */
    public PipelineView create(String principal, PipelineInput input) {
        Objects.requireNonNull(input, "input");
        PipelineResource pipeline = representation.toModel(input, null);
        ArtifactWriteResult result = apply.create(principal, pipeline);
        throwForWriteRefusal(result.write());
        return views.get(pipeline.id());
    }

    /** Replaces one Pipeline while its ETag still names the version the caller read. */
    public PipelineView replace(String principal, String id, String expectedContentHash, PipelineInput input) {
        Objects.requireNonNull(principal, "principal");
        Objects.requireNonNull(id, "id");
        Objects.requireNonNull(input, "input");
        requireMatchingId(id, input.id());
        requirePrecondition(id, expectedContentHash);
        StoredResource stored = artifacts.getResource(id)
                .filter(candidate -> candidate.resource() instanceof PipelineResource)
                .orElseThrow(() -> error(PipelineError.NOT_FOUND, Map.of("id", id)));
        PipelineResource replacement = representation.toModel(input, pipeline(stored.resource()));
        ArtifactWriteResult result = apply.replace(principal, replacement, expectedContentHash);
        throwForWriteRefusal(result.write());
        return views.get(id);
    }

    private static PipelineResource pipeline(Resource resource) {
        if (resource instanceof PipelineResource pipeline) {
            return pipeline;
        }
        throw new IllegalStateException("Pipeline projection received a non-Pipeline resource");
    }

    private static void throwForWriteRefusal(ArtifactBatchWrite outcome) {
        if (outcome.appliedSuccessfully()) {
            return;
        }
        PipelineError code = switch (outcome.refusal()) {
            case ALREADY_EXISTS -> PipelineError.ALREADY_EXISTS;
            case NOT_FOUND -> PipelineError.NOT_FOUND;
            case VERSION_CONFLICT -> PipelineError.VERSION_CONFLICT;
            default -> throw new IllegalStateException("unexpected Pipeline write outcome: " + outcome.refusal());
        };
        throw error(code, Map.of("id", outcome.refusedId()));
    }

    private static void requireMatchingId(String pathId, String bodyId) {
        if (bodyId == null || !pathId.equals(bodyId)) {
            throw error(PipelineError.ID_MISMATCH, Map.of("pathId", pathId, "bodyId", String.valueOf(bodyId)));
        }
    }

    private static void requirePrecondition(String id, String expectedContentHash) {
        if (expectedContentHash == null) {
            throw error(PipelineError.PRECONDITION_REQUIRED, Map.of("id", id));
        }
    }

    private static TapstateException error(PipelineError code, Map<String, Object> args) {
        return new TapstateException(code, args, null);
    }
}
