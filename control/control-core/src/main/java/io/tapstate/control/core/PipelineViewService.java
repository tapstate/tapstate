package io.tapstate.control.core;

import io.tapstate.core.model.PipelineResource;
import io.tapstate.core.model.Resource;
import io.tapstate.core.model.SourceResource;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Objects;
import java.util.Optional;

/** Read-only Pipeline projection over the generic artifact query path. */
public final class PipelineViewService {

    private final ArtifactQueryService artifacts;
    private final PipelineRepresentation representation;

    public PipelineViewService(ArtifactQueryService artifacts, PipelineRepresentation representation) {
        this.artifacts = Objects.requireNonNull(artifacts, "artifacts");
        this.representation = Objects.requireNonNull(representation, "representation");
    }

    /** Lists stored Pipelines in stable id order. */
    public List<PipelineView> list() {
        return artifacts.listResources().stream()
                .filter(stored -> stored.resource() instanceof PipelineResource)
                .sorted(Comparator.comparing(stored -> stored.resource().id()))
                .map(this::view)
                .toList();
    }

    /** Returns the typed Pipeline view when the id resolves to a stored Pipeline. */
    public Optional<PipelineView> find(String id) {
        Objects.requireNonNull(id, "id");
        return artifacts.getResource(id)
                .filter(stored -> stored.resource() instanceof PipelineResource)
                .map(this::view);
    }

    private PipelineView view(StoredResource stored) {
        PipelineResource pipeline = pipeline(stored.resource());
        return representation.toView(pipeline, stored.contentHash(), sourceSummaries(pipeline));
    }

    private List<PipelineSourceSummary> sourceSummaries(PipelineResource pipeline) {
        List<PipelineSourceSummary> summaries = new ArrayList<>(pipeline.sources().size());
        for (String sourceId : pipeline.sources()) {
            StoredResource stored = artifacts.getResource(sourceId)
                    .orElseThrow(() -> inconsistentSourceReference(pipeline.id(), sourceId));
            if (!(stored.resource() instanceof SourceResource source)) {
                throw inconsistentSourceReference(pipeline.id(), sourceId);
            }
            summaries.add(new PipelineSourceSummary(source.id(), source.metadata(), source.connector()));
        }
        return List.copyOf(summaries);
    }

    private static PipelineResource pipeline(Resource resource) {
        if (resource instanceof PipelineResource pipeline) {
            return pipeline;
        }
        throw new IllegalStateException("Pipeline projection received a non-Pipeline resource");
    }

    private static IllegalStateException inconsistentSourceReference(String pipelineId, String sourceId) {
        return new IllegalStateException(
                "Pipeline " + pipelineId + " references " + sourceId + " as a Source, but it does not resolve");
    }
}
