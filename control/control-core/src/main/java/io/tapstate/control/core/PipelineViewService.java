package io.tapstate.control.core;

import io.tapstate.core.common.TapstateException;
import io.tapstate.core.model.PipelineResource;
import io.tapstate.core.model.Resource;
import io.tapstate.core.model.SourceRef;
import io.tapstate.core.model.SourceResource;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;

/** Read-only Pipeline projection over the generic artifact query path. */
public final class PipelineViewService {

    private final ArtifactQueryService artifacts;
    private final PipelineRepresentation representation;
    private final PipelineObservationQueryService observations;

    public PipelineViewService(ArtifactQueryService artifacts, PipelineRepresentation representation) {
        this(artifacts, representation, null);
    }

    public PipelineViewService(
            ArtifactQueryService artifacts,
            PipelineRepresentation representation,
            PipelineObservationQueryService observations) {
        this.artifacts = Objects.requireNonNull(artifacts, "artifacts");
        this.representation = Objects.requireNonNull(representation, "representation");
        this.observations = observations;
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

    /** Returns a typed Pipeline view or the stable Pipeline not-found diagnostic. */
    public PipelineView get(String id) {
        Objects.requireNonNull(id, "id");
        return find(id).orElseThrow(() -> new TapstateException(
                PipelineError.NOT_FOUND, Map.of("id", id), null));
    }

    private PipelineView view(StoredResource stored) {
        PipelineResource pipeline = pipeline(stored.resource());
        PipelineStatus status = observations == null ? null : observations.findStatus(pipeline.id()).orElse(null);
        return representation.toView(pipeline, stored.contentHash(), sourceSummaries(pipeline), status);
    }

    private List<PipelineSourceSummary> sourceSummaries(PipelineResource pipeline) {
        List<PipelineSourceSummary> summaries = new ArrayList<>(pipeline.sources().size());
        for (SourceRef sourceRef : pipeline.sources()) {
            String sourceId = sourceRef.id();
            StoredResource stored = artifacts.getResource(sourceId).orElse(null);
            if (stored == null || !(stored.resource() instanceof SourceResource source)) {
                summaries.add(PipelineSourceSummary.unresolved(sourceId));
                continue;
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

}
