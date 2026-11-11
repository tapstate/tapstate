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
    private final PipelineOutputNames outputNames;

    public PipelineViewService(ArtifactQueryService artifacts, PipelineRepresentation representation) {
        this(artifacts, representation, null);
    }

    public PipelineViewService(
            ArtifactQueryService artifacts,
            PipelineRepresentation representation,
            PipelineObservationQueryService observations) {
        this(artifacts, representation, observations, null);
    }

    public PipelineViewService(ArtifactQueryService artifacts, PipelineRepresentation representation,
            PipelineObservationQueryService observations, PipelineOutputNames outputNames) {
        this.outputNames = outputNames;
        this.artifacts = Objects.requireNonNull(artifacts, "artifacts");
        this.representation = Objects.requireNonNull(representation, "representation");
        this.observations = observations;
    }

    /** Lists stored Pipelines in stable id order. */
    public List<PipelineView> list() {
        return list(0, Integer.MAX_VALUE);
    }

    /** Lists one artifact-backed page, resolving Source summaries only for rows in that page. */
    public List<PipelineView> list(int offset, int limit) {
        if (offset < 0 || limit < 1) {
            throw new IllegalArgumentException("pipeline page offset must be non-negative and limit positive");
        }
        List<StoredResource> matching = artifacts.listResources("pipeline").stream()
                .filter(stored -> stored.resource() instanceof PipelineResource)
                .sorted(Comparator.comparing(stored -> stored.resource().id()))
                .toList();
        if (offset >= matching.size()) return List.of();
        int end = (int) Math.min((long) matching.size(), (long) offset + limit);
        var page = matching.subList(offset, end);
        var statuses = observations == null ? Map.<String, PipelineStatus>of()
                : observations.findStatuses(page.stream().map(stored -> stored.resource().id()).toList());
        var sources = new java.util.HashMap<String, Optional<StoredResource>>();
        return page.stream().map(stored -> view(stored, statuses.get(stored.resource().id()), sources)).toList();
    }

    /** Returns the typed Pipeline view when the id resolves to a stored Pipeline. */
    public Optional<PipelineView> find(String id) {
        Objects.requireNonNull(id, "id");
        return artifacts.getResource("pipeline", id)
                .filter(stored -> stored.resource() instanceof PipelineResource)
                .map(this::view);
    }

    /** Returns a typed Pipeline view or the stable Pipeline not-found diagnostic. */
    public PipelineView get(String id) {
        Objects.requireNonNull(id, "id");
        return find(id).orElseThrow(() -> new TapstateException(
                PipelineError.NOT_FOUND, Map.of("id", id), null));
    }

    void requireDefinition(String id) {
        if (artifacts.getResource("pipeline", id).isEmpty()) {
            throw new TapstateException(PipelineError.NOT_FOUND, Map.of("id", id), null);
        }
    }

    private PipelineView view(StoredResource stored) {
        PipelineResource pipeline = pipeline(stored.resource());
        PipelineStatus status = observations == null ? null : observations.findStatuses(List.of(pipeline.id())).get(pipeline.id());
        return view(stored, status, new java.util.HashMap<>());
    }

    private PipelineView view(StoredResource stored, PipelineStatus status, Map<String, Optional<StoredResource>> sources) {
        PipelineResource pipeline = pipeline(stored.resource());
        var view = representation.toView(pipeline, stored.contentHash(), sourceSummaries(pipeline, sources), status);
        return outputNames == null ? view : view.withOutputTargets(outputNames.resolve(pipeline));
    }

    private List<PipelineSourceSummary> sourceSummaries(PipelineResource pipeline, Map<String, Optional<StoredResource>> sources) {
        List<PipelineSourceSummary> summaries = new ArrayList<>(pipeline.sources().size());
        for (SourceRef sourceRef : pipeline.sources()) {
            String sourceId = sourceRef.id();
            StoredResource stored = sources.computeIfAbsent(sourceId, id -> artifacts.getResource("source", id)).orElse(null);
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
