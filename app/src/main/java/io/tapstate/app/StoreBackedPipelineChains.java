package io.tapstate.app;

import io.tapstate.control.core.PipelineChains;
import io.tapstate.core.model.PipelineResource;
import io.tapstate.core.model.SourceResource;
import io.tapstate.spi.store.StorePort;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

/**
 * Resolves a pipeline into the mining chains it reads, through the one derivation the capture side and
 * the reading side also run — so a position reported or written back names the chain a run is actually on.
 *
 * <p>A source whose schema has not been discovered cannot be resolved into tables, and therefore not into
 * a chain: the coded refusal that raises is the answer. This face exists to be acted on, and a chain
 * silently missing from it would be one a write-back is then told is unknown.
 */
final class StoreBackedPipelineChains implements PipelineChains {

    private final StorePort storePort;

    StoreBackedPipelineChains(StorePort storePort) {
        this.storePort = Objects.requireNonNull(storePort, "storePort");
    }

    @Override
    public List<Chain> of(String pipelineId) {
        return resolutions(pipelineId).stream()
                .map(resolution -> new Chain(resolution.chainId().value(), resolution.sourceId(),
                        resolution.tables()))
                .toList();
    }

    @Override
    public List<String> progressChainsOf(String pipelineId) {
        return resolutions(pipelineId).stream()
                .flatMap(resolution -> resolution.progressChainIds(pipelineId).stream())
                .distinct()
                .toList();
    }

    /** Each source the pipeline reads, resolved the way its capture resolves it, in declaration order. */
    private List<SourceCaptureResolution> resolutions(String pipelineId) {
        Objects.requireNonNull(pipelineId, "pipelineId");
        PipelineResource pipeline = StoredArtifacts.requirePipeline(storePort.artifacts(), pipelineId);
        List<SourceCaptureResolution> resolutions = new ArrayList<>();
        for (String sourceId : pipeline.sourceIds()) {
            SourceResource source = StoredArtifacts.requireSource(storePort.artifacts(), sourceId);
            SourceCaptureResolution.forPipeline(pipeline, source, SourceDiscovery.model(storePort, source))
                    .ifPresent(resolutions::add);
        }
        return List.copyOf(resolutions);
    }
}
