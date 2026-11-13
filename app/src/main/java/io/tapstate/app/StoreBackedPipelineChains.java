package io.tapstate.app;

import io.tapstate.control.core.PipelineChains;
import io.tapstate.core.model.PipelineResource;
import io.tapstate.core.model.SourceResource;
import io.tapstate.spi.store.ArtifactStore;
import io.tapstate.spi.store.SourceModel;
import io.tapstate.spi.store.SrsStores;
import io.tapstate.spi.store.StorePort;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.function.Function;
import java.util.function.Supplier;

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
    @Override public Runnable readingGuard(String pipelineId) {
        return storePort instanceof ThreeLayerStorePort nativeStore?nativeStore.readingGuard(pipelineId):() -> {};
    }

    @Override
    public List<Chain> of(String pipelineId) {
        return resolutions(pipelineId,false).stream()
                .map(resolution -> new Chain(resolution.chainId().value(), resolution.sourceId(),
                        resolution.tables()))
                .toList();
    }

    @Override
    public boolean canReadPositions(String pipelineId, Chain chain) {
        return !(storePort instanceof ThreeLayerStorePort) || resolutions(pipelineId,true).stream()
                .anyMatch(resolution -> resolution.sourceId().equals(chain.sourceId())
                        && resolution.chainId().value().equals(chain.chainId()));
    }

    @Override
    public List<String> progressChainsOf(String pipelineId) {
        return progressChainsOf(pipelineId, storePort.artifacts(), source -> SourceDiscovery.model(storePort, source), null);
    }

    List<String> progressChainsOf(String pipelineId, ArtifactStore artifacts, Function<SourceResource, SourceModel> models,
            Supplier<SrsStores> stores) {
        return resolutions(pipelineId, true, artifacts, models, stores).stream()
                .flatMap(resolution -> storePort instanceof ThreeLayerStorePort
                        ?java.util.stream.Stream.of(resolution.chainId().value()):resolution.progressChainIds(pipelineId).stream())
                .distinct()
                .toList();
    }

    /** Each source the pipeline reads, resolved the way its capture resolves it, in declaration order. */
    private List<SourceCaptureResolution> resolutions(String pipelineId,boolean requireAttachment) {
        return resolutions(pipelineId, requireAttachment, storePort.artifacts(),
                source -> SourceDiscovery.model(storePort, source), null);
    }

    private List<SourceCaptureResolution> resolutions(String pipelineId, boolean requireAttachment,
            ArtifactStore artifacts, Function<SourceResource, SourceModel> models, Supplier<SrsStores> stores) {
        Objects.requireNonNull(pipelineId, "pipelineId");
        PipelineResource pipeline = StoredArtifacts.requirePipeline(artifacts, pipelineId);
        List<SourceCaptureResolution> resolutions = new ArrayList<>();
        for (String sourceId : pipeline.sourceIds()) {
            SourceResource source = StoredArtifacts.requireSource(artifacts, sourceId);
            var model = models.apply(source);
            if (stores != null && storePort instanceof ThreeLayerStorePort nativeStore) {
                SourceCaptureResolution.forPipeline(pipeline, source, model)
                        .flatMap(resolution -> nativeStore.sharedReadingForStart(pipeline, source, resolution, stores))
                        .ifPresent(resolutions::add);
            } else {
                SourceCaptureResolution.forStoredPipeline(storePort,pipeline, source, model,requireAttachment)
                        .ifPresent(resolutions::add);
            }
        }
        return List.copyOf(resolutions);
    }
}
