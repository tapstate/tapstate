package io.tapstate.app;

import io.tapstate.core.model.PipelineResource;
import io.tapstate.spi.store.ConsumerOffset;
import io.tapstate.spi.store.StorePort;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.function.Function;

/**
 * The read side of the durable sink-acked position, for the observation read face: given a pipeline id it
 * yields, per table the pipeline's sources read, the opaque source position that pipeline's sink has durably
 * acked. It resolves each source to its table and mining chain the same way the sink-ack writer does -- the
 * shared source resolution -- so the position a sink advances under a chain is the position this reads back
 * for that table. A single pipeline-level acked position keys onto the table its chain carries, so a
 * pipeline reading several tables reports each table's own acked position rather than one value repeated.
 *
 * <p>Present-only, so the projection stays honest: a table whose sink has not acked yet, a chain that holds
 * no consumer record for the pipeline, and a pipeline whose artifact is no longer stored all yield no entry
 * -- absence reads as "not acked yet", never a sentinel. Bound at the assembly point as the observation
 * publisher's position source, the one place with both the table names and the store to read them from.
 */
final class StoreBackedSinkPositions implements Function<String, Map<String, String>> {

    private final StorePort storePort;

    StoreBackedSinkPositions(StorePort storePort) {
        this.storePort = Objects.requireNonNull(storePort, "storePort");
    }

    @Override
    public Map<String, String> apply(String pipelineId) {
        Objects.requireNonNull(pipelineId, "pipelineId");
        Optional<PipelineResource> pipeline = pipeline(pipelineId);
        if (pipeline.isEmpty()) {
            return Map.of();
        }
        Map<String, String> positions = new LinkedHashMap<>();
        for (String sourceId : pipeline.get().sources()) {
            SourceCaptureResolution resolution = SourceCaptureResolution.of(
                    StoredArtifacts.requireSource(storePort.artifacts(), sourceId),
                    storePort.schemas().get(sourceId).map(io.tapstate.spi.store.DiscoveredSourceModel::model).orElse(null));
            ackedSrcpos(resolution.chainId().value(), pipelineId)
                    .ifPresent(srcpos -> resolution.tables().forEach(table -> positions.put(table, srcpos)));
        }
        return positions;
    }

    /** The stored pipeline for the id, or empty when its artifact is absent or names another kind. */
    private Optional<PipelineResource> pipeline(String pipelineId) {
        return storePort.artifacts().get(pipelineId)
                .filter(PipelineResource.class::isInstance)
                .map(PipelineResource.class::cast);
    }

    /** The pipeline's durable sink-acked source position on the chain, or empty when it has not acked one. */
    private Optional<String> ackedSrcpos(String miningChainId, String pipelineId) {
        return storePort.meta().read(miningChainId).stream()
                .flatMap(meta -> meta.consumerOffsets().stream())
                .filter(offset -> offset.pipelineId().equals(pipelineId))
                .map(ConsumerOffset::sinkAckedSrcpos)
                .filter(Objects::nonNull)
                .findFirst();
    }
}
