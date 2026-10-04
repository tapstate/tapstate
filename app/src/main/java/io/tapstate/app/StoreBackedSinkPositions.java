package io.tapstate.app;

import io.tapstate.core.common.TapstateException;
import io.tapstate.core.event.ChainPosition;
import io.tapstate.core.model.PipelineResource;
import io.tapstate.core.model.SourceResource;
import io.tapstate.spi.store.ConsumerOffset;
import io.tapstate.spi.store.ConsumerProgressKind;
import io.tapstate.spi.store.SrsConsumerId;
import io.tapstate.spi.store.StorePort;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.function.Function;

/**
 * The read side of the durable sink-acked position, for the observation read face: given a pipeline id it
 * yields, per selected table the pipeline's sources read, the opaque source position that pipeline's sink has
 * durably acked. It resolves each source to its tables and mining chain the same way the sink-ack writer does
 * -- the shared source resolution -- so the position a sink advances under a chain is the position this reads
 * back for that table and source node. A sibling table's cursor cannot serve as this table's confirmation.
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
        Map<String, SourceCaptureResolution> resolutions = new LinkedHashMap<>();
        for (String sourceId : pipeline.get().sourceIds()) {
            SourceResource source = StoredArtifacts.requireSource(storePort.artifacts(), sourceId);
            SourceCaptureResolution resolution;
            try {
                resolution = SourceCaptureResolution.forPipeline(
                        pipeline.get(), source, SourceDiscovery.model(storePort, source)).orElse(null);
            } catch (TapstateException unresolved) {
                if (unresolved.code() == ActuationError.SOURCE_SCHEMA_NOT_DISCOVERED) {
                    // The read face treats a not-yet-resolvable selection as absent while discovery catches up.
                    continue;
                }
                throw unresolved;
            }
            if (resolution != null) {
                resolutions.put(sourceId, resolution);
            }
        }
        Map<String, String> positions = new LinkedHashMap<>();
        resolutions.forEach((sourceId, resolution) -> {
            boolean singleNode = resolutions.values().stream()
                    .filter(candidate -> candidate.chainId().equals(resolution.chainId())).count() == 1;
            storePort.meta().read(resolution.chainId().value()).ifPresent(meta -> {
                ConsumerOffset offset = meta.consumerOffset(SrsConsumerId.of(pipelineId, sourceId).value())
                        .orElseGet(() -> singleNode ? meta.consumerOffset(pipelineId).orElse(null) : null);
                if (offset == null) {
                    return;
                }
                for (String table : resolution.tables()) {
                    ChainPosition confirmed = offset.sinkAckedByTable().get(table);
                    if (confirmed == null && (offset.progressKind() == ConsumerProgressKind.DIRECT_SOURCE
                            || (offset.progressKind() == ConsumerProgressKind.LEGACY
                                    && resolution.tables().size() == 1 && offset.perTableSeq().size() <= 1))) {
                        confirmed = offset.sinkAcked();
                    }
                    if (confirmed != null && confirmed.token() != null) {
                        positions.put(table, confirmed.token());
                    }
                }
            });
        });
        return positions;
    }

    /** The stored pipeline for the id, or empty when its artifact is absent or names another kind. */
    private Optional<PipelineResource> pipeline(String pipelineId) {
        return storePort.artifacts().get(pipelineId)
                .filter(PipelineResource.class::isInstance)
                .map(PipelineResource.class::cast);
    }

}
