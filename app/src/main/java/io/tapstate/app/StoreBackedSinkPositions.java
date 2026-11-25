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
        var nativeStore = storePort instanceof ThreeLayerStorePort scoped ? scoped : null;
        var current = nativeStore == null ? null : nativeStore.planningScope(pipelineId);
        Map<String, SourceCaptureResolution> resolutions = new LinkedHashMap<>();
        Map<String, SourceResource> shared = new LinkedHashMap<>();
        for (String binding : pipeline.get().sourceBindings()) {
            var ref = pipeline.get().sourceReference(binding);
            SourceResource source = StoredArtifacts.requireSource(storePort.artifacts(), ref.id());
            SourceCaptureResolution resolution;
            try {
                resolution = SourceCaptureResolution.forPipeline(
                        pipeline.get(), ref, source, SourceDiscovery.model(storePort, source)).orElse(null);
            } catch (TapstateException unresolved) {
                if (unresolved.code() == ActuationError.SOURCE_SCHEMA_NOT_DISCOVERED) {
                    // The read face treats a not-yet-resolvable selection as absent while discovery catches up.
                    continue;
                }
                throw unresolved;
            }
            if (resolution == null) continue;
            resolutions.put(binding, resolution);
            boolean direct = ref instanceof io.tapstate.core.model.SourceRef.Spec spec && !spec.srs();
            if (nativeStore != null && !direct && (pipeline.get().settings() == null
                    || pipeline.get().settings().readMode() != io.tapstate.core.model.ReadMode.SNAPSHOT_ONLY))
                shared.put(binding, source);
        }
        Map<String, String> positions = new LinkedHashMap<>();
        resolutions.forEach((sourceId, resolution) -> {
            if (shared.containsKey(sourceId)) {
                nativeStore.sharedSinkOffset(current, shared.get(sourceId), resolution)
                        .ifPresent(offset -> addLabeledPositions(positions, sourceId, resolution, resolutions.values(), offset));
                return;
            }
            boolean singleNode = resolutions.values().stream()
                    .filter(candidate -> candidate.chainId().equals(resolution.chainId())).count() == 1;
            ConsumerOffset offset = storePort.meta().read(resolution.chainId().value())
                    .flatMap(meta -> meta.consumerOffset(SrsConsumerId.of(pipelineId, sourceId).value())
                            .or(() -> singleNode ? meta.consumerOffset(pipelineId) : Optional.empty()))
                    .orElse(null);
            if (offset != null) {
                addLabeledPositions(positions, sourceId, resolution, resolutions.values(), offset);
            }
        });
        if (current != null) current.verify().run();
        return positions;
    }

    private static void addLabeledPositions(Map<String, String> positions, String binding,
            SourceCaptureResolution resolution, java.util.Collection<SourceCaptureResolution> resolutions,
            ConsumerOffset offset) {
        var confirmed = new LinkedHashMap<String, String>();
        addConfirmedPositions(confirmed, resolution.tables(), offset);
        confirmed.forEach((table, position) -> positions.put(PipelineTableSelection.label(binding, table,
                resolutions.stream().map(SourceCaptureResolution::tables)), position));
    }

    /** The same durable acknowledgement projection serves live samples and stored-position fallbacks. */
    static void addConfirmedPositions(Map<String, String> positions, java.util.Collection<String> tables,
            ConsumerOffset offset) {
        for (String table : tables) {
            ChainPosition confirmed = offset.sinkAckedByTable().get(table);
            if (confirmed == null && (offset.progressKind() == ConsumerProgressKind.DIRECT_SOURCE
                    || (offset.progressKind() == ConsumerProgressKind.LEGACY
                            && tables.size() == 1 && offset.perTableSeq().size() <= 1))) {
                confirmed = offset.sinkAcked();
            }
            if (confirmed != null && confirmed.token() != null) {
                positions.put(table, confirmed.token());
            }
        }
    }

    /** The stored pipeline for the id, or empty when its artifact is absent or names another kind. */
    private Optional<PipelineResource> pipeline(String pipelineId) {
        return storePort.artifacts().get(pipelineId)
                .filter(PipelineResource.class::isInstance)
                .map(PipelineResource.class::cast);
    }

}
