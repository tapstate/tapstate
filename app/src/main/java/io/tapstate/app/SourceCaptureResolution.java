package io.tapstate.app;

import io.tapstate.core.model.PipelineResource;
import io.tapstate.core.model.SourceResource;
import io.tapstate.core.model.SourceRef;
import io.tapstate.runtime.srs.MiningChainId;
import io.tapstate.runtime.srs.SrsRingbuffer;
import io.tapstate.spi.capture.CaptureConfig;
import io.tapstate.spi.store.SourceModel;
import java.util.List;
import java.util.Optional;
import java.util.stream.Stream;

/**
 * How a pipeline-referenced source resolves into its capture/read ring identity: the connector config, the
 * selected tables, the srs key, and the mining-chain id that keys each per-table change ring. The
 * authoring identity is canonical. A scoped run replaces its stream namespace only after matching that
 * canonical identity to its trusted capture preparation; its deferred readers consume the same binding.
 *
 * <p>An explicit {@code srs.key} keys the chain directly; otherwise it derives from the connector-and-settings
 * content hash. A pipeline reference with SRS disabled uses an independent recovery record keyed by its
 * pipeline and source instead of advancing the shared capture's record.
 *
 * @param sourceId the resource id of the source
 * @param config   the connector, its settings, and the selected streams to read
 * @param tables   the selected tables in deterministic selector/discovery order
 * @param srsKey   the explicit mining-chain key, or null to derive from the config hash
 * @param chainId  the mining-chain id both the writer and the reader key the ring under
 */
record SourceCaptureResolution(
        String sourceId,
        CaptureConfig config,
        List<String> tables,
        String srsKey,
        MiningChainId chainId) {

    static SourceCaptureResolution of(SourceResource source) {
        return of(source, null);
    }

    static SourceCaptureResolution of(SourceResource source, SourceModel discovered) {
        return withTables(source, SourceTableSelection.resolve(source, discovered));
    }

    static Optional<SourceCaptureResolution> forPipeline(
            PipelineResource pipeline, SourceResource source, SourceModel discovered) {
        return forSelectedTables(pipeline, source, PipelineTableSelection.resolve(pipeline, source, discovered));
    }

    /** Carries an already selected subset; write callers must validate it through forPipeline. */
    static Optional<SourceCaptureResolution> forSelectedTables(
            PipelineResource pipeline, SourceResource source, List<String> tables) {
        boolean direct = pipeline.sources().stream().anyMatch(ref -> ref.id().equals(source.id())
                && ref instanceof SourceRef.Spec spec && !spec.srs());
        return tables.isEmpty() ? Optional.empty()
                : Optional.of(withTables(source, tables).scopedTo(pipeline.id(), !direct));
    }

    /** Current diagnostic reads use the actual attached scope; retained run handles keep their original namespace. */
    static Optional<SourceCaptureResolution> forStoredPipeline(io.tapstate.spi.store.StorePort store,
            PipelineResource pipeline,SourceResource source,SourceModel discovered) {
        return forStoredPipeline(store,pipeline,source,discovered,true);
    }

    /** Describes a never-read chain without granting access to another consumer's progress. */
    static Optional<SourceCaptureResolution> forStoredPipeline(io.tapstate.spi.store.StorePort store,
            PipelineResource pipeline,SourceResource source,SourceModel discovered,boolean requireAttachment) {
        return forPipeline(pipeline,source,discovered).flatMap(resolution -> store instanceof ThreeLayerStorePort nativeStore
                ?nativeStore.sharedReading(pipeline,source,resolution,requireAttachment):Optional.of(resolution));
    }

    /** Resolves the same recovery namespace as the corresponding capture run. */
    SourceCaptureResolution scopedTo(String pipelineId, boolean srsEnabled) {
        MiningChainId scoped = srsEnabled ? MiningChainId.resolve(config, srsKey)
                : MiningChainId.forChannel(config, srsKey, pipelineId, sourceId);
        return new SourceCaptureResolution(sourceId, config, tables, srsKey, scoped);
    }

    SourceCaptureResolution onPhysicalChain(String canonical,String physical) {
        if(!chainId.value().equals(canonical) || physical==null || physical.isBlank())
            throw new io.tapstate.core.common.TapstateException(io.tapstate.spi.store.IoError.PIPELINE_INTENT_FENCED,java.util.Map.of(),null);
        return new SourceCaptureResolution(sourceId,config,tables,srsKey,new MiningChainId(physical));
    }

    /**
     * Every chain that can hold {@code pipelineId}'s durable progress through this source: the one it reads,
     * and the shared chain a direct capture recorded its progress on before it read a channel of its own.
     * A start's load is judged from all of them, by the run and by the verb predicting it alike.
     */
    List<String> progressChainIds(String pipelineId) {
        return chainId.equals(MiningChainId.forChannel(config,srsKey,pipelineId,sourceId))
                ?Stream.of(chainId.value(),scopedTo(pipelineId,true).chainId().value()).distinct().toList():List.of(chainId.value());
    }

    private static SourceCaptureResolution withTables(SourceResource source, List<String> tables) {
        CaptureConfig config = new CaptureConfig(source.connector(), source.config(), tables);
        String srsKey = source.srs() != null ? source.srs().key() : null;
        return new SourceCaptureResolution(
                source.id(), config, tables, srsKey, MiningChainId.resolve(config, srsKey));
    }

    /** Returns the first selected table; callers that need the full selection must use {@link #tables()}. */
    String table() {
        return tables.getFirst();
    }

    /** Returns the ring for the first selected table; callers that need all rings must iterate {@link #tables()}. */
    String ringName() {
        return ringName(table());
    }

    String ringName(String table) {
        return SrsRingbuffer.ringName(chainId.value(), table);
    }
}
