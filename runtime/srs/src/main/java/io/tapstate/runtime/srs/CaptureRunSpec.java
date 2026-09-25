package io.tapstate.runtime.srs;

import io.tapstate.spi.capture.CaptureConfig;
import io.tapstate.core.model.PipelineNode;
import io.tapstate.core.model.ReadMode;
import io.tapstate.spi.store.WorkloadClaimFence;

import java.util.List;
import java.util.Objects;

/**
 * One source run's inputs to the {@link CaptureRunUnit}: the connector config and the pipeline-level
 * dimensions that branch how it is consumed.
 *
 * <ul>
 *   <li>{@code config} / {@code readMode} / {@code srsEnabled} — what to read and how to consume it; the
 *       read mode and the source's {@code srs.enabled} flag resolve to a {@link ConsumptionPlan}.</li>
 *   <li>{@code srsKey} — an explicit mining-chain key overriding config-hash derivation, or null to
 *       derive from the config; {@code sourceId} / {@code pipelineId} — the source run-unit and the
 *       consumer pipeline the coordinator registers.</li>
 *   <li>{@code startFrom} — where this pipeline enters the incremental tail; {@code retention} — the
 *       pass-through retention config seeded on a new chain (may be null).</li>
 *   <li>{@code schemaVer} — the schema version stamped on ring items; {@code snapshotEpoch} — the
 *       generation assigned to a bounded read that has no change chain of its own.</li>
 *   <li>{@code captureFence} — the cluster claim generation a durable append must still match, or null on
 *       the unchanged single-member path.</li>
 *   <li>{@code selectedChainTables} — the full table selection of this pipeline on the mining chain,
 *       across its source run units; null when a standalone caller selects only this source's streams.</li>
 *   <li>{@code cursorWriterToken} — one internal cursor-writer fence shared by this pipeline's source run
 *       units during one start, distinct from the product's execution identity.</li>
 * </ul>
 *
 * <p>No connector position is carried here. Both a run's seam and its per-change positions are the
 * source's own and are learned from it as the read happens — the seam from the snapshot batch, each
 * change's from the change itself. {@code snapshotEpoch} is an engine order, not a resumable source
 * coordinate: it exists precisely where no tail and therefore no connector position exists. A source
 * position supplied alongside the run instead would be a stand-in for a connector, and a stand-in is what
 * makes a restart's positions start over while its generation rises.
 */
public record CaptureRunSpec(
        CaptureConfig config,
        ReadMode readMode,
        String srsKey,
        boolean srsEnabled,
        String sourceId,
        String pipelineId,
        StartFrom startFrom,
        String retention,
        long schemaVer,
        long snapshotEpoch,
        WorkloadClaimFence captureFence,
        List<String> selectedChainTables,
        String cursorWriterToken) {

    public CaptureRunSpec(
            CaptureConfig config,
            ReadMode readMode,
            String srsKey,
            boolean srsEnabled,
            String sourceId,
            String pipelineId,
            StartFrom startFrom,
            String retention,
            long schemaVer,
            long snapshotEpoch,
            WorkloadClaimFence captureFence) {
        this(config, readMode, srsKey, srsEnabled, sourceId, pipelineId,
                startFrom, retention, schemaVer, snapshotEpoch, captureFence, null, null);
    }

    /**
     * The ordinary construction used by callers that do not allocate a chainless snapshot generation.
     * Zero asks the run unit for its local next generation; the assembled product supplies its durable
     * per-pipeline generation explicitly instead.
     */
    public CaptureRunSpec(
            CaptureConfig config,
            ReadMode readMode,
            String srsKey,
            boolean srsEnabled,
            String sourceId,
            String pipelineId,
            StartFrom startFrom,
            String retention,
            long schemaVer) {
        this(config, readMode, srsKey, srsEnabled, sourceId, pipelineId, startFrom, retention, schemaVer, 0L);
    }

    /**
     * The same, for a caller that allocates a chainless snapshot generation but holds the run to no claim.
     * A single-member run is fenced against nobody: there is one member and one run, so there is no second
     * one for an append to be checked against.
     */
    public CaptureRunSpec(
            CaptureConfig config,
            ReadMode readMode,
            String srsKey,
            boolean srsEnabled,
            String sourceId,
            String pipelineId,
            StartFrom startFrom,
            String retention,
            long schemaVer,
            long snapshotEpoch) {
        this(config, readMode, srsKey, srsEnabled, sourceId, pipelineId,
                startFrom, retention, schemaVer, snapshotEpoch, null);
    }

    /** The same spec, with every durable append it drives held to {@code fence}'s claim generation. */
    public CaptureRunSpec withCaptureFence(WorkloadClaimFence fence) {
        return new CaptureRunSpec(
                config, readMode, srsKey, srsEnabled, sourceId, pipelineId,
                startFrom, retention, schemaVer, snapshotEpoch, fence, selectedChainTables,
                cursorWriterToken);
    }

    /** The same run with its pipeline's complete table selection and cursor-writer fence. */
    public CaptureRunSpec withChainSelection(List<String> tables, String runId) {
        return new CaptureRunSpec(
                config, readMode, srsKey, srsEnabled, sourceId, pipelineId,
                startFrom, retention, schemaVer, snapshotEpoch, captureFence,
                List.copyOf(tables), Objects.requireNonNull(runId, "runId"));
    }

    public CaptureRunSpec {
        Objects.requireNonNull(config, "config");
        Objects.requireNonNull(readMode, "readMode");
        Objects.requireNonNull(sourceId, "sourceId");
        Objects.requireNonNull(pipelineId, "pipelineId");
        Objects.requireNonNull(startFrom, "startFrom");
        if (snapshotEpoch < 0) {
            throw new IllegalArgumentException(
                    "a chainless snapshot generation must not be negative, got " + snapshotEpoch);
        }
        selectedChainTables = selectedChainTables == null ? null : List.copyOf(selectedChainTables);
        if (cursorWriterToken != null && cursorWriterToken.isBlank()) {
            throw new IllegalArgumentException("cursorWriterToken must be non-blank");
        }
        // The connector doing this read files notes it has to find again on a later drive, and which node
        // they belong to is the pair named right here. Scoped from those two rather than accepted on the
        // config, so there is one derivation of the pair instead of two held together by nobody: a caller
        // that stopped scoping the config, or scoped it to another node, would leave the connector filing
        // under a name no later drive looks at, and nothing above would say so - the rows all arrive, the
        // chain is still mined once, and the only trace is a connector reading its own notes as empty,
        // which is what a first run looks like.
        config = config.at(new PipelineNode(pipelineId, sourceId));
    }
}
