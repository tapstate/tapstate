package io.tapstate.runtime.srs;

import io.tapstate.spi.capture.CaptureConfig;
import io.tapstate.core.model.PipelineNode;
import io.tapstate.core.model.ReadMode;

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
 *   <li>{@code schemaVer} — the schema version stamped on ring items.</li>
 * </ul>
 *
 * <p>No position of any kind is carried here. Both a run's seam and its per-change positions are the
 * source's own and are learned from it as the read happens — the seam from the snapshot batch, each
 * change's from the change itself. A position supplied alongside the run instead was a stand-in for a
 * connector, and a stand-in is what makes a restart's positions start over while its generation rises.
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
        long schemaVer) {

    public CaptureRunSpec {
        Objects.requireNonNull(config, "config");
        Objects.requireNonNull(readMode, "readMode");
        Objects.requireNonNull(sourceId, "sourceId");
        Objects.requireNonNull(pipelineId, "pipelineId");
        Objects.requireNonNull(startFrom, "startFrom");
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
