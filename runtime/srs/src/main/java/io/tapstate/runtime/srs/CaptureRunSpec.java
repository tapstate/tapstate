package io.tapstate.runtime.srs;

import io.tapstate.spi.capture.CaptureConfig;
import io.tapstate.spi.capture.SourcePosition;
import io.tapstate.core.model.ReadMode;

import java.util.Objects;
import java.util.function.Supplier;

/**
 * One source run's inputs to the {@link CaptureRunUnit}: the connector config and the pipeline-level
 * dimensions that branch how it is consumed, plus the L1 mock collaborators standing in for real
 * connector machinery.
 *
 * <ul>
 *   <li>{@code config} / {@code readMode} / {@code srsEnabled} — what to read and how to consume it; the
 *       read mode and the source's {@code srs.enabled} flag resolve to a {@link ConsumptionPlan}.</li>
 *   <li>{@code srsKey} — an explicit mining-chain key overriding config-hash derivation, or null to
 *       derive from the config; {@code sourceId} / {@code pipelineId} — the source run-unit and the
 *       consumer pipeline the coordinator registers.</li>
 *   <li>{@code startFrom} — where this pipeline enters the incremental tail; {@code retention} — the
 *       pass-through retention config seeded on a new chain (may be null).</li>
 *   <li>{@code cdcStart} — the source position sampled at snapshot start (an L1 mock for the
 *       snapshot-to-cdc seam); {@code schemaVer} — the schema version stamped on ring items.</li>
 *   <li>{@code watermark} — the per-change source position generator (an L1 mock monotonic stand-in).
 *       What ranks the positions it hands out is not here: the order is the ring's own, assigned on
 *       append, so a run has nothing to be told about it.</li>
 * </ul>
 */
public record CaptureRunSpec(
        CaptureConfig config,
        ReadMode readMode,
        String srsKey,
        boolean srsEnabled,
        String sourceId,
        String pipelineId,
        StartFrom startFrom,
        SourcePosition cdcStart,
        String retention,
        long schemaVer,
        Supplier<SourcePosition> watermark) {

    public CaptureRunSpec {
        Objects.requireNonNull(config, "config");
        Objects.requireNonNull(readMode, "readMode");
        Objects.requireNonNull(sourceId, "sourceId");
        Objects.requireNonNull(pipelineId, "pipelineId");
        Objects.requireNonNull(startFrom, "startFrom");
        Objects.requireNonNull(cdcStart, "cdcStart");
        Objects.requireNonNull(watermark, "watermark");
    }
}
