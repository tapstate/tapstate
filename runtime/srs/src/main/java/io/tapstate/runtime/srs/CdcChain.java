package io.tapstate.runtime.srs;

import io.tapstate.spi.capture.SourcePosition;
import io.tapstate.spi.store.SrsMetaStore;

import java.util.Objects;
import java.util.function.Supplier;

/**
 * The write-side wiring for one mining chain's cdc phase: where change events land (the per-table headroom
 * {@code gate}), where the durable read offset is persisted ({@code meta} under {@code miningChainId}), the
 * {@code watermark} that stamps each change its source position, the {@code epoch} the ring is running
 * under, and the {@code schemaVer} in force. An immutable value grouping the stable collaborators the cdc
 * listener closes over.
 *
 * <p>The generation is what makes a ring sequence comparable at all. A sequence alone is only meaningful
 * within the ring that assigned it: a rebuilt ring numbers from zero again, so a change of the new ring
 * would read as older than one of the ring before it. The pair is what the durable-frontier bound ranks
 * positions by, in place of an order over tokens that no connector supplies.
 */
public record CdcChain(
        SrsWriteGate gate,
        SrsMetaStore meta,
        String miningChainId,
        Supplier<SourcePosition> watermark,
        long epoch,
        long schemaVer) {

    public CdcChain {
        Objects.requireNonNull(gate, "gate");
        Objects.requireNonNull(meta, "meta");
        Objects.requireNonNull(miningChainId, "miningChainId");
        Objects.requireNonNull(watermark, "watermark");
    }
}
