package io.tapstate.spi.store;

import java.util.Map;
import java.util.Objects;

/** Chain-owned references for a fixed fresh baseline until a real complete source batch consumes it. */
public record SrsFreshFull(SrsFreshFullOrigin origin, long snapshotEpoch, Map<String, SrsModelKey> models) {
    public static final int MAX_ENCODED_REQUEST_BYTES = 256 * 1024;

    public SrsFreshFull {
        Objects.requireNonNull(origin, "origin");
        models = Map.copyOf(Objects.requireNonNull(models, "models"));
        if (snapshotEpoch < 1 || snapshotEpoch == Long.MAX_VALUE || models.isEmpty())
            throw new IllegalArgumentException("invalid fresh full baseline");
        String chain = models.values().iterator().next().miningChainId();
        for (var entry : models.entrySet()) {
            var key = entry.getValue();
            if (!entry.getKey().equals(key.table()) || !chain.equals(key.miningChainId())
                    || !origin.targetScope().cacheIncarnation().equals(key.cacheIncarnation())
                    || key.captureEpoch() != snapshotEpoch)
                throw new IllegalArgumentException("fresh baseline references span different scopes");
        }
    }
}
