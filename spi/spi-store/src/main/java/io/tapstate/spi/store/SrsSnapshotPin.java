package io.tapstate.spi.store;

import java.util.Map;
import java.util.Objects;

/** Exact structural references held by one consumer's snapshot; row order does not grant capture authority. */
public record SrsSnapshotPin(long snapshotEpoch, String sourcePosition, Map<String, SrsModelKey> models) {
    public SrsSnapshotPin {
        Objects.requireNonNull(sourcePosition, "sourcePosition");
        models = Map.copyOf(Objects.requireNonNull(models, "models"));
        if (snapshotEpoch < 1 || snapshotEpoch == Long.MAX_VALUE || models.isEmpty()
                || models.entrySet().stream().anyMatch(entry -> !entry.getKey().equals(entry.getValue().table()))) {
            throw new IllegalArgumentException("invalid snapshot references");
        }
        var first = models.values().iterator().next();
        if (models.values().stream().anyMatch(key -> !first.miningChainId().equals(key.miningChainId())
                || !first.cacheIncarnation().equals(key.cacheIncarnation()))) throw new IllegalArgumentException("snapshot references span cache chains");
    }
}
