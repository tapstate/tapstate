package io.tapstate.spi.store;

/** Exact immutable model identity; source capture epochs are independent of Pipeline state generations. */
public record SrsModelKey(String miningChainId, String table, String cacheIncarnation, long captureEpoch, long modelVersion) {
    public SrsModelKey {
        if (miningChainId == null || miningChainId.isBlank() || table == null || table.isBlank()
                || cacheIncarnation == null || cacheIncarnation.isBlank() || captureEpoch < 1 || captureEpoch == Long.MAX_VALUE
                || modelVersion < 1 || modelVersion == Long.MAX_VALUE) {
            throw new IllegalArgumentException("invalid SRS model identity");
        }
    }
}
