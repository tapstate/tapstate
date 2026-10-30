package io.tapstate.spi.store;

import java.util.List;
import java.util.Objects;
import java.util.Arrays;

/** A narrowly indexed recovery facet for unfinished Nest handoffs, never an operator keyspace listing. */
public interface NestReleaseStore extends KeyedStateStore {
    int MAX_PAGE_KEYS = 256;

    /** The stored address and the original opaque, lossless routing key; no state body is prefetched. */
    record PendingKey(String stateKey, byte[] routingKey) {
        public PendingKey {
            Objects.requireNonNull(stateKey, "stateKey");
            routingKey = Objects.requireNonNull(routingKey, "routingKey").clone();
        }
        @Override public byte[] routingKey() { return routingKey.clone(); }
        @Override public boolean equals(Object value) {
            return value instanceof PendingKey other && stateKey.equals(other.stateKey) && Arrays.equals(routingKey, other.routingKey);
        }
        @Override public int hashCode() { return 31 * stateKey.hashCode() + Arrays.hashCode(routingKey); }
        @Override public String toString() { return "PendingKey[stateKey=redacted, routingKey=" + routingKey.length + " bytes]"; }
    }

    /** Writes state and its pending marker atomically; a null routing key removes that marker. */
    void saveWithRelease(String namespace, String key, byte[] state, byte[] routingKey);

    /**
     * Reads only unfinished rows of this fixed namespace, after the last returned state key, or from
     * the beginning for null. Pages are bounded by the existing key budget and backend byte budget.
     * An empty page ends this pass; the cursor is in memory and is never a new durable acknowledgement.
     */
    List<PendingKey> pendingReleases(String namespace, String afterKey);
}
