package io.tapstate.spi.store;

import java.util.Collection;
import java.util.List;
import java.util.Map;
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

    /** One key's write in a commit group: its state and, when it carries a pending release, the routing key. */
    record GroupWrite(byte[] state, byte[] routingKey) {
        public GroupWrite {
            state = Objects.requireNonNull(state, "state").clone();
            routingKey = routingKey == null ? null : routingKey.clone();
        }
    }

    /**
     * Writes and deletes a group of keys of one namespace. The default issues them as separate calls, so
     * a process that dies part way leaves some of the group in the store and some not; a store that can
     * do better commits the whole group in one transaction and says so through {@link #atomicGroups()}.
     */
    default void commitGroup(String namespace, Map<String, GroupWrite> writes, Collection<String> deletes) {
        Map<String, byte[]> plain = new java.util.LinkedHashMap<>();
        writes.forEach((key, write) -> { if (write.routingKey() == null) plain.put(key, write.state()); });
        if (!plain.isEmpty()) saveAll(namespace, plain);
        writes.forEach((key, write) -> {
            if (write.routingKey() != null) saveWithRelease(namespace, key, write.state(), write.routingKey());
        });
        for (String key : deletes) delete(namespace, key);
    }

    /** True when {@link #commitGroup} lands a group as a unit or not at all. */
    default boolean atomicGroups() {
        return false;
    }
}
