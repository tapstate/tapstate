package io.tapstate.spi.store;

import java.util.List;
import java.util.Objects;

/** Recovery metadata belongs to the same fenced row as its opaque Nest state. */
public interface NestStateStore extends KeyedStateStore {
    /** Replaces payload and pending metadata atomically; a null key removes the pending metadata. */
    void saveNest(String namespace, String key, byte[] state, byte[] pendingKey);

    /** A bounded, ordered page of pending identities only, strictly after the opaque cursor. */
    List<Pending> pending(String namespace, String after, int limit);

    record Pending(String cursor, byte[] key) {
        public Pending {
            Objects.requireNonNull(cursor, "cursor");
            key = Objects.requireNonNull(key, "key").clone();
        }
        @Override public byte[] key() { return key.clone(); }
    }
}
