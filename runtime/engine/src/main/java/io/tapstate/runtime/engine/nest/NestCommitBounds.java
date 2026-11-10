package io.tapstate.runtime.engine.nest;

import java.util.concurrent.TimeUnit;

/**
 * Where a vertex instance closes its commit group: at so many keys, at so many bytes, or once its first
 * staged change has waited so long, whichever comes first. The end of a drain is deliberately not one of
 * them: closing at every drain makes groups shrink exactly as the store slows down, the opposite of what
 * grouping is for.
 *
 * <p>The byte bound is also the most one value may weigh. A group is one store transaction and a value is
 * never split across two, so a value over the bound is refused rather than written.
 *
 * @param maxKeys    keys one group may stage before it closes
 * @param maxBytes   bytes one group may stage before it closes, and the most one value may weigh
 * @param maxDelayNanos how long the first staged change of a group may wait for the group to close; the send
 *                   window, so a change is not held back longer than a document already was
 */
public record NestCommitBounds(int maxKeys, long maxBytes, long maxDelayNanos) {

    static final String MAX_KEYS_PROPERTY = "tapstate.nest.commit.max-keys";
    static final String MAX_BYTES_PROPERTY = "tapstate.nest.commit.max-bytes";
    static final String MAX_DELAY_PROPERTY = "tapstate.nest.commit.max-delay-millis";

    public static final int DEFAULT_MAX_KEYS = 256;
    public static final long DEFAULT_MAX_BYTES = 8L * 1024 * 1024;
    public static final long DEFAULT_MAX_DELAY_MILLIS = 50L;

    public NestCommitBounds {
        if (maxKeys < 1 || maxBytes < 1 || maxDelayNanos < 0) {
            throw new IllegalArgumentException("commit bounds must be positive");
        }
    }

    /** The defaults, each replaced by its system property where one is set. */
    public static NestCommitBounds configured() {
        return new NestCommitBounds(Integer.getInteger(MAX_KEYS_PROPERTY, DEFAULT_MAX_KEYS),
                Long.getLong(MAX_BYTES_PROPERTY, DEFAULT_MAX_BYTES),
                TimeUnit.MILLISECONDS.toNanos(Long.getLong(MAX_DELAY_PROPERTY, DEFAULT_MAX_DELAY_MILLIS)));
    }
}
