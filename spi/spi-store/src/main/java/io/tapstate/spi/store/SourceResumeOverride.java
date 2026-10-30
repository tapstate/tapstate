package io.tapstate.spi.store;

import java.time.Instant;
import java.util.Objects;

/** An explicit next-start request, distinct from the source checkpoint that actually happened. */
public record SourceResumeOverride(String token, long fromEpoch, Instant editedAt) {
    public SourceResumeOverride {
        Objects.requireNonNull(token, "token"); Objects.requireNonNull(editedAt, "editedAt");
        if (fromEpoch < 0 || fromEpoch == Long.MAX_VALUE) throw new IllegalArgumentException("invalid original source epoch");
    }
}
